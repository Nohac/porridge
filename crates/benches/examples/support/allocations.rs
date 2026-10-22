//! Count allocator API activity only in isolated diagnostic intervals.
//! Requested bytes are not resident memory or live bytes: frees may refer to
//! allocations made before the interval, and realloc is reported separately.
use std::{
    alloc::{GlobalAlloc, Layout, System},
    cell::Cell,
    future::{Future, poll_fn},
    pin::pin,
    sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed},
};

pub struct CountingAllocator;
static ENABLED: AtomicBool = AtomicBool::new(false);
static COUNTS: [AtomicU64; 7] = [const { AtomicU64::new(0) }; 7];
static KERNEL_COUNTS: [AtomicU64; 7] = [const { AtomicU64::new(0) }; 7];
thread_local! {
    static IN_KERNEL: Cell<bool> = const { Cell::new(false) };
}

struct KernelScope(bool);

impl Drop for KernelScope {
    fn drop(&mut self) {
        IN_KERNEL.set(self.0);
    }
}

/// Attribute only explicit user computation, never an await-spanning TLS scope.
/// Engine API calls must stay outside this scope (or they would be mislabelled).
#[allow(dead_code)]
pub async fn kernel<F: Future>(future: F) -> F::Output {
    scope(future, true).await
}

/// Exclude explicit runtime/engine calls made inside a scoped control kernel.
#[allow(dead_code)]
pub async fn engine<F: Future>(future: F) -> F::Output {
    scope(future, false).await
}

async fn scope<F: Future>(future: F, kernel: bool) -> F::Output {
    let mut future = pin!(future);
    poll_fn(|cx| {
        let _scope = KernelScope(IN_KERNEL.replace(kernel));
        future.as_mut().poll(cx)
    })
    .await
}

fn count(index: usize, value: u64) {
    COUNTS[index].fetch_add(value, Relaxed);
    if IN_KERNEL.try_with(Cell::get).unwrap_or(false) {
        KERNEL_COUNTS[index].fetch_add(value, Relaxed);
    }
}

#[allow(dead_code)]
pub fn kernel_snapshot() -> Vec<(&'static str, u64)> {
    NAMES
        .into_iter()
        .zip(KERNEL_COUNTS.iter().map(|value| value.load(Relaxed)))
        .collect()
}

const NAMES: [&str; 7] = [
    "alloc_calls",
    "alloc_requested_bytes",
    "dealloc_calls",
    "dealloc_requested_bytes",
    "realloc_calls",
    "realloc_old_bytes",
    "realloc_new_bytes",
];

pub fn enable(enabled: bool) {
    ENABLED.store(enabled, Relaxed);
}

pub fn snapshot() -> Vec<(&'static str, u64)> {
    NAMES
        .into_iter()
        .zip(COUNTS.iter().map(|value| value.load(Relaxed)))
        .collect()
}

// SAFETY: all operations delegate to System with the original pointer/layout.
// Counter updates allocate no memory and never inspect the allocation contents.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() && ENABLED.load(Relaxed) {
            count(0, 1);
            count(1, layout.size() as u64);
        }
        ptr
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc_zeroed(layout) };
        if !ptr.is_null() && ENABLED.load(Relaxed) {
            count(0, 1);
            count(1, layout.size() as u64);
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        if ENABLED.load(Relaxed) {
            count(2, 1);
            count(3, layout.size() as u64);
        }
        unsafe { System.dealloc(ptr, layout) };
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        let result = unsafe { System.realloc(ptr, layout, size) };
        if !result.is_null() && ENABLED.load(Relaxed) {
            count(4, 1);
            count(5, layout.size() as u64);
            count(6, size as u64);
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::task::{Context, Poll};

    #[test]
    fn kernel_attribution_ends_at_each_poll_and_unwind() {
        let mut first = true;
        let mut future = pin!(kernel(poll_fn(|_| {
            assert!(IN_KERNEL.get());
            if std::mem::take(&mut first) {
                Poll::Pending
            } else {
                Poll::Ready(())
            }
        })));
        let mut context = Context::from_waker(futures::task::noop_waker_ref());
        assert!(future.as_mut().poll(&mut context).is_pending());
        assert!(!IN_KERNEL.get());
        assert!(future.as_mut().poll(&mut context).is_ready());
        assert!(!IN_KERNEL.get());

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let mut future = pin!(kernel(async { panic!("kernel panic") }));
            let _ = future.as_mut().poll(&mut context);
        }));
        assert!(result.is_err());
        assert!(!IN_KERNEL.get());
    }
}
