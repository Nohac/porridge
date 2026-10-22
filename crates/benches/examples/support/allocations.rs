//! Count allocator API activity only in isolated diagnostic intervals.
//! Requested bytes are not resident memory or live bytes: frees may refer to
//! allocations made before the interval, and realloc is reported separately.
use std::{
    alloc::{GlobalAlloc, Layout, System},
    sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed},
};

pub struct CountingAllocator;
static ENABLED: AtomicBool = AtomicBool::new(false);
static COUNTS: [AtomicU64; 7] = [const { AtomicU64::new(0) }; 7];
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
            COUNTS[0].fetch_add(1, Relaxed);
            COUNTS[1].fetch_add(layout.size() as u64, Relaxed);
        }
        ptr
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc_zeroed(layout) };
        if !ptr.is_null() && ENABLED.load(Relaxed) {
            COUNTS[0].fetch_add(1, Relaxed);
            COUNTS[1].fetch_add(layout.size() as u64, Relaxed);
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        if ENABLED.load(Relaxed) {
            COUNTS[2].fetch_add(1, Relaxed);
            COUNTS[3].fetch_add(layout.size() as u64, Relaxed);
        }
        unsafe { System.dealloc(ptr, layout) };
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        let result = unsafe { System.realloc(ptr, layout, size) };
        if !result.is_null() && ENABLED.load(Relaxed) {
            COUNTS[4].fetch_add(1, Relaxed);
            COUNTS[5].fetch_add(layout.size() as u64, Relaxed);
            COUNTS[6].fetch_add(size as u64, Relaxed);
        }
        result
    }
}
