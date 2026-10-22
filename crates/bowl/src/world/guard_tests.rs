use super::*;
use crate::query::{GuardStore, store_read_guard, store_write_guard};
use std::{
    future::Future,
    task::{Context, Poll},
};

struct Value(u64);

impl Component for Value {}

fn read(cell: &Arc<ComponentCell<Value>>) -> ComponentRef<'_, Value> {
    ComponentRef {
        guard: cell.read(),
        _marker: std::marker::PhantomData,
    }
}

fn write(cell: &Arc<ComponentCell<Value>>) -> ComponentMut<'_, Value> {
    ComponentMut {
        guard: cell.write(),
        _marker: std::marker::PhantomData,
    }
}

#[test]
fn erased_guards_survive_spill_move_and_cross_thread_drop() {
    let cells: Vec<_> = (0..24)
        .map(|i| Arc::new(ComponentCell::new(Value(i))))
        .collect();
    let mut guards = GuardStore::new();
    let mut readers = Vec::new();
    let mut writers = Vec::new();
    for (i, cell) in cells.iter().enumerate() {
        if i % 2 == 0 {
            // Two readers must each retain one reader slot.
            readers.push(store_read_guard(read(cell), &mut guards));
            readers.push(store_read_guard(read(cell), &mut guards));
        } else {
            writers.push(store_write_guard(write(cell), &mut guards));
        }
    }
    let moved = guards;
    for cell in &cells {
        assert!(cell.try_write().is_none());
    }
    for (i, value) in readers.iter().enumerate() {
        assert_eq!(value.0, (i / 2 * 2) as u64);
    }
    for value in &mut writers {
        value.0 += 100;
    }
    drop(readers);
    drop(writers);

    let custom_drops = Arc::new(AtomicU64::new(0));
    struct Custom(Arc<AtomicU64>);
    impl Drop for Custom {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    let mut moved = moved;
    moved.push(Box::new(Custom(custom_drops.clone())));
    std::thread::spawn(move || drop(moved)).join().unwrap();
    assert_eq!(custom_drops.load(Ordering::SeqCst), 1);
    for (i, cell) in cells.iter().enumerate() {
        let guard = cell.try_write().expect("every lock released exactly once");
        assert_eq!(guard.0, i as u64 + if i % 2 == 0 { 0 } else { 100 });
    }
}

#[test]
fn cancellation_and_unwind_release_erased_guards() {
    let cell = Arc::new(ComponentCell::new(Value(1)));
    let mut future = Box::pin(async {
        let mut guards = GuardStore::new();
        let value = store_write_guard(write(&cell), &mut guards);
        value.0 = 2;
        std::future::pending::<()>().await;
        drop(guards);
    });
    let mut context = Context::from_waker(futures::task::noop_waker_ref());
    assert!(matches!(future.as_mut().poll(&mut context), Poll::Pending));
    assert!(cell.try_write().is_none());
    drop(future);
    assert_eq!(cell.try_write().unwrap().0, 2);

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let mut guards = GuardStore::new();
        let _value = store_read_guard(read(&cell), &mut guards);
        panic!("kernel panic");
    }));
    assert!(result.is_err());
    assert!(cell.try_write().is_some());
}
