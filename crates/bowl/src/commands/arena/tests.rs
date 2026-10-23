use super::*;
use crate::{Commands, Component, declare::Anything, world::SystemId};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering::SeqCst},
};

fn owner() -> SystemInvocation {
    SystemInvocation {
        system: SystemId(0),
        keys: Default::default(),
    }
}

struct Record {
    log: Arc<Mutex<Vec<u8>>>,
    drops: Arc<AtomicUsize>,
    value: u8,
    deferred: bool,
    panic: bool,
}

impl CommandOp for Record {
    fn apply(self, _: &mut World, _: &SystemInvocation) {
        assert!(!self.panic, "intentional command panic");
        self.log.lock().unwrap().push(self.value);
    }

    fn defers_at_settle(&self) -> bool {
        self.deferred
    }
}

impl Drop for Record {
    fn drop(&mut self) {
        self.drops.fetch_add(1, SeqCst);
    }
}

fn record(value: u8, log: &Arc<Mutex<Vec<u8>>>, drops: &Arc<AtomicUsize>) -> Record {
    Record {
        log: log.clone(),
        drops: drops.clone(),
        value,
        deferred: false,
        panic: false,
    }
}

#[repr(align(256))]
struct Aligned(Record);

impl CommandOp for Aligned {
    fn apply(self, world: &mut World, owner: &SystemInvocation) {
        assert_eq!((&self as *const Self as usize) % 256, 0);
        self.0.apply(world, owner);
    }
}

#[test]
fn selective_deferral_keeps_values_owned_after_source_recycling() {
    let mut pool = Pool::default();
    let log = Arc::new(Mutex::new(Vec::new()));
    let drops = Arc::new(AtomicUsize::new(0));
    let mut commands = pool.take();
    commands.push(record(1, &log, &drops));
    let mut insert = record(2, &log, &drops);
    insert.deferred = true;
    commands.push(insert);
    commands.push(Aligned(record(3, &log, &drops)));
    let source = &**commands.storage.as_ref().unwrap() as *const Storage;
    let mut deferred = pool.take();
    let mut world = World::new();
    commands.apply_or_defer(&mut world, Some(&owner()), Some(&mut deferred));
    assert_eq!(*log.lock().unwrap(), [1, 3]);
    assert_eq!(drops.load(SeqCst), 2);
    drop(commands);

    let mut recycled = pool.take();
    assert_eq!(
        &**recycled.storage.as_ref().unwrap() as *const Storage,
        source
    );
    for _ in 0..256 {
        recycled.push(Aligned(record(9, &log, &drops)));
    }
    // Cancellation/discard returns this grown buffer without applying commands.
    std::thread::spawn(move || drop(recycled)).join().unwrap();
    deferred.apply(&mut world, Some(&owner()));
    assert_eq!(*log.lock().unwrap(), [1, 3, 2]);
    assert_eq!(drops.load(SeqCst), 259);
}

#[test]
fn panic_discards_remaining_values_and_returns_clean_storage() {
    let mut pool = Pool::default();
    let log = Arc::new(Mutex::new(Vec::new()));
    let drops = Arc::new(AtomicUsize::new(0));
    let mut commands = pool.take();
    commands.push(record(1, &log, &drops));
    let mut panicking = record(2, &log, &drops);
    panicking.panic = true;
    commands.push(panicking);
    commands.push(record(3, &log, &drops));
    let failure = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        commands.apply(&mut World::new(), Some(&owner()));
    }));
    assert!(failure.is_err());
    assert!(
        commands.is_empty(),
        "unwind must not leave commands for a later apply"
    );
    assert_eq!(drops.load(SeqCst), 3);
    drop(commands);
    assert_eq!(drops.load(SeqCst), 3);
    let mut recycled = pool.take();
    assert!(recycled.storage.is_some());
    recycled.push(record(4, &log, &drops));
    recycled.apply(&mut World::new(), Some(&owner()));
    assert_eq!(*log.lock().unwrap(), [1, 4]);
    assert_eq!(drops.load(SeqCst), 4);
}

struct Value(u64);
impl Component for Value {}

#[test]
fn old_producer_clone_cannot_write_into_recycled_payload_storage() {
    let mut pool = Pool::default();
    let mut world = World::new();
    let entity = world.spawn_empty();
    let mut first =
        Commands::<Anything>::with_queue(Vec::new(), world.entity_allocator(), pool.take());
    let mut old_handle = first.clone();
    first.entity(entity).insert(Value(1));
    let mut output = first.take();
    output.apply(&mut world, Some(&owner()));
    drop(output);

    let mut next =
        Commands::<Anything>::with_queue(Vec::new(), world.entity_allocator(), pool.take());
    next.entity(entity).insert(Value(2));
    old_handle.entity(entity).insert(Value(999));
    next.take().apply(&mut world, Some(&owner()));
    assert_eq!(world.get::<Value>(entity).unwrap().0, 2);
    drop(old_handle);
    assert_eq!(world.get::<Value>(entity).unwrap().0, 2);
}

#[test]
fn base_append_preserves_order_and_remains_valid_after_source_drop() {
    let mut world = World::new();
    let entity = world.spawn_empty();
    let mut first = BaseCommands::default();
    first.push(super::super::InsertBaseCommand {
        entity,
        value: Value(1),
    });
    let mut second = BaseCommands::default();
    second.push(super::super::RemoveComponentBaseCommand::<Value> {
        entity,
        _marker: std::marker::PhantomData,
    });
    second.push(super::super::InsertBaseCommand {
        entity,
        value: Value(3),
    });
    first.append(&mut second);
    drop(second);
    first.apply(&mut world);
    assert_eq!(world.get::<Value>(entity).unwrap().0, 3);
}

#[test]
fn dropping_pool_before_an_outstanding_queue_is_safe() {
    let mut pool = Pool::default();
    let log = Arc::new(Mutex::new(Vec::new()));
    let drops = Arc::new(AtomicUsize::new(0));
    let mut queue = pool.take();
    queue.push(Aligned(record(1, &log, &drops)));
    drop(pool);
    std::thread::spawn(move || drop(queue)).join().unwrap();
    assert_eq!(drops.load(SeqCst), 1);
}

#[test]
fn zero_sized_payloads_drop_after_apply_and_discard() {
    static DROPS: AtomicUsize = AtomicUsize::new(0);
    struct Empty;
    impl CommandOp for Empty {
        fn apply(self, _: &mut World, _: &SystemInvocation) {}
    }
    impl Drop for Empty {
        fn drop(&mut self) {
            DROPS.fetch_add(1, SeqCst);
        }
    }
    let mut queue = CommandQueue::default();
    queue.push(Empty);
    queue.apply(&mut World::new(), Some(&owner()));
    queue.push(Empty);
    drop(queue);
    assert_eq!(DROPS.load(SeqCst), 2);
}

#[test]
fn retained_base_queue_discards_tail_before_reuse_after_panic() {
    struct BaseRecord(Record);
    impl BaseCommandOp for BaseRecord {
        fn apply(self, world: &mut World) {
            self.0.apply(world, &owner());
        }
    }

    let log = Arc::new(Mutex::new(Vec::new()));
    let drops = Arc::new(AtomicUsize::new(0));
    let mut pending = BaseCommands::default();
    let mut panicking = record(2, &log, &drops);
    panicking.panic = true;
    pending.push(BaseRecord(record(1, &log, &drops)));
    pending.push(BaseRecord(panicking));
    pending.push(BaseRecord(record(3, &log, &drops)));
    let failure = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        pending.apply(&mut World::new())
    }));
    assert!(failure.is_err());
    assert!(pending.0.is_empty());
    assert_eq!(drops.load(SeqCst), 3);
    pending.push(BaseRecord(record(4, &log, &drops)));
    pending.apply(&mut World::new());
    assert_eq!(*log.lock().unwrap(), [1, 4]);
}

#[test]
fn panicking_input_bundle_cannot_publish_partial_commands() {
    struct PanickingBundle;
    impl crate::Bundle for PanickingBundle {
        fn singleton_key() -> Option<std::any::TypeId> {
            None
        }

        fn queue(self, entity: crate::Entity, commands: &mut BaseCommands) {
            commands.push(super::super::InsertBaseCommand {
                entity,
                value: Value(99),
            });
            panic!("input staging failure");
        }

        fn insert_derived(self, _: &mut World, _: crate::Entity, _: SystemInvocation) {
            unreachable!();
        }
    }

    let bowl = crate::Bowl::builder().build();
    let entity = futures::executor::block_on(async { bowl.insert((Value(1),)).await.entity() });
    let failure = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        futures::executor::block_on(async {
            bowl.entity(entity).insert(PanickingBundle).await;
        });
    }));
    assert_eq!(
        failure.unwrap_err().downcast_ref::<&str>(),
        Some(&"input staging failure")
    );
    futures::executor::block_on(async {
        let values = bowl.scoop::<crate::Query<&Value>>().await;
        assert_eq!(values.collect()[0].0, 1);
    });
}

#[test]
fn guard_capacity_returns_without_retaining_tokens_or_aliasing_leases() {
    let mut pool = Pool::default();
    let log = Arc::new(Mutex::new(Vec::new()));
    let drops = Arc::new(AtomicUsize::new(0));
    let mut first = pool.take();
    let mut guards = first.take_guards();
    guards.push(Box::new(record(1, &log, &drops)));
    let capacity = guards.capacity();
    let mut other = pool.take();
    let mut held = other.take_guards();
    held.push(Box::new(record(2, &log, &drops)));

    first.recycle_guards(guards);
    assert_eq!(drops.load(SeqCst), 1);
    std::thread::spawn(move || drop(first)).join().unwrap();
    let mut recycled = pool.take();
    let mut guards = recycled.take_guards();
    assert_eq!(guards.capacity(), capacity);
    assert_eq!(drops.load(SeqCst), 1, "other lease must remain live");
    guards.push(Box::new(record(3, &log, &drops)));
    recycled.recycle_guards(guards);
    assert_eq!(drops.load(SeqCst), 2);
    drop(held);
    assert_eq!(drops.load(SeqCst), 3);
    drop(recycled);
    drop(other);
    assert_eq!(drops.load(SeqCst), 3);

    let mut empty = CommandQueue::default();
    empty.recycle_guards(GuardStore::new());
    assert!(empty.storage.is_none(), "empty buffers need no storage allocation");
    let mut guards = GuardStore::new();
    guards.push(Box::new(record(4, &log, &drops)));
    empty.recycle_guards(guards);
    assert_eq!(drops.load(SeqCst), 4);
    assert!(empty.storage.is_none(), "unpooled paths cannot reuse retained capacity");
}

#[test]
fn panic_and_cancellation_drop_live_guards_before_queue_reuse() {
    use futures::{FutureExt, task::noop_waker};
    use std::task::Context;
    let mut pool = Pool::default();
    let log = Arc::new(Mutex::new(Vec::new()));
    let drops = Arc::new(AtomicUsize::new(0));
    let mut queue = pool.take();
    let mut guards = queue.take_guards();
    guards.push(Box::new(record(1, &log, &drops)));
    let failure = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _guards = guards;
        panic!("invocation failed");
    }));
    assert!(failure.is_err());
    assert_eq!(drops.load(SeqCst), 1);

    let mut guards = queue.take_guards();
    guards.push(Box::new(record(2, &log, &drops)));
    let mut waiting = async move {
        let _guards = guards;
        futures::future::pending::<()>().await;
    }.boxed();
    let waker = noop_waker();
    assert!(waiting.as_mut().poll(&mut Context::from_waker(&waker)).is_pending());
    drop(waiting);
    assert_eq!(drops.load(SeqCst), 2);
    let mut guards = queue.take_guards();
    guards.push(Box::new(record(3, &log, &drops)));
    queue.recycle_guards(guards);
    drop(queue);
    assert_eq!(drops.load(SeqCst), 3);
    assert!(pool.take().take_guards().capacity() > 0);
}
