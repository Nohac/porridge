//! Owned command payloads. The driver leases arenas; any thread can return them.
use bump_scope::{Bump, BumpBox};
use std::{
    ptr::NonNull,
    sync::mpsc::{self, Receiver, Sender},
};

use super::{BaseCommandOp, CommandOp};
use crate::world::{SystemInvocation, World};

trait Operation: Send + 'static {
    fn apply(self, world: &mut World, owner: Option<&SystemInvocation>);

    fn deferred(&self) -> bool {
        false
    }
}

struct Derived<C>(C);

impl<C: CommandOp> Operation for Derived<C> {
    fn apply(self, world: &mut World, owner: Option<&SystemInvocation>) {
        self.0
            .apply(world, owner.expect("derived command needs an invocation"));
    }

    fn deferred(&self) -> bool {
        self.0.defers_at_settle()
    }
}

struct Base<C>(C);

impl<C: BaseCommandOp> Operation for Base<C> {
    fn apply(self, world: &mut World, _: Option<&SystemInvocation>) {
        self.0.apply(world);
    }
}

enum Action<'a> {
    Apply(&'a mut World, Option<&'a SystemInvocation>),
    Move(&'a mut CommandQueue),
    Discard,
}

#[derive(Clone, Copy)]
struct Entry {
    pointer: NonNull<u8>,
    consume: unsafe fn(NonNull<u8>, Action<'_>),
    deferred: bool,
}

#[derive(Default)]
struct Storage {
    arena: Bump,
    entries: Vec<Entry>,
    cursor: usize,
}

// SAFETY: only Send operations enter; allocations are owned and no references
// escape. Ownership of a storage box moves with its queue or return message.
unsafe impl Send for Storage {}

impl Storage {
    fn consume(&mut self, action: Action<'_>) {
        let entry = self.entries[self.cursor];
        self.cursor += 1;
        // SAFETY: the cursor removes ownership before user code runs. Each
        // pointer is paired with its concrete consumer and natural alignment.
        unsafe { (entry.consume)(entry.pointer, action) };
    }

    fn clear(&mut self) {
        while self.cursor < self.entries.len() {
            self.consume(Action::Discard);
        }
        self.entries.clear();
        self.cursor = 0;
        self.arena.reset();
    }
}

impl Drop for Storage {
    fn drop(&mut self) {
        self.clear();
    }
}

/// A borrowed queue must also discard its tail on unwind: it may outlive the
/// failed apply (for example, the State's retained pending-input queue).
struct Drain<'a>(&'a mut Storage);

impl Drop for Drain<'_> {
    fn drop(&mut self) {
        self.0.clear();
    }
}

/// One reusable allocation per live batch, not one Box per command. Boxing the
/// reusable storage keeps queue handles small inside suspended future frames.
#[derive(Default)]
pub(crate) struct CommandQueue {
    storage: Option<Box<Storage>>,
    returns: Option<Sender<Box<Storage>>>,
}

impl CommandQueue {
    fn push_operation<C: Operation>(&mut self, value: C) {
        unsafe fn consume<C: Operation>(pointer: NonNull<u8>, action: Action<'_>) {
            // SAFETY: push pairs a live aligned C with this consumer exactly
            // once. read moves it out; all branches own its destruction.
            let value = unsafe { pointer.cast::<C>().as_ptr().read() };
            match action {
                Action::Apply(world, owner) => value.apply(world, owner),
                Action::Move(queue) => queue.push_operation(value),
                Action::Discard => drop(value),
            }
        }

        let storage = self.storage.get_or_insert_with(|| {
            crate::metrics::add(crate::metrics::Counter::CommandStorageCreated, 1);
            Box::default()
        });
        let trace = crate::metrics::enabled();
        let before_chunks = trace.then(|| storage.arena.stats().count());
        let before_capacity = storage.entries.capacity();
        let deferred = value.deferred();
        storage.entries.reserve(1);
        let pointer = BumpBox::into_raw(storage.arena.alloc(value)).cast();
        // No fallible work remains after transferring Drop ownership.
        storage.entries.push(Entry {
            pointer,
            consume: consume::<C>,
            deferred,
        });
        if let Some(before_chunks) = before_chunks {
            crate::metrics::add(
                crate::metrics::Counter::CommandArenaGrowth,
                (storage.arena.stats().count() - before_chunks) as u64,
            );
            crate::metrics::add(
                crate::metrics::Counter::CommandMetadataGrowth,
                u64::from(storage.entries.capacity() != before_capacity),
            );
        }
    }

    pub(crate) fn push<C: CommandOp>(&mut self, value: C) {
        self.push_operation(Derived(value));
    }

    pub(crate) fn len(&self) -> usize {
        self.storage
            .as_ref()
            .map_or(0, |s| s.entries.len() - s.cursor)
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub(crate) fn append(&mut self, other: &mut Self) {
        if let Some(storage) = &mut other.storage {
            let drain = Drain(storage);
            while drain.0.cursor < drain.0.entries.len() {
                drain.0.consume(Action::Move(self));
            }
        }
    }

    pub(crate) fn apply(&mut self, world: &mut World, owner: Option<&SystemInvocation>) {
        self.apply_or_defer(world, owner, None);
    }

    pub(crate) fn apply_or_defer(
        &mut self,
        world: &mut World,
        owner: Option<&SystemInvocation>,
        mut deferred: Option<&mut Self>,
    ) {
        if let Some(storage) = &mut self.storage {
            let drain = Drain(storage);
            while drain.0.cursor < drain.0.entries.len() {
                if drain.0.entries[drain.0.cursor].deferred
                    && let Some(target) = deferred.as_deref_mut()
                {
                    drain.0.consume(Action::Move(target));
                } else {
                    drain.0.consume(Action::Apply(world, owner));
                }
            }
        }
    }
}

impl Drop for CommandQueue {
    fn drop(&mut self) {
        if let Some(mut storage) = self.storage.take() {
            storage.clear();
            if let Some(returns) = &self.returns {
                // Disconnected means the Bowl was dropped; drop storage normally.
                let _ = returns.send(storage);
            }
        }
    }
}

/// Accessed by the existing evaluation/state owner, never shared with workers.
/// Returns from workers use a channel and are collected on the next checkout.
pub(crate) struct Pool {
    returns: Sender<Box<Storage>>,
    returned: Receiver<Box<Storage>>,
    #[expect(
        clippy::vec_box,
        reason = "reuse boxes to keep leased queues small inside future frames"
    )]
    free: Vec<Box<Storage>>,
}

impl Default for Pool {
    fn default() -> Self {
        let (returns, returned) = mpsc::channel();
        Self {
            returns,
            returned,
            free: Vec::new(),
        }
    }
}

impl Pool {
    pub(crate) fn take(&mut self) -> CommandQueue {
        self.free.extend(self.returned.try_iter());
        let storage = self.free.pop();
        crate::metrics::add(
            crate::metrics::Counter::CommandStorageReused,
            u64::from(storage.is_some()),
        );
        CommandQueue {
            storage,
            returns: Some(self.returns.clone()),
        }
    }
}

/// Hidden extension seam used by Bundle; normal callers pass component tuples.
#[doc(hidden)]
#[derive(Default)]
pub struct BaseCommands(pub(crate) CommandQueue);

impl BaseCommands {
    pub fn push<C: BaseCommandOp>(&mut self, command: C) {
        self.0.push_operation(Base(command));
    }

    pub(crate) fn append(&mut self, other: &mut Self) {
        self.0.append(&mut other.0);
    }

    pub(crate) fn apply(&mut self, world: &mut World) {
        self.0.apply(world, None);
    }
}

#[cfg(test)]
mod tests;
