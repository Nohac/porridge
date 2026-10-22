use bump_scope::{Bump, BumpBox};
use std::ptr::NonNull;

use super::{Command, Queue, State};

#[derive(Clone, Copy)]
struct Entry {
    pointer: NonNull<u8>,
    consume: unsafe fn(NonNull<u8>, Option<&mut State>),
}

/// Owns the arena and all erased allocations; no payload references escape.
#[derive(Default)]
pub struct BumpQueue {
    arena: Bump,
    entries: Vec<Entry>,
    cursor: usize,
}

// SAFETY: only Send commands can enter. The allocator owns stable allocations;
// moving this queue transfers exclusive access, with no outstanding references.
unsafe impl Send for BumpQueue {}

impl BumpQueue {
    fn consume(&mut self, state: Option<&mut State>) {
        let entry = self.entries[self.cursor];
        // Mark consumed before user code: panic must never consume it twice.
        self.cursor += 1;
        // SAFETY: push pairs each aligned live allocation with its concrete
        // consumer. The cursor grants this consumer ownership exactly once.
        unsafe { (entry.consume)(entry.pointer, state) };
    }

    fn reset(&mut self) {
        debug_assert_eq!(self.cursor, self.entries.len());
        self.entries.clear();
        self.cursor = 0;
        // All values have been consumed, including their destructors.
        self.arena.reset();
    }
}

impl Queue for BumpQueue {
    fn push<C: Command>(&mut self, command: C) {
        unsafe fn consume<C: Command>(pointer: NonNull<u8>, state: Option<&mut State>) {
            // SAFETY: push allocated C with its natural alignment. The entry
            // is consumed once; read moves ownership out of the arena.
            let command = unsafe { pointer.cast::<C>().as_ptr().read() };
            if let Some(state) = state {
                command.apply(state);
            }
        }

        // Reserve metadata before transferring Drop responsibility away from
        // BumpBox. Nothing fallible is left after into_raw.
        self.entries.reserve(1);
        let pointer = BumpBox::into_raw(self.arena.alloc(command)).cast();
        self.entries.push(Entry {
            pointer,
            consume: consume::<C>,
        });
    }

    fn apply(&mut self, state: &mut State) {
        while self.cursor < self.entries.len() {
            self.consume(Some(state));
        }
        self.reset();
    }

    fn discard(&mut self) {
        while self.cursor < self.entries.len() {
            self.consume(None);
        }
        self.reset();
    }
}

impl Drop for BumpQueue {
    fn drop(&mut self) {
        self.discard();
    }
}
