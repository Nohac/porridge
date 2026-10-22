//! Bevy-style packed commands: metadata + payload in retained byte storage.
//! No borrowing of payloads, recursive execution, or world flushing API.
use std::mem::{MaybeUninit, size_of};

use super::{Command, Queue, State};

#[derive(Clone, Copy)]
struct Meta {
    size: usize,
    consume: unsafe fn(*mut u8, Option<&mut State>),
}

#[derive(Default)]
pub struct Packed {
    bytes: Vec<MaybeUninit<u8>>,
    cursor: usize,
}

// SAFETY: erased payloads are constrained to Send and are only accessed with
// exclusive queue access. No reference into the byte vector ever escapes.
unsafe impl Send for Packed {}

impl Packed {
    fn consume(&mut self, state: Option<&mut State>) {
        // SAFETY: cursor is at the next live metadata record. push reserves
        // and initializes every complete record before extending byte length.
        let meta = unsafe {
            self.bytes
                .as_ptr()
                .add(self.cursor)
                .cast::<Meta>()
                .read_unaligned()
        };
        let payload = self.cursor + size_of::<Meta>();
        self.cursor = payload + meta.size;
        // Advance before calling user code so panic cannot consume C twice.
        // SAFETY: metadata matches this live payload; consumer reads it by
        // value with read_unaligned, never constructing a misaligned reference.
        unsafe { (meta.consume)(self.bytes.as_mut_ptr().add(payload).cast(), state) };
    }

    fn reset(&mut self) {
        debug_assert_eq!(self.cursor, self.bytes.len());
        self.bytes.clear();
        self.cursor = 0;
    }
}

impl Queue for Packed {
    fn push<C: Command>(&mut self, command: C) {
        unsafe fn consume<C: Command>(pointer: *mut u8, state: Option<&mut State>) {
            // SAFETY: payload may be unaligned, but contains exactly one live
            // C. Reading by value moves it to an aligned local; cursor prevents
            // reuse. On discard or unwinding, the owned local drops normally.
            let command = unsafe { pointer.cast::<C>().read_unaligned() };
            if let Some(state) = state {
                command.apply(state);
            }
        }

        let length = size_of::<Meta>()
            .checked_add(size_of::<C>())
            .expect("command size overflow");
        self.bytes.reserve(length);
        let start = self.bytes.len();
        let meta = Meta {
            size: size_of::<C>(),
            consume: consume::<C>,
        };
        // SAFETY: reserve guarantees room for both objects, including ZST C.
        // MaybeUninit<u8> permits uninitialized padding. Unaligned writes do
        // not borrow C in place; no code after the move can panic before len
        // records the initialized command. Reallocation only relocates bytes.
        unsafe {
            let pointer = self.bytes.as_mut_ptr().add(start);
            pointer.cast::<Meta>().write_unaligned(meta);
            pointer
                .add(size_of::<Meta>())
                .cast::<C>()
                .write_unaligned(command);
            self.bytes.set_len(start + length);
        }
    }

    fn apply(&mut self, state: &mut State) {
        while self.cursor < self.bytes.len() {
            self.consume(Some(state));
        }
        self.reset();
    }

    fn discard(&mut self) {
        while self.cursor < self.bytes.len() {
            self.consume(None);
        }
        self.reset();
    }
}

impl Drop for Packed {
    fn drop(&mut self) {
        self.discard();
    }
}
