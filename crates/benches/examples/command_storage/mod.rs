//! Isolated command storage experiment, not engine publication code.
mod boxed;
mod bump;
mod packed;

pub use boxed::Boxed;
pub use bump::BumpQueue;
pub use packed::Packed;

#[derive(Default, Debug, PartialEq, Eq)]
pub struct State {
    pub value: u64,
}

pub trait Command: Send + 'static {
    fn apply(self, state: &mut State);
}

pub trait Queue: Default + Send + 'static {
    fn push<C: Command>(&mut self, command: C);

    fn apply(&mut self, state: &mut State);

    fn discard(&mut self);
}

pub struct Add(pub u64);

impl Command for Add {
    fn apply(self, state: &mut State) {
        state.value = state.value.wrapping_add(self.0);
    }
}

pub struct Multiply(pub u64);

impl Command for Multiply {
    fn apply(self, state: &mut State) {
        state.value = state.value.wrapping_mul(self.0);
    }
}

#[repr(align(64))]
struct Large {
    value: u64,
    bytes: [u8; 512],
}

impl Command for Large {
    fn apply(self, state: &mut State) {
        let payload = std::hint::black_box(self);
        state.value = state
            .value
            .wrapping_add(payload.value)
            .wrapping_mul(u64::from(payload.bytes[0]) + u64::from(payload.bytes[511]) + 1);
    }
}

pub fn round<Q: Queue>(queue: &mut Q, state: &mut State, count: usize, mixed: bool) {
    use std::hint::black_box;
    for index in 0..count {
        let value = black_box(index as u64 + 1);
        if mixed && index % 8 == 1 {
            queue.push(black_box(Large {
                value,
                bytes: [value as u8; 512],
            }));
        } else if index % 2 == 0 {
            queue.push(Add(value));
        } else {
            queue.push(Multiply(value));
        }
    }
    queue.apply(black_box(state));
}

pub fn counts() -> Vec<usize> {
    let value = std::env::var("COMMAND_COUNTS").unwrap_or_else(|_| "2,32,1024".into());
    value
        .split(',')
        .map(|part| {
            let count = part.trim().parse().expect("invalid COMMAND_COUNTS");
            assert!(count > 0, "command counts must be positive");
            count
        })
        .collect()
}
