use crate::command_storage::*;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering::SeqCst},
};

struct Tracked {
    id: u64,
    drops: Arc<AtomicUsize>,
    panic: bool,
}

impl Command for Tracked {
    fn apply(self, state: &mut State) {
        assert!(!self.panic, "intentional command panic");
        state.value = state.value * 10 + self.id;
    }
}

impl Drop for Tracked {
    fn drop(&mut self) {
        self.drops.fetch_add(1, SeqCst);
    }
}

#[repr(align(256))]
struct Aligned(Tracked);

impl Command for Aligned {
    fn apply(self, state: &mut State) {
        assert_eq!((&self as *const Self as usize) % 256, 0);
        self.0.apply(state);
    }
}

fn check<Q: Queue>() {
    let mut queue = Q::default();
    let drops = Arc::new(AtomicUsize::new(0));
    for _ in 0..4 {
        let mut state = State::default();
        queue.push(Tracked {
            id: 1,
            drops: drops.clone(),
            panic: false,
        });
        queue.push(Aligned(Tracked {
            id: 2,
            drops: drops.clone(),
            panic: false,
        }));
        queue.push(Tracked {
            id: 3,
            drops: drops.clone(),
            panic: false,
        });
        queue.apply(&mut state);
        assert_eq!(state.value, 123);
    }
    assert_eq!(drops.load(SeqCst), 12);

    // Force multiple growth steps while retaining every payload's ownership.
    for _ in 0..1000 {
        queue.push(Aligned(Tracked {
            id: 0,
            drops: drops.clone(),
            panic: false,
        }));
    }
    queue.discard();
    assert_eq!(drops.load(SeqCst), 1012);
    queue.push(Add(7));
    let mut state = State::default();
    queue.apply(&mut state);
    assert_eq!(state.value, 7);

    // Cloned producer handles can release an owned batch to another worker.
    let shared = Arc::new(Mutex::new(queue));
    let writer = shared.clone();
    std::thread::spawn(move || writer.lock().unwrap().push(Multiply(3)))
        .join()
        .unwrap();
    let mut batch = std::mem::take(&mut *shared.lock().unwrap());
    drop(shared);
    let state = std::thread::spawn(move || {
        batch.apply(&mut state);
        state
    })
    .join()
    .unwrap();
    assert_eq!(state.value, 21);

    let mut queue = Q::default();
    for id in 1..=3 {
        queue.push(Tracked {
            id,
            drops: drops.clone(),
            panic: id == 2,
        });
    }
    let failure = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        queue.apply(&mut State::default())
    }));
    assert!(failure.is_err());
    drop(queue);
    assert_eq!(
        drops.load(SeqCst),
        1015,
        "applied, panicked, and abandoned commands each drop once"
    );
}

#[test]
fn ordered_owned_commands_survive_growth_transfer_and_panic() {
    check::<Boxed>();
    check::<BumpQueue>();
    check::<Packed>();
}

#[test]
fn zero_sized_commands_drop_on_apply_discard_and_queue_drop() {
    static DROPS: AtomicUsize = AtomicUsize::new(0);
    struct Empty;
    impl Command for Empty {
        fn apply(self, state: &mut State) {
            state.value += 1;
        }
    }
    impl Drop for Empty {
        fn drop(&mut self) {
            DROPS.fetch_add(1, SeqCst);
        }
    }

    fn check<Q: Queue>() {
        let before = DROPS.load(SeqCst);
        let mut queue = Q::default();
        queue.push(Empty);
        let mut state = State::default();
        queue.apply(&mut state);
        assert_eq!(state.value, 1);
        queue.push(Empty);
        queue.discard();
        queue.push(Empty);
        drop(queue);
        assert_eq!(DROPS.load(SeqCst) - before, 3);
    }

    check::<Boxed>();
    check::<BumpQueue>();
    check::<Packed>();
}
