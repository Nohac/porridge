use super::*;
use crate::{Commands, declare::Anything};
use futures::{executor::block_on, channel::oneshot, future::{BoxFuture, Shared}};
use std::task::{Context, Poll};

struct Input;
struct Output;
struct Finished;
struct Gate(Shared<BoxFuture<'static, ()>>);
impl Component for Input {}
impl Component for Output {}
impl Component for Finished {}
impl Component for Gate {}

fn produce(_: Query<&Input>, mut commands: Commands<Anything>) { commands.insert((Output,)); }

async fn wait(rows: Query<&Gate>, mut commands: Commands<Anything>) {
    rows.item().0.clone().await;
    commands.insert((Finished,));
}

#[test]
fn quiet_pending_phase_does_not_capture_a_new_snapshot_and_recovers_if_cancelled() {
    for cancel in [false, true] {
        let (release, gate) = oneshot::channel();
        let bowl = Bowl::builder().system(produce).system(wait).build();
        block_on(async {
            bowl.insert((Input,)).await;
            bowl.insert((Gate(gate.map(|value| value.unwrap()).boxed().shared()),)).await;
        });
        let mut driver = Box::pin(bowl.stir());
        let waker = futures::task::noop_waker();
        let mut context = Context::from_waker(&waker);
        for _ in 0..16 {
            assert_eq!(driver.as_mut().poll(&mut context), Poll::Pending);
            let state = bowl.inner.state.try_lock().unwrap();
            if state.world.store_len::<Output>() == 1 { break; }
        }
        {
            let state = bowl.inner.state.try_lock().unwrap();
            assert_eq!(state.world.store_len::<Output>(), 1, "ready result must publish before the gate opens");
            let (_, cached) = state.snapshot_cache.as_ref().unwrap();
            assert!(cached.revision_raw() < state.world.revision_raw(),
                "quiet waiting must not pin the freshly published world");
        }
        if cancel {
            drop(driver);
            release.send(()).unwrap();
            block_on(bowl.stir());
        } else {
            release.send(()).unwrap();
            block_on(driver);
        }
        block_on(async {
            assert_eq!(bowl.scoop::<Query<&Output>>().await.len(), 1);
            assert_eq!(bowl.scoop::<Query<&Finished>>().await.len(), 1);
        });
    }
}

#[test]
fn absent_settlement_work_does_not_capture_snapshot_or_touch_pending_inputs() {
    block_on(async {
        let bowl = Bowl::builder().system(produce).build();
        bowl.insert((Input,)).await;
        let mut budget = CommitBudget::new(bowl.commit_limit());
        assert!(!bowl.run_settled_hooks(&mut budget).await);
        assert!(!bowl.run_settle_phase().await);
        {
            let state = bowl.inner.state.lock().await;
            assert!(state.snapshot_cache.is_none());
            assert!(state.pending_generation.is_some());
            assert!(state.settled_hooks.is_empty());
            assert!(state.settle_systems.is_empty());
        }
        bowl.stir().await;
        assert_eq!(bowl.scoop::<Query<&Output>>().await.len(), 1);
    });
}
