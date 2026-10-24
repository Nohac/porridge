//! Log reclamation must not turn sparse work into population scans.
use super::*;
use crate::{Commands, SystemExt, Where, Eq as Equal, Without};
use crate::query::{Access, Dep, QueryFilter, QueryParam};
use futures::executor::block_on;
use std::{cell::Cell, sync::atomic::Ordering::Relaxed};

thread_local! { static EXAMINED: Cell<usize> = const { Cell::new(0) }; }

struct CountRows;
impl<Q: QueryParam> QueryFilter<Q> for CountRows {
    fn matches(_: &Snapshot, _: &Q::State) -> bool {
        EXAMINED.update(|n| n + 1);
        true
    }

    fn interest_types() -> Option<Vec<TypeId>> { Some(vec![]) }
    fn deps(_: &Snapshot, _: &Q::State) -> Vec<Dep> { vec![] }
    fn access(_: &Snapshot, _: &Q::State) -> Vec<Access> { vec![] }
}

struct Input(u64);
struct Output(u64);
struct Noise;
struct Bonus(u64);
struct Blocked;
struct Request;
struct Key(u64);
impl Component for Input {
    fn fingerprint(&self) -> Option<u64> { Some(self.0) }
}
impl Component for Noise {}
impl Component for Bonus {}
impl Component for Blocked {}
impl Component for Request {}
impl Component for Output {
    fn fingerprint(&self) -> Option<u64> { Some(self.0) }
}
impl Component for Key {
    fn fingerprint(&self) -> Option<u64> { Some(self.0) }
}

fn derive(rows: Query<(Entity, &Input), CountRows>, mut commands: Commands<(Output,)>) {
    let (id, value) = rows.item();
    commands.entity(id).insert(Output(value.0 * 2));
}

fn optional(rows: Query<(Entity, &Input, Option<&Bonus>), Without<Blocked>>, mut commands: Commands<(Output,)>) {
    let (id, input, bonus) = rows.item();
    commands.entity(id).insert(Output(input.0 + bonus.map_or(0, |value| value.0)));
}

fn join(requests: Query<(Entity, &Request, &Key)>, values: Query<&Input, Where<Equal<Key>>>, mut commands: Commands<(Output,)>) {
    let (id, _, _) = requests.item();
    commands.entity(id).insert(Output(values.item().0));
}

fn fill_and_compact(state: &mut State, noise: Entity) {
    for _ in 0..5000 { state.world.insert_base(noise, Noise); }
    let epoch = state.world.plan_log().1;
    compact_plan_log(state);
    assert_eq!(state.world.plan_log().1, epoch + 1);
    assert!(state.world.plan_log().0.is_empty());
}

async fn output(bowl: &Bowl, id: Entity) -> u64 {
    let rows = bowl.scoop::<Query<(Entity, &Output)>>().await;
    rows.collect().into_iter().find(|(entity, _)| *entity == id).unwrap().1.0
}

#[test]
fn busy_generations_keep_sparse_planning_and_explicit_reset_stays_full() {
    block_on(async {
        let bowl = Bowl::builder().system(derive).build();
        let mut ids = Vec::new();
        for value in 0..128 { ids.push(bowl.insert((Input(value),)).await.entity()); }
        let noise = bowl.insert((Noise,)).await.entity();
        bowl.stir().await;

        for round in 0..3 {
            for _ in 0..5000 { bowl.entity(noise).insert((Noise,)).await; }
            bowl.stir().await;
            EXAMINED.set(0);
            bowl.entity(ids[3]).insert((Input(1000 + round),)).await;
            bowl.stir().await;
            assert!(EXAMINED.get() < 8, "scanned {} rows for one changed input", EXAMINED.get());
            assert_eq!(output(&bowl, ids[3]).await, 2 * (1000 + round));
        }

        // Cancellation/preemption use reset_full; compaction must not turn it
        // into an apparently valid empty hint and skip the recovery pass.
        {
            let mut state = bowl.inner.state.lock().await;
            state.systems[0].reset_full();
            fill_and_compact(&mut state, noise);
            assert_eq!(state.systems[0].plan_epoch.load(Relaxed), u64::MAX);
        }
        EXAMINED.set(0);
        bowl.entity(noise).insert((Noise,)).await;
        bowl.stir().await;
        assert!(EXAMINED.get() >= 128);
    });
}

#[test]
fn unread_rows_survive_multiple_compactions_and_merge_with_retry_and_new_writes() {
    block_on(async {
        let bowl = Bowl::builder().system(derive).build();
        let mut ids = Vec::new();
        for value in 0..32 { ids.push(bowl.insert((Input(value),)).await.entity()); }
        let noise = bowl.insert((Noise,)).await.entity();
        bowl.stir().await;
        {
            let mut state = bowl.inner.state.lock().await;
            state.world.insert_base(ids[1], Input(50));
            state.systems[0].force_replan(&[ids[4], ids[4]]);
            fill_and_compact(&mut state, noise);
            state.world.insert_base(ids[2], Input(60));
            state.world.insert_base(ids[1], Input(70));
            fill_and_compact(&mut state, noise);
            assert_eq!(*state.systems[0].forced_dirty.lock().unwrap(), vec![ids[1], ids[2], ids[4]]);
        }
        EXAMINED.set(0);
        bowl.entity(ids[3]).insert((Input(80),)).await;
        bowl.stir().await;
        assert!(EXAMINED.get() < 12);
        assert_eq!(output(&bowl, ids[1]).await, 140);
        assert_eq!(output(&bowl, ids[2]).await, 120);
        assert_eq!(output(&bowl, ids[3]).await, 160);
        assert_eq!(output(&bowl, ids[4]).await, 8);
    });
}

#[test]
fn compacted_member_changes_still_translate_to_join_providers() {
    block_on(async {
        let bowl = Bowl::builder().system(join).build();
        let request = bowl.insert((Request, Key(7))).await.entity();
        let member = bowl.insert((Input(10), Key(7))).await.entity();
        let noise = bowl.insert((Noise,)).await.entity();
        assert_eq!(output(&bowl, request).await, 10);
        {
            let mut state = bowl.inner.state.lock().await;
            state.world.insert_base(member, Input(20));
            fill_and_compact(&mut state, noise);
            assert!(state.systems[0].forced_dirty.lock().unwrap().contains(&member));
        }
        bowl.entity(noise).insert((Noise,)).await;
        assert_eq!(output(&bowl, request).await, 20);
        // Ordinary retirement/replacement is unchanged after a cursor migration.
        bowl.entity(member).despawn().await;
        bowl.stir().await;
        assert!(bowl.scoop::<Query<&Output>>().await.is_empty());
        bowl.insert((Input(30), Key(7))).await;
        assert_eq!(output(&bowl, request).await, 30);
    });
}

#[test]
fn optional_and_negative_membership_changes_survive_compaction() {
    block_on(async {
        let bowl = Bowl::builder().system(optional).build();
        let plain = bowl.insert((Input(10),)).await.entity();
        let blocked = bowl.insert((Input(20), Blocked)).await.entity();
        let noise = bowl.insert((Noise,)).await.entity();
        assert_eq!(output(&bowl, plain).await, 10);
        {
            let mut state = bowl.inner.state.lock().await;
            state.world.insert_base(plain, Bonus(5));
            state.world.remove_component::<Blocked>(blocked, None);
            fill_and_compact(&mut state, noise);
        }
        bowl.entity(noise).insert((Noise,)).await;
        assert_eq!(output(&bowl, plain).await, 15);
        assert_eq!(output(&bowl, blocked).await, 20);
    });
}

#[test]
fn startup_does_not_retain_unconsumed_history_and_unrelated_system_stays_asleep() {
    block_on(async {
        let bowl = Bowl::builder().system(derive.run_during(Phase::Startup)).system(optional).build();
        let id = bowl.insert((Input(1),)).await.entity();
        let noise = bowl.insert((Noise,)).await.entity();
        bowl.stir().await;
        let mut state = bowl.inner.state.lock().await;
        let mark = state.systems[1].planned_mark.load(Relaxed);
        // Logged but fingerprint-equal: must not force an otherwise idle plan.
        state.world.insert_base(id, Input(1));
        fill_and_compact(&mut state, noise);
        assert!(state.systems[1].forced_dirty.lock().unwrap().is_empty());
        assert_eq!(state.systems[1].planned_mark.load(Relaxed), mark);
        assert!(!state.systems[1].peek_needs_planning(&state.world));
        state.world.insert_base(id, Input(2));
        fill_and_compact(&mut state, noise);
        assert!(state.systems[0].forced_dirty.lock().unwrap().is_empty());
        assert_ne!(state.systems[0].plan_epoch.load(Relaxed), state.world.plan_log().1);
        assert!(state.systems[1].forced_dirty.lock().unwrap().contains(&id));
    });
}
