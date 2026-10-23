use super::*;
use std::cell::Cell;

thread_local! {
    static ACCESS_CALLS: Cell<usize> = const { Cell::new(0) };
}

struct Probe<const ALWAYS: bool>;

impl<const ALWAYS: bool> SystemParam for Probe<ALWAYS> {
    type State = Entity;
    type Item<'a> = ();

    fn states(_: &Snapshot) -> Vec<Entity> {
        vec![Entity::from_raw(1), Entity::from_raw(2)]
    }

    fn keys(state: &Entity) -> Vec<Entity> {
        vec![*state]
    }

    fn deps(_: &Snapshot, _: &Entity) -> Vec<Dep> {
        Vec::new()
    }

    fn access(_: &Snapshot, _: &Entity) -> Vec<Access> {
        ACCESS_CALLS.update(|calls| calls + 1);
        Vec::new()
    }

    fn fetch(_: &Bowl, _: &Snapshot, _: &Entity, _: &Commands<Anything>, _: &mut GuardStore) {}

    fn always_run() -> bool {
        ALWAYS
    }
}

#[test]
fn memo_hits_do_not_construct_access_but_new_and_always_run_rows_do() {
    let snapshot = Snapshot::new();
    let id = SystemId(0);
    let owner = SystemInvocation {
        system: id,
        keys: vec![Entity::from_raw(1)].into(),
    };
    let memo = HashMap::from([(
        owner,
        MemoEntry {
            deps: Vec::new(),
            planned_revision: 0,
            settle_epoch: 0,
        },
    )]);

    ACCESS_CALLS.set(0);
    let planned = plan_invocations::<Probe<false>>(id, &snapshot, &memo);
    assert!(planned.completed);
    assert_eq!(planned.invocations.len(), 1);
    assert_eq!(planned.invocations[0].state.raw(), 2);
    assert_eq!(ACCESS_CALLS.get(), 1);

    ACCESS_CALLS.set(0);
    let planned = plan_invocations::<Probe<true>>(id, &snapshot, &memo);
    assert_eq!(planned.invocations.len(), 2);
    assert_eq!(ACCESS_CALLS.get(), 2);
}

struct Amount;
struct Flag;

impl crate::Component for Amount {}
impl crate::Component for Flag {}

#[test]
fn scratch_reset_preserves_admitted_metadata_and_memo_invalidation() {
    type Params = Query<(Entity, &'static Amount, Option<&'static Flag>)>;
    let mut world = Snapshot::new();
    let a = Entity::from_raw(10);
    let b = Entity::from_raw(20);
    world.insert_base(a, Amount);
    world.insert_base(b, Amount);
    world.insert_base(b, Flag);
    let id = SystemId(3);
    let mut scratch = Scratch::default();
    let prepared = Params::prepare(&world);
    let first = plan_prepared_with_scratch::<Params>(
        id,
        &world,
        &HashMap::new(),
        None,
        &prepared,
        &mut scratch,
    );
    assert_eq!(first.invocations.len(), 2);
    assert_eq!(scratch.arena.stats().allocated(), 0);
    let chunks = scratch.arena.stats().count();
    let keys_capacity = scratch.owner.keys.capacity();
    let memo: HashMap<_, _> = first
        .invocations
        .iter()
        .map(|row| {
            (
                row.owner.clone(),
                MemoEntry {
                    deps: row.deps.clone(),
                    planned_revision: world.revision_raw(),
                    settle_epoch: 1,
                },
            )
        })
        .collect();

    for _ in 0..4 {
        let cached =
            plan_prepared_with_scratch::<Params>(id, &world, &memo, None, &prepared, &mut scratch);
        assert!(cached.invocations.is_empty());
        // Deliberately overwrite scratch with a different invocation identity.
        let other = plan_prepared_with_scratch::<Params>(
            SystemId(4),
            &world,
            &memo,
            Some(&[b]),
            &prepared,
            &mut scratch,
        );
        assert_eq!(other.invocations[0].owner.keys.as_slice(), &[b]);
    }
    assert_eq!(scratch.arena.stats().allocated(), 0);
    assert_eq!(scratch.arena.stats().count(), chunks);
    assert_eq!(scratch.owner.keys.capacity(), keys_capacity);
    assert_eq!(first.invocations[0].owner.keys.as_slice(), &[a]);
    assert_eq!(
        first.invocations[0].access,
        [Access::read::<Amount>(a), Access::read::<Flag>(a),]
    );
    assert!(
        first.invocations[0]
            .deps
            .iter()
            .all(|dep| dep.is_current(&world))
    );

    // Absent -> present must still invalidate the original memo after reuse.
    world.insert_base(a, Flag);
    let prepared = Params::prepare(&world);
    let changed =
        plan_prepared_with_scratch::<Params>(id, &world, &memo, None, &prepared, &mut scratch);
    assert_eq!(changed.invocations.len(), 1);
    assert_eq!(changed.invocations[0].owner.keys.as_slice(), &[a]);
    assert!(!memo[&first.invocations[0].owner].is_current(&world));
}

struct PanicAccess;

impl<Q: QueryParam> QueryFilter<Q> for PanicAccess {
    fn matches(_: &Snapshot, _: &Q::State) -> bool {
        true
    }

    fn deps(_: &Snapshot, _: &Q::State) -> Vec<Dep> {
        Vec::new()
    }

    fn access(_: &Snapshot, _: &Q::State) -> Vec<Access> {
        panic!("legacy access must not be called");
    }

    fn access_into(_: &Snapshot, _: &Q::State, out: &mut impl Extend<Access>) {
        out.extend(std::iter::repeat_n(Access::read_all::<Amount>(), 128));
        panic!("injected planning failure");
    }
}

#[test]
fn failed_planning_releases_scratch_before_the_next_pass() {
    type Failing = Query<&'static Amount, PanicAccess>;
    type Working = Query<&'static Amount>;
    let mut world = Snapshot::new();
    world.insert_base(Entity::from_raw(1), Amount);
    let mut scratch = Scratch::default();
    let failure = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        plan_prepared_with_scratch::<Failing>(
            SystemId(0),
            &world,
            &HashMap::new(),
            None,
            &Failing::prepare(&world),
            &mut scratch,
        )
    }))
    .err()
    .expect("planning must fail");
    assert_eq!(
        failure.downcast_ref::<&str>(),
        Some(&"injected planning failure")
    );
    assert_eq!(scratch.arena.stats().allocated(), 0);
    let next = plan_prepared_with_scratch::<Working>(
        SystemId(1),
        &world,
        &HashMap::new(),
        None,
        &Working::prepare(&world),
        &mut scratch,
    );
    assert_eq!(next.invocations.len(), 1);
    assert_eq!(
        next.invocations[0].access,
        [Access::read::<Amount>(Entity::from_raw(1))]
    );
}
