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

    fn fetch(
        _: &Bowl,
        _: &Snapshot,
        _: &Entity,
        _: &Commands<Anything>,
        _: &mut GuardStore,
    ) {
    }

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
        keys: vec![Entity::from_raw(1)],
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
