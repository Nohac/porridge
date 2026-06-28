use std::{
    any::{Any, TypeId},
    cell::RefCell,
    collections::{BTreeMap, HashMap, HashSet},
    hash::Hash,
    marker::PhantomData,
    rc::Rc,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Entity(u64);

impl Entity {
    pub fn raw(self) -> u64 {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct Revision(u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SystemId(usize);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct SystemInvocation {
    system: SystemId,
    keys: Vec<Entity>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Origin {
    Base,
    Derived,
}

struct ComponentEntry<T> {
    value: T,
    revision: Revision,
    origin: Origin,
    owner: Option<SystemInvocation>,
}

trait StoreDyn {
    fn as_any(&self) -> &dyn Any;
    fn as_any_mut(&mut self) -> &mut dyn Any;
    fn remove_derived_owned(&mut self, owner: &SystemInvocation, revision: &mut Revision);
}

struct Store<T> {
    entries: BTreeMap<Entity, ComponentEntry<T>>,
}

impl<T> Default for Store<T> {
    fn default() -> Self {
        Self {
            entries: BTreeMap::new(),
        }
    }
}

impl<T: 'static> StoreDyn for Store<T> {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn remove_derived_owned(&mut self, owner: &SystemInvocation, revision: &mut Revision) {
        let before = self.entries.len();
        self.entries.retain(|_, entry| {
            entry.origin != Origin::Derived || entry.owner.as_ref() != Some(owner)
        });

        if self.entries.len() != before {
            bump(revision);
        }
    }
}

struct World {
    next_entity: u64,
    revision: Revision,
    stores: HashMap<TypeId, Box<dyn StoreDyn>>,
}

impl World {
    fn new() -> Self {
        Self {
            next_entity: 0,
            revision: Revision(0),
            stores: HashMap::new(),
        }
    }

    fn spawn_empty(&mut self) -> Entity {
        let entity = Entity(self.next_entity);
        self.next_entity += 1;
        entity
    }

    fn insert_base<T: 'static>(&mut self, entity: Entity, value: T) {
        self.insert(entity, value, Origin::Base, None);
    }

    fn insert_derived<T: 'static>(&mut self, entity: Entity, value: T, owner: SystemInvocation) {
        self.insert(entity, value, Origin::Derived, Some(owner));
    }

    fn insert<T: 'static>(
        &mut self,
        entity: Entity,
        value: T,
        origin: Origin,
        owner: Option<SystemInvocation>,
    ) {
        bump(&mut self.revision);
        let revision = self.revision;
        self.store_mut::<T>().entries.insert(
            entity,
            ComponentEntry {
                value,
                revision,
                origin,
                owner,
            },
        );
    }

    fn get<T: 'static>(&self, entity: Entity) -> Option<&T> {
        self.store::<T>()?
            .entries
            .get(&entity)
            .map(|entry| &entry.value)
    }

    fn revision<T: 'static>(&self, entity: Entity) -> Option<Revision> {
        self.store::<T>()?
            .entries
            .get(&entity)
            .map(|entry| entry.revision)
    }

    fn entities_with<T: 'static>(&self) -> Vec<Entity> {
        self.store::<T>()
            .map(|store| store.entries.keys().copied().collect())
            .unwrap_or_default()
    }

    fn has<T: 'static>(&self, entity: Entity) -> bool {
        self.store::<T>()
            .is_some_and(|store| store.entries.contains_key(&entity))
    }

    fn remove_derived_owned(&mut self, owner: &SystemInvocation) {
        for store in self.stores.values_mut() {
            store.remove_derived_owned(owner, &mut self.revision);
        }
    }

    fn store<T: 'static>(&self) -> Option<&Store<T>> {
        self.stores
            .get(&TypeId::of::<T>())
            .and_then(|store| store.as_any().downcast_ref())
    }

    fn store_mut<T: 'static>(&mut self) -> &mut Store<T> {
        self.stores
            .entry(TypeId::of::<T>())
            .or_insert_with(|| Box::<Store<T>>::default())
            .as_any_mut()
            .downcast_mut()
            .expect("component store has the wrong concrete type")
    }

    unsafe fn get_static<T: 'static>(&self, entity: Entity) -> &'static T {
        let value = self
            .get::<T>(entity)
            .expect("query row referenced a missing component");
        unsafe { std::mem::transmute::<&T, &'static T>(value) }
    }
}

fn bump(revision: &mut Revision) {
    revision.0 += 1;
}

#[derive(Clone)]
pub struct Commands {
    inner: Rc<RefCell<Vec<Box<dyn CommandOp>>>>,
}

impl Commands {
    pub fn insert<T: 'static>(&mut self, entity: Entity, value: T) {
        self.inner
            .borrow_mut()
            .push(Box::new(InsertCommand { entity, value }));
    }
}

trait CommandOp {
    fn apply(self: Box<Self>, world: &mut World, owner: &SystemInvocation);
}

struct InsertCommand<T> {
    entity: Entity,
    value: T,
}

impl<T: 'static> CommandOp for InsertCommand<T> {
    fn apply(self: Box<Self>, world: &mut World, owner: &SystemInvocation) {
        world.insert_derived(self.entity, self.value, owner.clone());
    }
}

pub struct Query<T>(pub T);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct Dep {
    type_id: TypeId,
    entity: Entity,
    revision: Revision,
}

#[derive(Debug, Clone)]
struct MemoEntry {
    deps: Vec<Dep>,
}

trait QueryParam: 'static {
    type State: Clone;

    fn rows(world: &World) -> Vec<Self::State>;
    fn keys(state: &Self::State) -> Vec<Entity>;
    fn deps(world: &World, state: &Self::State) -> Vec<Dep>;
    unsafe fn fetch(world: *const World, state: &Self::State) -> Self;
}

impl<T: 'static> QueryParam for &'static T {
    type State = Entity;

    fn rows(world: &World) -> Vec<Self::State> {
        world.entities_with::<T>()
    }

    fn keys(state: &Self::State) -> Vec<Entity> {
        vec![*state]
    }

    fn deps(world: &World, state: &Self::State) -> Vec<Dep> {
        vec![component_dep::<T>(world, *state)]
    }

    unsafe fn fetch(world: *const World, state: &Self::State) -> Self {
        unsafe { (*world).get_static::<T>(*state) }
    }
}

impl<T: 'static> QueryParam for (Entity, &'static T) {
    type State = Entity;

    fn rows(world: &World) -> Vec<Self::State> {
        world.entities_with::<T>()
    }

    fn keys(state: &Self::State) -> Vec<Entity> {
        vec![*state]
    }

    fn deps(world: &World, state: &Self::State) -> Vec<Dep> {
        vec![component_dep::<T>(world, *state)]
    }

    unsafe fn fetch(world: *const World, state: &Self::State) -> Self {
        unsafe { (*state, (*world).get_static::<T>(*state)) }
    }
}

impl<A: 'static, B: 'static> QueryParam for (Entity, &'static A, &'static B) {
    type State = Entity;

    fn rows(world: &World) -> Vec<Self::State> {
        world
            .entities_with::<A>()
            .into_iter()
            .filter(|entity| world.has::<B>(*entity))
            .collect()
    }

    fn keys(state: &Self::State) -> Vec<Entity> {
        vec![*state]
    }

    fn deps(world: &World, state: &Self::State) -> Vec<Dep> {
        vec![
            component_dep::<A>(world, *state),
            component_dep::<B>(world, *state),
        ]
    }

    unsafe fn fetch(world: *const World, state: &Self::State) -> Self {
        unsafe {
            (
                *state,
                (*world).get_static::<A>(*state),
                (*world).get_static::<B>(*state),
            )
        }
    }
}

fn component_dep<T: 'static>(world: &World, entity: Entity) -> Dep {
    Dep {
        type_id: TypeId::of::<T>(),
        entity,
        revision: world
            .revision::<T>(entity)
            .expect("query dependency referenced a missing component"),
    }
}

trait Runnable {
    fn run(&mut self, world: &mut World, memo: &mut HashMap<SystemInvocation, MemoEntry>);
}

pub struct BoxedSystem(Box<dyn Runnable>);

pub trait IntoSystem<Marker>: 'static {
    fn into_system(self, id: SystemId) -> BoxedSystem;
}

pub struct CommandsFirstOne;
pub struct QueryFirstOne;
pub struct CommandsFirstTwo;
pub struct QueriesFirstTwo;

struct SystemOne<F, Q> {
    id: SystemId,
    function: F,
    _query: PhantomData<Q>,
}

impl<F, Q> Runnable for SystemOne<F, Q>
where
    F: FnMut(Commands, Query<Q>) + 'static,
    Q: QueryParam,
{
    fn run(&mut self, world: &mut World, memo: &mut HashMap<SystemInvocation, MemoEntry>) {
        let rows = Q::rows(world);
        let mut seen = HashSet::new();

        for row in rows {
            let owner = SystemInvocation {
                system: self.id,
                keys: Q::keys(&row),
            };
            seen.insert(owner.clone());
            let deps = Q::deps(world, &row);

            if memo.get(&owner).is_some_and(|entry| entry.deps == deps) {
                continue;
            }

            run_with_commands(world, memo, owner, deps, |world_ptr, commands| {
                let query = unsafe { Query(Q::fetch(world_ptr, &row)) };
                (self.function)(commands, query);
            });
        }

        remove_unseen_invocations(self.id, seen, world, memo);
    }
}

struct SystemOneQueryFirst<F, Q> {
    id: SystemId,
    function: F,
    _query: PhantomData<Q>,
}

impl<F, Q> Runnable for SystemOneQueryFirst<F, Q>
where
    F: FnMut(Query<Q>, Commands) + 'static,
    Q: QueryParam,
{
    fn run(&mut self, world: &mut World, memo: &mut HashMap<SystemInvocation, MemoEntry>) {
        let rows = Q::rows(world);
        let mut seen = HashSet::new();

        for row in rows {
            let owner = SystemInvocation {
                system: self.id,
                keys: Q::keys(&row),
            };
            seen.insert(owner.clone());
            let deps = Q::deps(world, &row);

            if memo.get(&owner).is_some_and(|entry| entry.deps == deps) {
                continue;
            }

            run_with_commands(world, memo, owner, deps, |world_ptr, commands| {
                let query = unsafe { Query(Q::fetch(world_ptr, &row)) };
                (self.function)(query, commands);
            });
        }

        remove_unseen_invocations(self.id, seen, world, memo);
    }
}

struct SystemTwo<F, A, B> {
    id: SystemId,
    function: F,
    _queries: PhantomData<(A, B)>,
}

impl<F, A, B> Runnable for SystemTwo<F, A, B>
where
    F: FnMut(Commands, Query<A>, Query<B>) + 'static,
    A: QueryParam,
    B: QueryParam,
{
    fn run(&mut self, world: &mut World, memo: &mut HashMap<SystemInvocation, MemoEntry>) {
        let rows_a = A::rows(world);
        let rows_b = B::rows(world);
        let mut seen = HashSet::new();

        for row_a in &rows_a {
            for row_b in &rows_b {
                let owner = SystemInvocation {
                    system: self.id,
                    keys: [A::keys(row_a), B::keys(row_b)].concat(),
                };
                seen.insert(owner.clone());
                let deps = [A::deps(world, row_a), B::deps(world, row_b)].concat();

                if memo.get(&owner).is_some_and(|entry| entry.deps == deps) {
                    continue;
                }

                run_with_commands(world, memo, owner, deps, |world_ptr, commands| {
                    let query_a = unsafe { Query(A::fetch(world_ptr, row_a)) };
                    let query_b = unsafe { Query(B::fetch(world_ptr, row_b)) };
                    (self.function)(commands, query_a, query_b);
                });
            }
        }

        remove_unseen_invocations(self.id, seen, world, memo);
    }
}

struct SystemTwoQueriesFirst<F, A, B> {
    id: SystemId,
    function: F,
    _queries: PhantomData<(A, B)>,
}

impl<F, A, B> Runnable for SystemTwoQueriesFirst<F, A, B>
where
    F: FnMut(Query<A>, Query<B>, Commands) + 'static,
    A: QueryParam,
    B: QueryParam,
{
    fn run(&mut self, world: &mut World, memo: &mut HashMap<SystemInvocation, MemoEntry>) {
        let rows_a = A::rows(world);
        let rows_b = B::rows(world);
        let mut seen = HashSet::new();

        for row_a in &rows_a {
            for row_b in &rows_b {
                let owner = SystemInvocation {
                    system: self.id,
                    keys: [A::keys(row_a), B::keys(row_b)].concat(),
                };
                seen.insert(owner.clone());
                let deps = [A::deps(world, row_a), B::deps(world, row_b)].concat();

                if memo.get(&owner).is_some_and(|entry| entry.deps == deps) {
                    continue;
                }

                run_with_commands(world, memo, owner, deps, |world_ptr, commands| {
                    let query_a = unsafe { Query(A::fetch(world_ptr, row_a)) };
                    let query_b = unsafe { Query(B::fetch(world_ptr, row_b)) };
                    (self.function)(query_a, query_b, commands);
                });
            }
        }

        remove_unseen_invocations(self.id, seen, world, memo);
    }
}

impl<F, Q> IntoSystem<(CommandsFirstOne, Q)> for F
where
    F: FnMut(Commands, Query<Q>) + 'static,
    Q: QueryParam,
{
    fn into_system(self, id: SystemId) -> BoxedSystem {
        BoxedSystem(Box::new(SystemOne {
            id,
            function: self,
            _query: PhantomData,
        }))
    }
}

impl<F, Q> IntoSystem<(QueryFirstOne, Q)> for F
where
    F: FnMut(Query<Q>, Commands) + 'static,
    Q: QueryParam,
{
    fn into_system(self, id: SystemId) -> BoxedSystem {
        BoxedSystem(Box::new(SystemOneQueryFirst {
            id,
            function: self,
            _query: PhantomData,
        }))
    }
}

impl<F, A, B> IntoSystem<(CommandsFirstTwo, A, B)> for F
where
    F: FnMut(Commands, Query<A>, Query<B>) + 'static,
    A: QueryParam,
    B: QueryParam,
{
    fn into_system(self, id: SystemId) -> BoxedSystem {
        BoxedSystem(Box::new(SystemTwo {
            id,
            function: self,
            _queries: PhantomData,
        }))
    }
}

impl<F, A, B> IntoSystem<(QueriesFirstTwo, A, B)> for F
where
    F: FnMut(Query<A>, Query<B>, Commands) + 'static,
    A: QueryParam,
    B: QueryParam,
{
    fn into_system(self, id: SystemId) -> BoxedSystem {
        BoxedSystem(Box::new(SystemTwoQueriesFirst {
            id,
            function: self,
            _queries: PhantomData,
        }))
    }
}

fn run_with_commands(
    world: &mut World,
    memo: &mut HashMap<SystemInvocation, MemoEntry>,
    owner: SystemInvocation,
    deps: Vec<Dep>,
    run: impl FnOnce(*const World, Commands),
) {
    let command_buffer = Rc::new(RefCell::new(Vec::new()));
    let commands = Commands {
        inner: Rc::clone(&command_buffer),
    };

    run(world as *const World, commands);

    world.remove_derived_owned(&owner);
    for command in command_buffer.take() {
        command.apply(world, &owner);
    }
    memo.insert(owner, MemoEntry { deps });
}

fn remove_unseen_invocations(
    system: SystemId,
    seen: HashSet<SystemInvocation>,
    world: &mut World,
    memo: &mut HashMap<SystemInvocation, MemoEntry>,
) {
    let stale: Vec<_> = memo
        .keys()
        .filter(|owner| owner.system == system && !seen.contains(*owner))
        .cloned()
        .collect();

    for owner in stale {
        world.remove_derived_owned(&owner);
        memo.remove(&owner);
    }
}

pub struct Db<P> {
    world: World,
    pipelines: HashMap<P, Vec<BoxedSystem>>,
    memo: HashMap<SystemInvocation, MemoEntry>,
    next_system: usize,
}

impl<P> Default for Db<P>
where
    P: Copy + Eq + Hash + 'static,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<P> Db<P>
where
    P: Copy + Eq + Hash + 'static,
{
    pub fn new() -> Self {
        Self {
            world: World::new(),
            pipelines: HashMap::new(),
            memo: HashMap::new(),
            next_system: 0,
        }
    }

    pub fn add_system<S, M>(&mut self, pipeline: P, system: S)
    where
        S: IntoSystem<M>,
    {
        let id = SystemId(self.next_system);
        self.next_system += 1;
        self.pipelines
            .entry(pipeline)
            .or_default()
            .push(system.into_system(id));
    }

    pub fn run(&mut self, pipeline: P) {
        if let Some(systems) = self.pipelines.get_mut(&pipeline) {
            for system in systems {
                system.0.run(&mut self.world, &mut self.memo);
            }
        }
    }

    pub fn spawn<B: Bundle>(&mut self, bundle: B) -> Entity {
        let entity = self.world.spawn_empty();
        bundle.insert_bundle(self, entity);
        entity
    }

    pub fn insert<T: 'static>(&mut self, entity: Entity, component: T) {
        self.world.insert_base(entity, component);
    }

    pub fn get<T: 'static>(&self, entity: Entity) -> Option<&T> {
        self.world.get(entity)
    }
}

pub trait Bundle {
    fn insert_bundle<P>(self, db: &mut Db<P>, entity: Entity)
    where
        P: Copy + Eq + Hash + 'static;
}

impl<A: 'static> Bundle for (A,) {
    fn insert_bundle<P>(self, db: &mut Db<P>, entity: Entity)
    where
        P: Copy + Eq + Hash + 'static,
    {
        db.insert(entity, self.0);
    }
}

impl<A: 'static, B: 'static> Bundle for (A, B) {
    fn insert_bundle<P>(self, db: &mut Db<P>, entity: Entity)
    where
        P: Copy + Eq + Hash + 'static,
    {
        db.insert(entity, self.0);
        db.insert(entity, self.1);
    }
}

impl<A: 'static, B: 'static, C: 'static> Bundle for (A, B, C) {
    fn insert_bundle<P>(self, db: &mut Db<P>, entity: Entity)
    where
        P: Copy + Eq + Hash + 'static,
    {
        db.insert(entity, self.0);
        db.insert(entity, self.1);
        db.insert(entity, self.2);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    enum Pipe {
        Check,
    }

    struct A(u32);
    struct B(u32);

    fn make_b(mut commands: Commands, Query((entity, a)): Query<(Entity, &A)>) {
        commands.insert(entity, B(a.0 + 1));
    }

    #[test]
    fn skips_valid_invocations_and_reruns_changed_entities() {
        let mut db = Db::<Pipe>::new();
        db.add_system(Pipe::Check, make_b);

        let first = db.spawn((A(1),));
        let second = db.spawn((A(10),));

        db.run(Pipe::Check);
        assert_eq!(db.get::<B>(first).unwrap().0, 2);
        assert_eq!(db.get::<B>(second).unwrap().0, 11);

        db.insert(first, A(2));
        db.run(Pipe::Check);

        assert_eq!(db.get::<B>(first).unwrap().0, 3);
        assert_eq!(db.get::<B>(second).unwrap().0, 11);
    }
}
