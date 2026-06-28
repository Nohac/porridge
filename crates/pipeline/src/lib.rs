#![allow(private_interfaces)]

use std::{
    any::{Any, TypeId},
    cell::RefCell,
    collections::{BTreeMap, HashMap, HashSet},
    hash::{DefaultHasher, Hash, Hasher},
    marker::PhantomData,
    rc::Rc,
};

pub use pipeline_macros::Component;

pub trait Component: 'static {
    fn fingerprint(&self) -> Option<u64> {
        None
    }
}

pub fn hash_component<T: Hash>(value: &T) -> u64 {
    let mut hasher = DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}

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
    fingerprint: Option<u64>,
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

impl<T: Component> StoreDyn for Store<T> {
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

    fn insert_base<T: Component>(&mut self, entity: Entity, value: T) {
        self.insert(entity, value, Origin::Base, None);
    }

    fn insert_derived<T: Component>(&mut self, entity: Entity, value: T, owner: SystemInvocation) {
        self.insert(entity, value, Origin::Derived, Some(owner));
    }

    fn insert<T: Component>(
        &mut self,
        entity: Entity,
        value: T,
        origin: Origin,
        owner: Option<SystemInvocation>,
    ) {
        let fingerprint = value.fingerprint();
        let old_revision = self
            .store::<T>()
            .and_then(|store| store.entries.get(&entity))
            .and_then(|entry| {
                (entry.fingerprint.is_some() && entry.fingerprint == fingerprint)
                    .then_some(entry.revision)
            });

        let revision = match old_revision {
            Some(revision) => revision,
            None => {
                bump(&mut self.revision);
                self.revision
            }
        };

        self.store_mut::<T>().entries.insert(
            entity,
            ComponentEntry {
                value,
                revision,
                fingerprint,
                origin,
                owner,
            },
        );
    }

    fn get<T: Component>(&self, entity: Entity) -> Option<&T> {
        self.store::<T>()?
            .entries
            .get(&entity)
            .map(|entry| &entry.value)
    }

    fn revision<T: Component>(&self, entity: Entity) -> Option<Revision> {
        self.store::<T>()?
            .entries
            .get(&entity)
            .map(|entry| entry.revision)
    }

    fn entities_with<T: Component>(&self) -> Vec<Entity> {
        self.store::<T>()
            .map(|store| store.entries.keys().copied().collect())
            .unwrap_or_default()
    }

    fn has<T: Component>(&self, entity: Entity) -> bool {
        self.store::<T>()
            .is_some_and(|store| store.entries.contains_key(&entity))
    }

    fn remove_derived_owned(&mut self, owner: &SystemInvocation) {
        for store in self.stores.values_mut() {
            store.remove_derived_owned(owner, &mut self.revision);
        }
    }

    fn store<T: Component>(&self) -> Option<&Store<T>> {
        self.stores
            .get(&TypeId::of::<T>())
            .and_then(|store| store.as_any().downcast_ref())
    }

    fn store_mut<T: Component>(&mut self) -> &mut Store<T> {
        self.stores
            .entry(TypeId::of::<T>())
            .or_insert_with(|| Box::<Store<T>>::default())
            .as_any_mut()
            .downcast_mut()
            .expect("component store has the wrong concrete type")
    }

    unsafe fn get_static<T: Component>(&self, entity: Entity) -> &'static T {
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
    fn insert_component<T: Component>(&mut self, entity: Entity, value: T) {
        self.inner
            .borrow_mut()
            .push(Box::new(InsertCommand { entity, value }));
    }

    pub fn insert<B: Bundle + 'static>(&mut self, bundle: B) {
        self.inner
            .borrow_mut()
            .push(Box::new(SpawnCommand { bundle }));
    }

    pub fn entity(&mut self, entity: Entity) -> EntityCommands<'_> {
        EntityCommands {
            commands: self,
            entity,
        }
    }
}

pub struct EntityCommands<'a> {
    commands: &'a mut Commands,
    entity: Entity,
}

impl EntityCommands<'_> {
    pub fn insert<T: Component>(&mut self, value: T) {
        self.commands.insert_component(self.entity, value);
    }
}

trait CommandOp {
    fn apply(self: Box<Self>, world: &mut World, owner: &SystemInvocation);
}

struct InsertCommand<T> {
    entity: Entity,
    value: T,
}

impl<T: Component> CommandOp for InsertCommand<T> {
    fn apply(self: Box<Self>, world: &mut World, owner: &SystemInvocation) {
        world.insert_derived(self.entity, self.value, owner.clone());
    }
}

struct SpawnCommand<B> {
    bundle: B,
}

impl<B: Bundle> CommandOp for SpawnCommand<B> {
    fn apply(self: Box<Self>, world: &mut World, owner: &SystemInvocation) {
        let entity = world.spawn_empty();
        self.bundle
            .insert_bundle(world, entity, Origin::Derived, Some(owner.clone()));
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

pub trait QueryParam: 'static {
    type State: Clone;

    fn rows(world: &World) -> Vec<Self::State>;
    fn keys(state: &Self::State) -> Vec<Entity>;
    fn deps(world: &World, state: &Self::State) -> Vec<Dep>;
    unsafe fn fetch(world: *const World, state: &Self::State) -> Self;
}

impl<T: Component> QueryParam for &'static T {
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

impl<T: Component> QueryParam for (Entity, &'static T) {
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

impl<A: Component, B: Component> QueryParam for (Entity, &'static A, &'static B) {
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

fn component_dep<T: Component>(world: &World, entity: Entity) -> Dep {
    Dep {
        type_id: TypeId::of::<T>(),
        entity,
        revision: world
            .revision::<T>(entity)
            .expect("query dependency referenced a missing component"),
    }
}

trait Runnable {
    fn run(&mut self, world: &mut World, memo: &mut HashMap<SystemInvocation, MemoEntry>) -> bool;
}

pub struct BoxedSystem(Box<dyn Runnable>);

pub trait IntoSystem<Marker>: 'static {
    fn into_system(self, id: SystemId) -> BoxedSystem;
}

pub struct CommandsFirstOne;
pub struct QueryFirstOne;
pub struct CommandsFirstTwo;
pub struct QueriesFirstTwo;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SystemHandle(SystemId);

pub struct SystemConfig<S> {
    system: S,
    after: Vec<SystemHandle>,
    on_complete: Vec<Box<dyn CompleteCallback>>,
}

impl<S> SystemConfig<S> {
    pub fn run_after(mut self, system: SystemHandle) -> Self {
        self.after.push(system);
        self
    }

    pub fn on_complete<C>(mut self, callback: C) -> Self
    where
        C: CompleteCallback,
    {
        self.on_complete.push(Box::new(callback));
        self
    }
}

pub trait SystemExt: Sized {
    fn run_after(self, system: SystemHandle) -> SystemConfig<Self> {
        SystemConfig {
            system: self,
            after: vec![system],
            on_complete: Vec::new(),
        }
    }

    fn on_complete<C>(self, callback: C) -> SystemConfig<Self>
    where
        C: CompleteCallback,
    {
        SystemConfig {
            system: self,
            after: Vec::new(),
            on_complete: vec![Box::new(callback)],
        }
    }
}

impl<S> SystemExt for S {}

pub trait CompleteCallback: 'static {
    fn run(&mut self, commands: Commands);
}

impl<F> CompleteCallback for F
where
    F: FnMut(Commands) + 'static,
{
    fn run(&mut self, commands: Commands) {
        self(commands);
    }
}

pub fn insert<B>(bundle: B) -> impl CompleteCallback
where
    B: Bundle + Clone + 'static,
{
    move |mut commands: Commands| {
        commands.insert(bundle.clone());
    }
}

pub trait IntoSystemConfig<Marker>: 'static {
    fn into_system_config(self, id: SystemId) -> (BoxedSystem, Vec<SystemHandle>);
}

impl<S, M> IntoSystemConfig<M> for S
where
    S: IntoSystem<M>,
{
    fn into_system_config(self, id: SystemId) -> (BoxedSystem, Vec<SystemHandle>) {
        (self.into_system(id), Vec::new())
    }
}

impl<S, M> IntoSystemConfig<(SystemConfigMarker, M)> for SystemConfig<S>
where
    S: IntoSystem<M>,
{
    fn into_system_config(self, id: SystemId) -> (BoxedSystem, Vec<SystemHandle>) {
        let system = self.system.into_system(id);
        let system = if self.on_complete.is_empty() {
            system
        } else {
            BoxedSystem(Box::new(CompleteSystem {
                id,
                inner: system,
                callbacks: self.on_complete,
            }))
        };

        (system, self.after)
    }
}

pub struct SystemConfigMarker;

struct CompleteSystem {
    id: SystemId,
    inner: BoxedSystem,
    callbacks: Vec<Box<dyn CompleteCallback>>,
}

impl Runnable for CompleteSystem {
    fn run(&mut self, world: &mut World, memo: &mut HashMap<SystemInvocation, MemoEntry>) -> bool {
        let ran = self.inner.0.run(world, memo);

        if ran {
            let owner = SystemInvocation {
                system: self.id,
                keys: Vec::new(),
            };
            let command_buffer = Rc::new(RefCell::new(Vec::new()));

            for callback in &mut self.callbacks {
                callback.run(Commands {
                    inner: Rc::clone(&command_buffer),
                });
            }

            world.remove_derived_owned(&owner);
            for command in command_buffer.take() {
                command.apply(world, &owner);
            }
        }

        ran
    }
}

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
    fn run(&mut self, world: &mut World, memo: &mut HashMap<SystemInvocation, MemoEntry>) -> bool {
        let rows = Q::rows(world);
        let mut seen = HashSet::new();
        let mut ran = false;

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
            ran = true;
        }

        remove_unseen_invocations(self.id, seen, world, memo);
        ran
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
    fn run(&mut self, world: &mut World, memo: &mut HashMap<SystemInvocation, MemoEntry>) -> bool {
        let rows = Q::rows(world);
        let mut seen = HashSet::new();
        let mut ran = false;

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
            ran = true;
        }

        remove_unseen_invocations(self.id, seen, world, memo);
        ran
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
    fn run(&mut self, world: &mut World, memo: &mut HashMap<SystemInvocation, MemoEntry>) -> bool {
        let rows_a = A::rows(world);
        let rows_b = B::rows(world);
        let mut seen = HashSet::new();
        let mut ran = false;

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
                ran = true;
            }
        }

        remove_unseen_invocations(self.id, seen, world, memo);
        ran
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
    fn run(&mut self, world: &mut World, memo: &mut HashMap<SystemInvocation, MemoEntry>) -> bool {
        let rows_a = A::rows(world);
        let rows_b = B::rows(world);
        let mut seen = HashSet::new();
        let mut ran = false;

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
                ran = true;
            }
        }

        remove_unseen_invocations(self.id, seen, world, memo);
        ran
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

struct SystemEntry {
    id: SystemId,
    after: Vec<SystemHandle>,
    system: BoxedSystem,
}

pub struct Db {
    world: World,
    systems: Vec<SystemEntry>,
    memo: HashMap<SystemInvocation, MemoEntry>,
    next_system: usize,
}

impl Default for Db {
    fn default() -> Self {
        Self::new()
    }
}

impl Db {
    pub fn new() -> Self {
        Self {
            world: World::new(),
            systems: Vec::new(),
            memo: HashMap::new(),
            next_system: 0,
        }
    }

    pub fn add_system<S, M>(&mut self, system: S) -> SystemHandle
    where
        S: IntoSystemConfig<M>,
    {
        let id = SystemId(self.next_system);
        self.next_system += 1;
        let (system, after) = system.into_system_config(id);
        let handle = SystemHandle(id);

        self.systems.push(SystemEntry { id, after, system });
        handle
    }

    pub fn query<Q: QueryParam>(&mut self) -> Vec<Q> {
        self.materialize();
        let rows = Q::rows(&self.world);
        let world = &self.world as *const World;
        rows.into_iter()
            .map(|row| unsafe { Q::fetch(world, &row) })
            .collect()
    }

    fn materialize(&mut self) {
        let order = self.system_order();

        loop {
            let mut made_progress = false;

            for index in order.iter().copied() {
                let system = &mut self.systems[index].system;
                made_progress |= system.0.run(&mut self.world, &mut self.memo);
            }

            if !made_progress {
                break;
            }
        }
    }

    fn system_order(&self) -> Vec<usize> {
        let mut by_id = HashMap::new();
        for (index, system) in self.systems.iter().enumerate() {
            by_id.insert(system.id, index);
        }

        let mut order = Vec::new();
        let mut visiting = HashSet::new();
        let mut done = HashSet::new();

        for index in 0..self.systems.len() {
            self.visit_system(index, &by_id, &mut visiting, &mut done, &mut order);
        }

        order
    }

    fn visit_system(
        &self,
        index: usize,
        by_id: &HashMap<SystemId, usize>,
        visiting: &mut HashSet<SystemId>,
        done: &mut HashSet<SystemId>,
        order: &mut Vec<usize>,
    ) {
        let id = self.systems[index].id;

        if done.contains(&id) {
            return;
        }

        if !visiting.insert(id) {
            panic!("system run_after cycle");
        }

        for dependency in &self.systems[index].after {
            let Some(dependency_index) = by_id.get(&dependency.0).copied() else {
                panic!("system depends on a handle that is not registered");
            };
            self.visit_system(dependency_index, by_id, visiting, done, order);
        }

        visiting.remove(&id);
        done.insert(id);
        order.push(index);
    }

    pub fn insert<B: Bundle>(&mut self, bundle: B) -> Entity {
        let entity = self.world.spawn_empty();
        bundle.insert_bundle(&mut self.world, entity, Origin::Base, None);
        entity
    }

    pub fn insert_component<T: Component>(&mut self, entity: Entity, component: T) {
        self.world.insert_base(entity, component);
    }

    pub fn entity(&mut self, entity: Entity) -> EntityMut<'_> {
        EntityMut { db: self, entity }
    }

    pub fn get<T: Component>(&mut self, entity: Entity) -> Option<&T> {
        self.materialize();
        self.world.get(entity)
    }

    pub fn peek<T: Component>(&self, entity: Entity) -> Option<&T> {
        self.world.get(entity)
    }
}

pub struct EntityMut<'a> {
    db: &'a mut Db,
    entity: Entity,
}

impl EntityMut<'_> {
    pub fn insert<T: Component>(&mut self, component: T) {
        self.db.insert_component(self.entity, component);
    }
}

pub trait Bundle {
    fn insert_bundle(
        self,
        world: &mut World,
        entity: Entity,
        origin: Origin,
        owner: Option<SystemInvocation>,
    );
}

impl<A: Component> Bundle for (A,) {
    fn insert_bundle(
        self,
        world: &mut World,
        entity: Entity,
        origin: Origin,
        owner: Option<SystemInvocation>,
    ) {
        world.insert(entity, self.0, origin, owner);
    }
}

impl<A: Component, B: Component> Bundle for (A, B) {
    fn insert_bundle(
        self,
        world: &mut World,
        entity: Entity,
        origin: Origin,
        owner: Option<SystemInvocation>,
    ) {
        world.insert(entity, self.0, origin, owner.clone());
        world.insert(entity, self.1, origin, owner);
    }
}

impl<A: Component, B: Component, C: Component> Bundle for (A, B, C) {
    fn insert_bundle(
        self,
        world: &mut World,
        entity: Entity,
        origin: Origin,
        owner: Option<SystemInvocation>,
    ) {
        world.insert(entity, self.0, origin, owner.clone());
        world.insert(entity, self.1, origin, owner.clone());
        world.insert(entity, self.2, origin, owner);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct A(u32);
    struct B(u32);
    struct C(u32);
    #[derive(Clone, Copy)]
    struct Done;
    #[derive(Hash)]
    struct HashA(u32);

    impl Component for A {}
    impl Component for B {}
    impl Component for C {}
    impl Component for Done {}

    impl Component for HashA {
        fn fingerprint(&self) -> Option<u64> {
            Some(hash_component(self))
        }
    }

    fn make_b(mut commands: Commands, Query((entity, a)): Query<(Entity, &A)>) {
        commands.entity(entity).insert(B(a.0 + 1));
    }

    fn make_c(mut commands: Commands, Query((entity, b)): Query<(Entity, &B)>) {
        commands.entity(entity).insert(C(b.0 + 1));
    }

    fn make_c_after_done(
        Query((entity, b)): Query<(Entity, &B)>,
        Query((_done, _)): Query<(Entity, &Done)>,
        mut commands: Commands,
    ) {
        commands.entity(entity).insert(C(b.0 + 1));
    }

    static HASH_A_RUNS: AtomicUsize = AtomicUsize::new(0);

    fn count_hash_a(mut commands: Commands, Query((entity, a)): Query<(Entity, &HashA)>) {
        let _ = a;
        HASH_A_RUNS.fetch_add(1, Ordering::SeqCst);
        commands.entity(entity).insert(B(1));
    }

    #[test]
    fn skips_valid_invocations_and_reruns_changed_entities() {
        let mut db = Db::new();
        db.add_system(make_b);

        let first = db.insert((A(1),));
        let second = db.insert((A(10),));

        let rows = db.query::<(Entity, &B)>();
        assert_eq!(rows.len(), 2);
        assert_eq!(db.get::<B>(first).unwrap().0, 2);
        assert_eq!(db.get::<B>(second).unwrap().0, 11);

        db.entity(first).insert(A(2));
        let rows = db.query::<(Entity, &B)>();
        assert_eq!(rows.len(), 2);

        assert_eq!(db.get::<B>(first).unwrap().0, 3);
        assert_eq!(db.get::<B>(second).unwrap().0, 11);
    }

    #[test]
    fn run_after_orders_systems_before_query_results_are_returned() {
        let mut db = Db::new();
        let make_b = db.add_system(make_b);
        db.add_system(make_c.run_after(make_b));

        let entity = db.insert((A(10),));

        let rows = db.query::<(Entity, &C)>();
        assert_eq!(rows.len(), 1);

        assert_eq!(db.get::<B>(entity).unwrap().0, 11);
        assert_eq!(db.get::<C>(entity).unwrap().0, 12);
    }

    #[test]
    fn on_complete_can_insert_a_marker_for_later_systems() {
        let mut db = Db::new();
        let make_b = db.add_system(make_b.on_complete(insert((Done,))));
        db.add_system(make_c_after_done.run_after(make_b));

        let entity = db.insert((A(10),));

        let rows = db.query::<(Entity, &C)>();
        assert_eq!(rows.len(), 1);
        assert_eq!(db.get::<B>(entity).unwrap().0, 11);
        assert_eq!(db.get::<C>(entity).unwrap().0, 12);
    }

    #[test]
    fn hashed_components_do_not_bump_revisions_when_the_hash_is_unchanged() {
        HASH_A_RUNS.store(0, Ordering::SeqCst);
        let mut db = Db::new();
        db.add_system(count_hash_a);

        let entity = db.insert((HashA(10),));

        db.query::<(Entity, &B)>();
        assert_eq!(HASH_A_RUNS.load(Ordering::SeqCst), 1);

        db.entity(entity).insert(HashA(10));
        db.query::<(Entity, &B)>();
        assert_eq!(HASH_A_RUNS.load(Ordering::SeqCst), 1);

        db.entity(entity).insert(HashA(11));
        db.query::<(Entity, &B)>();
        assert_eq!(HASH_A_RUNS.load(Ordering::SeqCst), 2);
    }
}
