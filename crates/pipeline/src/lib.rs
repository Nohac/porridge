#![allow(private_interfaces)]

mod filter;

use std::{
    any::{Any, TypeId},
    cell::RefCell,
    collections::{BTreeMap, HashMap, HashSet},
    hash::{DefaultHasher, Hash, Hasher},
    marker::PhantomData,
    ops::Deref,
    ptr::NonNull,
    rc::Rc,
};

pub use filter::{And, Eq, Gte, Not, Or, QueryBuilder, Where, With, Without};
pub use macros::Component;
use variadics_please::{all_tuples, all_tuples_enumerated};

pub trait Component: 'static {
    fn tracked() -> bool {
        true
    }

    fn fingerprint(&self) -> Option<u64> {
        None
    }
}

#[derive(Debug, Clone, Copy)]
pub struct Ephemeral;

impl Component for Ephemeral {
    fn tracked() -> bool {
        false
    }
}

pub struct Take<T>(PhantomData<T>);

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
    fn remove_derived_touched_by(
        &mut self,
        keys: &HashSet<Entity>,
        revision: &mut Revision,
    ) -> Vec<Entity>;
    fn remove_entity(&mut self, entity: Entity, revision: &mut Revision) -> Vec<SystemInvocation>;
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

        if T::tracked() && self.entries.len() != before {
            bump(revision);
        }
    }

    fn remove_derived_touched_by(
        &mut self,
        keys: &HashSet<Entity>,
        revision: &mut Revision,
    ) -> Vec<Entity> {
        let mut removed = Vec::new();

        self.entries.retain(|entity, entry| {
            let remove = entry.origin == Origin::Derived
                && entry
                    .owner
                    .as_ref()
                    .is_some_and(|owner| owner.keys.iter().any(|key| keys.contains(key)));

            if remove {
                removed.push(*entity);
            }

            !remove
        });

        if T::tracked() && !removed.is_empty() {
            bump(revision);
        }

        removed
    }

    fn remove_entity(&mut self, entity: Entity, revision: &mut Revision) -> Vec<SystemInvocation> {
        let Some(removed) = self.entries.remove(&entity) else {
            return Vec::new();
        };

        if T::tracked() {
            bump(revision);
        }

        if removed.origin == Origin::Derived {
            removed.owner.into_iter().collect()
        } else {
            Vec::new()
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
        let revision = if T::tracked() {
            let old_revision = self
                .store::<T>()
                .and_then(|store| store.entries.get(&entity))
                .and_then(|entry| {
                    (entry.fingerprint.is_some() && entry.fingerprint == fingerprint)
                        .then_some(entry.revision)
                });

            match old_revision {
                Some(revision) => revision,
                None => {
                    bump(&mut self.revision);
                    self.revision
                }
            }
        } else {
            self.revision
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

    fn remove_derived_touched_by(&mut self, keys: &HashSet<Entity>) -> Vec<Entity> {
        self.stores
            .values_mut()
            .flat_map(|store| store.remove_derived_touched_by(keys, &mut self.revision))
            .collect()
    }

    fn remove_entity(&mut self, entity: Entity) -> Vec<SystemInvocation> {
        let mut owners = Vec::new();

        for store in self.stores.values_mut() {
            owners.extend(store.remove_entity(entity, &mut self.revision));
        }

        owners
    }

    fn remove_component<T: Component>(&mut self, entity: Entity) -> Option<ComponentEntry<T>> {
        let removed = self.store_mut::<T>().entries.remove(&entity)?;

        if T::tracked() {
            bump(&mut self.revision);
        }

        Some(removed)
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
        // SAFETY: this prototype widens component references so `QueryParam`
        // can be implemented without threading lifetimes through every system
        // adapter. System bodies are safe because writes are buffered in
        // `Commands` and are not applied to `World` until after the fetched
        // references are no longer used. Materialized `Db::query` results rely
        // on the caller not mutating the database while holding returned refs;
        // a production API should encode that lifetime in the return type.
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

pub struct View<T> {
    rows: Vec<T>,
}

impl<T> View<T> {
    pub fn iter(&self) -> impl Iterator<Item = &T> {
        self.rows.iter()
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    pub fn len(&self) -> usize {
        self.rows.len()
    }
}

impl<T> View<(Entity, T)> {
    pub fn get(&self, entity: Entity) -> Option<&T> {
        self.rows
            .iter()
            .find_map(|(row_entity, value)| (*row_entity == entity).then_some(value))
    }
}

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
    type Item;

    fn rows(world: &World) -> Vec<Self::State>;
    fn rows_with(world: &World, _bindings: &filter::Bindings) -> Vec<Self::State> {
        Self::rows(world)
    }
    fn rows_for_entity(
        world: &World,
        entity: Entity,
        bindings: &filter::Bindings,
    ) -> Vec<Self::State> {
        Self::rows_with(world, bindings)
            .into_iter()
            .filter(|state| Self::keys(state).contains(&entity))
            .collect()
    }
    fn keys(state: &Self::State) -> Vec<Entity>;
    fn deps(world: &World, state: &Self::State) -> Vec<Dep>;
    unsafe fn fetch(world: *const World, state: &Self::State) -> Self::Item;
}

pub trait OwnedQueryParam: QueryParam {
    fn fetch_owned(db: &mut Db, state: &Self::State) -> Self::Item;
}

impl<T: Component> QueryParam for &'static T {
    type State = Entity;
    type Item = Self;

    fn rows(world: &World) -> Vec<Self::State> {
        world.entities_with::<T>()
    }

    fn keys(state: &Self::State) -> Vec<Entity> {
        vec![*state]
    }

    fn deps(world: &World, state: &Self::State) -> Vec<Dep> {
        component_dep_if_tracked::<T>(world, *state)
            .into_iter()
            .collect()
    }

    unsafe fn fetch(world: *const World, state: &Self::State) -> Self {
        // SAFETY: callers pass a raw pointer derived from a live `World`.
        // `state` was produced by `rows` for that same world before buffered
        // commands are applied, so the referenced component is still present.
        unsafe { (*world).get_static::<T>(*state) }
    }
}

impl<T> QueryParam for Take<T>
where
    T: Component + Clone,
{
    type State = Entity;
    type Item = T;

    fn rows(world: &World) -> Vec<Self::State> {
        world.entities_with::<T>()
    }

    fn keys(state: &Self::State) -> Vec<Entity> {
        vec![*state]
    }

    fn deps(world: &World, state: &Self::State) -> Vec<Dep> {
        component_dep_if_tracked::<T>(world, *state)
            .into_iter()
            .collect()
    }

    unsafe fn fetch(world: *const World, state: &Self::State) -> Self::Item {
        // SAFETY: callers pass a raw pointer derived from a live `World`.
        // This fallback clones the component value for non-consuming query
        // paths; inserted owned queries use `OwnedQueryParam::fetch_owned`.
        unsafe { (*world).get_static::<T>(*state).clone() }
    }
}

impl<T> OwnedQueryParam for Take<T>
where
    T: Component + Clone,
{
    fn fetch_owned(db: &mut Db, state: &Self::State) -> Self::Item {
        db.take_component::<T>(*state)
            .expect("owned query row referenced a missing component")
    }
}

impl<T> QueryParam for (Entity, Take<T>)
where
    T: Component + Clone,
{
    type State = Entity;
    type Item = (Entity, T);

    fn rows(world: &World) -> Vec<Self::State> {
        world.entities_with::<T>()
    }

    fn keys(state: &Self::State) -> Vec<Entity> {
        vec![*state]
    }

    fn deps(world: &World, state: &Self::State) -> Vec<Dep> {
        component_dep_if_tracked::<T>(world, *state)
            .into_iter()
            .collect()
    }

    unsafe fn fetch(world: *const World, state: &Self::State) -> Self::Item {
        // SAFETY: callers pass a raw pointer derived from a live `World`.
        // This fallback clones the component value for non-consuming query
        // paths; inserted owned queries use `OwnedQueryParam::fetch_owned`.
        unsafe { (*state, (*world).get_static::<T>(*state).clone()) }
    }
}

impl<T> OwnedQueryParam for (Entity, Take<T>)
where
    T: Component + Clone,
{
    fn fetch_owned(db: &mut Db, state: &Self::State) -> Self::Item {
        (
            *state,
            db.take_component::<T>(*state)
                .expect("owned query row referenced a missing component"),
        )
    }
}

macro_rules! impl_entity_query_param {
    ($($T:ident),*) => {
        impl<$($T: Component),*> QueryParam for (Entity, $(&'static $T,)*)
        {
            type State = Entity;
            type Item = Self;

            fn rows(world: &World) -> Vec<Self::State> {
                (0..world.next_entity)
                    .map(Entity)
                    .filter(|entity| true $(&& world.has::<$T>(*entity))*)
                    .collect()
            }

            fn keys(state: &Self::State) -> Vec<Entity> {
                vec![*state]
            }

            fn deps(world: &World, state: &Self::State) -> Vec<Dep> {
                let mut deps = Vec::new();
                $(deps.extend(component_dep_if_tracked::<$T>(world, *state));)*
                deps
            }

            unsafe fn fetch(world: *const World, state: &Self::State) -> Self {
                // SAFETY: callers pass a raw pointer derived from a live `World`.
                // `state` was produced by `rows` for that same world before
                // buffered commands are applied, so every component in the
                // same-entity join is still present.
                unsafe {
                    (
                        *state,
                        $((*world).get_static::<$T>(*state),)*
                    )
                }
            }
        }
    };
}

all_tuples!(impl_entity_query_param, 1, 16, T);

macro_rules! impl_filtered_entity_query_param {
    ($($T:ident),*) => {
        impl<F, $($T: Component),*> QueryParam for (Entity, $(&'static $T,)* Where<F>)
        where
            F: filter::FilterExpr,
        {
            type State = Entity;
            type Item = (Entity, $(&'static $T,)*);

            fn rows(world: &World) -> Vec<Self::State> {
                Self::rows_with(world, &filter::Bindings::default())
            }

            fn rows_with(world: &World, bindings: &filter::Bindings) -> Vec<Self::State> {
                (0..world.next_entity)
                    .map(Entity)
                    .filter(|entity| true $(&& world.has::<$T>(*entity))*)
                    .filter(|entity| F::matches(*entity, world, bindings))
                    .collect()
            }

            fn keys(state: &Self::State) -> Vec<Entity> {
                vec![*state]
            }

            fn deps(world: &World, state: &Self::State) -> Vec<Dep> {
                let mut deps = Vec::new();
                $(deps.extend(component_dep_if_tracked::<$T>(world, *state));)*
                deps.extend(F::deps(*state, world));
                deps
            }

            unsafe fn fetch(world: *const World, state: &Self::State) -> Self::Item {
                // SAFETY: callers pass a raw pointer derived from a live `World`.
                // `state` was produced by `rows_with` for that same world before
                // buffered commands are applied, so every projected component is
                // still present.
                unsafe {
                    (
                        *state,
                        $((*world).get_static::<$T>(*state),)*
                    )
                }
            }
        }
    };
}

all_tuples!(impl_filtered_entity_query_param, 1, 16, T);

fn component_dep<T: Component>(world: &World, entity: Entity) -> Dep {
    Dep {
        type_id: TypeId::of::<T>(),
        entity,
        revision: world
            .revision::<T>(entity)
            .expect("query dependency referenced a missing component"),
    }
}

fn component_dep_if_present<T: Component>(world: &World, entity: Entity) -> Option<Dep> {
    if !T::tracked() {
        return None;
    }

    world.revision::<T>(entity).map(|revision| Dep {
        type_id: TypeId::of::<T>(),
        entity,
        revision,
    })
}

fn component_dep_if_tracked<T: Component>(world: &World, entity: Entity) -> Option<Dep> {
    T::tracked().then(|| component_dep::<T>(world, entity))
}

trait Runnable {
    fn run(&mut self, world: &mut World, memo: &mut HashMap<SystemInvocation, MemoEntry>) -> bool;
}

pub struct BoxedSystem(Box<dyn Runnable>);

pub trait IntoSystem<Marker>: 'static {
    fn into_system(self, id: SystemId) -> BoxedSystem;
}

pub struct CommandsQueries;
pub struct QueriesCommands;
pub struct QueryViewsCommands;

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

pub fn insert_on<T>(entity: Entity, component: T) -> impl CompleteCallback
where
    T: Component + Clone,
{
    move |mut commands: Commands| {
        commands.entity(entity).insert(component.clone());
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

struct SystemCommandsQueries<F, Queries> {
    id: SystemId,
    function: F,
    _queries: PhantomData<Queries>,
}

struct SystemQueriesCommands<F, Queries> {
    id: SystemId,
    function: F,
    _queries: PhantomData<Queries>,
}

macro_rules! query_rows_empty {
    ($rows:ident, $(($n:tt, $Q:ident)),*) => {
        false $(|| $rows.$n.is_empty())*
    };
}

macro_rules! query_lengths {
    ($rows:ident, $(($n:tt, $Q:ident)),*) => {
        vec![$($rows.$n.len(),)*]
    };
}

macro_rules! query_keys {
    ($rows:ident, $indices:ident, $(($n:tt, $Q:ident)),*) => {{
        let mut keys = Vec::new();
        $(keys.extend($Q::keys(&$rows.$n[$indices[$n]]));)*
        keys
    }};
}

macro_rules! query_deps {
    ($world:ident, $rows:ident, $indices:ident, $(($n:tt, $Q:ident)),*) => {{
        let mut deps = Vec::new();
        $(deps.extend($Q::deps($world, &$rows.$n[$indices[$n]]));)*
        deps
    }};
}

fn advance_indices(indices: &mut [usize], lengths: &[usize]) -> bool {
    let mut position = indices.len();

    while position > 0 {
        position -= 1;
        indices[position] += 1;

        if indices[position] < lengths[position] {
            return true;
        }

        indices[position] = 0;
    }

    false
}

macro_rules! impl_query_driver_system {
    ($(($n:tt, $Q:ident)),*) => {
        impl<F, $($Q),*> Runnable for SystemCommandsQueries<F, ($($Q,)*)>
        where
            F: FnMut(Commands, $(Query<$Q>),*) + 'static,
            $($Q: QueryParam<Item = $Q>,)*
        {
            fn run(
                &mut self,
                world: &mut World,
                memo: &mut HashMap<SystemInvocation, MemoEntry>,
            ) -> bool {
                let rows = ($($Q::rows(world),)*);
                if query_rows_empty!(rows, $(($n, $Q)),*) {
                    remove_unseen_invocations(self.id, HashSet::new(), world, memo);
                    return false;
                }

                let lengths = query_lengths!(rows, $(($n, $Q)),*);
                let mut indices = vec![0; lengths.len()];
                let mut seen = HashSet::new();
                let mut ran = false;

                loop {
                    let owner = SystemInvocation {
                        system: self.id,
                        keys: query_keys!(rows, indices, $(($n, $Q)),*),
                    };
                    seen.insert(owner.clone());
                    let deps = query_deps!(world, rows, indices, $(($n, $Q)),*);

                    if !memo.get(&owner).is_some_and(|entry| entry.deps == deps) {
                        run_with_commands(world, memo, owner, deps, |world_ptr, commands| {
                            (self.function)(
                                commands,
                                // SAFETY: `world_ptr` points at the same world
                                // used to build `rows`, and command writes are
                                // buffered until the function returns.
                                $(unsafe { Query($Q::fetch(world_ptr, &rows.$n[indices[$n]])) }),*
                            );
                        });
                        ran = true;
                    }

                    if !advance_indices(&mut indices, &lengths) {
                        break;
                    }
                }

                remove_unseen_invocations(self.id, seen, world, memo);
                ran
            }
        }

        impl<F, $($Q),*> Runnable for SystemQueriesCommands<F, ($($Q,)*)>
        where
            F: FnMut($(Query<$Q>,)* Commands) + 'static,
            $($Q: QueryParam<Item = $Q>,)*
        {
            fn run(
                &mut self,
                world: &mut World,
                memo: &mut HashMap<SystemInvocation, MemoEntry>,
            ) -> bool {
                let rows = ($($Q::rows(world),)*);
                if query_rows_empty!(rows, $(($n, $Q)),*) {
                    remove_unseen_invocations(self.id, HashSet::new(), world, memo);
                    return false;
                }

                let lengths = query_lengths!(rows, $(($n, $Q)),*);
                let mut indices = vec![0; lengths.len()];
                let mut seen = HashSet::new();
                let mut ran = false;

                loop {
                    let owner = SystemInvocation {
                        system: self.id,
                        keys: query_keys!(rows, indices, $(($n, $Q)),*),
                    };
                    seen.insert(owner.clone());
                    let deps = query_deps!(world, rows, indices, $(($n, $Q)),*);

                    if !memo.get(&owner).is_some_and(|entry| entry.deps == deps) {
                        run_with_commands(world, memo, owner, deps, |world_ptr, commands| {
                            (self.function)(
                                // SAFETY: `world_ptr` points at the same world
                                // used to build `rows`, and command writes are
                                // buffered until the function returns.
                                $(unsafe { Query($Q::fetch(world_ptr, &rows.$n[indices[$n]])) },)*
                                commands
                            );
                        });
                        ran = true;
                    }

                    if !advance_indices(&mut indices, &lengths) {
                        break;
                    }
                }

                remove_unseen_invocations(self.id, seen, world, memo);
                ran
            }
        }

        impl<F, $($Q),*> IntoSystem<(CommandsQueries, ($($Q,)*))> for F
        where
            F: FnMut(Commands, $(Query<$Q>),*) + 'static,
            $($Q: QueryParam<Item = $Q>,)*
        {
            fn into_system(self, id: SystemId) -> BoxedSystem {
                BoxedSystem(Box::new(SystemCommandsQueries {
                    id,
                    function: self,
                    _queries: PhantomData,
                }))
            }
        }

        impl<F, $($Q),*> IntoSystem<(QueriesCommands, ($($Q,)*))> for F
        where
            F: FnMut($(Query<$Q>,)* Commands) + 'static,
            $($Q: QueryParam<Item = $Q>,)*
        {
            fn into_system(self, id: SystemId) -> BoxedSystem {
                BoxedSystem(Box::new(SystemQueriesCommands {
                    id,
                    function: self,
                    _queries: PhantomData,
                }))
            }
        }
    };
}

all_tuples_enumerated!(impl_query_driver_system, 1, 8, Q);

fn fetch_view<V: QueryParam<Item = V>>(world: *const World) -> View<V> {
    // SAFETY: `world` is a raw pointer to the live world for the current system
    // invocation. View rows are fetched before buffered commands are applied.
    let rows = unsafe { V::rows(&*world) };
    View {
        rows: rows
            .into_iter()
            // SAFETY: each row came from `V::rows` for this same world, and the
            // world is not mutated while constructing the view.
            .map(|row| unsafe { V::fetch(world, &row) })
            .collect(),
    }
}

struct SystemQueryViews<F, Q, Views> {
    id: SystemId,
    function: F,
    _params: PhantomData<(Q, Views)>,
}

macro_rules! impl_query_views_system {
    ($($V:ident),*) => {
        #[allow(non_snake_case)]
        impl<F, Q, $($V),*> Runnable for SystemQueryViews<F, Q, ($($V,)*)>
        where
            F: FnMut(Query<Q>, $(View<$V>,)* Commands) + 'static,
            Q: QueryParam<Item = Q>,
            $($V: QueryParam<Item = $V>,)*
        {
            fn run(
                &mut self,
                world: &mut World,
                memo: &mut HashMap<SystemInvocation, MemoEntry>,
            ) -> bool {
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
                        // SAFETY: `world_ptr` points at the same world used to
                        // build `row`, and command writes are buffered until
                        // the function returns.
                        let query = unsafe { Query(Q::fetch(world_ptr, &row)) };
                        $(let $V = fetch_view::<$V>(world_ptr);)*
                        (self.function)(query, $($V,)* commands);
                    });
                    ran = true;
                }

                remove_unseen_invocations(self.id, seen, world, memo);
                ran
            }
        }

        impl<F, Q, $($V),*> IntoSystem<(QueryViewsCommands, Q, ($($V,)*))> for F
        where
            F: FnMut(Query<Q>, $(View<$V>,)* Commands) + 'static,
            Q: QueryParam<Item = Q>,
            $($V: QueryParam<Item = $V>,)*
        {
            fn into_system(self, id: SystemId) -> BoxedSystem {
                BoxedSystem(Box::new(SystemQueryViews {
                    id,
                    function: self,
                    _params: PhantomData,
                }))
            }
        }
    };
}

all_tuples!(impl_query_views_system, 1, 8, V);

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

#[derive(Debug, Clone, Copy)]
pub struct InsertedEntity {
    db: NonNull<Db>,
    entity: Entity,
}

impl InsertedEntity {
    pub fn entity(self) -> Entity {
        self.entity
    }

    pub fn raw(self) -> u64 {
        self.entity.raw()
    }

    pub fn query<Q: QueryParam>(self) -> InsertedQueryBuilder<Q> {
        InsertedQueryBuilder {
            db: self.db,
            entity: self.entity,
            bindings: filter::Bindings::default(),
            _query: PhantomData,
        }
    }
}

impl From<InsertedEntity> for Entity {
    fn from(value: InsertedEntity) -> Self {
        value.entity
    }
}

impl Deref for InsertedEntity {
    type Target = Entity;

    fn deref(&self) -> &Self::Target {
        &self.entity
    }
}

pub struct InsertedQueryBuilder<Q> {
    db: NonNull<Db>,
    entity: Entity,
    bindings: filter::Bindings,
    _query: PhantomData<Q>,
}

impl<Q> InsertedQueryBuilder<Q>
where
    Q: QueryParam,
{
    pub fn bind<T: Component>(mut self, value: T) -> Self {
        self.bindings.insert(value);
        self
    }

    pub fn collect(self) -> InsertedQueryResult<Q::Item> {
        // SAFETY: `InsertedEntity` handles are created from a live `Db` by
        // `Db::insert`. This prototype stores the raw pointer to support the
        // `db.insert(...).query()` API without keeping an active Rust borrow
        // from the insertion site. Callers must not use a handle after moving
        // or dropping its source `Db`.
        let db = unsafe {
            self.db
                .as_ptr()
                .as_mut()
                .expect("inserted entity db is null")
        };

        db.materialize();
        let rows = Q::rows_for_entity(&db.world, self.entity, &self.bindings);
        let world = &db.world as *const World;
        let rows = rows
            .into_iter()
            // SAFETY: `rows` was produced from `db.world` immediately above,
            // and no mutation occurs before the values are fetched. Cleanup of
            // ephemeral data is delayed until the result guard is dropped.
            .map(|row| unsafe { Q::fetch(world, &row) })
            .collect();

        InsertedQueryResult {
            db: self.db,
            cleanup_ephemeral: db.has_ephemeral_entities(),
            rows,
        }
    }
}

impl<Q> InsertedQueryBuilder<Q>
where
    Q: OwnedQueryParam,
{
    pub fn collect_owned(self) -> Vec<Q::Item> {
        // SAFETY: `InsertedEntity` handles are created from a live `Db` by
        // `Db::insert`. This prototype stores the raw pointer to support the
        // `db.insert(...).query()` API without keeping an active Rust borrow
        // from the insertion site. Callers must not use a handle after moving
        // or dropping its source `Db`.
        let db = unsafe {
            self.db
                .as_ptr()
                .as_mut()
                .expect("inserted entity db is null")
        };

        db.materialize();
        let rows = Q::rows_for_entity(&db.world, self.entity, &self.bindings);
        let items = rows
            .into_iter()
            .map(|row| Q::fetch_owned(db, &row))
            .collect();

        if db.has_ephemeral_entities() {
            db.cleanup_ephemeral_entities();
        }

        items
    }

    pub fn one(self) -> Option<Q::Item> {
        let mut items = self.collect_owned();
        assert!(
            items.len() <= 1,
            "inserted entity query returned more than one row"
        );
        items.pop()
    }
}

pub struct InsertedQueryResult<T> {
    db: NonNull<Db>,
    cleanup_ephemeral: bool,
    rows: Vec<T>,
}

impl<T> InsertedQueryResult<T> {
    pub fn iter(&self) -> impl Iterator<Item = &T> {
        self.rows.iter()
    }

    pub fn len(&self) -> usize {
        self.rows.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }
}

impl<T> Deref for InsertedQueryResult<T> {
    type Target = [T];

    fn deref(&self) -> &Self::Target {
        &self.rows
    }
}

impl<'a, T> IntoIterator for &'a InsertedQueryResult<T> {
    type Item = &'a T;
    type IntoIter = std::slice::Iter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.rows.iter()
    }
}

impl<T> Drop for InsertedQueryResult<T> {
    fn drop(&mut self) {
        if !self.cleanup_ephemeral {
            return;
        }

        // SAFETY: see the safety note in `InsertedQueryBuilder::collect`.
        let db = unsafe {
            self.db
                .as_ptr()
                .as_mut()
                .expect("inserted entity db is null")
        };
        self.rows.clear();
        db.cleanup_ephemeral_entities();
    }
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

    pub fn query<Q: QueryParam>(&mut self) -> QueryBuilder<'_, Q> {
        QueryBuilder {
            db: self,
            bindings: filter::Bindings::default(),
            _query: PhantomData,
        }
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

    pub fn insert<B: Bundle>(&mut self, bundle: B) -> InsertedEntity {
        let entity = self.world.spawn_empty();
        bundle.insert_bundle(&mut self.world, entity, Origin::Base, None);
        InsertedEntity {
            db: NonNull::from(self),
            entity,
        }
    }

    pub fn insert_component<T: Component>(&mut self, entity: impl Into<Entity>, component: T) {
        self.world.insert_base(entity.into(), component);
    }

    pub fn entity(&mut self, entity: impl Into<Entity>) -> EntityMut<'_> {
        EntityMut {
            db: self,
            entity: entity.into(),
        }
    }

    pub fn get<T: Component>(&mut self, entity: impl Into<Entity>) -> Option<&T> {
        self.materialize();
        self.world.get(entity.into())
    }

    pub fn peek<T: Component>(&self, entity: impl Into<Entity>) -> Option<&T> {
        self.world.get(entity.into())
    }

    fn has_ephemeral_entities(&self) -> bool {
        !self.world.entities_with::<Ephemeral>().is_empty()
    }

    fn take_component<T: Component>(&mut self, entity: Entity) -> Option<T> {
        let entry = self.world.remove_component::<T>(entity)?;

        if let Some(owner) = entry.owner {
            self.world.remove_derived_owned(&owner);
            self.memo.remove(&owner);
        }

        Some(entry.value)
    }

    fn cleanup_ephemeral_entities(&mut self) {
        let mut frontier: HashSet<_> = self
            .world
            .entities_with::<Ephemeral>()
            .into_iter()
            .collect();
        let mut removed_entities = HashSet::new();

        while !frontier.is_empty() {
            self.remove_memo_touched_by(&frontier);

            let removed = self.world.remove_derived_touched_by(&frontier);
            let mut next_frontier = HashSet::new();

            for entity in removed {
                if removed_entities.insert(entity) {
                    next_frontier.insert(entity);
                }
            }

            frontier = next_frontier;
        }

        let ephemeral_entities: HashSet<_> = self
            .world
            .entities_with::<Ephemeral>()
            .into_iter()
            .collect();
        let mut removed_owners = Vec::new();
        for entity in &ephemeral_entities {
            removed_owners.extend(self.world.remove_entity(*entity));
        }
        self.remove_memo_touched_by(&ephemeral_entities);

        for owner in removed_owners {
            self.world.remove_derived_owned(&owner);
            self.memo.remove(&owner);
        }
    }

    fn remove_memo_touched_by(&mut self, keys: &HashSet<Entity>) {
        self.memo
            .retain(|owner, _| !owner.keys.iter().any(|key| keys.contains(key)));
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

macro_rules! impl_bundle {
    ($($T:ident),*) => {
        #[allow(non_snake_case)]
        impl<$($T: Component),*> Bundle for ($($T,)*) {
            fn insert_bundle(
                self,
                world: &mut World,
                entity: Entity,
                origin: Origin,
                owner: Option<SystemInvocation>,
            ) {
                let ($($T,)*) = self;
                $(world.insert(entity, $T, origin, owner.clone());)*
            }
        }
    };
}

all_tuples!(impl_bundle, 1, 16, B);

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct A(u32);
    struct B(u32);
    struct C(u32);
    struct Request(u32);
    struct Scratch(u32);
    #[derive(Clone)]
    struct Answer(u32);
    #[derive(Clone, Copy)]
    struct Done;
    #[derive(Hash)]
    struct HashA(u32);

    impl Component for A {}
    impl Component for B {}
    impl Component for C {}
    impl Component for Request {}
    impl Component for Scratch {}
    impl Component for Answer {}
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

    fn answer_request(Query((entity, request)): Query<(Entity, &Request)>, mut commands: Commands) {
        commands.entity(entity).insert(Answer(request.0 + 1));
    }

    fn spawn_ephemeral_scratch(
        Query((_entity, request)): Query<(Entity, &Request)>,
        mut commands: Commands,
    ) {
        commands.insert((Ephemeral, Scratch(request.0 + 1)));
    }

    fn answer_scratch(Query((entity, scratch)): Query<(Entity, &Scratch)>, mut commands: Commands) {
        commands.entity(entity).insert(Answer(scratch.0 + 1));
    }

    static VIEW_RUNS: AtomicUsize = AtomicUsize::new(0);

    fn count_with_ambient_view(
        Query((entity, a)): Query<(Entity, &A)>,
        bs: View<(Entity, &B)>,
        mut commands: Commands,
    ) {
        VIEW_RUNS.fetch_add(1, Ordering::SeqCst);
        commands.entity(entity).insert(C(a.0 + bs.len() as u32));
    }

    #[test]
    fn skips_valid_invocations_and_reruns_changed_entities() {
        let mut db = Db::new();
        db.add_system(make_b);

        let first = db.insert((A(1),));
        let second = db.insert((A(10),));

        let rows = db.query::<(Entity, &B)>().collect();
        assert_eq!(rows.len(), 2);
        assert_eq!(db.get::<B>(first).unwrap().0, 2);
        assert_eq!(db.get::<B>(second).unwrap().0, 11);

        db.entity(first).insert(A(2));
        let rows = db.query::<(Entity, &B)>().collect();
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

        let rows = db.query::<(Entity, &C)>().collect();
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

        let rows = db.query::<(Entity, &C)>().collect();
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

        db.query::<(Entity, &B)>().collect();
        assert_eq!(HASH_A_RUNS.load(Ordering::SeqCst), 1);

        db.entity(entity).insert(HashA(10));
        db.query::<(Entity, &B)>().collect();
        assert_eq!(HASH_A_RUNS.load(Ordering::SeqCst), 1);

        db.entity(entity).insert(HashA(11));
        db.query::<(Entity, &B)>().collect();
        assert_eq!(HASH_A_RUNS.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn view_does_not_invalidate_memoized_query_rows() {
        VIEW_RUNS.store(0, Ordering::SeqCst);
        let mut db = Db::new();
        db.add_system(count_with_ambient_view);

        let entity = db.insert((A(1),));
        db.query::<(Entity, &C)>().collect();

        assert_eq!(VIEW_RUNS.load(Ordering::SeqCst), 1);
        assert_eq!(db.get::<C>(entity).unwrap().0, 1);

        db.insert((B(10),));
        db.query::<(Entity, &C)>().collect();

        assert_eq!(VIEW_RUNS.load(Ordering::SeqCst), 1);
        assert_eq!(db.get::<C>(entity).unwrap().0, 1);
    }

    #[test]
    fn inserted_entity_query_can_clean_up_ephemeral_inputs_and_outputs() {
        let mut db = Db::new();
        db.add_system(answer_request);

        let answers = db
            .insert((Ephemeral, Request(41)))
            .query::<(Entity, &Answer)>()
            .collect();

        assert_eq!(answers.len(), 1);
        assert_eq!(answers[0].1.0, 42);

        drop(answers);

        assert!(db.query::<(Entity, &Request)>().collect().is_empty());
        assert!(db.query::<(Entity, &Answer)>().collect().is_empty());
    }

    #[test]
    fn inserted_entity_query_keeps_durable_inputs_and_outputs() {
        let mut db = Db::new();
        db.add_system(answer_request);

        let request = db.insert((Request(9),));
        let answers = request.query::<(Entity, &Answer)>().collect();

        assert_eq!(answers.len(), 1);
        assert_eq!(answers[0].1.0, 10);

        drop(answers);

        assert_eq!(db.get::<Request>(request).unwrap().0, 9);
        assert_eq!(db.get::<Answer>(request).unwrap().0, 10);
    }

    #[test]
    fn command_inserted_ephemeral_entities_are_cleaned_up_after_inserted_query() {
        let mut db = Db::new();
        db.add_system(spawn_ephemeral_scratch);
        db.add_system(answer_scratch);

        let request = db.insert((Request(40),));
        let requests = request.query::<(Entity, &Request)>().collect();

        assert_eq!(requests.len(), 1);

        drop(requests);

        assert_eq!(db.world.entities_with::<Request>().len(), 1);
        assert!(db.world.entities_with::<Scratch>().is_empty());
        assert!(db.world.entities_with::<Answer>().is_empty());
    }

    #[test]
    fn inserted_entity_query_can_take_owned_output_and_clean_up_immediately() {
        let mut db = Db::new();
        db.add_system(answer_request);

        let answer = db
            .insert((Ephemeral, Request(41)))
            .query::<Take<Answer>>()
            .one()
            .unwrap();

        assert_eq!(answer.0, 42);
        assert!(db.world.entities_with::<Request>().is_empty());
        assert!(db.world.entities_with::<Answer>().is_empty());
    }
}
