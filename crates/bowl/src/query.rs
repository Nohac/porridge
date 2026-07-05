use std::{
    any::{Any, TypeId, type_name},
    cell::RefCell,
    collections::HashMap,
    marker::PhantomData,
};

use variadics_please::all_tuples;

use crate::{
    Bowl, Component, Entity,
    world::{ComponentMut, ComponentRef, Revision, Snapshot, World},
};

pub(crate) type GuardStore = Vec<Box<dyn Any + Send>>;

/// A tracked system input.
///
/// `Query<T, F>` is what makes a system row-addressable and memoizable. Each
/// row discovered by `T` and accepted by filter `F` contributes:
///
/// ```text
/// keys -> invocation identity
/// deps -> memoized input revisions
/// item -> values passed to the system
/// ```
///
/// Query items borrow from guard-backed component cells through normal Rust
/// lifetimes. The read guards backing those borrows are owned by the running
/// invocation and stay held until the system function returns, so consuming
/// the query with [`Query::item`] cannot release a row lock early.
pub struct Query<T, F = ()> {
    item: T,
    _filter: PhantomData<F>,
}

impl<T, F> Query<T, F> {
    pub(crate) fn new(item: T) -> Self {
        Self {
            item,
            _filter: PhantomData,
        }
    }

    /// Returns the row data selected by this query.
    pub fn item(self) -> T {
        self.item
    }

    /// Borrows the row data selected by this query.
    pub fn as_item(&self) -> &T {
        &self.item
    }
}

fn store_read_guard<'a, T: Component>(
    guard: ComponentRef<'a, T>,
    guards: &mut GuardStore,
) -> &'a T {
    let value = &*guard as *const T;

    // SAFETY: the returned reference points into the component protected by
    // `guard`. We erase the guard lifetime before storing it in the owning
    // guard store (the running invocation for system queries, the result value
    // for external queries) so the lock remains held at least as long as the
    // returned query item can be used. The guard store is dropped only after
    // the system function returns / the result is dropped, releasing the read
    // lock. The pointer does not point into the guard object itself, so moving
    // the boxed guard does not invalidate the reference.
    let guard =
        unsafe { std::mem::transmute::<ComponentRef<'a, T>, ComponentRef<'static, T>>(guard) };
    guards.push(Box::new(guard));

    // SAFETY: the read guard stored above keeps this component immutably locked
    // for the lifetime of the query item.
    unsafe { &*value }
}

fn store_write_guard<'a, T: Component>(
    mut guard: ComponentMut<'a, T>,
    guards: &mut GuardStore,
) -> &'a mut T {
    let value = &mut *guard as *mut T;

    // SAFETY: mirrors `store_read_guard`, but for the exclusive writer slot.
    // The write guard is stored in the invocation guard store, so the cell
    // stays exclusively locked until the system function returns; exactly one
    // `&mut T` is handed out per guard, and the planner never runs another
    // invocation touching this row concurrently.
    let guard =
        unsafe { std::mem::transmute::<ComponentMut<'a, T>, ComponentMut<'static, T>>(guard) };
    guards.push(Box::new(guard));

    // SAFETY: the write guard stored above keeps this component exclusively
    // locked for the lifetime of the query item.
    unsafe { &mut *value }
}

/// Exclusive in-place access to one component row inside a system.
///
/// A system declaring `Mut<T>` in its query owns the row for the whole
/// invocation: the scheduler runs no conflicting invocation concurrently, and
/// the cell's write lock is held until the system function returns. Mutate
/// through `Deref`/`DerefMut`; revision bookkeeping happens when the
/// invocation commits, and a value with an unchanged fingerprint keeps its
/// revision.
pub struct MutRef<'a, T> {
    entity: Entity,
    value: &'a mut T,
}

impl<T> MutRef<'_, T> {
    pub(crate) fn new(entity: Entity, value: &mut T) -> MutRef<'_, T> {
        MutRef { entity, value }
    }

    /// Entity this row belongs to.
    pub fn entity(&self) -> Entity {
        self.entity
    }
}

impl<T> std::ops::Deref for MutRef<'_, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.value
    }
}

impl<T> std::ops::DerefMut for MutRef<'_, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.value
    }
}

/// Assigns a type-level name to an external scoop query.
///
/// Named queries let one scoop request bind different runtime args of the same
/// component type:
///
/// ```text
/// struct Imports;
/// struct Diagnostics;
///
/// bowl.scoop::<(
///     Named<Imports, Query<(Entity, &Import), Where<Eq<FilePath>>>>,
///     Named<Diagnostics, Query<(Entity, &Diagnostic), Where<Eq<FilePath>>>>,
/// )>()
/// .args_for::<Imports>(FilePath("main.por"))
/// .args_for::<Diagnostics>(FilePath("lib.por"))
/// .await
/// ```
#[derive(Debug, Clone, Copy)]
pub struct Named<Tag, S>(PhantomData<fn() -> (Tag, S)>);

/// Marker query filter that requires `T` to be present without fetching it.
///
/// This is useful for components that act only as tags:
///
/// ```text
/// Query<(Entity, &FilePath), With<HoverRequest>>
/// ```
#[derive(Debug, Clone, Copy)]
pub struct With<T>(PhantomData<T>);

/// Query filter wrapper used by external bowl queries.
///
/// Unlike system-side [`Query<T, F>`] filters, `Where<F>` can use runtime
/// arguments supplied through `Bowl::scoop(...).args(...)`.
#[derive(Debug, Clone, Copy)]
pub struct Where<F>(PhantomData<F>);

/// Requires component `T` on the row entity to equal the bound query argument.
#[derive(Debug, Clone, Copy)]
pub struct Eq<T>(PhantomData<T>);

/// Requires component `T` on the row entity to be greater than or equal to the
/// bound query argument.
#[derive(Debug, Clone, Copy)]
pub struct Gte<T>(PhantomData<T>);

/// Boolean conjunction of two query filter expressions.
#[derive(Debug, Clone, Copy)]
pub struct And<A, B>(PhantomData<(A, B)>);

/// Boolean disjunction of two query filter expressions.
#[derive(Debug, Clone, Copy)]
pub struct Or<A, B>(PhantomData<(A, B)>);

/// Boolean negation of one query filter expression.
#[derive(Debug, Clone, Copy)]
pub struct Not<F>(PhantomData<F>);

/// Requires component `T` to be absent from the row entity.
#[derive(Debug, Clone, Copy)]
pub struct Without<T>(PhantomData<T>);

fn cow_rows_hinted<T: Component>(snapshot: &Snapshot, hint: Option<Vec<Entity>>) -> Vec<Entity> {
    match hint {
        Some(mut candidates) => {
            candidates.retain(|entity| snapshot.has::<T>(*entity));
            candidates
        }
        None => snapshot.entities_with::<T>(),
    }
}

/// Clone-on-write component projection for external update queries.
///
/// `Cow<T>` does not represent scheduler-level exclusive access. It is only
/// valid in `Bowl::scoop(...).for_each(...)`, where the current implementation
/// mutates the guarded live component cell while the live world is locked.
#[derive(Debug, Clone, Copy)]
pub struct Cow<T>(PhantomData<T>);

/// Scoped live mutable access to one component on one entity, obtained
/// through an external scoop.
///
/// Inside systems, use [`MutRef`] instead: the scheduler grants the
/// invocation exclusive row access, so systems mutate in place through a
/// plain mutable borrow rather than optimistic handles.
///
/// This type is intentionally not a conventional mutable reference. A
/// `Mut<T>` handle is inert until one of its mutation methods is called:
///
/// ```text
/// with_original
///   mutate only if the component still has the same revision observed when
///   this handle was scooped
///
/// with_latest
///   mutate whatever component value is currently attached to the entity
/// ```
///
/// Both methods expose live mutable access only inside a synchronous closure.
/// The closure cannot `.await`, which prevents the common accidental deadlock of
/// holding live mutable access while re-entering the bowl.
pub struct Mut<T> {
    bowl: Bowl,
    entity: Entity,
    revision: Option<Revision>,
    _marker: PhantomData<T>,
}

impl<T> Mut<T> {
    pub(crate) fn new(bowl: Bowl, entity: Entity, revision: Option<Revision>) -> Self {
        Self {
            bowl,
            entity,
            revision,
            _marker: PhantomData,
        }
    }

    /// Entity this access handle targets.
    pub fn entity(&self) -> Entity {
        self.entity
    }
}

impl<T> Mut<T>
where
    T: Component,
{
    /// Mutates the component only if it is still at the revision observed when
    /// this handle was created.
    ///
    /// Returns `None` if the component was removed or if another write changed
    /// its revision first. Use this when the mutation depends on facts observed
    /// by the scoop that produced the handle.
    pub async fn with_original<F, R>(&self, f: F) -> Option<R>
    where
        F: FnOnce(&mut T) -> R,
    {
        self.bowl
            .with_component_original::<T, F, R>(self.entity, self.revision, f)
            .await
    }

    /// Mutates the component currently attached to the entity.
    ///
    /// Returns `None` if the component was removed. Use this for operations
    /// that are valid against the latest value, such as setting an absolute
    /// value or appending to a current accumulator.
    pub async fn with_latest<F, R>(&self, f: F) -> Option<R>
    where
        F: FnOnce(&mut T) -> R,
    {
        self.bowl
            .with_component_mut::<T, F, R>(self.entity, f)
            .await
    }
}

/// Scheduler-visible access to one component row, or to a whole component
/// store when `entity` is `None`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[doc(hidden)]
pub struct Access {
    pub(crate) kind: AccessKind,
    pub(crate) component: TypeId,
    pub(crate) entity: Option<Entity>,
}

impl Access {
    pub(crate) fn read<T: Component>(entity: Entity) -> Self {
        Self {
            kind: AccessKind::Read,
            component: TypeId::of::<T>(),
            entity: Some(entity),
        }
    }

    pub(crate) fn write<T: Component>(entity: Entity) -> Self {
        Self {
            kind: AccessKind::Write,
            component: TypeId::of::<T>(),
            entity: Some(entity),
        }
    }

    /// Shared access to every row of `T`, used by ambient `View` params.
    pub(crate) fn read_all<T: Component>() -> Self {
        Self {
            kind: AccessKind::Read,
            component: TypeId::of::<T>(),
            entity: None,
        }
    }

    /// Exclusive access to every row of `T`.
    pub(crate) fn write_all<T: Component>() -> Self {
        Self {
            kind: AccessKind::Write,
            component: TypeId::of::<T>(),
            entity: None,
        }
    }

    pub(crate) fn conflicts(self, other: Self) -> bool {
        self.component == other.component
            && (self.kind == AccessKind::Write || other.kind == AccessKind::Write)
            && match (self.entity, other.entity) {
                (Some(own), Some(other)) => own == other,
                _ => true,
            }
    }
}

/// Shared or exclusive access kind for one component row.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[doc(hidden)]
pub enum AccessKind {
    Read,
    Write,
}

/// Runtime values bound to external query filters.
#[derive(Default)]
pub struct QueryArgs {
    shared: HashMap<TypeId, Box<dyn Any + Send + Sync>>,
    scoped: HashMap<(TypeId, TypeId), Box<dyn Any + Send + Sync>>,
}

impl QueryArgs {
    pub(crate) fn insert<T: Component>(&mut self, scope: Option<TypeId>, value: T) {
        let component = TypeId::of::<T>();
        let previous = match scope {
            Some(scope) => self.scoped.insert((scope, component), Box::new(value)),
            None => self.shared.insert(component, Box::new(value)),
        };

        if previous.is_some() {
            panic!("duplicate query argument {}", type_name::<T>());
        }
    }

    fn get<T: Component>(&self, scope: Option<TypeId>) -> &T {
        let component = TypeId::of::<T>();
        let value = scope
            .and_then(|scope| self.scoped.get(&(scope, component)))
            .or_else(|| self.shared.get(&component))
            .unwrap_or_else(|| panic!("missing query argument {}", type_name::<T>()));

        value
            .downcast_ref()
            .expect("query argument stored with wrong type")
    }
}

/// One or more runtime arguments for external `Where` filters.
pub trait ArgBundle: Send + 'static {
    #[doc(hidden)]
    fn insert_into(self, args: &mut QueryArgs, scope: Option<TypeId>);
}

impl<T> ArgBundle for T
where
    T: Component,
{
    fn insert_into(self, args: &mut QueryArgs, scope: Option<TypeId>) {
        args.insert(scope, self);
    }
}

macro_rules! impl_arg_bundle_tuple {
    ($($T:ident),*) => {
        impl<$($T: ArgBundle),*> ArgBundle for ($($T,)*)
        {
            #[allow(non_snake_case)]
            fn insert_into(self, args: &mut QueryArgs, scope: Option<TypeId>) {
                let ($($T,)*) = self;
                $($T.insert_into(args, scope);)*
            }
        }
    };
}

all_tuples!(impl_arg_bundle_tuple, 1, 8, T);

/// Ambient read-only snapshot access.
///
/// A `View<T>` is built from the same structural snapshot as the driving
/// [`Query`], but it is intentionally not part of the invocation memo key.
///
/// ```text
/// Query<T, F = ()>
///   tracked dependency
///
/// View<T, F = ()>
///   current snapshot context
///   no automatic invalidation
/// ```
///
/// This is useful for checks that need to inspect surrounding facts but should
/// only rerun when their driving row changes.
pub struct View<'a, T, F = ()>
where
    T: QueryParam,
    F: QueryFilter<T>,
{
    bowl: Bowl,
    snapshot: &'a Snapshot,
    rows: Vec<<T as QueryParam>::State>,
    guards: RefCell<GuardStore>,
    _marker: PhantomData<(T, F)>,
}

// View values are also invocation-local parameter wrappers. The guard store is
// only used by the running system that owns the view.
unsafe impl<T, F> Send for View<'_, T, F>
where
    T: QueryParam + Send,
    F: QueryFilter<T> + Send,
{
}

unsafe impl<T, F> Sync for View<'_, T, F>
where
    T: QueryParam + Sync,
    F: QueryFilter<T> + Sync,
{
}

impl<'a, T, F> View<'a, T, F>
where
    T: QueryParam,
    F: QueryFilter<T>,
{
    /// Builds an ambient view over `snapshot`.
    ///
    /// This records row states eagerly, then fetches borrowed items lazily while
    /// iterating. The snapshot is immutable for the whole system invocation, so
    /// rows cannot disappear between enumeration and fetch.
    pub(crate) fn new(bowl: Bowl, snapshot: &'a Snapshot) -> Self {
        Self {
            bowl,
            snapshot,
            rows: filtered_rows::<T, F>(snapshot),
            guards: RefCell::new(Vec::new()),
            _marker: PhantomData,
        }
    }

    /// Iterates rows visible in this snapshot.
    pub fn iter(&'a self) -> impl Iterator<Item = T::Item<'a>> + 'a {
        self.rows.iter().map(|row| {
            let mut guards = self.guards.borrow_mut();
            T::fetch(&self.bowl, self.snapshot, row, &mut guards)
        })
    }

    /// Number of rows in the view.
    pub fn len(&self) -> usize {
        self.rows.len()
    }

    /// Returns `true` when the view has no rows.
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }
}

/// Materialized result of a database query.
///
/// The result owns the snapshot it was read from. Borrowed rows returned by
/// [`QueryResult::collect`] are therefore tied to `&self`, not to the live bowl.
pub struct QueryResult<Q, F = ()>
where
    Q: QueryParam,
{
    bowl: Bowl,
    snapshot: std::sync::Arc<Snapshot>,
    rows: Vec<Q::State>,
    guards: RefCell<GuardStore>,
    _marker: PhantomData<(Q, F)>,
}

impl<Q, F> QueryResult<Q, F>
where
    Q: QueryParam,
    F: ExternalQueryFilter<Q>,
{
    /// Creates a result over every row of `Q` in `snapshot`.
    pub(crate) fn new(
        bowl: Bowl,
        snapshot: std::sync::Arc<Snapshot>,
        args: &QueryArgs,
        scope: Option<TypeId>,
    ) -> Self {
        let rows = external_filtered_rows::<Q, F>(&snapshot, args, scope);
        Self {
            bowl,
            snapshot,
            rows,
            guards: RefCell::new(Vec::new()),
            _marker: PhantomData,
        }
    }

    /// Fetches all rows from the owned snapshot.
    ///
    /// This returns borrowed values. Keep the [`QueryResult`] alive while using
    /// the collected rows.
    pub fn collect(&self) -> Vec<Q::Item<'_>> {
        self.rows
            .iter()
            .map(|row| {
                let mut guards = self.guards.borrow_mut();
                Q::fetch(&self.bowl, &self.snapshot, row, &mut guards)
            })
            .collect()
    }

    /// Number of rows in the result.
    pub fn len(&self) -> usize {
        self.rows.len()
    }

    /// Returns `true` when the result has no rows.
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }
}

/// Materialized result for `Query<(Mut<T>,), F>`.
pub struct MutResult<T, F = ()>
where
    T: Component,
{
    bowl: Bowl,
    rows: Vec<(Entity, Option<Revision>)>,
    _marker: PhantomData<(T, F)>,
}

impl<T, F> MutResult<T, F>
where
    T: Component,
{
    pub(crate) fn new(bowl: Bowl, rows: Vec<(Entity, Option<Revision>)>) -> Self {
        Self {
            bowl,
            rows,
            _marker: PhantomData,
        }
    }

    /// Fetches all mutable access handles.
    pub fn collect(&self) -> Vec<Mut<T>> {
        self.rows
            .iter()
            .map(|(entity, revision)| Mut::new(self.bowl.clone(), *entity, *revision))
            .collect()
    }

    /// Number of rows in the result.
    pub fn len(&self) -> usize {
        self.rows.len()
    }

    /// Returns `true` when the result has no rows.
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }
}

/// Materialized result for `Query<(Entity, Mut<T>), F>`.
pub struct EntityMutResult<T, F = ()>
where
    T: Component,
{
    bowl: Bowl,
    rows: Vec<(Entity, Option<Revision>)>,
    _marker: PhantomData<(T, F)>,
}

impl<T, F> EntityMutResult<T, F>
where
    T: Component,
{
    pub(crate) fn new(bowl: Bowl, rows: Vec<(Entity, Option<Revision>)>) -> Self {
        Self {
            bowl,
            rows,
            _marker: PhantomData,
        }
    }

    /// Fetches all entity ids and mutable access handles.
    pub fn collect(&self) -> Vec<(Entity, Mut<T>)> {
        self.rows
            .iter()
            .map(|(entity, revision)| (*entity, Mut::new(self.bowl.clone(), *entity, *revision)))
            .collect()
    }

    /// Number of rows in the result.
    pub fn len(&self) -> usize {
        self.rows.len()
    }

    /// Returns `true` when the result has no rows.
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[doc(hidden)]
pub struct Dep {
    type_id: TypeId,
    entity: Entity,
    revision: Revision,
}

impl Dep {
    pub(crate) fn is_current(&self, snapshot: &Snapshot) -> bool {
        snapshot.revision_by_type(self.type_id, self.entity) == Some(self.revision)
    }

    /// Absorbs the post-commit revision when the dep row was written by the
    /// owning invocation itself.
    pub(crate) fn refresh_written(&mut self, world: &Snapshot, writes: &[(TypeId, Entity)]) {
        let written = writes
            .iter()
            .any(|(type_id, entity)| *type_id == self.type_id && *entity == self.entity);

        if !written {
            return;
        }

        if let Some(revision) = world.revision_by_type(self.type_id, self.entity) {
            self.revision = revision;
        }
    }
}

/// Describes how a query-shaped type is enumerated and fetched from a snapshot.
///
/// This is the low-level trait behind both [`Query`] and [`View`].
///
/// The four pieces have distinct roles:
///
/// ```text
/// State
///   cheap row handle, usually Entity
///
/// Item<'a>
///   borrowed data passed to user code
///
/// keys
///   entity keys used to identify the system invocation
///
/// deps
///   tracked component revisions used for memoization
/// ```
///
/// `fetch` is safe because row states are produced from the same structural
/// snapshot and read guards are kept alive by the owning query/result.
pub trait QueryParam {
    type State: Clone + Send;
    type Item<'a>: Send;

    /// Enumerates all row states in `snapshot`.
    fn rows(snapshot: &Snapshot) -> Vec<Self::State>;
    /// Enumerates row states, optionally restricted to candidate entities
    /// supplied by a filter.
    ///
    /// Params that already iterate a component store ignore the hint; the
    /// unconstrained `Entity` param uses it to avoid a dense id scan.
    fn rows_hinted(snapshot: &Snapshot, hint: Option<Vec<Entity>>) -> Vec<Self::State> {
        let _ = hint;
        Self::rows(snapshot)
    }
    /// Returns entity keys that identify the invocation for this row.
    fn keys(state: &Self::State) -> Vec<Entity>;
    /// Returns tracked component revisions that should invalidate this row.
    fn deps(snapshot: &Snapshot, state: &Self::State) -> Vec<Dep>;
    /// Returns component rows this query item reads or writes while running.
    fn access(snapshot: &Snapshot, state: &Self::State) -> Vec<Access>;
    /// Returns component-level access covering every possible row, used by
    /// ambient params that read whole stores.
    fn access_all() -> Vec<Access>;
    /// Fetches the user-facing item for a previously enumerated row.
    fn fetch<'a>(
        bowl: &Bowl,
        snapshot: &'a Snapshot,
        state: &Self::State,
        guards: &mut GuardStore,
    ) -> Self::Item<'a>;
}

/// Query params that can use the generic external snapshot result.
#[doc(hidden)]
pub trait ExternalReadQueryParam: QueryParam {}

/// Query-shaped clone-on-write projection over the live world.
pub trait CowQueryParam {
    type State: Clone + EntityQueryState;
    type Item<'a>;

    /// Enumerates candidate row states from the current live world.
    fn rows(snapshot: &Snapshot) -> Vec<Self::State>;
    /// Enumerates row states, optionally restricted to filter candidates.
    fn rows_hinted(snapshot: &Snapshot, hint: Option<Vec<Entity>>) -> Vec<Self::State>;
    /// Mutates one previously-enumerated row.
    fn for_each_mut<F>(world: &mut World, state: &Self::State, f: F) -> bool
    where
        F: for<'a> FnOnce(Self::Item<'a>);
}

impl<T> CowQueryParam for (Cow<T>,)
where
    T: Component + Clone,
{
    type State = Entity;
    type Item<'a> = &'a mut T;

    fn rows(snapshot: &Snapshot) -> Vec<Self::State> {
        snapshot.entities_with::<T>()
    }

    fn rows_hinted(snapshot: &Snapshot, hint: Option<Vec<Entity>>) -> Vec<Self::State> {
        cow_rows_hinted::<T>(snapshot, hint)
    }

    fn for_each_mut<F>(world: &mut World, state: &Self::State, f: F) -> bool
    where
        F: for<'a> FnOnce(Self::Item<'a>),
    {
        world
            .update_component::<T, _, _>(*state, |component| f(component))
            .map(|(changed, _)| changed)
            .unwrap_or(false)
    }
}

impl<T> CowQueryParam for (Entity, Cow<T>)
where
    T: Component + Clone,
{
    type State = Entity;
    type Item<'a> = (Entity, &'a mut T);

    fn rows(snapshot: &Snapshot) -> Vec<Self::State> {
        snapshot.entities_with::<T>()
    }

    fn rows_hinted(snapshot: &Snapshot, hint: Option<Vec<Entity>>) -> Vec<Self::State> {
        cow_rows_hinted::<T>(snapshot, hint)
    }

    fn for_each_mut<F>(world: &mut World, state: &Self::State, f: F) -> bool
    where
        F: for<'a> FnOnce(Self::Item<'a>),
    {
        world
            .update_component::<T, _, _>(*state, |component| f((*state, component)))
            .map(|(changed, _)| changed)
            .unwrap_or(false)
    }
}

impl QueryParam for Entity {
    type State = Entity;
    type Item<'a> = Entity;

    fn rows(snapshot: &Snapshot) -> Vec<Self::State> {
        (0..snapshot.next_entity_raw()).map(Entity).collect()
    }

    fn rows_hinted(snapshot: &Snapshot, hint: Option<Vec<Entity>>) -> Vec<Self::State> {
        hint.unwrap_or_else(|| Self::rows(snapshot))
    }

    fn keys(state: &Self::State) -> Vec<Entity> {
        vec![*state]
    }

    fn deps(_snapshot: &Snapshot, _state: &Self::State) -> Vec<Dep> {
        Vec::new()
    }

    fn access(_snapshot: &Snapshot, _state: &Self::State) -> Vec<Access> {
        Vec::new()
    }

    fn access_all() -> Vec<Access> {
        Vec::new()
    }

    fn fetch<'a>(
        _bowl: &Bowl,
        _snapshot: &'a Snapshot,
        state: &Self::State,
        _guards: &mut GuardStore,
    ) -> Self::Item<'a> {
        *state
    }
}

impl ExternalReadQueryParam for Entity {}

impl<T: Component> QueryParam for &T {
    type State = Entity;
    type Item<'a> = &'a T;

    fn rows(snapshot: &Snapshot) -> Vec<Self::State> {
        snapshot.entities_with::<T>()
    }

    fn rows_hinted(snapshot: &Snapshot, hint: Option<Vec<Entity>>) -> Vec<Self::State> {
        match hint {
            Some(mut candidates) => {
                candidates.retain(|entity| snapshot.has::<T>(*entity));
                candidates
            }
            None => Self::rows(snapshot),
        }
    }

    fn keys(state: &Self::State) -> Vec<Entity> {
        vec![*state]
    }

    fn deps(snapshot: &Snapshot, state: &Self::State) -> Vec<Dep> {
        component_dep_if_tracked::<T>(snapshot, *state)
            .into_iter()
            .collect()
    }

    fn access(_snapshot: &Snapshot, state: &Self::State) -> Vec<Access> {
        vec![Access::read::<T>(*state)]
    }

    fn access_all() -> Vec<Access> {
        vec![Access::read_all::<T>()]
    }

    fn fetch<'a>(
        _bowl: &Bowl,
        snapshot: &'a Snapshot,
        state: &Self::State,
        guards: &mut GuardStore,
    ) -> Self::Item<'a> {
        store_read_guard(
            snapshot
                .get::<T>(*state)
                .expect("query row referenced a missing component"),
            guards,
        )
    }
}

impl<T: Component> ExternalReadQueryParam for &T {}

impl<T: Component> QueryParam for (MutRef<'_, T>,) {
    type State = Entity;
    type Item<'a> = MutRef<'a, T>;

    fn rows(snapshot: &Snapshot) -> Vec<Self::State> {
        snapshot.entities_with::<T>()
    }

    fn rows_hinted(snapshot: &Snapshot, hint: Option<Vec<Entity>>) -> Vec<Self::State> {
        match hint {
            Some(mut candidates) => {
                candidates.retain(|entity| snapshot.has::<T>(*entity));
                candidates
            }
            None => Self::rows(snapshot),
        }
    }

    fn keys(state: &Self::State) -> Vec<Entity> {
        vec![*state]
    }

    fn deps(snapshot: &Snapshot, state: &Self::State) -> Vec<Dep> {
        component_dep_if_tracked::<T>(snapshot, *state)
            .into_iter()
            .collect()
    }

    fn access(_snapshot: &Snapshot, state: &Self::State) -> Vec<Access> {
        vec![Access::write::<T>(*state)]
    }

    fn access_all() -> Vec<Access> {
        vec![Access::write_all::<T>()]
    }

    fn fetch<'a>(
        _bowl: &Bowl,
        snapshot: &'a Snapshot,
        state: &Self::State,
        guards: &mut GuardStore,
    ) -> Self::Item<'a> {
        let value = store_write_guard(
            snapshot
                .get_mut::<T>(*state)
                .expect("query row referenced a missing component"),
            guards,
        );
        MutRef::new(*state, value)
    }
}

/// One entry in an entity query tuple.
#[doc(hidden)]
pub trait QueryPart {
    type Item<'a>: Send;

    /// Number of entities this part's component store currently holds.
    ///
    /// Row enumeration iterates the smallest participating store and probes
    /// the other parts, so this should be cheap.
    fn store_len(snapshot: &Snapshot) -> usize;
    /// Entities this part could match, ascending.
    fn candidates(snapshot: &Snapshot) -> Vec<Entity>;
    fn matches(snapshot: &Snapshot, entity: Entity) -> bool;
    fn deps(snapshot: &Snapshot, entity: Entity) -> Vec<Dep>;
    fn access(snapshot: &Snapshot, entity: Entity) -> Vec<Access>;
    fn access_all() -> Access;
    fn fetch<'a>(
        bowl: &Bowl,
        snapshot: &'a Snapshot,
        entity: Entity,
        guards: &mut GuardStore,
    ) -> Self::Item<'a>;
}

/// Query tuple parts that can use the generic external snapshot result.
#[doc(hidden)]
pub trait ExternalReadQueryPart: QueryPart {}

impl<T: Component> QueryPart for &T {
    type Item<'a> = &'a T;

    fn store_len(snapshot: &Snapshot) -> usize {
        snapshot.store_len::<T>()
    }

    fn candidates(snapshot: &Snapshot) -> Vec<Entity> {
        snapshot.entities_with::<T>()
    }

    fn matches(snapshot: &Snapshot, entity: Entity) -> bool {
        snapshot.has::<T>(entity)
    }

    fn deps(snapshot: &Snapshot, entity: Entity) -> Vec<Dep> {
        component_dep_if_tracked::<T>(snapshot, entity)
            .into_iter()
            .collect()
    }

    fn access(_snapshot: &Snapshot, entity: Entity) -> Vec<Access> {
        vec![Access::read::<T>(entity)]
    }

    fn access_all() -> Access {
        Access::read_all::<T>()
    }

    fn fetch<'a>(
        _bowl: &Bowl,
        snapshot: &'a Snapshot,
        entity: Entity,
        guards: &mut GuardStore,
    ) -> Self::Item<'a> {
        store_read_guard(
            snapshot
                .get::<T>(entity)
                .expect("query row referenced a missing component"),
            guards,
        )
    }
}

impl<T: Component> ExternalReadQueryPart for &T {}

impl<T: Component> QueryPart for MutRef<'_, T> {
    type Item<'a> = MutRef<'a, T>;

    fn store_len(snapshot: &Snapshot) -> usize {
        snapshot.store_len::<T>()
    }

    fn candidates(snapshot: &Snapshot) -> Vec<Entity> {
        snapshot.entities_with::<T>()
    }

    fn matches(snapshot: &Snapshot, entity: Entity) -> bool {
        snapshot.has::<T>(entity)
    }

    fn deps(snapshot: &Snapshot, entity: Entity) -> Vec<Dep> {
        component_dep_if_tracked::<T>(snapshot, entity)
            .into_iter()
            .collect()
    }

    fn access(_snapshot: &Snapshot, entity: Entity) -> Vec<Access> {
        vec![Access::write::<T>(entity)]
    }

    fn access_all() -> Access {
        Access::write_all::<T>()
    }

    fn fetch<'a>(
        _bowl: &Bowl,
        snapshot: &'a Snapshot,
        entity: Entity,
        guards: &mut GuardStore,
    ) -> Self::Item<'a> {
        let value = store_write_guard(
            snapshot
                .get_mut::<T>(entity)
                .expect("query row referenced a missing component"),
            guards,
        );
        MutRef::new(entity, value)
    }
}

macro_rules! impl_entity_query_param {
    ($($P:ident),*) => {
        impl<$($P: QueryPart),*> QueryParam for (Entity, $($P,)*)
        {
            type State = Entity;
            type Item<'a> = (Entity, $(<$P as QueryPart>::Item<'a>,)*);

            fn rows(snapshot: &Snapshot) -> Vec<Self::State> {
                const PARTS: usize = [$(stringify!($P)),*].len();

                let mut best_len = usize::MAX;
                let mut best_index = 0usize;
                let mut index = 0usize;
                $(
                    let len = $P::store_len(snapshot);
                    if len < best_len {
                        best_len = len;
                        best_index = index;
                    }
                    index += 1;
                )*
                let _ = (index, best_len);

                let mut candidates = Vec::new();
                let mut index = 0usize;
                $(
                    if index == best_index {
                        candidates = $P::candidates(snapshot);
                    }
                    index += 1;
                )*
                let _ = index;

                // A single part's candidates are exactly its matches; only
                // multi-part queries need to probe the non-primary stores.
                if PARTS > 1 {
                    candidates.retain(|entity| true $(&& $P::matches(snapshot, *entity))*);
                }

                candidates
            }

            fn rows_hinted(snapshot: &Snapshot, hint: Option<Vec<Entity>>) -> Vec<Self::State> {
                match hint {
                    Some(mut candidates) => {
                        candidates.retain(|entity| true $(&& $P::matches(snapshot, *entity))*);
                        candidates
                    }
                    None => Self::rows(snapshot),
                }
            }


            fn keys(state: &Self::State) -> Vec<Entity> {
                vec![*state]
            }

            fn deps(snapshot: &Snapshot, state: &Self::State) -> Vec<Dep> {
                let mut deps = Vec::new();
                $(deps.extend($P::deps(snapshot, *state));)*
                deps
            }

            fn access(snapshot: &Snapshot, state: &Self::State) -> Vec<Access> {
                let mut access = Vec::new();
                $(access.extend($P::access(snapshot, *state));)*
                access
            }

            fn access_all() -> Vec<Access> {
                vec![$($P::access_all(),)*]
            }

            fn fetch<'a>(
                bowl: &Bowl,
                snapshot: &'a Snapshot,
                state: &Self::State,
                guards: &mut GuardStore,
            ) -> Self::Item<'a> {
                (
                    *state,
                    $($P::fetch(bowl, snapshot, *state, guards),)*
                )
            }
        }

        impl<$($P: ExternalReadQueryPart),*> ExternalReadQueryParam for (Entity, $($P,)*) {}
    };
}

all_tuples!(impl_entity_query_param, 1, 8, P);

#[doc(hidden)]
pub trait EntityQueryState {
    fn entity(&self) -> Entity;
}

impl EntityQueryState for Entity {
    fn entity(&self) -> Entity {
        *self
    }
}

/// Additional predicate applied to query data without changing the returned row.
///
/// Filters can contribute dependencies, so a memoized system row can be
/// invalidated when a tracked marker component changes even though that marker
/// is not part of the returned item.
pub trait QueryFilter<Q: QueryParam> {
    fn matches(snapshot: &Snapshot, state: &Q::State) -> bool;
    fn deps(snapshot: &Snapshot, state: &Q::State) -> Vec<Dep>;
    fn access(snapshot: &Snapshot, state: &Q::State) -> Vec<Access>;
    /// Component-level access covering every row this filter inspects.
    fn access_all() -> Vec<Access> {
        Vec::new()
    }
    /// Entities this filter could match, used to drive row enumeration for
    /// params without a component store of their own (`Query<Entity, ...>`).
    fn entity_candidates(_snapshot: &Snapshot) -> Option<Vec<Entity>> {
        None
    }
}

impl<Q: QueryParam> QueryFilter<Q> for () {
    fn matches(_snapshot: &Snapshot, _state: &Q::State) -> bool {
        true
    }

    fn deps(_snapshot: &Snapshot, _state: &Q::State) -> Vec<Dep> {
        Vec::new()
    }

    fn access(_snapshot: &Snapshot, _state: &Q::State) -> Vec<Access> {
        Vec::new()
    }
}

impl<Q, T> QueryFilter<Q> for With<T>
where
    Q: QueryParam,
    Q::State: EntityQueryState,
    T: Component,
{
    fn matches(snapshot: &Snapshot, state: &Q::State) -> bool {
        snapshot.has::<T>(state.entity())
    }

    fn deps(snapshot: &Snapshot, state: &Q::State) -> Vec<Dep> {
        component_dep_if_tracked::<T>(snapshot, state.entity())
            .into_iter()
            .collect()
    }

    fn access(_snapshot: &Snapshot, state: &Q::State) -> Vec<Access> {
        vec![Access::read::<T>(state.entity())]
    }

    fn access_all() -> Vec<Access> {
        vec![Access::read_all::<T>()]
    }

    fn entity_candidates(snapshot: &Snapshot) -> Option<Vec<Entity>> {
        Some(snapshot.entities_with::<T>())
    }
}

impl<Q, T> QueryFilter<Q> for Without<T>
where
    Q: QueryParam,
    Q::State: EntityQueryState,
    T: Component,
{
    fn matches(snapshot: &Snapshot, state: &Q::State) -> bool {
        !snapshot.has::<T>(state.entity())
    }

    fn deps(_snapshot: &Snapshot, _state: &Q::State) -> Vec<Dep> {
        Vec::new()
    }

    fn access(_snapshot: &Snapshot, state: &Q::State) -> Vec<Access> {
        vec![Access::read::<T>(state.entity())]
    }

    fn access_all() -> Vec<Access> {
        vec![Access::read_all::<T>()]
    }
}

/// Filter over an external query row state.
pub trait ExternalFilter<State>: 'static {
    fn matches(snapshot: &Snapshot, args: &QueryArgs, scope: Option<TypeId>, state: &State)
    -> bool;

    /// Entities this filter could match, resolved through an index.
    fn entity_candidates(
        _snapshot: &Snapshot,
        _args: &QueryArgs,
        _scope: Option<TypeId>,
    ) -> Option<Vec<Entity>> {
        None
    }
}

impl<State> ExternalFilter<State> for () {
    fn matches(
        _snapshot: &Snapshot,
        _args: &QueryArgs,
        _scope: Option<TypeId>,
        _state: &State,
    ) -> bool {
        true
    }
}

impl<State, F> ExternalFilter<State> for Where<F>
where
    State: EntityQueryState,
    F: FilterExpr,
{
    fn matches(
        snapshot: &Snapshot,
        args: &QueryArgs,
        scope: Option<TypeId>,
        state: &State,
    ) -> bool {
        F::matches(state.entity(), snapshot, args, scope)
    }

    fn entity_candidates(
        snapshot: &Snapshot,
        args: &QueryArgs,
        scope: Option<TypeId>,
    ) -> Option<Vec<Entity>> {
        F::candidates(snapshot, args, scope)
    }
}

impl<State, T> ExternalFilter<State> for With<T>
where
    State: EntityQueryState,
    T: Component,
{
    fn matches(
        snapshot: &Snapshot,
        _args: &QueryArgs,
        _scope: Option<TypeId>,
        state: &State,
    ) -> bool {
        snapshot.has::<T>(state.entity())
    }

    fn entity_candidates(
        snapshot: &Snapshot,
        _args: &QueryArgs,
        _scope: Option<TypeId>,
    ) -> Option<Vec<Entity>> {
        Some(snapshot.entities_with::<T>())
    }
}

impl<State, T> ExternalFilter<State> for Without<T>
where
    State: EntityQueryState,
    T: Component,
{
    fn matches(
        snapshot: &Snapshot,
        _args: &QueryArgs,
        _scope: Option<TypeId>,
        state: &State,
    ) -> bool {
        !snapshot.has::<T>(state.entity())
    }
}

/// Filter used by external read-only bowl queries.
pub trait ExternalQueryFilter<Q: QueryParam>: 'static {
    fn matches(
        snapshot: &Snapshot,
        args: &QueryArgs,
        scope: Option<TypeId>,
        state: &Q::State,
    ) -> bool;

    /// Entities this filter could match, resolved through an index.
    fn entity_candidates(
        _snapshot: &Snapshot,
        _args: &QueryArgs,
        _scope: Option<TypeId>,
    ) -> Option<Vec<Entity>> {
        None
    }
}

impl<Q, F> ExternalQueryFilter<Q> for F
where
    Q: QueryParam,
    F: ExternalFilter<Q::State>,
{
    fn matches(
        snapshot: &Snapshot,
        args: &QueryArgs,
        scope: Option<TypeId>,
        state: &Q::State,
    ) -> bool {
        F::matches(snapshot, args, scope, state)
    }

    fn entity_candidates(
        snapshot: &Snapshot,
        args: &QueryArgs,
        scope: Option<TypeId>,
    ) -> Option<Vec<Entity>> {
        F::entity_candidates(snapshot, args, scope)
    }
}

/// Runtime-argument filter expression used inside [`Where`].
pub trait FilterExpr: 'static {
    fn matches(
        entity: Entity,
        snapshot: &Snapshot,
        args: &QueryArgs,
        scope: Option<TypeId>,
    ) -> bool;

    /// Entities this expression could match, resolved through an index.
    ///
    /// `None` means the expression cannot narrow candidates and enumeration
    /// falls back to store iteration. Candidates are a superset of matches;
    /// `matches` is always applied afterwards.
    fn candidates(
        _snapshot: &Snapshot,
        _args: &QueryArgs,
        _scope: Option<TypeId>,
    ) -> Option<Vec<Entity>> {
        None
    }
}

impl<T> FilterExpr for Eq<T>
where
    T: Component + PartialEq,
{
    fn matches(
        entity: Entity,
        snapshot: &Snapshot,
        args: &QueryArgs,
        scope: Option<TypeId>,
    ) -> bool {
        snapshot
            .get::<T>(entity)
            .is_some_and(|value| *value == *args.get::<T>(scope))
    }

    fn candidates(
        snapshot: &Snapshot,
        args: &QueryArgs,
        scope: Option<TypeId>,
    ) -> Option<Vec<Entity>> {
        let fingerprint = args.get::<T>(scope).fingerprint()?;
        Some(snapshot.entities_with_fingerprint::<T>(fingerprint))
    }
}

impl<T> FilterExpr for Gte<T>
where
    T: Component + PartialOrd,
{
    fn matches(
        entity: Entity,
        snapshot: &Snapshot,
        args: &QueryArgs,
        scope: Option<TypeId>,
    ) -> bool {
        snapshot
            .get::<T>(entity)
            .is_some_and(|value| *value >= *args.get::<T>(scope))
    }
}

impl<T: Component> FilterExpr for With<T> {
    fn matches(
        entity: Entity,
        snapshot: &Snapshot,
        _args: &QueryArgs,
        _scope: Option<TypeId>,
    ) -> bool {
        snapshot.has::<T>(entity)
    }

    fn candidates(
        snapshot: &Snapshot,
        _args: &QueryArgs,
        _scope: Option<TypeId>,
    ) -> Option<Vec<Entity>> {
        Some(snapshot.entities_with::<T>())
    }
}

impl<T: Component> FilterExpr for Without<T> {
    fn matches(
        entity: Entity,
        snapshot: &Snapshot,
        _args: &QueryArgs,
        _scope: Option<TypeId>,
    ) -> bool {
        !snapshot.has::<T>(entity)
    }
}

impl<A, B> FilterExpr for And<A, B>
where
    A: FilterExpr,
    B: FilterExpr,
{
    fn matches(
        entity: Entity,
        snapshot: &Snapshot,
        args: &QueryArgs,
        scope: Option<TypeId>,
    ) -> bool {
        A::matches(entity, snapshot, args, scope) && B::matches(entity, snapshot, args, scope)
    }

    fn candidates(
        snapshot: &Snapshot,
        args: &QueryArgs,
        scope: Option<TypeId>,
    ) -> Option<Vec<Entity>> {
        // Either side narrows the candidate set; `matches` still verifies the
        // full conjunction.
        A::candidates(snapshot, args, scope).or_else(|| B::candidates(snapshot, args, scope))
    }
}

impl<A, B> FilterExpr for Or<A, B>
where
    A: FilterExpr,
    B: FilterExpr,
{
    fn matches(
        entity: Entity,
        snapshot: &Snapshot,
        args: &QueryArgs,
        scope: Option<TypeId>,
    ) -> bool {
        A::matches(entity, snapshot, args, scope) || B::matches(entity, snapshot, args, scope)
    }
}

impl<F: FilterExpr> FilterExpr for Not<F> {
    fn matches(
        entity: Entity,
        snapshot: &Snapshot,
        args: &QueryArgs,
        scope: Option<TypeId>,
    ) -> bool {
        !F::matches(entity, snapshot, args, scope)
    }
}

pub(crate) fn filtered_rows<Q, F>(snapshot: &Snapshot) -> Vec<Q::State>
where
    Q: QueryParam,
    F: QueryFilter<Q>,
{
    Q::rows_hinted(snapshot, F::entity_candidates(snapshot))
        .into_iter()
        .filter(|state| F::matches(snapshot, state))
        .collect()
}

pub(crate) fn external_filtered_rows<Q, F>(
    snapshot: &Snapshot,
    args: &QueryArgs,
    scope: Option<TypeId>,
) -> Vec<Q::State>
where
    Q: QueryParam,
    F: ExternalQueryFilter<Q>,
{
    Q::rows_hinted(snapshot, F::entity_candidates(snapshot, args, scope))
        .into_iter()
        .filter(|state| F::matches(snapshot, args, scope, state))
        .collect()
}

pub(crate) fn external_filtered_cow_rows<Q, F>(
    snapshot: &Snapshot,
    args: &QueryArgs,
    scope: Option<TypeId>,
) -> Vec<Q::State>
where
    Q: CowQueryParam,
    F: ExternalFilter<Q::State>,
{
    Q::rows_hinted(snapshot, F::entity_candidates(snapshot, args, scope))
        .into_iter()
        .filter(|state| F::matches(snapshot, args, scope, state))
        .collect()
}

pub(crate) fn external_mut_rows<T, F>(
    snapshot: &Snapshot,
    args: &QueryArgs,
    scope: Option<TypeId>,
) -> Vec<(Entity, Option<Revision>)>
where
    T: Component,
    F: ExternalFilter<Entity>,
{
    let candidates = match F::entity_candidates(snapshot, args, scope) {
        Some(mut candidates) => {
            candidates.retain(|entity| snapshot.has::<T>(*entity));
            candidates
        }
        None => snapshot.entities_with::<T>(),
    };

    candidates
        .into_iter()
        .filter(|entity| F::matches(snapshot, args, scope, entity))
        .map(|entity| (entity, snapshot.revision::<T>(entity)))
        .collect()
}

pub(crate) fn filtered_deps<Q, F>(snapshot: &Snapshot, state: &Q::State) -> Vec<Dep>
where
    Q: QueryParam,
    F: QueryFilter<Q>,
{
    let mut deps = Q::deps(snapshot, state);
    deps.extend(F::deps(snapshot, state));
    deps
}

pub(crate) fn filtered_access<Q, F>(snapshot: &Snapshot, state: &Q::State) -> Vec<Access>
where
    Q: QueryParam,
    F: QueryFilter<Q>,
{
    let mut access = Q::access(snapshot, state);
    access.extend(F::access(snapshot, state));
    access
}

/// Produces a dependency for `T` on `entity` when `T` participates in revision
/// tracking.
fn component_dep_if_tracked<T: Component>(snapshot: &Snapshot, entity: Entity) -> Option<Dep> {
    T::tracked().then(|| Dep {
        type_id: TypeId::of::<T>(),
        entity,
        revision: snapshot
            .revision::<T>(entity)
            .expect("query dependency referenced a missing component"),
    })
}
