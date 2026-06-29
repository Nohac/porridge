use std::{collections::HashMap, marker::PhantomData, sync::Arc};

use futures::future::{FutureExt, LocalBoxFuture};

use crate::{
    Commands, Entity, Query, View, With,
    commands::CommandOp,
    query::{Dep, QueryFilter, QueryParam, filtered_deps, filtered_rows},
    world::{Snapshot, SystemId, SystemInvocation},
};
use variadics_please::all_tuples;

/// Memoized dependency record for one system invocation.
///
/// Invocation identity lives in [`SystemInvocation`]; this entry records the
/// tracked component revisions observed the last time that invocation ran.
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct MemoEntry {
    deps: Vec<Dep>,
}

/// Buffered output from one system invocation.
///
/// The runner uses `owner` to remove old derived facts for that invocation
/// before applying `commands`.
pub(crate) struct SystemOutput {
    pub(crate) owner: SystemInvocation,
    pub(crate) commands: Vec<Box<dyn CommandOp>>,
}

/// Type-erased executable system.
///
/// `Runnable` receives an immutable snapshot plus the memo table. It returns all
/// command buffers that need to be committed for this generation.
///
/// The returned future is local rather than `Send`. This lets ordinary async
/// functions borrow snapshot data across `.await` without forcing the first
/// implementation to solve cross-thread spawning. The bowl can still be shared;
/// this only constrains where the evaluation future may be polled.
pub(crate) trait Runnable: Send + Sync {
    fn run<'a>(
        &'a self,
        snapshot: &'a Snapshot,
        memo: &'a mut HashMap<SystemInvocation, MemoEntry>,
    ) -> LocalBoxFuture<'a, Vec<SystemOutput>>;
}

/// Type-erased registered system.
#[derive(Clone)]
pub struct BoxedSystem(pub(crate) Arc<dyn Runnable>);

/// Converts a user function into a registered system.
///
/// The `Marker` parameter is the usual Rust trick used to distinguish function
/// shapes without overlapping trait impls. Users do not name it directly.
///
pub trait IntoSystem<Marker>: Send + Sync + 'static {
    fn into_system(self, id: SystemId) -> BoxedSystem;
}

pub struct QueryCommands;
pub struct TwoQueriesCommands;
pub struct TwoQueriesViewCommands;
pub struct TwoQueriesTwoViewsCommands;
pub struct QueryViewCommands;
pub struct QueryTwoViewsCommands;
pub struct OnCompleteMarker;

/// Callback run once after a system has finished iterating its driving query.
pub trait CompleteCallback: Send + Sync + 'static {
    fn run(&self, commands: Commands);
}

impl<F> CompleteCallback for F
where
    F: Fn(Commands) + Send + Sync + 'static,
{
    fn run(&self, commands: Commands) {
        self(commands);
    }
}

/// Extension methods for system configuration.
pub trait SystemExt: Sized {
    /// Runs `callback` once after this system has completed all invocations that
    /// were invalid for the current snapshot.
    fn on_complete<C>(self, callback: C) -> OnComplete<Self, C>
    where
        C: CompleteCallback,
    {
        OnComplete {
            system: self,
            callback,
        }
    }
}

impl<S> SystemExt for S {}

/// System wrapper produced by [`SystemExt::on_complete`].
pub struct OnComplete<S, C> {
    system: S,
    callback: C,
}

struct OnCompleteSystem<C> {
    id: SystemId,
    system: BoxedSystem,
    callback: C,
}

impl<C> Runnable for OnCompleteSystem<C>
where
    C: CompleteCallback,
{
    fn run<'a>(
        &'a self,
        snapshot: &'a Snapshot,
        memo: &'a mut HashMap<SystemInvocation, MemoEntry>,
    ) -> LocalBoxFuture<'a, Vec<SystemOutput>> {
        async move {
            let mut outputs = self.system.0.run(snapshot, memo).await;

            if !outputs.is_empty() {
                let commands = Commands::new();
                self.callback.run(commands.clone());
                outputs.push(SystemOutput {
                    owner: SystemInvocation {
                        system: self.id,
                        keys: Vec::new(),
                    },
                    commands: commands.take(),
                });
            }

            outputs
        }
        .boxed_local()
    }
}

impl<S, C, M> IntoSystem<(OnCompleteMarker, M)> for OnComplete<S, C>
where
    S: IntoSystem<M>,
    C: CompleteCallback,
{
    fn into_system(self, id: SystemId) -> BoxedSystem {
        let system = self.system.into_system(id);
        BoxedSystem(Arc::new(OnCompleteSystem {
            id,
            system,
            callback: self.callback,
        }))
    }
}

/// Builds a completion callback that inserts `component` on `entity`.
pub fn insert_on<T>(entity: Entity, component: T) -> impl CompleteCallback
where
    T: crate::Component + Clone,
{
    move |mut commands: Commands| {
        commands.entity(entity).insert(component.clone());
    }
}

struct QuerySystem<F, Q, Filter> {
    id: SystemId,
    function: F,
    _marker: PhantomData<(Q, Filter)>,
}

impl<F, Q, Filter> Runnable for QuerySystem<F, Q, Filter>
where
    F: Send + Sync + 'static,
    Q: QueryParam + Send + Sync + 'static,
    Filter: QueryFilter<Q> + Send + Sync + 'static,
    for<'a> F: AsyncFn(Query<Q::Item<'a>, Filter>, Commands),
{
    /// Runs every memo-invalid row of `Q` against the current snapshot.
    ///
    /// `View` is absent here, so the invocation dependencies are exactly the
    /// driving query deps.
    fn run<'a>(
        &'a self,
        snapshot: &'a Snapshot,
        memo: &'a mut HashMap<SystemInvocation, MemoEntry>,
    ) -> LocalBoxFuture<'a, Vec<SystemOutput>> {
        async move {
            let mut outputs = Vec::new();

            for row in filtered_rows::<Q, Filter>(snapshot) {
                let owner = SystemInvocation {
                    system: self.id,
                    keys: Q::keys(&row),
                };
                let deps = filtered_deps::<Q, Filter>(snapshot, &row);

                if memo.get(&owner).is_some_and(|entry| entry.deps == deps) {
                    continue;
                }

                let commands = Commands::new();
                (self.function)(Query::new(Q::fetch(snapshot, &row)), commands.clone()).await;

                outputs.push(SystemOutput {
                    owner: owner.clone(),
                    commands: commands.take(),
                });
                memo.insert(owner, MemoEntry { deps });
            }

            outputs
        }
        .boxed_local()
    }
}

macro_rules! impl_query_system {
    ($($T:ident),*) => {
        impl<F, $($T: crate::Component),*> IntoSystem<(QueryCommands, (Entity, $(& $T,)*))> for F
        where
            F: Send + Sync + 'static,
            for<'a> F: AsyncFn(Query<(Entity, $(&'a $T,)*)>, Commands),
        {
            fn into_system(self, id: SystemId) -> BoxedSystem {
                BoxedSystem(Arc::new(QuerySystem {
                    id,
                    function: self,
                    _marker: PhantomData::<((Entity, $(& $T,)*), ())>,
                }))
            }
        }
    };
}

all_tuples!(impl_query_system, 1, 8, T);

macro_rules! impl_query_with_system {
    ($($T:ident),*) => {
        impl<F, Marker, $($T: crate::Component),*> IntoSystem<(QueryCommands, (Entity, $(& $T,)*), With<Marker>)> for F
        where
            F: Send + Sync + 'static,
            Marker: crate::Component,
            for<'a> F: AsyncFn(Query<(Entity, $(&'a $T,)*), With<Marker>>, Commands),
        {
            fn into_system(self, id: SystemId) -> BoxedSystem {
                BoxedSystem(Arc::new(QuerySystem {
                    id,
                    function: self,
                    _marker: PhantomData::<((Entity, $(& $T,)*), With<Marker>)>,
                }))
            }
        }
    };
}

all_tuples!(impl_query_with_system, 1, 8, T);

impl<F, Marker> IntoSystem<(QueryCommands, Entity, With<Marker>)> for F
where
    F: Send + Sync + 'static,
    Marker: crate::Component,
    for<'a> F: AsyncFn(Query<Entity, With<Marker>>, Commands),
{
    fn into_system(self, id: SystemId) -> BoxedSystem {
        BoxedSystem(Arc::new(QuerySystem {
            id,
            function: self,
            _marker: PhantomData::<(Entity, With<Marker>)>,
        }))
    }
}

struct TwoQueriesSystem<F, Q0, Filter0, Q1, Filter1> {
    id: SystemId,
    function: F,
    _marker: PhantomData<(Q0, Filter0, Q1, Filter1)>,
}

impl<F, Q0, Filter0, Q1, Filter1> Runnable for TwoQueriesSystem<F, Q0, Filter0, Q1, Filter1>
where
    F: Send + Sync + 'static,
    Q0: QueryParam + Send + Sync + 'static,
    Q1: QueryParam + Send + Sync + 'static,
    Filter0: QueryFilter<Q0> + Send + Sync + 'static,
    Filter1: QueryFilter<Q1> + Send + Sync + 'static,
    for<'a> F: AsyncFn(Query<Q0::Item<'a>, Filter0>, Query<Q1::Item<'a>, Filter1>, Commands),
{
    /// Runs every memo-invalid cross product row of `Q0 x Q1`.
    ///
    /// Multi-query systems are useful for joins and readiness gates. The
    /// invocation identity and memo dependencies include both driving rows.
    fn run<'a>(
        &'a self,
        snapshot: &'a Snapshot,
        memo: &'a mut HashMap<SystemInvocation, MemoEntry>,
    ) -> LocalBoxFuture<'a, Vec<SystemOutput>> {
        async move {
            let mut outputs = Vec::new();
            let rows_0 = filtered_rows::<Q0, Filter0>(snapshot);
            let rows_1 = filtered_rows::<Q1, Filter1>(snapshot);

            for row_0 in &rows_0 {
                for row_1 in &rows_1 {
                    let mut keys = Q0::keys(row_0);
                    keys.extend(Q1::keys(row_1));
                    let owner = SystemInvocation {
                        system: self.id,
                        keys,
                    };

                    let mut deps = filtered_deps::<Q0, Filter0>(snapshot, row_0);
                    deps.extend(filtered_deps::<Q1, Filter1>(snapshot, row_1));

                    if memo.get(&owner).is_some_and(|entry| entry.deps == deps) {
                        continue;
                    }

                    let commands = Commands::new();
                    (self.function)(
                        Query::new(Q0::fetch(snapshot, row_0)),
                        Query::new(Q1::fetch(snapshot, row_1)),
                        commands.clone(),
                    )
                    .await;

                    outputs.push(SystemOutput {
                        owner: owner.clone(),
                        commands: commands.take(),
                    });
                    memo.insert(owner, MemoEntry { deps });
                }
            }

            outputs
        }
        .boxed_local()
    }
}

struct TwoQueriesViewSystem<F, Q0, Filter0, Q1, Filter1, V> {
    id: SystemId,
    function: F,
    _marker: PhantomData<(Q0, Filter0, Q1, Filter1, V)>,
}

impl<F, Q0, Filter0, Q1, Filter1, V> Runnable
    for TwoQueriesViewSystem<F, Q0, Filter0, Q1, Filter1, V>
where
    F: Send + Sync + 'static,
    Q0: QueryParam + Send + Sync + 'static,
    Q1: QueryParam + Send + Sync + 'static,
    Filter0: QueryFilter<Q0> + Send + Sync + 'static,
    Filter1: QueryFilter<Q1> + Send + Sync + 'static,
    V: QueryParam + Send + Sync + 'static,
    for<'a> F:
        AsyncFn(Query<Q0::Item<'a>, Filter0>, Query<Q1::Item<'a>, Filter1>, View<'a, V>, Commands),
{
    fn run<'a>(
        &'a self,
        snapshot: &'a Snapshot,
        memo: &'a mut HashMap<SystemInvocation, MemoEntry>,
    ) -> LocalBoxFuture<'a, Vec<SystemOutput>> {
        async move {
            let mut outputs = Vec::new();
            let rows_0 = filtered_rows::<Q0, Filter0>(snapshot);
            let rows_1 = filtered_rows::<Q1, Filter1>(snapshot);

            for row_0 in &rows_0 {
                for row_1 in &rows_1 {
                    let mut keys = Q0::keys(row_0);
                    keys.extend(Q1::keys(row_1));
                    let owner = SystemInvocation {
                        system: self.id,
                        keys,
                    };

                    let mut deps = filtered_deps::<Q0, Filter0>(snapshot, row_0);
                    deps.extend(filtered_deps::<Q1, Filter1>(snapshot, row_1));

                    if memo.get(&owner).is_some_and(|entry| entry.deps == deps) {
                        continue;
                    }

                    let commands = Commands::new();
                    (self.function)(
                        Query::new(Q0::fetch(snapshot, row_0)),
                        Query::new(Q1::fetch(snapshot, row_1)),
                        View::<V>::new(snapshot),
                        commands.clone(),
                    )
                    .await;

                    outputs.push(SystemOutput {
                        owner: owner.clone(),
                        commands: commands.take(),
                    });
                    memo.insert(owner, MemoEntry { deps });
                }
            }

            outputs
        }
        .boxed_local()
    }
}

struct TwoQueriesTwoViewsSystem<F, Q0, Filter0, Q1, Filter1, V0, V1> {
    id: SystemId,
    function: F,
    _marker: PhantomData<(Q0, Filter0, Q1, Filter1, V0, V1)>,
}

impl<F, Q0, Filter0, Q1, Filter1, V0, V1> Runnable
    for TwoQueriesTwoViewsSystem<F, Q0, Filter0, Q1, Filter1, V0, V1>
where
    F: Send + Sync + 'static,
    Q0: QueryParam + Send + Sync + 'static,
    Q1: QueryParam + Send + Sync + 'static,
    Filter0: QueryFilter<Q0> + Send + Sync + 'static,
    Filter1: QueryFilter<Q1> + Send + Sync + 'static,
    V0: QueryParam + Send + Sync + 'static,
    V1: QueryParam + Send + Sync + 'static,
    for<'a> F: AsyncFn(
        Query<Q0::Item<'a>, Filter0>,
        Query<Q1::Item<'a>, Filter1>,
        View<'a, V0>,
        View<'a, V1>,
        Commands,
    ),
{
    fn run<'a>(
        &'a self,
        snapshot: &'a Snapshot,
        memo: &'a mut HashMap<SystemInvocation, MemoEntry>,
    ) -> LocalBoxFuture<'a, Vec<SystemOutput>> {
        async move {
            let mut outputs = Vec::new();
            let rows_0 = filtered_rows::<Q0, Filter0>(snapshot);
            let rows_1 = filtered_rows::<Q1, Filter1>(snapshot);

            for row_0 in &rows_0 {
                for row_1 in &rows_1 {
                    let mut keys = Q0::keys(row_0);
                    keys.extend(Q1::keys(row_1));
                    let owner = SystemInvocation {
                        system: self.id,
                        keys,
                    };

                    let mut deps = filtered_deps::<Q0, Filter0>(snapshot, row_0);
                    deps.extend(filtered_deps::<Q1, Filter1>(snapshot, row_1));

                    if memo.get(&owner).is_some_and(|entry| entry.deps == deps) {
                        continue;
                    }

                    let commands = Commands::new();
                    (self.function)(
                        Query::new(Q0::fetch(snapshot, row_0)),
                        Query::new(Q1::fetch(snapshot, row_1)),
                        View::<V0>::new(snapshot),
                        View::<V1>::new(snapshot),
                        commands.clone(),
                    )
                    .await;

                    outputs.push(SystemOutput {
                        owner: owner.clone(),
                        commands: commands.take(),
                    });
                    memo.insert(owner, MemoEntry { deps });
                }
            }

            outputs
        }
        .boxed_local()
    }
}

macro_rules! impl_two_query_family {
    ($($A:ident),*; $($B:ident),*) => {
        impl<F, $($A: crate::Component,)* $($B: crate::Component),*> IntoSystem<(TwoQueriesCommands, (Entity, $(& $A,)*), (Entity, $(& $B,)*))> for F
        where
            F: Send + Sync + 'static,
            for<'a> F: AsyncFn(
                Query<(Entity, $(&'a $A,)*)>,
                Query<(Entity, $(&'a $B,)*)>,
                Commands,
            ),
        {
            fn into_system(self, id: SystemId) -> BoxedSystem {
                BoxedSystem(Arc::new(TwoQueriesSystem {
                    id,
                    function: self,
                    _marker: PhantomData::<((Entity, $(& $A,)*), (), (Entity, $(& $B,)*), ())>,
                }))
            }
        }

        impl<F, V, $($A: crate::Component,)* $($B: crate::Component),*> IntoSystem<(TwoQueriesViewCommands, (Entity, $(& $A,)*), (Entity, $(& $B,)*), V)> for F
        where
            F: Send + Sync + 'static,
            V: QueryParam + Send + Sync + 'static,
            for<'a> F: AsyncFn(
                Query<(Entity, $(&'a $A,)*)>,
                Query<(Entity, $(&'a $B,)*)>,
                View<'a, V>,
                Commands,
            ),
        {
            fn into_system(self, id: SystemId) -> BoxedSystem {
                BoxedSystem(Arc::new(TwoQueriesViewSystem {
                    id,
                    function: self,
                    _marker: PhantomData::<((Entity, $(& $A,)*), (), (Entity, $(& $B,)*), (), V)>,
                }))
            }
        }

        impl<F, V0, V1, $($A: crate::Component,)* $($B: crate::Component),*> IntoSystem<(TwoQueriesTwoViewsCommands, (Entity, $(& $A,)*), (Entity, $(& $B,)*), V0, V1)> for F
        where
            F: Send + Sync + 'static,
            V0: QueryParam + Send + Sync + 'static,
            V1: QueryParam + Send + Sync + 'static,
            for<'a> F: AsyncFn(
                Query<(Entity, $(&'a $A,)*)>,
                Query<(Entity, $(&'a $B,)*)>,
                View<'a, V0>,
                View<'a, V1>,
                Commands,
            ),
        {
            fn into_system(self, id: SystemId) -> BoxedSystem {
                BoxedSystem(Arc::new(TwoQueriesTwoViewsSystem {
                    id,
                    function: self,
                    _marker: PhantomData::<(
                        (Entity, $(& $A,)*),
                        (),
                        (Entity, $(& $B,)*),
                        (),
                        V0,
                        V1,
                    )>,
                }))
            }
        }
    };
}

macro_rules! impl_two_query_family_b1 {
    ($($A:ident),*) => { impl_two_query_family!($($A),*; B0); };
}

macro_rules! impl_two_query_family_b2 {
    ($($A:ident),*) => { impl_two_query_family!($($A),*; B0, B1); };
}

macro_rules! impl_two_query_family_b3 {
    ($($A:ident),*) => { impl_two_query_family!($($A),*; B0, B1, B2); };
}

macro_rules! impl_two_query_family_b4 {
    ($($A:ident),*) => { impl_two_query_family!($($A),*; B0, B1, B2, B3); };
}

all_tuples!(impl_two_query_family_b1, 1, 8, A);
all_tuples!(impl_two_query_family_b2, 1, 8, A);
all_tuples!(impl_two_query_family_b3, 1, 8, A);
all_tuples!(impl_two_query_family_b4, 1, 8, A);

impl<F, Ready, Marker, T0, T1, V0, V1>
    IntoSystem<(
        TwoQueriesTwoViewsCommands,
        Entity,
        With<Ready>,
        (Entity, &T0, &T1),
        With<Marker>,
        V0,
        V1,
    )> for F
where
    F: Send + Sync + 'static,
    Ready: crate::Component,
    Marker: crate::Component,
    T0: crate::Component,
    T1: crate::Component,
    V0: QueryParam + Send + Sync + 'static,
    V1: QueryParam + Send + Sync + 'static,
    for<'a> F: AsyncFn(
        Query<Entity, With<Ready>>,
        Query<(Entity, &'a T0, &'a T1), With<Marker>>,
        View<'a, V0>,
        View<'a, V1>,
        Commands,
    ),
{
    fn into_system(self, id: SystemId) -> BoxedSystem {
        BoxedSystem(Arc::new(TwoQueriesTwoViewsSystem {
            id,
            function: self,
            _marker: PhantomData::<(
                Entity,
                With<Ready>,
                (Entity, &T0, &T1),
                With<Marker>,
                V0,
                V1,
            )>,
        }))
    }
}

struct QueryViewSystem<F, Q, Filter, V> {
    id: SystemId,
    function: F,
    _marker: PhantomData<(Q, Filter, V)>,
}

impl<F, Q, Filter, V> Runnable for QueryViewSystem<F, Q, Filter, V>
where
    F: Send + Sync + 'static,
    Q: QueryParam + Send + Sync + 'static,
    Filter: QueryFilter<Q> + Send + Sync + 'static,
    V: QueryParam + Send + Sync + 'static,
    for<'a> F: AsyncFn(Query<Q::Item<'a>, Filter>, View<'a, V>, Commands),
{
    /// Runs every memo-invalid row of `Q` and passes an ambient `View<V>`.
    ///
    /// The view is fetched from the same immutable snapshot, but its rows are
    /// not added to `deps`. This preserves the `Query = tracked`, `View =
    /// ambient` distinction.
    fn run<'a>(
        &'a self,
        snapshot: &'a Snapshot,
        memo: &'a mut HashMap<SystemInvocation, MemoEntry>,
    ) -> LocalBoxFuture<'a, Vec<SystemOutput>> {
        async move {
            let mut outputs = Vec::new();

            for row in filtered_rows::<Q, Filter>(snapshot) {
                let owner = SystemInvocation {
                    system: self.id,
                    keys: Q::keys(&row),
                };
                let deps = filtered_deps::<Q, Filter>(snapshot, &row);

                if memo.get(&owner).is_some_and(|entry| entry.deps == deps) {
                    continue;
                }

                let commands = Commands::new();
                (self.function)(
                    Query::new(Q::fetch(snapshot, &row)),
                    View::<V>::new(snapshot),
                    commands.clone(),
                )
                .await;

                outputs.push(SystemOutput {
                    owner: owner.clone(),
                    commands: commands.take(),
                });
                memo.insert(owner, MemoEntry { deps });
            }

            outputs
        }
        .boxed_local()
    }
}

macro_rules! impl_query_view_system {
    ($($T:ident),*) => {
        impl<F, V, $($T: crate::Component),*> IntoSystem<(QueryViewCommands, (Entity, $(& $T,)*), V)> for F
        where
            F: Send + Sync + 'static,
            V: QueryParam + Send + Sync + 'static,
            for<'a> F: AsyncFn(Query<(Entity, $(&'a $T,)*)>, View<'a, V>, Commands),
        {
            fn into_system(self, id: SystemId) -> BoxedSystem {
                BoxedSystem(Arc::new(QueryViewSystem {
                    id,
                    function: self,
                    _marker: PhantomData::<((Entity, $(& $T,)*), (), V)>,
                }))
            }
        }
    };
}

all_tuples!(impl_query_view_system, 1, 8, T);

macro_rules! impl_query_with_view_system {
    ($($T:ident),*) => {
        impl<F, Marker, V, $($T: crate::Component),*> IntoSystem<(QueryViewCommands, (Entity, $(& $T,)*), With<Marker>, V)> for F
        where
            F: Send + Sync + 'static,
            Marker: crate::Component,
            V: QueryParam + Send + Sync + 'static,
            for<'a> F: AsyncFn(Query<(Entity, $(&'a $T,)*), With<Marker>>, View<'a, V>, Commands),
        {
            fn into_system(self, id: SystemId) -> BoxedSystem {
                BoxedSystem(Arc::new(QueryViewSystem {
                    id,
                    function: self,
                    _marker: PhantomData::<((Entity, $(& $T,)*), With<Marker>, V)>,
                }))
            }
        }
    };
}

all_tuples!(impl_query_with_view_system, 1, 8, T);

struct QueryTwoViewsSystem<F, Q, Filter, V0, V1> {
    id: SystemId,
    function: F,
    _marker: PhantomData<(Q, Filter, V0, V1)>,
}

impl<F, Q, Filter, V0, V1> Runnable for QueryTwoViewsSystem<F, Q, Filter, V0, V1>
where
    F: Send + Sync + 'static,
    Q: QueryParam + Send + Sync + 'static,
    Filter: QueryFilter<Q> + Send + Sync + 'static,
    V0: QueryParam + Send + Sync + 'static,
    V1: QueryParam + Send + Sync + 'static,
    for<'a> F: AsyncFn(Query<Q::Item<'a>, Filter>, View<'a, V0>, View<'a, V1>, Commands),
{
    fn run<'a>(
        &'a self,
        snapshot: &'a Snapshot,
        memo: &'a mut HashMap<SystemInvocation, MemoEntry>,
    ) -> LocalBoxFuture<'a, Vec<SystemOutput>> {
        async move {
            let mut outputs = Vec::new();

            for row in filtered_rows::<Q, Filter>(snapshot) {
                let owner = SystemInvocation {
                    system: self.id,
                    keys: Q::keys(&row),
                };
                let deps = filtered_deps::<Q, Filter>(snapshot, &row);

                if memo.get(&owner).is_some_and(|entry| entry.deps == deps) {
                    continue;
                }

                let commands = Commands::new();
                (self.function)(
                    Query::new(Q::fetch(snapshot, &row)),
                    View::<V0>::new(snapshot),
                    View::<V1>::new(snapshot),
                    commands.clone(),
                )
                .await;

                outputs.push(SystemOutput {
                    owner: owner.clone(),
                    commands: commands.take(),
                });
                memo.insert(owner, MemoEntry { deps });
            }

            outputs
        }
        .boxed_local()
    }
}

macro_rules! impl_query_two_views_system {
    ($($T:ident),*) => {
        impl<F, V0, V1, $($T: crate::Component),*> IntoSystem<(QueryTwoViewsCommands, (Entity, $(& $T,)*), V0, V1)> for F
        where
            F: Send + Sync + 'static,
            V0: QueryParam + Send + Sync + 'static,
            V1: QueryParam + Send + Sync + 'static,
            for<'a> F: AsyncFn(Query<(Entity, $(&'a $T,)*)>, View<'a, V0>, View<'a, V1>, Commands),
        {
            fn into_system(self, id: SystemId) -> BoxedSystem {
                BoxedSystem(Arc::new(QueryTwoViewsSystem {
                    id,
                    function: self,
                    _marker: PhantomData::<((Entity, $(& $T,)*), (), V0, V1)>,
                }))
            }
        }
    };
}

all_tuples!(impl_query_two_views_system, 1, 8, T);

macro_rules! impl_query_with_two_views_system {
    ($($T:ident),*) => {
        impl<F, Marker, V0, V1, $($T: crate::Component),*> IntoSystem<(QueryTwoViewsCommands, (Entity, $(& $T,)*), With<Marker>, V0, V1)> for F
        where
            F: Send + Sync + 'static,
            Marker: crate::Component,
            V0: QueryParam + Send + Sync + 'static,
            V1: QueryParam + Send + Sync + 'static,
            for<'a> F: AsyncFn(Query<(Entity, $(&'a $T,)*), With<Marker>>, View<'a, V0>, View<'a, V1>, Commands),
        {
            fn into_system(self, id: SystemId) -> BoxedSystem {
                BoxedSystem(Arc::new(QueryTwoViewsSystem {
                    id,
                    function: self,
                    _marker: PhantomData::<((Entity, $(& $T,)*), With<Marker>, V0, V1)>,
                }))
            }
        }
    };
}

all_tuples!(impl_query_with_two_views_system, 1, 8, T);
