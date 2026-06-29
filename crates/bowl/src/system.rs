use std::{collections::HashMap, marker::PhantomData, sync::Arc};

use futures::future::{FutureExt, LocalBoxFuture};

use crate::{
    Commands, Entity, Query, View,
    commands::CommandOp,
    query::{Dep, QueryParam},
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
pub struct QueryViewCommands;
pub struct QueryTwoViewsCommands;

struct QuerySystem<F, Q> {
    id: SystemId,
    function: F,
    _marker: PhantomData<Q>,
}

impl<F, Q> Runnable for QuerySystem<F, Q>
where
    F: Send + Sync + 'static,
    Q: QueryParam + Send + Sync + 'static,
    for<'a> F: AsyncFn(Query<Q::Item<'a>>, Commands),
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

            for row in Q::rows(snapshot) {
                let owner = SystemInvocation {
                    system: self.id,
                    keys: Q::keys(&row),
                };
                let deps = Q::deps(snapshot, &row);

                if memo.get(&owner).is_some_and(|entry| entry.deps == deps) {
                    continue;
                }

                let commands = Commands::new();
                (self.function)(Query(Q::fetch(snapshot, &row)), commands.clone()).await;

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
                    _marker: PhantomData::<(Entity, $(& $T,)*)>,
                }))
            }
        }
    };
}

all_tuples!(impl_query_system, 1, 8, T);

struct QueryViewSystem<F, Q, V> {
    id: SystemId,
    function: F,
    _marker: PhantomData<(Q, V)>,
}

impl<F, Q, V> Runnable for QueryViewSystem<F, Q, V>
where
    F: Send + Sync + 'static,
    Q: QueryParam + Send + Sync + 'static,
    V: QueryParam + Send + Sync + 'static,
    for<'a> F: AsyncFn(Query<Q::Item<'a>>, View<'a, V>, Commands),
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

            for row in Q::rows(snapshot) {
                let owner = SystemInvocation {
                    system: self.id,
                    keys: Q::keys(&row),
                };
                let deps = Q::deps(snapshot, &row);

                if memo.get(&owner).is_some_and(|entry| entry.deps == deps) {
                    continue;
                }

                let commands = Commands::new();
                (self.function)(
                    Query(Q::fetch(snapshot, &row)),
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
                    _marker: PhantomData::<((Entity, $(& $T,)*), V)>,
                }))
            }
        }
    };
}

all_tuples!(impl_query_view_system, 1, 8, T);

struct QueryTwoViewsSystem<F, Q, V0, V1> {
    id: SystemId,
    function: F,
    _marker: PhantomData<(Q, V0, V1)>,
}

impl<F, Q, V0, V1> Runnable for QueryTwoViewsSystem<F, Q, V0, V1>
where
    F: Send + Sync + 'static,
    Q: QueryParam + Send + Sync + 'static,
    V0: QueryParam + Send + Sync + 'static,
    V1: QueryParam + Send + Sync + 'static,
    for<'a> F: AsyncFn(Query<Q::Item<'a>>, View<'a, V0>, View<'a, V1>, Commands),
{
    fn run<'a>(
        &'a self,
        snapshot: &'a Snapshot,
        memo: &'a mut HashMap<SystemInvocation, MemoEntry>,
    ) -> LocalBoxFuture<'a, Vec<SystemOutput>> {
        async move {
            let mut outputs = Vec::new();

            for row in Q::rows(snapshot) {
                let owner = SystemInvocation {
                    system: self.id,
                    keys: Q::keys(&row),
                };
                let deps = Q::deps(snapshot, &row);

                if memo.get(&owner).is_some_and(|entry| entry.deps == deps) {
                    continue;
                }

                let commands = Commands::new();
                (self.function)(
                    Query(Q::fetch(snapshot, &row)),
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
                    _marker: PhantomData::<((Entity, $(& $T,)*), V0, V1)>,
                }))
            }
        }
    };
}

all_tuples!(impl_query_two_views_system, 1, 8, T);
