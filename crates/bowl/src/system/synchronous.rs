//! Inferred synchronous functions; the normal worker path calls them directly.
use super::*;

#[doc(hidden)]
pub struct SynchronousMarker;

pub(crate) trait SyncParamFunction<Marker>: Send + Sync + 'static {
    type Param: SystemParam;
    fn call<'a>(&'a self, params: <Self::Param as SystemParam>::Item<'a>);
}

macro_rules! impl_sync_function {
    ($($P:ident),+) => {
        impl<F, $($P),+> SyncParamFunction<fn($($P),+)> for F
        where
            F: Fn($($P),+) + Send + Sync + 'static,
            $($P: SystemParam + 'static,)+
            for<'a> F: Fn($($P::Item<'a>),+),
        {
            type Param = ($($P,)+);
            fn call<'a>(&'a self, params: <Self::Param as SystemParam>::Item<'a>) {
                #[allow(non_snake_case)]
                let ($($P,)+) = params;
                self($($P),+);
            }
        }
    };
}

impl_sync_function!(P0);
impl_sync_function!(P0, P1);
impl_sync_function!(P0, P1, P2);
impl_sync_function!(P0, P1, P2, P3);
impl_sync_function!(P0, P1, P2, P3, P4);
impl_sync_function!(P0, P1, P2, P3, P4, P5);
impl_sync_function!(P0, P1, P2, P3, P4, P5, P6);
impl_sync_function!(P0, P1, P2, P3, P4, P5, P6, P7);

impl<F, Marker> SystemParamFunction<(SynchronousMarker, Marker)> for F
where
    F: SyncParamFunction<Marker>,
    F::Param: 'static,
{
    type Param = F::Param;

    fn plan_execution(
        system: &FunctionSystem<Self, (SynchronousMarker, Marker)>, bowl: Bowl, snapshot: Arc<Snapshot>,
        memo: &HashMap<SystemInvocation, MemoEntry>, hint: Option<&[Entity]>,
        buffers: &mut PlanningStorage,
    ) -> Box<dyn crate::dispatch::RunPlan>
    where (SynchronousMarker, Marker): Send + Sync + 'static, Self::Param: Send + Sync + 'static,
    {
        crate::dispatch::prepare_calls(system.plan_sync_rows(bowl, snapshot, memo, hint, buffers))
    }

    fn run_sync<'a>(&'a self, params: <Self::Param as SystemParam>::Item<'a>) {
        self.call(params);
    }

    fn run<'a>(&'a self, params: <Self::Param as SystemParam>::Item<'a>)
        -> impl Future<Output = ()> + Send + 'a {
        // Compatibility path for grouped/settle execution.
        async move { self.call(params); }
    }
}

impl<F, Marker> FunctionSystem<F, Marker>
where
    Marker: Send + Sync + 'static,
    F: SystemParamFunction<Marker>,
    F::Param: Send + Sync + 'static,
{
    pub(super) fn plan_sync_rows(
        &self,
        bowl: Bowl,
        snapshot: Arc<Snapshot>,
        memo: &HashMap<SystemInvocation, MemoEntry>,
        hint: Option<&[Entity]>,
        buffers: &mut PlanningStorage,
    ) -> impl ExactSizeIterator<
        Item = PlannedSystemRun<impl FnOnce() -> SystemRun + Send + 'static>,
    > + Send
    + 'static {
        let function = Arc::clone(&self.function);
        let prepared = F::Param::prepare(&snapshot);
        let planned = prepare_invocations::<F::Param>(
            self.id, &snapshot, memo, hint, &prepared, Some(buffers),
        );
        // Immutable plan context needs one shared handle per row. Keep the
        // Bowl and callee here too, avoiding their per-row reference counts.
        let context = Arc::new((bowl, snapshot, prepared, function));
        planned
            .invocations
            .into_iter()
            .map(move |mut invocation| {
                let owner = invocation.owner.clone();
                // Reservations own access metadata; execution only needs the
                // rows whose revisions must be reconciled after MutRef writes.
                let access = invocation.access;
                let writes = written_rows(&access);
                let context = Arc::clone(&context);
                let run = move || {
                    let (bowl, snapshot, prepared, function) = context.as_ref();
                    let mut guards = invocation.commands.take_guards();
                    let commands = Commands::with_queue(
                        snapshot.spawn_slots(&invocation.owner),
                        snapshot.entity_allocator(),
                        invocation.commands,
                    );
                    // Read guards live here, in the invocation frame, so
                    // borrows handed to the system stay locked until the
                    // system function returns.
                    let params =
                        prepared.fetch(bowl, snapshot, &invocation.state, &commands, &mut guards);
                    function.run_sync(params);
                    guards.clear();

                    let (mut output, memo_update) = finish_invocation(
                        invocation.owner,
                        invocation.deps,
                        snapshot.revision_raw(),
                        commands,
                    );
                    output.commands.recycle_guards(guards);

                    SystemRun {
                        completed: true,
                        outputs: vec![output],
                        memo_updates: vec![memo_update],
                        writes,
                    }
                };

                PlannedSystemRun { owner, access, run }
            })
    }
}

#[cfg(test)]
mod tests;
