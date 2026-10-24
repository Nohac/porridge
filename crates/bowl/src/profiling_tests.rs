use crate::{Bowl, Commands, Component, Entity, Query, declare::Anything};

struct Input(u64);
struct Output(u64);

impl Component for Input {}
impl Component for Output {}

fn derive(input: Query<(Entity, &Input)>, mut commands: Commands<Anything>) {
    let (entity, input) = input.item();
    commands.entity(entity).insert(Output(input.0 * 2));
}

async fn derive_async(input: Query<(Entity, &Input)>, commands: Commands<Anything>) {
    derive(input, commands);
}

async fn check_collection_toggle() {
    for asynchronous in [false, true] {
        let builder = Bowl::builder();
        let bowl = if asynchronous { builder.system(derive_async) } else { builder.system(derive) }.build();
        assert!(bowl.system_profiling_enabled());
        let entity = bowl.insert((Input(1),)).await.entity();
        assert_eq!(bowl.scoop::<Query<&Output>>().await.collect()[0].0, 2);
        let before = bowl.profile_all().await.remove(0);
        assert_eq!(before.runs, 1);

        bowl.set_system_profiling(false);
        assert!(!bowl.system_profiling_enabled());
        bowl.entity(entity).insert((Input(2),)).await;
        assert_eq!(bowl.scoop::<Query<&Output>>().await.collect()[0].0, 4);
        let disabled = bowl.profile_all().await.remove(0);
        assert_eq!((disabled.runs, disabled.plan_nanos, disabled.run_nanos),
            (before.runs, before.plan_nanos, before.run_nanos));

        bowl.set_system_profiling(true);
        bowl.entity(entity).insert((Input(3),)).await;
        assert_eq!(bowl.scoop::<Query<&Output>>().await.collect()[0].0, 6);
        let resumed = bowl.profile_all().await.remove(0);
        assert_eq!(resumed.runs, 2);
        assert!(resumed.plan_nanos >= before.plan_nanos);
        assert!(resumed.run_nanos >= before.run_nanos);
    }
}

#[test]
fn system_profiling_can_pause_and_resume_without_affecting_results() {
    futures::executor::block_on(check_collection_toggle());
    #[cfg(feature = "parallel")]
    tokio::runtime::Builder::new_multi_thread().worker_threads(2).build().unwrap()
        .block_on(check_collection_toggle());
}
