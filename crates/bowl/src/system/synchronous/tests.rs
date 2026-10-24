use crate::{Bowl, Commands, Component, Entity, MutRef, Query, SystemExt, declare::Anything};
use futures::{FutureExt, channel::oneshot, future::{BoxFuture, Shared, Either}};

struct Input(u64);
struct Derived(u64);
struct Counter(u64);
struct Hook;
impl Component for Input {}
impl Component for Derived {}
impl Component for Counter {}
impl Component for Hook {}

fn derive(rows: Query<(Entity, &Input)>, mut commands: Commands<Anything>) {
    let (entity, input) = rows.item();
    commands.entity(entity).insert(Derived(input.0 * 2));
}

fn increment(rows: Query<(Entity, MutRef<'_, Counter>)>) {
    let (_, mut counter) = rows.item();
    counter.0 += 1;
}

async fn check_mutation_and_memo() {
    let bowl = Bowl::builder().system(derive).system(increment).build();
    let entity = bowl.insert((Input(3), Counter(0))).await.entity();
    assert_eq!(bowl.scoop::<Query<&Derived>>().await.collect()[0].0, 6);
    assert_eq!(bowl.scoop::<Query<&Counter>>().await.collect()[0].0, 1);
    bowl.entity(entity).insert((Input(9),)).await;
    assert_eq!(bowl.scoop::<Query<&Derived>>().await.collect()[0].0, 18);
    assert_eq!(bowl.scoop::<Query<&Counter>>().await.collect()[0].0, 1);
}

#[test]
fn sync_mutation_and_derived_outputs_keep_memo_semantics() {
    futures::executor::block_on(check_mutation_and_memo());
    #[cfg(feature = "parallel")]
    tokio::runtime::Builder::new_multi_thread().worker_threads(2).build().unwrap()
        .block_on(check_mutation_and_memo());
}

#[test]
fn synchronous_functions_work_through_grouped_hook_fallback() {
    futures::executor::block_on(async {
        let bowl = Bowl::builder().system(derive
            .on_complete(|mut commands: Commands<Anything>| { commands.insert((Hook,)); })).build();
        bowl.insert((Input(4),)).await;
        assert_eq!(bowl.scoop::<Query<&Derived>>().await.collect()[0].0, 8);
        assert_eq!(bowl.scoop::<Query<&Hook>>().await.len(), 1);
    });
}

struct Gate(Shared<BoxFuture<'static, ()>>);
struct Signal(Option<oneshot::Sender<()>>);
struct Finished;
impl Component for Gate {}
impl Component for Signal {}
impl Component for Finished {}

async fn wait_for_gate(rows: Query<&Gate>, mut commands: Commands<Anything>) {
    rows.item().0.clone().await;
    commands.insert((Finished,));
}

fn notify_after_publication(rows: Query<(Entity, &Derived, MutRef<'_, Signal>)>) {
    let (_, _, mut signal) = rows.item();
    if let Some(sender) = signal.0.take() { sender.send(()).unwrap(); }
}

#[test]
#[cfg(feature = "parallel")]
fn sync_publication_progresses_while_an_async_sibling_is_pending() {
    tokio::runtime::Builder::new_multi_thread().worker_threads(2).build().unwrap().block_on(async {
        let (release, gate) = oneshot::channel();
        let (signal, observed) = oneshot::channel();
        let bowl = Bowl::builder().system(wait_for_gate)
            .system(derive).system(notify_after_publication).build();
        bowl.insert((Gate(gate.map(|result| result.unwrap()).boxed().shared()),)).await;
        bowl.insert((Input(5), Signal(Some(signal)))).await;
        let driver = Box::pin(bowl.stir());
        let driver = match futures::future::select(driver, observed).await {
            Either::Right((result, driver)) => { result.unwrap(); driver }
            Either::Left(_) => panic!("settled before the async gate was released"),
        };
        release.send(()).unwrap();
        driver.await;
        assert_eq!(bowl.scoop::<Query<&Finished>>().await.len(), 1);
    });
}
