use super::*;
use crate::{Bowl, Commands, Query, declare::Anything};

struct Left(u64);
struct Middle(u64);
struct Right(u64);
struct Answer(u64);
impl Component for Left {}
impl Component for Middle {}
impl Component for Right {}
impl Component for Answer {}

async fn derive_three_keys(
    left: Query<&Left>, middle: Query<&Middle>, right: Query<&Right>,
    mut commands: Commands<Anything>,
) {
    commands.insert((Answer(left.item().0 + 10 * middle.item().0 + 100 * right.item().0),));
}

#[test]
fn wide_invocation_keys_preserve_all_product_rows_and_output_ownership() {
    futures::executor::block_on(async {
        let bowl = Bowl::builder().system(derive_three_keys).build();
        let left = bowl.insert((Left(1),)).await.entity();
        bowl.insert((Left(2),)).await;
        bowl.insert((Middle(3),)).await;
        bowl.insert((Right(4),)).await;
        bowl.insert((Right(5),)).await;
        let read = async || {
            let result = bowl.scoop::<Query<&Answer>>().await;
            let mut values: Vec<_> = result.collect().into_iter().map(|value| value.0).collect();
            values.sort_unstable();
            values
        };
        assert_eq!(read().await, [431, 432, 531, 532]);
        assert_eq!(read().await, [431, 432, 531, 532]);
        bowl.entity(left).insert((Left(9),)).await;
        assert_eq!(read().await, [432, 439, 532, 539]);
    });
}

#[test]
fn report_invocation_key_layout() {
    println!("invocation_layout keys={} vec={} owner={} optional_owner={} optional_vec_owner={}",
        std::mem::size_of::<InvocationKeys>(), std::mem::size_of::<Vec<Entity>>(),
        std::mem::size_of::<SystemInvocation>(), std::mem::size_of::<Option<SystemInvocation>>(),
        std::mem::size_of::<Option<(SystemId, Vec<Entity>)>>());
    // Protect the measured common descriptor size on the current 64-bit target.
    // Optional owner layout and whole-workload memory are measured separately.
    #[cfg(target_pointer_width = "64")]
    assert_eq!(std::mem::size_of::<InvocationKeys>(), std::mem::size_of::<Vec<Entity>>());
}
