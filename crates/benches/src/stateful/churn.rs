//! Equal live populations with independently controlled retired-entity history.
use bowl::{Bowl, Component, Entity, Query, Schema};

#[derive(Component)]
struct Retired;

#[derive(Component)]
struct Resident;

#[derive(Component)]
struct Value(u64);

#[derive(Schema)]
struct ChurnSchema {
    transient: (Retired,),
    resident: (Resident, Value),
}

pub async fn create(live: usize, history: usize) -> Bowl {
    let bowl = Bowl::builder().schema::<ChurnSchema>().build();
    // Bounded setup batches avoid retaining all historical objects at once.
    // The batch size is a setup detail, not an input-size limit.
    for start in (0..history).step_by(256) {
        for _ in start..history.min(start.saturating_add(256)) {
            bowl.insert((Retired,)).await;
        }
        bowl.scoop::<Query<(Entity, &Retired)>>().drain().await;
    }
    for value in 0..live {
        bowl.insert((Resident, Value(value as u64))).await;
    }
    scan(&bowl).await;
    bowl
}

/// A read-only operation: repeated measurement does not grow entity history.
pub async fn scan_store(bowl: &Bowl) -> usize {
    let rows = bowl.scoop::<Query<(Entity, &Resident, &Value)>>().await;
    let mut count = 0;
    for (_, _, value) in rows.collect() {
        std::hint::black_box(value.0);
        count += 1;
    }
    count
}

/// Typed facet enumeration exercises the historical presence-mask scan.
pub async fn scan(bowl: &Bowl) -> usize {
    let rows = bowl
        .scoop::<Query<(Entity<churn_schema::Resident>, &Resident, &Value)>>()
        .await;
    let mut count = 0;
    for (_, _, value) in rows.collect() {
        std::hint::black_box(value.0);
        count += 1;
    }
    count
}
