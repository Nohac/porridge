//! Fixed purchase traffic competing with a persistent monitoring pipeline.
use std::{any::{TypeId, type_name}, time::{Duration, Instant}};
use bowl::{Bowl, BowlBuilder, Commands, Component, Entity, Query, Schema, ShapeDesc};
use super::purchases::{self, Inventory, KernelMode, Outcome, Owner, Purchase, Sequence, Wallet};

#[derive(Component, Hash)]
#[component(hash)]
pub struct Device(pub usize);

#[derive(Component, Hash, Clone, Copy, Debug, PartialEq, Eq)]
#[component(hash)]
pub struct Condition { pub wear: u64, pub load: u64 }

#[derive(Component, Hash, Debug, PartialEq, Eq)]
#[component(hash)]
pub struct Assessment(pub u64);

#[derive(Component, Hash, Debug, PartialEq, Eq)]
#[component(hash)]
pub struct Maintenance(pub bool);

#[derive(Schema)]
struct FleetSchema {
    device: (Device, Condition, Option<Assessment>, Option<Maintenance>),
}

fn assess(rows: Query<(Entity<fleet_schema::Device>, &Condition)>, mut commands: Commands<(fleet_schema::Device,)>) {
    let (entity, condition) = rows.item();
    commands.entity(entity).insert(Assessment(condition.wear * 3 + condition.load));
}

fn maintain(rows: Query<(Entity<fleet_schema::Device>, &Assessment)>, mut commands: Commands<(fleet_schema::Device,)>) {
    let (entity, assessment) = rows.item();
    commands.entity(entity).insert(Maintenance(assessment.0 >= 180));
}

async fn assess_async(rows: Query<(Entity<fleet_schema::Device>, &Condition)>, commands: Commands<(fleet_schema::Device,)>) {
    assess(rows, commands);
}

async fn maintain_async(rows: Query<(Entity<fleet_schema::Device>, &Assessment)>, commands: Commands<(fleet_schema::Device,)>) {
    maintain(rows, commands);
}

// Different monomorphizations are distinct schema types and distinct observers.
struct Background<const N: usize>(u64);
impl<const N: usize> Component for Background<N> {}
struct BackgroundSchema<const N: usize>;
impl<const N: usize> Schema for BackgroundSchema<N> {
    fn shapes() -> Vec<ShapeDesc> {
        vec![ShapeDesc { name: type_name::<Self>(), required: vec![(TypeId::of::<Background<N>>(), type_name::<Background<N>>())], optional: vec![] }]
    }
}

fn observe<const N: usize>(rows: Query<&Background<N>>) {
    std::hint::black_box(rows.item().0);
}

macro_rules! background {
    ($builder:ident, $config:ident; $($n:literal),*) => {
        $(if $n < $config.extra_types {
            $builder = $builder.schema::<BackgroundSchema<$n>>();
            if $config.observers { $builder = $builder.system(observe::<$n>); }
        })*
    };
}

macro_rules! populate_background {
    ($bowl:ident, $config:ident; $($n:literal),*) => {
        $(if $n < $config.extra_types {
            for row in 0..$config.rows_per_type { $bowl.insert((Background::<$n>(row as u64),)).await; }
        })*
    };
}

#[derive(Clone, Copy, Debug)]
pub struct Config {
    pub active: usize,
    pub dormant: usize,
    pub devices: usize,
    pub updates: usize,
    pub extra_types: usize,
    pub rows_per_type: usize,
    pub observers: bool,
    pub mode: KernelMode,
    pub workers: usize,
}

pub struct Population {
    pub bowl: Bowl,
    config: Config,
    requests: Vec<Entity>,
    devices: Vec<Entity>,
    rounds: u64,
    updates: usize,
}

pub struct Sample {
    pub submit: Duration,
    pub settle: Duration,
    pub total: Duration,
}

fn condition(changes: usize) -> Condition {
    Condition { wear: (changes % 101) as u64, load: ((changes % 101) * 7 % 101) as u64 }
}

impl Population {
    pub async fn new(config: Config) -> Self {
        assert!(config.active > 0 && config.updates <= config.devices);
        assert!(config.extra_types <= 64, "this fixture instantiates 64 distinct optional schema types");
        let mut builder: BowlBuilder = purchases::builder(config.mode).schema::<FleetSchema>();
        builder = match config.mode {
            KernelMode::Async => builder.system(assess_async).system(maintain_async),
            KernelMode::Sync => builder.system(assess).system(maintain),
        };
        background!(builder, config; 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63);
        let bowl = builder.build();
        bowl.set_system_profiling(false);
        bowl.set_dispatch_batch_size(16);
        purchases::populate(&bowl, config.active.checked_add(config.dormant).expect("account count overflow"), u64::MAX, u64::MAX).await;
        let mut requests = Vec::with_capacity(config.active);
        for owner in 0..config.active {
            requests.push(bowl.insert((Owner(owner), Purchase { sequence: 0, price: 1, work: 0 }, Outcome(Some(false)))).await.entity());
        }
        let mut devices = Vec::with_capacity(config.devices);
        for id in 0..config.devices {
            devices.push(bowl.insert((Device(id), condition(0))).await.entity());
        }
        populate_background!(bowl, config; 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63);
        bowl.stir().await;
        Self { bowl, config, requests, devices, rounds: 0, updates: 0 }
    }

    /// Includes ordinary input submission, settlement and purchase reply materialization.
    pub async fn round(&mut self) -> Sample {
        let start = Instant::now();
        self.rounds = self.rounds.checked_add(1).expect("round overflow");
        for &entity in &self.requests {
            self.bowl.entity(entity).insert((Purchase { sequence: self.rounds, price: 1, work: 0 }, Outcome(None))).await;
        }
        for _ in 0..self.config.updates {
            let index = self.updates % self.devices.len();
            let changes = self.updates / self.devices.len() + 1;
            self.bowl.entity(self.devices[index]).insert((condition(changes),)).await;
            self.updates = self.updates.checked_add(1).expect("update count overflow");
        }
        let submit = start.elapsed();
        let settle_start = Instant::now();
        let replies = self.bowl.scoop::<Query<&Outcome>>().await;
        std::hint::black_box(replies.collect());
        Sample { submit, settle: settle_start.elapsed(), total: start.elapsed() }
    }

    /// Deliberately outside timed intervals. Also checks dormant data stayed intact.
    pub async fn verify(&self) {
        let accounts = self.bowl.scoop::<Query<(Entity, &Owner, &Wallet, &Inventory, &Sequence)>>().await;
        assert_eq!(accounts.len(), self.config.active + self.config.dormant);
        for (_, owner, wallet, inventory, sequence) in accounts.collect() {
            let count = if owner.0 < self.config.active { self.rounds } else { 0 };
            assert_eq!(wallet.0, u64::MAX - count);
            assert_eq!(inventory.units, count);
            assert_eq!(sequence.0, count);
        }
        let rows = self.bowl.scoop::<Query<(Entity, &Device, &Condition, &Assessment, &Maintenance)>>().await;
        assert_eq!(rows.len(), self.config.devices);
        for (_, device, actual, assessment, maintenance) in rows.collect() {
            let changes = self.updates / self.config.devices + usize::from(device.0 < self.updates % self.config.devices);
            let expected = condition(changes);
            assert_eq!(*actual, expected);
            assert_eq!(assessment.0, expected.wear * 3 + expected.load);
            assert_eq!(maintenance.0, assessment.0 >= 180);
        }
        let replies = self.bowl.scoop::<Query<&Outcome>>().await;
        assert_eq!(replies.len(), self.config.active);
        for reply in replies.collect() { assert_eq!(reply.0, Some(true)); }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sparse_updates_rotate_while_dormant_accounts_and_background_stay_settled() {
        for mode in [KernelMode::Async, KernelMode::Sync] {
            for workers in [1, 4] {
                tokio::runtime::Builder::new_multi_thread().worker_threads(workers).build().unwrap().block_on(async {
                    let config = Config { active: 4, dormant: 9, devices: 7, updates: 2, extra_types: 3, rows_per_type: 4, observers: true, mode, workers };
                    let mut population = Population::new(config).await;
                    for _ in 0..12 {
                        population.round().await;
                        population.verify().await;
                    }
                });
            }
        }
    }
}
