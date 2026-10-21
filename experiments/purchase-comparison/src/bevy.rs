//! Ordinary Bevy systems and deferred commands; no custom task-per-request loop.
use crate::{AccountState, Purchase, Response};
use bevy_ecs::{prelude::*, schedule::MultiThreadedExecutor};
use bevy_tasks::{ComputeTaskPool, TaskPoolBuilder};
use fixtures::stateful::purchases::{Inventory, Wallet};

pub fn init_workers(workers: usize) {
    assert!(workers > 0);
    let pool = ComputeTaskPool::get_or_init(|| TaskPoolBuilder::new().num_threads(workers).build());
    assert_eq!(
        pool.thread_num(),
        workers,
        "Bevy worker count is process-global; use a fresh process"
    );
}

#[derive(Component)]
struct Owner(usize);

#[derive(Component)]
struct Balance(Wallet);

#[derive(Component)]
struct Stock(Inventory);

#[derive(Component)]
struct Sequence(u64);

#[derive(Component)]
struct Request(Purchase);

#[derive(Component)]
struct Outcome(Option<bool>);

#[derive(Component)]
struct Debit {
    observed: u64,
    next: Option<u64>,
}

#[derive(Component)]
struct Grant {
    observed: Inventory,
    next: Option<u64>,
}

#[derive(Resource)]
struct Accounts(Vec<Entity>);

// Same optional synthetic arithmetic as the existing purchase fixture.
fn compute(work: u32) {
    let mut value = 1_u64;
    for _ in 0..work {
        value = std::hint::black_box(value.wrapping_mul(6364136223846793005).wrapping_add(1));
    }
    std::hint::black_box(value);
}

fn validate_wallet(
    requests: Query<(Entity, &Owner, &Request, &Outcome)>,
    accounts: Res<Accounts>,
    wallets: Query<&Balance>,
    mut commands: Commands,
) {
    for (entity, owner, request, outcome) in &requests {
        if outcome.0.is_some() {
            continue;
        }
        let Some(account) = accounts.0.get(owner.0) else {
            continue;
        };
        let Ok(wallet) = wallets.get(*account) else {
            continue;
        };
        compute(request.0.work);
        commands.entity(entity).insert(Debit {
            observed: wallet.0.0,
            next: wallet.0.0.checked_sub(request.0.price),
        });
    }
}

fn validate_inventory(
    requests: Query<(Entity, &Owner, &Request, &Outcome)>,
    accounts: Res<Accounts>,
    inventories: Query<&Stock>,
    mut commands: Commands,
) {
    for (entity, owner, request, outcome) in &requests {
        if outcome.0.is_some() {
            continue;
        }
        let Some(account) = accounts.0.get(owner.0) else {
            continue;
        };
        let Ok(inventory) = inventories.get(*account) else {
            continue;
        };
        compute(request.0.work);
        commands.entity(entity).insert(Grant {
            observed: inventory.0.clone(),
            next: inventory
                .0
                .units
                .checked_add(1)
                .filter(|units| *units <= inventory.0.capacity),
        });
    }
}

fn apply(
    mut requests: Query<(&Owner, &Request, &Debit, &Grant, &mut Outcome)>,
    accounts: Res<Accounts>,
    mut players: Query<(&mut Balance, &mut Stock, &mut Sequence)>,
) {
    for (owner, request, debit, grant, mut outcome) in &mut requests {
        if outcome.0.is_some() {
            continue;
        }
        let Some(account) = accounts.0.get(owner.0) else {
            continue;
        };
        let Ok((mut wallet, mut inventory, mut sequence)) = players.get_mut(*account) else {
            continue;
        };
        if request.0.sequence <= sequence.0 {
            outcome.0 = Some(false);
            continue;
        }
        if sequence.0.checked_add(1) != Some(request.0.sequence) {
            continue;
        }
        if debit.observed != wallet.0.0 || grant.observed != inventory.0 {
            continue;
        }

        let accepted = match (debit.next, grant.next) {
            (Some(balance), Some(units)) => {
                wallet.0.0 = balance;
                inventory.0.units = units;
                true
            }
            _ => false,
        };
        sequence.0 = request.0.sequence;
        outcome.0 = Some(accepted);
    }
}

pub struct Engine {
    world: World,
    schedule: Schedule,
    pending: usize,
}

impl Engine {
    pub fn new(owners: usize, balance: u64, capacity: u64) -> Self {
        let mut world = World::new();
        let accounts = (0..owners)
            .map(|owner| {
                world
                    .spawn((
                        Owner(owner),
                        Balance(Wallet(balance)),
                        Stock(Inventory { units: 0, capacity }),
                        Sequence(0),
                    ))
                    .id()
            })
            .collect();
        world.insert_resource(Accounts(accounts));
        let mut schedule = Schedule::default();
        schedule.set_executor(MultiThreadedExecutor::new());
        // Validators are independent. Bevy flushes their deferred insertions
        // before the dependent apply system accesses Debit and Grant.
        schedule.add_systems(((validate_wallet, validate_inventory), apply).chain());
        schedule.run(&mut world);
        Self {
            world,
            schedule,
            pending: 0,
        }
    }

    pub fn submit(&mut self, owner: usize, request: Purchase) {
        self.world
            .spawn((Owner(owner), Request(request), Outcome(None)));
        self.pending += 1;
    }

    pub fn finish(&mut self) -> Vec<Response> {
        let mut responses = Vec::new();
        while self.pending != 0 {
            self.schedule.run(&mut self.world);
            let mut query = self.world.query::<(Entity, &Owner, &Request, &Outcome)>();
            let completed: Vec<_> = query
                .iter(&self.world)
                .filter_map(|(entity, owner, request, outcome)| {
                    outcome.0.map(|accepted| {
                        (
                            entity,
                            Response {
                                owner: owner.0,
                                sequence: request.0.sequence,
                                accepted,
                            },
                        )
                    })
                })
                .collect();
            // Like an unresolved Bowl request, a missing predecessor remains
            // pending. A later submit can make it runnable; never busy-loop here.
            if completed.is_empty() {
                break;
            }
            self.pending -= completed.len();
            for (entity, response) in completed {
                self.world.despawn(entity);
                responses.push(response);
            }
        }
        responses
    }

    pub fn states(&mut self) -> Vec<AccountState> {
        let mut query = self.world.query::<(&Owner, &Balance, &Stock, &Sequence)>();
        let mut states: Vec<_> = query
            .iter(&self.world)
            .map(|(owner, wallet, inventory, sequence)| AccountState {
                owner: owner.0,
                balance: wallet.0.0,
                units: inventory.0.units,
                capacity: inventory.0.capacity,
                sequence: sequence.0,
            })
            .collect();
        states.sort_by_key(|state| state.owner);
        states
    }
}
