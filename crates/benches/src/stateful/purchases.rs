//! Independent validation artifacts followed by one guarded state update.
use bowl::{Bowl, Commands, CommitLimit, Component, Entity, Eq, MutRef, Not, Query, Schema, Where};

#[derive(Component, Hash)]
#[component(hash)]
pub struct Owner(pub usize);

#[derive(Component, Hash)]
#[component(hash)]
pub struct Wallet(pub u64);

#[derive(Component, Hash, Clone, PartialEq, Eq)]
#[component(hash)]
pub struct Inventory {
    pub units: u64,
    pub capacity: u64,
}

#[derive(Component, Hash)]
#[component(hash)]
pub struct Sequence(pub u64);

#[derive(Component, Hash, Clone, Copy)]
#[component(hash)]
pub struct Purchase {
    pub sequence: u64,
    pub price: u64,
    pub work: u32,
}

#[derive(Component, Hash, PartialEq)]
#[component(hash)]
pub struct Outcome(pub Option<bool>);

#[derive(Component, Hash)]
#[component(hash)]
struct Debit {
    observed: u64,
    next: Option<u64>,
}

#[derive(Component, Hash)]
#[component(hash)]
struct Grant {
    observed: Inventory,
    next: Option<u64>,
}

#[derive(Schema)]
struct PurchaseSchema {
    account: (Owner, Wallet, Inventory, Sequence),
    request: (Owner, Purchase, Outcome, Option<Debit>, Option<Grant>),
}

fn compute(work: u32) {
    let mut value = 1_u64;
    for _ in 0..work {
        value = std::hint::black_box(value.wrapping_mul(6364136223846793005).wrapping_add(1));
    }
    std::hint::black_box(value);
}

type ValidationRequests<'a> = Query<(Entity<purchase_schema::Request>, &'a Owner, &'a Purchase, &'a Outcome)>;
type ApplyRequests<'a> = Query<(Entity, &'a Owner, &'a Purchase, &'a Debit, &'a Grant, MutRef<'a, Outcome>)>;
type ApplyAccounts<'a> = Query<(Entity, MutRef<'a, Wallet>, MutRef<'a, Inventory>, MutRef<'a, Sequence>), Where<Eq<Owner>>>;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum KernelMode {
    #[default]
    Async,
    Sync,
}

impl std::str::FromStr for KernelMode {
    type Err = &'static str;

    fn from_str(name: &str) -> Result<Self, Self::Err> {
        match name {
            "async" => Ok(Self::Async),
            "sync" => Ok(Self::Sync),
            _ => Err("kernel mode must be async or sync"),
        }
    }
}

fn validate_wallet(
    requests: ValidationRequests<'_>,
    wallets: Query<&Wallet, Where<Eq<Owner>>>,
    mut commands: Commands<(purchase_schema::Request,)>,
) {
    let (entity, _, purchase, outcome) = requests.item();
    if outcome.0.is_some() {
        return;
    }

    compute(purchase.work);
    let wallet = wallets.item();
    commands.entity(entity).insert(Debit {
        observed: wallet.0,
        next: wallet.0.checked_sub(purchase.price),
    });
}

fn validate_inventory(
    requests: ValidationRequests<'_>,
    inventories: Query<&Inventory, Where<Eq<Owner>>>,
    mut commands: Commands<(purchase_schema::Request,)>,
) {
    let (entity, _, purchase, outcome) = requests.item();
    if outcome.0.is_some() {
        return;
    }

    compute(purchase.work);
    let inventory = inventories.item();
    commands.entity(entity).insert(Grant {
        observed: inventory.clone(),
        next: inventory
            .units
            .checked_add(1)
            .filter(|units| *units <= inventory.capacity),
    });
}

fn apply(requests: ApplyRequests<'_>, accounts: ApplyAccounts<'_>) {
    let (_, _, purchase, debit, grant, mut outcome) = requests.item();
    let (_, mut wallet, mut inventory, mut sequence) = accounts.item();
    if outcome.0.is_some() {
        return;
    }
    if purchase.sequence <= sequence.0 {
        outcome.0 = Some(false);
        return;
    }
    // Conflicts serialize access, but do not define request order.
    if sequence.0.checked_add(1) != Some(purchase.sequence) {
        return;
    }
    if debit.observed != wallet.0 || grant.observed != *inventory {
        return;
    }

    let accepted = match (debit.next, grant.next) {
        (Some(balance), Some(units)) => {
            wallet.0 = balance;
            inventory.units = units;
            true
        }
        _ => false,
    };
    sequence.0 = purchase.sequence;
    outcome.0 = Some(accepted);
}

// Both execution modes run exactly the same purchase bodies.
async fn validate_wallet_async(requests: ValidationRequests<'_>, wallets: Query<&Wallet, Where<Eq<Owner>>>, commands: Commands<(purchase_schema::Request,)>) {
    validate_wallet(requests, wallets, commands);
}

async fn validate_inventory_async(requests: ValidationRequests<'_>, inventories: Query<&Inventory, Where<Eq<Owner>>>, commands: Commands<(purchase_schema::Request,)>) {
    validate_inventory(requests, inventories, commands);
}

async fn apply_async(requests: ApplyRequests<'_>, accounts: ApplyAccounts<'_>) {
    apply(requests, accounts);
}

/// Settled persistent accounts, with no request artifacts yet.
pub async fn create(owners: usize, balance: u64, capacity: u64) -> Bowl {
    create_with_mode(owners, balance, capacity, KernelMode::Async).await
}

pub async fn create_with_mode(owners: usize, balance: u64, capacity: u64, mode: KernelMode) -> Bowl {
    let bowl = builder(mode).build();
    populate(&bowl, owners, balance, capacity).await;
    bowl
}

/// Reuse the purchase pipeline in mixed-world benchmarks.
pub fn builder(mode: KernelMode) -> bowl::BowlBuilder {
    let builder = Bowl::builder().schema::<PurchaseSchema>();
    match mode {
        KernelMode::Async => builder
            .system(validate_wallet_async).system(validate_inventory_async).system(apply_async),
        KernelMode::Sync => builder
            .system(validate_wallet).system(validate_inventory).system(apply),
    }
}

pub async fn populate(bowl: &Bowl, owners: usize, balance: u64, capacity: u64) {
    // Workload sizes must not silently trip the engine's application guardrail.
    bowl.set_commit_limit(CommitLimit::None);
    for owner in 0..owners {
        bowl.insert((
            Owner(owner),
            Wallet(balance),
            Inventory { units: 0, capacity },
            Sequence(0),
        ))
        .await;
    }
    bowl.scoop::<Query<&Wallet>>().await;
}

pub async fn submit(bowl: &Bowl, owner: usize, request: Purchase) {
    bowl.insert((Owner(owner), request, Outcome(None))).await;
}

#[derive(Debug, PartialEq, Eq)]
pub struct Response {
    pub owner: usize,
    pub sequence: u64,
    pub accepted: bool,
}

/// Settles, copies completed replies, and retires their request entities.
pub async fn drain(bowl: &Bowl) -> Vec<Response> {
    bowl.scoop::<Query<(Entity, &Owner, &Purchase, &Outcome), Where<Not<Eq<Outcome>>>>>()
        .args(Outcome(None))
        .drain()
        .await
        .collect()
        .into_iter()
        .filter_map(|(_, owner, request, outcome)| {
            outcome.0.map(|accepted| Response {
                owner: owner.0,
                sequence: request.sequence,
                accepted,
            })
        })
        .collect()
}

/// One batch, starting with fresh account sequences. Shared-owner requests are
/// deliberately submitted newest first to exercise dependency-driven retries.
pub async fn batch(bowl: &Bowl, requests: usize, per_owner: usize, work: u32) -> Vec<Response> {
    for index in (0..requests).rev() {
        submit(
            bowl,
            index / per_owner,
            Purchase {
                sequence: (index % per_owner) as u64 + 1,
                price: 1,
                work,
            },
        )
        .await;
    }
    drain(bowl).await
}
