use crate::{AccountState, Purchase, Response};
use bowl::{Bowl, Entity, Query};
use fixtures::stateful::purchases::{self, Inventory, Owner, Sequence, Wallet};

pub struct Engine(Bowl);

impl Engine {
    pub async fn new(owners: usize, balance: u64, capacity: u64) -> Self {
        Self(purchases::create(owners, balance, capacity).await)
    }

    pub fn set_dispatch_batch_size(&self, maximum: usize) {
        self.0.set_dispatch_batch_size(maximum);
    }

    pub async fn submit(&self, owner: usize, request: Purchase) {
        purchases::submit(&self.0, owner, request).await;
    }

    pub async fn finish(&self) -> Vec<Response> {
        purchases::drain(&self.0).await
    }

    pub async fn states(&self) -> Vec<AccountState> {
        let rows = self
            .0
            .scoop::<Query<(Entity, &Owner, &Wallet, &Inventory, &Sequence)>>()
            .await;
        let mut states: Vec<_> = rows
            .collect()
            .into_iter()
            .map(|(_, owner, wallet, inventory, sequence)| AccountState {
                owner: owner.0,
                balance: wallet.0,
                units: inventory.units,
                capacity: inventory.capacity,
                sequence: sequence.0,
            })
            .collect();
        states.sort_by_key(|state| state.owner);
        states
    }
}
