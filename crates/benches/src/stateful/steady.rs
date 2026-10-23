//! Reuse one request slot per account on a single warmed world.
use bowl::{Bowl, Entity, Query};

use super::purchases::{self, Inventory, Outcome, Owner, Purchase, Sequence, Wallet};

pub struct Purchases {
    pub bowl: Bowl,
    slots: Vec<Entity>,
    sequence: u64,
}

impl Purchases {
    pub async fn new(owners: usize) -> Self {
        let bowl = purchases::create(owners, u64::MAX, u64::MAX).await;
        let mut slots = Vec::with_capacity(owners);
        for owner in 0..owners {
            slots.push(
                bowl.insert((
                    Owner(owner),
                    Purchase {
                        sequence: 0,
                        price: 1,
                        work: 0,
                    },
                    Outcome(Some(false)),
                ))
                .await
                .entity(),
            );
        }
        bowl.scoop::<Query<&Outcome>>().await;

        Self {
            bowl,
            slots,
            sequence: 0,
        }
    }

    /// Submit fresh revisions to the same entities, then settle and read replies.
    /// No world recreation or entity retirement; includes ordinary result materialization.
    pub async fn round(&mut self) -> usize {
        self.submit().await;
        self.read().await
    }

    /// Same input and settlement, without result materialization. Verification
    /// outside the measured interval checks that every request was applied.
    pub async fn drive_round(&mut self) -> usize {
        self.submit().await;
        self.bowl.stir().await;
        self.slots.len()
    }

    async fn submit(&mut self) {
        self.sequence = self.sequence.checked_add(1).expect("sequence overflow");
        for &entity in &self.slots {
            self.bowl
                .entity(entity)
                .insert((
                    Purchase {
                        sequence: self.sequence,
                        price: 1,
                        work: 0,
                    },
                    Outcome(None),
                ))
                .await;
        }
    }

    /// A settled read materializes ordinary query guards, without new requests.
    pub async fn read(&self) -> usize {
        let replies = self.bowl.scoop::<Query<&Outcome>>().await;
        replies
            .collect()
            .into_iter()
            .filter(|reply| reply.0 == Some(true))
            .count()
    }

    /// Outside measured intervals: verify state and stable request identities.
    pub async fn verify(&self) {
        let accounts = self
            .bowl
            .scoop::<Query<(Entity, &Wallet, &Inventory, &Sequence)>>()
            .await;
        assert_eq!(accounts.len(), self.slots.len());
        for (_, wallet, inventory, sequence) in accounts.collect().into_iter() {
            assert_eq!(wallet.0, u64::MAX - self.sequence);
            assert_eq!(inventory.units, self.sequence);
            assert_eq!(sequence.0, self.sequence);
        }
        drop(accounts);

        let entities = self.bowl.scoop::<Query<Entity>>().await;
        assert_eq!(entities.len(), self.slots.len() * 2);
        drop(entities);

        let requests = self.bowl.scoop::<Query<(Entity, &Purchase)>>().await;
        let mut ids: Vec<_> = requests
            .collect()
            .into_iter()
            .map(|(entity, _)| entity)
            .collect();
        ids.sort();
        let mut expected = self.slots.clone();
        expected.sort();
        assert_eq!(ids, expected);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn request_slots_reuse_identity_but_recompute_each_revision() {
        let mut fixture = Purchases::new(4).await;
        for _ in 0..5 {
            assert_eq!(fixture.round().await, 4);
            fixture.verify().await;
        }
    }
}

#[cfg(test)]
mod drive_tests {
    use super::*;

    #[tokio::test]
    async fn drive_only_uses_the_same_request_and_cleanup_path() {
        let mut fixture = Purchases::new(4).await;
        for _ in 0..5 {
            assert_eq!(fixture.drive_round().await, 4);
            fixture.verify().await;
            assert_eq!(fixture.read().await, 4);
        }

        let pending = fixture
            .bowl
            .insert((
                Owner(99),
                Purchase {
                    sequence: 1,
                    price: 1,
                    work: 0,
                },
                Outcome(None),
            ))
            .await
            .bind();
        let entity = pending.entity();
        drop(pending);
        fixture.bowl.stir().await;
        let entities = fixture
            .bowl
            .scoop::<Query<(Entity, &Purchase)>>()
            .last_settled()
            .await;
        assert!(entities.collect().into_iter().all(|(id, _)| id != entity));
    }
}
