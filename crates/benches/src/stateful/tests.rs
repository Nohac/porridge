use super::{churn, purchases};
use bowl::{Entity, Query};
use purchases::{Inventory, Owner, Purchase, Sequence, Wallet};

fn runtime(workers: usize) -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(workers)
        .build()
        .unwrap()
}

#[test]
fn purchases_preserve_order_and_state_after_response_retirement() {
    for workers in [1, 4] {
        for mode in [purchases::KernelMode::Async, purchases::KernelMode::Sync] {
        runtime(workers).block_on(async {
            let bowl = purchases::create_with_mode(8, 20, 20, mode).await;
            let mut responses = purchases::batch(&bowl, 32, 4, 0).await;
            responses.sort_by_key(|response| (response.owner, response.sequence));
            assert_eq!(responses.len(), 32);
            for (index, response) in responses.iter().enumerate() {
                assert_eq!(response.owner, index / 4);
                assert_eq!(response.sequence, (index % 4 + 1) as u64);
                assert!(response.accepted);
            }

            let accounts = bowl
                .scoop::<Query<(Entity, &Owner, &Wallet, &Inventory, &Sequence)>>()
                .await;
            assert_eq!(accounts.len(), 8);
            for (_, _, wallet, inventory, sequence) in accounts.collect() {
                assert_eq!((wallet.0, inventory.units, sequence.0), (16, 4, 4));
            }
            assert_eq!(bowl.scoop::<Query<(Entity, &Purchase)>>().await.len(), 0);
            assert!(purchases::drain(&bowl).await.is_empty());
        });
        }
    }
}

#[test]
fn refusal_does_not_partially_mutate_and_replay_cannot_charge_twice() {
    runtime(4).block_on(async {
        for mode in [purchases::KernelMode::Async, purchases::KernelMode::Sync] {
        for (balance, capacity) in [(0, 10), (10, 0)] {
            let bowl = purchases::create_with_mode(1, balance, capacity, mode).await;
            let request = Purchase {
                sequence: 1,
                price: 1,
                work: 0,
            };
            purchases::submit(&bowl, 0, request).await;
            assert!(!purchases::drain(&bowl).await[0].accepted);
            purchases::submit(&bowl, 0, request).await;
            assert!(!purchases::drain(&bowl).await[0].accepted);
            let rows = bowl
                .scoop::<Query<(Entity, &Wallet, &Inventory, &Sequence)>>()
                .await;
            let (_, wallet, inventory, sequence) = &rows.collect()[0];
            assert_eq!((wallet.0, inventory.units, sequence.0), (balance, 0, 1));
        }

        let bowl = purchases::create_with_mode(1, 10, 10, mode).await;
        let request = Purchase {
            sequence: 1,
            price: 3,
            work: 0,
        };
        purchases::submit(&bowl, 0, request).await;
        assert!(purchases::drain(&bowl).await[0].accepted);
        purchases::submit(&bowl, 0, request).await;
        assert!(!purchases::drain(&bowl).await[0].accepted);
        let rows = bowl.scoop::<Query<(Entity, &Wallet, &Inventory)>>().await;
        let (_, wallet, inventory) = &rows.collect()[0];
        assert_eq!((wallet.0, inventory.units), (7, 1));
        }
    });
}

#[test]
fn retired_history_does_not_change_visible_population() {
    runtime(1).block_on(async {
        for history in [0, 257, 1024] {
            let bowl = churn::create(7, history).await;
            assert_eq!(churn::scan(&bowl).await, 7);
            assert_eq!(churn::scan_store(&bowl).await, 7);
        }
    });
}
