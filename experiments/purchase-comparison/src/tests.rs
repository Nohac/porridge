use crate::{AccountState, Purchase, Response, bevy, porridge};

fn sort(responses: &mut [Response]) {
    responses.sort_by_key(|response| (response.owner, response.sequence));
}

#[test]
fn both_engines_follow_ordering_refusal_replay_and_cleanup_contracts() {
    bevy::init_workers(4);
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .build()
        .unwrap();
    runtime.block_on(async {
        for (balance, capacity) in [(20, 20), (0, 20), (20, 0)] {
            let mut ecs = bevy::Engine::new(8, balance, capacity);
            let bowl = porridge::Engine::new(8, balance, capacity).await;
            for index in (0..32).rev() {
                let request = Purchase {
                    sequence: (index % 4 + 1) as u64,
                    price: 1,
                    work: 0,
                };
                ecs.submit(index / 4, request);
                bowl.submit(index / 4, request).await;
            }
            let mut a = ecs.finish();
            let mut b = bowl.finish().await;
            sort(&mut a);
            sort(&mut b);
            assert_eq!(a, b);
            assert_eq!(a.len(), 32);
            let accepted = balance != 0 && capacity != 0;
            assert!(a.iter().all(|response| response.accepted == accepted));
            let expected: Vec<_> = (0..8)
                .map(|owner| AccountState {
                    owner,
                    balance: balance - if accepted { 4 } else { 0 },
                    units: if accepted { 4 } else { 0 },
                    capacity,
                    sequence: 4,
                })
                .collect();
            assert_eq!(ecs.states(), expected);
            assert_eq!(bowl.states().await, expected);
            assert!(ecs.finish().is_empty());
            assert!(bowl.finish().await.is_empty());

            let replay = Purchase {
                sequence: 4,
                price: 1,
                work: 0,
            };
            ecs.submit(0, replay);
            bowl.submit(0, replay).await;
            assert!(!ecs.finish()[0].accepted);
            assert!(!bowl.finish().await[0].accepted);
            assert_eq!(ecs.states(), expected);
            assert_eq!(bowl.states().await, expected);
        }
    });
}

#[test]
fn missing_predecessor_stays_pending_until_supplied() {
    bevy::init_workers(4);
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .build()
        .unwrap();
    runtime.block_on(async {
        let mut ecs = bevy::Engine::new(1, 10, 10);
        let bowl = porridge::Engine::new(1, 10, 10).await;
        let later = Purchase {
            sequence: 2,
            price: 2,
            work: 0,
        };
        ecs.submit(0, later);
        bowl.submit(0, later).await;
        assert!(ecs.finish().is_empty());
        assert!(bowl.finish().await.is_empty());
        let first = Purchase {
            sequence: 1,
            price: 2,
            work: 0,
        };
        ecs.submit(0, first);
        bowl.submit(0, first).await;
        let mut a = ecs.finish();
        let mut b = bowl.finish().await;
        sort(&mut a);
        sort(&mut b);
        assert_eq!(a, b);
        assert_eq!(a.len(), 2);
        assert!(a.iter().all(|response| response.accepted));
        assert_eq!(ecs.states(), bowl.states().await);
        assert_eq!(ecs.states()[0].balance, 6);
    });
}

#[test]
fn large_independent_batch_completes_every_request() {
    bevy::init_workers(4);
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .build()
        .unwrap();
    runtime.block_on(async {
        let mut ecs = bevy::Engine::new(1000, 10, 10);
        let bowl = porridge::Engine::new(1000, 10, 10).await;
        for owner in (0..1000).rev() {
            let request = Purchase {
                sequence: 1,
                price: 1,
                work: 0,
            };
            ecs.submit(owner, request);
            bowl.submit(owner, request).await;
        }
        let mut a = ecs.finish();
        let mut b = bowl.finish().await;
        sort(&mut a);
        sort(&mut b);
        assert_eq!(a, b);
        assert_eq!(a.len(), 1000);
        for (owner, response) in a.iter().enumerate() {
            assert_eq!(response.owner, owner);
            assert_eq!(response.sequence, 1);
            assert!(response.accepted);
        }
        let expected: Vec<_> = (0..1000)
            .map(|owner| AccountState {
                owner,
                balance: 9,
                units: 1,
                capacity: 10,
                sequence: 1,
            })
            .collect();
        assert_eq!(ecs.states(), expected);
        assert_eq!(bowl.states().await, expected);
        assert!(ecs.finish().is_empty());
        assert!(bowl.finish().await.is_empty());
    });
}
