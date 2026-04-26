//! End-to-end consistency test under concurrent load on a 3-node Raft cluster.
//!
//! Ported from commit `04e0cbf` of `/Users/theoem/Development/kronosdb` with
//! signature adaptations for this worktree (Phase 2 canonical forms):
//!   - `LogStore::new(&raft_dir, LogStoreConfig::default())` (2-arg form).
//!   - `EventStoreStateMachine::new(Arc::clone(&contexts))` (1-arg form).
//!   - `DurabilityMode::Strict` only (other modes unreachable from Raft path
//!     per PROJECT.md).
//!
//! Spawns K concurrent "actors" that each race to be the first to place an
//! `OrderPlaced` event for one of M aggregate ids. Each actor uses a DCB
//! consistency condition: "reject this append if any event with the chosen
//! orderId already exists."
//!
//! What this exercises:
//!   - The concurrent-claim writer under real load (many appends in flight).
//!   - DCB condition checking with overlapping contention windows.
//!   - Raft commit + apply + cross-node replication under concurrent writes.
//!   - Read-back consistency on every node (not just the leader).
//!
//! What it asserts:
//!   - Exactly AGGREGATES successful appends (winners).
//!   - Exactly AGGREGATES * (ACTORS_PER_AGGREGATE - 1) rejections.
//!   - Every rejection is `Error::ConsistencyConditionViolated` — no
//!     transport / io / corrupted errors leak as rejections.
//!   - All three nodes converge on the same head position and same events.
//!
//! If DCB under concurrent client_write had a TOCTOU race (check at propose
//! time, not at apply time), this test would observe it as "expected 1 winner
//! per aggregate, got N". The phase-3 apply-authoritative taxonomy is what
//! this test gates.

use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use openraft::{BasicNode, Config, Raft};
use tonic::transport::Server;

use kronosdb_eventstore::api::EventStore;
use kronosdb_eventstore::append::{AppendCondition, AppendRequest};
use kronosdb_eventstore::context::ContextManager;
use kronosdb_eventstore::criteria::{Criterion, SourcingCondition};
use kronosdb_eventstore::error::Error;
use kronosdb_eventstore::event::{AppendEvent, Position, Tag};
use kronosdb_eventstore::raft::cluster::RaftEngine;
use kronosdb_eventstore::raft::log_store::{LogStore, LogStoreConfig};
use kronosdb_eventstore::raft::network::NetworkFactory;
use kronosdb_eventstore::raft::state_machine::EventStoreStateMachine;
use kronosdb_eventstore::raft::transport::RaftTransportService;
use kronosdb_eventstore::raft::types::{NodeId, TypeConfig};
use kronosdb_eventstore::segment::DEFAULT_SEGMENT_SIZE;

struct TestNode {
    id: NodeId,
    raft: Arc<Raft<TypeConfig>>,
    contexts: Arc<ContextManager>,
    engine: Arc<RaftEngine>,
}

async fn start_node(id: NodeId, port: u16, dir: &Path) -> TestNode {
    let contexts =
        Arc::new(ContextManager::new(dir, DEFAULT_SEGMENT_SIZE).expect("create context manager"));
    if !contexts.context_exists("default") {
        contexts
            .create_context("default")
            .expect("create default context");
    }

    let raft_dir = dir.join("raft");
    let log_store = LogStore::new(&raft_dir, LogStoreConfig::default()).expect("create log store");
    let state_machine =
        EventStoreStateMachine::new(Arc::clone(&contexts)).expect("recover state machine");

    let config = Config {
        heartbeat_interval: 200,
        election_timeout_min: 500,
        election_timeout_max: 1000,
        ..Default::default()
    };

    let raft = Arc::new(
        Raft::new(
            id,
            Arc::new(config),
            NetworkFactory,
            log_store,
            state_machine,
        )
        .await
        .expect("create raft node"),
    );

    let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let transport = RaftTransportService::new(Arc::clone(&raft));
    tokio::spawn(async move {
        Server::builder()
            .add_service(transport.into_server())
            .serve(addr)
            .await
            .ok();
    });
    tokio::time::sleep(Duration::from_millis(100)).await;

    let local_engine = contexts.get_context("default").expect("get context");
    let engine = Arc::new(RaftEngine::new(
        Arc::clone(&raft),
        local_engine,
        "default".into(),
    ));

    TestNode {
        id,
        raft,
        contexts,
        engine,
    }
}

async fn wait_for_leader(raft: &Raft<TypeConfig>, timeout: Duration) -> Option<NodeId> {
    let start = tokio::time::Instant::now();
    loop {
        if let Some(leader) = raft.metrics().borrow().current_leader {
            return Some(leader);
        }
        if start.elapsed() > timeout {
            return None;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn wait_for_head(contexts: &ContextManager, expected: Position, timeout: Duration) -> bool {
    let start = tokio::time::Instant::now();
    loop {
        if let Ok(store) = contexts.get_context("default")
            && store.head() >= expected
        {
            return true;
        }
        if start.elapsed() > timeout {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

fn order_placed(order_id: &str) -> AppendEvent {
    AppendEvent {
        identifier: format!("evt-{order_id}"),
        name: "OrderPlaced".into(),
        version: "1.0".into(),
        timestamp: 1_712_345_678_000,
        payload: format!("payload-{order_id}").into_bytes(),
        metadata: vec![],
        tags: vec![Tag::from_str("orderId", order_id)],
    }
}

/// Builds a DCB condition: "reject if any OrderPlaced event with this
/// orderId tag already exists". The consistency marker is Position(0)
/// so the check covers the entire log.
fn reject_if_duplicate(order_id: &str) -> AppendCondition {
    AppendCondition {
        consistency_marker: Position(0),
        criteria: SourcingCondition {
            criteria: vec![Criterion {
                names: vec!["OrderPlaced".into()],
                tags: vec![Tag::from_str("orderId", order_id)],
            }],
        },
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_dcb_three_nodes() {
    // Wrap in a top-level timeout so a deadlock (e.g. missed leader election,
    // unreplicated write) fails the test fast instead of hanging the suite.
    tokio::time::timeout(Duration::from_secs(60), run_three_node_workload())
        .await
        .expect("concurrent_dcb_three_nodes timed out");
}

async fn run_three_node_workload() {
    let dir = tempfile::tempdir().unwrap();
    // Use a high port range disjoint from cluster_test.rs (19100/19200/19300)
    // to allow parallel `cargo test` runs without port conflicts.
    let base_port = 19400;

    // Boot 3-node cluster.
    let node1 = start_node(1, base_port, &dir.path().join("node1")).await;
    let node2 = start_node(2, base_port + 1, &dir.path().join("node2")).await;
    let node3 = start_node(3, base_port + 2, &dir.path().join("node3")).await;

    // Initialize cluster.
    let mut members = BTreeMap::new();
    for (id, port_offset) in [(1u64, 0u16), (2, 1), (3, 2)] {
        members.insert(
            id,
            BasicNode {
                addr: format!("127.0.0.1:{}", base_port + port_offset),
            },
        );
    }
    node1
        .raft
        .initialize(members)
        .await
        .expect("initialize cluster");

    let leader_id = wait_for_leader(&node1.raft, Duration::from_secs(10))
        .await
        .expect("leader elected");

    let leader: &TestNode = match leader_id {
        1 => &node1,
        2 => &node2,
        3 => &node3,
        _ => panic!("unexpected leader"),
    };
    eprintln!("leader elected: node {leader_id}");

    // Workload parameters (preserved verbatim from reference 04e0cbf).
    //   AGGREGATES: number of distinct orderIds being fought over.
    //   ACTORS_PER_AGGREGATE: how many concurrent actors race for each.
    //   Total append attempts = AGGREGATES * ACTORS_PER_AGGREGATE.
    //   Expected winners = AGGREGATES (one winner per aggregate, others
    //     get ConsistencyConditionViolated).
    const AGGREGATES: usize = 50;
    const ACTORS_PER_AGGREGATE: usize = 4;

    let winners = Arc::new(AtomicUsize::new(0));
    let rejected = Arc::new(AtomicUsize::new(0));
    let other_errors = Arc::new(AtomicUsize::new(0));

    let mut join_set = tokio::task::JoinSet::new();

    for agg in 0..AGGREGATES {
        for actor in 0..ACTORS_PER_AGGREGATE {
            let engine = Arc::clone(&leader.engine);
            let winners = Arc::clone(&winners);
            let rejected = Arc::clone(&rejected);
            let other_errors = Arc::clone(&other_errors);
            let order_id = format!("order-{agg}");
            let _ = actor; // used only for logging if needed

            join_set.spawn(async move {
                let request = AppendRequest {
                    condition: Some(reject_if_duplicate(&order_id)),
                    events: vec![order_placed(&order_id)],
                };
                match engine.append(request).await {
                    Ok(_) => {
                        winners.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(ref err) if matches!(err, Error::ConsistencyConditionViolated { .. }) => {
                        rejected.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(e) => {
                        eprintln!("unexpected error for {order_id}: {e}");
                        other_errors.fetch_add(1, Ordering::Relaxed);
                    }
                }
            });
        }
    }

    while let Some(res) = join_set.join_next().await {
        res.expect("actor task panicked");
    }

    let winners = winners.load(Ordering::Relaxed);
    let rejected = rejected.load(Ordering::Relaxed);
    let other_errors = other_errors.load(Ordering::Relaxed);

    eprintln!(
        "workload done: {AGGREGATES} aggregates, {ACTORS_PER_AGGREGATE} actors each; \
         winners={winners}, rejected={rejected}, other_errors={other_errors}"
    );

    assert_eq!(
        other_errors, 0,
        "unexpected errors beyond ConsistencyConditionViolated — apply-error taxonomy \
         (03-02) must not leak Io/Corrupted/transport errors as rejections"
    );
    assert_eq!(
        winners, AGGREGATES,
        "expected exactly {AGGREGATES} winners (one per aggregate), got {winners}. \
         If this is greater than {AGGREGATES}, DCB has a TOCTOU race between \
         check_condition (at propose time) and apply (via Raft)."
    );
    assert_eq!(
        rejected,
        AGGREGATES * (ACTORS_PER_AGGREGATE - 1),
        "expected {} rejections, got {rejected}",
        AGGREGATES * (ACTORS_PER_AGGREGATE - 1)
    );

    // Wait for all nodes to replicate to the expected head.
    // Head semantics in this crate: `head()` == next position to be written,
    // so after N successful appends the head is Position(N + 1). See
    // cluster_test.rs::three_node_cluster_replication for the same convention
    // (3 appends → head >= 4).
    let expected_head = Position((AGGREGATES as u64) + 1);
    for node in [&node1, &node2, &node3] {
        assert!(
            wait_for_head(&node.contexts, expected_head, Duration::from_secs(10)).await,
            "node {} did not replicate to head {}",
            node.id,
            expected_head.0
        );
    }

    // Read back on every node. Each should have exactly one OrderPlaced per aggregate.
    for node in [&node1, &node2, &node3] {
        let store = node.contexts.get_context("default").unwrap();

        // Sanity: head matches.
        assert_eq!(
            store.head(),
            expected_head,
            "node {} head mismatch",
            node.id
        );

        // Source every OrderPlaced event and bucket by orderId.
        let cond = SourcingCondition {
            criteria: vec![Criterion {
                names: vec!["OrderPlaced".into()],
                tags: vec![],
            }],
        };
        let events = store.source(Position(0), &cond).unwrap();
        let mut per_order: HashMap<String, usize> = HashMap::new();
        for ev in &events {
            // The orderId is encoded on the identifier as "evt-order-{agg}"; strip
            // the "evt-" prefix to get the aggregate key for grouping.
            let key = ev
                .identifier
                .strip_prefix("evt-")
                .unwrap_or(&ev.identifier)
                .to_string();
            *per_order.entry(key).or_insert(0) += 1;
        }

        assert_eq!(
            per_order.len(),
            AGGREGATES,
            "node {}: expected {AGGREGATES} distinct orderIds, got {}",
            node.id,
            per_order.len()
        );
        for (order_id, count) in &per_order {
            assert_eq!(
                *count, 1,
                "node {}: orderId {order_id} has {count} events (expected 1); \
                 this means DCB admitted duplicate winners",
                node.id
            );
        }
    }

    let _ = node1.raft.shutdown().await;
    let _ = node2.raft.shutdown().await;
    let _ = node3.raft.shutdown().await;
}
