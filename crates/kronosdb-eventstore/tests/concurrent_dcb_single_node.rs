//! Single-node sibling of `concurrent_dcb_cluster.rs`.
//!
//! Per D-11: openraft serializes `client_write` on a single leader so Raft-layer
//! contention is degenerate here, but the DCB-02 invariant ("zero consistency
//! violations under concurrent conditional appends") must still hold. This
//! test exercises the apply-path determinism/rejection path (03-02's error
//! taxonomy) without the gRPC transport surface area.
//!
//! Topology: 1 node, id=1, in-process Raft. No gRPC transport spawned — the
//! node talks to itself via NetworkFactory but that code path is never
//! exercised because there are no peers.
//!
//! Workload: 10 aggregates × 4 actors per aggregate (smaller than the
//! 3-node test's 50 × 4 per D-11).
//!
//! Assertion shape (same as 3-node):
//!   - Exactly 10 winners.
//!   - Exactly 30 rejections.
//!   - Every rejection matches `Error::ConsistencyConditionViolated`.
//!   - Node head converges to Position(10).

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use openraft::{BasicNode, Config, Raft};

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
use kronosdb_eventstore::raft::types::{NodeId, TypeConfig};
use kronosdb_eventstore::segment::DEFAULT_SEGMENT_SIZE;

struct TestNode {
    raft: Arc<Raft<TypeConfig>>,
    contexts: Arc<ContextManager>,
    engine: Arc<RaftEngine>,
}

/// In-process single-node bootstrap — no gRPC transport needed since there are
/// no peers. Matches cluster.rs::tests::single_node_init_and_get_store style
/// but uses manual Raft::new to keep topology explicit and mirror the 3-node
/// sibling's setup shape.
async fn start_single_node(id: NodeId, dir: &Path) -> TestNode {
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

    let local_engine = contexts.get_context("default").expect("get context");
    let engine = Arc::new(RaftEngine::new(
        Arc::clone(&raft),
        local_engine,
        "default".into(),
    ));

    TestNode {
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
        tokio::time::sleep(Duration::from_millis(50)).await;
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
        tokio::time::sleep(Duration::from_millis(25)).await;
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

/// Same DCB condition shape as the 3-node sibling: reject if any OrderPlaced
/// event with this orderId exists anywhere in the log.
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_dcb_single_node() {
    tokio::time::timeout(Duration::from_secs(30), run_single_node_workload())
        .await
        .expect("concurrent_dcb_single_node timed out");
}

async fn run_single_node_workload() {
    let dir = tempfile::tempdir().unwrap();
    let node = start_single_node(1, dir.path()).await;

    // Initialize single-node cluster. Addr is unused (no peers to contact).
    let mut members = BTreeMap::new();
    members.insert(
        1,
        BasicNode {
            addr: "127.0.0.1:0".into(),
        },
    );
    node.raft.initialize(members).await.expect("init");

    wait_for_leader(&node.raft, Duration::from_secs(5))
        .await
        .expect("leader elected");

    // Workload parameters per D-11: smaller than 3-node to reflect that
    // single-leader Raft serializes client_write; 10 × 4 is enough to prove
    // the apply-path determinism path.
    const AGGREGATES: usize = 10;
    const ACTORS_PER_AGGREGATE: usize = 4;

    let winners = Arc::new(AtomicUsize::new(0));
    let rejected = Arc::new(AtomicUsize::new(0));
    let other_errors = Arc::new(AtomicUsize::new(0));

    let mut join_set = tokio::task::JoinSet::new();

    for agg in 0..AGGREGATES {
        for actor in 0..ACTORS_PER_AGGREGATE {
            let engine = Arc::clone(&node.engine);
            let winners = Arc::clone(&winners);
            let rejected = Arc::clone(&rejected);
            let other_errors = Arc::clone(&other_errors);
            let order_id = format!("order-{agg}");
            let _ = actor;

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
        "single-node workload done: {AGGREGATES} aggregates, {ACTORS_PER_AGGREGATE} actors each; \
         winners={winners}, rejected={rejected}, other_errors={other_errors}"
    );

    assert_eq!(
        other_errors, 0,
        "unexpected errors beyond ConsistencyConditionViolated — apply-error taxonomy \
         (03-02) must not leak Io/Corrupted errors as rejections"
    );
    assert_eq!(
        winners, AGGREGATES,
        "expected exactly {AGGREGATES} winners (one per aggregate), got {winners}"
    );
    assert_eq!(
        rejected,
        AGGREGATES * (ACTORS_PER_AGGREGATE - 1),
        "expected {} rejections, got {rejected}",
        AGGREGATES * (ACTORS_PER_AGGREGATE - 1)
    );

    // Head convention: head() == next position to write (0-based, next-exclusive),
    // so after N successful single-event appends head is Position(N).
    let expected_head = Position(AGGREGATES as u64);
    assert!(
        wait_for_head(&node.contexts, expected_head, Duration::from_secs(5)).await,
        "node did not reach head {}",
        expected_head.0
    );

    let store = node.contexts.get_context("default").unwrap();
    assert_eq!(store.head(), expected_head, "single-node head mismatch");

    // Verify exactly one OrderPlaced per aggregate.
    let cond = SourcingCondition {
        criteria: vec![Criterion {
            names: vec!["OrderPlaced".into()],
            tags: vec![],
        }],
    };
    let events = store.source(Position(0), &cond).unwrap();
    assert_eq!(
        events.len(),
        AGGREGATES,
        "expected {AGGREGATES} total OrderPlaced events, got {}",
        events.len()
    );

    let _ = node.raft.shutdown().await;
}
