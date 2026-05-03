//! Integration test for Phase 4 SNAP-02: a fresh learner that joins after the
//! snapshot threshold is crossed must install the leader's snapshot and end up
//! apply-consistent with the leader. Per 04-CONTEXT.md `<specifics>` and the
//! revision feedback (option (d)): the gate is that calling
//! `RaftStateMachine::apply` directly on both the leader's and the follower's
//! state machines (via sibling instances sharing ContextManager) with the
//! same Entry produces identical RaftResponse values.
//!
//! Port range: 19500-19504 (disjoint from the other 3-node integration suites).

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use openraft::storage::RaftStateMachine;
use openraft::{
    BasicNode, CommittedLeaderId, Config, Entry, EntryPayload, LogId, Raft, SnapshotPolicy,
};
use tonic::transport::Server;

use kronosdb_eventstore::context::ContextManager;
use kronosdb_eventstore::event::Position;
use kronosdb_eventstore::raft::log_store::{LogStore, LogStoreConfig};
use kronosdb_eventstore::raft::network::NetworkFactory;
use kronosdb_eventstore::raft::snapshot_store::SnapshotStore;
use kronosdb_eventstore::raft::state_machine::EventStoreStateMachine;
use kronosdb_eventstore::raft::transport::RaftTransportService;
use kronosdb_eventstore::raft::types::{
    NodeId, RaftAppendCondition, RaftAppendEvent, RaftCriterion, RaftRejectReason, RaftRequest,
    RaftResponse, TypeConfig,
};
use kronosdb_eventstore::segment::DEFAULT_SEGMENT_SIZE;

const SNAPSHOT_THRESHOLD: u64 = 32;

struct TestNode {
    #[allow(dead_code)]
    id: NodeId,
    raft: Arc<Raft<TypeConfig>>,
    contexts: Arc<ContextManager>,
    addr: SocketAddr,
}

async fn start_node(id: NodeId, port: u16, dir: &std::path::Path) -> TestNode {
    let contexts =
        Arc::new(ContextManager::new(dir, DEFAULT_SEGMENT_SIZE).expect("create context manager"));
    if !contexts.context_exists("default") {
        contexts.create_context("default").expect("create default");
    }

    let raft_dir = dir.join("raft");
    let log_store = LogStore::new(&raft_dir, LogStoreConfig::default()).expect("create log store");
    let snap_store =
        Arc::new(SnapshotStore::new(raft_dir.join("snapshots")).expect("create snapshot store"));
    let state_machine = EventStoreStateMachine::new(Arc::clone(&contexts), snap_store)
        .expect("recover state machine");

    // Aggressive snapshot threshold to force install on the learner.
    let config = Config {
        heartbeat_interval: 200,
        election_timeout_min: 500,
        election_timeout_max: 1000,
        snapshot_policy: SnapshotPolicy::LogsSinceLast(SNAPSHOT_THRESHOLD),
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
            .expect("transport server failed");
    });
    tokio::time::sleep(Duration::from_millis(100)).await;

    TestNode {
        id,
        raft,
        contexts,
        addr,
    }
}

async fn wait_for_leader(raft: &Raft<TypeConfig>, timeout: Duration) -> Option<NodeId> {
    let start = tokio::time::Instant::now();
    loop {
        let metrics = raft.metrics().borrow().clone();
        if let Some(leader) = metrics.current_leader {
            return Some(leader);
        }
        if start.elapsed() > timeout {
            return None;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn wait_for_last_applied(
    raft: &Raft<TypeConfig>,
    expected_min: u64,
    timeout: Duration,
) -> bool {
    let start = tokio::time::Instant::now();
    loop {
        let m = raft.metrics().borrow().clone();
        if let Some(id) = m.last_applied
            && id.index >= expected_min
        {
            return true;
        }
        if start.elapsed() > timeout {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn unconditional_event(idx: u64, tag_value: &[u8]) -> RaftAppendEvent {
    RaftAppendEvent {
        identifier: format!("ev-{idx}"),
        name: "OrderPlaced".to_string(),
        version: "1.0".to_string(),
        timestamp: 1712345678000,
        payload: format!("payload-{idx}").into_bytes(),
        metadata: vec![],
        tags: vec![(b"orderId".to_vec(), tag_value.to_vec())],
    }
}

/// Build a synthetic Entry<TypeConfig> wrapping a RaftRequest::Append —
/// used to drive apply directly on sibling state machines for the
/// option (d) apply-consistency assertion.
fn make_entry(term: u64, index: u64, req: RaftRequest) -> Entry<TypeConfig> {
    Entry {
        log_id: LogId {
            leader_id: CommittedLeaderId::new(term, 0),
            index,
        },
        payload: EntryPayload::Normal(req),
    }
}

#[tokio::test]
async fn snapshot_coldjoin_apply_consistency() {
    // --- SETUP: two voters. ---
    let dir1 = tempfile::tempdir().unwrap();
    let dir2 = tempfile::tempdir().unwrap();
    let node1 = start_node(1, 19500, dir1.path()).await;
    let node2 = start_node(2, 19501, dir2.path()).await;

    // Bootstrap: members are nodes 1 and 2. BasicNode.addr is the raw
    // "host:port" — NetworkFactory prepends "http://" when building the
    // tonic endpoint (see raft/network.rs). Prefixing here would double up.
    let mut members = BTreeMap::new();
    members.insert(
        1,
        BasicNode {
            addr: format!("127.0.0.1:{}", 19500),
        },
    );
    members.insert(
        2,
        BasicNode {
            addr: format!("127.0.0.1:{}", 19501),
        },
    );
    node1.raft.initialize(members).await.expect("initialize");

    let leader = wait_for_leader(&node1.raft, Duration::from_secs(5))
        .await
        .expect("leader elected");
    let leader_raft = if leader == 1 {
        &node1.raft
    } else {
        &node2.raft
    };
    let leader_contexts: Arc<ContextManager> = if leader == 1 {
        Arc::clone(&node1.contexts)
    } else {
        Arc::clone(&node2.contexts)
    };

    // --- DRIVE: 64 unconditional appends via leader.client_write. ---
    // This is 2x the snapshot threshold so at least one snapshot must build.
    for i in 1..=64u64 {
        let req = RaftRequest::Append {
            context: "default".to_string(),
            events: vec![unconditional_event(i, format!("order-{i}").as_bytes())],
            condition: None,
        };
        let resp = leader_raft.client_write(req).await.expect("client_write");
        match resp.data {
            RaftResponse::Append { .. } => {}
            other => panic!("expected Append, got {other:?}"),
        }
    }

    // Wait for both voters to apply all 64.
    assert!(
        wait_for_last_applied(&node1.raft, 64, Duration::from_secs(5)).await,
        "node1 failed to apply 64 entries"
    );
    assert!(
        wait_for_last_applied(&node2.raft, 64, Duration::from_secs(5)).await,
        "node2 failed to apply 64 entries"
    );

    // --- ADD LEARNER (node 3) AFTER the snapshot threshold. ---
    let dir3 = tempfile::tempdir().unwrap();
    let node3 = start_node(3, 19502, dir3.path()).await;
    // NB: node3 starts fresh — ContextManager is empty, no prior state.
    assert!(
        node3
            .contexts
            .list_contexts()
            .iter()
            .any(|n| n == "default")
    );
    node3
        .contexts
        .with_context("default", |engine| {
            assert_eq!(
                engine.head(),
                Position(0),
                "node3 starts empty — head is 0 before any events"
            );
            Ok(())
        })
        .unwrap();

    leader_raft
        .add_learner(
            3,
            BasicNode {
                addr: format!("127.0.0.1:{}", 19502),
            },
            true,
        )
        .await
        .expect("add_learner");

    // --- WAIT FOR INSTALL: node 3 catches up to leader's last_applied. ---
    let leader_last = node1
        .raft
        .metrics()
        .borrow()
        .clone()
        .last_applied
        .expect("leader has last_applied")
        .index;
    let coldjoin_start = tokio::time::Instant::now();
    assert!(
        wait_for_last_applied(&node3.raft, leader_last, Duration::from_secs(10)).await,
        "node3 failed to catch up via snapshot install (leader_last={leader_last})"
    );
    let coldjoin_latency = coldjoin_start.elapsed();
    eprintln!(
        "snapshot_coldjoin: node3 reached last_applied>={leader_last} in {:?}",
        coldjoin_latency
    );

    // --- ASSERT 1: apply-consistent head position on default. ---
    let leader_head = leader_contexts.get_context("default").unwrap().head();
    let follower_head = node3.contexts.get_context("default").unwrap().head();
    assert_eq!(
        leader_head, follower_head,
        "head mismatch after install: leader={leader_head:?} follower={follower_head:?}"
    );

    // --- ASSERT 2 (SNAP-02 GATE — option (d) direct apply on both state machines). ---
    // Build sibling state machines sharing each node's live ContextManager.
    // These siblings are NOT wired into openraft — they're a direct handle
    // into the apply() path for deterministic side-by-side comparison.
    // Throwaway snapshot stores — these sibling SMs are not wired into
    // openraft, they're just direct apply()-path probes for side-by-side
    // determinism comparison. A scratch dir per sibling keeps them
    // independent of the live raft snapshot dirs.
    let leader_snap_scratch = tempfile::tempdir().expect("scratch dir");
    let follower_snap_scratch = tempfile::tempdir().expect("scratch dir");
    let mut leader_sibling = EventStoreStateMachine::new(
        Arc::clone(&leader_contexts),
        Arc::new(SnapshotStore::new(leader_snap_scratch.path()).unwrap()),
    )
    .expect("recover leader sibling");
    let mut follower_sibling = EventStoreStateMachine::new(
        Arc::clone(&node3.contexts),
        Arc::new(SnapshotStore::new(follower_snap_scratch.path()).unwrap()),
    )
    .expect("recover follower sibling");

    // Craft a conditional append that MUST be rejected: criterion matches
    // tag orderId=order-1 which was appended in the loop above, with
    // consistency_marker=0 so any match triggers rejection.
    let reject_req = RaftRequest::Append {
        context: "default".to_string(),
        events: vec![unconditional_event(999, b"should-reject")],
        condition: Some(RaftAppendCondition {
            consistency_marker: 0,
            criteria: vec![RaftCriterion {
                names: vec![],
                tags: vec![(b"orderId".to_vec(), b"order-1".to_vec())],
            }],
        }),
    };

    // Use a throwaway log_id — the sibling state machines track
    // last_applied independently of openraft, and we only care about the
    // RaftResponse returned from apply().
    let reject_entry = make_entry(99, 9999, reject_req);

    let leader_responses = leader_sibling
        .apply(vec![reject_entry.clone()])
        .await
        .expect("leader sibling apply reject");
    let follower_responses = follower_sibling
        .apply(vec![reject_entry])
        .await
        .expect("follower sibling apply reject");

    assert_eq!(leader_responses.len(), 1);
    assert_eq!(follower_responses.len(), 1);

    match (&leader_responses[0], &follower_responses[0]) {
        (
            RaftResponse::AppendRejected {
                reason:
                    RaftRejectReason::ConsistencyConditionViolated {
                        conflicting_position: lp,
                    },
            },
            RaftResponse::AppendRejected {
                reason:
                    RaftRejectReason::ConsistencyConditionViolated {
                        conflicting_position: fp,
                    },
            },
        ) => {
            assert_eq!(
                lp, fp,
                "SNAP-02 GATE FAILED: leader conflicting_position={lp} \
                 follower conflicting_position={fp} — apply-consistency broken"
            );
            eprintln!("SNAP-02 GATE PASSED: both nodes rejected at conflicting_position={lp}");
        }
        (l, f) => panic!(
            "SNAP-02 GATE FAILED: expected both AppendRejected with matching \
             reasons, got leader={l:?} follower={f:?}"
        ),
    }

    // Rejection must not have moved head on either side.
    let leader_head_after = leader_contexts.get_context("default").unwrap().head();
    let follower_head_after = node3.contexts.get_context("default").unwrap().head();
    assert_eq!(leader_head_after, leader_head);
    assert_eq!(follower_head_after, follower_head);

    // --- ASSERT 3 (SNAP-02 positive case — direct apply on separate contexts). ---
    // To avoid mutating the shared "default" context (which would race with
    // Raft log apply), create a dedicated context on each side and apply
    // there. Both should return Ok(Append) with count=1.
    leader_contexts
        .create_context("leader-success-ctx")
        .unwrap();
    node3
        .contexts
        .create_context("follower-success-ctx")
        .unwrap();

    let accept_leader = RaftRequest::Append {
        context: "leader-success-ctx".to_string(),
        events: vec![unconditional_event(10_001, b"leader-unique")],
        // Fresh context — head is 0, consistency_marker=0 with empty
        // criterion list means the condition is trivially satisfiable.
        condition: Some(RaftAppendCondition {
            consistency_marker: 0,
            criteria: vec![RaftCriterion {
                names: vec![],
                tags: vec![(b"orderId".to_vec(), b"leader-unique".to_vec())],
            }],
        }),
    };
    let accept_follower = RaftRequest::Append {
        context: "follower-success-ctx".to_string(),
        events: vec![unconditional_event(10_002, b"follower-unique")],
        condition: Some(RaftAppendCondition {
            consistency_marker: 0,
            criteria: vec![RaftCriterion {
                names: vec![],
                tags: vec![(b"orderId".to_vec(), b"follower-unique".to_vec())],
            }],
        }),
    };

    let leader_accept = leader_sibling
        .apply(vec![make_entry(99, 10_001, accept_leader)])
        .await
        .expect("leader sibling apply accept");
    let follower_accept = follower_sibling
        .apply(vec![make_entry(99, 10_002, accept_follower)])
        .await
        .expect("follower sibling apply accept");

    match (&leader_accept[0], &follower_accept[0]) {
        (RaftResponse::Append { count: lc, .. }, RaftResponse::Append { count: fc, .. }) => {
            assert_eq!(*lc, 1);
            assert_eq!(*fc, 1);
            assert_eq!(lc, fc, "count mismatch: leader={lc} follower={fc}");
        }
        (l, f) => panic!(
            "SNAP-02 positive case FAILED: expected Append on both, \
             got leader={l:?} follower={f:?}"
        ),
    }

    // --- ASSERT 4 (auxiliary smoke test — cluster transport end-to-end). ---
    // Not the SNAP-02 gate; just confirms client_write path still works
    // after cold-join. A reject via the real Raft transport.
    let smoke_req = RaftRequest::Append {
        context: "default".to_string(),
        events: vec![unconditional_event(8888, b"smoke-reject")],
        condition: Some(RaftAppendCondition {
            consistency_marker: 0,
            criteria: vec![RaftCriterion {
                names: vec![],
                tags: vec![(b"orderId".to_vec(), b"order-2".to_vec())],
            }],
        }),
    };
    let smoke_resp = leader_raft
        .client_write(smoke_req)
        .await
        .expect("smoke client_write")
        .data;
    match smoke_resp {
        RaftResponse::AppendRejected { .. } => {}
        other => panic!("smoke test expected cluster-transport AppendRejected, got {other:?}"),
    }

    // cleanup (tempdirs drop via their handles). Avoid touching node.addr here —
    // all nodes are still owned, RAII handles the cleanup.
    let _ = &node1.addr;
    let _ = &node2.addr;
    let _ = &node3.addr;
    drop((dir1, dir2, dir3));
}
