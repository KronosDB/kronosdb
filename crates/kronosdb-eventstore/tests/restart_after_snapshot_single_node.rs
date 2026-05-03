//! Regression test for the membership-after-restart-purge bug.
//!
//! Reproduces the failure mode found in production data on 2026-05-03:
//! a single-voter cluster appends past the snapshot threshold, which causes
//! openraft to (a) build a snapshot and (b) purge log entries up through the
//! snapshot's `last_log_id`. Because `EventStoreStateMachine::new` does not
//! recover `last_membership` from any persistent source, on the next restart
//! `applied_state()` returns `StoredMembership::default()` — which openraft
//! treats as "no voters known," defaulting the node to Learner. Learners
//! never campaign, so the node refuses to elect a leader and writes hang
//! forever.
//!
//! Pre-fix: this test must fail at `wait_for_leader` after restart.
//! Post-fix: this test must elect a leader and accept a fresh append.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use openraft::{BasicNode, Config, Raft, SnapshotPolicy};

use kronosdb_eventstore::api::EventStore;
use kronosdb_eventstore::append::AppendRequest;
use kronosdb_eventstore::context::ContextManager;
use kronosdb_eventstore::event::{AppendEvent, Tag};
use kronosdb_eventstore::raft::cluster::RaftEngine;
use kronosdb_eventstore::raft::log_store::{LogStore, LogStoreConfig};
use kronosdb_eventstore::raft::network::NetworkFactory;
use kronosdb_eventstore::raft::snapshot_store::SnapshotStore;
use kronosdb_eventstore::raft::state_machine::EventStoreStateMachine;
use kronosdb_eventstore::raft::types::{NodeId, TypeConfig};
use kronosdb_eventstore::segment::DEFAULT_SEGMENT_SIZE;

/// Aggressive snapshot threshold so we cross it within a small write count.
/// The bug triggers as soon as openraft purges past the cluster-init
/// Membership entry (index 1), which happens on the first purge.
const SNAPSHOT_THRESHOLD: u64 = 16;

/// Number of appends — needs to be enough to trigger at least one snapshot
/// AND a follow-up purge. With LogsSinceLast(16), one snapshot fires after
/// ~16 entries; a buffer past that ensures purge has settled before we
/// shut down.
const APPEND_COUNT: u64 = 64;

struct TestNode {
    raft: Arc<Raft<TypeConfig>>,
    contexts: Arc<ContextManager>,
    engine: Arc<RaftEngine>,
}

async fn start_single_node(id: NodeId, dir: &Path) -> TestNode {
    let contexts =
        Arc::new(ContextManager::new(dir, DEFAULT_SEGMENT_SIZE).expect("create context manager"));
    if !contexts.context_exists("default") {
        contexts
            .create_context("default")
            .expect("create default context");
    }

    let raft_dir = dir.join("default").join("raft");
    let log_store = LogStore::new(&raft_dir, LogStoreConfig::default()).expect("create log store");
    let snap_store =
        Arc::new(SnapshotStore::new(raft_dir.join("snapshots")).expect("create snapshot store"));
    let state_machine = EventStoreStateMachine::new(Arc::clone(&contexts), snap_store)
        .expect("recover state machine");

    let config = Config {
        heartbeat_interval: 100,
        election_timeout_min: 300,
        election_timeout_max: 600,
        snapshot_policy: SnapshotPolicy::LogsSinceLast(SNAPSHOT_THRESHOLD),
        // Purge as soon as the snapshot is built so the cluster-init
        // Membership entry at index 1 actually leaves the log — without
        // this the bug doesn't reproduce (the entry remains and recovery
        // works trivially via log scan).
        max_in_snapshot_log_to_keep: 0,
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
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

async fn wait_for_purge(raft: &Raft<TypeConfig>, timeout: Duration) -> Option<u64> {
    let start = tokio::time::Instant::now();
    loop {
        let last_purged = raft.metrics().borrow().purged.as_ref().map(|id| id.index);
        if let Some(idx) = last_purged
            && idx > 0
        {
            return Some(idx);
        }
        if start.elapsed() > timeout {
            return last_purged;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

fn order_event(i: u64) -> AppendEvent {
    AppendEvent {
        identifier: format!("evt-{i}"),
        name: "OrderPlaced".into(),
        version: "1.0".into(),
        timestamp: 1_712_345_678_000,
        payload: format!("payload-{i}").into_bytes(),
        metadata: vec![],
        tags: vec![Tag::from_str("orderId", &format!("order-{i}"))],
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restart_after_snapshot_purge_recovers_leader_and_writes() {
    tokio::time::timeout(Duration::from_secs(60), run())
        .await
        .expect("test timed out");
}

async fn run() {
    let dir = tempfile::tempdir().expect("tempdir");

    // --- Phase 1: bring up cluster, drive past the snapshot threshold ---
    let node = start_single_node(1, dir.path()).await;

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
        .expect("leader elected pre-restart");

    for i in 0..APPEND_COUNT {
        node.engine
            .append(AppendRequest {
                condition: None,
                events: vec![order_event(i)],
            })
            .await
            .expect("append pre-restart");
    }

    // Wait for the snapshot machinery to actually purge — without this the
    // restart wouldn't reproduce the bug because the cluster-init Membership
    // entry at index 1 would still be present in the log.
    let purged_index = wait_for_purge(&node.raft, Duration::from_secs(10))
        .await
        .expect("openraft never purged after crossing snapshot threshold");
    assert!(
        purged_index >= 1,
        "purge must cover index 1 (cluster-init membership) for this regression test \
         to reproduce the bug; purged_index={purged_index}"
    );

    // Clean shutdown so on-disk state is settled.
    let _ = node.raft.shutdown().await;
    drop(node);

    // --- Phase 2: restart from the same dir; nothing crashed, just a normal
    // restart that exercises the recovery path ---
    let restarted = start_single_node(1, dir.path()).await;

    // The bug: this elect-loop never completes because the node thinks it's
    // a Learner (no membership recovered).
    let leader = wait_for_leader(&restarted.raft, Duration::from_secs(10)).await;
    assert!(
        leader.is_some(),
        "post-restart: no leader elected within 10s — node is stuck as Learner because \
         last_membership was lost when the cluster-init Membership entry was purged"
    );

    // Sanity: writes work after restart.
    restarted
        .engine
        .append(AppendRequest {
            condition: None,
            events: vec![order_event(APPEND_COUNT)],
        })
        .await
        .expect("post-restart append must succeed");

    // Sanity: the new event landed in the segment.
    let store = restarted.contexts.get_context("default").unwrap();
    assert!(
        store.head().0 > APPEND_COUNT,
        "head must advance past pre-restart count after the post-restart append; \
         got head={}",
        store.head().0
    );

    let _ = restarted.raft.shutdown().await;
}
