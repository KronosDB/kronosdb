//! Integration test: boots a real 3-node Raft cluster with gRPC transport,
//! appends events on the leader, and verifies all followers replicate the data.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use openraft::{BasicNode, Config, Raft};
use tonic::transport::Server;

use kronosdb_eventstore::api::EventStore;
use kronosdb_eventstore::context::ContextManager;
use kronosdb_eventstore::criteria::{Criterion, SourcingCondition};
use kronosdb_eventstore::event::Position;
use kronosdb_eventstore::segment::DEFAULT_SEGMENT_SIZE;
use kronosdb_raft::log_store::LogStore;
use kronosdb_raft::network::NetworkFactory;
use kronosdb_raft::node::RaftEventStore;
use kronosdb_raft::state_machine::EventStoreStateMachine;
use kronosdb_raft::transport::RaftTransportService;
use kronosdb_raft::types::{NodeId, RaftAppendEvent, RaftRequest, RaftResponse, TypeConfig};

struct TestNode {
    #[allow(dead_code)]
    id: NodeId,
    raft: Arc<Raft<TypeConfig>>,
    contexts: Arc<ContextManager>,
    #[allow(dead_code)]
    addr: SocketAddr,
}

/// Creates a Raft node and starts its gRPC transport server.
async fn start_node(id: NodeId, port: u16, dir: &std::path::Path) -> TestNode {
    let contexts = Arc::new(
        ContextManager::new(dir, DEFAULT_SEGMENT_SIZE).expect("create context manager"),
    );
    if !contexts.context_exists("default") {
        contexts.create_context("default").expect("create default");
    }

    let raft_dir = dir.join("raft");
    let log_store = LogStore::new(&raft_dir).expect("create log store");
    let state_machine = EventStoreStateMachine::new(Arc::clone(&contexts));

    let config = Config {
        heartbeat_interval: 200,
        election_timeout_min: 500,
        election_timeout_max: 1000,
        ..Default::default()
    };

    let raft = Arc::new(
        Raft::new(id, Arc::new(config), NetworkFactory, log_store, state_machine)
            .await
            .expect("create raft node"),
    );

    // Start gRPC transport server.
    let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let transport = RaftTransportService::new(Arc::clone(&raft));

    tokio::spawn(async move {
        Server::builder()
            .add_service(transport.into_server())
            .serve(addr)
            .await
            .expect("transport server failed");
    });

    // Give the server a moment to bind.
    tokio::time::sleep(Duration::from_millis(100)).await;

    TestNode {
        id,
        raft,
        contexts,
        addr,
    }
}

fn make_event(name: &str, id: &str) -> RaftAppendEvent {
    RaftAppendEvent {
        identifier: id.to_string(),
        name: name.to_string(),
        version: "1.0".to_string(),
        timestamp: 1712345678000,
        payload: format!("payload-{id}").into_bytes(),
        metadata: vec![],
        tags: vec![(b"orderId".to_vec(), id.as_bytes().to_vec())],
    }
}

/// Wait until a node sees a leader.
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

/// Wait until a node's event store reaches the expected head position.
async fn wait_for_head(
    contexts: &ContextManager,
    context: &str,
    expected: Position,
    timeout: Duration,
) -> bool {
    let start = tokio::time::Instant::now();
    loop {
        if let Ok(store) = contexts.get_context(context) {
            if store.head() >= expected {
                return true;
            }
        }
        if start.elapsed() > timeout {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test]
async fn three_node_cluster_replication() {
    let dir = tempfile::tempdir().unwrap();
    let base_port = 19100; // Use high ports to avoid conflicts.

    // Start 3 nodes with real gRPC transport.
    let node1 = start_node(1, base_port, &dir.path().join("node1")).await;
    let node2 = start_node(2, base_port + 1, &dir.path().join("node2")).await;
    let node3 = start_node(3, base_port + 2, &dir.path().join("node3")).await;

    // Initialize the cluster with all 3 nodes as voters.
    let mut members = BTreeMap::new();
    members.insert(1, BasicNode { addr: format!("127.0.0.1:{}", base_port) });
    members.insert(2, BasicNode { addr: format!("127.0.0.1:{}", base_port + 1) });
    members.insert(3, BasicNode { addr: format!("127.0.0.1:{}", base_port + 2) });

    node1
        .raft
        .initialize(members)
        .await
        .expect("initialize cluster");

    // Wait for a leader to be elected.
    let leader_id = wait_for_leader(&node1.raft, Duration::from_secs(10))
        .await
        .expect("leader should be elected");

    println!("Leader elected: node {leader_id}");

    // Find the leader node.
    let leader = match leader_id {
        1 => &node1,
        2 => &node2,
        3 => &node3,
        _ => panic!("unexpected leader id"),
    };

    // Append events through the leader.
    let request = RaftRequest::Append {
        context: "default".to_string(),
        events: vec![
            make_event("OrderPlaced", "order-1"),
            make_event("OrderPlaced", "order-2"),
            make_event("PaymentReceived", "order-1"),
        ],
        condition: None,
    };

    let response = leader.raft.client_write(request).await.expect("append events");
    match &response.data {
        RaftResponse::Append { first_position, count, .. } => {
            assert_eq!(*first_position, 1);
            assert_eq!(*count, 3);
            println!("Appended 3 events on leader (positions 1-3)");
        }
        other => panic!("unexpected response: {:?}", other),
    }

    // Wait for all nodes to replicate.
    let expected_head = Position(4); // 3 events → head at 4
    let timeout = Duration::from_secs(10);

    assert!(
        wait_for_head(&node1.contexts, "default", expected_head, timeout).await,
        "node 1 should have head >= 4"
    );
    assert!(
        wait_for_head(&node2.contexts, "default", expected_head, timeout).await,
        "node 2 should have head >= 4"
    );
    assert!(
        wait_for_head(&node3.contexts, "default", expected_head, timeout).await,
        "node 3 should have head >= 4"
    );

    println!("All 3 nodes replicated to head >= {}", expected_head.0);

    // Verify we can read from each follower.
    let condition = SourcingCondition {
        criteria: vec![Criterion {
            names: vec!["OrderPlaced".to_string(), "PaymentReceived".to_string()],
            tags: vec![],
        }],
    };

    for (name, node) in [("node1", &node1), ("node2", &node2), ("node3", &node3)] {
        let store = node.contexts.get_context("default").unwrap();
        let events = store.source(Position(1), &condition).unwrap();
        assert_eq!(events.len(), 3, "{name} should have 3 events");
        assert_eq!(events[0].name, "OrderPlaced");
        assert_eq!(events[1].name, "OrderPlaced");
        assert_eq!(events[2].name, "PaymentReceived");
        println!("{name}: verified 3 events readable");
    }

    // Append more events and verify replication again.
    let request2 = RaftRequest::Append {
        context: "default".to_string(),
        events: vec![
            make_event("OrderShipped", "order-1"),
            make_event("OrderShipped", "order-2"),
        ],
        condition: None,
    };

    leader.raft.client_write(request2).await.expect("append more events");

    let expected_head2 = Position(6);
    for (name, node) in [("node1", &node1), ("node2", &node2), ("node3", &node3)] {
        assert!(
            wait_for_head(&node.contexts, "default", expected_head2, timeout).await,
            "{name} should replicate to head >= 6"
        );
    }

    println!("Second batch replicated. All nodes at head >= 6.");

    let _ = node1.raft.shutdown().await;
    let _ = node2.raft.shutdown().await;
    let _ = node3.raft.shutdown().await;
}

#[tokio::test]
async fn passive_backup_receives_replicated_events() {
    let dir = tempfile::tempdir().unwrap();
    let base_port = 19300;

    // Start 2 voters + 1 passive backup (learner).
    let voter1 = start_node(1, base_port, &dir.path().join("voter1")).await;
    let voter2 = start_node(2, base_port + 1, &dir.path().join("voter2")).await;
    let backup = start_node(10, base_port + 2, &dir.path().join("backup")).await;

    // Initialize cluster with only the 2 voters.
    let mut members = BTreeMap::new();
    members.insert(1, BasicNode { addr: format!("127.0.0.1:{}", base_port) });
    members.insert(2, BasicNode { addr: format!("127.0.0.1:{}", base_port + 1) });

    voter1.raft.initialize(members).await.expect("initialize");

    let leader_id = wait_for_leader(&voter1.raft, Duration::from_secs(10))
        .await
        .expect("leader elected");
    println!("Leader: node {leader_id}");

    let leader = if leader_id == 1 { &voter1 } else { &voter2 };

    // Wait for initial membership to settle before adding learner.
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Add the backup as a learner.
    leader.raft
        .add_learner(10, BasicNode { addr: format!("127.0.0.1:{}", base_port + 2) }, true)
        .await
        .expect("add learner");
    println!("Backup node 10 added as learner");

    // Append events on the leader.
    let request = RaftRequest::Append {
        context: "default".to_string(),
        events: vec![
            make_event("OrderPlaced", "order-1"),
            make_event("OrderPlaced", "order-2"),
            make_event("PaymentReceived", "order-1"),
        ],
        condition: None,
    };
    leader.raft.client_write(request).await.expect("append");
    println!("Appended 3 events on leader");

    // Wait for all nodes (including backup) to receive events.
    let timeout = Duration::from_secs(10);
    let expected = Position(4);

    assert!(
        wait_for_head(&voter1.contexts, "default", expected, timeout).await,
        "voter1 should replicate"
    );
    assert!(
        wait_for_head(&voter2.contexts, "default", expected, timeout).await,
        "voter2 should replicate"
    );
    assert!(
        wait_for_head(&backup.contexts, "default", expected, timeout).await,
        "backup should replicate (as learner)"
    );

    println!("All nodes (including passive backup) have head >= {}", expected.0);

    // Verify backup can serve reads.
    let condition = SourcingCondition {
        criteria: vec![Criterion {
            names: vec!["OrderPlaced".to_string(), "PaymentReceived".to_string()],
            tags: vec![],
        }],
    };
    let backup_store = backup.contexts.get_context("default").unwrap();
    let events = backup_store.source(Position(1), &condition).unwrap();
    assert_eq!(events.len(), 3);
    println!("Backup serving reads: {} events", events.len());

    let _ = voter1.raft.shutdown().await;
    let _ = voter2.raft.shutdown().await;
    let _ = backup.raft.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn follower_forwards_writes_to_leader() {
    let dir = tempfile::tempdir().unwrap();
    let base_port = 19200;

    let node1 = start_node(1, base_port, &dir.path().join("node1")).await;
    let node2 = start_node(2, base_port + 1, &dir.path().join("node2")).await;
    let node3 = start_node(3, base_port + 2, &dir.path().join("node3")).await;

    let mut members = BTreeMap::new();
    members.insert(1, BasicNode { addr: format!("127.0.0.1:{}", base_port) });
    members.insert(2, BasicNode { addr: format!("127.0.0.1:{}", base_port + 1) });
    members.insert(3, BasicNode { addr: format!("127.0.0.1:{}", base_port + 2) });

    node1.raft.initialize(members).await.expect("initialize");

    let leader_id = wait_for_leader(&node1.raft, Duration::from_secs(10))
        .await
        .expect("leader elected");
    // Wait for all nodes to see the leader.
    wait_for_leader(&node2.raft, Duration::from_secs(10)).await;
    wait_for_leader(&node3.raft, Duration::from_secs(10)).await;

    println!("Leader: node {leader_id}");

    // Find a follower node.
    let follower = if leader_id == 1 { &node2 } else { &node1 };
    println!("Writing through follower node {}", follower.id);

    // Create a RaftEventStore wrapping the follower's Raft node.
    let follower_engine = follower.contexts.get_context("default").unwrap();
    let follower_store = RaftEventStore::new(
        Arc::clone(&follower.raft),
        follower_engine,
        "default".to_string(),
    );

    // Append through the follower — should forward to leader.
    let append_req = kronosdb_eventstore::append::AppendRequest {
        condition: None,
        events: vec![
            kronosdb_eventstore::event::AppendEvent {
                identifier: "evt-1".to_string(),
                name: "OrderPlaced".to_string(),
                version: "1.0".to_string(),
                timestamp: 1712345678000,
                payload: b"test".to_vec(),
                metadata: vec![],
                tags: vec![kronosdb_eventstore::event::Tag {
                    key: b"orderId".to_vec(),
                    value: b"order-1".to_vec(),
                }],
            },
        ],
    };

    let result = follower_store.append(append_req);
    assert!(result.is_ok(), "follower append should succeed via forwarding: {:?}", result.err());

    let resp = result.unwrap();
    assert_eq!(resp.first_position, Position(1));
    assert_eq!(resp.count, 1);
    println!("Follower write forwarded to leader, got position {}", resp.first_position.0);

    // Verify all nodes have the event.
    let timeout = Duration::from_secs(10);
    let expected = Position(2);
    for (name, node) in [("node1", &node1), ("node2", &node2), ("node3", &node3)] {
        assert!(
            wait_for_head(&node.contexts, "default", expected, timeout).await,
            "{name} should have head >= 2"
        );
        println!("{name}: replicated");
    }

    let _ = node1.raft.shutdown().await;
    let _ = node2.raft.shutdown().await;
    let _ = node3.raft.shutdown().await;
}
