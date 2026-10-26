//! Messaging-fabric e2e (ADR-0007 Tier 2): a command dispatched on node A
//! reaches a handler connected to node B and the response flows back.
//!
//! Two real server processes form a Raft cluster. The handler registers on
//! B only; its registration replicates through the metadata Raft into A's
//! routing table, A's dispatch resolves the remote owner and forwards over
//! a native peer connection. Queries gather across nodes, subscriptions
//! relay, and node death is asserted to fail fast (retriable error), not
//! hang.

use std::net::TcpListener;
use std::process::{Child, Command as ProcCommand, Stdio};
use std::time::Duration;

use bytes::Bytes;
use kronosdb_client::{
    Client, Command, ConnectOptions, ErrorCode, Payload, Query, QueryHandlerEvent, Reply,
    SubscriptionEvent,
};

struct Node {
    child: Child,
    addr: String,
    admin: String,
}

impl Drop for Node {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn free_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

fn spawn_node(node_id: u64, port: u16, admin_port: u16, peers: &[(u64, u16)]) -> Node {
    let data_dir = format!(
        "{}/fabric-{}-node-{}",
        env!("CARGO_TARGET_TMPDIR"),
        std::process::id(),
        node_id
    );
    let peer_str = peers
        .iter()
        .map(|(id, port)| format!("{id}=127.0.0.1:{port}"))
        .collect::<Vec<_>>()
        .join(",");

    let mut cmd = ProcCommand::new(env!("CARGO_BIN_EXE_kronosdb-server"));
    // The client port is also the address the cluster advertises: peers
    // reach each other's handlers there.
    cmd.env("KRONOSDB_LISTEN", format!("127.0.0.1:{port}"))
        .env("KRONOSDB_ADMIN_LISTEN", format!("127.0.0.1:{admin_port}"))
        .env("KRONOSDB_DATA_DIR", &data_dir)
        .env("KRONOSDB_NODE_NAME", format!("fabric-node-{node_id}"))
        .env("KRONOSDB_CLUSTER_NODE_ID", node_id.to_string())
        .env("KRONOSDB_CLUSTER_PEERS", &peer_str)
        .env("RUST_LOG", "warn");
    if std::env::var_os("KRONOSDB_TEST_LOGS").is_some() {
        cmd.stdout(Stdio::inherit()).stderr(Stdio::inherit());
    } else {
        cmd.stdout(Stdio::null()).stderr(Stdio::null());
    }
    Node {
        child: cmd.spawn().expect("spawn kronosdb-server"),
        addr: format!("127.0.0.1:{port}"),
        admin: format!("127.0.0.1:{admin_port}"),
    }
}

/// Waits until the node can serve: a leader claim is committed.
async fn wait_ready(node: &Node) {
    for _ in 0..300 {
        if let Ok(response) = reqwest::get(format!("http://{}/ready", node.admin)).await
            && response.status().is_success()
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("{} never became ready", node.addr);
}

async fn connect(node: &Node) -> Client {
    for _ in 0..100 {
        if let Ok(client) = Client::connect(&node.addr, &ConnectOptions::default()).await {
            return client;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    panic!("could not connect to {}", node.addr);
}

fn payload(text: &str) -> Payload {
    Payload::new("String", Bytes::from(text.to_string()))
}

fn text(reply: &Reply) -> String {
    String::from_utf8_lossy(&reply.payload.as_ref().expect("a payload").data).to_string()
}

/// Serves `command_name` on `client`: echoes every command's payload back.
async fn start_echo_handler(client: &Client, command_name: &str, client_id: &str) {
    let mut handlers = client
        .default_bus()
        .command_handlers(client_id, "fabric-test", 1_000)
        .await
        .unwrap();
    handlers.subscribe(command_name, 100).await.unwrap();
    tokio::spawn(async move {
        while let Ok(Some(incoming)) = handlers.next().await {
            let reply = Reply::ok(
                incoming.command.message_id.clone(),
                incoming.command.payload.clone(),
            );
            if incoming.respond(&reply).await.is_err() {
                break;
            }
        }
    });
}

fn make_command(seq: u32, name: &str, routing_key: Option<&str>) -> Command {
    let mut command = Command::new(
        format!("fabric-cmd-{seq}"),
        name,
        payload(&format!("payload-{seq}")),
    );
    command.client_id = "fabric-dispatcher".into();
    command.component_name = "fabric-test".into();
    command.routing_key = routing_key.map(str::to_string);
    command
}

/// Retries dispatch until the handler registration has replicated and the
/// cluster has settled — registration rides a Raft round and leader
/// election takes a moment after spawn.
async fn dispatch_until_ok(client: &Client, name: &str, routing_key: Option<&str>) -> Reply {
    for seq in 1..=150 {
        match client
            .default_bus()
            .dispatch_command(&make_command(seq, name, routing_key))
            .await
        {
            Ok(reply) => return reply,
            Err(_) => tokio::time::sleep(Duration::from_millis(200)).await,
        }
    }
    panic!("dispatch never succeeded for {name}");
}

/// Serves `query_name`: answers every query with one result tagged with this
/// handler's id; answers a subscription query with an initial result and one
/// update.
async fn start_query_handler(client: &Client, query_name: &str, client_id: &str) {
    let mut handlers = client
        .default_bus()
        .query_handlers(client_id, "fabric-test", 1_000)
        .await
        .unwrap();
    handlers.subscribe(query_name, "String").await.unwrap();
    let responder = client_id.to_string();
    tokio::spawn(async move {
        while let Ok(Some(event)) = handlers.next().await {
            match event {
                QueryHandlerEvent::Subscription(sub) => {
                    let id = sub.subscription_id.clone();
                    let initial = Reply::ok(id, payload(&format!("initial-from-{responder}")));
                    if sub.initial(&initial).await.is_err()
                        || sub
                            .update(payload(&format!("update-from-{responder}")))
                            .await
                            .is_err()
                    {
                        break;
                    }
                }
                QueryHandlerEvent::Query(incoming) => {
                    let id = incoming.query.message_id.clone();
                    let answer = Reply::ok(id, payload(&format!("answer-from-{responder}")));
                    if incoming.respond(&answer).await.is_err() {
                        break;
                    }
                }
                _ => {}
            }
        }
    });
}

/// Retries a scatter-gather query until it yields at least
/// `expected_responses` answers within one collection window.
async fn query_until_responses(
    client: &Client,
    query_name: &str,
    expected_responses: usize,
) -> Vec<String> {
    for attempt in 0..150u32 {
        let mut query = Query::new(format!("fabric-query-{attempt}"), query_name, payload(""));
        query.client_id = "fabric-querier".into();
        query.component_name = "fabric-test".into();
        if let Ok(results) = client.default_bus().dispatch_query(&query).await
            && let Ok(results) =
                tokio::time::timeout(Duration::from_secs(5), results.collect()).await
            && let Ok(results) = results
        {
            let answers: Vec<String> = results.iter().map(text).collect();
            if answers.len() >= expected_responses {
                return answers;
            }
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    panic!("scatter-gather never yielded {expected_responses} responses for {query_name}");
}

/// Opens a subscription query and collects events until an initial result
/// AND an update arrive; retries while registrations replicate.
async fn subscribe_until_events(client: &Client, query_name: &str) -> Vec<String> {
    for attempt in 0..150u32 {
        let id = format!("fabric-sub-{attempt}");
        let mut query = Query::new(id.clone(), query_name, payload(""));
        query.client_id = "fabric-subscriber".into();
        query.component_name = "fabric-test".into();
        if let Ok(mut stream) = client
            .default_bus()
            .subscription_query(&id, &query, 64)
            .await
        {
            let mut events = Vec::new();
            while let Ok(Ok(Some(event))) =
                tokio::time::timeout(Duration::from_secs(3), stream.next()).await
            {
                match event {
                    SubscriptionEvent::Initial(r) => events.push(text(&r)),
                    SubscriptionEvent::Update {
                        payload: Some(p), ..
                    } => events.push(String::from_utf8_lossy(&p.data).to_string()),
                    _ => {}
                }
                if events.len() >= 2 {
                    return events;
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    panic!("subscription never yielded initial+update for {query_name}");
}

#[tokio::test(flavor = "multi_thread")]
async fn command_dispatched_on_a_reaches_handler_on_b() {
    let port_a = free_port();
    let port_b = free_port();
    let admin_a = free_port();
    let admin_b = free_port();
    let peers = [(1u64, port_a), (2u64, port_b)];

    let node_a = spawn_node(1, port_a, admin_a, &peers);
    let node_b = spawn_node(2, port_b, admin_b, &peers);

    wait_ready(&node_a).await;
    wait_ready(&node_b).await;
    let client_a = connect(&node_a).await;
    let client_b = connect(&node_b).await;

    // Handler lives ONLY on node B.
    start_echo_handler(&client_b, "FabricEcho", "handler-on-b").await;

    // Dispatch through node A: the routing table says the owner is on B, so
    // the command crosses the fabric and the response crosses back.
    let reply = dispatch_until_ok(&client_a, "FabricEcho", None).await;
    assert!(
        text(&reply).starts_with("payload-"),
        "expected echoed payload, got {reply:?}"
    );

    // Keyed dispatch takes the ring path to the same remote handler.
    let reply = dispatch_until_ok(&client_a, "FabricEcho", Some("order-42")).await;
    assert!(reply.is_ok() && reply.payload.is_some(), "{reply:?}");

    // ── Scatter-gather across the fabric ────────────────────────────
    // Query handlers on BOTH nodes; a query through node A must gather
    // answers from the local handler and the remote one.
    start_query_handler(&client_a, "FabricGather", "query-handler-a").await;
    start_query_handler(&client_b, "FabricGather", "query-handler-b").await;

    let answers = query_until_responses(&client_a, "FabricGather", 2).await;
    assert!(
        answers.iter().any(|a| a == "answer-from-query-handler-a"),
        "missing local answer: {answers:?}"
    );
    assert!(
        answers.iter().any(|a| a == "answer-from-query-handler-b"),
        "missing remote answer: {answers:?}"
    );

    // Remote-only point-to-point: handler exists ONLY on B, query via A.
    start_query_handler(&client_b, "FabricRemoteOnly", "query-handler-b2").await;
    let answers = query_until_responses(&client_a, "FabricRemoteOnly", 1).await;
    assert_eq!(answers, vec!["answer-from-query-handler-b2".to_string()]);

    // ── Subscription relay across the fabric ────────────────────────
    // Subscription-query handler ONLY on B; subscriber connects to A.
    // Initial result and update must relay B → A → subscriber.
    start_query_handler(&client_b, "FabricSubQ", "sub-handler-b").await;
    let events = subscribe_until_events(&client_a, "FabricSubQ").await;
    assert!(
        events.contains(&"initial-from-sub-handler-b".to_string()),
        "missing relayed initial result: {events:?}"
    );
    assert!(
        events.contains(&"update-from-sub-handler-b".to_string()),
        "missing relayed update: {events:?}"
    );

    // Kill the owning node: dispatch must fail fast with a retriable
    // error, not hang. (The registry row lingers until membership or
    // restart cleanup — the forward itself surfaces the failure.)
    drop(node_b);
    tokio::time::sleep(Duration::from_millis(300)).await;
    let result = tokio::time::timeout(
        Duration::from_secs(10),
        client_a
            .default_bus()
            .dispatch_command(&make_command(9_999, "FabricEcho", None)),
    )
    .await;
    match result {
        Ok(Err(e)) => assert_eq!(
            e.failure().map(|f| f.code),
            Some(ErrorCode::UNAVAILABLE),
            "retriable: {e}"
        ),
        Ok(Ok(reply)) => panic!("dispatch succeeded against a dead handler node: {reply:?}"),
        Err(_) => panic!("dispatch hung after handler node death"),
    }
}

/// A handler that registers while the cluster has no leader (here: one of
/// two voters is not up yet, so there is no quorum) still reaches the
/// routing table once a leader exists — the only way a command dispatched on
/// the other node can find it.
#[tokio::test(flavor = "multi_thread")]
async fn a_handler_registered_before_a_leader_exists_is_routed_once_there_is_one() {
    let (port_a, port_b) = (free_port(), free_port());
    let (admin_a, admin_b) = (free_port(), free_port());
    let peers = [(11u64, port_a), (12u64, port_b)];

    let node_a = spawn_node(11, port_a, admin_a, &peers);
    let client_a = connect(&node_a).await;
    start_echo_handler(&client_a, "LateEcho", "handler-before-leader").await;
    // Long enough for the first registration attempts to find no leader.
    tokio::time::sleep(Duration::from_secs(2)).await;

    let node_b = spawn_node(12, port_b, admin_b, &peers);
    let client_b = connect(&node_b).await;
    let reply = dispatch_until_ok(&client_b, "LateEcho", None).await;
    assert!(text(&reply).starts_with("payload-"), "{reply:?}");
    drop(node_a);
}
