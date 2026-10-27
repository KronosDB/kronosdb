//! The hub end to end without a wire: handler streams are the channels the
//! hub gives them, and the "client" is a test driving those channels. The
//! cluster is never started, so registrations in the replicated table only
//! log a warning and dispatch uses the node-local path.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use kronosdb_eventstore::context::ContextManager;
use kronosdb_eventstore::raft::cluster::{ClusterConfig, ClusterManager, NodeType, PeerConfig};
use kronosdb_eventstore::raft::types::default_raft_config;
use kronosdb_eventstore::replication::peer::PeerTransportConfig;
use kronosdb_messaging::client::ClientRegistry;
use kronosdb_messaging::command::{Command, CommandResult};
use kronosdb_messaging::manager::MessagingManager;
use kronosdb_messaging::query::{Query, QueryResult};
use kronosdb_messaging::subscription::{SubscriptionQuery, SubscriptionUpdate};
use kronosdb_messaging::types::{ClientId, ComponentName, Payload};

use super::*;
use crate::processor::ProcessorRegistry;

struct Fixture {
    hub: Arc<Hub>,
    _dir: tempfile::TempDir,
}

fn fixture() -> Fixture {
    fixture_with(Duration::from_secs(5))
}

fn fixture_with(command_timeout: Duration) -> Fixture {
    let dir = tempfile::tempdir_in(concat!(env!("CARGO_MANIFEST_DIR"), "/../../target")).unwrap();
    let contexts = Arc::new(ContextManager::new(dir.path(), 1 << 20).unwrap());
    let cluster = Arc::new(ClusterManager::new(
        contexts,
        ClusterConfig {
            node_id: 1,
            node_type: NodeType::Standard,
            advertise_addr: "127.0.0.1:1".into(),
            voters: vec![PeerConfig {
                id: 1,
                addr: "127.0.0.1:1".into(),
            }],
            learners: vec![],
            raft_config: default_raft_config(),
            peer_transport: PeerTransportConfig::default(),
        },
    ));
    let hub = Hub::new(
        HubConfig {
            node_name: "test-node".into(),
            command_timeout,
            query_timeout: Duration::from_secs(5),
            heartbeat_interval: Duration::from_secs(10),
            heartbeat_timeout: Duration::from_secs(15),
        },
        Shared {
            manager: Arc::new(MessagingManager::new()),
            client_registry: Arc::new(ClientRegistry::new()),
            processors: Arc::new(ProcessorRegistry::new()),
        },
        cluster,
        Arc::new(peers::NoPeers),
        Arc::new(|| vec!["default".to_string()]),
    );
    Fixture { hub, _dir: dir }
}

fn payload(text: &'static str) -> Payload {
    Payload {
        payload_type: "text".into(),
        revision: "1".into(),
        data: Bytes::from_static(text.as_bytes()),
    }
}

fn command(id: &str, name: &str) -> Command {
    Command {
        message_id: id.into(),
        name: name.into(),
        timestamp: 0,
        payload: payload("cmd"),
        metadata: HashMap::new(),
        processing_instructions: vec![],
        routing_key: None,
        client_id: ClientId("caller".into()),
        component_name: ComponentName("tests".into()),
    }
}

fn ok_result(request_id: &str) -> CommandResult {
    CommandResult {
        message_id: String::new(),
        request_id: request_id.into(),
        error_code: None,
        error: None,
        payload: Some(payload("done")),
        metadata: HashMap::new(),
        processing_instructions: vec![],
    }
}

fn query(id: &str, name: &str, expected: i32) -> Query {
    Query {
        message_id: id.into(),
        name: name.into(),
        timestamp: 0,
        payload: payload("q"),
        metadata: HashMap::new(),
        processing_instructions: vec![],
        client_id: ClientId("caller".into()),
        component_name: ComponentName("tests".into()),
        expected_results: expected,
    }
}

fn query_result(request_id: &str, answer: &'static str) -> QueryResult {
    QueryResult {
        message_id: String::new(),
        request_id: request_id.into(),
        error_code: None,
        error: None,
        payload: Some(payload(answer)),
        metadata: HashMap::new(),
        processing_instructions: vec![],
    }
}

fn who(id: &str) -> Who {
    Who {
        client_id: id.into(),
        component_name: "svc".into(),
    }
}

async fn recv<T>(rx: &mut tokio::sync::mpsc::Receiver<T>) -> T {
    tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("a message in time")
        .expect("an open channel")
}

#[tokio::test]
async fn a_command_reaches_its_handler_and_the_result_comes_back() {
    let f = fixture();
    let bus = f.hub.bus("default");
    let (mut handlers, mut queue) = f.hub.open_command_handlers(Arc::clone(&bus), who("h1"));
    handlers.subscribe("Echo", 100);
    handlers.grant(10);

    let dispatch = {
        let hub = Arc::clone(&f.hub);
        let bus = Arc::clone(&bus);
        tokio::spawn(async move { hub.dispatch_command(&bus, command("c1", "Echo")).await })
    };
    let ToCommandHandler::Command(delivered) = recv(&mut queue).await;
    assert_eq!(delivered.message_id, "c1");
    handlers.complete(ok_result("c1"));

    let result = dispatch.await.unwrap().unwrap();
    assert_eq!(result.request_id, "c1");
    assert!(result.error_code.is_none());
}

#[tokio::test]
async fn a_command_nobody_handles_is_a_fault() {
    let f = fixture();
    let bus = f.hub.bus("default");
    let fault = f
        .hub
        .dispatch_command(&bus, command("c1", "Nobody"))
        .await
        .unwrap_err();
    assert_eq!(fault.kind, FaultKind::Unavailable);
}

#[tokio::test]
async fn a_handler_that_never_answers_times_out_in_band_and_frees_its_slot() {
    let f = fixture_with(Duration::from_millis(200));
    let bus = f.hub.bus("default");
    let (mut handlers, mut queue) = f.hub.open_command_handlers(Arc::clone(&bus), who("h1"));
    handlers.subscribe("Slow", 100);
    handlers.grant(10);

    // The default budget is 200ms; the message says nothing, so it applies.
    let result = f
        .hub
        .dispatch_command(&bus, command("c1", "Slow"))
        .await
        .unwrap();
    assert_eq!(result.error_code.as_deref(), Some("KRONOSDB-4005"));
    let _ = recv(&mut queue).await;
    assert_eq!(
        bus.engine.command_bus().in_flight_count(),
        0,
        "the slot is free"
    );
}

#[tokio::test]
async fn abandoning_a_dispatch_frees_its_slot() {
    let f = fixture();
    let bus = f.hub.bus("default");
    let (mut handlers, mut queue) = f.hub.open_command_handlers(Arc::clone(&bus), who("h1"));
    handlers.subscribe("Slow", 100);
    handlers.grant(10);

    let dispatch = {
        let hub = Arc::clone(&f.hub);
        let bus = Arc::clone(&bus);
        tokio::spawn(async move { hub.dispatch_command(&bus, command("c1", "Slow")).await })
    };
    let _ = recv(&mut queue).await;
    assert_eq!(bus.engine.command_bus().in_flight_count(), 1);
    dispatch.abort();
    let _ = dispatch.await;
    assert_eq!(bus.engine.command_bus().in_flight_count(), 0);
}

#[tokio::test]
async fn a_handler_stream_ending_fails_what_was_in_flight_to_it() {
    let f = fixture();
    let bus = f.hub.bus("default");
    let (mut handlers, mut queue) = f.hub.open_command_handlers(Arc::clone(&bus), who("h1"));
    handlers.subscribe("Slow", 100);
    handlers.grant(10);

    let dispatch = {
        let hub = Arc::clone(&f.hub);
        let bus = Arc::clone(&bus);
        tokio::spawn(async move { hub.dispatch_command(&bus, command("c1", "Slow")).await })
    };
    let _ = recv(&mut queue).await;
    drop(handlers);

    let result = dispatch.await.unwrap().unwrap();
    assert_eq!(result.error_code.as_deref(), Some("KRONOSDB-4006"));
    // And the handler is gone from the bus.
    let fault = f
        .hub
        .dispatch_command(&bus, command("c2", "Slow"))
        .await
        .unwrap_err();
    assert_eq!(fault.kind, FaultKind::Unavailable);
}

#[tokio::test]
async fn a_newer_stream_takes_over_and_the_older_ones_end_leaves_its_registrations() {
    let f = fixture();
    let bus = f.hub.bus("default");
    let (mut old, mut old_queue) = f.hub.open_command_handlers(Arc::clone(&bus), who("h1"));
    old.subscribe("Echo", 100);
    let (mut new, mut new_queue) = f.hub.open_command_handlers(Arc::clone(&bus), who("h1"));
    // The older stream is told to end, and its sender is gone.
    old.cancelled().await;
    assert!(old_queue.recv().await.is_none());
    new.subscribe("Echo", 100);
    new.grant(10);
    // The older stream ending now must not take the newer one's handler.
    drop(old);

    let dispatch = {
        let hub = Arc::clone(&f.hub);
        let bus = Arc::clone(&bus);
        tokio::spawn(async move { hub.dispatch_command(&bus, command("c1", "Echo")).await })
    };
    let ToCommandHandler::Command(delivered) = recv(&mut new_queue).await;
    new.complete(ok_result(&delivered.message_id));
    assert!(dispatch.await.unwrap().unwrap().error_code.is_none());
}

#[tokio::test]
async fn a_takeover_fails_the_commands_still_in_flight_to_the_older_stream() {
    let f = fixture();
    let bus = f.hub.bus("default");
    let (mut old, mut old_queue) = f.hub.open_command_handlers(Arc::clone(&bus), who("h1"));
    old.subscribe("Slow", 100);
    old.grant(10);
    let dispatch = {
        let hub = Arc::clone(&f.hub);
        let bus = Arc::clone(&bus);
        tokio::spawn(async move { hub.dispatch_command(&bus, command("c1", "Slow")).await })
    };
    let _ = recv(&mut old_queue).await;

    // The client reconnects; the older stream can no longer answer c1.
    let (_new, _new_queue) = f.hub.open_command_handlers(Arc::clone(&bus), who("h1"));
    let result = tokio::time::timeout(Duration::from_secs(5), dispatch)
        .await
        .expect("the caller is not left to time out")
        .unwrap()
        .unwrap();
    assert_eq!(result.error_code.as_deref(), Some("KRONOSDB-4006"));
    drop(old);
}

#[tokio::test]
async fn a_scatter_gather_query_ends_when_every_handler_is_done() {
    let f = fixture();
    let bus = f.hub.bus("default");
    let (mut h1, mut q1) = f.hub.open_query_handlers(Arc::clone(&bus), who("h1"));
    let (mut h2, mut q2) = f.hub.open_query_handlers(Arc::clone(&bus), who("h2"));
    for h in [&mut h1, &mut h2] {
        h.subscribe("Count");
        h.grant(10);
    }

    let mut results = f
        .hub
        .dispatch_query(&bus, query("q1", "Count", -1))
        .await
        .unwrap();
    for (handler, queue, answer) in [(&h1, &mut q1, "one"), (&h2, &mut q2, "two")] {
        let ToQueryHandler::Query(q) = recv(queue).await else {
            panic!("expected a query");
        };
        handler.handled(query_result(&q.message_id, answer));
        handler.done(&q.message_id);
    }
    let mut answers = Vec::new();
    while let Some(result) = results.next().await {
        answers.push(String::from_utf8(result.unwrap().payload.unwrap().data.to_vec()).unwrap());
    }
    answers.sort();
    assert_eq!(answers, vec!["one", "two"]);
}

#[tokio::test]
async fn a_point_to_point_query_goes_to_one_handler() {
    let f = fixture();
    let bus = f.hub.bus("default");
    let (mut h1, mut q1) = f.hub.open_query_handlers(Arc::clone(&bus), who("h1"));
    let (mut h2, mut q2) = f.hub.open_query_handlers(Arc::clone(&bus), who("h2"));
    for h in [&mut h1, &mut h2] {
        h.subscribe("Count");
        h.grant(10);
    }
    let _results = f
        .hub
        .dispatch_query(&bus, query("q1", "Count", 1))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    let delivered = [q1.try_recv().is_ok(), q2.try_recv().is_ok()];
    assert_eq!(delivered.iter().filter(|d| **d).count(), 1);
}

#[tokio::test]
async fn giving_up_a_query_cancels_the_handlers_still_on_it() {
    let f = fixture();
    let bus = f.hub.bus("default");
    let (mut h1, mut q1) = f.hub.open_query_handlers(Arc::clone(&bus), who("h1"));
    h1.subscribe("Slow");
    h1.grant(10);
    let results = f
        .hub
        .dispatch_query(&bus, query("q1", "Slow", -1))
        .await
        .unwrap();
    assert!(matches!(recv(&mut q1).await, ToQueryHandler::Query(_)));
    drop(results);
    assert!(matches!(
        recv(&mut q1).await,
        ToQueryHandler::CancelQuery { request_id } if request_id == "q1"
    ));
    assert!(bus.collectors.is_empty());
}

#[tokio::test]
async fn a_handler_that_goes_away_does_not_hold_a_query_open() {
    let f = fixture();
    let bus = f.hub.bus("default");
    let (mut h1, mut q1) = f.hub.open_query_handlers(Arc::clone(&bus), who("h1"));
    h1.subscribe("Slow");
    h1.grant(10);
    let mut results = f
        .hub
        .dispatch_query(&bus, query("q1", "Slow", -1))
        .await
        .unwrap();
    let _ = recv(&mut q1).await;
    drop(h1);
    let ended = tokio::time::timeout(Duration::from_secs(1), results.next()).await;
    assert!(
        matches!(ended, Ok(None)),
        "the query ends at once, not at its deadline"
    );
}

fn subscription(id: &str, name: &str) -> SubscriptionQuery {
    SubscriptionQuery {
        subscription_id: id.into(),
        query_name: name.into(),
        timestamp: 0,
        payload: payload("sub"),
        metadata: HashMap::new(),
        client_id: ClientId("subscriber".into()),
        component_name: ComponentName("ui".into()),
        initial_permits: 4,
    }
}

fn update(id: &str, text: &'static str) -> SubscriptionEvent {
    SubscriptionEvent::Update(SubscriptionUpdate {
        subscription_id: id.into(),
        payload: Some(payload(text)),
        metadata: HashMap::new(),
        error_code: None,
        error: None,
        complete: false,
    })
}

#[tokio::test]
async fn a_subscription_delivers_its_initial_result_before_any_update() {
    let f = fixture();
    let bus = f.hub.bus("default");
    let (mut handlers, mut queue) = f.hub.open_query_handlers(Arc::clone(&bus), who("h1"));
    handlers.subscribe("Watch");
    handlers.grant(10);

    let mut feed = f
        .hub
        .open_subscription(&bus, subscription("s1", "Watch"))
        .await
        .unwrap();
    let ToQueryHandler::Subscription(asked) = recv(&mut queue).await else {
        panic!("expected a subscription");
    };
    assert_eq!(asked.subscription_id, "s1");
    // The handler sends everything at once; order must hold regardless.
    handlers.subscription_event("s1", update("s1", "u1"));
    handlers.subscription_event(
        "s1",
        SubscriptionEvent::Initial(query_result("s1", "initial")),
    );
    handlers.subscription_event("s1", update("s1", "u2"));
    handlers.subscription_event("s1", SubscriptionEvent::Complete);

    let mut seen = Vec::new();
    while let Some(event) = feed.next().await {
        seen.push(match event.unwrap() {
            SubscriptionEvent::Initial(_) => "initial",
            SubscriptionEvent::Update(_) => "update",
            SubscriptionEvent::Complete => "complete",
            SubscriptionEvent::Failed { .. } => "failed",
        });
    }
    // The update sent before the initial result was already queued; the
    // initial result still comes first, and the end comes last.
    assert_eq!(seen.first(), Some(&"initial"));
    assert_eq!(seen.last(), Some(&"complete"));
    assert_eq!(seen.iter().filter(|e| **e == "update").count(), 2);
}

#[tokio::test]
async fn a_subscriber_that_leaves_tells_the_handler_and_credits_reach_it() {
    let f = fixture();
    let bus = f.hub.bus("default");
    let (mut handlers, mut queue) = f.hub.open_query_handlers(Arc::clone(&bus), who("h1"));
    handlers.subscribe("Watch");
    handlers.grant(10);
    let feed = f
        .hub
        .open_subscription(&bus, subscription("s1", "Watch"))
        .await
        .unwrap();
    let _ = recv(&mut queue).await;

    feed.grant(8);
    assert!(matches!(
        recv(&mut queue).await,
        ToQueryHandler::SubscriptionPermits { permits: 8, .. }
    ));
    drop(feed);
    assert!(matches!(
        recv(&mut queue).await,
        ToQueryHandler::CancelSubscription { subscription_id } if subscription_id == "s1"
    ));
    assert!(bus.initials.is_empty());
}

#[tokio::test]
async fn a_handler_that_dies_ends_its_subscriptions_with_the_loss_code() {
    let f = fixture();
    let bus = f.hub.bus("default");
    let (mut handlers, mut queue) = f.hub.open_query_handlers(Arc::clone(&bus), who("h1"));
    handlers.subscribe("Watch");
    handlers.grant(10);
    let mut feed = f
        .hub
        .open_subscription(&bus, subscription("s1", "Watch"))
        .await
        .unwrap();
    let _ = recv(&mut queue).await;
    // The platform stream dying is what removes a client from the buses.
    let (session, _client_queue) = f
        .hub
        .open_platform(ClientIdentity {
            client_id: "h1".into(),
            component_name: "svc".into(),
            version: "1".into(),
        })
        .unwrap();
    drop(session);
    match feed.next().await {
        Some(Ok(SubscriptionEvent::Failed { error_code, .. })) => {
            assert_eq!(error_code, "KRONOSDB-4006");
        }
        other => panic!("expected the loss, got {other:?}"),
    }
    // The handler's own stream was ended by the same event.
    handlers.cancelled().await;
}

#[tokio::test]
async fn a_handler_stream_ending_alone_ends_its_subscriptions_with_the_loss_code() {
    let f = fixture();
    let bus = f.hub.bus("default");
    let (mut handlers, mut queue) = f.hub.open_query_handlers(Arc::clone(&bus), who("h1"));
    handlers.subscribe("Watch");
    handlers.grant(10);
    let mut feed = f
        .hub
        .open_subscription(&bus, subscription("s1", "Watch"))
        .await
        .unwrap();
    let _ = recv(&mut queue).await;
    assert_eq!(bus.engine.subscription_stats().len(), 1);

    // No platform stream is involved: the handler's stream alone ends.
    drop(handlers);

    let ended = tokio::time::timeout(Duration::from_secs(5), feed.next())
        .await
        .expect("the subscriber hears that its handler is gone");
    match ended {
        Some(Ok(SubscriptionEvent::Failed { error_code, .. })) => {
            assert_eq!(error_code, "KRONOSDB-4006");
        }
        other => panic!("expected the loss, got {other:?}"),
    }
    assert!(
        bus.engine.subscription_stats().is_empty(),
        "the ended subscription does not linger"
    );
}

#[tokio::test]
async fn a_platform_stream_registers_the_client_and_its_end_removes_it() {
    let f = fixture();
    let identity = ClientIdentity {
        client_id: "svc-1".into(),
        component_name: "orders".into(),
        version: "1.0".into(),
    };
    assert!(
        f.hub
            .register_client(ClientIdentity {
                client_id: String::new(),
                ..identity.clone()
            })
            .is_err()
    );
    let (session, mut queue) = f.hub.open_platform(identity.clone()).unwrap();
    let ToClient::Node(info) = recv(&mut queue).await else {
        panic!("the node comes first");
    };
    assert_eq!(info.node_name, "test-node");
    assert_eq!(info.contexts, vec!["default"]);
    assert!(
        f.hub
            .client_registry
            .is_registered(&ClientId("svc-1".into()))
    );

    session.report(ProcessorReport {
        processor_name: "proj".into(),
        mode: "Tracking".into(),
        running: true,
        error: false,
        streaming: true,
        token_store_identifier: "s".into(),
        segments: vec![],
    });
    assert_eq!(f.hub.processors.list().len(), 1);

    // Instructions reach the connected client, and only it.
    let instruction = ProcessorInstruction {
        instruction_id: "i1".into(),
        action: ProcessorAction::Pause,
        processor_name: "proj".into(),
        segment: 0,
    };
    assert!(f.hub.clients.send_instruction("svc-1", instruction).await);
    assert!(matches!(recv(&mut queue).await, ToClient::Instruction(_)));

    drop(session);
    assert!(
        !f.hub
            .client_registry
            .is_registered(&ClientId("svc-1".into()))
    );
    assert!(f.hub.processors.list().is_empty());
}

#[tokio::test]
async fn a_handler_registration_is_announced_to_connected_clients() {
    let f = fixture();
    let (_session, mut queue) = f
        .hub
        .open_platform(ClientIdentity {
            client_id: "watcher".into(),
            component_name: "ui".into(),
            version: "1".into(),
        })
        .unwrap();
    let _node = recv(&mut queue).await;
    let bus = f.hub.bus("default");
    let (mut handlers, _q) = f.hub.open_command_handlers(bus, who("h1"));
    handlers.subscribe("Echo", 100);
    let ToClient::Topology(change) = recv(&mut queue).await else {
        panic!("expected a topology change");
    };
    assert_eq!(change.event, TopologyEvent::HandlerRegistered);
    assert_eq!(change.message_type, "Echo");
    handlers.unsubscribe("Echo");
    let ToClient::Topology(change) = recv(&mut queue).await else {
        panic!("expected a topology change");
    };
    assert_eq!(change.event, TopologyEvent::HandlerDeregistered);
}

#[tokio::test]
async fn a_takeover_ends_the_subscriptions_the_older_stream_served() {
    let f = fixture();
    let bus = f.hub.bus("default");
    let (mut old, mut old_queue) = f.hub.open_query_handlers(Arc::clone(&bus), who("h1"));
    old.subscribe("Watch");
    old.grant(10);
    let mut feed = f
        .hub
        .open_subscription(&bus, subscription("s1", "Watch"))
        .await
        .unwrap();
    let _ = recv(&mut old_queue).await;

    // A newer stream of the same client takes over; the older one ends
    // leaving its cleanup to it.
    let (_new, _new_queue) = f.hub.open_query_handlers(Arc::clone(&bus), who("h1"));
    drop(old);

    let ended = tokio::time::timeout(Duration::from_secs(5), feed.next())
        .await
        .expect("the subscriber hears that its handler is gone");
    match ended {
        Some(Ok(SubscriptionEvent::Failed { error_code, .. })) => {
            assert_eq!(error_code, "KRONOSDB-4006");
        }
        other => panic!("expected the loss, got {other:?}"),
    }
    assert!(bus.engine.subscription_stats().is_empty());
}
