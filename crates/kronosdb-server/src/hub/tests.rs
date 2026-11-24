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
use kronosdb_eventstore::raft::handler_registry::{HandlerKind, HandlerRegistration};
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
    /// The routing table the hub's writes land in.
    table: Arc<FakeTable>,
    _dir: tempfile::TempDir,
}

fn fixture() -> Fixture {
    fixture_with(Duration::from_secs(5))
}

fn fixture_with(command_timeout: Duration) -> Fixture {
    let dir = tempfile::tempdir().unwrap();
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
    let table = Arc::new(FakeTable::default());
    let hub = Hub::with_writes(
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
        &cluster,
        table.clone(),
        Arc::new(peers::NoPeers),
        Arc::new(|| vec!["default".to_string()]),
    );
    Fixture {
        hub,
        table,
        _dir: dir,
    }
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
    assert_eq!(fault.kind, FaultKind::NoHandler);
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
    let ToCommandHandler::Command(sent) = recv(&mut queue).await;
    handlers.taken(&sent.message_id);
    drop(handlers);

    let result = dispatch.await.unwrap().unwrap();
    assert_eq!(result.error_code.as_deref(), Some("KRONOSDB-4006"));
    // And the handler is gone from the bus.
    let fault = f
        .hub
        .dispatch_command(&bus, command("c2", "Slow"))
        .await
        .unwrap_err();
    assert_eq!(fault.kind, FaultKind::NoHandler);
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
    let ToCommandHandler::Command(sent) = recv(&mut old_queue).await;
    old.taken(&sent.message_id);

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
    let ToQueryHandler::Query(sent) = recv(&mut q1).await else {
        panic!("the query");
    };
    h1.taken(&sent.message_id);
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

#[tokio::test(start_paused = true)]
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
    // A client gone silent is what removes it from the buses.
    tokio::time::advance(Duration::from_secs(16)).await;
    let (mut session, _client_queue) = f
        .hub
        .open_platform(ClientIdentity {
            client_id: "h1".into(),
            component_name: "svc".into(),
            version: "1".into(),
        })
        .unwrap();
    session.expire();
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

// ── A client that reopens its streams keeps its handlers ───────────────

type Row = (String, HandlerKind, String, String);

/// The replicated routing table as a test can see it, with writes that can
/// be held so a test decides which one lands last.
#[derive(Default)]
struct FakeTable {
    state: parking_lot::Mutex<FakeState>,
    changed: tokio::sync::Notify,
}

#[derive(Default)]
struct FakeState {
    rows: std::collections::BTreeSet<Row>,
    /// Removals wait while this is set.
    hold: bool,
    /// How many removals have arrived at the hold.
    held: usize,
    /// Registrations that fail as the control plane not being there yet.
    failing_registrations: usize,
}

impl FakeTable {
    fn rows(&self) -> std::collections::BTreeSet<Row> {
        self.state.lock().rows.clone()
    }

    fn hold_removals(&self) {
        self.state.lock().hold = true;
    }

    fn release_removals(&self) {
        self.state.lock().hold = false;
        self.changed.notify_waiters();
    }

    /// Resolves once `count` removals are waiting at the hold.
    async fn removals_held(&self, count: usize) {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.state.lock().held >= count {
                return;
            }
            changed.await;
        }
    }

    async fn gate(&self) {
        self.state.lock().held += 1;
        self.changed.notify_waiters();
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if !self.state.lock().hold {
                return;
            }
            changed.await;
        }
    }
}

impl RoutingWrites for FakeTable {
    fn node_id(&self) -> u64 {
        1
    }

    fn register(
        &self,
        registration: HandlerRegistration,
    ) -> peers::BoxFuture<'_, Result<(), kronosdb_eventstore::error::Error>> {
        Box::pin(async move {
            let mut state = self.state.lock();
            if state.failing_registrations > 0 {
                state.failing_registrations -= 1;
                return Err(kronosdb_eventstore::error::Error::Unavailable {
                    message: "no control-plane leader available".into(),
                });
            }
            state.rows.insert((
                registration.bus,
                registration.kind,
                registration.message_type,
                registration.client_id,
            ));
            Ok(())
        })
    }

    fn deregister<'a>(
        &'a self,
        bus: &'a str,
        kind: HandlerKind,
        message_type: &'a str,
        client_id: &'a str,
    ) -> peers::BoxFuture<'a, Result<(), kronosdb_eventstore::error::Error>> {
        Box::pin(async move {
            self.gate().await;
            self.state.lock().rows.remove(&(
                bus.to_string(),
                kind,
                message_type.to_string(),
                client_id.to_string(),
            ));
            Ok(())
        })
    }
}

/// Waits for a handler stream the hub ended.
async fn ended(cancelled: impl std::future::Future<Output = ()>) {
    tokio::time::timeout(Duration::from_secs(5), cancelled)
        .await
        .expect("the stream was ended");
}

fn row(bus: &str, kind: HandlerKind, message_type: &str, client: &str) -> Row {
    (bus.into(), kind, message_type.into(), client.into())
}

/// Waits for the replicated table to hold exactly `expected`, and to go on
/// holding it while the writes still in flight land.
async fn table_settles_to(table: &FakeTable, expected: &[Row]) {
    let expected: std::collections::BTreeSet<Row> = expected.iter().cloned().collect();
    for _ in 0..400 {
        if table.rows() == expected {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(table.rows(), expected, "the routing table did not settle");
}

/// What a bus's engine holds for handlers: (kind, message type, client).
fn local_rows(bus: &Bus) -> std::collections::BTreeSet<(HandlerKind, String, String)> {
    let mut rows = std::collections::BTreeSet::new();
    for (kind, details) in [
        (HandlerKind::Command, bus.engine.command_details()),
        (HandlerKind::Query, bus.engine.query_details()),
    ] {
        for detail in details {
            for handler in detail.handlers {
                rows.insert((kind, detail.name.clone(), handler.client_id));
            }
        }
    }
    rows
}

fn local(kind: HandlerKind, message_type: &str, client: &str) -> (HandlerKind, String, String) {
    (kind, message_type.into(), client.into())
}

fn identity(client: &str) -> ClientIdentity {
    ClientIdentity {
        client_id: client.into(),
        component_name: "svc".into(),
        version: "1".into(),
    }
}

#[tokio::test]
async fn a_platform_stream_the_client_closed_leaves_its_handler_streams_alone() {
    let f = fixture();
    let bus = f.hub.bus("default");
    let (session, _platform) = f.hub.open_platform(identity("h1")).unwrap();
    let (mut old, _old_queue) = f.hub.open_command_handlers(Arc::clone(&bus), who("h1"));
    old.subscribe("Echo", 100);
    // The client reopens its handler stream, then its old platform stream
    // is finally processed as closed.
    let (mut new, mut new_queue) = f.hub.open_command_handlers(Arc::clone(&bus), who("h1"));
    new.subscribe("Echo", 100);
    new.grant(10);
    drop(session);
    drop(old);

    assert!(
        tokio::time::timeout(Duration::from_millis(50), new.cancelled())
            .await
            .is_err(),
        "the new stream is not ended by the old platform stream"
    );
    assert_eq!(
        local_rows(&bus),
        [local(HandlerKind::Command, "Echo", "h1")].into()
    );
    table_settles_to(
        &f.table,
        &[row("default", HandlerKind::Command, "Echo", "h1")],
    )
    .await;
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
async fn a_platform_stream_ending_before_the_new_streams_open_ends_nothing() {
    let f = fixture();
    let bus = f.hub.bus("default");
    let (session, _platform) = f.hub.open_platform(identity("h1")).unwrap();
    let (mut handlers, _queue) = f.hub.open_command_handlers(Arc::clone(&bus), who("h1"));
    handlers.subscribe("Echo", 100);
    let (mut queries, _query_queue) = f.hub.open_query_handlers(Arc::clone(&bus), who("h1"));
    queries.subscribe("Ask");
    drop(session);

    // Whatever the client did to its platform stream, its handler streams
    // are its own connection's: they end when that connection does.
    assert!(
        tokio::time::timeout(Duration::from_millis(50), handlers.cancelled())
            .await
            .is_err()
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(50), queries.cancelled())
            .await
            .is_err()
    );
    table_settles_to(
        &f.table,
        &[
            row("default", HandlerKind::Command, "Echo", "h1"),
            row("default", HandlerKind::Query, "Ask", "h1"),
        ],
    )
    .await;
    drop(handlers);
    drop(queries);
    table_settles_to(&f.table, &[]).await;
    assert!(local_rows(&bus).is_empty());
}

#[tokio::test]
async fn an_older_stream_ending_after_the_newer_one_subscribed_keeps_its_rows() {
    let f = fixture();
    let bus = f.hub.bus("default");
    let (mut old, _old_queue) = f.hub.open_command_handlers(Arc::clone(&bus), who("h1"));
    old.subscribe("Echo", 100);
    old.subscribe("Gone", 100);
    let (mut new, _new_queue) = f.hub.open_command_handlers(Arc::clone(&bus), who("h1"));
    new.subscribe("Echo", 100);
    drop(old);

    // What the replaced stream held and the new one does not is gone; what
    // the new one subscribed stays.
    assert_eq!(
        local_rows(&bus),
        [local(HandlerKind::Command, "Echo", "h1")].into()
    );
    table_settles_to(
        &f.table,
        &[row("default", HandlerKind::Command, "Echo", "h1")],
    )
    .await;
}

#[tokio::test]
async fn a_stream_ending_late_does_not_remove_what_a_new_stream_on_the_same_bus_registered() {
    let f = fixture();
    let bus = f.hub.bus("default");
    let (mut old, _old_queue) = f.hub.open_command_handlers(Arc::clone(&bus), who("h1"));
    old.subscribe("Echo", 100);
    table_settles_to(
        &f.table,
        &[row("default", HandlerKind::Command, "Echo", "h1")],
    )
    .await;

    // The old stream's end writes its removal, which is slow to land.
    f.table.hold_removals();
    drop(old);
    f.table.removals_held(1).await;
    let (mut new, _new_queue) = f.hub.open_command_handlers(Arc::clone(&bus), who("h1"));
    new.subscribe("Echo", 100);
    tokio::time::sleep(Duration::from_millis(50)).await;
    f.table.release_removals();

    table_settles_to(
        &f.table,
        &[row("default", HandlerKind::Command, "Echo", "h1")],
    )
    .await;
    assert_eq!(
        local_rows(&bus),
        [local(HandlerKind::Command, "Echo", "h1")].into()
    );
}

#[tokio::test]
async fn a_stream_ending_late_does_not_remove_what_a_new_stream_on_another_bus_registered() {
    let f = fixture();
    let (a, b) = (f.hub.bus("a"), f.hub.bus("b"));
    let (mut old, _old_queue) = f.hub.open_command_handlers(Arc::clone(&a), who("h1"));
    old.subscribe("Echo", 100);
    table_settles_to(&f.table, &[row("a", HandlerKind::Command, "Echo", "h1")]).await;

    f.table.hold_removals();
    drop(old);
    f.table.removals_held(1).await;
    let (mut new, _new_queue) = f.hub.open_query_handlers(Arc::clone(&b), who("h1"));
    new.subscribe("Ask");
    // Whatever the hub writes meanwhile, the removal lands last.
    tokio::time::sleep(Duration::from_millis(50)).await;
    f.table.release_removals();

    table_settles_to(&f.table, &[row("b", HandlerKind::Query, "Ask", "h1")]).await;
    assert!(local_rows(&a).is_empty());
    assert_eq!(
        local_rows(&b),
        [local(HandlerKind::Query, "Ask", "h1")].into()
    );
}

#[tokio::test]
async fn a_replaced_stream_cannot_subscribe_anything() {
    let f = fixture();
    let bus = f.hub.bus("default");
    let (mut old, _old_queue) = f.hub.open_command_handlers(Arc::clone(&bus), who("h1"));
    let (_new, _new_queue) = f.hub.open_command_handlers(Arc::clone(&bus), who("h1"));
    // A frame of the old stream the server had not read yet.
    old.subscribe("Late", 100);
    old.grant(5);
    drop(old);

    assert!(local_rows(&bus).is_empty());
    table_settles_to(&f.table, &[]).await;
}

#[tokio::test]
async fn a_takeover_starts_the_new_stream_with_nothing_subscribed() {
    let f = fixture();
    let bus = f.hub.bus("default");
    let (mut old, _old_queue) = f.hub.open_command_handlers(Arc::clone(&bus), who("h1"));
    old.subscribe("Echo", 100);
    table_settles_to(
        &f.table,
        &[row("default", HandlerKind::Command, "Echo", "h1")],
    )
    .await;

    let (_new, _new_queue) = f.hub.open_command_handlers(Arc::clone(&bus), who("h1"));
    assert!(local_rows(&bus).is_empty());
    table_settles_to(&f.table, &[]).await;
    let fault = f
        .hub
        .dispatch_command(&bus, command("c1", "Echo"))
        .await
        .unwrap_err();
    assert_eq!(fault.kind, FaultKind::NoHandler);
}

#[tokio::test]
async fn a_takeover_does_not_leave_a_query_waiting_on_the_older_stream() {
    let f = fixture();
    let bus = f.hub.bus("default");
    let (mut old, mut old_queue) = f.hub.open_query_handlers(Arc::clone(&bus), who("h1"));
    old.subscribe("Ask");
    old.grant(10);
    let mut results = f
        .hub
        .dispatch_query(&bus, query("q1", "Ask", 1))
        .await
        .unwrap();
    let ToQueryHandler::Query(sent) = recv(&mut old_queue).await else {
        panic!("the query");
    };
    old.taken(&sent.message_id);

    let (_new, _new_queue) = f.hub.open_query_handlers(Arc::clone(&bus), who("h1"));
    let ended = tokio::time::timeout(Duration::from_secs(1), results.next())
        .await
        .expect("the query is not left to wait out its deadline");
    assert!(ended.is_none() || matches!(ended, Some(Err(_))));
}

#[tokio::test(start_paused = true)]
async fn a_client_gone_silent_loses_its_streams_and_every_row() {
    let f = fixture();
    let bus = f.hub.bus("default");
    let (mut session, _platform) = f.hub.open_platform(identity("h1")).unwrap();
    let (mut commands, _commands_queue) = f.hub.open_command_handlers(Arc::clone(&bus), who("h1"));
    commands.subscribe("Echo", 100);
    let (mut queries, _queries_queue) = f.hub.open_query_handlers(Arc::clone(&bus), who("h1"));
    queries.subscribe("Ask");
    table_settles_to(
        &f.table,
        &[
            row("default", HandlerKind::Command, "Echo", "h1"),
            row("default", HandlerKind::Query, "Ask", "h1"),
        ],
    )
    .await;

    // Silent for longer than the timeout; its platform stream gives up.
    tokio::time::advance(Duration::from_secs(16)).await;
    // A stream opened since is proof of life, and stays.
    let other = f.hub.bus("other");
    let (mut fresh, _fresh_queue) = f.hub.open_command_handlers(Arc::clone(&other), who("h1"));
    fresh.subscribe("Fresh", 100);
    session.expire();
    drop(session);

    ended(commands.cancelled()).await;
    ended(queries.cancelled()).await;
    assert!(local_rows(&bus).is_empty());
    assert!(
        tokio::time::timeout(Duration::from_millis(50), fresh.cancelled())
            .await
            .is_err()
    );
    table_settles_to(
        &f.table,
        &[row("other", HandlerKind::Command, "Fresh", "h1")],
    )
    .await;
    drop(commands);
    drop(queries);
    drop(fresh);
    table_settles_to(&f.table, &[]).await;
}

#[tokio::test(start_paused = true)]
async fn the_reaper_ends_the_handler_streams_of_a_client_that_went_silent() {
    let f = fixture();
    let bus = f.hub.bus("default");
    let (mut commands, _queue) = f.hub.open_command_handlers(Arc::clone(&bus), who("h1"));
    commands.subscribe("Echo", 100);
    table_settles_to(
        &f.table,
        &[row("default", HandlerKind::Command, "Echo", "h1")],
    )
    .await;
    tokio::time::advance(Duration::from_secs(16)).await;

    f.hub.forget_client(&ClientId("h1".into()));

    ended(commands.cancelled()).await;
    assert!(local_rows(&bus).is_empty());
    table_settles_to(&f.table, &[]).await;
}

#[tokio::test]
async fn a_command_its_handlers_stream_ended_before_taking_is_not_a_lost_handler() {
    let f = fixture();
    let bus = f.hub.bus("default");
    let (mut handlers, _queue) = f.hub.open_command_handlers(Arc::clone(&bus), who("h1"));
    handlers.subscribe("Echo", 100);
    handlers.grant(10);
    let dispatch = {
        let hub = Arc::clone(&f.hub);
        let bus = Arc::clone(&bus);
        tokio::spawn(async move { hub.dispatch_command(&bus, command("c1", "Echo")).await })
    };
    // The command is in the stream's queue; the stream ends without taking it.
    while bus.engine.command_bus().in_flight_count() == 0 {
        tokio::task::yield_now().await;
    }
    tokio::task::yield_now().await;
    drop(handlers);

    // Nothing saw it: the caller is told there is no handler, which it can
    // try again, not that a handler disconnected while answering.
    let fault = dispatch.await.unwrap().unwrap_err();
    assert_eq!(fault.kind, FaultKind::NoHandler);
}

#[tokio::test]
async fn a_command_a_replaced_stream_never_took_is_not_a_lost_handler() {
    let f = fixture();
    let bus = f.hub.bus("default");
    let (mut old, _old_queue) = f.hub.open_command_handlers(Arc::clone(&bus), who("h1"));
    old.subscribe("Echo", 100);
    old.grant(10);
    let dispatch = {
        let hub = Arc::clone(&f.hub);
        let bus = Arc::clone(&bus);
        tokio::spawn(async move { hub.dispatch_command(&bus, command("c1", "Echo")).await })
    };
    while bus.engine.command_bus().in_flight_count() == 0 {
        tokio::task::yield_now().await;
    }
    tokio::task::yield_now().await;
    let (_new, _new_queue) = f.hub.open_command_handlers(Arc::clone(&bus), who("h1"));

    let fault = dispatch.await.unwrap().unwrap_err();
    assert_eq!(fault.kind, FaultKind::NoHandler);
}

#[tokio::test]
async fn a_query_its_handlers_stream_ended_before_taking_is_not_an_empty_answer() {
    let f = fixture();
    let bus = f.hub.bus("default");
    let (mut handlers, _queue) = f.hub.open_query_handlers(Arc::clone(&bus), who("h1"));
    handlers.subscribe("Ask");
    handlers.grant(10);
    let mut results = f
        .hub
        .dispatch_query(&bus, query("q1", "Ask", 1))
        .await
        .unwrap();
    drop(handlers);

    match tokio::time::timeout(Duration::from_secs(1), results.next()).await {
        Ok(Some(Err(fault))) => assert_eq!(fault.kind, FaultKind::NoHandler),
        other => panic!("expected no handler, got {other:?}"),
    }
}

#[tokio::test]
async fn a_registration_the_control_plane_cannot_take_yet_is_written_when_it_can() {
    let f = fixture();
    let bus = f.hub.bus("default");
    f.table.state.lock().failing_registrations = 2;
    let (mut handlers, _queue) = f.hub.open_command_handlers(Arc::clone(&bus), who("h1"));
    handlers.subscribe("Echo", 100);
    handlers.subscribe("Other", 100);

    table_settles_to(
        &f.table,
        &[
            row("default", HandlerKind::Command, "Echo", "h1"),
            row("default", HandlerKind::Command, "Other", "h1"),
        ],
    )
    .await;
    handlers.unsubscribe("Echo");
    table_settles_to(
        &f.table,
        &[row("default", HandlerKind::Command, "Other", "h1")],
    )
    .await;
}
