//! Commands, queries, subscription queries and the platform over the native
//! protocol: a real server process, the Rust client. Dispatchers and
//! handlers use separate connections, as they do in real deployments.

use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command as Process, Stdio};
use std::time::Duration;

use bytes::Bytes;
use kronosdb_client::{
    Client, Command, ConnectOptions, ErrorCode, Identity, Payload, PlatformEvent, ProcessorAction,
    ProcessorStatus, Query, QueryHandlerEvent, Reply, SegmentStatus, SubscriptionEvent,
};
use tokio::task::JoinHandle;

struct Node {
    child: Child,
    native: String,
    admin: String,
}

impl Drop for Node {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn free_port() -> u16 {
    // The kernel can hand a port it just released straight back, so ports this
    // process already gave out are skipped: two nodes must never share one.
    static GIVEN: std::sync::Mutex<Vec<u16>> = std::sync::Mutex::new(Vec::new());
    let mut given = GIVEN.lock().unwrap();
    loop {
        let port = TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        if !given.contains(&port) {
            given.push(port);
            return port;
        }
    }
}

fn work_dir(name: &str) -> PathBuf {
    let dir = Path::new(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("native-messaging-{}-{name}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn spawn(name: &str, envs: &[(&str, &str)]) -> Node {
    let dir = work_dir(name);
    let native = format!("127.0.0.1:{}", free_port());
    let admin = format!("127.0.0.1:{}", free_port());
    let mut cmd = Process::new(env!("CARGO_BIN_EXE_kronosdb-server"));
    cmd.current_dir(&dir)
        .env("KRONOSDB_LISTEN", &native)
        .env("KRONOSDB_ADMIN_LISTEN", &admin)
        .env("KRONOSDB_DATA_DIR", dir.join("data"))
        .env("RUST_LOG", "warn");
    for (key, value) in envs {
        cmd.env(key, value);
    }
    if std::env::var_os("KRONOSDB_TEST_LOGS").is_some() {
        cmd.stdout(Stdio::inherit()).stderr(Stdio::inherit());
    } else {
        cmd.stdout(Stdio::null()).stderr(Stdio::null());
    }
    Node {
        child: cmd.spawn().expect("spawn kronosdb-server"),
        native,
        admin,
    }
}

/// A new connection, once the server is up. Generous: the suite starts a
/// server per test, all at once.
async fn connect(node: &Node) -> Client {
    for _ in 0..600 {
        if let Ok(client) = Client::connect(&node.native, &ConnectOptions::default()).await
            && client.default_context().head().await.is_ok()
        {
            return client;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("the server never became ready on {}", node.native);
}

fn payload(text: &str) -> Payload {
    Payload::new("text", Bytes::from(text.to_string()))
}

fn command(id: &str, name: &str, body: &str) -> Command {
    let mut c = Command::new(id, name, payload(body));
    c.client_id = "dispatcher".into();
    c.component_name = "tests".into();
    c
}

fn query(id: &str, name: &str) -> Query {
    let mut q = Query::new(id, name, payload("?"));
    q.client_id = "dispatcher".into();
    q.component_name = "tests".into();
    q
}

fn text(reply: &Reply) -> String {
    String::from_utf8(reply.payload.as_ref().expect("a payload").data.to_vec()).unwrap()
}

async fn within<T>(what: &str, fut: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(10), fut)
        .await
        .unwrap_or_else(|_| panic!("timed out: {what}"))
}

/// A command handler on its own connection that answers every `name`
/// command with `"<tag>:<body>"`. Aborting the task takes the handler away.
async fn echo_handler(node: &Node, name: &str, tag: &str) -> (Client, JoinHandle<()>) {
    let client = connect(node).await;
    let mut handlers = client
        .default_bus()
        .command_handlers(tag, "echo-service", 16)
        .await
        .unwrap();
    handlers.subscribe(name, 100).await.unwrap();
    let tag = tag.to_string();
    let task = tokio::spawn(async move {
        while let Ok(Some(incoming)) = handlers.next().await {
            let body = String::from_utf8_lossy(&incoming.command.payload.data).to_string();
            let reply = Reply::ok(
                incoming.command.message_id.clone(),
                payload(&format!("{tag}:{body}")),
            );
            if incoming.respond(&reply).await.is_err() {
                break;
            }
        }
    });
    (client, task)
}

/// Dispatches until the handler's registration is in place.
async fn dispatch_when_ready(client: &Client, cmd: &Command) -> Reply {
    for _ in 0..100 {
        match client.default_bus().dispatch_command(cmd).await {
            Ok(reply) => return reply,
            Err(e)
                if e.failure()
                    .is_some_and(|f| f.code == ErrorCode::UNAVAILABLE) =>
            {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Err(e) => panic!("dispatch failed: {e}"),
        }
    }
    panic!("the command never reached a handler");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_command_goes_to_its_handler_and_the_answer_comes_back() {
    let node = spawn("command", &[]);
    let (_handler_conn, _handler) = echo_handler(&node, "Echo", "h1").await;
    let dispatcher = connect(&node).await;

    let reply = dispatch_when_ready(&dispatcher, &command("c1", "Echo", "hello")).await;
    assert!(reply.is_ok(), "{reply:?}");
    assert_eq!(text(&reply), "h1:hello");
    assert_eq!(reply.request_id, "c1");

    // Metadata and the routing key survive the trip; so does a handler's own failure.
    let mut failing = command("c2", "Fail", "x");
    failing
        .metadata
        .push(("trace".into(), kronosdb_client::Value::Text("t".into())));
    let bus = dispatcher.default_bus();
    let conn = connect(&node).await;
    let mut handlers = conn
        .default_bus()
        .command_handlers("failer", "svc", 4)
        .await
        .unwrap();
    handlers.subscribe("Fail", 100).await.unwrap();
    let task = tokio::spawn(async move {
        let incoming = handlers.next().await.unwrap().unwrap();
        assert_eq!(incoming.command.metadata.len(), 1);
        let message_id = incoming.command.message_id.clone();
        incoming
            .respond(&Reply::failed(message_id, "APP-1", "no stock"))
            .await
            .unwrap();
        // Keep the stream open until the test is over.
        tokio::time::sleep(Duration::from_secs(5)).await;
    });
    let reply = dispatch_when_ready(&dispatcher, &failing).await;
    assert_eq!(reply.error_code, "APP-1");
    assert_eq!(reply.error.unwrap().message, "no stock");
    task.abort();

    // Nothing handles this one.
    let err = bus
        .dispatch_command(&command("c3", "Nobody", "x"))
        .await
        .unwrap_err();
    assert_eq!(err.failure().unwrap().code, ErrorCode::UNAVAILABLE);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_routing_key_always_reaches_the_same_handler() {
    let node = spawn("sticky", &[]);
    let (_c1, _h1) = echo_handler(&node, "Keyed", "alpha").await;
    let (_c2, _h2) = echo_handler(&node, "Keyed", "beta").await;
    let dispatcher = connect(&node).await;
    dispatch_when_ready(&dispatcher, &command("warm", "Keyed", "w")).await;

    let bus = dispatcher.default_bus();
    let owner = |reply: &Reply| text(reply).split(':').next().unwrap().to_string();
    // A handler joining the ring moves keys, as consistent hashing should;
    // measure only once both registrations are in it, which is when keyed
    // dispatches have reached both.
    let mut owners = std::collections::HashSet::new();
    for probe in 0.. {
        assert!(probe < 500, "keyed dispatches never reached both handlers");
        let cmd =
            command(&format!("probe-{probe}"), "Keyed", "x").with_routing_key(format!("p{probe}"));
        owners.insert(owner(&bus.dispatch_command(&cmd).await.unwrap()));
        if owners.len() == 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let mut owners_by_key = std::collections::HashMap::new();
    for key in 0..12 {
        let key = format!("order-{key}");
        let mut seen = std::collections::HashSet::new();
        for n in 0..6 {
            let cmd = command(&format!("{key}-{n}"), "Keyed", "x").with_routing_key(&key);
            let reply = bus.dispatch_command(&cmd).await.unwrap();
            seen.insert(owner(&reply));
        }
        assert_eq!(seen.len(), 1, "key {key} moved between handlers: {seen:?}");
        owners_by_key.insert(key, seen.into_iter().next().unwrap());
    }
    let distinct: std::collections::HashSet<_> = owners_by_key.values().collect();
    assert_eq!(
        distinct.len(),
        2,
        "keys spread over both handlers: {owners_by_key:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_handler_that_disconnects_fails_its_commands_in_flight() {
    let node = spawn("disconnect", &[]);
    let handler_conn = connect(&node).await;
    let mut handlers = handler_conn
        .default_bus()
        .command_handlers("doomed", "svc", 4)
        .await
        .unwrap();
    handlers.subscribe("Slow", 100).await.unwrap();
    let dispatcher = connect(&node).await;

    let pending = {
        let dispatcher = dispatcher.clone();
        tokio::spawn(async move {
            dispatch_when_ready(&dispatcher, &command("slow-1", "Slow", "x")).await
        })
    };
    // The handler gets the command and never answers; then it goes away.
    let incoming = within("the handler receives the command", handlers.next())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(incoming.command.message_id, "slow-1");
    drop(incoming);
    drop(handlers);

    let reply = within("the caller hears of the loss", pending)
        .await
        .unwrap();
    assert_eq!(reply.error_code, "KRONOSDB-4006", "{reply:?}");
    assert_eq!(reply.request_id, "slow-1");

    // The handler is gone: new commands find none.
    drop(handler_conn);
    tokio::time::sleep(Duration::from_millis(200)).await;
    let err = dispatcher
        .default_bus()
        .dispatch_command(&command("slow-2", "Slow", "x"))
        .await
        .unwrap_err();
    assert_eq!(err.failure().unwrap().code, ErrorCode::UNAVAILABLE);
}

/// A handler on a runtime of its own, so the whole connection can be cut
/// without the client saying goodbye on any stream.
fn crashing_handler(
    node_addr: String,
    command: &'static str,
) -> (
    std::sync::mpsc::Receiver<String>,
    std::sync::mpsc::Sender<()>,
) {
    let (received_tx, received) = std::sync::mpsc::channel();
    let (crash, crash_rx) = std::sync::mpsc::channel::<()>();
    std::thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let (client, mut handlers) = rt.block_on(async {
            let client = loop {
                match Client::connect(&node_addr, &ConnectOptions::default()).await {
                    Ok(c) if c.default_context().head().await.is_ok() => break c,
                    _ => tokio::time::sleep(Duration::from_millis(50)).await,
                }
            };
            let mut handlers = client
                .default_bus()
                .command_handlers("crashing", "svc", 4)
                .await
                .unwrap();
            handlers.subscribe(command, 100).await.unwrap();
            (client, handlers)
        });
        let incoming = rt.block_on(handlers.next()).unwrap().unwrap();
        received_tx
            .send(incoming.command.message_id.clone())
            .unwrap();
        // Nothing is closed politely: the streams and the connection are
        // forgotten, and the runtime that drives them is torn down.
        std::mem::forget((client, handlers, incoming));
        let _ = crash_rx.recv();
        rt.shutdown_background();
    });
    (received, crash)
}

#[tokio::test(flavor = "multi_thread")]
async fn closing_the_connection_ends_its_handlers_the_same_way() {
    let node = spawn("connection-loss", &[]);
    let dispatcher = connect(&node).await;
    let (received, crash) = crashing_handler(node.native.clone(), "Slow");
    let pending = {
        let dispatcher = dispatcher.clone();
        tokio::spawn(async move {
            dispatch_when_ready(&dispatcher, &command("slow-1", "Slow", "x")).await
        })
    };
    let id = tokio::task::spawn_blocking(move || received.recv_timeout(Duration::from_secs(10)))
        .await
        .unwrap()
        .expect("the handler receives the command");
    assert_eq!(id, "slow-1");
    crash.send(()).unwrap();
    let reply = within("the caller hears of the loss", pending)
        .await
        .unwrap();
    assert_eq!(reply.error_code, "KRONOSDB-4006", "{reply:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_handler_without_permits_holds_commands_back_until_it_grants_more() {
    let node = spawn("permits", &[]);
    let handler_conn = connect(&node).await;
    // A window of one: a second command waits for the first to be answered.
    let mut handlers = handler_conn
        .default_bus()
        .command_handlers("narrow", "svc", 1)
        .await
        .unwrap();
    handlers.subscribe("Narrow", 100).await.unwrap();
    let dispatcher = connect(&node).await;
    dispatch_when_ready_probe(&dispatcher, &mut handlers).await;

    let first = {
        let d = dispatcher.clone();
        tokio::spawn(async move {
            d.default_bus()
                .dispatch_command(&command("n-1", "Narrow", "a"))
                .await
        })
    };
    let second = {
        let d = dispatcher.clone();
        tokio::spawn(async move {
            d.default_bus()
                .dispatch_command(&command("n-2", "Narrow", "b"))
                .await
        })
    };

    let one = within("the first command", handlers.next())
        .await
        .unwrap()
        .unwrap();
    // The other has no permit to travel on.
    assert!(
        tokio::time::timeout(Duration::from_millis(400), handlers.next())
            .await
            .is_err(),
        "a second command arrived without a permit"
    );
    let id = one.command.message_id.clone();
    one.respond(&Reply::ok(id.clone(), payload("done")))
        .await
        .unwrap();
    // Answering granted a permit back, so the waiting one is delivered.
    let two = within("the second command", handlers.next())
        .await
        .unwrap()
        .unwrap();
    let id2 = two.command.message_id.clone();
    two.respond(&Reply::ok(id2.clone(), payload("done")))
        .await
        .unwrap();

    for task in [first, second] {
        let reply = within("a dispatch", task).await.unwrap().unwrap();
        assert!(reply.is_ok());
    }
}

/// Permits belong to the handler stream: a window of two is two commands in
/// flight, whatever their names — not two per name.
#[tokio::test(flavor = "multi_thread")]
async fn a_handler_streams_permits_are_shared_by_every_command_it_handles() {
    let node = spawn("shared-permits", &[]);
    let handler_conn = connect(&node).await;
    let mut handlers = handler_conn
        .default_bus()
        .command_handlers("pooled", "svc", 2)
        .await
        .unwrap();
    handlers.subscribe("Narrow", 100).await.unwrap();
    handlers.subscribe("Other", 100).await.unwrap();
    let dispatcher = connect(&node).await;
    dispatch_when_ready_probe(&dispatcher, &mut handlers).await;

    let sent: Vec<_> = [("s-1", "Narrow"), ("s-2", "Other"), ("s-3", "Narrow")]
        .into_iter()
        .map(|(id, name)| {
            let d = dispatcher.clone();
            tokio::spawn(async move {
                d.default_bus()
                    .dispatch_command(&command(id, name, "x"))
                    .await
            })
        })
        .collect();

    let first = within("a command", handlers.next()).await.unwrap().unwrap();
    let second = within("another command", handlers.next())
        .await
        .unwrap()
        .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(400), handlers.next())
            .await
            .is_err(),
        "a third command arrived on a window of two"
    );
    // Answering one hands a permit back, and the third is delivered.
    for incoming in [first, second] {
        let id = incoming.command.message_id.clone();
        incoming
            .respond(&Reply::ok(id, payload("done")))
            .await
            .unwrap();
    }
    let third = within("the third command", handlers.next())
        .await
        .unwrap()
        .unwrap();
    let id = third.command.message_id.clone();
    third
        .respond(&Reply::ok(id, payload("done")))
        .await
        .unwrap();

    for task in sent {
        let reply = within("a dispatch", task).await.unwrap().unwrap();
        assert!(reply.is_ok(), "{reply:?}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_reconnecting_handler_takes_over_from_its_stale_stream() {
    let node = spawn("takeover", &[]);
    let old_conn = connect(&node).await;
    let mut old = old_conn
        .default_bus()
        .command_handlers("twin", "svc", 8)
        .await
        .unwrap();
    old.subscribe("Twin", 100).await.unwrap();

    // The same client opens a new stream before the server noticed the old
    // one is dead. The old one is ended; its end must not take the new one's
    // registrations with it.
    let new_conn = connect(&node).await;
    let mut new = new_conn
        .default_bus()
        .command_handlers("twin", "svc", 8)
        .await
        .unwrap();
    new.subscribe("Twin", 100).await.unwrap();
    let err = within("the old stream is ended", old.next()).await.err();
    assert_eq!(
        err.and_then(|e| e.failure().map(|f| f.code)),
        Some(ErrorCode::UNAVAILABLE)
    );
    drop(old);

    let dispatcher = connect(&node).await;
    let pending = {
        let dispatcher = dispatcher.clone();
        tokio::spawn(
            async move { dispatch_when_ready(&dispatcher, &command("t1", "Twin", "x")).await },
        )
    };
    let incoming = within("the new stream serves", new.next())
        .await
        .unwrap()
        .unwrap();
    incoming
        .respond(&Reply::ok("t1", payload("from the new stream")))
        .await
        .unwrap();
    assert_eq!(
        text(&within("the answer", pending).await.unwrap()),
        "from the new stream"
    );
}

/// Serves one probe command on `handlers` so a test starts with the
/// registration in place and no permit in flight.
async fn dispatch_when_ready_probe(
    dispatcher: &Client,
    handlers: &mut kronosdb_client::CommandHandlers,
) {
    let probe = {
        let d = dispatcher.clone();
        tokio::spawn(async move { dispatch_when_ready(&d, &command("probe", "Narrow", "p")).await })
    };
    let incoming = within("the probe", handlers.next()).await.unwrap().unwrap();
    incoming
        .respond(&Reply::ok("probe", payload("ok")))
        .await
        .unwrap();
    assert!(within("the probe answer", probe).await.unwrap().is_ok());
}

/// A query handler on its own connection answering every `name` query with
/// `"<tag>"`.
async fn query_handler(node: &Node, name: &str, tag: &str) -> (Client, JoinHandle<()>) {
    let client = connect(node).await;
    let mut handlers = client
        .default_bus()
        .query_handlers(tag, "query-service", 16)
        .await
        .unwrap();
    handlers.subscribe(name, "text").await.unwrap();
    let tag = tag.to_string();
    let task = tokio::spawn(async move {
        while let Ok(Some(event)) = handlers.next().await {
            if let QueryHandlerEvent::Query(incoming) = event {
                let id = incoming.query.message_id.clone();
                if incoming
                    .respond(&Reply::ok(id, payload(&tag)))
                    .await
                    .is_err()
                {
                    break;
                }
            }
        }
    });
    (client, task)
}

async fn gather(client: &Client, q: &Query, expect: usize) -> Vec<String> {
    for _ in 0..100 {
        if let Ok(results) = client.default_bus().dispatch_query(q).await {
            let mut answers: Vec<String> =
                results.collect().await.unwrap().iter().map(text).collect();
            answers.sort();
            if answers.len() >= expect {
                return answers;
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("the query never gathered {expect} answers");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_scatter_gather_query_collects_every_handlers_answer() {
    let node = spawn("scatter", &[]);
    let (_c1, _h1) = query_handler(&node, "Count", "one").await;
    let (_c2, _h2) = query_handler(&node, "Count", "two").await;
    let dispatcher = connect(&node).await;

    let answers = gather(&dispatcher, &query("q1", "Count"), 2).await;
    assert_eq!(answers, vec!["one", "two"]);

    // Asking for one result goes to one handler.
    let mut single = query("q2", "Count");
    single.nr_of_results = Some(1);
    let results = dispatcher
        .default_bus()
        .dispatch_query(&single)
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(results.len(), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn cancelling_a_query_tells_the_handlers_still_working_on_it() {
    let node = spawn("cancel", &[]);
    let handler_conn = connect(&node).await;
    let mut handlers = handler_conn
        .default_bus()
        .query_handlers("slow-q", "svc", 4)
        .await
        .unwrap();
    handlers.subscribe("Slow", "text").await.unwrap();
    let dispatcher = connect(&node).await;

    let mut results = None;
    for _ in 0..100 {
        if let Ok(r) = dispatcher
            .default_bus()
            .dispatch_query(&query("slow-q1", "Slow"))
            .await
        {
            results = Some(r);
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let results = results.expect("the query is accepted");
    let event = within("the handler receives the query", handlers.next())
        .await
        .unwrap()
        .unwrap();
    let QueryHandlerEvent::Query(incoming) = event else {
        panic!("expected a query");
    };
    assert_eq!(incoming.query.message_id, "slow-q1");

    // The dispatcher gives up while the handler is still working.
    results.cancel().await;
    let event = within("the handler hears of the cancel", handlers.next())
        .await
        .unwrap()
        .unwrap();
    assert!(
        matches!(&event, QueryHandlerEvent::CancelQuery(id) if id == "slow-q1"),
        "unexpected event"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_subscription_query_delivers_an_initial_result_updates_and_a_completion() {
    let node = spawn("subscription", &[]);
    let handler_conn = connect(&node).await;
    let mut handlers = handler_conn
        .default_bus()
        .query_handlers("live", "svc", 4)
        .await
        .unwrap();
    handlers.subscribe("Watch", "text").await.unwrap();
    let handler = tokio::spawn(async move {
        loop {
            match handlers.next().await {
                Ok(Some(QueryHandlerEvent::Subscription(sub))) => {
                    sub.initial(&Reply::ok(sub.subscription_id.clone(), payload("initial")))
                        .await
                        .unwrap();
                    sub.update(payload("update-1")).await.unwrap();
                    sub.update(payload("update-2")).await.unwrap();
                    sub.complete().await.unwrap();
                }
                Ok(Some(_)) => {}
                _ => break,
            }
        }
    });

    let subscriber = connect(&node).await;
    let mut stream = None;
    for attempt in 0..100 {
        let q = query(&format!("sub-{attempt}"), "Watch");
        if let Ok(mut s) = subscriber
            .default_bus()
            .subscription_query(&format!("sub-{attempt}"), &q, 8)
            .await
        {
            // The first event tells whether a handler was there.
            if let Ok(Some(first)) = s.next().await {
                stream = Some((s, first));
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let (mut stream, first) = stream.expect("a subscription opens");
    let payload_of =
        |p: &Option<Payload>| String::from_utf8_lossy(&p.as_ref().unwrap().data).to_string();
    let SubscriptionEvent::Initial(initial) = first else {
        panic!("the initial result comes first");
    };
    assert_eq!(text(&initial), "initial");
    for expected in ["update-1", "update-2"] {
        match within("an update", stream.next()).await.unwrap() {
            Some(SubscriptionEvent::Update { payload, .. }) => {
                assert_eq!(payload_of(&payload), expected);
            }
            other => panic!("expected {expected}, got {other:?}"),
        }
    }
    assert_eq!(
        within("the completion", stream.next()).await.unwrap(),
        Some(SubscriptionEvent::Complete)
    );
    assert_eq!(within("the end", stream.next()).await.unwrap(), None);
    handler.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_subscriber_that_cancels_tells_its_handler() {
    let node = spawn("sub-cancel", &[]);
    let handler_conn = connect(&node).await;
    let mut handlers = handler_conn
        .default_bus()
        .query_handlers("live", "svc", 4)
        .await
        .unwrap();
    handlers.subscribe("Watch", "text").await.unwrap();
    let subscriber = connect(&node).await;
    let mut opened = None;
    for _ in 0..100 {
        if let Ok(s) = subscriber
            .default_bus()
            .subscription_query("sub-x", &query("sub-x", "Watch"), 8)
            .await
        {
            opened = Some(s);
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let stream = opened.expect("a subscription opens");
    let event = within("the handler is asked", handlers.next())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(event, QueryHandlerEvent::Subscription(_)));
    stream.cancel().await;
    let event = within("the handler hears of the cancel", handlers.next())
        .await
        .unwrap()
        .unwrap();
    assert!(
        matches!(&event, QueryHandlerEvent::CancelSubscription(id) if id == "sub-x"),
        "unexpected event"
    );
}

fn identity(id: &str) -> Identity {
    Identity {
        client_id: id.into(),
        component_name: "orders".into(),
        version: "1.2.3".into(),
        tags: vec![],
    }
}

async fn admin_json(node: &Node, path: &str) -> serde_json::Value {
    reqwest::get(format!("http://{}{path}", node.admin))
        .await
        .unwrap()
        .json()
        .await
        .unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn clients_register_on_the_platform_and_show_up_in_the_console() {
    let node = spawn("platform", &[]);
    let client = connect(&node).await;

    let info = client.register(&identity("svc-1")).await.unwrap();
    assert!(info.contexts.contains(&"default".to_string()));
    assert_eq!(info.version, env!("CARGO_PKG_VERSION"));

    // A client with no id cannot register.
    let err = client.register(&Identity::default()).await.unwrap_err();
    assert_eq!(err.failure().unwrap().code, ErrorCode::INVALID_ARGUMENT);

    let mut platform = client.open_platform(&identity("svc-1")).await.unwrap();
    let first = within("the node info", platform.next())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(first, PlatformEvent::Node(_)));
    let clients = admin_json(&node, "/api/v1/clients").await;
    let clients = clients.as_array().unwrap();
    assert_eq!(clients.len(), 1);
    assert_eq!(clients[0]["client_id"], "svc-1");
    assert_eq!(clients[0]["component"], "orders");
    assert_eq!(clients[0]["streaming"], true);

    // Handlers registering are announced to connected clients.
    let (_h, _task) = echo_handler(&node, "Announced", "announcer").await;
    loop {
        match within("a topology change", platform.next())
            .await
            .unwrap()
            .unwrap()
        {
            PlatformEvent::Topology(t) => {
                assert_eq!(t.change_type, "handler_registered");
                assert_eq!(t.message_type, "Announced");
                assert_eq!(t.handler_kind, "command");
                break;
            }
            _ => continue,
        }
    }

    // When the platform stream ends the client is gone.
    drop(platform);
    for _ in 0..100 {
        if admin_json(&node, "/api/v1/clients")
            .await
            .as_array()
            .unwrap()
            .is_empty()
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("the client stayed registered after its platform stream ended");
}

#[tokio::test(flavor = "multi_thread")]
async fn processor_status_is_shown_and_instructions_reach_the_client() {
    let node = spawn("processors", &[]);
    let client = connect(&node).await;
    let mut platform = client
        .open_platform(&identity("proc-client"))
        .await
        .unwrap();
    within("the node info", platform.next()).await.unwrap();

    platform
        .report(&ProcessorStatus {
            processor_name: "OrderProjection".into(),
            mode: "Tracking".into(),
            active_threads: 1,
            available_threads: 3,
            running: true,
            streaming: true,
            token_store_identifier: "store-1".into(),
            segments: vec![SegmentStatus {
                segment_id: 0,
                caught_up: true,
                one_part_of: 1,
                token_position: 42,
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .unwrap();
    let mut shown = serde_json::Value::Null;
    for _ in 0..100 {
        let processors = admin_json(&node, "/api/v1/processors").await;
        if processors.as_array().is_some_and(|p| !p.is_empty()) {
            shown = processors;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(shown[0]["name"], "OrderProjection");
    assert_eq!(
        shown[0]["instances"][0]["segments"][0]["token_position"],
        42
    );

    // The console pauses it; the client is told, and answers.
    let http = reqwest::Client::new();
    let sent: serde_json::Value = http
        .post(format!(
            "http://{}/api/processors/OrderProjection/pause",
            node.admin
        ))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(sent["ok"], true, "{sent}");
    let instruction = loop {
        match within("the instruction", platform.next())
            .await
            .unwrap()
            .unwrap()
        {
            PlatformEvent::Instruction(i) => break i,
            _ => continue,
        }
    };
    assert_eq!(instruction.action, ProcessorAction::Pause);
    assert_eq!(instruction.processor_name, "OrderProjection");
    platform
        .answer(&instruction.instruction_id, true)
        .await
        .unwrap();

    // Splitting names the segment.
    let _ = http
        .post(format!(
            "http://{}/api/processors/OrderProjection/split",
            node.admin
        ))
        .send()
        .await
        .unwrap();
    let instruction = loop {
        match within("the split", platform.next()).await.unwrap().unwrap() {
            PlatformEvent::Instruction(i) => break i,
            _ => continue,
        }
    };
    assert_eq!(instruction.action, ProcessorAction::SplitSegment);
    assert_eq!(instruction.segment, 0);

    // A processor nobody runs has no one to tell.
    let none: serde_json::Value = http
        .post(format!("http://{}/api/processors/Nobody/pause", node.admin))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(none["ok"], false);
}

#[tokio::test(flavor = "multi_thread")]
async fn clients_are_asked_to_reconnect_when_the_server_shuts_down() {
    let mut node = spawn("shutdown", &[("KRONOSDB_DRAIN_DEADLINE", "5")]);
    let client = connect(&node).await;
    let mut platform = client
        .open_platform(&identity("shutdown-client"))
        .await
        .unwrap();
    within("the node info", platform.next()).await.unwrap();

    let status = Process::new("kill")
        .arg("-TERM")
        .arg(node.child.id().to_string())
        .status()
        .unwrap();
    assert!(status.success());
    loop {
        match within("the reconnect request", platform.next())
            .await
            .unwrap()
        {
            Some(PlatformEvent::ReconnectRequested) => break,
            Some(_) => continue,
            None => panic!("the stream ended without asking to reconnect"),
        }
    }
    assert!(
        client.connection().is_draining(),
        "and the connection was told to drain"
    );
    let _ = node.child.wait();
}
