//! The native protocol end to end: a real server process, the Rust client.

use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use kronosdb_client::{
    Appended, Client, Condition, ConnectOptions, Criterion, Delivery, ErrorCode, NewEvent,
    SubscribeOptions,
};

struct Node {
    child: Child,
    native: String,
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
        .join(format!("native-e2e-{}-{name}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn spawn(dir: &Path) -> Node {
    spawn_with(dir, &[])
}

fn spawn_with(dir: &Path, env: &[(&str, &str)]) -> Node {
    let native = format!("127.0.0.1:{}", free_port());
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_kronosdb-server"));
    cmd.envs(env.iter().copied());
    cmd.current_dir(dir)
        .env("KRONOSDB_LISTEN", &native)
        .env(
            "KRONOSDB_ADMIN_LISTEN",
            format!("127.0.0.1:{}", free_port()),
        )
        .env("KRONOSDB_DATA_DIR", dir.join("data"))
        .env("RUST_LOG", "warn");
    if std::env::var_os("KRONOSDB_TEST_LOGS").is_some() {
        cmd.stdout(Stdio::inherit()).stderr(Stdio::inherit());
    } else {
        cmd.stdout(Stdio::null()).stderr(Stdio::null());
    }
    Node {
        child: cmd.spawn().expect("spawn kronosdb-server"),
        native,
    }
}

/// Connects once the server is up and its write path is ready.
async fn connect(node: &Node) -> Client {
    for _ in 0..200 {
        if let Ok(client) = Client::connect(&node.native, &ConnectOptions::default()).await
            && client.default_context().head().await.is_ok()
        {
            return client;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("the server never became ready on {}", node.native);
}

fn event(name: &str, order: &str, n: u32) -> NewEvent {
    NewEvent {
        identifier: format!("{order}-{n}"),
        name: name.into(),
        version: "1".into(),
        timestamp: 1_761_300_000_000 + n as i64,
        metadata: vec![("n".into(), n.to_string())],
        tags: vec![(b"orderId".to_vec(), order.as_bytes().to_vec())],
        payload: format!("{{\"n\":{n}}}").into_bytes().into(),
    }
}

async fn append_ok(
    ctx: &kronosdb_client::Context,
    events: &[NewEvent],
    condition: Option<&Condition>,
) -> u64 {
    // Right after startup the write path may still be installing its epoch.
    for _ in 0..100 {
        match ctx.append(events, condition).await {
            Ok(Appended::Ok { marker, .. }) => return marker,
            Ok(Appended::Conflict { position }) => panic!("unexpected conflict at {position}"),
            Err(e)
                if e.failure()
                    .is_some_and(|f| f.code == ErrorCode::UNAVAILABLE) =>
            {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Err(e) => panic!("append failed: {e}"),
        }
    }
    panic!("the write path never became available");
}

#[tokio::test]
async fn event_store_over_the_native_protocol() {
    let dir = work_dir("event-store");
    let node = spawn(&dir);
    let client = connect(&node).await;
    let ctx = client.default_context();
    let me = client.who_am_i().await.unwrap();
    assert_eq!(
        (me.subject.as_str(), me.authentication_enabled),
        ("anonymous", false)
    );

    // Appends, and a read of one aggregate.
    for n in 0..5 {
        append_ok(
            &ctx,
            &[event("ItemAdded", "o-1", n), event("ItemAdded", "o-2", n)],
            None,
        )
        .await;
    }
    let (batches, marker) = ctx
        .source_all(0, &[Criterion::tag("orderId", "o-1")])
        .await
        .unwrap();
    let events: Vec<_> = batches
        .iter()
        .flat_map(|b| {
            b.iter()
                .map(|e| e.identifier.to_string())
                .collect::<Vec<_>>()
        })
        .collect();
    assert_eq!(events, vec!["o-1-0", "o-1-1", "o-1-2", "o-1-3", "o-1-4"]);
    let first = batches[0].iter().next().unwrap();
    assert_eq!(first.name, "ItemAdded");
    assert_eq!(first.payload, b"{\"n\":0}");
    assert_eq!(first.tags[0].value, b"o-1");
    assert_eq!(marker, ctx.head().await.unwrap());

    // The decide loop: an append conditioned on the read succeeds once, and
    // a second writer holding the same marker conflicts.
    let condition = Condition {
        marker,
        criteria: vec![Criterion::tag("orderId", "o-1")],
    };
    let ok = ctx
        .append(&[event("OrderPlaced", "o-1", 99)], Some(&condition))
        .await
        .unwrap();
    assert!(matches!(ok, Appended::Ok { count: 1, .. }));
    let late = ctx
        .append(&[event("OrderPlaced", "o-1", 100)], Some(&condition))
        .await
        .unwrap();
    assert!(matches!(late, Appended::Conflict { .. }), "got {late:?}");

    // Many small batches in one read: the server pages by batch size.
    let mut source = ctx.source(0, &[], 3).await.unwrap();
    let mut total = 0;
    let mut frames = 0;
    while let Some(batch) = source.next().await.unwrap() {
        frames += 1;
        total += batch.len();
        if batch.marker().is_some() {
            break;
        }
    }
    assert_eq!(total, 11);
    assert!(frames >= 4, "{frames} frames for 11 events at batch 3");

    // A filtered subscription: catches up, then sees new events live.
    let mut sub = ctx
        .subscribe(
            0,
            SubscribeOptions {
                criteria: vec![Criterion::tag("orderId", "o-2")],
                credits: 2,
                ..SubscribeOptions::default()
            },
        )
        .await
        .unwrap();
    let mut seen = Vec::new();
    while seen.len() < 5 {
        match tokio::time::timeout(Duration::from_secs(5), sub.next())
            .await
            .unwrap()
            .unwrap()
        {
            Some(Delivery::Events(batch)) => {
                assert!(batch.len() <= 2, "the server sent past its credit");
                seen.extend(batch.iter().map(|e| e.identifier.to_string()));
            }
            Some(Delivery::Progress(_)) => {}
            None => panic!("subscription ended"),
        }
    }
    assert_eq!(seen, vec!["o-2-0", "o-2-1", "o-2-2", "o-2-3", "o-2-4"]);
    append_ok(
        &ctx,
        &[event("ItemAdded", "o-3", 1), event("ItemAdded", "o-2", 7)],
        None,
    )
    .await;
    let live = loop {
        match tokio::time::timeout(Duration::from_secs(5), sub.next())
            .await
            .unwrap()
            .unwrap()
        {
            Some(Delivery::Events(batch)) => {
                break batch
                    .iter()
                    .map(|e| e.identifier.to_string())
                    .collect::<Vec<_>>();
            }
            Some(Delivery::Progress(_)) => continue,
            None => panic!("subscription ended"),
        }
    };
    assert_eq!(live, vec!["o-2-7"], "the o-3 event must be filtered out");
    sub.cancel().await;

    // A context that does not exist cannot be bound.
    let missing = client.context("nope").await.err().unwrap();
    assert_eq!(missing.failure().unwrap().code, ErrorCode::NOT_FOUND);
    // The connection is fine afterwards.
    assert!(ctx.head().await.unwrap() > 0);
}

/// Records go from the client's frame into the segment as they are (the server
/// sets the position and the CRC): whatever shape an event has comes back
/// exactly, in the order sent, and an event reaching into the server's `$`
/// namespace is refused as before, without writing its batch.
#[tokio::test]
async fn appended_records_read_back_exactly_and_reserved_ones_are_refused() {
    let dir = work_dir("records");
    let node = spawn(&dir);
    let client = connect(&node).await;
    let ctx = client.default_context();

    let shapes = vec![
        NewEvent {
            identifier: String::new(),
            name: "Bare".into(),
            version: String::new(),
            timestamp: -5,
            metadata: vec![],
            tags: vec![],
            payload: bytes::Bytes::new(),
        },
        NewEvent {
            identifier: "päid-1".into(),
            name: "日本語".into(),
            version: "2.0".into(),
            timestamp: i64::MAX,
            metadata: vec![("k1".into(), "välue".into()), ("k2".into(), String::new())],
            tags: vec![
                (b"orderId".to_vec(), b"o-9".to_vec()),
                (vec![0xff, 0xfe], vec![0, 1, 2]),
                (b"empty".to_vec(), vec![]),
            ],
            payload: bytes::Bytes::from(vec![7u8; 300_000]),
        },
        event("OrderPlaced", "o-9", 3),
    ];
    append_ok(&ctx, &shapes, None).await;
    // A second, conditioned append goes the same way.
    let marker = ctx.head().await.unwrap();
    let condition = Condition {
        marker,
        criteria: vec![Criterion::tag("orderId", "o-9")],
    };
    append_ok(&ctx, &[event("OrderPlaced", "o-9", 4)], Some(&condition)).await;

    let (batches, _) = ctx.source_all(0, &[]).await.unwrap();
    let read: Vec<_> = batches.iter().flat_map(|b| b.to_events()).collect();
    assert_eq!(read.len(), 4);
    for (i, (got, sent)) in read.iter().zip(shapes.iter()).enumerate() {
        assert_eq!(got.position, i as u64);
        assert_eq!(got.identifier, sent.identifier);
        assert_eq!(got.name, sent.name);
        assert_eq!(got.version, sent.version);
        assert_eq!(got.timestamp, sent.timestamp);
        assert_eq!(got.metadata, sent.metadata);
        assert_eq!(got.tags, sent.tags);
        assert_eq!(got.payload, sent.payload);
    }
    assert_eq!(read[3].identifier, "o-9-4");

    // The tag index was built from the record bytes: lookups find the events.
    let (by_tag, _) = ctx
        .source_all(0, &[Criterion::tag("orderId", "o-9")])
        .await
        .unwrap();
    let ids: Vec<_> = by_tag
        .iter()
        .flat_map(|b| b.to_events())
        .map(|e| e.position)
        .collect();
    assert_eq!(ids, vec![1, 2, 3]);

    // The reserved namespace is closed to clients, event types and tags alike,
    // and a refused batch writes nothing, not even its good events.
    let head = ctx.head().await.unwrap();
    for bad in [
        NewEvent {
            name: "$schedule.created".into(),
            ..event("x", "o-1", 1)
        },
        NewEvent {
            tags: vec![(b"$sys".to_vec(), b"1".to_vec())],
            ..event("OrderPlaced", "o-1", 2)
        },
    ] {
        let refused = ctx
            .append(&[event("OrderPlaced", "o-1", 3), bad], None)
            .await
            .unwrap_err();
        let failure = refused.failure().unwrap();
        assert_eq!(failure.code, ErrorCode::INVALID_ARGUMENT);
        assert!(
            failure.message.starts_with("reserved namespace: "),
            "{}",
            failure.message
        );
    }
    assert_eq!(ctx.head().await.unwrap(), head);
}

/// What one subscription of the live-tail test asks for.
struct Ask {
    label: &'static str,
    from: u64,
    criteria: Vec<Criterion>,
    exclude: Vec<&'static str>,
    /// Events the server may send ahead of the consumer.
    credits: u32,
}

/// Events of `ctx` a subscription from `from` with these criteria and
/// exclusions must deliver, by a plain read.
async fn expected_events(ctx: &kronosdb_client::Context, ask: &Ask) -> Vec<(u64, String)> {
    let (batches, _) = ctx.source_all(ask.from, &ask.criteria).await.unwrap();
    batches
        .iter()
        .flat_map(|b| {
            b.iter()
                .filter(|e| !ask.exclude.contains(&e.name))
                .map(|e| (e.position, e.identifier.to_string()))
                .collect::<Vec<_>>()
        })
        .collect()
}

/// A subscription read on its own task and its own connection, as a real
/// consumer would: what it has delivered so far is shared.
struct Collector {
    got: std::sync::Arc<std::sync::Mutex<Vec<(u64, String)>>>,
    task: tokio::task::JoinHandle<()>,
}

impl Collector {
    async fn start(node: &Node, ask: &Ask) -> Collector {
        Collector::start_slow(node, ask, Duration::ZERO).await
    }

    /// A consumer that takes `pause` to handle every batch.
    async fn start_slow(node: &Node, ask: &Ask, pause: Duration) -> Collector {
        // Its own connection: a client connection is read by one task, so a
        // subscription nobody reads would stall every request sharing it
        // once its stream queue is full.
        let client = Client::connect(&node.native, &ConnectOptions::default())
            .await
            .unwrap();
        let mut sub = client
            .default_context()
            .subscribe(
                ask.from,
                SubscribeOptions {
                    criteria: ask.criteria.clone(),
                    exclude_names: ask.exclude.iter().map(|n| n.to_string()).collect(),
                    credits: ask.credits,
                    progress_interval_ms: 25,
                    ..SubscribeOptions::default()
                },
            )
            .await
            .unwrap();
        let got = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let task = tokio::spawn({
            let got = got.clone();
            async move {
                let _client = client;
                while let Ok(Some(delivery)) = sub.next().await {
                    if let Delivery::Events(batch) = delivery {
                        got.lock().unwrap().extend(
                            batch
                                .iter()
                                .map(|e| (e.position, e.identifier.to_string()))
                                .collect::<Vec<_>>(),
                        );
                        if !pause.is_zero() {
                            tokio::time::sleep(pause).await;
                        }
                    }
                }
            }
        });
        Collector { got, task }
    }

    /// Waits until `want` events arrived, then checks that nothing more does.
    async fn finish(self, want: usize, label: &str) -> Vec<(u64, String)> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        while self.got.lock().unwrap().len() < want {
            assert!(
                tokio::time::Instant::now() < deadline,
                "{label}: stalled at {} of {want} events",
                self.got.lock().unwrap().len()
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        // Anything repeated or stray arrives within a moment.
        tokio::time::sleep(Duration::from_millis(400)).await;
        self.task.abort();
        self.got.lock().unwrap().clone()
    }
}

/// Runs a test body under a hard deadline: a hang fails fast instead of
/// holding a test run (and whoever waits on it) for hours.
async fn within<F: std::future::Future>(secs: u64, label: &str, body: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(secs), body)
        .await
        .unwrap_or_else(|_| panic!("{label}: no result within {secs}s"))
}

/// Subscriptions that start far behind the head, at it, and in the middle of
/// the writes all receive exactly the events they asked for, in order and
/// once, while eight writers append and the log rotates.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn subscriptions_catch_up_through_heavy_appends_and_get_exactly_their_events() {
    within(180, "heavy appends", subscriptions_under_heavy_appends()).await;
}

async fn subscriptions_under_heavy_appends() {
    let dir = work_dir("live-tail");
    let node = spawn_with(&dir, &[("KRONOSDB_SEGMENT_SIZE", "1048576")]);
    let client = connect(&node).await;
    let ctx = client.default_context();

    const NAMES: [&str; 4] = ["ItemAdded", "OrderPlaced", "Noise", "OrderShipped"];
    let batch = |writer: u64, round: u64| -> Vec<NewEvent> {
        (0..1 + round % 4)
            .map(|i| {
                let n = writer * 1_000_000 + round * 10 + i;
                let order = format!("o-{}", (writer * 7 + round + i) % 40);
                event(NAMES[(n % 4) as usize], &order, n as u32)
            })
            .collect()
    };

    // A backlog every early subscription has to read through.
    for round in 0..1_500 {
        append_ok(&ctx, &batch(99, round), None).await;
    }
    let start = ctx.head().await.unwrap();

    let asks = vec![
        Ask {
            label: "everything from the start",
            from: 0,
            criteria: vec![],
            exclude: vec![],
            credits: 64,
        },
        Ask {
            label: "everything but Noise",
            from: 0,
            criteria: vec![],
            exclude: vec!["Noise"],
            credits: 64,
        },
        Ask {
            label: "one order from the start",
            from: 0,
            criteria: vec![Criterion::tag("orderId", "o-3")],
            exclude: vec![],
            credits: 64,
        },
        Ask {
            label: "two orders and a type, from the middle of the backlog",
            from: start / 2,
            criteria: vec![
                Criterion::tag("orderId", "o-5"),
                Criterion {
                    names: vec!["OrderShipped".into()],
                    tags: vec![(b"orderId".to_vec(), b"o-9".to_vec())],
                },
            ],
            exclude: vec![],
            credits: 64,
        },
        Ask {
            label: "types only, live",
            from: start,
            criteria: vec![Criterion {
                names: vec!["ItemAdded".into(), "OrderPlaced".into()],
                tags: vec![],
            }],
            exclude: vec![],
            credits: 64,
        },
        Ask {
            label: "everything from the start, credit for whole shared batches",
            from: 0,
            criteria: vec![],
            exclude: vec![],
            credits: 100_000,
        },
        Ask {
            label: "everything, live, credit for whole shared batches",
            from: start,
            criteria: vec![],
            exclude: vec![],
            credits: 100_000,
        },
        Ask {
            label: "an order nobody writes",
            from: 0,
            criteria: vec![Criterion::tag("orderId", "nobody")],
            exclude: vec![],
            credits: 64,
        },
    ];
    let mut collectors = Vec::new();
    for ask in &asks {
        collectors.push(Collector::start(&node, ask).await);
    }

    // Heavy appends while the subscriptions catch up; one more subscription
    // joins mid-flight.
    let mut writers = Vec::new();
    for w in 0..8u64 {
        let (ctx, writes) = (ctx.clone(), batch);
        writers.push(tokio::spawn(async move {
            for round in 0..250 {
                append_ok(&ctx, &writes(w, round), None).await;
            }
        }));
    }
    tokio::time::sleep(Duration::from_millis(50)).await;
    let late_from = ctx.head().await.unwrap().saturating_sub(300);
    let late_ask = Ask {
        label: "joined mid-flight",
        from: late_from,
        criteria: vec![Criterion::tag("orderId", "o-1")],
        exclude: vec![],
        credits: 16,
    };
    let late = Collector::start(&node, &late_ask).await;
    for writer in writers {
        writer.await.unwrap();
    }

    let mut all: Vec<(Ask, Collector)> = asks.into_iter().zip(collectors).collect();
    all.push((late_ask, late));
    for (ask, collector) in all {
        let expected = expected_events(&ctx, &ask).await;
        let got = collector.finish(expected.len(), ask.label).await;
        assert!(
            got.windows(2).all(|w| w[0].0 < w[1].0),
            "{}: events out of order or repeated",
            ask.label
        );
        assert_eq!(got.len(), expected.len(), "{}", ask.label);
        assert_eq!(got, expected, "{}", ask.label);
    }
}

/// A page that delivers a match and then scans a quiet tail must still let
/// the reader learn about the tail: delivered events only account for the
/// log up to the last of them, so a Progress has to cover the rest even when
/// nothing is appended after it.
#[tokio::test]
async fn a_quiet_tail_behind_the_last_match_is_reported() {
    within(
        60,
        "a_quiet_tail_behind_the_last_match_is_reported",
        async {
            let dir = work_dir("quiet-tail");
            let node = spawn(&dir);
            let client = connect(&node).await;
            let ctx = client.default_context();
            // One match, then a quiet stretch, in one append.
            let mut events = vec![event("ItemAdded", "tail-match", 0)];
            events.extend((0..30).map(|n| event("ItemAdded", "elsewhere", n)));
            append_ok(&ctx, &events, None).await;
            let head = ctx.head().await.unwrap();

            let mut sub = ctx
                .subscribe(
                    0,
                    SubscribeOptions {
                        criteria: vec![Criterion::tag("orderId", "tail-match")],
                        progress_interval_ms: 20,
                        ..SubscribeOptions::default()
                    },
                )
                .await
                .unwrap();
            let mut matched = false;
            let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
            loop {
                match tokio::time::timeout_at(deadline, sub.next())
                    .await
                    .expect("the quiet tail was never reported")
                    .unwrap()
                {
                    Some(Delivery::Events(batch)) => {
                        assert_eq!(batch.len(), 1, "only the match is delivered");
                        matched = true;
                    }
                    Some(Delivery::Progress(p)) if p >= head => break,
                    Some(Delivery::Progress(_)) => {}
                    None => panic!("subscription ended"),
                }
            }
            assert!(matched, "the match arrives before the progress past it");
        },
    )
    .await;
}

/// A filtered subscription whose events never come still sees its position
/// move: the server reports the frontier even though no commit wakes it.
#[tokio::test]
async fn a_quiet_filtered_subscription_still_reports_progress() {
    within(
        60,
        "a_quiet_filtered_subscription_still_reports_progress",
        async {
            let dir = work_dir("quiet-progress");
            let node = spawn(&dir);
            let client = connect(&node).await;
            let ctx = client.default_context();
            append_ok(&ctx, &[event("ItemAdded", "o-1", 0)], None).await;

            let mut sub = ctx
                .subscribe(
                    0,
                    SubscribeOptions {
                        criteria: vec![Criterion::tag("orderId", "quiet")],
                        progress_interval_ms: 20,
                        ..SubscribeOptions::default()
                    },
                )
                .await
                .unwrap();
            for n in 0..30 {
                append_ok(&ctx, &[event("ItemAdded", "o-2", n)], None).await;
            }
            let head = ctx.head().await.unwrap();
            let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
            loop {
                match tokio::time::timeout_at(deadline, sub.next())
                    .await
                    .expect("progress never reached the head")
                    .unwrap()
                {
                    Some(Delivery::Progress(p)) if p >= head => break,
                    Some(Delivery::Progress(_)) => {}
                    other => panic!("nothing matches, got {other:?}"),
                }
            }

            // A match after the quiet stretch is delivered, and progress never
            // claims past an event the client has not been given.
            append_ok(&ctx, &[event("ItemAdded", "quiet", 1)], None).await;
            let mut position = None;
            let mut last_progress = head;
            while position.is_none() {
                match tokio::time::timeout(Duration::from_secs(10), sub.next())
                    .await
                    .expect("the matching event never arrived")
                    .unwrap()
                {
                    Some(Delivery::Events(batch)) => {
                        position = Some(batch.iter().next().unwrap().position);
                    }
                    Some(Delivery::Progress(p)) => {
                        // The event sits at `head`: progress past it, before the
                        // client has the event, would tell it nothing is missing.
                        assert!(p <= head, "progress {p} claims past the unseen event");
                        last_progress = p;
                    }
                    None => panic!("subscription ended"),
                }
            }
            assert_eq!(position, Some(head));
            assert!(last_progress <= head);
        },
    )
    .await;
}

/// A stream that wants every event shares the dispatcher's batches, but a
/// batch is only sent whole when its client has the credit for all of it.
#[tokio::test]
async fn a_live_stream_that_wants_everything_never_outruns_its_credit() {
    within(
        60,
        "a_live_stream_that_wants_everything_never_outruns_its_credit",
        async {
            let dir = work_dir("shared-credit");
            let node = spawn(&dir);
            let client = connect(&node).await;
            let ctx = client.default_context();
            append_ok(&ctx, &[event("ItemAdded", "o-1", 0)], None).await;

            let from = ctx.head().await.unwrap();
            let mut sub = ctx
                .subscribe(
                    from,
                    SubscribeOptions {
                        credits: 2,
                        ..SubscribeOptions::default()
                    },
                )
                .await
                .unwrap();
            // One commit of ten events: a shared batch of ten, for a client that
            // granted two.
            let events: Vec<_> = (1..=10).map(|n| event("ItemAdded", "o-1", n)).collect();
            append_ok(&ctx, &events, None).await;
            append_ok(&ctx, &[event("ItemAdded", "o-2", 11)], None).await;

            let mut seen = Vec::new();
            while seen.len() < 11 {
                match tokio::time::timeout(Duration::from_secs(5), sub.next())
                    .await
                    .expect("the stream stalled")
                    .unwrap()
                {
                    Some(Delivery::Events(batch)) => {
                        assert!(batch.len() <= 2, "{} events for a credit of 2", batch.len());
                        seen.extend(batch.iter().map(|e| e.identifier.to_string()));
                    }
                    Some(Delivery::Progress(_)) => {}
                    None => panic!("subscription ended"),
                }
            }
            let expected: Vec<String> = (1..=10)
                .map(|n| format!("o-1-{n}"))
                .chain(["o-2-11".to_string()])
                .collect();
            assert_eq!(seen, expected);
            sub.cancel().await;
        },
    )
    .await;
}

/// Filtering subscriptions at the live tail take their events from the
/// dispatcher's chunks. Whether they start behind the head, at it or in the
/// middle of the writes, run slowly on a small credit window, or share the
/// server with subscriptions that come and go, each gets exactly its events,
/// in order and once, while eight writers append and the log rotates.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn filtered_subscriptions_served_from_live_chunks_get_exactly_their_events() {
    within(
        240,
        "filtered live chunks",
        filtered_subscriptions_scenario(),
    )
    .await;
}

async fn filtered_subscriptions_scenario() {
    let dir = work_dir("filtered-chunks");
    let node = spawn_with(&dir, &[("KRONOSDB_SEGMENT_SIZE", "1048576")]);
    let client = connect(&node).await;
    let ctx = client.default_context();

    const NAMES: [&str; 4] = ["ItemAdded", "OrderPlaced", "Noise", "OrderShipped"];
    // Forty orders: each is about 2.5% of the events.
    let batch = |writer: u64, round: u64| -> Vec<NewEvent> {
        (0..1 + round % 4)
            .map(|i| {
                let n = writer * 1_000_000 + round * 10 + i;
                let order = format!("o-{}", (writer * 7 + round * 3 + i) % 40);
                event(NAMES[(n % 4) as usize], &order, n as u32)
            })
            .collect()
    };
    for round in 0..800 {
        append_ok(&ctx, &batch(99, round), None).await;
    }
    let start = ctx.head().await.unwrap();

    let by_order = |o: u32| vec![Criterion::tag("orderId", format!("o-{o}"))];
    let mut asks = Vec::new();
    for o in 0..12 {
        asks.push((
            Ask {
                label: "one order, live",
                from: start,
                criteria: by_order(o),
                exclude: vec![],
                credits: 64,
            },
            Duration::ZERO,
        ));
    }
    asks.extend([
        (
            Ask {
                label: "one order from the start, then live",
                from: 0,
                criteria: by_order(7),
                exclude: vec![],
                credits: 64,
            },
            Duration::ZERO,
        ),
        (
            Ask {
                label: "a slow consumer on a credit of two",
                from: start,
                criteria: by_order(3),
                exclude: vec![],
                credits: 2,
            },
            Duration::from_millis(15),
        ),
        (
            Ask {
                label: "a slow consumer that starts behind the head",
                from: start / 2,
                criteria: by_order(11),
                exclude: vec![],
                credits: 4,
            },
            Duration::from_millis(8),
        ),
        (
            Ask {
                label: "two orders, one narrowed to a type, minus a type",
                from: start,
                criteria: vec![
                    Criterion::tag("orderId", "o-5"),
                    Criterion {
                        names: vec!["OrderShipped".into(), "Noise".into()],
                        tags: vec![(b"orderId".to_vec(), b"o-9".to_vec())],
                    },
                ],
                exclude: vec!["Noise"],
                credits: 64,
            },
            Duration::ZERO,
        ),
        (
            Ask {
                label: "types only, minus one",
                from: start,
                criteria: vec![Criterion {
                    names: vec!["ItemAdded".into(), "OrderPlaced".into()],
                    tags: vec![],
                }],
                exclude: vec!["OrderPlaced"],
                credits: 256,
            },
            Duration::ZERO,
        ),
        (
            Ask {
                label: "everything but a type (picks, not a shared batch)",
                from: start,
                criteria: vec![],
                exclude: vec!["Noise"],
                credits: 256,
            },
            Duration::ZERO,
        ),
        (
            Ask {
                label: "an order nobody writes",
                from: start,
                criteria: by_order(99),
                exclude: vec![],
                credits: 64,
            },
            Duration::ZERO,
        ),
    ]);
    let mut collectors = Vec::new();
    for (ask, pause) in &asks {
        collectors.push(Collector::start_slow(&node, ask, *pause).await);
    }

    let mut writers = Vec::new();
    for w in 0..8u64 {
        let (ctx, writes) = (ctx.clone(), batch);
        writers.push(tokio::spawn(async move {
            for round in 0..250 {
                append_ok(&ctx, &writes(w, round), None).await;
            }
        }));
    }
    // Subscriptions that come and go while the writes run: they take and
    // give back slots the others' picks are filed under.
    let churn = {
        let node_addr = node.native.clone();
        tokio::spawn(async move {
            for i in 0..40u32 {
                let client = Client::connect(&node_addr, &ConnectOptions::default())
                    .await
                    .unwrap();
                let mut sub = client
                    .default_context()
                    .subscribe(
                        0,
                        SubscribeOptions {
                            criteria: vec![Criterion::tag("orderId", format!("o-{}", i % 40))],
                            credits: 8,
                            ..SubscribeOptions::default()
                        },
                    )
                    .await
                    .unwrap();
                let _ = tokio::time::timeout(Duration::from_millis(30), sub.next()).await;
                sub.cancel().await;
            }
        })
    };
    // And one that joins mid-flight, asking for a little of the past.
    tokio::time::sleep(Duration::from_millis(60)).await;
    let late_ask = Ask {
        label: "joined mid-flight",
        from: ctx.head().await.unwrap().saturating_sub(200),
        criteria: by_order(1),
        exclude: vec![],
        credits: 16,
    };
    let late = Collector::start(&node, &late_ask).await;
    for writer in writers {
        writer.await.unwrap();
    }
    churn.await.unwrap();

    let mut all: Vec<(Ask, Collector)> = asks
        .into_iter()
        .map(|(ask, _)| ask)
        .zip(collectors)
        .collect();
    all.push((late_ask, late));
    let mut delivered = 0;
    for (ask, collector) in all {
        let expected = expected_events(&ctx, &ask).await;
        let got = collector.finish(expected.len(), ask.label).await;
        assert!(
            got.windows(2).all(|w| w[0].0 < w[1].0),
            "{}: events out of order or repeated",
            ask.label
        );
        assert_eq!(got.len(), expected.len(), "{}", ask.label);
        assert_eq!(got, expected, "{}", ask.label);
        delivered += got.len();
    }
    assert!(delivered > 1_000, "the scenario delivered only {delivered}");
}

/// A filtering stream sends a chunk's matches only as far as its client's
/// credit goes, then carries on from the same chunk.
#[tokio::test]
async fn a_filtered_live_stream_never_outruns_its_credit() {
    within(
        60,
        "a_filtered_live_stream_never_outruns_its_credit",
        async {
            let dir = work_dir("filtered-credit");
            let node = spawn(&dir);
            let client = connect(&node).await;
            let ctx = client.default_context();
            append_ok(&ctx, &[event("ItemAdded", "o-1", 0)], None).await;

            let from = ctx.head().await.unwrap();
            let mut sub = ctx
                .subscribe(
                    from,
                    SubscribeOptions {
                        criteria: vec![Criterion::tag("orderId", "o-1")],
                        credits: 2,
                        ..SubscribeOptions::default()
                    },
                )
                .await
                .unwrap();
            // One commit of twenty events, ten of them the stream's, mixed in.
            let events: Vec<_> = (1..=20)
                .map(|n| event("ItemAdded", if n % 2 == 0 { "o-1" } else { "o-2" }, n))
                .collect();
            append_ok(&ctx, &events, None).await;
            append_ok(&ctx, &[event("ItemAdded", "o-1", 21)], None).await;

            let mut seen = Vec::new();
            while seen.len() < 11 {
                match tokio::time::timeout(Duration::from_secs(5), sub.next())
                    .await
                    .expect("the stream stalled")
                    .unwrap()
                {
                    Some(Delivery::Events(batch)) => {
                        assert!(batch.len() <= 2, "{} events for a credit of 2", batch.len());
                        seen.extend(batch.iter().map(|e| e.identifier.to_string()));
                    }
                    Some(Delivery::Progress(_)) => {}
                    None => panic!("subscription ended"),
                }
            }
            let expected: Vec<String> = (1..=10)
                .map(|k| format!("o-1-{}", 2 * k))
                .chain(["o-1-21".to_string()])
                .collect();
            assert_eq!(seen, expected);
            // Nothing else is coming.
            assert!(
                tokio::time::timeout(Duration::from_millis(300), sub.next())
                    .await
                    .is_err()
            );
            sub.cancel().await;
        },
    )
    .await;
}

/// Progress on a filtered stream fed from chunks still never claims a
/// position past an event the client has not been given, and still reaches
/// the head through quiet stretches.
#[tokio::test]
async fn filtered_progress_never_claims_past_an_unseen_event() {
    within(
        60,
        "filtered_progress_never_claims_past_an_unseen_event",
        async {
            let dir = work_dir("filtered-progress");
            let node = spawn(&dir);
            let client = connect(&node).await;
            let ctx = client.default_context();
            append_ok(&ctx, &[event("ItemAdded", "o-0", 0)], None).await;
            let from = ctx.head().await.unwrap();

            let mut sub = ctx
                .subscribe(
                    from,
                    SubscribeOptions {
                        criteria: vec![Criterion::tag("orderId", "hot")],
                        progress_interval_ms: 5,
                        ..SubscribeOptions::default()
                    },
                )
                .await
                .unwrap();

            // Every seventh event is the stream's; commits are single events,
            // so the matches are spread over many chunks.
            let mut hot = Vec::new();
            for n in 1..=140u32 {
                let order = if n % 7 == 0 { "hot" } else { "cold" };
                append_ok(&ctx, &[event("ItemAdded", order, n)], None).await;
                if n % 7 == 0 {
                    hot.push(from + n as u64 - 1);
                }
                if n % 20 == 0 {
                    tokio::time::sleep(Duration::from_millis(15)).await;
                }
            }
            let head = ctx.head().await.unwrap();

            let mut got = Vec::new();
            let mut progress = 0;
            let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
            while got.len() < hot.len() || progress < head {
                match tokio::time::timeout_at(deadline, sub.next())
                    .await
                    .expect("the stream stalled")
                    .unwrap()
                {
                    Some(Delivery::Events(batch)) => {
                        got.extend(batch.iter().map(|e| e.position));
                        // Delivered events tell the client as much as a Progress.
                        progress = progress.max(*got.last().unwrap() + 1);
                    }
                    Some(Delivery::Progress(p)) => {
                        let unseen = hot.iter().find(|&&h| !got.contains(&h));
                        assert!(
                            unseen.is_none_or(|&h| p <= h),
                            "progress {p} claims past the unseen event at {unseen:?}"
                        );
                        assert!(p >= progress, "progress went back");
                        progress = p;
                    }
                    None => panic!("subscription ended"),
                }
            }
            assert_eq!(got, hot);
            sub.cancel().await;
        },
    )
    .await;
}
