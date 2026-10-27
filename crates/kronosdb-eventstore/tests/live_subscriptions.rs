//! Live subscriptions against a real engine: what wakes them, and that under
//! heavy concurrent appends, rotation and catch-up each one receives exactly
//! the events it asked for, once, in order.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use kronosdb_eventstore::append::AppendRequest;
use kronosdb_eventstore::criteria::{Criterion, SourcingCondition};
use kronosdb_eventstore::event::{AppendEvent, Position, Tag};
use kronosdb_eventstore::store::{EventStoreEngine, PageBounds};
use kronosdb_eventstore::stream::EventStream;
use tokio::time::timeout;

fn event(name: &str, tags: &[(&str, &str)]) -> AppendEvent {
    AppendEvent {
        identifier: format!("id-{name}"),
        name: name.into(),
        version: "1".into(),
        timestamp: 0,
        payload: vec![b'x'; 40],
        metadata: vec![],
        tags: tags.iter().map(|(k, v)| Tag::from_str(k, v)).collect(),
    }
}

fn request(events: Vec<AppendEvent>) -> AppendRequest {
    AppendRequest {
        condition: None,
        events,
    }
}

fn crit(names: &[&str], tags: &[(&str, &str)]) -> Criterion {
    Criterion {
        names: names.iter().map(|n| n.to_string()).collect(),
        tags: tags.iter().map(|(k, v)| Tag::from_str(k, v)).collect(),
    }
}

fn cond(criteria: Vec<Criterion>) -> SourcingCondition {
    SourcingCondition { criteria }
}

fn match_all() -> SourcingCondition {
    cond(vec![crit(&[], &[])])
}

const QUIET: Duration = Duration::from_millis(150);

/// Waits for the stream to be woken, or reports that it was not.
async fn woken(stream: &mut EventStream) -> Option<u64> {
    timeout(QUIET, stream.wait_for_new_events()).await.ok()
}

/// Waits until the stream's frontier reaches `head`.
async fn frontier_reaches(stream: &EventStream, head: Position) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while stream.frontier() < head {
        assert!(
            Instant::now() < deadline,
            "frontier stuck at {:?}, head {:?}",
            stream.frontier(),
            head
        );
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
}

/// The positions in an `Events` body, as `read_records` wrote it
/// (`len u32 | flags u8 | body`, the body opening with the position).
fn positions(records: &[u8]) -> Vec<u64> {
    let mut out = Vec::new();
    let mut at = 0;
    while at < records.len() {
        let len = u32::from_le_bytes(records[at..at + 4].try_into().unwrap()) as usize;
        out.push(u64::from_le_bytes(
            records[at + 5..at + 13].try_into().unwrap(),
        ));
        at += 4 + len;
    }
    out
}

/// Where a followed stream's events came from.
#[derive(Default, Debug)]
struct Followed {
    delivered: Vec<u64>,
    /// Events taken from shared match-everything chunks.
    shared: usize,
    /// Events taken from the dispatcher's picks for a filtering stream.
    picked: usize,
    /// Events read through the index.
    read: usize,
}

/// The records of an `Events` body (`count u32 | records | marker u8`).
fn body_records(body: &[u8]) -> &[u8] {
    assert_eq!(*body.last().unwrap(), 0, "no marker");
    &body[4..body.len() - 1]
}

/// Drives a stream the way the native server does until its frontier reaches
/// `head`, returning every position it delivered and where it got them.
///
/// Whatever the stream takes from the dispatcher's chunks is checked against
/// the index: the same records, byte for byte, as reading the range it
/// advances over.
async fn follow(
    store: Arc<EventStoreEngine>,
    from: u64,
    condition: SourcingCondition,
    exclude: Vec<String>,
    batch_limit: usize,
    head: Arc<AtomicU64>,
    done: Arc<AtomicBool>,
) -> Followed {
    let stream = store.subscribe(Position(from), condition.clone(), &exclude);
    follow_stream(
        store,
        stream,
        condition,
        exclude,
        batch_limit,
        Duration::ZERO,
        head,
        done,
    )
    .await
}

/// `follow` for a stream that already exists; `pause` makes a slow consumer
/// out of it by sleeping after every step.
#[allow(clippy::too_many_arguments)]
async fn follow_stream(
    store: Arc<EventStoreEngine>,
    mut stream: EventStream,
    condition: SourcingCondition,
    exclude: Vec<String>,
    batch_limit: usize,
    pause: Duration,
    head: Arc<AtomicU64>,
    done: Arc<AtomicBool>,
) -> Followed {
    let mut out = Followed::default();
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        assert!(Instant::now() < deadline, "a stream never caught up");
        if !pause.is_zero() {
            tokio::time::sleep(pause).await;
        }
        let bound = match timeout(Duration::from_millis(20), stream.wait_for_new_events()).await {
            Ok(bound) => bound,
            Err(_) => {
                if done.load(Ordering::Acquire)
                    && stream.frontier().0 >= head.load(Ordering::Acquire)
                {
                    return out;
                }
                continue;
            }
        };
        let bound = Position(bound);
        assert!(bound > stream.cursor, "woken with nothing to read");
        // At the live tail a stream that wants everything takes the shared
        // batch, as the server does.
        if let Some(chunk) = stream.live_batch(batch_limit) {
            assert_eq!(chunk.from, stream.cursor.0);
            assert!(chunk.to > chunk.from);
            let records = &chunk.body[4..chunk.body.len() - 1];
            let got = positions(records);
            assert_eq!(chunk.body[0..4], chunk.count.to_le_bytes(), "count prefix");
            assert_eq!(chunk.body[chunk.body.len() - 1], 0, "no marker");
            assert_eq!(got.len() as u32, chunk.count);
            assert!(got.iter().all(|p| (chunk.from..chunk.to).contains(p)));
            out.shared += got.len();
            out.delivered.extend(got);
            stream.advance_cursor(Position(chunk.to));
            continue;
        }
        // A filtering stream takes its own matches out of the same chunk.
        if let Some(pick) = stream.live_picks(batch_limit) {
            assert!(pick.next > stream.cursor.0, "a pick must move the cursor");
            let mut expected = Vec::new();
            store
                .read_records(
                    &condition,
                    &PageBounds {
                        exclude_names: &exclude,
                        ..PageBounds::new(stream.cursor, Position(pick.next))
                    },
                    &mut expected,
                )
                .unwrap();
            let got = pick.body.as_deref().map(body_records).unwrap_or(&[]);
            assert_eq!(
                got,
                &expected[..],
                "pick over [{}, {}) differs from the index",
                stream.cursor.0,
                pick.next
            );
            if let Some(body) = &pick.body {
                assert_eq!(body[0..4], pick.count.to_le_bytes(), "count prefix");
                assert!(pick.count as usize <= batch_limit, "more than the credit");
            }
            let got = positions(got);
            assert_eq!(got.len() as u32, pick.count);
            out.picked += got.len();
            out.delivered.extend(got);
            stream.advance_cursor(Position(pick.next));
            continue;
        }
        if !store.has_matches_between(stream.cursor, bound, &condition) {
            stream.advance_cursor(bound);
            continue;
        }
        let mut records = Vec::new();
        // Small pages: a stream behind the head reads many of them.
        let page = store
            .read_records(
                &condition,
                &PageBounds {
                    limit: 7,
                    exclude_names: &exclude,
                    ..PageBounds::new(stream.cursor, bound)
                },
                &mut records,
            )
            .unwrap();
        let got = positions(&records);
        out.read += got.len();
        out.delivered.extend(got);
        stream.advance_cursor(match (page.full, page.scanned_to) {
            (true, Some(last)) => Position(last.0 + 1),
            _ => bound,
        });
        tokio::task::yield_now().await;
    }
}

#[tokio::test]
async fn a_stream_is_woken_only_by_events_that_match_it() {
    let dir = tempfile::tempdir().unwrap();
    let store = EventStoreEngine::create(dir.path()).unwrap();
    let mut stream = store.subscribe(store.head(), cond(vec![crit(&[], &[("order", "7")])]), &[]);
    assert_eq!(woken(&mut stream).await, None, "nothing yet");

    for n in 0..20 {
        let order = format!("other-{n}");
        store
            .append(request(vec![event("E", &[("order", &order)])]))
            .unwrap();
    }
    assert_eq!(
        woken(&mut stream).await,
        None,
        "twenty commits that hold nothing for it must not wake it"
    );
    frontier_reaches(&stream, store.head()).await;

    let r = store
        .append(request(vec![event("E", &[("order", "7")])]))
        .unwrap();
    let bound = woken(&mut stream).await.expect("a matching event wakes it");
    assert_eq!(bound, r.first_position.0 + 1);
    // The wake is not consumed twice.
    stream.advance_cursor(Position(bound));
    assert_eq!(woken(&mut stream).await, None);
}

#[tokio::test]
async fn system_events_and_excluded_types_wake_nobody() {
    let dir = tempfile::tempdir().unwrap();
    let store = EventStoreEngine::create(dir.path()).unwrap();
    let mut all = store.subscribe(store.head(), match_all(), &["Noise".to_string()]);
    let mut by_name = store.subscribe(
        store.head(),
        cond(vec![crit(&["Noise", "Signal"], &[])]),
        &["Noise".to_string()],
    );
    assert_eq!(woken(&mut all).await, None);
    assert_eq!(woken(&mut by_name).await, None);

    store
        .append_system(request(vec![event("$schedule.created", &[])]))
        .unwrap();
    store
        .append(request(vec![event("Noise", &[("k", "v")])]))
        .unwrap();
    assert_eq!(woken(&mut all).await, None, "hidden and excluded events");
    assert_eq!(woken(&mut by_name).await, None);
    frontier_reaches(&all, store.head()).await;
    frontier_reaches(&by_name, store.head()).await;

    let r = store
        .append(request(vec![event("Signal", &[("k", "v")])]))
        .unwrap();
    assert_eq!(woken(&mut all).await, Some(r.first_position.0 + 1));
    assert_eq!(woken(&mut by_name).await, Some(r.first_position.0 + 1));
}

#[tokio::test]
async fn the_frontier_holds_while_a_match_is_unread() {
    let dir = tempfile::tempdir().unwrap();
    let store = EventStoreEngine::create(dir.path()).unwrap();
    let mut stream = store.subscribe(Position(0), cond(vec![crit(&["Hit"], &[])]), &[]);
    store.append(request(vec![event("Miss", &[])])).unwrap();
    store.append(request(vec![event("Hit", &[])])).unwrap();
    store.append(request(vec![event("Miss", &[])])).unwrap();
    // The hit has not been read: the frontier may not claim anything past it.
    tokio::time::sleep(QUIET).await;
    assert_eq!(stream.frontier(), Position(0));

    // The bound is the end of the dispatch chunk that held the hit: past the
    // hit at position 1, at most the head.
    let bound = woken(&mut stream).await.unwrap();
    assert!((2..=3).contains(&bound), "bound {bound}");
    assert_eq!(stream.frontier(), Position(0), "still reading");
    stream.advance_cursor(Position(bound));
    frontier_reaches(&stream, Position(3)).await;
}

#[tokio::test]
async fn a_stream_starting_ahead_of_the_head_waits_for_its_position() {
    let dir = tempfile::tempdir().unwrap();
    let store = EventStoreEngine::create(dir.path()).unwrap();
    let mut stream = store.subscribe(Position(5), match_all(), &[]);
    for _ in 0..5 {
        store.append(request(vec![event("E", &[])])).unwrap();
    }
    assert_eq!(woken(&mut stream).await, None);
    store.append(request(vec![event("E", &[])])).unwrap();
    assert_eq!(woken(&mut stream).await, Some(6));
}

#[tokio::test]
async fn dropping_the_future_loses_no_wake() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(EventStoreEngine::create(dir.path()).unwrap());
    let mut stream = store.subscribe(store.head(), match_all(), &[]);
    let appender = {
        let store = store.clone();
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(20));
            store.append(request(vec![event("E", &[])])).unwrap();
        })
    };
    // Cancelled over and over, as a select loop does when another branch wins.
    let deadline = Instant::now() + Duration::from_secs(5);
    let bound = loop {
        assert!(Instant::now() < deadline, "the wake was lost");
        if let Ok(bound) = timeout(Duration::from_micros(200), stream.wait_for_new_events()).await {
            break bound;
        }
    };
    assert_eq!(bound, 1);
    appender.join().unwrap();
}

#[tokio::test]
async fn a_closed_store_releases_its_streams() {
    let dir = tempfile::tempdir().unwrap();
    let store = EventStoreEngine::create(dir.path()).unwrap();
    let mut stream = store.subscribe(store.head(), match_all(), &[]);
    assert_eq!(woken(&mut stream).await, None);
    drop(store);
    let bound = timeout(Duration::from_secs(2), stream.wait_for_new_events())
        .await
        .expect("closing the store wakes its streams");
    assert!(bound <= stream.cursor.0, "nothing is left to read");
}

#[tokio::test]
async fn dropped_streams_leave_no_trace_and_new_ones_still_work() {
    let dir = tempfile::tempdir().unwrap();
    let store = EventStoreEngine::create(dir.path()).unwrap();
    for _ in 0..50 {
        let stream = store.subscribe(Position(0), cond(vec![crit(&["E"], &[("k", "v")])]), &[]);
        drop(stream);
    }
    store
        .append(request(vec![event("E", &[("k", "v")])]))
        .unwrap();
    let mut stream = store.subscribe(Position(1), match_all(), &[]);
    store.append(request(vec![event("E", &[])])).unwrap();
    assert_eq!(woken(&mut stream).await, Some(2));
}

#[tokio::test]
async fn a_shared_batch_holds_exactly_the_visible_events_of_its_range() {
    let dir = tempfile::tempdir().unwrap();
    let store = EventStoreEngine::create(dir.path()).unwrap();
    let mut all = store.subscribe(store.head(), match_all(), &[]);
    let mut filtered = store.subscribe(store.head(), cond(vec![crit(&["A"], &[])]), &[]);
    assert_eq!(woken(&mut all).await, None);

    store
        .append(request(vec![event("A", &[("k", "1")])]))
        .unwrap();
    store
        .append_system(request(vec![event("$tick", &[])]))
        .unwrap();
    store
        .append(request(vec![event("B", &[("k", "2")])]))
        .unwrap();

    // Read through the dispatcher's batches until position 3 is covered.
    let mut seen = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(5);
    while all.cursor.0 < 3 {
        assert!(
            Instant::now() < deadline,
            "the batches never covered the log"
        );
        if timeout(Duration::from_millis(50), all.wait_for_new_events())
            .await
            .is_err()
        {
            continue;
        }
        let Some(batch) = all.live_batch(1024) else {
            // Not at a batch boundary yet (the first wake owes a read).
            let mut out = Vec::new();
            let bound = Position(3.min(store.head().0));
            store
                .read_records(&match_all(), &PageBounds::new(all.cursor, bound), &mut out)
                .unwrap();
            seen.extend(positions(&out));
            all.advance_cursor(bound);
            continue;
        };
        seen.extend(positions(&batch.body[4..batch.body.len() - 1]));
        all.advance_cursor(Position(batch.to));
    }
    // The system event at position 1 is in no batch and in no read.
    assert_eq!(seen, vec![0, 2]);

    // A filtering stream never gets a shared batch.
    let _ = woken(&mut filtered).await;
    assert!(filtered.live_batch(1024).is_none());
}

/// The heart of it: appenders, rotation, system events, and streams that
/// start behind the head, at it, and mid-flight all end up with exactly the
/// matching events.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_stream_receives_exactly_its_events_under_concurrent_appends() {
    // A hang fails fast instead of holding a test run for hours.
    timeout(Duration::from_secs(180), concurrent_appends_scenario())
        .await
        .expect("the scenario did not finish in 180s");
}

async fn concurrent_appends_scenario() {
    const WORKERS: usize = 6;
    const PER_WORKER: usize = 1_500;
    let dir = tempfile::tempdir().unwrap();
    // Tiny segments: the log rotates many times while streams read it.
    let store = Arc::new(EventStoreEngine::create_with_options(dir.path(), 32 * 1024).unwrap());
    let names = ["N0", "N1", "N2"];

    // A backlog every early stream has to catch up through.
    for i in 0..300 {
        store
            .append(request(vec![event(
                names[i % 3],
                &[("k", &format!("{}", i % 3)), ("w", "pre")],
            )]))
            .unwrap();
    }

    type Case = (&'static str, u64, SourcingCondition, Vec<String>);
    let cases: Vec<Case> = vec![
        ("all from the start", 0, match_all(), vec![]),
        (
            "all from the middle of the backlog",
            150,
            match_all(),
            vec![],
        ),
        ("all excluding N1", 0, match_all(), vec!["N1".into()]),
        ("tag k=1", 0, cond(vec![crit(&[], &[("k", "1")])]), vec![]),
        (
            "two tags",
            0,
            cond(vec![crit(&[], &[("k", "2"), ("w", "w3")])]),
            vec![],
        ),
        (
            "name or tag",
            0,
            cond(vec![crit(&["N2"], &[]), crit(&["N0"], &[("w", "w1")])]),
            vec![],
        ),
        (
            "name and tag, excluding another name",
            150,
            cond(vec![crit(&["N0", "N1"], &[("k", "0")])]),
            vec!["N1".into()],
        ),
        (
            "tag that nothing carries",
            0,
            cond(vec![crit(&[], &[("k", "nope")])]),
            vec![],
        ),
    ];

    let head = Arc::new(AtomicU64::new(0));
    let done = Arc::new(AtomicBool::new(false));
    let mut followers = Vec::new();
    for (label, from, condition, exclude) in cases.clone() {
        // Half of them could take a whole shared batch; the other half only
        // ones of at most three events, so they mix batches and reads.
        for batch_limit in [1024, 3] {
            let (store, head, done) = (store.clone(), head.clone(), done.clone());
            followers.push((
                label,
                from,
                condition.clone(),
                exclude.clone(),
                tokio::spawn(follow(
                    store,
                    from,
                    condition.clone(),
                    exclude.clone(),
                    batch_limit,
                    head,
                    done,
                )),
            ));
        }
    }

    let appenders: Vec<_> = (0..WORKERS)
        .map(|w| {
            let store = store.clone();
            std::thread::spawn(move || {
                let worker = format!("w{w}");
                for i in 0..PER_WORKER {
                    let batch = 1 + i % 3;
                    let events = (0..batch)
                        .map(|b| {
                            let n = i + b;
                            event(
                                names[n % 3],
                                &[("k", &format!("{}", n % 3)), ("w", &worker)],
                            )
                        })
                        .collect();
                    store.append(request(events)).unwrap();
                    if w == 0 && i % 97 == 0 {
                        store
                            .append_system(request(vec![event("$tick", &[("$sys-tag", "1")])]))
                            .unwrap();
                    }
                }
            })
        })
        .collect();

    // Streams that register while the appends are in flight: their own
    // backlog and the live events race.
    let mut late = Vec::new();
    for i in 0..4u64 {
        tokio::time::sleep(Duration::from_millis(15)).await;
        let from = store.head().0.saturating_sub(i * 40);
        let condition = cond(vec![crit(&[], &[("k", &format!("{}", i % 3))])]);
        late.push((
            from,
            condition.clone(),
            tokio::spawn(follow(
                store.clone(),
                from,
                condition,
                vec![],
                1024,
                head.clone(),
                done.clone(),
            )),
        ));
    }

    // Subscriptions coming and going must not disturb the others.
    let churn = {
        let store = store.clone();
        let done = done.clone();
        tokio::spawn(async move {
            while !done.load(Ordering::Acquire) {
                let s = store.subscribe(Position(0), cond(vec![crit(&["N0"], &[])]), &[]);
                tokio::time::sleep(Duration::from_millis(1)).await;
                drop(s);
            }
        })
    };

    for appender in appenders {
        tokio::task::spawn_blocking(move || appender.join().unwrap())
            .await
            .unwrap();
    }
    head.store(store.head().0, Ordering::Release);
    done.store(true, Ordering::Release);
    churn.await.unwrap();

    let check = |label: &str,
                 from: u64,
                 condition: &SourcingCondition,
                 exclude: &[String],
                 got: Vec<u64>| {
        let expected: Vec<u64> = store
            .source(Position(from), condition)
            .unwrap()
            .into_iter()
            .filter(|e| !exclude.contains(&e.name))
            .map(|e| e.position.0)
            .collect();
        let unique: BTreeSet<u64> = got.iter().copied().collect();
        assert_eq!(unique.len(), got.len(), "{label}: an event arrived twice");
        assert!(
            got.windows(2).all(|w| w[0] < w[1]),
            "{label}: events out of order"
        );
        assert_eq!(
            got.len(),
            expected.len(),
            "{label}: delivered {} of {} events",
            got.len(),
            expected.len()
        );
        assert_eq!(got, expected, "{label}");
    };
    let (mut picked, mut shared) = (0, 0);
    for (label, from, condition, exclude, task) in followers {
        let got = task.await.unwrap();
        picked += got.picked;
        shared += got.shared;
        check(label, from, &condition, &exclude, got.delivered);
    }
    for (from, condition, task) in late {
        let got = task.await.unwrap();
        picked += got.picked;
        check("late subscriber", from, &condition, &[], got.delivered);
    }
    // The fast paths were really taken, not just the index.
    assert!(picked > 0, "no filtering stream took events from a chunk");
    assert!(shared > 0, "no stream took a shared chunk");
    // Sanity: the workload really did rotate and really was concurrent.
    assert!(store.head().0 > (WORKERS * PER_WORKER) as u64);
    assert!(std::fs::read_dir(dir.path()).unwrap().count() > 10);
}

/// A one-event append.
fn put(store: &EventStoreEngine, name: &str, tags: &[(&str, &str)]) -> u64 {
    store
        .append(request(vec![event(name, tags)]))
        .unwrap()
        .first_position
        .0
}

fn tagged(key: &str, value: &str) -> SourcingCondition {
    cond(vec![crit(&[], &[(key, value)])])
}

/// What the index returns for the same range: the reference for a pick.
fn read_range(
    store: &EventStoreEngine,
    condition: &SourcingCondition,
    exclude: &[String],
    from: u64,
    to: u64,
) -> Vec<u8> {
    let mut out = Vec::new();
    store
        .read_records(
            condition,
            &PageBounds {
                exclude_names: exclude,
                ..PageBounds::new(Position(from), Position(to))
            },
            &mut out,
        )
        .unwrap();
    out
}

#[tokio::test]
async fn a_filtering_stream_takes_its_matches_from_the_dispatchers_chunk() {
    timeout(Duration::from_secs(30), async {
        let dir = tempfile::tempdir().unwrap();
        let store = EventStoreEngine::create(dir.path()).unwrap();
        let condition = tagged("k", "1");
        let mut stream = store.subscribe(Position(0), condition.clone(), &[]);

        // One commit, one chunk: positions 0, 2 and 4 are the stream's.
        let ks = ["1", "2", "1", "3", "1", "2"];
        store
            .append(request(
                ks.iter().map(|k| event("E", &[("k", k)])).collect(),
            ))
            .unwrap();
        assert_eq!(woken(&mut stream).await, Some(6));

        let pick = stream.live_picks(10).expect("the chunk is in the ring");
        assert_eq!((pick.count, pick.next), (3, 6));
        let body = pick.body.unwrap();
        assert_eq!(body[0..4], 3u32.to_le_bytes());
        assert_eq!(positions(body_records(&body)), vec![0, 2, 4]);
        assert_eq!(
            body_records(&body),
            &read_range(&store, &condition, &[], 0, 6)[..],
            "the same bytes the index would have produced"
        );
        // Nothing more is owed until the cursor moves and something matches.
        stream.advance_cursor(Position(pick.next));
        assert!(stream.live_picks(10).is_none());

        // A chunk with nothing for this stream: no body, the cursor moves on
        // (it is another stream's commit that put the chunk there).
        let mut other = store.subscribe(Position(6), tagged("k", "2"), &[]);
        put(&store, "E", &[("k", "2")]);
        assert_eq!(woken(&mut other).await, Some(7));
        assert_eq!(woken(&mut stream).await, None, "not a match for it");
        put(&store, "E", &[("k", "1")]);
        assert_eq!(woken(&mut stream).await, Some(8));
        // The chunk at the cursor holds the other stream's event: nothing to
        // send, the cursor moves past it.
        let pick = stream.live_picks(10).unwrap();
        assert_eq!((pick.count, pick.next), (0, 7));
        assert!(pick.body.is_none());
        stream.advance_cursor(Position(pick.next));
        let pick = stream.live_picks(10).unwrap();
        assert_eq!((pick.count, pick.next), (1, 8));
        assert_eq!(positions(body_records(&pick.body.unwrap())), vec![7]);
    })
    .await
    .expect("did not finish in 30s");
}

#[tokio::test]
async fn a_pick_stops_at_the_credit_and_resumes_inside_the_chunk() {
    timeout(Duration::from_secs(30), async {
        let dir = tempfile::tempdir().unwrap();
        let store = EventStoreEngine::create(dir.path()).unwrap();
        let condition = tagged("k", "1");
        let mut stream = store.subscribe(Position(0), condition.clone(), &[]);
        let ks = ["1", "2", "1", "1", "3", "1", "2", "1", "2"];
        store
            .append(request(
                ks.iter().map(|k| event("E", &[("k", k)])).collect(),
            ))
            .unwrap();
        assert_eq!(woken(&mut stream).await, Some(9));

        // Credit for two events at a time: matches are 0, 2, 3, 5, 7.
        let mut got = Vec::new();
        let mut steps = Vec::new();
        while stream.cursor.0 < 9 {
            let from = stream.cursor.0;
            let pick = stream.live_picks(2).expect("inside the chunk");
            assert!(pick.count <= 2);
            assert!(pick.next > from);
            let body = pick.body.as_deref().map(body_records).unwrap_or(&[]);
            assert_eq!(
                body,
                &read_range(&store, &condition, &[], from, pick.next)[..]
            );
            got.extend(positions(body));
            steps.push(pick.next);
            stream.advance_cursor(Position(pick.next));
        }
        assert_eq!(got, vec![0, 2, 3, 5, 7]);
        // Two, two, then the last one and the rest of the chunk.
        assert_eq!(steps, vec![3, 6, 9]);
    })
    .await
    .expect("did not finish in 30s");
}

#[tokio::test]
async fn exclusions_and_system_events_never_reach_a_pick() {
    timeout(Duration::from_secs(30), async {
        let dir = tempfile::tempdir().unwrap();
        let store = EventStoreEngine::create(dir.path()).unwrap();
        let exclude = vec!["Noise".to_string()];
        // Match-all-but-Noise and a tag filter that also excludes it.
        let mut all = store.subscribe(Position(0), match_all(), &exclude);
        let tag = tagged("k", "1");
        let mut by_tag = store.subscribe(Position(0), tag.clone(), &exclude);

        store
            .append(request(vec![event("Signal", &[("k", "1")])]))
            .unwrap();
        store
            .append_system(request(vec![event("$tick", &[("$sys", "1")])]))
            .unwrap();
        store
            .append(request(vec![event("Noise", &[("k", "1")])]))
            .unwrap();
        // A visible event carrying a system tag: delivered without the tag.
        store
            .append_system(request(vec![event(
                "Reminder",
                &[("k", "1"), ("$schedule", "tok")],
            )]))
            .unwrap();
        store
            .append(request(vec![event("Signal", &[("k", "2")])]))
            .unwrap();
        let head = store.head();
        assert_eq!(head, Position(5));

        for (stream, condition) in [(&mut all, match_all()), (&mut by_tag, tag)] {
            let mut got = Vec::new();
            let deadline = Instant::now() + Duration::from_secs(5);
            while stream.frontier() < head {
                assert!(Instant::now() < deadline, "the stream stalled");
                if woken(stream).await.is_none() {
                    continue;
                }
                let from = stream.cursor.0;
                let (body, next) = match stream.live_picks(100) {
                    Some(pick) => (pick.body.map(|b| body_records(&b).to_vec()), pick.next),
                    None => {
                        let to = head.0.min(stream.cursor.0 + 100);
                        (Some(read_range(&store, &condition, &exclude, from, to)), to)
                    }
                };
                if let Some(body) = body {
                    assert_eq!(
                        body,
                        read_range(&store, &condition, &exclude, from, next),
                        "bytes differ from the index"
                    );
                    assert!(
                        !body.windows(9).any(|w| w == b"$schedule"),
                        "a system tag reached a client"
                    );
                    got.extend(positions(&body));
                }
                stream.advance_cursor(Position(next));
            }
            assert!(!got.contains(&1), "a system event was delivered");
            assert!(!got.contains(&2), "an excluded event was delivered");
        }
    })
    .await
    .expect("did not finish in 30s");
}

#[tokio::test]
async fn a_stream_never_takes_picks_from_before_it_existed() {
    timeout(Duration::from_secs(30), async {
        let dir = tempfile::tempdir().unwrap();
        let store = EventStoreEngine::create(dir.path()).unwrap();
        // Someone keeps the dispatcher busy, so the chunks exist.
        let mut first = store.subscribe(Position(0), tagged("k", "1"), &[]);
        let hits = [
            put(&store, "E", &[("k", "1")]),
            put(&store, "E", &[("k", "2")]),
            put(&store, "E", &[("k", "1")]),
        ];
        assert_eq!(hits, [0, 1, 2]);
        // Its matches are read (as a server would), so its frontier can move.
        let deadline = Instant::now() + Duration::from_secs(5);
        while first.frontier() < store.head() {
            assert!(Instant::now() < deadline, "the first stream stalled");
            if let Some(bound) = woken(&mut first).await {
                first.advance_cursor(Position(bound));
            }
        }

        // A newcomer asking for the past: the chunks that hold positions 0..3
        // were matched without it, so they say nothing about it.
        let mut late = store.subscribe(Position(0), tagged("k", "1"), &[]);
        let bound = woken(&mut late).await.expect("it owes a read of its past");
        assert_eq!(bound, 3);
        assert!(
            late.live_picks(10).is_none(),
            "no pick from before it existed"
        );

        // ... which the index serves; after it, the live tail is picks.
        let past = read_range(&store, &tagged("k", "1"), &[], 0, 3);
        assert_eq!(positions(&past), vec![0, 2]);
        late.advance_cursor(Position(3));
        let at = put(&store, "E", &[("k", "1")]);
        assert_eq!(woken(&mut late).await, Some(at + 1));
        let pick = late.live_picks(10).expect("a chunk made after it existed");
        assert_eq!(positions(body_records(&pick.body.unwrap())), vec![at]);
    })
    .await
    .expect("did not finish in 30s");
}

#[tokio::test]
async fn a_match_everything_stream_never_takes_a_chunk_that_only_holds_picks() {
    timeout(Duration::from_secs(30), async {
        let dir = tempfile::tempdir().unwrap();
        let store = EventStoreEngine::create(dir.path()).unwrap();
        // Only a filtering stream is subscribed, so the dispatcher keeps just
        // the events that filter picked.
        let mut filtering = store.subscribe(Position(0), tagged("k", "1"), &[]);
        store
            .append(request(
                ["1", "2", "2", "1", "2"]
                    .iter()
                    .map(|k| event("E", &[("k", k)]))
                    .collect(),
            ))
            .unwrap();
        assert_eq!(woken(&mut filtering).await, Some(5));
        let pick = filtering.live_picks(10).unwrap();
        assert_eq!(positions(body_records(&pick.body.unwrap())), vec![0, 3]);

        // A stream that wants everything and starts where that chunk starts
        // would send two events of five if it trusted the chunk.
        let mut all = store.subscribe(Position(0), match_all(), &[]);
        assert_eq!(woken(&mut all).await, Some(5));
        assert!(
            all.live_batch(1024).is_none(),
            "a chunk that only holds some events is not a shared batch"
        );
        assert!(all.live_picks(1024).is_none(), "and it does not pick");
        let read = read_range(&store, &match_all(), &[], 0, 5);
        assert_eq!(positions(&read), vec![0, 1, 2, 3, 4]);
    })
    .await
    .expect("did not finish in 30s");
}

#[tokio::test]
async fn a_stream_behind_the_ring_reads_the_index_and_loses_nothing() {
    timeout(Duration::from_secs(60), async {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(EventStoreEngine::create(dir.path()).unwrap());
        // Room for two chunks: a stream that does not read falls behind it.
        store.limit_live_ring(2, usize::MAX);
        let condition = tagged("k", "1");
        let mut stream = store.subscribe(Position(0), condition.clone(), &[]);
        for i in 0..60u64 {
            put(
                &store,
                "E",
                &[("k", if i % 3 == 1 || i == 59 { "1" } else { "2" })],
            );
            // The dispatcher makes a chunk of every commit it is given time for.
            tokio::time::sleep(Duration::from_millis(3)).await;
        }
        frontier_reaches(&stream, Position(0)).await;
        assert_eq!(woken(&mut stream).await, Some(60));
        assert!(
            stream.live_picks(1024).is_none(),
            "the chunk at the cursor has left the ring"
        );
        // Near the head the chunks are still there.
        stream.advance_cursor(Position(59));
        let pick = stream.live_picks(1024).expect("the newest chunk is kept");
        assert_eq!(positions(body_records(&pick.body.unwrap())), vec![59]);

        // And a stream that was left behind catches up exactly, one way or
        // the other, when it is driven the usual way.
        let slow = store.subscribe(Position(0), condition.clone(), &[]);
        let head = Arc::new(AtomicU64::new(0));
        let done = Arc::new(AtomicBool::new(false));
        let follower = tokio::spawn(follow_stream(
            store.clone(),
            slow,
            condition.clone(),
            vec![],
            4,
            Duration::from_millis(2),
            head.clone(),
            done.clone(),
        ));
        for i in 0..200u64 {
            put(&store, "E", &[("k", if i % 4 == 0 { "1" } else { "2" })]);
        }
        head.store(store.head().0, Ordering::Release);
        done.store(true, Ordering::Release);
        let got = follower.await.unwrap();
        let expected: Vec<u64> = store
            .source(Position(0), &condition)
            .unwrap()
            .into_iter()
            .map(|e| e.position.0)
            .collect();
        assert_eq!(got.delivered, expected);
        assert!(got.read > 0, "the ring was too small to serve it all");
    })
    .await
    .expect("did not finish in 60s");
}

/// A small deterministic generator for the randomized scenario.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

fn random_condition(rng: &mut Rng) -> (SourcingCondition, Vec<String>) {
    const NAMES: [&str; 5] = ["N0", "N1", "N2", "N3", "N4"];
    let keys = ["a", "b", "c"];
    let mut criteria = Vec::new();
    for _ in 0..rng.below(4) {
        let names: Vec<&str> = (0..rng.below(3))
            .map(|_| NAMES[rng.below(5) as usize])
            .collect();
        let tags: Vec<(String, String)> = (0..rng.below(3))
            .map(|_| {
                (
                    keys[rng.below(3) as usize].to_string(),
                    // Values past the domain never match anything.
                    format!("{}", rng.below(7)),
                )
            })
            .collect();
        let tags: Vec<(&str, &str)> = tags.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
        criteria.push(crit(&names, &tags));
    }
    let exclude = (0..rng.below(3))
        .map(|_| NAMES[rng.below(5) as usize].to_string())
        .collect();
    (cond(criteria), exclude)
}

/// Random conditions, random events, rotation, system events, slow
/// consumers, a tiny ring and subscriptions coming and going: every stream
/// ends up with exactly what the index says it should.
async fn random_filters_scenario(seed: u64) {
    const WORKERS: u64 = 4;
    const PER_WORKER: u64 = 700;
    const NAMES: [&str; 5] = ["N0", "N1", "N2", "N3", "N4"];
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(EventStoreEngine::create_with_options(dir.path(), 48 * 1024).unwrap());
    store.limit_live_ring(6, 256 * 1024);
    let mut rng = Rng(seed);

    let head = Arc::new(AtomicU64::new(0));
    let done = Arc::new(AtomicBool::new(false));
    let mut followers = Vec::new();
    let mut start = |rng: &mut Rng, from: u64| {
        let (condition, exclude) = random_condition(rng);
        let batch_limit = [1usize, 3, 1024][rng.below(3) as usize];
        let pause = Duration::from_millis([0u64, 0, 0, 1, 4][rng.below(5) as usize]);
        let stream = store.subscribe(Position(from), condition.clone(), &exclude);
        followers.push((
            from,
            condition.clone(),
            exclude.clone(),
            tokio::spawn(follow_stream(
                store.clone(),
                stream,
                condition,
                exclude,
                batch_limit,
                pause,
                head.clone(),
                done.clone(),
            )),
        ));
    };
    for _ in 0..24 {
        start(&mut rng, 0);
    }

    let appenders: Vec<_> = (0..WORKERS)
        .map(|w| {
            let store = store.clone();
            let mut rng = Rng(seed ^ (w + 1).wrapping_mul(0x9e3779b97f4a7c15));
            std::thread::spawn(move || {
                for i in 0..PER_WORKER {
                    let events = (0..1 + rng.below(4))
                        .map(|_| {
                            let mut tags: Vec<(String, String)> = Vec::new();
                            for key in ["a", "b", "c"] {
                                if rng.below(3) != 0 {
                                    tags.push((key.to_string(), format!("{}", rng.below(6))));
                                }
                            }
                            let tags: Vec<(&str, &str)> =
                                tags.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
                            event(NAMES[rng.below(5) as usize], &tags)
                        })
                        .collect();
                    store.append(request(events)).unwrap();
                    if w == 0 && i % 53 == 0 {
                        store
                            .append_system(request(vec![event("$tick", &[("$sys", "1")])]))
                            .unwrap();
                        // Visible, carrying a system tag.
                        store
                            .append_system(request(vec![event(
                                "N2",
                                &[("a", "1"), ("$schedule", "t")],
                            )]))
                            .unwrap();
                    }
                    if i % 40 == 0 {
                        std::thread::sleep(Duration::from_millis(1));
                    }
                }
            })
        })
        .collect();

    // Streams that register while the appends are in flight, some behind the
    // head, some at it.
    for i in 0..10u64 {
        tokio::time::sleep(Duration::from_millis(12)).await;
        let behind = rng.below(3) * 60;
        start(&mut rng, store.head().0.saturating_sub(behind * (i % 2)));
    }
    // Churn: subscriptions that come and go reuse slots the live ones' picks
    // are filed under; they must never disturb them.
    let churn = {
        let store = store.clone();
        let done = done.clone();
        tokio::spawn(async move {
            let mut rng = Rng(seed ^ 0xabcdef);
            while !done.load(Ordering::Acquire) {
                let (condition, exclude) = random_condition(&mut rng);
                let mut s = store.subscribe(Position(store.head().0), condition, &exclude);
                let _ = timeout(Duration::from_millis(2), s.wait_for_new_events()).await;
                drop(s);
            }
        })
    };

    for appender in appenders {
        tokio::task::spawn_blocking(move || appender.join().unwrap())
            .await
            .unwrap();
    }
    head.store(store.head().0, Ordering::Release);
    done.store(true, Ordering::Release);
    churn.await.unwrap();

    let (mut picked, mut read, mut total) = (0, 0, 0);
    for (n, (from, condition, exclude, task)) in followers.into_iter().enumerate() {
        let got = task.await.unwrap();
        let expected: Vec<u64> = store
            .source(Position(from), &condition)
            .unwrap()
            .into_iter()
            .filter(|e| !exclude.contains(&e.name))
            .map(|e| e.position.0)
            .collect();
        assert_eq!(
            got.delivered, expected,
            "seed {seed}, stream {n} (from {from}, {condition:?}, excluding {exclude:?})"
        );
        picked += got.picked;
        read += got.read;
        total += expected.len();
    }
    assert!(total > 0, "the scenario matched nothing at all");
    assert!(
        picked > 0,
        "no stream took a pick (read {read} through the index)"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn random_filters_get_exactly_what_the_index_says() {
    for seed in [0x5eed_0001, 0x5eed_0002, 0x5eed_0003] {
        timeout(Duration::from_secs(120), random_filters_scenario(seed))
            .await
            .unwrap_or_else(|_| panic!("seed {seed:#x}: the scenario did not finish in 120s"));
    }
}
