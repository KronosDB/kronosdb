//! Wire-level proof of the snapshot RPCs (ADR-0005).
//!
//! Drives the production server over the native protocol and verifies the
//! client-observable contract: the append/get roundtrip, the fused read's
//! frame ordering and consistency marker, invisibility on the ordinary read
//! path, the request rejections, and — the reason snapshots ride the log at
//! all — that an acknowledged snapshot survives SIGKILL and restart.

mod crash_harness;
use crash_harness::*;

use std::time::Duration;

use kronosdb_client::Snapshot;

const READY_TIMEOUT: Duration = Duration::from_secs(10);

fn single_node_config(data_dir: std::path::PathBuf) -> SpawnConfig {
    SpawnConfig {
        data_dir,
        listen_port: free_port(),
        admin_port: free_port(),
        node_id: 1,
        peers: vec![],
        group_commit_ms: Some(2),
        segment_size: None,
        backup: None,
        max_snapshot_size: None,
    }
}

async fn connect_to(srv: &ServerHandle) -> Context {
    connect(srv.listen)
        .await
        .expect("connect event store client")
        .default_context()
}

fn tagged_event(name: &str, tag_key: &str, tag_value: &str, payload: &[u8]) -> NewEvent {
    event(
        format!("id-{name}-{tag_value}"),
        0,
        name,
        "1",
        payload.to_vec(),
        vec![(tag_key.as_bytes().to_vec(), tag_value.as_bytes().to_vec())],
    )
}

/// Appends without a condition and returns the consistency marker.
async fn append_events(context: &Context, events: Vec<NewEvent>) -> (u64, u32, u64) {
    match context.append(&events, None).await.expect("append") {
        Appended::Ok {
            first_position,
            count,
            marker,
        } => (first_position, count, marker),
        Appended::Conflict { position } => panic!("unexpected conflict at {position}"),
    }
}

/// Drains a SnapshottedSource into (snapshot, events as (position, payload), marker).
async fn drain_snapshotted_source(
    context: &Context,
    key: &[u8],
    criteria: &[Criterion],
) -> (Option<Snapshot>, Vec<(u64, Vec<u8>)>, Option<u64>) {
    let mut source = context
        .snapshotted_source(key, criteria, 0)
        .await
        .expect("snapshotted_source");
    let snapshot = source.snapshot.clone();
    let mut events = Vec::new();
    let mut marker = None;
    while let Some(batch) = source.next().await.expect("stream message") {
        assert!(marker.is_none(), "no batch may follow the marker batch");
        events.extend(batch.iter().map(|e| (e.position, e.payload.to_vec())));
        marker = batch.marker();
    }
    (snapshot, events, marker)
}

/// The full client lifecycle over the wire: fold events, store a snapshot at
/// the marker, keep appending, then rehydrate through the fused read and
/// conditionally append with the marker it returned.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fused_read_roundtrip_and_marker_contract() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let mut srv = spawn_server(&single_node_config(tmp.path().join("data"))).expect("spawn");
    wait_until_ready(srv.listen, srv.admin, READY_TIMEOUT)
        .await
        .expect("ready");
    let context = connect_to(&srv).await;

    // Fold 3 course events; the append response's marker is the fold marker.
    let (_, _, fold_marker) = append_events(
        &context,
        vec![
            tagged_event("CourseCreated", "courseId", "cs-101", b"created"),
            tagged_event("StudentEnrolled", "courseId", "cs-101", b"alice"),
            tagged_event("StudentEnrolled", "courseId", "cs-101", b"bob"),
        ],
    )
    .await;

    // Store the snapshot "as the client would": after its transaction commits.
    let key = b"course:cs-101".to_vec();
    let landed = context
        .append_snapshot(&key, b"folded:3-events", fold_marker)
        .await
        .expect("append snapshot");
    assert!(
        landed >= fold_marker,
        "the record lands at or after the state it summarizes"
    );

    // More events land after the snapshot, some for an unrelated course.
    append_events(
        &context,
        vec![
            tagged_event("StudentEnrolled", "courseId", "cs-101", b"carol"),
            tagged_event("CourseCreated", "courseId", "cs-999", b"other"),
            tagged_event("StudentEnrolled", "courseId", "cs-101", b"dave"),
        ],
    )
    .await;

    // GetSnapshot alone returns the stored state byte-exact.
    let got = context
        .get_snapshot(&key)
        .await
        .expect("get snapshot")
        .expect("snapshot must exist");
    assert_eq!(got.state.as_ref(), b"folded:3-events");
    assert_eq!(got.position, fold_marker);

    // The fused read: snapshot first, then ONLY the matching events
    // strictly after the fold position, then the marker.
    let criteria = vec![Criterion::tag("courseId", "cs-101")];
    let (snapshot, events, marker) = drain_snapshotted_source(&context, &key, &criteria).await;

    let snapshot = snapshot.expect("fused read must start with the snapshot");
    assert_eq!(snapshot.state.as_ref(), b"folded:3-events");
    assert_eq!(snapshot.position, fold_marker);

    let payloads: Vec<&[u8]> = events
        .iter()
        .map(|(_, payload)| payload.as_slice())
        .collect();
    assert_eq!(
        payloads,
        vec![b"carol".as_slice(), b"dave".as_slice()],
        "only matching events after the fold position, in order"
    );
    for (position, _) in &events {
        assert!(
            *position >= fold_marker,
            "replay resumes at the (next-exclusive) fold marker"
        );
    }

    // The marker must work as a DCB condition, exactly as Source's would.
    let marker = marker.expect("final batch carries the consistency marker");
    let conditional = context
        .append(
            &[tagged_event(
                "CourseCapacityChanged",
                "courseId",
                "cs-101",
                b"cap",
            )],
            Some(&Condition { marker, criteria }),
        )
        .await
        .expect("conditional append with the fused read's marker must succeed");
    assert!(matches!(conditional, Appended::Ok { count: 1, .. }));

    srv.kill();
}

/// Without a snapshot the fused read is exactly a plain Source: no snapshot,
/// events from the beginning, marker on the final batch — including on a
/// completely empty result.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fused_read_without_snapshot_is_plain_source() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let mut srv = spawn_server(&single_node_config(tmp.path().join("data"))).expect("spawn");
    wait_until_ready(srv.listen, srv.admin, READY_TIMEOUT)
        .await
        .expect("ready");
    let context = connect_to(&srv).await;

    // Empty store: no snapshot, one empty marker-carrying batch.
    let (snapshot, events, marker) =
        drain_snapshotted_source(&context, b"never-written", &[]).await;
    assert!(snapshot.is_none());
    assert!(events.is_empty());
    assert_eq!(marker, Some(0));

    append_events(
        &context,
        vec![
            tagged_event("CourseCreated", "courseId", "cs-101", b"created"),
            tagged_event("StudentEnrolled", "courseId", "cs-101", b"alice"),
        ],
    )
    .await;

    let (snapshot, events, marker) =
        drain_snapshotted_source(&context, b"never-written", &[]).await;
    assert!(snapshot.is_none(), "a miss produces no snapshot");
    assert_eq!(events.len(), 2, "a miss replays from the beginning");
    assert_eq!(marker, Some(2));

    srv.kill();
}

/// A superseded snapshot stays inert history; reads see only the latest.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn newer_snapshot_supersedes_older() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let mut srv = spawn_server(&single_node_config(tmp.path().join("data"))).expect("spawn");
    wait_until_ready(srv.listen, srv.admin, READY_TIMEOUT)
        .await
        .expect("ready");
    let context = connect_to(&srv).await;

    let key = b"course:cs-101".to_vec();
    for (state, position) in [(b"v1".as_slice(), 1), (b"v2".as_slice(), 2)] {
        context
            .append_snapshot(&key, state, position)
            .await
            .expect("append snapshot");
    }

    let got = context
        .get_snapshot(&key)
        .await
        .expect("get snapshot")
        .expect("snapshot exists");
    assert_eq!(got.state.as_ref(), b"v2");
    assert_eq!(got.position, 2);

    srv.kill();
}

/// Snapshot records must be invisible everywhere a client reads events:
/// Source never returns them, and GetHead never counts them at the tail —
/// a drained consumer's cursor always reaches the head it is shown.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn snapshot_records_are_invisible_on_the_wire() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let mut srv = spawn_server(&single_node_config(tmp.path().join("data"))).expect("spawn");
    wait_until_ready(srv.listen, srv.admin, READY_TIMEOUT)
        .await
        .expect("ready");
    let context = connect_to(&srv).await;

    append_events(
        &context,
        vec![tagged_event(
            "CourseCreated",
            "courseId",
            "cs-101",
            b"created",
        )],
    )
    .await;
    context
        .append_snapshot(b"course:cs-101", b"state", 1)
        .await
        .expect("append snapshot");

    // GetHead hides the trailing snapshot record's position.
    assert_eq!(
        context.head().await.expect("get head"),
        1,
        "visible head excludes the snapshot record"
    );

    // Source never returns the record.
    let names: Vec<String> = read_events(&context, &[])
        .await
        .expect("source")
        .into_iter()
        .map(|(_, name, _)| name)
        .collect();
    assert_eq!(names, vec!["CourseCreated"]);

    srv.kill();
}

fn rejected_as(error: ClientError, code: ErrorCode, what: &str) {
    let failure = error
        .failure()
        .unwrap_or_else(|| panic!("{what}: expected a failure from the server, got {error}"));
    assert_eq!(
        failure.code, code,
        "{what} must map to {code:?}, got: {failure}"
    );
}

/// The request validations: empty key, oversized state (server spawned with a
/// 1 KB cap), and the reserved-namespace guards that stop a client from
/// forging or querying snapshot records directly. (A negative position cannot
/// be said in the native protocol: positions are unsigned.)
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn append_snapshot_rejections() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let mut cfg = single_node_config(tmp.path().join("data"));
    cfg.max_snapshot_size = Some(1024);
    let mut srv = spawn_server(&cfg).expect("spawn");
    wait_until_ready(srv.listen, srv.admin, READY_TIMEOUT)
        .await
        .expect("ready");
    let context = connect_to(&srv).await;

    let cases: Vec<(&[u8], Vec<u8>, &str)> = vec![
        (b"", b"x".to_vec(), "empty key"),
        (b"k", vec![0u8; 2048], "state above max-snapshot-size"),
    ];
    for (key, state, what) in cases {
        let error = context
            .append_snapshot(key, &state, 0)
            .await
            .expect_err(&format!("{what} must be rejected"));
        rejected_as(error, ErrorCode::INVALID_ARGUMENT, what);
    }

    // A client cannot forge a snapshot record through Append...
    let forged = context
        .append(
            &[tagged_event("$snapshot.written", "courseId", "x", b"")],
            None,
        )
        .await
        .expect_err("forged $ event must be rejected");
    rejected_as(forged, ErrorCode::INVALID_ARGUMENT, "forged $ event");

    // ...nor reach one through a Source condition on the reserved tag —
    // the call is rejected outright, before any events are returned.
    let queried = context
        .source_all(0, &[Criterion::tag("$snapshot", "k")])
        .await
        .expect_err("querying the reserved namespace must fail");
    rejected_as(queried, ErrorCode::INVALID_ARGUMENT, "reserved-tag Source");

    srv.kill();
}

/// The reason snapshots ride the log: an acknowledged snapshot survives
/// SIGKILL and restart, resolved through crash recovery (which also
/// exercises the unindexed-segment fallback of the latest-lookup).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn acknowledged_snapshots_survive_crash_restart() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let data_dir = tmp.path().join("data");
    let cfg = single_node_config(data_dir.clone());

    {
        let mut srv = spawn_server(&cfg).expect("spawn");
        wait_until_ready(srv.listen, srv.admin, READY_TIMEOUT)
            .await
            .expect("ready");
        let context = connect_to(&srv).await;

        append_events(
            &context,
            vec![tagged_event(
                "CourseCreated",
                "courseId",
                "cs-101",
                b"created",
            )],
        )
        .await;
        context
            .append_snapshot(b"course:cs-101", b"durable state", 1)
            .await
            .expect("append snapshot acked");

        // Acked means quorum-durable: SIGKILL, no graceful shutdown.
        srv.kill();
    }

    let restart_cfg = SpawnConfig {
        data_dir,
        listen_port: free_port(),
        admin_port: free_port(),
        ..single_node_config(tmp.path().join("unused"))
    };
    let mut srv = spawn_server(&restart_cfg).expect("respawn");
    wait_until_ready(srv.listen, srv.admin, READY_TIMEOUT)
        .await
        .expect("ready after restart");
    let context = connect_to(&srv).await;

    let got = context
        .get_snapshot(b"course:cs-101")
        .await
        .expect("get snapshot after restart")
        .expect("acked snapshot must survive the crash");
    assert_eq!(got.state.as_ref(), b"durable state");
    assert_eq!(got.position, 1);

    srv.kill();
}
