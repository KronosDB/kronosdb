//! Wire-level proof of the snapshot RPCs (ADR-0005).
//!
//! Drives the production server over gRPC and verifies the client-observable
//! contract: the append/get roundtrip, the fused read's frame ordering and
//! consistency marker, invisibility on the ordinary read path, the request
//! rejections, and — the reason snapshots ride the log at all — that an
//! acknowledged snapshot survives SIGKILL and restart.

#![allow(clippy::result_large_err)]

mod crash_harness;
use crash_harness::pb::eventstore as pb;
use crash_harness::*;
use pb::Tag;

use std::time::Duration;

use pb::event_store_client::EventStoreClient;
use tonic::transport::Channel;

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

async fn connect(srv: &ServerHandle) -> EventStoreClient<Channel> {
    EventStoreClient::connect(format!("http://{}", srv.listen))
        .await
        .expect("connect event store client")
}

fn tagged_event(name: &str, tag_key: &str, tag_value: &str, payload: &[u8]) -> pb::TaggedEvent {
    pb::TaggedEvent {
        event: Some(pb::Event {
            identifier: format!("id-{name}-{tag_value}"),
            timestamp: 0,
            name: name.into(),
            version: "1".into(),
            payload: payload.to_vec(),
            metadata: Default::default(),
        }),
        tags: vec![Tag {
            key: tag_key.as_bytes().to_vec(),
            value: tag_value.as_bytes().to_vec(),
        }],
    }
}

async fn append_events(
    client: &mut EventStoreClient<Channel>,
    events: Vec<pb::TaggedEvent>,
) -> pb::AppendResponse {
    client
        .append(pb::AppendRequest {
            condition: None,
            events,
        })
        .await
        .expect("append")
        .into_inner()
}

/// Drains a SnapshottedSource stream into (snapshot frame, events, marker).
async fn drain_snapshotted_source(
    client: &mut EventStoreClient<Channel>,
    request: pb::SnapshottedSourceRequest,
) -> (Option<pb::Snapshot>, Vec<pb::SequencedEvent>, Option<i64>) {
    let mut stream = client
        .snapshotted_source(request)
        .await
        .expect("snapshotted_source")
        .into_inner();

    let mut snapshot = None;
    let mut events = Vec::new();
    let mut marker = None;
    let mut saw_batch = false;
    while let Some(response) = stream.message().await.expect("stream message") {
        match response.frame.expect("frame must be set") {
            pb::snapshotted_source_response::Frame::Snapshot(s) => {
                assert!(!saw_batch, "snapshot frame must precede every batch");
                assert!(snapshot.is_none(), "at most one snapshot frame");
                snapshot = Some(s);
            }
            pb::snapshotted_source_response::Frame::Batch(batch) => {
                saw_batch = true;
                assert!(marker.is_none(), "no batch may follow the marker batch");
                events.extend(batch.events);
                marker = batch.consistency_marker;
            }
        }
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
    let mut client = connect(&srv).await;

    // Fold 3 course events; the append response's marker is the fold marker.
    let first = append_events(
        &mut client,
        vec![
            tagged_event("CourseCreated", "courseId", "cs-101", b"created"),
            tagged_event("StudentEnrolled", "courseId", "cs-101", b"alice"),
            tagged_event("StudentEnrolled", "courseId", "cs-101", b"bob"),
        ],
    )
    .await;
    let fold_marker = first.consistency_marker;

    // Store the snapshot "as the client would": after its transaction commits.
    let key = b"course:cs-101".to_vec();
    let append_snapshot = client
        .append_snapshot(pb::AppendSnapshotRequest {
            key: key.clone(),
            state: b"folded:3-events".to_vec(),
            position: fold_marker,
        })
        .await
        .expect("append snapshot")
        .into_inner();
    assert!(
        append_snapshot.sequence >= fold_marker,
        "the record lands at or after the state it summarizes"
    );

    // More events land after the snapshot, some for an unrelated course.
    append_events(
        &mut client,
        vec![
            tagged_event("StudentEnrolled", "courseId", "cs-101", b"carol"),
            tagged_event("CourseCreated", "courseId", "cs-999", b"other"),
            tagged_event("StudentEnrolled", "courseId", "cs-101", b"dave"),
        ],
    )
    .await;

    // GetSnapshot alone returns the stored state byte-exact.
    let got = client
        .get_snapshot(pb::GetSnapshotRequest { key: key.clone() })
        .await
        .expect("get snapshot")
        .into_inner()
        .snapshot
        .expect("snapshot must exist");
    assert_eq!(got.state, b"folded:3-events");
    assert_eq!(got.position, fold_marker);

    // The fused read: snapshot frame first, then ONLY the matching events
    // strictly after the fold position, then the marker.
    let criteria = vec![pb::Criterion {
        names: vec![],
        tags: vec![Tag {
            key: b"courseId".to_vec(),
            value: b"cs-101".to_vec(),
        }],
    }];
    let (snapshot, events, marker) = drain_snapshotted_source(
        &mut client,
        pb::SnapshottedSourceRequest {
            criteria: criteria.clone(),
            key: key.clone(),
            batch_size: 0,
        },
    )
    .await;

    let snapshot = snapshot.expect("fused read must start with the snapshot frame");
    assert_eq!(snapshot.state, b"folded:3-events");
    assert_eq!(snapshot.position, fold_marker);

    let payloads: Vec<&[u8]> = events
        .iter()
        .map(|e| e.event.as_ref().unwrap().payload.as_slice())
        .collect();
    assert_eq!(
        payloads,
        vec![b"carol".as_slice(), b"dave".as_slice()],
        "only matching events after the fold position, in order"
    );
    for event in &events {
        assert!(
            event.sequence >= fold_marker,
            "replay resumes at the (next-exclusive) fold marker"
        );
    }

    // The marker must work as a DCB condition, exactly as Source's would.
    let marker = marker.expect("final batch carries the consistency marker");
    let conditional = client
        .append(pb::AppendRequest {
            condition: Some(pb::ConsistencyCondition {
                consistency_marker: marker,
                criteria,
            }),
            events: vec![tagged_event(
                "CourseCapacityChanged",
                "courseId",
                "cs-101",
                b"cap",
            )],
        })
        .await
        .expect("conditional append with the fused read's marker must succeed");
    assert_eq!(conditional.into_inner().count, 1);

    srv.kill();
}

/// Without a snapshot the fused read is exactly a plain Source: no snapshot
/// frame, events from the beginning, marker on the final batch — including
/// on a completely empty result.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fused_read_without_snapshot_is_plain_source() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let mut srv = spawn_server(&single_node_config(tmp.path().join("data"))).expect("spawn");
    wait_until_ready(srv.listen, srv.admin, READY_TIMEOUT)
        .await
        .expect("ready");
    let mut client = connect(&srv).await;

    // Empty store: no snapshot frame, one empty marker-carrying batch.
    let (snapshot, events, marker) = drain_snapshotted_source(
        &mut client,
        pb::SnapshottedSourceRequest {
            criteria: vec![],
            key: b"never-written".to_vec(),
            batch_size: 0,
        },
    )
    .await;
    assert!(snapshot.is_none());
    assert!(events.is_empty());
    assert_eq!(marker, Some(0));

    append_events(
        &mut client,
        vec![
            tagged_event("CourseCreated", "courseId", "cs-101", b"created"),
            tagged_event("StudentEnrolled", "courseId", "cs-101", b"alice"),
        ],
    )
    .await;

    let (snapshot, events, marker) = drain_snapshotted_source(
        &mut client,
        pb::SnapshottedSourceRequest {
            criteria: vec![],
            key: b"never-written".to_vec(),
            batch_size: 0,
        },
    )
    .await;
    assert!(snapshot.is_none(), "a miss produces no snapshot frame");
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
    let mut client = connect(&srv).await;

    let key = b"course:cs-101".to_vec();
    for (state, position) in [(b"v1".as_slice(), 1), (b"v2".as_slice(), 2)] {
        client
            .append_snapshot(pb::AppendSnapshotRequest {
                key: key.clone(),
                state: state.to_vec(),
                position,
            })
            .await
            .expect("append snapshot");
    }

    let got = client
        .get_snapshot(pb::GetSnapshotRequest { key })
        .await
        .expect("get snapshot")
        .into_inner()
        .snapshot
        .expect("snapshot exists");
    assert_eq!(got.state, b"v2");
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
    let mut client = connect(&srv).await;

    append_events(
        &mut client,
        vec![tagged_event(
            "CourseCreated",
            "courseId",
            "cs-101",
            b"created",
        )],
    )
    .await;
    client
        .append_snapshot(pb::AppendSnapshotRequest {
            key: b"course:cs-101".to_vec(),
            state: b"state".to_vec(),
            position: 1,
        })
        .await
        .expect("append snapshot");

    // GetHead hides the trailing snapshot record's position.
    let head = client
        .get_head(pb::GetHeadRequest {})
        .await
        .expect("get head")
        .into_inner();
    assert_eq!(
        head.sequence, 1,
        "visible head excludes the snapshot record"
    );

    // Source never returns the record.
    let mut stream = client
        .source(pb::SourceRequest {
            from_sequence: 0,
            criteria: vec![],
            batch_size: 0,
        })
        .await
        .expect("source")
        .into_inner();
    let mut names = Vec::new();
    while let Some(response) = stream.message().await.expect("source message") {
        if let Some(batch) = response.batch {
            names.extend(
                batch
                    .events
                    .into_iter()
                    .map(|e| e.event.unwrap().name.clone()),
            );
        }
    }
    assert_eq!(names, vec!["CourseCreated"]);

    srv.kill();
}

/// The request validations: empty key, negative position, oversized state
/// (server spawned with a 1 KB cap), and the reserved-namespace guards that
/// stop a client from forging or querying snapshot records directly.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn append_snapshot_rejections() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let mut cfg = single_node_config(tmp.path().join("data"));
    cfg.max_snapshot_size = Some(1024);
    let mut srv = spawn_server(&cfg).expect("spawn");
    wait_until_ready(srv.listen, srv.admin, READY_TIMEOUT)
        .await
        .expect("ready");
    let mut client = connect(&srv).await;

    let cases: Vec<(pb::AppendSnapshotRequest, &str)> = vec![
        (
            pb::AppendSnapshotRequest {
                key: vec![],
                state: b"x".to_vec(),
                position: 0,
            },
            "empty key",
        ),
        (
            pb::AppendSnapshotRequest {
                key: b"k".to_vec(),
                state: b"x".to_vec(),
                position: -1,
            },
            "negative position",
        ),
        (
            pb::AppendSnapshotRequest {
                key: b"k".to_vec(),
                state: vec![0u8; 2048],
                position: 0,
            },
            "state above max-snapshot-size",
        ),
    ];
    for (request, what) in cases {
        let status = client
            .append_snapshot(request)
            .await
            .expect_err(&format!("{what} must be rejected"));
        assert_eq!(
            status.code(),
            tonic::Code::InvalidArgument,
            "{what} must map to InvalidArgument, got: {status}"
        );
    }

    // A client cannot forge a snapshot record through Append...
    let forged = client
        .append(pb::AppendRequest {
            condition: None,
            events: vec![tagged_event("$snapshot.written", "courseId", "x", b"")],
        })
        .await
        .expect_err("forged $ event must be rejected");
    assert_eq!(forged.code(), tonic::Code::InvalidArgument);

    // ...nor reach one through a Source condition on the reserved tag —
    // the call is rejected outright, before any stream opens.
    let queried = client
        .source(pb::SourceRequest {
            from_sequence: 0,
            criteria: vec![pb::Criterion {
                names: vec![],
                tags: vec![Tag {
                    key: b"$snapshot".to_vec(),
                    value: b"k".to_vec(),
                }],
            }],
            batch_size: 0,
        })
        .await
        .expect_err("querying the reserved namespace must fail");
    assert_eq!(queried.code(), tonic::Code::InvalidArgument);

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
        let mut client = connect(&srv).await;

        append_events(
            &mut client,
            vec![tagged_event(
                "CourseCreated",
                "courseId",
                "cs-101",
                b"created",
            )],
        )
        .await;
        client
            .append_snapshot(pb::AppendSnapshotRequest {
                key: b"course:cs-101".to_vec(),
                state: b"durable state".to_vec(),
                position: 1,
            })
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
    let mut client = connect(&srv).await;

    let got = client
        .get_snapshot(pb::GetSnapshotRequest {
            key: b"course:cs-101".to_vec(),
        })
        .await
        .expect("get snapshot after restart")
        .into_inner()
        .snapshot
        .expect("acked snapshot must survive the crash");
    assert_eq!(got.state, b"durable state");
    assert_eq!(got.position, 1);

    srv.kill();
}
