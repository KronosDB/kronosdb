//! Three-node proof of the snapshot cluster path (ADR-0005).
//!
//! A snapshot appended through a FOLLOWER must forward to the claimed leader
//! (the `system` flag on ForwardAppend), replicate like any append, and then
//! be readable through every node's local read path — including the fused
//! read — since snapshot reads never leave the node that serves them.

mod crash_harness;
use crash_harness::*;

use std::time::{Duration, Instant};

const READY_TIMEOUT: Duration = Duration::from_secs(15);
const LEADER_TIMEOUT: Duration = Duration::from_secs(20);
const REPLICATION_TIMEOUT: Duration = Duration::from_secs(15);

async fn connect_to(srv: &ServerHandle) -> Context {
    connect(srv.listen)
        .await
        .expect("connect event store client")
        .default_context()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn snapshot_via_follower_forwards_and_replicates() {
    tokio::time::timeout(Duration::from_secs(120), run())
        .await
        .expect("snapshot_via_follower_forwards_and_replicates timed out");
}

async fn run() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let ports: Vec<u16> = (0..6).map(|_| free_port()).collect();
    let listen: [u16; 3] = [ports[0], ports[1], ports[2]];
    let admin: [u16; 3] = [ports[3], ports[4], ports[5]];
    let peers: Vec<(u64, String)> = (0..3)
        .map(|i| (i as u64 + 1, format!("127.0.0.1:{}", listen[i])))
        .collect();

    let mut srvs: Vec<ServerHandle> = Vec::new();
    for i in 0..3 {
        let data_dir = tmp.path().join(format!("node{}", i + 1)).join("data");
        std::fs::create_dir_all(&data_dir).expect("mkdir");
        let cfg = SpawnConfig {
            data_dir,
            listen_port: listen[i],
            admin_port: admin[i],
            node_id: (i + 1) as u64,
            peers: peers.clone(),
            group_commit_ms: Some(2),
            segment_size: None,
            backup: None,
            max_snapshot_size: None,
        };
        srvs.push(spawn_server(&cfg).expect("spawn node"));
    }
    for srv in &srvs {
        wait_until_ready(srv.listen, srv.admin, READY_TIMEOUT)
            .await
            .expect("node ready");
    }

    let leader = wait_for_raft_leader(&srvs, LEADER_TIMEOUT)
        .await
        .expect("one leader elected");
    let follower = (0..3).find(|&i| i != leader).expect("a follower exists");
    eprintln!(
        "leader = node {}, appending via follower node {}",
        leader + 1,
        follower + 1
    );

    // Seed a user event through the follower (exercises plain forwarding and
    // gives the snapshot a fold marker to reference).
    let follower_client = connect_to(&srvs[follower]).await;
    let seeded = follower_client
        .append(
            &[event(
                "id-created".into(),
                0,
                "CourseCreated",
                "1",
                b"created".to_vec(),
                vec![(b"courseId".to_vec(), b"cs-101".to_vec())],
            )],
            None,
        )
        .await
        .expect("forwarded user append");
    let Appended::Ok {
        marker: fold_marker,
        ..
    } = seeded
    else {
        panic!("the seed append conflicted: {seeded:?}");
    };

    // The snapshot append lands through the follower: rejected before the
    // fix that forwards system-framed appends, so this call IS the proof.
    let record_position = follower_client
        .append_snapshot(b"course:cs-101", b"replicated state", fold_marker)
        .await
        .expect("snapshot append via follower must forward to the leader");
    assert!(record_position >= fold_marker);

    // Every node must serve the snapshot from its LOCAL log once the record
    // replicates — leader immediately, followers within the tail's lag.
    for (index, srv) in srvs.iter().enumerate() {
        let context = connect_to(srv).await;
        let deadline = Instant::now() + REPLICATION_TIMEOUT;
        let snapshot = loop {
            let found = context
                .get_snapshot(b"course:cs-101")
                .await
                .expect("get snapshot");
            if let Some(snapshot) = found {
                break snapshot;
            }
            assert!(
                Instant::now() < deadline,
                "node {} never saw the replicated snapshot",
                index + 1
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        };
        assert_eq!(
            snapshot.state.as_ref(),
            b"replicated state",
            "node {}",
            index + 1
        );
        assert_eq!(snapshot.position, fold_marker, "node {}", index + 1);
    }

    // The fused read works on a follower: the snapshot plus the marker, with
    // the seeded event already summarized (nothing to replay).
    let mut source = follower_client
        .snapshotted_source(b"course:cs-101", &[Criterion::tag("courseId", "cs-101")], 0)
        .await
        .expect("snapshotted source on follower");
    let snapshot = source
        .snapshot
        .clone()
        .expect("fused read on follower returns the snapshot");
    assert_eq!(snapshot.state.as_ref(), b"replicated state");
    let mut events = 0;
    let mut marker = None;
    while let Some(batch) = source.next().await.expect("stream message") {
        events += batch.len();
        marker = batch.marker().or(marker);
    }
    assert_eq!(events, 0, "the seeded event is already summarized");
    assert!(marker.is_some(), "fused read ends with the marker");

    for srv in &mut srvs {
        srv.kill();
    }
}
