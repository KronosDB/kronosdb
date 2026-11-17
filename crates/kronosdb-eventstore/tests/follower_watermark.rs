//! Every follower learns the quorum watermark as soon as it moves.
//!
//! In a three-node cluster an append is acknowledged once the leader and one
//! follower have it. The follower whose acknowledgement moved the watermark
//! hears about it; the other one must too, or it serves the old head to reads
//! and subscriptions until the next write or the leader's 5 s checkpoint. The
//! cluster here is idle after each append, so only a watermark sent to every
//! session can bring both followers up to date inside the budget.

mod crash_harness;
use crash_harness::*;

use std::time::{Duration, Instant};

const READY_TIMEOUT: Duration = Duration::from_secs(15);
/// Well under the 5 s watermark-checkpoint tick, which would otherwise mask a
/// follower that was never told.
const FOLLOWER_BUDGET: Duration = Duration::from_millis(1500);
const APPENDS: usize = 4;

fn node_event(sequence: usize) -> NewEvent {
    event(
        format!("follower-watermark-{sequence}"),
        1_712_000_000_000 + sequence as i64,
        "Incremented",
        "1.0",
        format!("payload-{sequence}").into_bytes(),
        vec![(b"agg".to_vec(), b"agg-0".to_vec())],
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn both_followers_see_an_append_well_before_the_next_checkpoint() {
    tokio::time::timeout(Duration::from_secs(120), run())
        .await
        .expect("both_followers_see_an_append_well_before_the_next_checkpoint timed out");
}

async fn run() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let evidence = EvidenceGuard::new("follower_watermark", 0, tmp.path());
    let ports: Vec<u16> = (0..6).map(|_| free_port()).collect();
    let listen: [u16; 3] = [ports[0], ports[1], ports[2]];
    let admin: [u16; 3] = [ports[3], ports[4], ports[5]];
    let peers: Vec<(u64, String)> = (0..3)
        .map(|i| (i as u64 + 1, format!("127.0.0.1:{}", listen[i])))
        .collect();

    let mut srvs: Vec<ServerHandle> = Vec::new();
    for node in 0..3 {
        let data_dir = tmp.path().join(format!("node{}", node + 1)).join("data");
        std::fs::create_dir_all(&data_dir).expect("mkdir");
        srvs.push(
            spawn_server(&SpawnConfig {
                data_dir,
                listen_port: listen[node],
                admin_port: admin[node],
                node_id: (node + 1) as u64,
                peers: peers.clone(),
                group_commit_ms: Some(2),
                segment_size: None,
                archive: None,
                max_snapshot_size: None,
            })
            .expect("spawn node"),
        );
    }
    for srv in &srvs {
        wait_until_ready(srv.listen, srv.admin, READY_TIMEOUT)
            .await
            .expect("node ready");
    }
    let leader = wait_for_raft_leader(&srvs, READY_TIMEOUT)
        .await
        .expect("leader");

    let leader_context = connect(srvs[leader].listen)
        .await
        .expect("connect leader")
        .default_context();
    let mut follower_contexts = Vec::new();
    for node in (0..3).filter(|node| *node != leader) {
        follower_contexts.push((
            node,
            connect(srvs[node].listen)
                .await
                .expect("connect follower")
                .default_context(),
        ));
    }

    for sequence in 0..APPENDS {
        let deadline = Instant::now() + Duration::from_secs(15);
        let expected_head = loop {
            match leader_context.append(&[node_event(sequence)], None).await {
                Ok(Appended::Ok {
                    first_position,
                    count,
                    ..
                }) => break first_position + count as u64,
                other => {
                    assert!(
                        Instant::now() < deadline,
                        "append {sequence} never acknowledged: {other:?}"
                    );
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        };
        let appended = Instant::now();
        for (node, context) in &follower_contexts {
            loop {
                let head = context.head().await.expect("follower head");
                if head >= expected_head {
                    break;
                }
                assert!(
                    appended.elapsed() < FOLLOWER_BUDGET,
                    "append {sequence}: node{} still reports head {head} (want {expected_head}) \
                     {FOLLOWER_BUDGET:?} after the append was acknowledged",
                    node + 1
                );
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
    }
    evidence.finish();
}
