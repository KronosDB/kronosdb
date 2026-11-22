//! An append that cannot be committed is answered at the append deadline,
//! instead of waiting for as long as no quorum answers, and sending it again
//! does not write it twice.
//!
//! A three-node cluster with a two-second deadline: both followers are frozen,
//! so the leader writes the append and can never collect a quorum for it. The
//! client must hear `unavailable` around the deadline. With the followers back
//! it sends the same append again, to whoever leads by then, and the log must
//! hold the event exactly once, whether the first attempt survived or not.

mod crash_harness;

use std::time::{Duration, Instant};

use crash_harness::*;
use kronosdb_protocol::net::NetError;

const READY_TIMEOUT: Duration = Duration::from_secs(15);
const DEADLINE_SECS: u64 = 2;

fn new_event(sequence: usize) -> NewEvent {
    event(
        format!("deadline-{sequence}"),
        1_714_000_000_000 + sequence as i64,
        "Incremented",
        "1.0",
        format!("payload-{sequence}").into_bytes(),
        vec![(b"agg".to_vec(), b"agg-0".to_vec())],
    )
}

/// Event records (control records carry flags 0x2x) across a valid log.
fn count_event_records(log: &[(String, Vec<u8>)]) -> usize {
    let mut events = 0;
    for (_, bytes) in log {
        let mut offset = 13;
        while offset + 9 <= bytes.len() {
            let record_len = u32::from_le_bytes(bytes[offset + 4..offset + 8].try_into().unwrap());
            if bytes[offset + 8] & 0xF0 == 0 {
                events += 1;
            }
            offset += 9 + record_len as usize - 1;
        }
    }
    events
}

/// Appends until one is acknowledged, within `within`.
async fn append_acknowledged(addr: std::net::SocketAddr, sequence: usize, within: Duration) {
    let deadline = Instant::now() + within;
    loop {
        if let Ok(client) = connect(addr).await {
            let context = client.default_context();
            if let Ok(Appended::Ok { .. }) = context.append(&[new_event(sequence)], None).await {
                return;
            }
        }
        assert!(
            Instant::now() < deadline,
            "append {sequence} was never acknowledged"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_append_without_a_quorum_is_answered_at_the_deadline_and_can_be_sent_again() {
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
        srvs.push(
            spawn_server(&SpawnConfig {
                data_dir,
                listen_port: listen[i],
                admin_port: admin[i],
                node_id: (i + 1) as u64,
                peers: peers.clone(),
                group_commit_ms: Some(2),
                segment_size: None,
                archive: None,
                max_snapshot_size: None,
                append_timeout_secs: Some(DEADLINE_SECS),
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
        .expect("a leader");
    append_acknowledged(srvs[leader].listen, 0, Duration::from_secs(15)).await;

    // No quorum from here on: both followers are frozen.
    let followers: Vec<usize> = (0..3).filter(|&i| i != leader).collect();
    for &follower in &followers {
        srvs[follower].pause();
    }
    let client = connect(srvs[leader].listen).await.expect("connect leader");
    let context = client.default_context();
    let started = Instant::now();
    let answer = tokio::time::timeout(
        Duration::from_secs(30),
        context.append(&[new_event(1)], None),
    )
    .await
    .expect("the append was never answered");
    let waited = started.elapsed();
    match answer {
        Err(NetError::Failed(failure)) => {
            assert_eq!(failure.code, ErrorCode::UNAVAILABLE, "{failure}");
            assert!(
                failure.message.contains("send the same append again"),
                "{failure}"
            );
        }
        other => panic!("expected unavailable, got {other:?}"),
    }
    assert!(
        waited >= Duration::from_millis(DEADLINE_SECS * 1000 - 200),
        "answered after {waited:?}, before the {DEADLINE_SECS} s deadline"
    );

    // With the followers back, the client sends the same append again until
    // it is acknowledged.
    for &follower in &followers {
        srvs[follower].resume();
    }
    let retry_deadline = Instant::now() + Duration::from_secs(60);
    loop {
        if let Ok(leader) = wait_for_raft_leader(&srvs, READY_TIMEOUT).await
            && let Ok(client) = connect(srvs[leader].listen).await
            && let Ok(Appended::Ok { .. }) =
                client.default_context().append(&[new_event(1)], None).await
        {
            break;
        }
        assert!(
            Instant::now() < retry_deadline,
            "the retried append was never acknowledged"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    // And once more, acknowledged this time for certain: still one copy.
    let leader = wait_for_raft_leader(&srvs, READY_TIMEOUT)
        .await
        .expect("a leader");
    append_acknowledged(srvs[leader].listen, 1, Duration::from_secs(30)).await;

    // Every node's log holds the first event and the retried one, once each.
    let converged = Instant::now() + Duration::from_secs(30);
    loop {
        let counts: Vec<usize> = (0..3)
            .map(|node| {
                let dir = tmp.path().join(format!("node{}", node + 1)).join("data");
                read_valid_event_log(&dir, "default")
                    .map(|log| count_event_records(&log))
                    .unwrap_or(0)
            })
            .collect();
        if counts == [2, 2, 2] {
            break;
        }
        assert!(
            Instant::now() < converged,
            "expected two events on every node, found {counts:?}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}
