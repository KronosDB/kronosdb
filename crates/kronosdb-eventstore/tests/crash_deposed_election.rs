//! A deposed leader that wins the Raft election before its Tail session has
//! cut it back.
//!
//! The control plane only compares Raft logs when it votes, and the old leader
//! can have the new epoch's claim in its Raft log while its segments still hold
//! the unreplicated suffix of the epoch it led. It then claims leadership on
//! top of that suffix. The cluster has acknowledged an event in the epoch that
//! replaced the old leader's; the old leader's own events at the same positions
//! were never acknowledged and must not win over it.
//!
//! The state is built offline so no Raft timer decides it: both survivors of
//! the first failover are killed once the new leader has acknowledged an event,
//! the old leader gets the new leader's Raft state (its log now holds the
//! claim), and only the old leader and one survivor are started. The election
//! between those two is a race, so the setup is restored and repeated until the
//! old leader wins it.

mod crash_harness;
use crash_harness::*;

use std::path::Path;
use std::time::{Duration, Instant};

const READY_TIMEOUT: Duration = Duration::from_secs(15);
const CONVERGENCE_TIMEOUT: Duration = Duration::from_secs(30);
const ACKED_EVENTS: usize = 5;
const UNREPLICATED_EVENTS: usize = 2;
const ELECTION_ATTEMPTS: usize = 8;

fn spawn_config(
    tmp: &Path,
    node: usize,
    listen: &[u16; 3],
    admin: &[u16; 3],
    peers: &[(u64, String)],
) -> SpawnConfig {
    SpawnConfig {
        data_dir: tmp.join(format!("node{}", node + 1)).join("data"),
        listen_port: listen[node],
        admin_port: admin[node],
        node_id: (node + 1) as u64,
        peers: peers.to_vec(),
        group_commit_ms: Some(2),
        segment_size: None,
        backup: None,
        max_snapshot_size: None,
    }
}

fn node_event(label: &str, sequence: usize) -> NewEvent {
    event(
        format!("{label}-{sequence}"),
        1_712_000_000_000 + sequence as i64,
        "Incremented",
        "1.0",
        format!("{label}-payload-{sequence}").into_bytes(),
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

fn log_of(tmp: &Path, node: usize) -> Vec<(String, Vec<u8>)> {
    let data_dir = tmp.join(format!("node{}", node + 1)).join("data");
    read_valid_event_log(&data_dir, "default").unwrap_or_default()
}

fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    haystack
        .windows(needle.len())
        .any(|window| window == needle)
}

fn copy_dir(from: &Path, to: &Path) {
    let status = std::process::Command::new("cp")
        .arg("-R")
        .arg(from)
        .arg(to)
        .status()
        .expect("invoke cp");
    assert!(status.success(), "cp -R {from:?} {to:?} failed");
}

async fn append_acked(context: &Context, event: NewEvent, what: &str) {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        match context.append(std::slice::from_ref(&event), None).await {
            Ok(Appended::Ok { .. }) => return,
            other => {
                assert!(
                    Instant::now() < deadline,
                    "{what} never acknowledged: {other:?}"
                );
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
}

/// Before the claim ranked voters by epoch, the old leader claimed on top of
/// its unacknowledged suffix, the survivor was cut back to its watermark and
/// took that suffix, and the event the new leader acknowledged was gone from
/// both live nodes.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_deposed_leader_that_wins_the_election_does_not_keep_its_unacknowledged_suffix() {
    tokio::time::timeout(Duration::from_secs(360), run())
        .await
        .expect("scenario timed out");
}

async fn run() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let evidence = EvidenceGuard::new("crash_deposed_election", 0, tmp.path());
    let ports: Vec<u16> = (0..6).map(|_| free_port()).collect();
    let listen: [u16; 3] = [ports[0], ports[1], ports[2]];
    let admin: [u16; 3] = [ports[3], ports[4], ports[5]];
    let peers: Vec<(u64, String)> = (0..3)
        .map(|i| (i as u64 + 1, format!("127.0.0.1:{}", listen[i])))
        .collect();
    let config = |node: usize| spawn_config(tmp.path(), node, &listen, &admin, &peers);

    let mut srvs: Vec<ServerHandle> = Vec::new();
    for node in 0..3 {
        std::fs::create_dir_all(tmp.path().join(format!("node{}", node + 1)).join("data"))
            .expect("mkdir");
        srvs.push(spawn_server(&config(node)).expect("spawn node"));
    }
    for srv in &srvs {
        wait_until_ready(srv.listen, srv.admin, READY_TIMEOUT)
            .await
            .expect("node ready");
    }
    let old = wait_for_raft_leader(&srvs, READY_TIMEOUT)
        .await
        .expect("initial leader");
    evidence.note(format!("old leader is node{}", old + 1));

    // Acknowledged history on every node.
    let context = connect(srvs[old].listen)
        .await
        .expect("connect leader")
        .default_context();
    for sequence in 0..ACKED_EVENTS {
        append_acked(&context, node_event("shared", sequence), "initial append").await;
    }

    // The old leader is cut off from both followers and holds events that can
    // never reach a quorum.
    let survivors: Vec<usize> = (0..3).filter(|node| *node != old).collect();
    for survivor in &survivors {
        srvs[*survivor].pause();
    }
    let suffix: Vec<NewEvent> = (0..UNREPLICATED_EVENTS)
        .map(|i| node_event("unreplicated", ACKED_EVENTS + i))
        .collect();
    let unreplicated =
        tokio::time::timeout(Duration::from_millis(1500), context.append(&suffix, None)).await;
    assert!(
        !matches!(unreplicated, Ok(Ok(Appended::Ok { .. }))),
        "an isolated leader acknowledged an append: {unreplicated:?}"
    );
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        count_event_records(&log_of(tmp.path(), old)),
        ACKED_EVENTS + UNREPLICATED_EVENTS,
        "the isolated leader should hold exactly the unreplicated events"
    );
    drop(context);
    // The frozen survivors still have the old leader's wave in their socket
    // buffers; killing them drops it.
    for survivor in &survivors {
        srvs[*survivor].kill();
    }
    srvs[old].kill();
    for survivor in &survivors {
        srvs[*survivor] = spawn_server(&config(*survivor)).expect("respawn survivor");
    }

    // The survivors elect and take over, and acknowledge an event the old
    // leader has never seen.
    let new = wait_for_raft_leader(&srvs, READY_TIMEOUT)
        .await
        .expect("failover election");
    let other = *survivors.iter().find(|node| **node != new).unwrap();
    evidence.note(format!(
        "new leader is node{}, the other survivor node{}",
        new + 1,
        other + 1
    ));
    wait_until_ready(srvs[new].listen, srvs[new].admin, READY_TIMEOUT)
        .await
        .expect("new leader ready");
    let context = connect(srvs[new].listen)
        .await
        .expect("connect new leader")
        .default_context();
    append_acked(
        &context,
        node_event("new-leader", ACKED_EVENTS),
        "the new leader's append",
    )
    .await;
    drop(context);
    let start = Instant::now();
    loop {
        let (a, b) = (log_of(tmp.path(), new), log_of(tmp.path(), other));
        if a == b && count_event_records(&a) == ACKED_EVENTS + 1 {
            break;
        }
        assert!(
            start.elapsed() < CONVERGENCE_TIMEOUT,
            "the survivors never converged on the new leader's event"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    srvs[other].kill();
    srvs[new].kill();

    // Everything is down. Keep the state, then try the election until the old
    // leader wins it.
    let snapshot = tmp.path().join("snapshot");
    std::fs::create_dir_all(&snapshot).unwrap();
    for node in 0..3 {
        copy_dir(
            &tmp.path().join(format!("node{}", node + 1)),
            &snapshot.join(format!("node{}", node + 1)),
        );
    }
    let mut won = false;
    for attempt in 0..ELECTION_ATTEMPTS {
        for node in 0..3 {
            let live = tmp.path().join(format!("node{}", node + 1));
            std::fs::remove_dir_all(&live).unwrap();
            copy_dir(&snapshot.join(format!("node{}", node + 1)), &live);
        }
        // The old leader now knows the new leader's claim: its Raft log is
        // the new leader's.
        let old_raft = tmp.path().join(format!("node{}/data/raft", old + 1));
        std::fs::remove_dir_all(&old_raft).unwrap();
        copy_dir(
            &tmp.path().join(format!("node{}/data/raft", new + 1)),
            &old_raft,
        );

        srvs[old] = spawn_server(&config(old)).expect("start old leader");
        tokio::time::sleep(Duration::from_millis(300)).await;
        srvs[other] = spawn_server(&config(other)).expect("start survivor");
        let elected = wait_for_raft_leader(&srvs, READY_TIMEOUT).await;
        evidence.note(format!("attempt {attempt}: elected {elected:?}"));
        if elected == Ok(old) {
            won = true;
            break;
        }
        srvs[old].kill();
        srvs[other].kill();
    }
    assert!(
        won,
        "the old leader never won the election in {ELECTION_ATTEMPTS} attempts"
    );

    // The old leader leads on the survivor's vote. What it holds beyond the
    // cluster's acknowledged history was never acknowledged, so the cluster
    // must come back with the acknowledged history, accept writes, and keep
    // the event the new leader acknowledged.
    wait_until_ready(srvs[old].listen, srvs[old].admin, CONVERGENCE_TIMEOUT)
        .await
        .unwrap_or_else(|error| {
            evidence.note(format!(
                "the old leader never opened its write gate: {error}"
            ));
            panic!("the old leader won the election but never became writable: {error}")
        });
    let context = connect(srvs[old].listen)
        .await
        .expect("connect old leader")
        .default_context();
    append_acked(
        &context,
        node_event("after-election", ACKED_EVENTS + 1),
        "an append after the election",
    )
    .await;
    drop(context);

    let start = Instant::now();
    loop {
        let (a, b) = (log_of(tmp.path(), old), log_of(tmp.path(), other));
        let events = count_event_records(&a);
        let keeps_acknowledged = a
            .iter()
            .any(|(_, bytes)| contains(bytes, b"new-leader-payload-5"));
        let kept_unacknowledged = a
            .iter()
            .any(|(_, bytes)| contains(bytes, b"unreplicated-payload-5"));
        if describe_log_divergence(&[a, b]).is_none()
            && events == ACKED_EVENTS + 2
            && keeps_acknowledged
            && !kept_unacknowledged
        {
            break;
        }
        assert!(
            start.elapsed() < CONVERGENCE_TIMEOUT,
            "the nodes never converged on the acknowledged history"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    evidence.finish();
}
