//! A deposed leader that holds an unreplicated suffix rejoins a new leader
//! that has written no events since taking over.
//!
//! The new leader's whole post-election history is control records: it rotates
//! to a fresh segment at its head and writes an `EpochChange` (and later a
//! `WatermarkCheckpoint`), both at an unchanged event position. The returning
//! node has to be truncated back to the leader's boundary *and* receive those
//! control records, or it stays byte-divergent for as long as the cluster is
//! idle: heads agree, so nothing else prompts a second look.
//!
//! A second pair of cases has the new leader write events (and, in the last,
//! a `WatermarkCheckpoint` after them) before the old leader returns with more
//! unreplicated events than the new leader has written. The returning node's
//! position is then past the leader's tail, but the two histories split much
//! lower, where the leader's epoch took over: the cut has to be there, not at
//! the leader's tail.
//!
//! Further cases have the leader go through more failovers while the old leader
//! is away (the cut is still the end of the old leader's own epoch), and have
//! the old leader's bytes below that point differ from the cluster's, which is
//! real corruption and has to stay loud instead of being repaired.

mod crash_harness;
use crash_harness::*;

use std::time::{Duration, Instant};

const READY_TIMEOUT: Duration = Duration::from_secs(15);
const CONVERGENCE_TIMEOUT: Duration = Duration::from_secs(30);
const ACKED_EVENTS: usize = 5;
/// Well under the 5s watermark-checkpoint tick, which would otherwise mask a
/// follower that was sent nothing.
const CATCH_UP_BUDGET: Duration = Duration::from_secs(3);

fn spawn_config(
    tmp: &std::path::Path,
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

fn node_event(sequence: usize) -> NewEvent {
    event(
        format!("idle-failover-{sequence}"),
        1_712_000_000_000 + sequence as i64,
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

fn logs_of(tmp: &std::path::Path) -> Vec<Vec<(String, Vec<u8>)>> {
    (0..3)
        .map(|node| {
            let data_dir = tmp.join(format!("node{}", node + 1)).join("data");
            read_valid_event_log(&data_dir, "default").unwrap_or_default()
        })
        .collect()
}

/// What the old leader holds that nobody else does, and what the new leader
/// writes before the old one returns.
#[derive(Clone, Copy)]
struct Scenario {
    name: &'static str,
    /// Events only the isolated old leader holds (positions 5..).
    unreplicated: usize,
    /// Events the leader writes in each epoch after the old leader is gone.
    new_events: usize,
    /// Wait for the new leader's WatermarkCheckpoint after those events.
    wait_checkpoint: bool,
    /// Failovers beyond the first while the old leader is away: the leader is
    /// killed and brought back, which claims another epoch.
    extra_failovers: usize,
    /// The old leader's last record below the cut is changed (with its CRC
    /// kept valid) before it returns: its history is not the cluster's.
    forged: bool,
}

async fn run_scenario(scenario: Scenario) {
    tokio::time::timeout(Duration::from_secs(240), run(scenario))
        .await
        .unwrap_or_else(|_| panic!("{} timed out", scenario.name));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn deposed_leader_with_unreplicated_suffix_rejoins_an_idle_leader() {
    run_scenario(Scenario {
        name: "idle",
        unreplicated: 1,
        new_events: 0,
        wait_checkpoint: false,
        extra_failovers: 0,
        forged: false,
    })
    .await;
}

/// The new leader wrote one event and then a checkpoint; the old leader holds
/// two unreplicated events, so its position is past the leader's tail and it
/// is cut back to a boundary whose preceding record is that checkpoint.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn deposed_leader_ahead_of_a_leader_ending_in_a_checkpoint_rejoins() {
    run_scenario(Scenario {
        name: "ahead-checkpoint",
        unreplicated: 2,
        new_events: 1,
        wait_checkpoint: true,
        extra_failovers: 0,
        forged: false,
    })
    .await;
}

/// The same position overlap without a checkpoint at the end of the leader's
/// log: its preceding record is an event the returning node never had.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn deposed_leader_ahead_of_a_leader_ending_in_an_event_rejoins() {
    run_scenario(Scenario {
        name: "ahead-event",
        unreplicated: 2,
        new_events: 1,
        wait_checkpoint: false,
        extra_failovers: 0,
        forged: false,
    })
    .await;
}

/// The overlap the other way round: the new leader has written more events
/// than the old leader holds unreplicated, so the returning node is *behind*
/// the leader's tail and is repaired through the follower-watermark path.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn deposed_leader_behind_a_leader_with_new_events_rejoins() {
    run_scenario(Scenario {
        name: "behind",
        unreplicated: 1,
        new_events: 2,
        wait_checkpoint: true,
        extra_failovers: 0,
        forged: false,
    })
    .await;
}

/// The leader changed once more while the old leader was away, so its log
/// holds three epochs. The old leader's own epoch ended at the first
/// failover, so that is where it is cut, however many epochs follow it.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn deposed_leader_several_epochs_behind_rejoins() {
    run_scenario(Scenario {
        name: "epochs-behind",
        unreplicated: 2,
        new_events: 1,
        wait_checkpoint: true,
        extra_failovers: 1,
        forged: false,
    })
    .await;
}

/// The old leader's record at the boundary is not the cluster's. That is not
/// a rejoin to repair: the node must keep failing loudly, keep its bytes, and
/// not be wiped or accepted.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn deposed_leader_with_a_different_history_below_the_cut_stays_loud() {
    run_scenario(Scenario {
        name: "forged",
        unreplicated: 2,
        new_events: 1,
        wait_checkpoint: false,
        extra_failovers: 0,
        forged: true,
    })
    .await;
}

/// The last record of a valid log is a WatermarkCheckpoint.
fn ends_in_checkpoint(log: &[(String, Vec<u8>)]) -> bool {
    let Some((_, bytes)) = log.last() else {
        return false;
    };
    let (mut offset, mut last_flags) = (13, None);
    while offset + 9 <= bytes.len() {
        let record_len = u32::from_le_bytes(bytes[offset + 4..offset + 8].try_into().unwrap());
        last_flags = Some(bytes[offset + 8]);
        offset += 9 + record_len as usize - 1;
    }
    last_flags == Some(0x22)
}

async fn run(scenario: Scenario) {
    let tmp = tempfile::tempdir().expect("tempdir");
    let evidence = EvidenceGuard::new("crash_idle_failover", 0, tmp.path());
    evidence.note(format!("scenario {}", scenario.name));
    let ports: Vec<u16> = (0..6).map(|_| free_port()).collect();
    let listen: [u16; 3] = [ports[0], ports[1], ports[2]];
    let admin: [u16; 3] = [ports[3], ports[4], ports[5]];
    let peers: Vec<(u64, String)> = (0..3)
        .map(|i| (i as u64 + 1, format!("127.0.0.1:{}", listen[i])))
        .collect();

    let mut srvs: Vec<ServerHandle> = Vec::new();
    for node in 0..3 {
        std::fs::create_dir_all(tmp.path().join(format!("node{}", node + 1)).join("data"))
            .expect("mkdir");
        srvs.push(
            spawn_server(&spawn_config(tmp.path(), node, &listen, &admin, &peers))
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
        .expect("initial leader");

    // Acknowledged history on every node.
    let context = connect(srvs[leader].listen)
        .await
        .expect("connect leader")
        .default_context();
    for sequence in 0..ACKED_EVENTS {
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            match context.append(&[node_event(sequence)], None).await {
                Ok(Appended::Ok { .. }) => break,
                other => {
                    assert!(
                        Instant::now() < deadline,
                        "initial append {sequence} never acknowledged: {other:?}"
                    );
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        }
    }

    // Cut the leader off from both followers and give it one more event. It
    // lands in the leader's own log but can never reach a quorum.
    let followers: Vec<usize> = (0..3).filter(|node| *node != leader).collect();
    for follower in &followers {
        srvs[*follower].pause();
    }
    let suffix: Vec<NewEvent> = (0..scenario.unreplicated)
        .map(|i| node_event(ACKED_EVENTS + i))
        .collect();
    let unreplicated =
        tokio::time::timeout(Duration::from_millis(1500), context.append(&suffix, None)).await;
    assert!(
        !matches!(unreplicated, Ok(Ok(Appended::Ok { .. }))),
        "an isolated leader acknowledged an append: {unreplicated:?}"
    );
    // The leader's group commit makes the event durable locally.
    tokio::time::sleep(Duration::from_millis(200)).await;
    let leader_dir = tmp.path().join(format!("node{}", leader + 1)).join("data");
    let leader_events =
        count_event_records(&read_valid_event_log(&leader_dir, "default").expect("leader log"));
    evidence.note(format!(
        "leader=node{} holds {leader_events} events before the kill",
        leader + 1
    ));
    assert_eq!(
        leader_events,
        ACKED_EVENTS + scenario.unreplicated,
        "the isolated leader should hold exactly the unreplicated events"
    );
    drop(context);
    // The followers were only frozen, so the wave the leader sent them is
    // still in their socket buffers and would be applied on SIGCONT. Kill
    // them instead, which drops those bytes, and bring them back without it.
    for follower in &followers {
        srvs[*follower].kill();
    }
    srvs[leader].kill();
    for follower in &followers {
        srvs[*follower] = spawn_server(&spawn_config(
            tmp.path(),
            *follower,
            &listen,
            &admin,
            &peers,
        ))
        .expect("respawn follower");
    }

    // The survivors elect, take over, and (the point of the test) write no
    // events: wait until both hold the new epoch's EpochChange.
    wait_for_raft_leader(&srvs, READY_TIMEOUT)
        .await
        .expect("failover election");
    let start = Instant::now();
    loop {
        let logs = logs_of(tmp.path());
        let [a, b] = [&logs[followers[0]], &logs[followers[1]]];
        if a.len() >= 2 && a == b {
            break;
        }
        assert!(
            start.elapsed() < CONVERGENCE_TIMEOUT,
            "the survivors never converged on a new epoch's EpochChange"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // The new leader writes its own history in each epoch it holds while the
    // old leader is away, and may end it in a checkpoint.
    let mut written = 0;
    for round in 0..=scenario.extra_failovers {
        if round > 0 {
            // Another failover: freeze the leader until the other survivor
            // times out and campaigns. The node that wins the higher term
            // claims one more epoch (a fresh segment on both nodes).
            // Restarting the leader would not do: it resumes its own term and
            // epoch.
            let segments_before = logs_of(tmp.path())[followers[0]].len();
            // Whether the frozen leader loses its term is up to Raft's timers,
            // so freeze it again if the first time changed nothing.
            let mut failed_over = false;
            for _attempt in 0..4 {
                let leader_now = wait_for_raft_leader(&srvs, READY_TIMEOUT)
                    .await
                    .expect("leader before another failover");
                srvs[leader_now].pause();
                tokio::time::sleep(Duration::from_secs(6)).await;
                srvs[leader_now].resume();
                wait_for_raft_leader(&srvs, READY_TIMEOUT)
                    .await
                    .expect("election after another failover");
                let start = Instant::now();
                while start.elapsed() < Duration::from_secs(12) {
                    let logs = logs_of(tmp.path());
                    let [a, b] = [&logs[followers[0]], &logs[followers[1]]];
                    if a.len() > segments_before && a == b {
                        failed_over = true;
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                if failed_over {
                    break;
                }
            }
            assert!(
                failed_over,
                "the survivors never converged on failover {round}'s EpochChange"
            );
        }
        if scenario.new_events == 0 {
            continue;
        }
        let new_leader = wait_for_raft_leader(&srvs, READY_TIMEOUT)
            .await
            .expect("new leader");
        let context = connect(srvs[new_leader].listen)
            .await
            .expect("connect new leader")
            .default_context();
        for _ in 0..scenario.new_events {
            let i = written;
            let deadline = Instant::now() + Duration::from_secs(15);
            loop {
                let fresh = event(
                    format!("new-leader-{i}"),
                    1_713_000_000_000 + i as i64,
                    "Incremented",
                    "1.0",
                    format!("new-leader-payload-{i}").into_bytes(),
                    vec![(b"agg".to_vec(), b"agg-0".to_vec())],
                );
                match context.append(&[fresh], None).await {
                    Ok(Appended::Ok { .. }) => break,
                    other => {
                        assert!(
                            Instant::now() < deadline,
                            "new leader append {i} never acknowledged: {other:?}"
                        );
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                }
            }
            written += 1;
        }
    }
    if scenario.wait_checkpoint {
        // The checkpoint tick is every 5s and only writes once the head moved.
        let start = Instant::now();
        loop {
            let logs = logs_of(tmp.path());
            let [a, b] = [&logs[followers[0]], &logs[followers[1]]];
            if a == b && ends_in_checkpoint(a) {
                break;
            }
            assert!(
                start.elapsed() < CONVERGENCE_TIMEOUT,
                "the survivors never both ended in a WatermarkCheckpoint"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
    let expected_head = (ACKED_EVENTS + written) as u64;

    let mut forged_log = None;
    if scenario.forged {
        forge_boundary_record(
            &tmp.path()
                .join(format!("node{}", followers[0] + 1))
                .join("data"),
            &leader_dir,
        );
        forged_log = Some(read_valid_event_log(&leader_dir, "default").expect("forged log"));
    }

    // The deposed leader returns with its unreplicated events.
    srvs[leader] = spawn_server(&spawn_config(tmp.path(), leader, &listen, &admin, &peers))
        .expect("respawn deposed leader");
    wait_until_ready(srvs[leader].listen, srvs[leader].admin, READY_TIMEOUT)
        .await
        .expect("restarted node ready");

    if let Some(forged_log) = forged_log {
        expect_loud_refusal(tmp.path(), leader, &leader_dir, &forged_log, &evidence).await;
        evidence.finish();
        return;
    }

    // Its unreplicated events are cut first: the head falls back to the
    // cluster's history.
    let start = Instant::now();
    let context = loop {
        let attempt = async {
            let context = connect(srvs[leader].listen).await.ok()?.default_context();
            (context.head().await.ok()? == expected_head).then_some(context)
        };
        if let Some(context) = attempt.await {
            break context;
        }
        assert!(
            start.elapsed() < CONVERGENCE_TIMEOUT,
            "the returning node never reached the cluster's head"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    drop(context);

    // Heads now agree, so byte equality has to follow promptly: the next
    // WatermarkCheckpoint tick (5s) or new write must not be what repairs it.
    let start = Instant::now();
    loop {
        let logs = logs_of(tmp.path());
        let Some(detail) = describe_log_divergence(&logs) else {
            break;
        };
        if start.elapsed() > CATCH_UP_BUDGET {
            evidence.note(format!(
                "not byte-identical after {CATCH_UP_BUDGET:?}: {detail}"
            ));
            panic!(
                "the returning node is not byte-identical to the new leader {CATCH_UP_BUDGET:?} after its heads agreed: {detail}"
            );
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    evidence.finish();
}

/// Splits a segment's bytes into its records: (offset, record bytes).
fn records_of(bytes: &[u8]) -> Vec<(usize, &[u8])> {
    let mut records = Vec::new();
    let mut offset = 13;
    while offset + 9 <= bytes.len() {
        let record_len = u32::from_le_bytes(bytes[offset + 4..offset + 8].try_into().unwrap());
        let end = offset + 8 + record_len as usize;
        if record_len == 0 || end > bytes.len() {
            break;
        }
        records.push((offset, &bytes[offset..end]));
        offset = end;
    }
    records
}

/// Changes the record the cluster's first segment ends with in the old
/// leader's copy of that segment, and keeps the record's CRC valid. The node
/// still opens cleanly and holds a well-formed log; it just is not the
/// cluster's.
fn forge_boundary_record(survivor_data: &std::path::Path, old_leader_data: &std::path::Path) {
    let survivor = read_valid_event_log(survivor_data, "default").expect("survivor log");
    let (_, survivor_first) = &survivor[0];
    let (_, last) = *records_of(survivor_first).last().expect("a record");
    let boundary_crc = u32::from_le_bytes(last[..4].try_into().unwrap());

    let path = old_leader_data.join("default").join(&survivor[0].0);
    let mut bytes = std::fs::read(&path).expect("old leader segment");
    let (offset, record_len) = records_of(&bytes)
        .into_iter()
        .find(|(_, record)| u32::from_le_bytes(record[..4].try_into().unwrap()) == boundary_crc)
        .map(|(offset, record)| (offset, record.len()))
        .expect("the old leader holds the cluster's boundary record");
    let end = offset + record_len;
    bytes[end - 1] ^= 0xFF;
    let flags = bytes[offset + 8];
    let crc = crc32c::crc32c_append(crc32c::crc32c(&[flags]), &bytes[offset + 9..end]);
    bytes[offset..offset + 4].copy_from_slice(&crc.to_le_bytes());
    std::fs::write(&path, &bytes).expect("write forged segment");
}

/// The returning node must say what is wrong, keep saying it, and keep its
/// bytes: no repair, no silent acceptance, no wipe.
async fn expect_loud_refusal(
    tmp: &std::path::Path,
    leader: usize,
    leader_dir: &std::path::Path,
    before: &[(String, Vec<u8>)],
    evidence: &EvidenceGuard,
) {
    let server_log = tmp
        .join(format!("node{}", leader + 1))
        .join(format!("server-{}.log", leader + 1));
    let start = Instant::now();
    loop {
        let said = std::fs::read_to_string(&server_log).unwrap_or_default();
        if said.contains("committed prefix diverges") {
            break;
        }
        assert!(
            start.elapsed() < CONVERGENCE_TIMEOUT,
            "the returning node never reported its diverged history"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    // Several retries later it is still refusing and has changed nothing.
    tokio::time::sleep(Duration::from_secs(2)).await;
    let said = std::fs::read_to_string(&server_log).unwrap_or_default();
    let reports = said.matches("committed prefix diverges").count();
    evidence.note(format!("refused {reports} times"));
    assert!(reports >= 2, "it should keep retrying and keep refusing");
    assert_eq!(
        read_valid_event_log(leader_dir, "default").expect("old leader log"),
        before,
        "a node that failed the check must be left as it was"
    );
}
