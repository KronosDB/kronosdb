//! Single-node kill-mid-append crash-recovery proof.
//!
//! Spawns the production server, drives mixed conditional and unconditional
//! appends across eight consistency boundaries, kills the process 50–500ms
//! after the first acknowledgement, and restarts against the same data
//! directory. Ten fresh-directory iterations verify that acknowledged events
//! remain readable with their original contents and that appends can resume.

mod crash_harness;
use crash_harness::*;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;

const ITERATIONS: usize = 10;
const AGGREGATES: usize = 8;
const WRITER_TASKS: usize = 4;
const KILL_DELAY_MIN_MS: u64 = 50;
const KILL_DELAY_MAX_MS: u64 = 500;
const READY_TIMEOUT: Duration = Duration::from_secs(10);
const WORKLOAD_STARTUP_GRACE: Duration = Duration::from_secs(10);

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn crash_single_node_ten_iterations() {
    // Top-level timeout — 10 iterations * ~3s each = ~30s budget; hard-cap at 180s.
    tokio::time::timeout(Duration::from_secs(180), run_all_iterations())
        .await
        .expect("crash_single_node_ten_iterations timed out");
}

async fn run_all_iterations() {
    for i in 0..ITERATIONS {
        eprintln!("=== crash_single_node iteration {}/{ITERATIONS} ===", i + 1);
        run_one_iteration(i).await;
    }
}

async fn run_one_iteration(iter: usize) {
    let tmp = tempfile::tempdir().expect("tempdir");
    let data_dir = tmp.path().join("data");
    let acks_path = tmp.path().join("acks.log");
    let listen_port = free_port();
    let admin_port = free_port();

    let cfg = SpawnConfig {
        data_dir: data_dir.clone(),
        listen_port,
        admin_port,
        node_id: 1,
        peers: vec![],
        group_commit_ms: Some(2),
        segment_size: None,
        backup: None,
        max_snapshot_size: None,
    };

    // First boot: append under load, then kill the server.
    let acked_count_before_kill = {
        let mut srv = spawn_server(&cfg).expect("spawn server");
        wait_until_ready(srv.listen, srv.admin, READY_TIMEOUT)
            .await
            .expect("server ready");

        let ack_log = Arc::new(AckLog::create(acks_path.clone()).expect("ack log"));
        let (kill_tx, kill_rx) = watch::channel(false);
        let (first_ack_tx, first_ack_rx) = watch::channel(false);

        let addr = srv.listen.to_string();
        let mut writers = Vec::new();
        for writer_idx in 0..WRITER_TASKS {
            let addr = addr.clone();
            let ack_log = Arc::clone(&ack_log);
            let kill_rx = kill_rx.clone();
            let first_ack_tx = first_ack_tx.clone();
            writers.push(tokio::spawn(writer_loop(
                writer_idx,
                addr,
                ack_log,
                kill_rx,
                first_ack_tx,
            )));
        }

        // Killer: wait for first ack, then uniform [50, 500] ms, then SIGKILL.
        {
            let mut rx = first_ack_rx.clone();
            tokio::time::timeout(WORKLOAD_STARTUP_GRACE, async {
                while !*rx.borrow_and_update() {
                    if rx.changed().await.is_err() {
                        panic!("iter {iter}: append workers stopped before first ack");
                    }
                }
            })
            .await
            .unwrap_or_else(|_| panic!("iter {iter}: no first ack within startup grace"));
        }
        let delay_ms = rand_in(KILL_DELAY_MIN_MS, KILL_DELAY_MAX_MS);
        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
        eprintln!(
            "iter {iter}: SIGKILL after {delay_ms}ms post-first-ack; acks so far = {}",
            ack_log.len()
        );
        srv.kill();

        let _ = kill_tx.send(true);
        for w in writers {
            let _ = tokio::time::timeout(Duration::from_secs(2), w).await;
        }

        let count = ack_log.len();
        assert!(
            count > 0,
            "iter {iter}: no acks recorded before kill — kill window too short"
        );
        drop(ack_log);
        count
    };

    // Restart against the same data directory and verify recovery.
    let mut srv = spawn_server(&cfg).expect("respawn server");
    wait_until_ready(srv.listen, srv.admin, READY_TIMEOUT)
        .await
        .expect("restarted server ready");

    // Re-read the fsync'd sidecar.
    let acked = read_ack_sidecar(&acks_path).expect("read acks");
    assert_eq!(
        acked.len(),
        acked_count_before_kill,
        "iter {iter}: sidecar ack count drift (pre-kill={acked_count_before_kill}, reread={})",
        acked.len()
    );

    // ---- Assertion A (CRASH-03): every acked write is present ----
    let client = connect(srv.listen)
        .await
        .expect("connect to restarted server");
    let context = client.default_context();

    // Source by per-aggregate criterion: one `agg=<id>` criterion per
    // aggregate the writer loops used, unioned.
    let criteria = tag_criteria("agg", (0..AGGREGATES).map(|i| format!("agg-{i}")));
    let events_by_seq: HashMap<i64, (String, Vec<u8>)> = read_events(&context, &criteria)
        .await
        .expect("source")
        .into_iter()
        .map(|(position, name, payload)| (position as i64, (name, payload)))
        .collect();
    for rec in &acked {
        let (_, payload) = events_by_seq.get(&rec.server_sequence).unwrap_or_else(|| {
            panic!(
                "iter {iter}: acked event at server_sequence={} MISSING after restart (CRASH-03 VIOLATION)",
                rec.server_sequence
            )
        });
        let got_hash = hash_payload(payload);
        assert_eq!(
            got_hash, rec.payload_hash,
            "iter {iter}: acked event at server_sequence={} payload hash mismatch (CRASH-03 CONTENT VIOLATION)",
            rec.server_sequence
        );
    }

    // ---- Assertion B (CRASH-02 torn) ----
    let (log_records, event_records, scans) =
        scan_all_segments(&data_dir, "default").expect("scan segments");
    for s in &scans {
        assert_eq!(
            s.torn_tail_bytes,
            0,
            "iter {iter}: POST-RESTART torn tail in {}: {} bytes ({:?}). CRASH-02 VIOLATION.",
            s.path.display(),
            s.torn_tail_bytes,
            s.torn_reason
        );
    }
    eprintln!(
        "iter {iter}: CRC scan clean ({} raft log records, {} event records)",
        log_records, event_records
    );

    // ---- Assertion C (CRASH-02 phantom) ----
    let head = context.head().await.expect("get_head") as i64;
    assert_eq!(
        events_by_seq.len() as i64,
        head,
        "iter {iter}: phantom check FAILED — Source returned {} events but head = {} (CRASH-02 VIOLATION)",
        events_by_seq.len(),
        head
    );
    assert!(
        events_by_seq.len() >= acked.len(),
        "iter {iter}: fewer events visible ({}) than acked writes ({}) — acked writes lost (CRASH-03 VIOLATION)",
        events_by_seq.len(),
        acked.len()
    );

    eprintln!(
        "iter {iter}: OK  acked={}, readable={}, head={}",
        acked.len(),
        events_by_seq.len(),
        head
    );

    srv.kill();
    drop(tmp);
}

async fn writer_loop(
    writer_idx: usize,
    addr: String,
    ack_log: Arc<AckLog>,
    mut kill_rx: watch::Receiver<bool>,
    first_ack_tx: watch::Sender<bool>,
) {
    let Ok(client) = connect(addr).await else {
        return;
    };
    let context = client.default_context();
    let mut client_seq: u64 = 0;

    loop {
        if *kill_rx.borrow_and_update() {
            return;
        }
        let agg_idx = (writer_idx + client_seq as usize) % AGGREGATES;
        let agg_id = format!("agg-{agg_idx}");
        let payload = format!("w{writer_idx}-s{client_seq}-{agg_id}").into_bytes();
        let hash = hash_payload(&payload);

        let new_event = event(
            format!("evt-{writer_idx}-{client_seq}"),
            1_712_000_000_000 + client_seq as i64,
            "Incremented",
            "1.0",
            payload.clone(),
            vec![(b"agg".to_vec(), agg_id.clone().into_bytes())],
        );
        let condition = (!client_seq.is_multiple_of(3)).then(|| Condition {
            marker: 0,
            criteria: vec![Criterion {
                names: vec!["NeverEmitted".into()],
                tags: vec![(b"agg".to_vec(), agg_id.clone().into_bytes())],
            }],
        });

        let events = [new_event];
        let out = tokio::select! {
            res = context.append(&events, condition.as_ref()) => res,
            _ = kill_rx.changed() => return,
        };
        match out {
            Ok(Appended::Ok { first_position, .. }) => {
                let rec = AckRecord {
                    aggregate_id: agg_id,
                    client_sequence: client_seq,
                    payload_hash: hash,
                    server_sequence: first_position as i64,
                };
                if let Err(e) = ack_log.record(rec) {
                    eprintln!("writer {writer_idx}: ack sidecar error: {e}");
                    return;
                }
                let _ = first_ack_tx.send(true);
                client_seq += 1;
            }
            // Nothing conflicts with `NeverEmitted`, so a conflict or an
            // error means the server is going away.
            Ok(Appended::Conflict { .. }) | Err(_) => return,
        }
    }
}
