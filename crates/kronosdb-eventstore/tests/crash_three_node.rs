//! Phase 6 CRASH-02/CRASH-03 on a 3-node cluster (D-07, D-08).
//! Leader is SIGKILL'd mid-commit; remaining two nodes elect a new leader;
//! killed node restarts and catches up; all three converge.
//!
//! Lives in kronosdb-eventstore per D-06.

mod crash_harness;
use crash_harness::pb::eventstore as pb;
use crash_harness::*;
use pb::Tag;

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;
use tonic::transport::Channel;

use pb::event_store_client::EventStoreClient;

const ITERATIONS: usize = 10;
const AGGREGATES: usize = 8;
const WRITER_TASKS: usize = 4;
const KILL_DELAY_MIN_MS: u64 = 50; // D-04 LITERAL — same as 1-node
const KILL_DELAY_MAX_MS: u64 = 500; // D-04 LITERAL
const READY_TIMEOUT: Duration = Duration::from_secs(15);
const CONVERGENCE_TIMEOUT: Duration = Duration::from_secs(15);

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn crash_three_node_ten_iterations() {
    tokio::time::timeout(Duration::from_secs(300), run_all_iterations())
        .await
        .expect("crash_three_node_ten_iterations timed out");
}

async fn run_all_iterations() {
    for i in 0..ITERATIONS {
        eprintln!("=== crash_three_node iteration {}/{ITERATIONS} ===", i + 1);
        run_one_iteration(i).await;
    }
}

async fn run_one_iteration(iter: usize) {
    let tmp = tempfile::tempdir().expect("tempdir");
    let acks_path = tmp.path().join("acks.log");

    // Allocate 3 listen + 3 admin ports.
    let ports: Vec<u16> = (0..6).map(|_| free_port()).collect();
    let listen: [u16; 3] = [ports[0], ports[1], ports[2]];
    let admin: [u16; 3] = [ports[3], ports[4], ports[5]];

    let peers_str: Vec<(u64, String)> = (0..3)
        .map(|i| (i as u64 + 1, format!("127.0.0.1:{}", listen[i])))
        .collect();

    // Spawn 3 nodes.
    let mut srvs: Vec<ServerHandle> = Vec::new();
    for i in 0..3 {
        let data_dir = tmp.path().join(format!("node{}", i + 1)).join("data");
        std::fs::create_dir_all(&data_dir).expect("mkdir");
        let cfg = SpawnConfig {
            data_dir,
            listen_port: listen[i],
            admin_port: admin[i],
            node_id: (i + 1) as u64,
            peers: peers_str.clone(),
            group_commit_ms: Some(2),
        };
        srvs.push(spawn_server(&cfg).expect("spawn node"));
    }

    // Wait for each node's listener.
    for srv in &srvs {
        wait_until_ready(srv.listen, READY_TIMEOUT)
            .await
            .unwrap_or_else(|e| panic!("iter {iter}: node not ready: {e}"));
    }
    // Grace for leader election.
    tokio::time::sleep(Duration::from_secs(2)).await;

    // --- Drive workload targeting node 1 first; writers fall back on error.
    let ack_log = Arc::new(AckLog::create(acks_path.clone()).expect("ack log"));
    let (kill_tx, kill_rx) = watch::channel(false);
    let (first_ack_tx, first_ack_rx) = watch::channel(Option::<usize>::None);

    let addrs: Vec<String> = (0..3)
        .map(|i| format!("http://127.0.0.1:{}", listen[i]))
        .collect();
    let mut writers = Vec::new();
    for widx in 0..WRITER_TASKS {
        let addrs = addrs.clone();
        let ack_log = Arc::clone(&ack_log);
        let kill_rx = kill_rx.clone();
        let first_ack_tx = first_ack_tx.clone();
        writers.push(tokio::spawn(writer_loop(
            widx,
            addrs,
            ack_log,
            kill_rx,
            first_ack_tx,
        )));
    }

    // --- Killer: wait for first ack, identify leader, random 50-500ms delay, SIGKILL.
    let leader_idx = {
        let mut rx = first_ack_rx.clone();
        match tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if let Some(idx) = *rx.borrow_and_update() {
                    return idx;
                }
                if rx.changed().await.is_err() {
                    return 0;
                }
            }
        })
        .await
        {
            Ok(idx) => idx,
            Err(_) => panic!("iter {iter}: no acks in 10s — cluster not functional"),
        }
    };
    let delay_ms = rand_in(KILL_DELAY_MIN_MS, KILL_DELAY_MAX_MS);
    tokio::time::sleep(Duration::from_millis(delay_ms)).await;
    eprintln!(
        "iter {iter}: SIGKILL leader=node{} after {delay_ms}ms; acks so far = {}",
        leader_idx + 1,
        ack_log.len()
    );
    srvs[leader_idx].kill();

    // Stop writers.
    let _ = kill_tx.send(true);
    for w in writers {
        let _ = tokio::time::timeout(Duration::from_secs(3), w).await;
    }
    let acks_before_restart = ack_log.len();
    assert!(acks_before_restart > 0, "iter {iter}: no acks before kill");
    drop(ack_log);

    // Hand off to Task 2's verifier.
    post_restart_verify(
        iter,
        &tmp,
        &acks_path,
        acks_before_restart,
        &mut srvs,
        leader_idx,
        listen,
        admin,
        peers_str,
    )
    .await;

    for srv in srvs.iter_mut() {
        srv.kill();
    }
    drop(tmp);
}

async fn writer_loop(
    widx: usize,
    addrs: Vec<String>,
    ack_log: Arc<AckLog>,
    mut kill_rx: watch::Receiver<bool>,
    first_ack_tx: watch::Sender<Option<usize>>,
) {
    let mut node_idx = 0;
    let mut client_opt: Option<EventStoreClient<Channel>> = None;
    let mut client_seq: u64 = 0;

    loop {
        if *kill_rx.borrow_and_update() {
            return;
        }

        if client_opt.is_none() {
            match Channel::from_shared(addrs[node_idx].clone())
                .unwrap()
                .connect()
                .await
            {
                Ok(ch) => client_opt = Some(EventStoreClient::new(ch)),
                Err(_) => {
                    node_idx = (node_idx + 1) % 3;
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
            }
        }
        let client = client_opt.as_mut().unwrap();

        let agg_idx = (widx + client_seq as usize) % AGGREGATES;
        let agg_id = format!("agg-{agg_idx}");
        let payload = format!("w{widx}-s{client_seq}-{agg_id}").into_bytes();
        let hash = hash_payload(&payload);
        let tagged = pb::TaggedEvent {
            event: Some(pb::Event {
                identifier: format!("evt-{widx}-{client_seq}"),
                timestamp: 1_712_000_000_000 + client_seq as i64,
                name: "Incremented".into(),
                version: "1.0".into(),
                payload: payload.clone(),
                metadata: Default::default(),
            }),
            tags: vec![Tag {
                key: b"agg".to_vec(),
                value: agg_id.clone().into_bytes(),
            }],
        };
        let condition = if client_seq % 3 != 0 {
            Some(pb::ConsistencyCondition {
                consistency_marker: 0,
                criteria: vec![pb::Criterion {
                    names: vec!["NeverEmitted".into()],
                    tags: vec![Tag {
                        key: b"agg".to_vec(),
                        value: agg_id.clone().into_bytes(),
                    }],
                }],
            })
        } else {
            None
        };

        let req = pb::AppendRequest {
            condition,
            events: vec![tagged],
        };
        let stream = tokio_stream::iter(vec![req]);
        let out = tokio::select! {
            res = client.append(stream) => res,
            _ = kill_rx.changed() => return,
        };
        match out {
            Ok(resp) => {
                let resp = resp.into_inner();
                let rec = AckRecord {
                    aggregate_id: agg_id,
                    client_sequence: client_seq,
                    payload_hash: hash,
                    server_sequence: resp.first_sequence,
                };
                ack_log.record(rec).expect("ack sidecar");
                let _ = first_ack_tx.send(Some(node_idx));
                client_seq += 1;
            }
            Err(_status) => {
                client_opt = None;
                node_idx = (node_idx + 1) % 3;
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn post_restart_verify(
    _iter: usize,
    _tmp: &tempfile::TempDir,
    _acks_path: &std::path::Path,
    _acks_before_restart: usize,
    _srvs: &mut Vec<ServerHandle>,
    _leader_idx: usize,
    _listen: [u16; 3],
    _admin: [u16; 3],
    _peers_str: Vec<(u64, String)>,
) {
    panic!("post_restart_verify not implemented — Task 2 must fill this in");
}
