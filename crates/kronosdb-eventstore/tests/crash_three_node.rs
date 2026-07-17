//! Three-node kill-mid-commit failover and recovery proof.
//!
//! The leader is killed while writes are active, the remaining voters elect a
//! new leader, and writes continue. The killed node then restarts, catches up,
//! and all three nodes must converge on byte-identical valid segment prefixes.

#![allow(clippy::result_large_err)]

mod crash_harness;
use crash_harness::pb::eventstore as pb;
use crash_harness::*;
use pb::Tag;

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::watch;
use tonic::transport::Channel;

use pb::event_store_client::EventStoreClient;

const ITERATIONS: usize = 10;
const AGGREGATES: usize = 8;
const WRITER_TASKS: usize = 4;
const KILL_DELAY_MIN_MS: u64 = 50;
const KILL_DELAY_MAX_MS: u64 = 500;
const READY_TIMEOUT: Duration = Duration::from_secs(15);
const CONVERGENCE_TIMEOUT: Duration = Duration::from_secs(30);

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
        wait_until_ready(srv.listen, srv.admin, READY_TIMEOUT)
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

    // Wait for the workload to become active, then identify the actual metadata
    // leader rather than the follower through which the first append was sent.
    let _first_request_target = {
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
    let leader_idx = wait_for_raft_leader(&srvs, READY_TIMEOUT)
        .await
        .unwrap_or_else(|error| panic!("iter {iter}: {error}"));
    let delay_ms = rand_in(KILL_DELAY_MIN_MS, KILL_DELAY_MAX_MS);
    tokio::time::sleep(Duration::from_millis(delay_ms)).await;
    eprintln!(
        "iter {iter}: SIGKILL leader=node{} after {delay_ms}ms; acks so far = {}",
        leader_idx + 1,
        ack_log.len()
    );
    srvs[leader_idx].kill();

    let new_leader = wait_for_raft_leader(&srvs, READY_TIMEOUT)
        .await
        .unwrap_or_else(|error| panic!("iter {iter}: failover did not elect: {error}"));
    assert_ne!(new_leader, leader_idx);
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Stop writers after they have crossed the election and resumed through
    // the surviving claimed leader.
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
        let condition = if !client_seq.is_multiple_of(3) {
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
    iter: usize,
    tmp: &tempfile::TempDir,
    acks_path: &std::path::Path,
    acks_before_restart: usize,
    srvs: &mut [ServerHandle],
    leader_idx: usize,
    listen: [u16; 3],
    admin: [u16; 3],
    peers_str: Vec<(u64, String)>,
) {
    // The survivors already elected and accepted post-failover writes; restart
    // the killed node and require it to converge from its durable cursor.

    // Restart the killed node against its same data dir.
    let killed_data_dir = tmp
        .path()
        .join(format!("node{}", leader_idx + 1))
        .join("data");
    let cfg = SpawnConfig {
        data_dir: killed_data_dir.clone(),
        listen_port: listen[leader_idx],
        admin_port: admin[leader_idx],
        node_id: (leader_idx + 1) as u64,
        peers: peers_str.clone(),
        group_commit_ms: Some(2),
    };
    srvs[leader_idx] = spawn_server(&cfg).expect("respawn killed node");
    wait_until_ready(
        srvs[leader_idx].listen,
        srvs[leader_idx].admin,
        READY_TIMEOUT,
    )
    .await
    .expect("restarted node ready");

    // Grace for follower catch-up.
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Read ack sidecar (shared helper from crash_harness).
    let acked = read_ack_sidecar(acks_path).expect("read acks");
    assert_eq!(
        acked.len(),
        acks_before_restart,
        "iter {iter}: sidecar drift"
    );

    // Wait for cross-node native-log convergence after the restarted node
    // reconnects from its durable physical cursor.
    {
        let start = Instant::now();
        let mut clients: Vec<EventStoreClient<Channel>> = Vec::new();
        for srv in srvs.iter() {
            let addr = format!("http://{}", srv.listen);
            let channel = Channel::from_shared(addr)
                .unwrap()
                .connect()
                .await
                .expect("connect for convergence poll");
            clients.push(EventStoreClient::new(channel));
        }
        let mut last_log = Instant::now();
        loop {
            let mut heads = Vec::new();
            for c in clients.iter_mut() {
                let h = c
                    .get_head(pb::GetHeadRequest {})
                    .await
                    .expect("get_head")
                    .into_inner()
                    .sequence;
                heads.push(h);
            }
            if heads.iter().all(|h| *h == heads[0]) && heads[0] > 1 {
                eprintln!(
                    "iter {iter}: converged at head={} (after {:?})",
                    heads[0],
                    start.elapsed()
                );
                break;
            }
            if last_log.elapsed() > Duration::from_secs(2) {
                eprintln!(
                    "iter {iter}: waiting on convergence... heads={:?} (elapsed {:?})",
                    heads,
                    start.elapsed()
                );
                last_log = Instant::now();
            }
            if start.elapsed() > CONVERGENCE_TIMEOUT {
                // Print per-node log/event segment counts for diagnosis.
                for i in 0..3 {
                    let data_dir = tmp.path().join(format!("node{}", i + 1)).join("data");
                    if let Ok((log_rs, event_rs, _)) = scan_all_segments(&data_dir, "default") {
                        eprintln!(
                            "iter {iter} node{}: raft log records={}, event records={}",
                            i + 1,
                            log_rs,
                            event_rs
                        );
                    }
                }
                panic!(
                    "iter {iter}: heads did not converge within {:?}: {:?} — CRASH-02/CRASH-03 VIOLATION",
                    CONVERGENCE_TIMEOUT, heads
                );
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    // For EACH node: scan segments + Source-All + record head.
    let mut per_node_events: Vec<HashMap<i64, Vec<u8>>> = Vec::new();
    let mut per_node_head: Vec<i64> = Vec::new();
    let mut per_node_log_bytes = Vec::new();

    for (i, srv) in srvs.iter().enumerate() {
        // Raw CRC scan (CRASH-02 torn detection, per-node).
        let data_dir = tmp.path().join(format!("node{}", i + 1)).join("data");
        let (log_rs, event_rs, scans) =
            scan_all_segments(&data_dir, "default").expect("scan segments");
        for s in &scans {
            assert_eq!(
                s.torn_tail_bytes,
                0,
                "iter {iter} node{}: POST-RESTART torn tail in {}: {} bytes ({:?}) — CRASH-02 VIOLATION",
                i + 1,
                s.path.display(),
                s.torn_tail_bytes,
                s.torn_reason
            );
        }
        eprintln!(
            "iter {iter} node{}: CRC scan clean ({} metadata records, {} event records)",
            i + 1,
            log_rs,
            event_rs
        );
        per_node_log_bytes
            .push(read_valid_event_log(&data_dir, "default").expect("read valid event log bytes"));

        // Connect + Source-All via per-aggregate criteria (empty criteria returns
        // nothing in this DCB engine; union one criterion per aggregate mirrors
        // the 1-node test's read pattern).
        let addr = format!("http://{}", srv.listen);
        let channel = Channel::from_shared(addr)
            .unwrap()
            .connect()
            .await
            .unwrap_or_else(|e| panic!("iter {iter}: connect node{}: {e}", i + 1));
        let mut client = EventStoreClient::new(channel);

        // Head was already waited-on above in the convergence loop; this is
        // a direct read now.
        let head = client
            .get_head(pb::GetHeadRequest {})
            .await
            .expect("get_head")
            .into_inner()
            .sequence;
        per_node_head.push(head);
        eprintln!("iter {iter} node{}: head={head}", i + 1);

        let criteria: Vec<pb::Criterion> = (0..AGGREGATES)
            .map(|a| pb::Criterion {
                names: vec![],
                tags: vec![Tag {
                    key: b"agg".to_vec(),
                    value: format!("agg-{a}").into_bytes(),
                }],
            })
            .collect();
        let mut stream = tokio::time::timeout(
            Duration::from_secs(15),
            client.source(pb::SourceRequest {
                from_sequence: 0,
                criteria,
                batch_size: 0,
            }),
        )
        .await
        .unwrap_or_else(|_| panic!("iter {iter} node{}: source rpc open timeout", i + 1))
        .expect("source")
        .into_inner();
        let mut events: HashMap<i64, Vec<u8>> = HashMap::new();
        loop {
            let msg = tokio::time::timeout(Duration::from_secs(15), stream.message())
                .await
                .unwrap_or_else(|_| {
                    panic!(
                        "iter {iter} node{}: source stream message timeout (events so far = {})",
                        i + 1,
                        events.len()
                    )
                })
                .expect("stream msg");
            let Some(resp) = msg else { break };
            if let Some(pb::source_response::Result::Batch(batch)) = resp.result {
                for ev in batch.events {
                    let payload = ev.event.map(|e| e.payload).unwrap_or_default();
                    events.insert(ev.sequence, payload);
                }
            }
        }
        eprintln!(
            "iter {iter} node{}: Source returned {} events",
            i + 1,
            events.len()
        );
        per_node_events.push(events);
    }

    // Cross-node head convergence.
    let head0 = per_node_head[0];
    for (i, h) in per_node_head.iter().enumerate() {
        assert_eq!(
            *h,
            head0,
            "iter {iter}: head divergence — node{} head = {}, node1 head = {} (CRASH-02 PHANTOM / CRASH-03 DIVERGENCE)",
            i + 1,
            h,
            head0
        );
    }

    // Cross-node event-set equality.
    let keys0: BTreeSet<i64> = per_node_events[0].keys().copied().collect();
    for (i, events) in per_node_events.iter().enumerate() {
        let keys: BTreeSet<i64> = events.keys().copied().collect();
        assert_eq!(
            keys,
            keys0,
            "iter {iter}: event-set divergence — node{} keys differ from node1",
            i + 1
        );
        for k in &keys {
            assert_eq!(
                events.get(k),
                per_node_events[0].get(k),
                "iter {iter}: payload divergence — event at seq={} differs between node1 and node{}",
                k,
                i + 1
            );
        }
    }

    // The native segment log is the replicated authority: segment boundaries,
    // control records, framing, CRCs, and event bytes must converge exactly.
    for (i, bytes) in per_node_log_bytes.iter().enumerate().skip(1) {
        assert_eq!(
            bytes,
            &per_node_log_bytes[0],
            "iter {iter}: byte-exact event log divergence between node1 and node{}",
            i + 1
        );
    }

    // CRASH-03 under replication: every acked write present on EVERY node.
    for rec in &acked {
        for (i, events) in per_node_events.iter().enumerate() {
            let payload = events.get(&rec.server_sequence).unwrap_or_else(|| {
                panic!(
                    "iter {iter} node{}: acked write at server_sequence={} MISSING — CRASH-03 VIOLATION",
                    i + 1,
                    rec.server_sequence
                )
            });
            let got_hash = hash_payload(payload);
            assert_eq!(
                got_hash,
                rec.payload_hash,
                "iter {iter} node{}: payload hash mismatch at seq={} — CRASH-03 CONTENT VIOLATION",
                i + 1,
                rec.server_sequence
            );
        }
    }

    // A leader isolated from both followers may write an uncommitted suffix,
    // but it must never acknowledge it without a fresh durable quorum.
    let isolated_leader = wait_for_raft_leader(srvs, READY_TIMEOUT)
        .await
        .expect("leader before quorum-loss check");
    for (index, server) in srvs.iter_mut().enumerate() {
        if index != isolated_leader {
            server.kill();
        }
    }
    let channel = Channel::from_shared(format!("http://127.0.0.1:{}", listen[isolated_leader]))
        .unwrap()
        .connect()
        .await
        .expect("connect isolated leader");
    let mut client = EventStoreClient::new(channel);
    let request = pb::AppendRequest {
        condition: None,
        events: vec![pb::TaggedEvent {
            event: Some(pb::Event {
                identifier: format!("quorum-loss-{iter}"),
                timestamp: 1_712_999_999_999,
                name: "MustNotAck".into(),
                version: "1.0".into(),
                payload: b"uncommitted".to_vec(),
                metadata: Default::default(),
            }),
            tags: vec![],
        }],
    };
    let result = tokio::time::timeout(
        Duration::from_secs(2),
        client.append(tokio_stream::iter([request])),
    )
    .await;
    assert!(
        !matches!(result, Ok(Ok(_))),
        "iter {iter}: isolated leader acknowledged without a quorum"
    );

    eprintln!(
        "iter {iter}: OK  acked={}, readable={}, head={}",
        acked.len(),
        per_node_events[0].len(),
        head0
    );
}
