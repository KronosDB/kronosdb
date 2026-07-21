//! Follower-kill proof: losing a non-leader must not stall the cluster.
//!
//! Three voters, writes active. A follower (never the leader) is SIGKILLed
//! mid-commit. The 2-of-3 quorum must keep acknowledging appends while the
//! follower is down — measured, not assumed. The follower then restarts
//! against its data dir, catches up from its durable cursor, and all three
//! nodes must converge on the same head with every acknowledged event intact.

#![allow(clippy::result_large_err)]

mod crash_harness;
use crash_harness::pb::eventstore as pb;
use crash_harness::*;
use pb::Tag;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::watch;
use tonic::transport::Channel;

use pb::event_store_client::EventStoreClient;

const ITERATIONS: usize = 3;
const AGGREGATES: usize = 8;
const WRITER_TASKS: usize = 4;
const KILL_DELAY_MIN_MS: u64 = 50;
const KILL_DELAY_MAX_MS: u64 = 500;
/// Acks that must land while the follower is dead — proof the quorum survived.
const REQUIRED_ACKS_WHILE_DOWN: usize = 25;
const READY_TIMEOUT: Duration = Duration::from_secs(15);
const CONVERGENCE_TIMEOUT: Duration = Duration::from_secs(30);

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn crash_follower_three_iterations() {
    tokio::time::timeout(Duration::from_secs(300), async {
        for i in 0..ITERATIONS {
            eprintln!("=== crash_follower iteration {}/{ITERATIONS} ===", i + 1);
            run_one_iteration(i).await;
        }
    })
    .await
    .expect("crash_follower_three_iterations timed out");
}

async fn run_one_iteration(iter: usize) {
    let tmp = tempfile::tempdir().expect("tempdir");
    let acks_path = tmp.path().join("acks.log");
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
                backup: None,
            })
            .expect("spawn node"),
        );
    }
    for srv in &srvs {
        wait_until_ready(srv.listen, srv.admin, READY_TIMEOUT)
            .await
            .unwrap_or_else(|e| panic!("iter {iter}: node not ready: {e}"));
    }
    tokio::time::sleep(Duration::from_secs(2)).await;

    let ack_log = Arc::new(AckLog::create(acks_path.clone()).expect("ack log"));
    let (stop_tx, stop_rx) = watch::channel(false);
    let addrs: Vec<String> = (0..3)
        .map(|i| format!("http://127.0.0.1:{}", listen[i]))
        .collect();
    let mut writers = Vec::new();
    for widx in 0..WRITER_TASKS {
        writers.push(tokio::spawn(writer_loop(
            widx,
            addrs.clone(),
            Arc::clone(&ack_log),
            stop_rx.clone(),
        )));
    }

    // Wait for first acks, then identify the leader and kill a FOLLOWER.
    let ack_deadline = Instant::now() + Duration::from_secs(10);
    while ack_log.len() == 0 {
        assert!(
            Instant::now() < ack_deadline,
            "iter {iter}: no acks in 10s — cluster not functional"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let leader_idx = wait_for_raft_leader(&srvs, READY_TIMEOUT)
        .await
        .unwrap_or_else(|error| panic!("iter {iter}: {error}"));
    let follower_idx = (0..3)
        .find(|i| *i != leader_idx)
        .expect("two followers exist");
    let delay_ms = rand_in(KILL_DELAY_MIN_MS, KILL_DELAY_MAX_MS);
    tokio::time::sleep(Duration::from_millis(delay_ms)).await;
    let acks_at_kill = ack_log.len();
    eprintln!(
        "iter {iter}: SIGKILL follower=node{} (leader=node{}) after {delay_ms}ms; acks={acks_at_kill}",
        follower_idx + 1,
        leader_idx + 1
    );
    srvs[follower_idx].kill();

    // The quorum of two must keep acknowledging appends. This is the assert
    // that distinguishes "cluster degraded" from "cluster stalled".
    let progress_deadline = Instant::now() + Duration::from_secs(15);
    while ack_log.len() < acks_at_kill + REQUIRED_ACKS_WHILE_DOWN {
        assert!(
            Instant::now() < progress_deadline,
            "iter {iter}: appends stalled with follower down — {} acks at kill, {} after 15s",
            acks_at_kill,
            ack_log.len()
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    eprintln!(
        "iter {iter}: quorum kept committing ({} acks while follower down)",
        ack_log.len() - acks_at_kill
    );

    // Restart the follower against its data dir; it must catch up.
    let data_dir = tmp
        .path()
        .join(format!("node{}", follower_idx + 1))
        .join("data");
    srvs[follower_idx] = spawn_server(&SpawnConfig {
        data_dir,
        listen_port: listen[follower_idx],
        admin_port: admin[follower_idx],
        node_id: (follower_idx + 1) as u64,
        peers: peers.clone(),
        group_commit_ms: Some(2),
        segment_size: None,
        backup: None,
    })
    .expect("respawn follower");
    wait_until_ready(
        srvs[follower_idx].listen,
        srvs[follower_idx].admin,
        READY_TIMEOUT,
    )
    .await
    .unwrap_or_else(|e| panic!("iter {iter}: restarted follower not ready: {e}"));

    // Let writes continue across the rejoin, then stop.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let _ = stop_tx.send(true);
    for writer in writers {
        let _ = tokio::time::timeout(Duration::from_secs(3), writer).await;
    }
    let total_acks = ack_log.len();
    drop(ack_log);

    // Convergence: all three heads equal.
    let mut clients = Vec::new();
    for srv in srvs.iter() {
        let channel = Channel::from_shared(format!("http://{}", srv.listen))
            .unwrap()
            .connect()
            .await
            .expect("connect for convergence");
        clients.push(EventStoreClient::new(channel));
    }
    let start = Instant::now();
    loop {
        let mut heads = Vec::new();
        for client in clients.iter_mut() {
            heads.push(
                client
                    .get_head(pb::GetHeadRequest {})
                    .await
                    .expect("get_head")
                    .into_inner()
                    .sequence,
            );
        }
        if heads.iter().all(|h| *h == heads[0]) && heads[0] > 0 {
            eprintln!(
                "iter {iter}: converged at head={} after {:?}",
                heads[0],
                start.elapsed()
            );
            break;
        }
        assert!(
            start.elapsed() < CONVERGENCE_TIMEOUT,
            "iter {iter}: heads did not converge: {heads:?}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    // Verify: torn-free files, byte-identical logs, all acked events present.
    let acked = read_ack_sidecar(&acks_path).expect("read acks");
    assert_eq!(acked.len(), total_acks, "iter {iter}: ack sidecar drift");
    let mut reference_log: Option<Vec<(String, Vec<u8>)>> = None;
    for i in 0..3 {
        let data_dir = tmp.path().join(format!("node{}", i + 1)).join("data");
        let (_, _, scans) = scan_all_segments(&data_dir, "default").expect("scan");
        for scan in &scans {
            assert_eq!(
                scan.torn_tail_bytes,
                0,
                "iter {iter} node{}: torn tail in {}: {:?}",
                i + 1,
                scan.path.display(),
                scan.torn_reason
            );
        }
        let log = read_valid_event_log(&data_dir, "default").expect("read log");
        match &reference_log {
            None => reference_log = Some(log),
            Some(reference) => {
                assert_eq!(
                    reference.len(),
                    log.len(),
                    "iter {iter} node{}: segment count diverges",
                    i + 1
                );
                for ((n1, b1), (n2, b2)) in reference.iter().zip(log.iter()) {
                    assert_eq!(n1, n2, "iter {iter} node{}: file names diverge", i + 1);
                    assert_eq!(
                        b1,
                        b2,
                        "iter {iter} node{}: segment {n1} not byte-identical",
                        i + 1
                    );
                }
            }
        }
    }
    let events = source_all(&mut clients[follower_idx]).await;
    for rec in &acked {
        let payload = events.get(&rec.server_sequence).unwrap_or_else(|| {
            panic!(
                "iter {iter}: acked seq {} missing on restarted follower",
                rec.server_sequence
            )
        });
        assert_eq!(
            hash_payload(payload),
            rec.payload_hash,
            "iter {iter}: payload mismatch at seq {}",
            rec.server_sequence
        );
    }
    eprintln!(
        "iter {iter}: verified {} acked events on restarted follower",
        acked.len()
    );

    for srv in srvs.iter_mut() {
        srv.kill();
    }
}

async fn writer_loop(
    widx: usize,
    addrs: Vec<String>,
    ack_log: Arc<AckLog>,
    mut stop_rx: watch::Receiver<bool>,
) {
    let mut node_idx = 0;
    let mut client: Option<EventStoreClient<Channel>> = None;
    let mut client_seq: u64 = 0;
    loop {
        if *stop_rx.borrow_and_update() {
            return;
        }
        if client.is_none() {
            let endpoint = Channel::from_shared(addrs[node_idx].clone()).unwrap();
            match tokio::time::timeout(Duration::from_secs(1), endpoint.connect()).await {
                Ok(Ok(ch)) => client = Some(EventStoreClient::new(ch)),
                _ => {
                    node_idx = (node_idx + 1) % addrs.len();
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
            }
        }

        let agg_id = format!("agg-{}", (widx + client_seq as usize) % AGGREGATES);
        let payload = format!("f{widx}-s{client_seq}-{agg_id}").into_bytes();
        let hash = hash_payload(&payload);
        let request = pb::AppendRequest {
            condition: None,
            events: vec![pb::TaggedEvent {
                event: Some(pb::Event {
                    identifier: format!("evt-{widx}-{client_seq}"),
                    timestamp: 1_712_000_000_000 + client_seq as i64,
                    name: "Incremented".into(),
                    version: "1.0".into(),
                    payload,
                    metadata: Default::default(),
                }),
                tags: vec![Tag {
                    key: b"agg".to_vec(),
                    value: agg_id.clone().into_bytes(),
                }],
            }],
        };
        let result = tokio::select! {
            res = tokio::time::timeout(Duration::from_secs(5), client.as_mut().unwrap().append(request)) => res,
            _ = stop_rx.changed() => return,
        };
        match result {
            Ok(Ok(response)) => {
                ack_log
                    .record(AckRecord {
                        aggregate_id: agg_id,
                        client_sequence: client_seq,
                        payload_hash: hash,
                        server_sequence: response.into_inner().first_sequence,
                    })
                    .expect("ack sidecar");
                client_seq += 1;
            }
            _ => {
                client = None;
                node_idx = (node_idx + 1) % addrs.len();
            }
        }
    }
}

async fn source_all(client: &mut EventStoreClient<Channel>) -> HashMap<i64, Vec<u8>> {
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
    .expect("source rpc open timeout")
    .expect("source")
    .into_inner();
    let mut events = HashMap::new();
    loop {
        let msg = tokio::time::timeout(Duration::from_secs(15), stream.message())
            .await
            .expect("source stream timeout")
            .expect("source stream");
        let Some(resp) = msg else { break };
        if let Some(batch) = resp.batch {
            for ev in batch.events {
                events.insert(ev.sequence, ev.event.map(|e| e.payload).unwrap_or_default());
            }
        }
    }
    events
}
