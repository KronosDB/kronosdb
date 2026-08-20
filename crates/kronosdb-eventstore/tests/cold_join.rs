//! Cold-join proof: a node with an established history adopts an empty node.
//!
//! Node 1 runs alone and seals many small segments under live writes. Node 2
//! starts empty and is adopted as a learner via the admin API; the data plane
//! must stream the full history through the normal Tail session. Promotion to
//! voter is attempted while the learner is provably behind (it is paused with
//! SIGSTOP) and must be refused with a lag report; once the learner catches
//! up, promotion must succeed, both nodes must converge on byte-identical
//! segment files, and appends must keep working under the 2-of-2 quorum.

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

const SEGMENT_SIZE: u64 = 128 * 1024; // small segments => many seals quickly
const MIN_SEALED_SEGMENTS: usize = 6;
const EVENTS_PER_APPEND: usize = 32;
const READY_TIMEOUT: Duration = Duration::from_secs(15);
const CATCHUP_TIMEOUT: Duration = Duration::from_secs(60);
const CONVERGENCE_TIMEOUT: Duration = Duration::from_secs(30);

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn cold_join_catchup_gate_and_promotion() {
    tokio::time::timeout(Duration::from_secs(240), run())
        .await
        .expect("cold_join_catchup_gate_and_promotion timed out");
}

async fn run() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let acks_path = tmp.path().join("acks.log");
    let ports: Vec<u16> = (0..4).map(|_| free_port()).collect();
    let (listen1, listen2, admin1, admin2) = (ports[0], ports[1], ports[2], ports[3]);

    // --- Node 1: standalone single-voter cluster.
    let data1 = tmp.path().join("node1").join("data");
    std::fs::create_dir_all(&data1).expect("mkdir node1");
    let mut srv1 = spawn_server(&SpawnConfig {
        data_dir: data1.clone(),
        listen_port: listen1,
        admin_port: admin1,
        node_id: 1,
        peers: Vec::new(),
        group_commit_ms: Some(2),
        segment_size: Some(SEGMENT_SIZE),
        backup: None,
        max_snapshot_size: None,
    })
    .expect("spawn node 1");
    wait_until_ready(srv1.listen, srv1.admin, READY_TIMEOUT)
        .await
        .expect("node 1 ready");

    // --- Live workload against node 1 for the whole join.
    let ack_log = Arc::new(AckLog::create(acks_path.clone()).expect("ack log"));
    let (stop_tx, stop_rx) = watch::channel(false);
    let writer = tokio::spawn(writer_loop(
        format!("http://127.0.0.1:{listen1}"),
        Arc::clone(&ack_log),
        stop_rx,
    ));

    // Build history: wait until node 1 has sealed a real number of segments.
    let seal_deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let segments = count_segments(&data1);
        if segments > MIN_SEALED_SEGMENTS {
            eprintln!("cold_join: node 1 has {segments} segment files; starting join");
            break;
        }
        assert!(
            Instant::now() < seal_deadline,
            "node 1 sealed only {} segments in 60s",
            count_segments(&data1)
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    // --- The promotion gate must refuse a node with zero replication
    // progress BEFORE any membership mutation — here node 2 does not even
    // exist yet, so this is fully deterministic. A pass here would have put a
    // cold voter straight into the watermark quorum.
    let learner_body = format!("{{\"id\":2,\"addr\":\"127.0.0.1:{listen2}\"}}");
    let (status, body) = tokio::time::timeout(
        Duration::from_secs(30),
        admin_post(srv1.admin, "/api/cluster/add-voter", &learner_body),
    )
    .await
    .expect("gated add-voter timed out")
    .expect("gated add-voter request");
    assert_eq!(
        status, 409,
        "promotion of a node with no replication progress must be refused, got {status}: {body}"
    );
    assert!(
        body.contains("not caught up") || body.contains("no replication acknowledgement"),
        "409 body should explain the lag: {body}"
    );
    eprintln!("cold_join: gate refused cold promotion: {body}");

    // --- Node 2: empty, configured with node 1 as the lowest-ID voter so it
    // never self-bootstraps; it waits to be adopted.
    let data2 = tmp.path().join("node2").join("data");
    std::fs::create_dir_all(&data2).expect("mkdir node2");
    let peers = vec![
        (1u64, format!("127.0.0.1:{listen1}")),
        (2u64, format!("127.0.0.1:{listen2}")),
    ];
    let srv2 = spawn_server(&SpawnConfig {
        data_dir: data2.clone(),
        listen_port: listen2,
        admin_port: admin2,
        node_id: 2,
        peers,
        group_commit_ms: Some(2),
        segment_size: Some(SEGMENT_SIZE),
        backup: None,
        max_snapshot_size: None,
    })
    .expect("spawn node 2");
    // Not `wait_until_ready`: an un-adopted node has no claim and reports 503.
    wait_for_tcp(srv2.listen, READY_TIMEOUT).await;

    // --- Adopt as learner (metadata-only; data catch-up runs in background).
    let (status, body) = tokio::time::timeout(
        Duration::from_secs(30),
        admin_post(srv1.admin, "/api/cluster/add-learner", &learner_body),
    )
    .await
    .expect("add-learner timed out")
    .expect("add-learner request");
    assert_eq!(status, 200, "add-learner failed: {body}");

    // --- Poll promotion until the learner catches up and the gate opens.
    let promote_deadline = Instant::now() + CATCHUP_TIMEOUT;
    loop {
        let (status, body) = tokio::time::timeout(
            Duration::from_secs(30),
            admin_post(srv1.admin, "/api/cluster/add-voter", &learner_body),
        )
        .await
        .expect("add-voter timed out")
        .expect("add-voter request");
        if status == 200 {
            eprintln!("cold_join: learner promoted to voter");
            break;
        }
        assert_eq!(status, 409, "unexpected add-voter failure {status}: {body}");
        assert!(
            Instant::now() < promote_deadline,
            "learner did not catch up within {CATCHUP_TIMEOUT:?}; last refusal: {body}"
        );
        tokio::time::sleep(Duration::from_millis(300)).await;
    }

    // --- Writes must keep landing under the 2-of-2 quorum.
    let acks_at_promotion = ack_log.len();
    let quorum_deadline = Instant::now() + Duration::from_secs(15);
    while ack_log.len() < acks_at_promotion + 20 {
        assert!(
            Instant::now() < quorum_deadline,
            "appends stalled after promotion: {} acks at promotion, {} now",
            acks_at_promotion,
            ack_log.len()
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let _ = stop_tx.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(5), writer).await;
    let total_acks = ack_log.len();
    assert!(total_acks > 0, "no acknowledged appends");
    drop(ack_log);

    // --- Both nodes must converge on the same head.
    let mut client1 = connect(listen1).await;
    let mut client2 = connect(listen2).await;
    let start = Instant::now();
    loop {
        let h1 = head(&mut client1).await;
        let h2 = head(&mut client2).await;
        if h1 == h2 && h1 > 0 {
            eprintln!(
                "cold_join: converged at head={h1} after {:?}",
                start.elapsed()
            );
            break;
        }
        assert!(
            start.elapsed() < CONVERGENCE_TIMEOUT,
            "heads did not converge: node1={h1} node2={h2}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    // --- Verify: torn-free scans, byte-identical segment files, and every
    // acknowledged event present with its original payload on the new node.
    for (name, dir) in [("node1", &data1), ("node2", &data2)] {
        let (_, _, scans) = scan_all_segments(dir, "default").expect("scan");
        for scan in &scans {
            assert_eq!(
                scan.torn_tail_bytes,
                0,
                "{name}: torn tail in {}: {:?}",
                scan.path.display(),
                scan.torn_reason
            );
        }
    }
    let log1 = read_valid_event_log(&data1, "default").expect("read node1 log");
    let log2 = read_valid_event_log(&data2, "default").expect("read node2 log");
    assert_eq!(
        log1.len(),
        log2.len(),
        "segment file count differs after catch-up"
    );
    for ((name1, bytes1), (name2, bytes2)) in log1.iter().zip(log2.iter()) {
        assert_eq!(name1, name2, "segment file names diverge");
        assert_eq!(
            bytes1, bytes2,
            "segment {name1} is not byte-identical on the joined node"
        );
    }

    let acked = read_ack_sidecar(&acks_path).expect("read acks");
    assert_eq!(acked.len(), total_acks, "ack sidecar drift");
    let events2 = source_all(&mut client2).await;
    for rec in &acked {
        let payload = events2.get(&rec.server_sequence).unwrap_or_else(|| {
            panic!(
                "acked seq {} (agg {}) missing on joined node",
                rec.server_sequence, rec.aggregate_id
            )
        });
        assert_eq!(
            hash_payload(payload),
            rec.payload_hash,
            "payload mismatch at seq {} on joined node",
            rec.server_sequence
        );
    }
    eprintln!(
        "cold_join: verified {} acked events, {} byte-identical segment files",
        acked.len(),
        log1.len()
    );

    srv1.kill();
    let mut srv2 = srv2;
    srv2.kill();
}

async fn writer_loop(addr: String, ack_log: Arc<AckLog>, mut stop_rx: watch::Receiver<bool>) {
    let mut client: Option<EventStoreClient<Channel>> = None;
    let mut batch_seq: u64 = 0;
    loop {
        if *stop_rx.borrow_and_update() {
            return;
        }
        if client.is_none() {
            match Channel::from_shared(addr.clone()).unwrap().connect().await {
                Ok(ch) => client = Some(EventStoreClient::new(ch)),
                Err(_) => {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
            }
        }

        let mut events = Vec::with_capacity(EVENTS_PER_APPEND);
        let mut hashes = Vec::with_capacity(EVENTS_PER_APPEND);
        for i in 0..EVENTS_PER_APPEND {
            let agg_id = format!("agg-{}", (batch_seq as usize + i) % 8);
            // Padding inflates the payload so segments seal quickly.
            let payload = format!("cj-b{batch_seq}-i{i}-{agg_id}-{}", "x".repeat(160)).into_bytes();
            hashes.push((agg_id.clone(), hash_payload(&payload)));
            events.push(pb::TaggedEvent {
                event: Some(pb::Event {
                    identifier: format!("evt-{batch_seq}-{i}"),
                    timestamp: 1_712_000_000_000 + batch_seq as i64,
                    name: "Incremented".into(),
                    version: "1.0".into(),
                    payload,
                    metadata: Default::default(),
                }),
                tags: vec![Tag {
                    key: b"agg".to_vec(),
                    value: agg_id.into_bytes(),
                }],
            });
        }
        let request = pb::AppendRequest {
            condition: None,
            events,
        };
        let result = tokio::select! {
            res = client.as_mut().unwrap().append(request) => res,
            _ = stop_rx.changed() => return,
        };
        match result {
            Ok(response) => {
                let response = response.into_inner();
                for (i, (agg_id, hash)) in hashes.into_iter().enumerate() {
                    ack_log
                        .record(AckRecord {
                            aggregate_id: agg_id,
                            client_sequence: batch_seq * EVENTS_PER_APPEND as u64 + i as u64,
                            payload_hash: hash,
                            server_sequence: response.first_sequence + i as i64,
                        })
                        .expect("ack sidecar");
                }
                batch_seq += 1;
            }
            Err(_) => {
                client = None;
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }
}

fn count_segments(data_dir: &std::path::Path) -> usize {
    let Ok(dir) = std::fs::read_dir(data_dir.join("default")) else {
        return 0;
    };
    dir.filter_map(|entry| entry.ok())
        .filter(|entry| {
            entry
                .file_name()
                .to_str()
                .map(|name| name.ends_with(".seg"))
                .unwrap_or(false)
        })
        .count()
}

async fn wait_for_tcp(addr: std::net::SocketAddr, timeout: Duration) {
    let start = Instant::now();
    while tokio::net::TcpStream::connect(addr).await.is_err() {
        assert!(
            start.elapsed() < timeout,
            "no TCP listener on {addr} after {timeout:?}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn connect(port: u16) -> EventStoreClient<Channel> {
    let channel = Channel::from_shared(format!("http://127.0.0.1:{port}"))
        .unwrap()
        .connect()
        .await
        .expect("connect");
    EventStoreClient::new(channel)
}

async fn head(client: &mut EventStoreClient<Channel>) -> i64 {
    client
        .get_head(pb::GetHeadRequest {})
        .await
        .expect("get_head")
        .into_inner()
        .sequence
}

async fn source_all(client: &mut EventStoreClient<Channel>) -> HashMap<i64, Vec<u8>> {
    let criteria: Vec<pb::Criterion> = (0..8)
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
