//! The leader is killed while it archives (ADR-0011). Three nodes archive to
//! one object store, writes keep coming, and the leader dies between passes
//! or in the middle of one. The new leader must carry on where the archive
//! ends: every archived segment committed by its summary, the summaries one
//! unbroken chain from position 0, and every archived body the log's bytes.

mod crash_harness;
use crash_harness::*;

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

const SEGMENT_SIZE: u64 = 64 * 1024;
const READY_TIMEOUT: Duration = Duration::from_secs(15);
const TIMEOUT: Duration = Duration::from_secs(60);

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_new_leader_archives_on_where_the_old_one_stopped() {
    tokio::time::timeout(Duration::from_secs(240), run())
        .await
        .expect("a_new_leader_archives_on_where_the_old_one_stopped timed out");
}

async fn run() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let evidence = EvidenceGuard::new("archive_failover", 0, tmp.path());
    let tier = tmp.path().join("tier");
    std::fs::create_dir_all(&tier).expect("mkdir tier");
    let url = url::Url::from_directory_path(&tier)
        .expect("tier url")
        .to_string();
    let segments = tier.join("default").join("segments");

    let ports: Vec<u16> = (0..6).map(|_| free_port()).collect();
    let peers: Vec<(u64, String)> = (0..3)
        .map(|i| (i as u64 + 1, format!("127.0.0.1:{}", ports[i])))
        .collect();
    let mut servers = Vec::new();
    let mut data_dirs = Vec::new();
    for i in 0..3 {
        let data_dir = tmp.path().join(format!("node{}", i + 1)).join("data");
        std::fs::create_dir_all(&data_dir).expect("mkdir node");
        servers.push(
            spawn_server(&SpawnConfig {
                data_dir: data_dir.clone(),
                listen_port: ports[i],
                admin_port: ports[i + 3],
                node_id: i as u64 + 1,
                peers: peers.clone(),
                group_commit_ms: Some(2),
                segment_size: Some(SEGMENT_SIZE),
                archive: Some(ArchiveSpawn {
                    url: url.clone(),
                    interval_secs: 1,
                    max_local_bytes: None,
                }),
                max_snapshot_size: None,
            })
            .expect("spawn node"),
        );
        data_dirs.push(data_dir);
    }
    for server in &servers {
        wait_until_ready(server.listen, server.admin, READY_TIMEOUT)
            .await
            .expect("node ready");
    }

    // Writes keep coming throughout, through whichever node answers.
    let stop = Arc::new(AtomicBool::new(false));
    let written = Arc::new(AtomicU64::new(0));
    let addrs: Vec<String> = (0..3).map(|i| format!("127.0.0.1:{}", ports[i])).collect();
    let writer = tokio::spawn(write_until(addrs, Arc::clone(&stop), Arc::clone(&written)));

    // The leader archives a while, then dies mid-stream.
    wait_for(&segments, 6).await;
    let leader = wait_for_raft_leader(&servers, READY_TIMEOUT)
        .await
        .expect("a leader");
    let before = summaries(&segments).len();
    eprintln!(
        "archive_failover: killing node {} with {before} segments archived, {} batches written",
        leader + 1,
        written.load(Ordering::Relaxed)
    );
    servers[leader].kill();

    // The new leader archives on.
    wait_for(&segments, before + 6).await;
    stop.store(true, Ordering::Relaxed);
    writer.await.expect("writer");
    let survivors: Vec<usize> = (0..3).filter(|&i| i != leader).collect();
    let new_leader = {
        let deadline = Instant::now() + TIMEOUT;
        loop {
            let mut found = None;
            for &i in &survivors {
                let lag = metric(
                    servers[i].admin,
                    "kronosdb_archive_lag_events{context=\"default\"}",
                )
                .await;
                if lag == Some(0) {
                    found = Some(i);
                }
            }
            if let Some(i) = found {
                break i;
            }
            assert!(
                Instant::now() < deadline,
                "no survivor reports an archive caught up"
            );
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    };
    eprintln!(
        "archive_failover: node {} archives now; {} segments archived",
        new_leader + 1,
        summaries(&segments).len()
    );

    // One unbroken chain of summaries from 0, each over the log's bytes.
    let log: HashMap<String, Vec<u8>> = read_valid_event_log(&data_dirs[new_leader], "default")
        .expect("read the new leader's log")
        .into_iter()
        .collect();
    let chain = summaries(&segments);
    let mut next = 0;
    for (base, summary) in &chain {
        assert_eq!(*base, next, "the archive skips from {next} to {base}");
        let name = format!("{base:020}.seg");
        let body = std::fs::read(segments.join(&name)).expect("an archived body");
        assert_eq!(
            body.len() as u64,
            summary["data_len"].as_u64().unwrap(),
            "{name}"
        );
        assert_eq!(
            blake3::hash(&body).to_hex().as_str(),
            summary["blake3"].as_str().unwrap(),
            "{name} is not what its summary says"
        );
        assert!(
            log.get(&name) == Some(&body),
            "archived {name} is not the new leader's segment"
        );
        next = summary["end"].as_u64().unwrap();
    }
    assert!(chain.len() > before, "nothing archived after the failover");

    for server in &mut servers {
        server.kill();
    }
    evidence.finish();
}

/// The archived summaries, by base.
fn summaries(dir: &std::path::Path) -> Vec<(u64, serde_json::Value)> {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return Vec::new();
    };
    let mut out: Vec<(u64, serde_json::Value)> = entries
        .filter_map(|entry| {
            let path = entry.ok()?.path();
            if path.extension()? != "sum" {
                return None;
            }
            let base = path.file_stem()?.to_str()?.parse().ok()?;
            let summary = serde_json::from_slice(&std::fs::read(&path).ok()?).ok()?;
            Some((base, summary))
        })
        .collect();
    out.sort_by_key(|(base, _)| *base);
    out
}

async fn wait_for(dir: &std::path::Path, count: usize) {
    let deadline = Instant::now() + TIMEOUT;
    while summaries(dir).len() < count {
        assert!(
            Instant::now() < deadline,
            "the archive holds {} segments, waiting for {count}",
            summaries(dir).len()
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn write_until(addrs: Vec<String>, stop: Arc<AtomicBool>, written: Arc<AtomicU64>) {
    let mut target = 0;
    let mut context = None;
    let mut batch = 0u64;
    while !stop.load(Ordering::Relaxed) {
        if context.is_none() {
            context = connect(addrs[target].clone())
                .await
                .ok()
                .map(|client| client.default_context());
        }
        let Some(ctx) = &context else {
            target = (target + 1) % addrs.len();
            tokio::time::sleep(Duration::from_millis(100)).await;
            continue;
        };
        let events: Vec<NewEvent> = (0..16)
            .map(|i| {
                event(
                    format!("evt-{batch}-{i}"),
                    1_712_000_000_000 + batch as i64,
                    "Archived",
                    "1.0",
                    vec![b'a'; 300],
                    vec![(b"agg".to_vec(), format!("agg-{}", batch % 4).into_bytes())],
                )
            })
            .collect();
        match ctx.append(&events, None).await {
            Ok(_) => {
                batch += 1;
                written.store(batch, Ordering::Relaxed);
            }
            Err(_) => {
                // The node died or lost its leader: try the next one.
                context = None;
                target = (target + 1) % addrs.len();
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
}
