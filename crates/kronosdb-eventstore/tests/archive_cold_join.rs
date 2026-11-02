//! A node joins a cluster whose leader has evicted its oldest segments
//! (ADR-0011). Node 1 runs alone with an archive and a cap on local bytes,
//! so most of its history is only in the archive. Node 2 starts empty, with
//! the same cap, and is adopted as a learner: the leader sends it to the
//! archive for what it no longer holds, the learner installs the segments
//! its cap would not keep as locators and key tables only, downloads the
//! rest, and then tails what follows. Every segment body it holds must be
//! the log's, every event readable on it, and it promotable once caught up.

mod crash_harness;
use crash_harness::*;

use std::collections::HashMap;
use std::time::{Duration, Instant};

const SEGMENT_SIZE: u64 = 64 * 1024;
const READY_TIMEOUT: Duration = Duration::from_secs(15);
const TIMEOUT: Duration = Duration::from_secs(60);

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_learner_joins_from_the_archive() {
    tokio::time::timeout(Duration::from_secs(240), run())
        .await
        .expect("a_learner_joins_from_the_archive timed out");
}

async fn run() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let evidence = EvidenceGuard::new("archive_cold_join", 0, tmp.path());
    let tier = tmp.path().join("tier");
    std::fs::create_dir_all(&tier).expect("mkdir tier");
    let url = url::Url::from_directory_path(&tier)
        .expect("tier url")
        .to_string();
    let ports: Vec<u16> = (0..4).map(|_| free_port()).collect();
    let (listen1, listen2, admin1, admin2) = (ports[0], ports[1], ports[2], ports[3]);

    // --- Node 1: alone, archiving, keeping two segments' worth on disk.
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
        archive: Some(ArchiveSpawn {
            url: url.clone(),
            interval_secs: 1,
            max_local_bytes: Some(2 * SEGMENT_SIZE),
        }),
        max_snapshot_size: None,
    })
    .expect("spawn node 1");
    wait_until_ready(srv1.listen, srv1.admin, READY_TIMEOUT)
        .await
        .expect("node 1 ready");

    let client1 = connect_port(listen1).await;
    for batch in 0..120 {
        append_batch(&client1, batch).await;
    }

    // Wait until most of node 1's history is only in the archive.
    let dir1 = data1.join("default");
    let deadline = Instant::now() + TIMEOUT;
    while local_segments(&dir1).len() > 4 || local_segments(&dir1).first() == Some(&0) {
        assert!(
            Instant::now() < deadline,
            "node 1 did not evict down to its cap: {:?}",
            local_segments(&dir1)
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    eprintln!(
        "archive_cold_join: node 1 holds segments {:?} locally",
        local_segments(&dir1)
    );

    // --- Node 2: empty, with the archive and no cap, adopted as a learner.
    let data2 = tmp.path().join("node2").join("data");
    std::fs::create_dir_all(&data2).expect("mkdir node2");
    let srv2 = spawn_server(&SpawnConfig {
        data_dir: data2.clone(),
        listen_port: listen2,
        admin_port: admin2,
        node_id: 2,
        peers: vec![
            (1u64, format!("127.0.0.1:{listen1}")),
            (2u64, format!("127.0.0.1:{listen2}")),
        ],
        group_commit_ms: Some(2),
        segment_size: Some(SEGMENT_SIZE),
        archive: Some(ArchiveSpawn {
            url: url.clone(),
            interval_secs: 1,
            max_local_bytes: Some(2 * SEGMENT_SIZE),
        }),
        max_snapshot_size: None,
    })
    .expect("spawn node 2");
    wait_for_tcp(srv2.listen, READY_TIMEOUT).await;
    let learner = format!("{{\"id\":2,\"addr\":\"127.0.0.1:{listen2}\"}}");
    let (status, body) = admin_post(srv1.admin, "/api/cluster/add-learner", &learner)
        .await
        .expect("add-learner");
    assert_eq!(status, 200, "add-learner failed: {body}");

    // Writes keep coming while it joins.
    for batch in 120..140 {
        append_batch(&client1, batch).await;
    }

    // --- The learner catches up: promotion opens once it has.
    let deadline = Instant::now() + TIMEOUT;
    loop {
        let (status, body) = admin_post(srv1.admin, "/api/cluster/add-voter", &learner)
            .await
            .expect("add-voter");
        if status == 200 {
            break;
        }
        assert_eq!(status, 409, "unexpected add-voter failure {status}: {body}");
        assert!(
            Instant::now() < deadline,
            "the learner did not catch up within {TIMEOUT:?}: {body}"
        );
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
    for batch in 140..150 {
        append_batch(&client1, batch).await;
    }
    let client2 = connect_port(listen2).await;
    let deadline = Instant::now() + TIMEOUT;
    loop {
        let (h1, h2) = (
            client1.head().await.expect("head 1"),
            client2.head().await.expect("head 2"),
        );
        if h1 == h2 {
            break;
        }
        assert!(Instant::now() < deadline, "heads differ: {h1} vs {h2}");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    // --- Every segment of node 2 is the log's: node 1's file, or the
    // archive's copy of one node 1 evicted.
    let log2 = read_valid_event_log(&data2, "default").expect("read node 2");
    let log1: HashMap<String, Vec<u8>> = read_valid_event_log(&data1, "default")
        .expect("read node 1")
        .into_iter()
        .collect();
    let mut from_archive = 0;
    for (name, bytes) in &log2 {
        let expected = match log1.get(name) {
            Some(bytes) => bytes.clone(),
            None => {
                from_archive += 1;
                std::fs::read(tier.join("default").join("segments").join(name))
                    .unwrap_or_else(|_| panic!("{name} is neither on node 1 nor archived"))
            }
        };
        assert!(bytes == &expected, "segment {name} differs on node 2");
    }
    // The oldest segments were installed without their bodies.
    let dir2 = data2.join("default");
    let bodiless = std::fs::read_dir(&dir2)
        .expect("list node 2")
        .filter_map(|entry| {
            let path = entry.ok()?.path();
            (path.extension()? == "idx" && !path.with_extension("seg").exists()).then_some(())
        })
        .count();
    assert!(
        bodiless > 0,
        "node 2 holds every body: nothing was installed without one"
    );

    // --- And every event reads the same on both.
    let events1 = source_all(&client1).await;
    let events2 = source_all(&client2).await;
    assert_eq!(events1.len(), 150 * 16);
    assert!(events1 == events2, "node 2 reads different events");
    eprintln!(
        "archive_cold_join: node 2 holds {} bodies ({from_archive} downloaded from the archive) \
         and {bodiless} segments without one",
        log2.len()
    );

    srv1.kill();
    let mut srv2 = srv2;
    srv2.kill();
    evidence.finish();
}

async fn append_batch(context: &Context, batch: u64) {
    let events: Vec<NewEvent> = (0..16)
        .map(|i| {
            event(
                format!("evt-{batch}-{i}"),
                1_712_000_000_000 + batch as i64,
                "Joined",
                "1.0",
                vec![b'j'; 300],
                vec![(b"agg".to_vec(), format!("agg-{}", batch % 4).into_bytes())],
            )
        })
        .collect();
    // A leader installing a new epoch (after a promotion) asks for a retry.
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        match context.append(&events, None).await {
            Ok(_) => return,
            Err(error) if Instant::now() < deadline => {
                eprintln!("archive_cold_join: append retried: {error:?}");
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err(error) => panic!("append: {error:?}"),
        }
    }
}

fn local_segments(dir: &std::path::Path) -> Vec<u64> {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return Vec::new();
    };
    let mut bases: Vec<u64> = entries
        .filter_map(|entry| {
            let path = entry.ok()?.path();
            (path.extension()? == "seg").then(|| path.file_stem()?.to_str()?.parse().ok())?
        })
        .collect();
    bases.sort_unstable();
    bases
}

async fn wait_for_tcp(addr: std::net::SocketAddr, timeout: Duration) {
    let start = Instant::now();
    while tokio::net::TcpStream::connect(addr).await.is_err() {
        assert!(start.elapsed() < timeout, "no TCP listener on {addr}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn connect_port(port: u16) -> Context {
    connect(format!("127.0.0.1:{port}"))
        .await
        .expect("connect")
        .default_context()
}

async fn source_all(context: &Context) -> HashMap<u64, Vec<u8>> {
    let criteria = tag_criteria("agg", (0..4).map(|a| format!("agg-{a}")));
    tokio::time::timeout(Duration::from_secs(15), read_events(context, &criteria))
        .await
        .expect("source timeout")
        .expect("source")
        .into_iter()
        .map(|(position, _, payload)| (position, payload))
        .collect()
}
