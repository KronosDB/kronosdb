//! The archive tier through the production server (ADR-0011): env-var
//! config, leader gating, the periodic pass, and byte-exact archived
//! segments committed by their summaries.

mod crash_harness;
use crash_harness::*;

use std::time::{Duration, Instant};

const SEGMENT_SIZE: u64 = 64 * 1024;
const READY_TIMEOUT: Duration = Duration::from_secs(15);
const ARCHIVE_TIMEOUT: Duration = Duration::from_secs(30);

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_leader_archives_final_segments_end_to_end() {
    tokio::time::timeout(Duration::from_secs(120), run())
        .await
        .expect("the_leader_archives_final_segments_end_to_end timed out");
}

async fn run() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let data_dir = tmp.path().join("data");
    let tier_dir = tmp.path().join("tier");
    std::fs::create_dir_all(&data_dir).expect("mkdir data");
    std::fs::create_dir_all(&tier_dir).expect("mkdir tier");
    let archive_url = url::Url::from_directory_path(&tier_dir)
        .expect("tier url")
        .to_string();

    let mut srv = spawn_server(&SpawnConfig {
        data_dir: data_dir.clone(),
        listen_port: free_port(),
        admin_port: free_port(),
        node_id: 1,
        peers: Vec::new(),
        group_commit_ms: Some(2),
        segment_size: Some(SEGMENT_SIZE),
        archive: Some((archive_url, 1)),
        max_snapshot_size: None,
    })
    .expect("spawn server");
    wait_until_ready(srv.listen, srv.admin, READY_TIMEOUT)
        .await
        .expect("server ready");

    // Write enough to seal several segments.
    let context = connect(srv.listen)
        .await
        .expect("connect")
        .default_context();
    for batch in 0..40 {
        let events: Vec<NewEvent> = (0..16)
            .map(|i| {
                event(
                    format!("evt-{batch}-{i}"),
                    1_712_000_000_000 + batch,
                    "Archived",
                    "1.0",
                    vec![b'x'; 256],
                    vec![(b"agg".to_vec(), format!("agg-{}", batch % 4).into_bytes())],
                )
            })
            .collect();
        context.append(&events, None).await.expect("append");
    }

    // Wait for the leader to archive at least two segments.
    let segments_dir = tier_dir.join("default").join("segments");
    let summaries = || -> Vec<serde_json::Value> {
        let Ok(entries) = std::fs::read_dir(&segments_dir) else {
            return Vec::new();
        };
        let mut summaries: Vec<serde_json::Value> = entries
            .filter_map(|entry| {
                let path = entry.ok()?.path();
                (path.extension()? == "sum").then(|| {
                    serde_json::from_slice(&std::fs::read(&path).expect("read summary"))
                        .expect("parse summary")
                })
            })
            .collect();
        summaries.sort_by_key(|summary| summary["base"].as_u64());
        summaries
    };
    let start = Instant::now();
    let summaries = loop {
        let summaries = summaries();
        if summaries.len() >= 2 {
            break summaries;
        }
        assert!(
            start.elapsed() < ARCHIVE_TIMEOUT,
            "fewer than 2 segments archived after {ARCHIVE_TIMEOUT:?}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    };

    // Archived segments are a prefix of the log, each byte-identical to the
    // data bytes of its local original and matching its summary.
    eprintln!("archive_e2e: {} segments archived", summaries.len());
    let mut expected_base = 0;
    for summary in &summaries {
        let base = summary["base"].as_u64().expect("base");
        assert_eq!(base, expected_base, "the archive skips a segment");
        expected_base = summary["end"].as_u64().expect("end");
        let name = format!("{base:020}.seg");
        let data_len = summary["data_len"].as_u64().expect("data_len") as usize;
        let local = std::fs::read(data_dir.join("default").join(&name)).expect("local segment");
        let archived = std::fs::read(segments_dir.join(&name)).expect("archived segment");
        assert_eq!(
            archived,
            local[..data_len],
            "archived {name} differs from local bytes"
        );
        assert_eq!(
            summary["blake3"].as_str().unwrap(),
            blake3::hash(&archived).to_hex().to_string(),
            "summary blake3 mismatch for {name}"
        );
    }

    // Only final segments: each ends strictly below the watermark.
    let head = context.head().await.expect("get_head");
    assert!(expected_base < head, "archived a segment that is not final");

    srv.kill();
}
