//! End-to-end proof of the ADR-0002 stage-1 backup uploader through the
//! production server: env-var config, leader gating, the periodic pass, and
//! byte-exact archived segments with a consistent manifest.

#![allow(clippy::result_large_err)]

mod crash_harness;
use crash_harness::pb::eventstore as pb;
use crash_harness::*;
use pb::Tag;

use std::time::{Duration, Instant};

use tonic::transport::Channel;

use pb::event_store_client::EventStoreClient;

const SEGMENT_SIZE: u64 = 64 * 1024;
const READY_TIMEOUT: Duration = Duration::from_secs(15);
const BACKUP_TIMEOUT: Duration = Duration::from_secs(30);

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn backup_uploads_sealed_segments_end_to_end() {
    tokio::time::timeout(Duration::from_secs(120), run())
        .await
        .expect("backup_uploads_sealed_segments_end_to_end timed out");
}

async fn run() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let data_dir = tmp.path().join("data");
    let tier_dir = tmp.path().join("tier");
    std::fs::create_dir_all(&data_dir).expect("mkdir data");
    std::fs::create_dir_all(&tier_dir).expect("mkdir tier");
    let backup_url = url::Url::from_directory_path(&tier_dir)
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
        backup: Some((backup_url, 1)),
    })
    .expect("spawn server");
    wait_until_ready(srv.listen, srv.admin, READY_TIMEOUT)
        .await
        .expect("server ready");

    // Write enough to seal several segments.
    let channel = Channel::from_shared(format!("http://{}", srv.listen))
        .unwrap()
        .connect()
        .await
        .expect("connect");
    let mut client = EventStoreClient::new(channel);
    for batch in 0..40 {
        let events: Vec<pb::TaggedEvent> = (0..16)
            .map(|i| pb::TaggedEvent {
                event: Some(pb::Event {
                    identifier: format!("evt-{batch}-{i}"),
                    timestamp: 1_712_000_000_000 + batch,
                    name: "BackedUp".into(),
                    version: "1.0".into(),
                    payload: vec![b'x'; 256],
                    metadata: Default::default(),
                }),
                tags: vec![Tag {
                    key: b"agg".to_vec(),
                    value: format!("agg-{}", batch % 4).into_bytes(),
                }],
            })
            .collect();
        client
            .append(pb::AppendRequest {
                condition: None,
                events,
            })
            .await
            .expect("append");
    }

    // Wait for the uploader to ship at least two segments and a manifest.
    let manifest_path = tier_dir.join("default").join("manifest.json");
    let start = Instant::now();
    let manifest: serde_json::Value = loop {
        if let Ok(bytes) = std::fs::read(&manifest_path) {
            let manifest: serde_json::Value =
                serde_json::from_slice(&bytes).expect("parse manifest");
            let segments = manifest["segments"].as_array().expect("segments array");
            if segments.len() >= 2 {
                break manifest;
            }
        }
        assert!(
            start.elapsed() < BACKUP_TIMEOUT,
            "no backup manifest with >=2 segments after {BACKUP_TIMEOUT:?}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    };

    // Every archived segment must be byte-identical to its local original,
    // match its manifest blake3, and lie fully below the watermark.
    let segments = manifest["segments"].as_array().unwrap();
    eprintln!("backup_e2e: manifest lists {} segments", segments.len());
    for entry in segments {
        let base = entry["base"].as_u64().expect("base");
        let name = format!("{base:020}.seg");
        let local = std::fs::read(data_dir.join("default").join(&name)).expect("local segment");
        let archived = std::fs::read(tier_dir.join("default").join("segments").join(&name))
            .expect("archived segment");
        assert_eq!(local, archived, "archived {name} differs from local bytes");
        assert_eq!(
            entry["blake3"].as_str().unwrap(),
            blake3::hash(&local).to_hex().to_string(),
            "manifest blake3 mismatch for {name}"
        );
        assert_eq!(entry["size"].as_u64().unwrap(), local.len() as u64);
    }

    // The active segment must not be archived.
    let head = client
        .get_head(pb::GetHeadRequest {})
        .await
        .expect("get_head")
        .into_inner()
        .sequence as u64;
    for entry in segments {
        assert!(
            entry["end"].as_u64().unwrap() <= head,
            "archived segment ends past the watermark"
        );
    }

    srv.kill();
}
