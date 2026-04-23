//! Shared harness for Phase 6 crash tests.
//!
//! Lives in kronosdb-eventstore per D-06: tests run on default `cargo test`, same crate
//! as concurrent_dcb_cluster.rs and snapshot_coldjoin.rs. This harness shells out to the
//! production `kronosdb-server` binary via env!("CARGO_BIN_EXE_kronosdb-server").
//!
//! Responsibilities:
//!   1. Spawn kronosdb-server as a child process with env-var config.
//!   2. Wait until the gRPC port accepts connections (with timeout).
//!   3. Provide an `AckLog` that records every successful append to an in-memory Vec
//!      AND an fsync-per-line sidecar file on the test side (D-10).
//!   4. SIGKILL the child (std::process::Child::kill() — SIGKILL on Unix; satisfies D-01/D-13).
//!   5. Raw-disk CRC scanner for log segments + event segments (CRASH-02).
//!   6. Shared helpers: hash_payload (blake3), read_ack_sidecar, rand_in.

#![allow(dead_code)] // Not every helper is used by every test file.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::{Duration, Instant};

/// Generated client stubs — compiled by crates/kronosdb-eventstore/build.rs
/// (client-only for eventstore.proto). Usable ONLY from this harness module and
/// its downstream test files; NOT exported from the library.
pub mod pb {
    tonic::include_proto!("kronosdb");
    pub mod eventstore {
        tonic::include_proto!("kronosdb.eventstore");
    }
}

pub struct ServerHandle {
    pub child: Child,
    pub listen: SocketAddr,
    pub admin: SocketAddr,
    pub data_dir: PathBuf,
}

impl ServerHandle {
    /// SIGKILL the child and reap it. Idempotent if already exited.
    pub fn kill(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Drop for ServerHandle {
    fn drop(&mut self) {
        // Safety net: test panics must not leak child processes holding ports.
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Allocates a free localhost port by binding 127.0.0.1:0 and dropping the listener.
/// Small TOCTOU window — acceptable in a test harness.
pub fn free_port() -> u16 {
    let l = TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    let port = l.local_addr().unwrap().port();
    drop(l);
    port
}

pub struct SpawnConfig {
    pub data_dir: PathBuf,
    pub listen_port: u16,
    pub admin_port: u16,
    pub node_id: u64,
    pub peers: Vec<(u64, String)>, // (id, addr); empty = single-node
    pub group_commit_ms: Option<u64>,
}

/// Spawn kronosdb-server as a child. Returns immediately after spawn — caller must
/// call `wait_until_ready`.
pub fn spawn_server(cfg: &SpawnConfig) -> std::io::Result<ServerHandle> {
    // `KRONOSDB_SERVER_BIN` is set by crates/kronosdb-eventstore/build.rs. That script
    // locates `target/<profile>/kronosdb-server` (and runs `cargo build -p kronosdb-server`
    // if the binary is missing) and exports the absolute path via rustc-env. Stable
    // Cargo does NOT expose `CARGO_BIN_EXE_kronosdb-server` for cross-package bins, and
    // adding `[lib]` to kronosdb-server is explicitly out of scope for Phase 6 (the
    // plan forbids touching the server crate at all — see acceptance criteria).
    // Synonym for `CARGO_BIN_EXE_kronosdb-server` in intent; different mechanism.
    let bin = env!("KRONOSDB_SERVER_BIN");
    let listen: SocketAddr = format!("127.0.0.1:{}", cfg.listen_port).parse().unwrap();
    let admin: SocketAddr = format!("127.0.0.1:{}", cfg.admin_port).parse().unwrap();

    let mut cmd = Command::new(bin);
    cmd.env("KRONOSDB_LISTEN", listen.to_string())
        .env("KRONOSDB_ADMIN_LISTEN", admin.to_string())
        .env("KRONOSDB_DATA_DIR", &cfg.data_dir)
        .env("KRONOSDB_NODE_NAME", format!("crash-node-{}", cfg.node_id))
        .env("KRONOSDB_CLUSTER_NODE_ID", cfg.node_id.to_string())
        .env("RUST_LOG", "kronosdb=info,warn")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    if !cfg.peers.is_empty() {
        let peer_str = cfg
            .peers
            .iter()
            .map(|(id, addr)| format!("{id}={addr}"))
            .collect::<Vec<_>>()
            .join(",");
        cmd.env("KRONOSDB_CLUSTER_PEERS", peer_str);
    }
    if let Some(ms) = cfg.group_commit_ms {
        cmd.env("KRONOSDB_GROUP_COMMIT_MS", ms.to_string());
    }

    let child = cmd.spawn()?;
    Ok(ServerHandle {
        child,
        listen,
        admin,
        data_dir: cfg.data_dir.clone(),
    })
}

/// Polls the gRPC port until a TCP connection succeeds or timeout elapses.
pub async fn wait_until_ready(addr: SocketAddr, timeout: Duration) -> Result<(), String> {
    let start = Instant::now();
    loop {
        if tokio::net::TcpStream::connect(addr).await.is_ok() {
            // Brief grace — tonic service registration happens a moment after listener binds.
            tokio::time::sleep(Duration::from_millis(50)).await;
            return Ok(());
        }
        if start.elapsed() > timeout {
            return Err(format!("server not ready on {addr} after {:?}", timeout));
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// One record the client successfully appended. Content-addressable for
/// post-restart verification.
#[derive(Clone, Debug)]
pub struct AckRecord {
    pub aggregate_id: String,
    pub client_sequence: u64,
    pub payload_hash: Vec<u8>, // 32 bytes (blake3)
    pub server_sequence: i64,
}

/// In-memory + fsync'd-sidecar ack log (D-10).
pub struct AckLog {
    path: PathBuf,
    file: std::sync::Mutex<std::fs::File>,
    recs: std::sync::Mutex<Vec<AckRecord>>,
}

impl AckLog {
    pub fn create(path: PathBuf) -> std::io::Result<Self> {
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)?;
        Ok(Self {
            path,
            file: std::sync::Mutex::new(file),
            recs: std::sync::Mutex::new(Vec::new()),
        })
    }

    /// Record + fsync. Called after every gRPC Ok response.
    pub fn record(&self, rec: AckRecord) -> std::io::Result<()> {
        let line = format!(
            "{}\t{}\t{}\t{}\n",
            rec.aggregate_id,
            rec.client_sequence,
            hex_encode(&rec.payload_hash),
            rec.server_sequence
        );
        {
            let mut f = self.file.lock().unwrap();
            f.write_all(line.as_bytes())?;
            f.sync_all()?;
        }
        self.recs.lock().unwrap().push(rec);
        Ok(())
    }

    pub fn snapshot(&self) -> Vec<AckRecord> {
        self.recs.lock().unwrap().clone()
    }

    pub fn len(&self) -> usize {
        self.recs.lock().unwrap().len()
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{:02x}", b));
    }
    s
}

/// Payload hash — blake3, 32 bytes. LOCKED per plan revision.
pub fn hash_payload(payload: &[u8]) -> Vec<u8> {
    blake3::hash(payload).as_bytes().to_vec()
}

/// Re-read the ack sidecar from disk after restart. Used by both 1-node and 3-node tests.
pub fn read_ack_sidecar(path: &Path) -> std::io::Result<Vec<AckRecord>> {
    let contents = std::fs::read_to_string(path)?;
    let mut out = Vec::new();
    for line in contents.lines() {
        let parts: Vec<&str> = line.split('\t').collect();
        if parts.len() != 4 {
            continue;
        }
        let hash_hex = parts[2];
        if hash_hex.len() != 64 {
            continue;
        }
        let mut hash = Vec::with_capacity(32);
        let mut ok = true;
        for i in 0..32 {
            match u8::from_str_radix(&hash_hex[i * 2..i * 2 + 2], 16) {
                Ok(b) => hash.push(b),
                Err(_) => {
                    ok = false;
                    break;
                }
            }
        }
        if !ok {
            continue;
        }
        out.push(AckRecord {
            aggregate_id: parts[0].to_string(),
            client_sequence: parts[1].parse().unwrap_or(0),
            payload_hash: hash,
            server_sequence: parts[3].parse().unwrap_or(0),
        });
    }
    Ok(out)
}

/// Simple time-seeded xorshift — no rand crate needed. Returns integer in [lo, hi].
pub fn rand_in(lo: u64, hi: u64) -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    let mut x = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    lo + (x % (hi - lo + 1))
}

/// Raw CRC scan result for one segment file.
#[derive(Debug)]
pub struct SegmentScanResult {
    pub path: PathBuf,
    pub valid_records: u64,
    pub torn_tail_bytes: u64, // bytes past the last valid record
    pub torn_reason: Option<String>, // short-read | len-zero | bincode-fail | crc-mismatch | None if clean
}

/// Scan a raft log segment (log-*.bin) using Phase 2's format:
///   [u32 len_be][bincode payload][u32 crc32c_le], CRC over payload only.
pub fn scan_raft_log_segment(path: &Path) -> std::io::Result<SegmentScanResult> {
    let mut f = std::fs::File::open(path)?;
    let total_len = f.metadata()?.len();
    let mut offset: u64 = 0;
    let mut valid_records: u64 = 0;
    let mut torn_reason: Option<String> = None;

    loop {
        if total_len - offset < 4 {
            if total_len - offset > 0 {
                torn_reason = Some("short-read at len prefix".into());
            }
            break;
        }
        let mut len_buf = [0u8; 4];
        f.read_exact(&mut len_buf)?;
        let payload_len = u32::from_be_bytes(len_buf) as u64;
        if payload_len == 0 {
            // Preallocated zero-tail — legitimate EOF per Phase 2 D-15.
            break;
        }
        if total_len - offset < 4 + payload_len + 4 {
            torn_reason = Some("short-read payload+crc".into());
            break;
        }
        let mut payload = vec![0u8; payload_len as usize];
        f.read_exact(&mut payload)?;
        let mut crc_buf = [0u8; 4];
        f.read_exact(&mut crc_buf)?;
        let expected = u32::from_le_bytes(crc_buf);
        let actual = crc32c::crc32c(&payload);
        if expected != actual {
            torn_reason = Some(format!(
                "crc mismatch (expected {:08x}, got {:08x})",
                expected, actual
            ));
            break;
        }
        valid_records += 1;
        offset += 4 + payload_len + 4;
    }

    let torn_tail_bytes = total_len
        .saturating_sub(offset)
        .saturating_sub(if torn_reason.is_some() { 4 } else { 0 });
    Ok(SegmentScanResult {
        path: path.to_path_buf(),
        valid_records,
        torn_tail_bytes,
        torn_reason,
    })
}

/// Scan an event segment (*.seg):
///   File header (13 bytes): [4 magic "KRON"][1 version=2][8 base_position]
///   Record header (9 bytes): [4 crc32c_le][4 record_len_le][1 flags]
///   CRC over [flags || payload]
pub fn scan_event_segment(path: &Path) -> std::io::Result<SegmentScanResult> {
    let mut f = std::fs::File::open(path)?;
    let total_len = f.metadata()?.len();
    if total_len < 13 {
        return Ok(SegmentScanResult {
            path: path.to_path_buf(),
            valid_records: 0,
            torn_tail_bytes: total_len,
            torn_reason: Some("segment shorter than file header".into()),
        });
    }
    let mut hdr = [0u8; 13];
    f.read_exact(&mut hdr)?;
    if &hdr[0..4] != b"KRON" {
        return Ok(SegmentScanResult {
            path: path.to_path_buf(),
            valid_records: 0,
            torn_tail_bytes: total_len,
            torn_reason: Some("bad magic".into()),
        });
    }
    let mut offset: u64 = 13;
    let mut valid_records: u64 = 0;
    let mut torn_reason: Option<String> = None;

    loop {
        if total_len - offset < 9 {
            if total_len - offset > 0 {
                torn_reason = Some("short-read record header".into());
            }
            break;
        }
        let mut rh = [0u8; 9];
        f.read_exact(&mut rh)?;
        let expected_crc = u32::from_le_bytes([rh[0], rh[1], rh[2], rh[3]]);
        let record_len = u32::from_le_bytes([rh[4], rh[5], rh[6], rh[7]]) as u64;
        let flags = rh[8];
        if record_len == 0 {
            break;
        }
        let payload_len = record_len.saturating_sub(1);
        if total_len - offset - 9 < payload_len {
            torn_reason = Some("short-read payload".into());
            break;
        }
        let mut payload = vec![0u8; payload_len as usize];
        f.read_exact(&mut payload)?;
        let mut crc_input = Vec::with_capacity(1 + payload.len());
        crc_input.push(flags);
        crc_input.extend_from_slice(&payload);
        let actual_crc = crc32c::crc32c(&crc_input);
        if expected_crc != actual_crc {
            torn_reason = Some(format!("crc mismatch at offset {offset}"));
            break;
        }
        valid_records += 1;
        offset += 9 + payload_len;
    }

    let torn_tail_bytes = total_len.saturating_sub(offset);
    Ok(SegmentScanResult {
        path: path.to_path_buf(),
        valid_records,
        torn_tail_bytes,
        torn_reason,
    })
}

/// Walks `<data_dir>/<ctx>/` and scans every log-*.bin and *.seg.
/// Returns (total_valid_log_records, total_valid_event_records, scan_results).
pub fn scan_all_segments(
    data_dir: &Path,
    ctx: &str,
) -> std::io::Result<(u64, u64, Vec<SegmentScanResult>)> {
    let ctx_dir = data_dir.join(ctx);
    let raft_dir = ctx_dir.join("raft");
    let mut results = Vec::new();
    let mut total_log_records = 0u64;
    let mut total_event_records = 0u64;

    if raft_dir.is_dir() {
        let mut entries: Vec<_> = std::fs::read_dir(&raft_dir)?
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.file_name()
                    .to_str()
                    .map(|s| s.starts_with("log-") && s.ends_with(".bin"))
                    .unwrap_or(false)
            })
            .collect();
        entries.sort_by_key(|e| e.file_name());
        for e in entries {
            let r = scan_raft_log_segment(&e.path())?;
            total_log_records += r.valid_records;
            results.push(r);
        }
    }

    if ctx_dir.is_dir() {
        let mut entries: Vec<_> = std::fs::read_dir(&ctx_dir)?
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.file_name()
                    .to_str()
                    .map(|s| s.ends_with(".seg"))
                    .unwrap_or(false)
            })
            .collect();
        entries.sort_by_key(|e| e.file_name());
        for e in entries {
            let r = scan_event_segment(&e.path())?;
            total_event_records += r.valid_records;
            results.push(r);
        }
    }

    Ok((total_log_records, total_event_records, results))
}

pub type Seq = Arc<AtomicU64>;

pub fn new_seq() -> Seq {
    Arc::new(AtomicU64::new(0))
}
