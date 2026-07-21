//! Shared harness for server crash-recovery tests.
//!
//! The harness belongs to the event-store crate so it runs under the normal
//! workspace test command. It drives the production `kronosdb-server` binary
//! over gRPC and verifies the files it leaves on disk.
//!
//! Responsibilities:
//!   1. Spawn kronosdb-server as a child process with env-var config.
//!   2. Wait until both gRPC and application readiness succeed.
//!   3. Persist every successful append in a separately fsynced acknowledgement log.
//!   4. SIGKILL and reap the child process.
//!   5. Scan metadata-log and event-segment records for torn writes.
//!   6. Provide shared restart and payload-verification helpers.

#![allow(dead_code)] // Not every helper is used by every test file.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Generated client stubs — compiled by crates/kronosdb-eventstore/build.rs
/// (client-only for eventstore.proto). Usable ONLY from this harness module and
/// its downstream test files; NOT exported from the library.
#[allow(clippy::enum_variant_names)]
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

    /// SIGSTOP the child: the process freezes mid-flight without dying. Its
    /// sockets stay bound but nothing answers — the closest a process-level
    /// harness gets to a "stale leader that doesn't know it was deposed".
    pub fn pause(&self) {
        signal_child(self.child.id(), "STOP");
    }

    /// SIGCONT a paused child. It resumes exactly where it froze, with its
    /// pre-pause view of the cluster — and must discover it is stale.
    pub fn resume(&self) {
        signal_child(self.child.id(), "CONT");
    }
}

fn signal_child(pid: u32, signal: &str) {
    let status = Command::new("kill")
        .arg(format!("-{signal}"))
        .arg(pid.to_string())
        .status()
        .expect("invoke kill");
    assert!(status.success(), "kill -{signal} {pid} failed");
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
    /// Event segment cap in bytes. Small values let tests seal many segments
    /// quickly (cold-join catch-up). `None` keeps the 256 MB default.
    pub segment_size: Option<u64>,
}

/// Ensures the kronosdb-server binary is built once before any crash test runs it.
/// Build script cannot do this (would be re-entrant through the eventstore → server
/// → eventstore dev-dependency edge), so we do it on first spawn via `cargo build`.
/// Runs exactly once per test-binary process.
fn ensure_server_built() {
    static BUILT: OnceLock<Mutex<bool>> = OnceLock::new();
    let m = BUILT.get_or_init(|| Mutex::new(false));
    let mut guard = m.lock().unwrap();
    if *guard {
        return;
    }
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let workspace_root = manifest_dir.parent().and_then(|p| p.parent()).unwrap();
    let mut cmd = Command::new(std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into()));
    cmd.current_dir(workspace_root)
        .arg("build")
        .arg("-p")
        .arg("kronosdb-server")
        .arg("--bin")
        .arg("kronosdb-server");
    // Build the profile this test binary will spawn (KRONOSDB_SERVER_BIN
    // points into target/<profile>/) — otherwise release-mode tests run a
    // stale release server while only the debug binary gets rebuilt.
    if !cfg!(debug_assertions) {
        cmd.arg("--release");
    }
    let status = cmd
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .status()
        .expect("invoke cargo build -p kronosdb-server");
    assert!(status.success(), "cargo build -p kronosdb-server failed");
    *guard = true;
}

/// Spawn kronosdb-server as a child. Returns immediately after spawn — caller must
/// call `wait_until_ready`.
pub fn spawn_server(cfg: &SpawnConfig) -> std::io::Result<ServerHandle> {
    // `KRONOSDB_SERVER_BIN` is set by crates/kronosdb-eventstore/build.rs. Stable
    // Cargo does not expose `CARGO_BIN_EXE_kronosdb-server` for cross-package
    // binaries, so the harness builds the server once per test-binary process.
    ensure_server_built();
    let bin = env!("KRONOSDB_SERVER_BIN");
    let listen: SocketAddr = format!("127.0.0.1:{}", cfg.listen_port).parse().unwrap();
    let admin: SocketAddr = format!("127.0.0.1:{}", cfg.admin_port).parse().unwrap();

    let mut cmd = Command::new(bin);
    cmd.env("KRONOSDB_LISTEN", listen.to_string())
        .env("KRONOSDB_ADMIN_LISTEN", admin.to_string())
        .env("KRONOSDB_DATA_DIR", &cfg.data_dir)
        .env("KRONOSDB_NODE_NAME", format!("crash-node-{}", cfg.node_id))
        .env("KRONOSDB_CLUSTER_NODE_ID", cfg.node_id.to_string())
        .env("RUST_LOG", "kronosdb=debug,warn");
    if std::env::var_os("KRONOSDB_TEST_LOGS").is_some() {
        cmd.stdout(Stdio::inherit()).stderr(Stdio::inherit());
    } else {
        cmd.stdout(Stdio::null()).stderr(Stdio::null());
    }

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
    if let Some(bytes) = cfg.segment_size {
        cmd.env("KRONOSDB_SEGMENT_SIZE", bytes.to_string());
    }

    let child = cmd.spawn()?;
    Ok(ServerHandle {
        child,
        listen,
        admin,
        data_dir: cfg.data_dir.clone(),
    })
}

/// Polls until the gRPC listener accepts connections and the native write gate
/// reports ready through the admin endpoint.
pub async fn wait_until_ready(
    listen: SocketAddr,
    admin: SocketAddr,
    timeout: Duration,
) -> Result<(), String> {
    let start = Instant::now();
    loop {
        if tokio::net::TcpStream::connect(listen).await.is_ok() && admin_reports_ready(admin).await
        {
            return Ok(());
        }
        if start.elapsed() > timeout {
            return Err(format!(
                "server not ready on {listen} (admin {admin}) after {timeout:?}"
            ));
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn admin_reports_ready(addr: SocketAddr) -> bool {
    let Ok(mut stream) = tokio::net::TcpStream::connect(addr).await else {
        return false;
    };
    if stream
        .write_all(b"GET /ready HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .await
        .is_err()
    {
        return false;
    }
    let mut response = Vec::new();
    stream.read_to_end(&mut response).await.is_ok() && response.starts_with(b"HTTP/1.1 200")
}

/// Minimal HTTP/1.1 JSON POST against a node's admin listener. Returns
/// (status code, response body). Used for cluster-membership calls; the test
/// servers run with admin auth `none`.
pub async fn admin_post(
    addr: SocketAddr,
    path: &str,
    json_body: &str,
) -> Result<(u16, String), String> {
    let mut stream = tokio::net::TcpStream::connect(addr)
        .await
        .map_err(|e| format!("connect {addr}: {e}"))?;
    let request = format!(
        "POST {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\
         Content-Type: application/json\r\nContent-Length: {}\r\n\r\n{json_body}",
        json_body.len()
    );
    stream
        .write_all(request.as_bytes())
        .await
        .map_err(|e| format!("write {path}: {e}"))?;
    let mut response = Vec::new();
    stream
        .read_to_end(&mut response)
        .await
        .map_err(|e| format!("read {path}: {e}"))?;
    let response = String::from_utf8_lossy(&response);
    let status: u16 = response
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| format!("malformed response from {path}: {response}"))?;
    let body = response
        .split_once("\r\n\r\n")
        .map(|(_, body)| body.to_string())
        .unwrap_or_default();
    Ok((status, body))
}

pub async fn wait_for_raft_leader(
    servers: &[ServerHandle],
    timeout: Duration,
) -> Result<usize, String> {
    let start = Instant::now();
    loop {
        let mut leaders = Vec::new();
        for (index, server) in servers.iter().enumerate() {
            // Per-probe timeout: a SIGSTOP'd node accepts the TCP connection
            // (kernel backlog) but never answers, which would otherwise hang
            // the poll loop on read_to_end forever.
            let probe = tokio::time::timeout(Duration::from_millis(500), async {
                let mut stream = tokio::net::TcpStream::connect(server.admin).await.ok()?;
                stream
                    .write_all(
                        b"GET /metrics HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
                    )
                    .await
                    .ok()?;
                let mut response = Vec::new();
                stream.read_to_end(&mut response).await.ok()?;
                Some(response)
            });
            let Ok(Some(response)) = probe.await else {
                continue;
            };
            if String::from_utf8_lossy(&response)
                .lines()
                .any(|line| line == "kronosdb_raft_is_leader 1")
            {
                leaders.push(index);
            }
        }
        if leaders.len() == 1 {
            return Ok(leaders[0]);
        }
        if start.elapsed() > timeout {
            return Err(format!(
                "expected one metadata leader within {timeout:?}, observed {leaders:?}"
            ));
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
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

/// In-memory acknowledgement log with an fsynced sidecar.
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

/// Payload hash used for post-restart content verification.
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

/// Global xorshift state, seeded once per test-binary process. The seed is
/// taken from `KRONOSDB_CRASH_SEED` when set (reproduction) or from the clock
/// (exploration), and is printed either way so a CI failure can be replayed:
///
/// ```text
/// KRONOSDB_CRASH_SEED=<seed> cargo test -p kronosdb-eventstore --test crash_three_node
/// ```
///
/// OS scheduling still varies between runs, so a replay is best-effort — but
/// the kill delays and any other harness randomness become identical.
fn rng_state() -> &'static AtomicU64 {
    static STATE: OnceLock<AtomicU64> = OnceLock::new();
    STATE.get_or_init(|| {
        use std::time::{SystemTime, UNIX_EPOCH};
        let seed = match std::env::var("KRONOSDB_CRASH_SEED")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
        {
            Some(seed) => {
                eprintln!("crash harness: replaying KRONOSDB_CRASH_SEED={seed}");
                seed
            }
            None => {
                let seed = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_nanos() as u64;
                eprintln!("crash harness: seed={seed} (set KRONOSDB_CRASH_SEED={seed} to replay)");
                seed
            }
        };
        // xorshift must never be seeded with 0.
        AtomicU64::new(seed.max(1))
    })
}

/// Seeded xorshift — no rand crate needed. Returns integer in [lo, hi].
pub fn rand_in(lo: u64, hi: u64) -> u64 {
    use std::sync::atomic::Ordering;
    let state = rng_state();
    let mut x = state.load(Ordering::Relaxed);
    loop {
        let mut next = x;
        next ^= next << 13;
        next ^= next >> 7;
        next ^= next << 17;
        match state.compare_exchange_weak(x, next, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => return lo + (next % (hi - lo + 1)),
            Err(actual) => x = actual,
        }
    }
}

/// Raw CRC scan result for one segment file.
#[derive(Debug)]
pub struct SegmentScanResult {
    pub path: PathBuf,
    pub valid_records: u64,
    pub torn_tail_bytes: u64,        // bytes past the last valid record
    pub torn_reason: Option<String>, // short-read | len-zero | bincode-fail | crc-mismatch | None if clean
}

/// Scan a metadata Raft log segment (`log-*.bin`):
/// `[u32 len_be][bincode payload][u32 crc32c_le]`, CRC over payload only.
///
/// A "torn tail" is post-last-valid-record bytes that do not belong to either
/// a valid record or preallocated zero padding. A zero length prefix marks the
/// legitimate start of the preallocated tail and is not a torn record.
pub fn scan_raft_log_segment(path: &Path) -> std::io::Result<SegmentScanResult> {
    let mut f = std::fs::File::open(path)?;
    let total_len = f.metadata()?.len();
    let mut offset: u64 = 0;
    let mut valid_records: u64 = 0;
    let mut torn_reason: Option<String> = None;
    let mut hit_preallocated_zero = false;

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
            // Preallocated zero tail is a legitimate end-of-log marker.
            hit_preallocated_zero = true;
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

    // Preallocated zero tail is NOT a torn record.
    let torn_tail_bytes = if hit_preallocated_zero {
        0
    } else {
        total_len
            .saturating_sub(offset)
            .saturating_sub(if torn_reason.is_some() { 4 } else { 0 })
    };
    Ok(SegmentScanResult {
        path: path.to_path_buf(),
        valid_records,
        torn_tail_bytes,
        torn_reason,
    })
}

/// Scan an event segment (*.seg):
///   File header (13 bytes): [4 magic "KRON"][1 version=3][8 base_position]
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
    if &hdr[0..4] != b"KRON" || hdr[4] != 3 {
        return Ok(SegmentScanResult {
            path: path.to_path_buf(),
            valid_records: 0,
            torn_tail_bytes: total_len,
            torn_reason: Some("bad segment header".into()),
        });
    }
    let mut offset: u64 = 13;
    let mut valid_records: u64 = 0;
    let mut torn_reason: Option<String> = None;
    let mut hit_preallocated_zero = false;

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
            // Preallocated zero record header — event segments are preallocated
            // to segment_size (e.g. 256 MB) and the tail is all zeros until the
            // next rotation writes records into it. Not a torn record.
            hit_preallocated_zero = true;
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

    let torn_tail_bytes = if hit_preallocated_zero {
        0
    } else {
        total_len.saturating_sub(offset)
    };
    Ok(SegmentScanResult {
        path: path.to_path_buf(),
        valid_records,
        torn_tail_bytes,
        torn_reason,
    })
}

pub fn read_valid_event_log(data_dir: &Path, ctx: &str) -> std::io::Result<Vec<(String, Vec<u8>)>> {
    let ctx_dir = data_dir.join(ctx);
    let mut entries: Vec<_> = std::fs::read_dir(ctx_dir)?
        .filter_map(|entry| entry.ok())
        .filter(|entry| {
            entry
                .file_name()
                .to_str()
                .map(|name| name.ends_with(".seg"))
                .unwrap_or(false)
        })
        .collect();
    entries.sort_by_key(|entry| entry.file_name());

    entries
        .into_iter()
        .map(|entry| {
            let mut file = std::fs::File::open(entry.path())?;
            let mut bytes = vec![0u8; 13];
            file.read_exact(&mut bytes)?;
            loop {
                let mut header = [0u8; 9];
                match file.read_exact(&mut header) {
                    Ok(()) => {}
                    Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => break,
                    Err(error) => return Err(error),
                }
                let record_len = u32::from_le_bytes(header[4..8].try_into().unwrap()) as usize;
                if record_len == 0 {
                    break;
                }
                let mut payload = vec![0u8; record_len - 1];
                file.read_exact(&mut payload)?;
                bytes.extend_from_slice(&header);
                bytes.extend_from_slice(&payload);
            }
            Ok((entry.file_name().to_string_lossy().into_owned(), bytes))
        })
        .collect()
}

/// Walks the node-wide metadata journal and one context's event segments.
/// Returns (total_valid_log_records, total_valid_event_records, scan_results).
pub fn scan_all_segments(
    data_dir: &Path,
    ctx: &str,
) -> std::io::Result<(u64, u64, Vec<SegmentScanResult>)> {
    let ctx_dir = data_dir.join(ctx);
    let raft_dir = data_dir.join("raft");
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
