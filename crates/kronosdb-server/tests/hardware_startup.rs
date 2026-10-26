//! Startup hardware checks, end to end: a real server process whose JSON logs
//! are read back, and whose admin endpoints are queried.

use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde_json::Value;

struct Server {
    child: Child,
    admin: String,
    logs: Arc<Mutex<Vec<Value>>>,
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Server {
    fn logs(&self) -> Vec<Value> {
        self.logs.lock().unwrap().clone()
    }

    /// The first log record whose message starts with `prefix`.
    fn record(&self, prefix: &str) -> Option<Value> {
        self.logs()
            .into_iter()
            .find(|r| message(r).starts_with(prefix))
    }
}

fn message(record: &Value) -> &str {
    record["fields"]["message"].as_str().unwrap_or("")
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// A directory under the target dir, removed first and left for the next run
/// to remove (the server's data dir must start empty).
fn work_dir(name: &str) -> PathBuf {
    let dir = Path::new(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("hardware-{}-{name}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// Starts the server with JSON logs and waits for its startup to finish.
fn spawn(data_dir: &Path, env: &[(&str, &str)]) -> Server {
    let admin = format!("127.0.0.1:{}", free_port());
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_kronosdb-server"));
    cmd.current_dir(data_dir.parent().unwrap())
        // A stray kronosdb.toml or environment must not change the run.
        .env_remove("KRONOSDB_DEV")
        .env_remove("KRONOSDB_ACK_MODE")
        .env("KRONOSDB_LISTEN", format!("127.0.0.1:{}", free_port()))
        .env("KRONOSDB_ADMIN_LISTEN", &admin)
        .env("KRONOSDB_DATA_DIR", data_dir)
        .env("KRONOSDB_LOG_FORMAT", "json")
        .env("RUST_LOG", "info")
        .envs(env.iter().copied())
        .stdout(Stdio::piped())
        .stderr(Stdio::null());
    let mut child = cmd.spawn().expect("spawn kronosdb-server");
    let stdout = child.stdout.take().unwrap();
    let logs = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&logs);
    std::thread::spawn(move || {
        use std::io::BufRead;
        for line in std::io::BufReader::new(stdout)
            .lines()
            .map_while(Result::ok)
        {
            if let Ok(record) = serde_json::from_str::<Value>(&line) {
                sink.lock().unwrap().push(record);
            }
        }
    });
    let server = Server { child, admin, logs };
    let deadline = Instant::now() + Duration::from_secs(60);
    while server.record("listening").is_none() {
        assert!(
            Instant::now() < deadline,
            "the server never finished starting; logs: {:#?}",
            server.logs()
        );
        std::thread::sleep(Duration::from_millis(50));
    }
    server
}

async fn get(server: &Server, path: &str) -> String {
    let url = format!("http://{}{path}", server.admin);
    for _ in 0..100 {
        if let Ok(response) = reqwest::get(&url).await
            && response.status().is_success()
        {
            return response.text().await.unwrap();
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("{url} never answered");
}

#[tokio::test]
async fn the_probe_runs_is_logged_and_reaches_the_admin_surfaces() {
    let dir = work_dir("probe").join("data");
    let server = spawn(&dir, &[]);

    // The probe line, with its measurements as structured fields.
    let probe = server
        .record("disk probe: fdatasync")
        .expect("probe log line");
    let fields = &probe["fields"];
    let (p50, p99) = (
        fields["p50_us"].as_u64().unwrap(),
        fields["p99_us"].as_u64().unwrap(),
    );
    assert!(fields["samples"].as_u64().unwrap() >= 8);
    assert!(p50 <= p99);
    assert!(fields["probe_ms"].as_u64().unwrap() < 3000, "{fields}");

    // The ack decision line follows the threshold: durable under 1 ms.
    let ack = server.record("ack mode:").expect("ack mode log line");
    let expect_multi = if p99 < 1000 { "durable" } else { "written" };
    assert_eq!(ack["fields"]["ack_mode"], "auto");
    assert_eq!(ack["fields"]["single_voter"], "durable");
    assert_eq!(
        ack["fields"]["multi_voter"], expect_multi,
        "p99 {p99}us: {ack}"
    );

    // The admin JSON API carries the same numbers.
    let node: Value = serde_json::from_str(&get(&server, "/api/v1/node").await).unwrap();
    let hardware = &node["hardware"];
    assert_eq!(hardware["disk"]["probe"]["p99_us"].as_u64(), Some(p99));
    assert_eq!(hardware["ack"]["multi_voter"], expect_multi);
    assert_eq!(hardware["ack"]["configured"], "auto");
    assert_eq!(hardware["dev_mode"], false);
    assert!(hardware["findings"].is_array());
    let snapshot: Value = serde_json::from_str(&get(&server, "/api/v1/snapshot").await).unwrap();
    assert_eq!(snapshot["node"]["hardware"], *hardware);

    // And so does /metrics.
    let metrics = get(&server, "/metrics").await;
    assert!(
        metrics.contains(&format!(
            "kronosdb_disk_fsync_microseconds{{quantile=\"0.99\"}} {p99}"
        )),
        "{metrics}"
    );
    assert!(metrics.contains("kronosdb_disk_probe_ok 1"));
    assert!(metrics.contains("kronosdb_dev_mode 0"));
    assert!(metrics.contains("kronosdb_ack_durable{topology=\"single_voter\"} 1"));
    let multi_durable = (expect_multi == "durable") as u8;
    assert!(metrics.contains(&format!(
        "kronosdb_ack_durable{{topology=\"multi_voter\"}} {multi_durable}"
    )));
    for check in [
        "memory_filesystem",
        "network_filesystem",
        "slow_fsync",
        "probe_failed",
        "platform_macos",
        "low_memory",
        "swap_in_use",
        "cpu_limit",
        "few_cores",
    ] {
        assert!(
            metrics.contains(&format!("kronosdb_hardware_finding{{check=\"{check}\"")),
            "no series for {check}"
        );
    }

    // The probe leaves nothing behind in the data dir.
    assert!(!dir.join(".disk-probe").exists());
}

#[tokio::test]
async fn explicit_ack_modes_are_not_decided_from_the_disk() {
    for mode in ["written", "durable"] {
        let dir = work_dir(mode).join("data");
        let server = spawn(&dir, &[("KRONOSDB_ACK_MODE", mode)]);
        let ack = server.record("ack mode:").expect("ack mode log line");
        assert_eq!(ack["fields"]["ack_mode"], mode);
        assert_eq!(ack["fields"]["single_voter"], mode);
        assert_eq!(ack["fields"]["multi_voter"], mode);
        // The probe still ran: it feeds the warnings.
        assert!(server.record("disk probe: fdatasync").is_some());
        let node: Value = serde_json::from_str(&get(&server, "/api/v1/node").await).unwrap();
        assert_eq!(node["hardware"]["ack"]["multi_voter"], mode);
    }
}

/// A data dir that is guaranteed to trip a check on this platform, and the
/// check: macOS always does; on Linux, tmpfs under /dev/shm.
fn data_dir_that_trips_a_check(name: &str) -> Option<(PathBuf, &'static str)> {
    if cfg!(target_os = "macos") {
        return Some((work_dir(name).join("data"), "platform_macos"));
    }
    let shm = Path::new("/dev/shm");
    if cfg!(target_os = "linux") && shm.is_dir() {
        let dir = shm.join(format!("kronosdb-hw-{}-{name}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        return Some((dir.join("data"), "memory_filesystem"));
    }
    None
}

/// The findings that were logged, as (level, check).
fn finding_levels(server: &Server) -> Vec<(String, String)> {
    server
        .logs()
        .iter()
        .filter_map(|r| {
            let check = r["fields"]["check"].as_str()?;
            Some((r["level"].as_str()?.to_string(), check.to_string()))
        })
        .collect()
}

#[test]
fn findings_are_loud_by_default() {
    let Some((dir, check)) = data_dir_that_trips_a_check("loud") else {
        return;
    };
    // An empty KRONOSDB_DEV (compose passthrough) means off, and boots.
    let server = spawn(&dir, &[("KRONOSDB_DEV", "")]);
    let expected = if check == "memory_filesystem" {
        "ERROR"
    } else {
        "WARN"
    };
    let levels = finding_levels(&server);
    assert!(
        levels.contains(&(expected.to_string(), check.to_string())),
        "{levels:?}"
    );
    // The closing line points at dev mode.
    let summary = server
        .logs()
        .into_iter()
        .find(|r| message(r).contains("below the recommended production setup"))
        .expect("summary line");
    assert_eq!(summary["level"], "WARN");
    let _ = std::fs::remove_dir_all(dir.parent().unwrap());
}

#[test]
fn dev_mode_logs_findings_quietly() {
    for (name, env) in [
        ("dev-env", ("KRONOSDB_DEV", "1")),
        ("dev-true", ("KRONOSDB_DEV", "true")),
    ] {
        let Some((dir, check)) = data_dir_that_trips_a_check(name) else {
            return;
        };
        let server = spawn(&dir, &[env]);
        let levels = finding_levels(&server);
        assert!(
            levels.contains(&("INFO".to_string(), check.to_string())),
            "{levels:?}"
        );
        assert!(
            levels.iter().all(|(level, _)| level == "INFO"),
            "dev mode must not warn: {levels:?}"
        );
        // Nothing hardware-related above INFO, including the summary.
        for record in server.logs() {
            let text = message(&record);
            if text.starts_with("hardware:") || text.starts_with("disk probe") {
                assert_eq!(record["level"], "INFO", "{text}");
            }
        }
        let _ = std::fs::remove_dir_all(dir.parent().unwrap());
    }
}

#[test]
fn dev_mode_can_come_from_the_config_file() {
    let Some((dir, check)) = data_dir_that_trips_a_check("dev-file") else {
        return;
    };
    let config = dir.parent().unwrap().join("kronosdb.toml");
    std::fs::write(&config, "dev = true\n").unwrap();
    let server = spawn(&dir, &[("KRONOSDB_CONFIG", config.to_str().unwrap())]);
    let levels = finding_levels(&server);
    assert!(
        levels.contains(&("INFO".to_string(), check.to_string())),
        "{levels:?}"
    );
    assert!(
        levels.iter().all(|(level, _)| level == "INFO"),
        "{levels:?}"
    );
    let _ = std::fs::remove_dir_all(dir.parent().unwrap());
}
