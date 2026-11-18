//! A node restarted on its own data: a real server process whose JSON logs
//! are read back. A routine restart must not log errors.

use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde_json::Value;

mod support;
use support::free_port;

struct Server {
    child: Child,
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

    fn has(&self, prefix: &str) -> bool {
        self.logs().iter().any(|r| message(r).starts_with(prefix))
    }

    /// Waits until a log record's message starts with `prefix`.
    fn wait_for(&self, prefix: &str) {
        let deadline = Instant::now() + Duration::from_secs(60);
        while !self.has(prefix) {
            assert!(
                Instant::now() < deadline,
                "no log record starting with {prefix:?}; logs: {:#?}",
                self.logs()
            );
            std::thread::sleep(Duration::from_millis(50));
        }
    }
}

fn message(record: &Value) -> &str {
    record["fields"]["message"].as_str().unwrap_or("")
}

/// Starts a single node with JSON logs on `data_dir`.
fn spawn(data_dir: &Path) -> Server {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_kronosdb-server"));
    support::inherit_only_stdio(&mut cmd);
    cmd.current_dir(data_dir.parent().unwrap())
        .env("KRONOSDB_DEV", "1")
        .env("KRONOSDB_LISTEN", format!("127.0.0.1:{}", free_port()))
        .env(
            "KRONOSDB_ADMIN_LISTEN",
            format!("127.0.0.1:{}", free_port()),
        )
        .env("KRONOSDB_DATA_DIR", data_dir)
        .env("KRONOSDB_LOG_FORMAT", "json")
        .env("RUST_LOG", "info")
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
    Server { child, logs }
}

fn errors(server: &Server) -> Vec<Value> {
    server
        .logs()
        .into_iter()
        .filter(|r| r["level"] == "ERROR")
        .collect()
}

#[test]
fn a_restarted_single_node_logs_no_error() {
    let work = support::work_dir("restart-logs");
    let dir = work.join("data");

    let first = spawn(&dir);
    first.wait_for("native leader gate opened");
    drop(first);

    // The same data: the Raft group is already initialized.
    let second = spawn(&dir);
    second.wait_for("native leader gate opened");
    let errors = errors(&second);
    assert!(
        errors.is_empty(),
        "a routine restart logged errors: {errors:#?}"
    );
}
