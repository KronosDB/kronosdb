//! A voter that comes back with an empty data directory.
//!
//! The Kubernetes case: a StatefulSet on local disks, a node is replaced, the
//! pod returns with the same ordinal (so the same node id and the same static
//! peer list) and nothing on its disk. The node has lost its Raft vote, its
//! Raft log and every event it acknowledged, and nothing in its configuration
//! says so.
//!
//! Every test here asserts the SAFE behaviour, so a failure is a finding:
//!
//! - `acked_events_survive_*`: an event acknowledged by two voters while the
//!   third was down must survive one of the two losing its disk and the other
//!   being lost before the wiped one caught up. The remaining majority (the
//!   wiped voter and the one that was down) does not hold the event, so the
//!   only safe outcomes are "not writable until the voter that holds it
//!   returns" or "writable with the event".
//! - `wiped_*_rejoins_a_healthy_pair`: with the other two healthy, the wiped
//!   voter must rejoin and catch up without a second leader, without taking
//!   the healthy pair's leader down, and without losing anything.
//! - `wiped_*_does_not_vote_twice_in_a_term`: the wiped voter must not grant
//!   a vote in a term in which its previous incarnation already voted for
//!   somebody else.
//! - `rolling_replacement_*`: all three wiped one at a time, each caught up
//!   before the next, must lose nothing. Leaders first, every step is also a
//!   failover; followers first, the leader sees a voter it had replicated to
//!   come back empty.
//! - `wiped_voter_is_not_ready_before_it_caught_up`: `/ready` is what
//!   Kubernetes waits for before it takes the next pod, so a wiped voter must
//!   not report it while it holds a fraction of the log.
//! - `two_wiped_voters_*`: past what three voters tolerate. Two empty voters
//!   are a majority; they must not open for writes without the log the third
//!   still holds.
//!
//! The servers are the production binary; the tests only kill processes and
//! delete directories.
//!
//! Two of the outcomes depend on the build. openraft checks a follower whose
//! log went backwards, and a committed entry that differs, with
//! `debug_assert!`: a debug server panics its Raft core there and a release
//! one carries on. To see the second with a debug build:
//!
//! ```text
//! CARGO_PROFILE_DEV_DEBUG_ASSERTIONS=false CARGO_PROFILE_DEV_OVERFLOW_CHECKS=false \
//! CARGO_PROFILE_TEST_DEBUG_ASSERTIONS=true \
//!     cargo test -p kronosdb-eventstore --test wiped_voter -- --test-threads=1
//! ```

mod crash_harness;
use crash_harness::*;

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use kronosdb_eventstore::raft::network::NetworkFactory;
use kronosdb_eventstore::replication::peer::PeerTransportConfig;
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::VoteRequest;
use openraft::{BasicNode, CommittedLeaderId, LogId, Vote};

const READY_TIMEOUT: Duration = crash_harness::STARTUP;
/// Long enough for the slowest election the cluster's Raft settings allow
/// (12 s, see `crash_idle_failover::longest_election_wait`), the fencing
/// claim and a catch-up of a handful of events, with room to spare.
const SETTLE: Duration = Duration::from_secs(45);
const PROBE: Duration = Duration::from_secs(2);

/// An acknowledged event: where the server said it is, and what it holds.
type Acked = Vec<(u64, Vec<u8>)>;

struct Cluster {
    // Field order is drop order: servers die first, then the evidence is
    // copied, then the temp dir goes.
    srvs: Vec<ServerHandle>,
    evidence: EvidenceGuard,
    tmp: tempfile::TempDir,
    listen: [u16; 3],
    admin: [u16; 3],
    peers: Vec<(u64, String)>,
    up: [bool; 3],
    ack_mode: &'static str,
    segment_size: Option<u64>,
    sequence: usize,
}

impl Cluster {
    async fn start(test: &'static str, ack_mode: &'static str) -> Self {
        Self::start_with(test, ack_mode, None).await
    }

    /// [`start`](Self::start) with a segment size, for a log of many
    /// segments.
    async fn start_with(
        test: &'static str,
        ack_mode: &'static str,
        segment_size: Option<u64>,
    ) -> Self {
        let tmp = tempfile::tempdir().expect("tempdir");
        let evidence = EvidenceGuard::new(test, 0, tmp.path());
        evidence.note(format!("ack-mode {ack_mode}"));
        let ports: Vec<u16> = (0..6).map(|_| free_port()).collect();
        let listen = [ports[0], ports[1], ports[2]];
        let admin = [ports[3], ports[4], ports[5]];
        let peers = (0..3)
            .map(|i| (i as u64 + 1, format!("127.0.0.1:{}", listen[i])))
            .collect();
        let mut cluster = Self {
            srvs: Vec::new(),
            evidence,
            tmp,
            listen,
            admin,
            peers,
            up: [false; 3],
            ack_mode,
            segment_size,
            sequence: 0,
        };
        for node in 0..3 {
            std::fs::create_dir_all(cluster.data_dir(node)).expect("mkdir");
            let server = cluster.spawn_handle(node);
            cluster.srvs.push(server);
            cluster.up[node] = true;
        }
        for node in 0..3 {
            cluster.wait_ready(node).await;
        }
        cluster
    }

    fn data_dir(&self, node: usize) -> PathBuf {
        self.tmp
            .path()
            .join(format!("node{}", node + 1))
            .join("data")
    }

    fn spawn_handle(&self, node: usize) -> ServerHandle {
        spawn_server_with_env(
            &SpawnConfig {
                data_dir: self.data_dir(node),
                listen_port: self.listen[node],
                admin_port: self.admin[node],
                node_id: node as u64 + 1,
                peers: self.peers.clone(),
                group_commit_ms: Some(2),
                segment_size: self.segment_size,
                archive: None,
                max_snapshot_size: None,
                append_timeout_secs: None,
            },
            &[("KRONOSDB_ACK_MODE", self.ack_mode)],
        )
        .expect("spawn node")
    }

    fn spawn(&mut self, node: usize) {
        assert!(!self.up[node], "node{} is already running", node + 1);
        self.srvs[node] = self.spawn_handle(node);
        self.up[node] = true;
    }

    fn kill(&mut self, node: usize) {
        self.srvs[node].kill();
        self.up[node] = false;
    }

    /// The node's disk is replaced: everything under its data directory goes.
    /// The server log sits beside it and is kept.
    fn wipe(&mut self, node: usize) {
        assert!(!self.up[node], "node{} must be down to be wiped", node + 1);
        let dir = self.data_dir(node);
        std::fs::remove_dir_all(&dir).expect("wipe data dir");
        std::fs::create_dir_all(&dir).expect("recreate data dir");
        self.evidence
            .note(format!("node{} wiped (empty data dir)", node + 1));
    }

    async fn wait_ready(&self, node: usize) {
        wait_until_ready(self.srvs[node].listen, self.srvs[node].admin, READY_TIMEOUT)
            .await
            .unwrap_or_else(|error| panic!("node{} not ready: {error}", node + 1));
    }

    /// One look at `/ready` and the client port.
    async fn is_ready(&self, node: usize) -> bool {
        if !self.up[node] {
            return false;
        }
        let server = &self.srvs[node];
        tokio::time::timeout(
            PROBE,
            wait_until_ready(server.listen, server.admin, Duration::ZERO),
        )
        .await
        .map(|ready| ready.is_ok())
        .unwrap_or(false)
    }

    async fn metric(&self, node: usize, name: &str) -> Option<u64> {
        if !self.up[node] {
            return None;
        }
        tokio::time::timeout(PROBE, metric(self.srvs[node].admin, name))
            .await
            .ok()
            .flatten()
    }

    /// The node's visible head (the quorum watermark as it knows it).
    async fn head(&self, node: usize) -> Option<u64> {
        self.metric(node, "kronosdb_head_position{context=\"default\"}")
            .await
    }

    async fn gate_open(&self, node: usize) -> bool {
        self.metric(node, "kronosdb_native_write_gate_open").await == Some(1)
    }

    /// Nodes whose metrics say they lead, with the term they say it in.
    async fn leaders(&self) -> Vec<(usize, u64)> {
        let mut leaders = Vec::new();
        for node in 0..3 {
            if self.metric(node, "kronosdb_raft_is_leader").await == Some(1) {
                let term = self
                    .metric(node, "kronosdb_raft_current_term")
                    .await
                    .unwrap_or(0);
                leaders.push((node, term));
            }
        }
        leaders
    }

    async fn wait_leader(&self) -> usize {
        wait_for_raft_leader(&self.srvs, READY_TIMEOUT)
            .await
            .unwrap_or_else(|error| panic!("no single leader: {error}"))
    }

    /// The node that holds the open write gate, once exactly one does.
    async fn wait_writable(&self, timeout: Duration) -> Option<usize> {
        let start = Instant::now();
        loop {
            let mut open = Vec::new();
            for node in 0..3 {
                if self.gate_open(node).await {
                    open.push(node);
                }
            }
            if open.len() == 1 {
                return Some(open[0]);
            }
            if start.elapsed() > timeout {
                return None;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// Each node as its metrics show it.
    async fn view(&self) -> String {
        let mut out = Vec::new();
        for node in 0..3 {
            if !self.up[node] {
                out.push(format!("node{} DOWN", node + 1));
                continue;
            }
            let show = |value: Option<u64>| value.map_or("?".to_string(), |v| v.to_string());
            out.push(format!(
                "node{} leader={} term={} raft_log={} voters={} epoch={} gate={} ready={} head={} local_tail={}",
                node + 1,
                show(self.metric(node, "kronosdb_raft_is_leader").await),
                show(self.metric(node, "kronosdb_raft_current_term").await),
                show(self.metric(node, "kronosdb_raft_last_log_index").await),
                show(self.metric(node, "kronosdb_raft_voters").await),
                show(self.metric(node, "kronosdb_native_epoch").await),
                show(self.metric(node, "kronosdb_native_write_gate_open").await),
                self.is_ready(node).await,
                show(self.head(node).await),
                show(
                    self.metric(node, "kronosdb_local_tail_position{context=\"default\"}")
                        .await
                ),
            ));
        }
        out.join(" | ")
    }

    fn next_event(&mut self, label: &str) -> (NewEvent, Vec<u8>) {
        let n = self.sequence;
        self.sequence += 1;
        let payload = format!("{label}-{n}").into_bytes();
        (
            event(
                format!("wiped-voter-{label}-{n}"),
                1_712_000_000_000 + n as i64,
                "Incremented",
                "1.0",
                payload.clone(),
                vec![(b"agg".to_vec(), b"agg-0".to_vec())],
            ),
            payload,
        )
    }

    /// One append through `node`, retried until it is acknowledged.
    async fn append_one(&mut self, node: usize, label: &str, patience: Duration) -> Option<Acked> {
        let (new_event, payload) = self.next_event(label);
        let deadline = Instant::now() + patience;
        loop {
            let attempt = async {
                let context = connect(self.srvs[node].listen)
                    .await
                    .map_err(|e| format!("connect: {e}"))?
                    .default_context();
                context
                    .append(std::slice::from_ref(&new_event), None)
                    .await
                    .map_err(|e| format!("append: {e}"))
            };
            let last = match tokio::time::timeout(Duration::from_secs(5), attempt).await {
                Ok(Ok(Appended::Ok { first_position, .. })) => {
                    return Some(vec![(first_position, payload)]);
                }
                Ok(Ok(other)) => format!("{other:?}"),
                Ok(Err(error)) => error,
                Err(_) => "timed out after 5s".into(),
            };
            if Instant::now() > deadline {
                self.evidence.note(format!(
                    "append `{label}` through node{} never acknowledged: {last}",
                    node + 1
                ));
                return None;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// `count` acknowledged events through `node`; panics if one never is.
    async fn append_acked(&mut self, node: usize, label: &str, count: usize) -> Acked {
        let mut acked = Vec::new();
        for _ in 0..count {
            let one = self
                .append_one(node, label, Duration::from_secs(30))
                .await
                .unwrap_or_else(|| panic!("append `{label}` never acknowledged"));
            acked.extend(one);
        }
        acked
    }

    /// Every event the node serves, by position.
    async fn events(&self, node: usize) -> Result<BTreeMap<u64, Vec<u8>>, String> {
        let read = async {
            let context = connect(self.srvs[node].listen)
                .await
                .map_err(|e| format!("connect: {e}"))?
                .default_context();
            read_events(&context, &tag_criteria("agg", ["agg-0".to_string()]))
                .await
                .map_err(|e| format!("source: {e}"))
        };
        let events = tokio::time::timeout(Duration::from_secs(15), read)
            .await
            .map_err(|_| "source timed out".to_string())??;
        Ok(events
            .into_iter()
            .map(|(position, _, payload)| (position, payload))
            .collect())
    }

    /// The acknowledged events `node` does not serve as acknowledged; empty
    /// when it serves them all.
    async fn missing_on(&self, node: usize, acked: &Acked) -> Vec<String> {
        let events = match self.events(node).await {
            Ok(events) => events,
            Err(error) => return vec![format!("node{} cannot be read: {error}", node + 1)],
        };
        acked
            .iter()
            .filter_map(|(position, payload)| match events.get(position) {
                Some(found) if found == payload => None,
                Some(found) => Some(format!(
                    "node{}: position {position} was acknowledged as {:?} and now holds {:?}",
                    node + 1,
                    String::from_utf8_lossy(payload),
                    String::from_utf8_lossy(found)
                )),
                None => Some(format!(
                    "node{}: acknowledged position {position} ({:?}) is gone",
                    node + 1,
                    String::from_utf8_lossy(payload)
                )),
            })
            .collect()
    }

    fn server_log(&self, node: usize) -> String {
        let path = self
            .tmp
            .path()
            .join(format!("node{}", node + 1))
            .join(format!("server-{}.log", node + 1));
        std::fs::read_to_string(path).unwrap_or_default()
    }

    /// Lines of every server log that report a panic.
    fn panics(&self) -> Vec<String> {
        (0..3)
            .flat_map(|node| {
                self.server_log(node)
                    .lines()
                    .filter(|line| line.contains("panicked at"))
                    .map(|line| format!("node{}: {line}", node + 1))
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    fn disk_logs(&self) -> Vec<Vec<(String, Vec<u8>)>> {
        (0..3)
            .map(|node| read_valid_event_log(&self.data_dir(node), "default").unwrap_or_default())
            .collect()
    }

    /// Waits until every running node is ready, exactly one leads with its
    /// write gate open, every head is at least `head`, and the valid logs of
    /// the running nodes are byte-identical. `Err` says what was still wrong.
    async fn wait_converged(&self, head: u64, timeout: Duration) -> Result<(), String> {
        let start = Instant::now();
        loop {
            let mut wrong = Vec::new();
            let leaders = self.leaders().await;
            if leaders.len() != 1 {
                wrong.push(format!("leaders (node index, term): {leaders:?}"));
            } else if !self.gate_open(leaders[0].0).await {
                wrong.push(format!(
                    "leader node{} has not opened its write gate",
                    leaders[0].0 + 1
                ));
            }
            for node in (0..3).filter(|node| self.up[*node]) {
                if !self.is_ready(node).await {
                    wrong.push(format!("node{} is not ready", node + 1));
                }
                match self.head(node).await {
                    Some(found) if found >= head => {}
                    found => wrong.push(format!(
                        "node{} head is {found:?}, expected at least {head}",
                        node + 1
                    )),
                }
            }
            if wrong.is_empty() {
                let logs: Vec<_> = self
                    .disk_logs()
                    .into_iter()
                    .enumerate()
                    .filter(|(node, _)| self.up[*node])
                    .map(|(_, log)| log)
                    .collect();
                match describe_log_divergence(&logs) {
                    None => return Ok(()),
                    Some(detail) => wrong.push(format!("logs differ: {detail}")),
                }
            }
            if start.elapsed() > timeout {
                return Err(format!("{}; {}", wrong.join("; "), self.view().await));
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    /// Moves leadership off `node` if it has it, so that `node` is a follower
    /// the leader has replicated to.
    async fn make_follower(&mut self, node: usize) -> usize {
        let leader = self.wait_leader().await;
        if leader != node {
            return leader;
        }
        self.kill(node);
        let leader = self.wait_leader().await;
        self.spawn(node);
        self.wait_ready(node).await;
        assert_ne!(leader, node);
        leader
    }
}

/// Fails the test with everything that went wrong, after the evidence notes
/// say the same.
fn finish(cluster: &Cluster, violations: Vec<String>) {
    if violations.is_empty() {
        cluster.evidence.finish();
        return;
    }
    for violation in &violations {
        cluster.evidence.note(format!("VIOLATION: {violation}"));
    }
    panic!(
        "{} unsafe outcome(s):\n  - {}",
        violations.len(),
        violations.join("\n  - ")
    );
}

/// The last record of a valid log is a WatermarkCheckpoint.
fn ends_in_checkpoint(log: &[(String, Vec<u8>)]) -> bool {
    let Some((_, bytes)) = log.last() else {
        return false;
    };
    let (mut offset, mut last_flags) = (13, None);
    while offset + 9 <= bytes.len() {
        let record_len = u32::from_le_bytes(bytes[offset + 4..offset + 8].try_into().unwrap());
        last_flags = Some(bytes[offset + 8]);
        offset += 9 + record_len as usize - 1;
    }
    last_flags == Some(0x22)
}

// ── 1. The loss sequence ────────────────────────────────────────────────

/// Events are acknowledged by `wiped` and `holder` while `lagging` is down.
/// `wiped` loses its disk; `holder` is lost before `wiped` could catch up from
/// it; `wiped` and `lagging` come up and are a majority of the configured
/// voters. Later `holder` returns.
///
/// With `checkpointed` the two that hold the events are left running until
/// the leader has written a `WatermarkCheckpoint` after them (it does every
/// five seconds), so the holder still knows they were committed when it
/// restarts. Without it the holder restarts with its watermark at the last
/// checkpoint, below those events.
async fn acked_events_survive(
    test: &'static str,
    wiped: usize,
    ack_mode: &'static str,
    checkpointed: bool,
) {
    let holder = if wiped == 0 { 1 } else { 0 };
    let lagging = 2;
    let mut c = Cluster::start(test, ack_mode).await;

    let leader = c.wait_leader().await;
    let acked = c.append_acked(leader, "base", 5).await;
    c.wait_converged(5, SETTLE).await.expect("initial cluster");
    loss_sequence(c, acked, wiped, holder, lagging, checkpointed).await;
}

/// The loss sequence with the voter that misses the late events being the
/// leader of the time: it is killed while it leads, the other two elect a
/// leader and acknowledge more events, one of them loses its disk and the
/// other is lost. The old leader comes back holding its own committed vote,
/// and openraft has such a node lead again at once, without an election: the
/// wiped voter, which has forgotten the later term, follows it.
async fn acked_events_survive_beside_an_old_leader(test: &'static str) {
    let mut c = Cluster::start(test, "durable").await;

    let leader = c.wait_leader().await;
    let acked = c.append_acked(leader, "base", 5).await;
    c.wait_converged(5, SETTLE).await.expect("initial cluster");
    let lagging = leader;
    let others: Vec<usize> = (0..3).filter(|node| *node != lagging).collect();
    loss_sequence(c, acked, others[0], others[1], lagging, false).await;
}

/// The part of the loss sequence after the first events: see
/// [`acked_events_survive`].
async fn loss_sequence(
    mut c: Cluster,
    mut acked: Acked,
    wiped: usize,
    holder: usize,
    lagging: usize,
    checkpointed: bool,
) {
    let mut violations = Vec::new();

    // One voter is down while more events are acknowledged: only the other
    // two hold them.
    c.kill(lagging);
    let leader = c.wait_leader().await;
    assert_ne!(leader, lagging);
    acked.extend(c.append_acked(leader, "late", 5).await);
    let acked_head = acked
        .iter()
        .map(|(position, _)| position + 1)
        .max()
        .unwrap();
    c.wait_converged(acked_head, SETTLE)
        .await
        .expect("both remaining voters hold the acknowledged events");
    if checkpointed {
        let start = Instant::now();
        loop {
            let logs = c.disk_logs();
            if ends_in_checkpoint(&logs[wiped]) && ends_in_checkpoint(&logs[holder]) {
                break;
            }
            assert!(
                start.elapsed() < Duration::from_secs(20),
                "no WatermarkCheckpoint after the acknowledged events"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        c.evidence
            .note("both holders' logs end in a WatermarkCheckpoint");
    }
    c.evidence.note(format!(
        "{} events acknowledged, head {acked_head}; leader node{}; lagging node{} was down for the last 5; {}",
        acked.len(),
        leader + 1,
        lagging + 1,
        c.view().await
    ));

    // The disk of one of the two is replaced, and the other is lost before
    // the replaced one caught up. The voter that was down is back.
    c.kill(wiped);
    c.kill(holder);
    c.wipe(wiped);
    c.spawn(wiped);
    c.spawn(lagging);

    // A majority of the configured voters is up, and neither holds the last
    // five acknowledged events. Safe: nobody opens the write gate. Unsafe: a
    // leader opens it below the acknowledged head.
    let writable = c.wait_writable(SETTLE).await;
    c.evidence.note(format!(
        "wiped node{} + lagging node{} up, holder node{} down: writable={:?}; {}",
        wiped + 1,
        lagging + 1,
        holder + 1,
        writable.map(|node| node + 1),
        c.view().await
    ));
    if let Some(node) = writable {
        let head = c.head(node).await;
        if head.is_none_or(|head| head < acked_head) {
            violations.push(format!(
                "node{} opened its write gate at head {head:?}, below the acknowledged head \
                 {acked_head}, with the only voter holding those events down",
                node + 1
            ));
        }
        let missing = c.missing_on(node, &acked).await;
        if !missing.is_empty() {
            violations.push(format!(
                "the writable leader does not serve {} acknowledged event(s): {}",
                missing.len(),
                missing.join("; ")
            ));
        }
        // What a client sees next: is an acknowledged position handed out
        // a second time?
        if let Some(again) = c
            .append_one(node, "after-loss", Duration::from_secs(10))
            .await
        {
            let (position, _) = &again[0];
            c.evidence.note(format!(
                "a new append was acknowledged at position {position}"
            ));
            if acked.iter().any(|(taken, _)| taken == position) {
                violations.push(format!(
                    "position {position} was acknowledged to two different appends"
                ));
            }
        }
    }

    // The voter that holds the acknowledged events returns.
    c.spawn(holder);
    let converged = c.wait_converged(acked_head, SETTLE).await;
    c.evidence.note(format!(
        "holder node{} back: converged={converged:?}; {}",
        holder + 1,
        c.view().await
    ));
    if let Err(detail) = &converged {
        violations.push(format!(
            "the cluster did not come back whole after the holder returned: {detail}"
        ));
    }
    for node in 0..3 {
        let missing = c.missing_on(node, &acked).await;
        if !missing.is_empty() {
            violations.push(format!("after the holder returned: {}", missing.join("; ")));
        }
    }
    let panics = c.panics();
    if !panics.is_empty() {
        violations.push(format!("a server panicked: {}", panics.join(" || ")));
    }
    finish(&c, violations);
}

async fn bounded(name: &str, test: impl Future<Output = ()>) {
    tokio::time::timeout(Duration::from_secs(420), test)
        .await
        .unwrap_or_else(|_| panic!("{name} timed out"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn acked_events_survive_a_wiped_bootstrapper_durable() {
    let name = "wiped_voter_loss_node1_durable";
    bounded(name, acked_events_survive(name, 0, "durable", false)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn acked_events_survive_a_wiped_bootstrapper_written() {
    let name = "wiped_voter_loss_node1_written";
    bounded(name, acked_events_survive(name, 0, "written", false)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn acked_events_survive_a_wiped_non_bootstrapper_durable() {
    let name = "wiped_voter_loss_node2_durable";
    bounded(name, acked_events_survive(name, 1, "durable", false)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn acked_events_survive_a_wiped_non_bootstrapper_written() {
    let name = "wiped_voter_loss_node2_written";
    bounded(name, acked_events_survive(name, 1, "written", false)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn acked_events_survive_a_wiped_bootstrapper_after_a_checkpoint() {
    let name = "wiped_voter_loss_node1_checkpointed";
    bounded(name, acked_events_survive(name, 0, "durable", true)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn acked_events_survive_a_wiped_non_bootstrapper_after_a_checkpoint() {
    let name = "wiped_voter_loss_node2_checkpointed";
    bounded(name, acked_events_survive(name, 1, "durable", true)).await;
}

/// A voter that started empty abstains from votes and from a leader's claim,
/// but it still follows whichever leader reaches it, and a restarted old
/// leader needs no vote to lead. If that leader served its log before it had
/// reconciled its claim, the wiped voter would catch up with a stale log,
/// count as whole, and the two would open for writes below the acknowledged
/// head.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn acked_events_survive_a_wiped_voter_beside_an_old_leader() {
    let name = "wiped_voter_loss_old_leader";
    bounded(name, acked_events_survive_beside_an_old_leader(name)).await;
}

// ── 2. A wiped voter and a healthy pair ─────────────────────────────────

/// What a background writer saw.
#[derive(Default)]
struct WriterLog {
    acked: Acked,
    /// When each acknowledgement arrived.
    at: Vec<Instant>,
}

async fn writer(
    addr: String,
    log: Arc<Mutex<WriterLog>>,
    stop: tokio::sync::watch::Receiver<bool>,
) {
    let mut context: Option<Context> = None;
    let mut n = 0usize;
    while !*stop.borrow() {
        if context.is_none() {
            context = connect(&addr).await.ok().map(|c| c.default_context());
        }
        let Some(open) = context.as_ref() else {
            tokio::time::sleep(Duration::from_millis(50)).await;
            continue;
        };
        let payload = format!("live-{n}").into_bytes();
        let new_event = event(
            format!("wiped-voter-live-{n}"),
            1_713_000_000_000 + n as i64,
            "Incremented",
            "1.0",
            payload.clone(),
            vec![(b"agg".to_vec(), b"agg-0".to_vec())],
        );
        match tokio::time::timeout(Duration::from_secs(5), open.append(&[new_event], None)).await {
            Ok(Ok(Appended::Ok { first_position, .. })) => {
                let mut log = log.lock().unwrap();
                log.acked.push((first_position, payload));
                log.at.push(Instant::now());
                n += 1;
            }
            // An append whose outcome is unknown may have landed: never send
            // the same event again, or the read-back could not tell.
            _ => {
                context = None;
                n += 1;
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// `wiped` loses its disk and restarts while the other two are healthy and
/// taking writes.
async fn wiped_voter_rejoins(test: &'static str, wiped: usize) {
    let mut c = Cluster::start(test, "durable").await;
    let mut violations = Vec::new();

    let leader = c.make_follower(wiped).await;
    let mut acked = c.append_acked(leader, "base", 5).await;
    c.wait_converged(5, SETTLE).await.expect("initial cluster");
    let term_before = c
        .metric(leader, "kronosdb_raft_current_term")
        .await
        .expect("leader term");
    c.evidence.note(format!(
        "before the wipe: leader node{} term {term_before}; {}",
        leader + 1,
        c.view().await
    ));

    // Writes go through the healthy follower, which forwards them.
    let through = (0..3)
        .find(|node| *node != wiped && *node != leader)
        .unwrap();
    let log = Arc::new(Mutex::new(WriterLog::default()));
    let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
    let writing = tokio::spawn(writer(
        format!("127.0.0.1:{}", c.listen[through]),
        Arc::clone(&log),
        stop_rx,
    ));
    tokio::time::sleep(Duration::from_secs(1)).await;

    c.kill(wiped);
    c.wipe(wiped);
    let wiped_at = Instant::now();
    c.spawn(wiped);

    // Watch the three for a while.
    let mut two_leaders_same_term = Vec::new();
    let mut two_leaders = 0usize;
    let mut wiped_led = false;
    let mut terms_seen = std::collections::BTreeSet::new();
    let mut wiped_voters_seen = std::collections::BTreeSet::new();
    let watch_until = Instant::now() + Duration::from_secs(30);
    while Instant::now() < watch_until {
        let leaders = c.leaders().await;
        if leaders.len() > 1 {
            two_leaders += 1;
            let mut terms: Vec<u64> = leaders.iter().map(|(_, term)| *term).collect();
            terms.sort_unstable();
            terms.dedup();
            if terms.len() < leaders.len() {
                two_leaders_same_term.push(format!("{leaders:?}"));
            }
        }
        wiped_led |= leaders.iter().any(|(node, _)| *node == wiped);
        for node in (0..3).filter(|node| *node != wiped) {
            if let Some(term) = c.metric(node, "kronosdb_raft_current_term").await {
                terms_seen.insert(term);
            }
        }
        if let Some(voters) = c.metric(wiped, "kronosdb_raft_voters").await {
            wiped_voters_seen.insert(voters);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let _ = stop_tx.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(10), writing).await;

    let (live, longest_stall) = {
        let log = log.lock().unwrap();
        let after: Vec<_> = log.at.iter().filter(|at| **at > wiped_at).collect();
        let mut longest = Duration::ZERO;
        let mut previous = wiped_at;
        for at in after {
            longest = longest.max(*at - previous);
            previous = *at;
        }
        // The writer was stopped a moment ago: the time since its last
        // acknowledgement counts too, or a writer that never recovered
        // would show no stall at all.
        longest = longest.max(Instant::now().saturating_duration_since(previous));
        (log.acked.clone(), longest)
    };
    c.evidence.note(format!(
        "30s after the wipe: {} live appends acknowledged, longest gap between acknowledgements \
         {longest_stall:?}; healthy pair's terms seen {terms_seen:?} (was {term_before}); \
         samples with two leaders {two_leaders}; wiped node led: {wiped_led}; voter counts the \
         wiped node reported {wiped_voters_seen:?}; {}",
        live.len(),
        c.view().await
    ));
    acked.extend(live);
    let acked_head = acked
        .iter()
        .map(|(position, _)| position + 1)
        .max()
        .unwrap();

    if !two_leaders_same_term.is_empty() {
        violations.push(format!(
            "two nodes led in the same term: {}",
            two_leaders_same_term.join(", ")
        ));
    }
    if wiped_led {
        violations.push("the wiped node reported itself leader".to_string());
    }
    if terms_seen.iter().any(|term| *term != term_before) {
        violations.push(format!(
            "the healthy pair changed term ({term_before} -> {terms_seen:?}): the returning \
             node cost it its leader"
        ));
    }
    if longest_stall > Duration::from_secs(5) {
        violations.push(format!(
            "appends through the healthy pair stalled for {longest_stall:?}"
        ));
    }
    if let Err(detail) = c.wait_converged(acked_head, SETTLE).await {
        violations.push(format!(
            "the wiped node did not rejoin and catch up: {detail}"
        ));
    }
    for node in 0..3 {
        let missing = c.missing_on(node, &acked).await;
        if !missing.is_empty() {
            violations.push(missing.join("; "));
        }
    }
    let panics = c.panics();
    if !panics.is_empty() {
        violations.push(format!("a server panicked: {}", panics.join(" || ")));
    }
    c.evidence.note(format!("at the end: {}", c.view().await));
    finish(&c, violations);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn wiped_bootstrapper_rejoins_a_healthy_pair() {
    let name = "wiped_voter_rejoin_node1";
    bounded(name, wiped_voter_rejoins(name, 0)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn wiped_non_bootstrapper_rejoins_a_healthy_pair() {
    let name = "wiped_voter_rejoin_node2";
    bounded(name, wiped_voter_rejoins(name, 1)).await;
}

// ── 2b. The vote a wiped voter forgot ───────────────────────────────────

/// The vote a node's Raft store holds on disk (`raft/vote.bin`).
fn stored_vote(data_dir: &std::path::Path) -> Option<Vote<u64>> {
    let bytes = std::fs::read(data_dir.join("raft").join("vote.bin")).ok()?;
    bincode::deserialize(&bytes).ok()
}

/// `wiped` voted in term T (its vote is on disk). It loses its disk. Another
/// voter asks it for its vote in the same term T.
async fn wiped_voter_votes_once(test: &'static str, wiped: usize) {
    let mut c = Cluster::start(test, "durable").await;

    // Two failovers first, so the term is past what a lone, freshly
    // initialized node reaches by itself before the test asks.
    for round in 0..2 {
        let leader = c.wait_leader().await;
        c.kill(leader);
        let next = c.wait_leader().await;
        c.spawn(leader);
        c.wait_ready(leader).await;
        c.evidence.note(format!(
            "failover {round}: node{} -> node{}",
            leader + 1,
            next + 1
        ));
    }
    let leader = c.make_follower(wiped).await;
    c.append_acked(leader, "base", 3).await;
    c.wait_converged(3, SETTLE)
        .await
        .expect("cluster before the wipe");

    let last_log_index = c
        .metric(leader, "kronosdb_raft_last_log_index")
        .await
        .expect("leader's last log index");
    for node in 0..3 {
        c.kill(node);
    }
    let before = stored_vote(&c.data_dir(wiped)).expect("the voter's vote is on disk");
    let term = before.leader_id().get_term();
    let voted_for = before.leader_id().voted_for().expect("a vote names a node");
    c.evidence.note(format!(
        "node{} holds vote {before} on disk (term {term}, for node {voted_for})",
        wiped + 1
    ));
    assert!(
        term >= 3,
        "two failovers should have reached term 3, got {term}"
    );
    assert_ne!(
        voted_for,
        wiped as u64 + 1,
        "the wiped node must be a follower"
    );
    // Somebody else, who did not get this node's vote in that term.
    let candidate = (1..=3u64)
        .find(|id| *id != voted_for && *id != wiped as u64 + 1)
        .unwrap();

    c.wipe(wiped);
    c.spawn(wiped);
    let start = Instant::now();
    while tokio::net::TcpStream::connect(c.srvs[wiped].listen)
        .await
        .is_err()
    {
        assert!(
            start.elapsed() < READY_TIMEOUT,
            "the wiped node never listened"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // A candidate whose log is as long as the cluster's was.
    let request = VoteRequest::new(
        Vote::new(term, candidate),
        Some(LogId::new(
            CommittedLeaderId::new(term, voted_for),
            last_log_index,
        )),
    );
    let mut factory = NetworkFactory::new(PeerTransportConfig::default());
    let mut connection = factory
        .new_client(
            wiped as u64 + 1,
            &BasicNode::new(format!("127.0.0.1:{}", c.listen[wiped])),
        )
        .await;
    let response = connection
        .vote(request, RPCOption::new(Duration::from_secs(5)))
        .await
        .expect("vote RPC");
    c.evidence.note(format!(
        "asked node{} for its vote in term {term} for node {candidate}: granted={} (its vote is \
         now {}); on disk now: {:?}",
        wiped + 1,
        response.vote_granted,
        response.vote,
        stored_vote(&c.data_dir(wiped)).map(|vote| vote.to_string())
    ));
    assert!(
        response.vote.leader_id().get_term() <= term,
        "inconclusive: the wiped node had already moved past term {term} by itself ({})",
        response.vote
    );
    let violations = if response.vote_granted {
        vec![format!(
            "node{} granted its vote in term {term} to node {candidate}; before its disk was \
             wiped it had voted for node {voted_for} in that same term ({before})",
            wiped + 1
        )]
    } else {
        Vec::new()
    };
    finish(&c, violations);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn wiped_bootstrapper_does_not_vote_twice_in_a_term() {
    let name = "wiped_voter_vote_node1";
    bounded(name, wiped_voter_votes_once(name, 0)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn wiped_non_bootstrapper_does_not_vote_twice_in_a_term() {
    let name = "wiped_voter_vote_node2";
    bounded(name, wiped_voter_votes_once(name, 1)).await;
}

// ── 3. Rolling replacement ──────────────────────────────────────────────

/// Every node is wiped and restarted, one at a time, and each is caught up
/// (ready, same head, byte-identical log) before the next goes.
///
/// With `followers_first` each step replaces a follower while one is left,
/// so the leader sees a voter it had replicated to come back empty; without
/// it each step replaces the leader while it is one not yet replaced, so
/// every step is also a failover.
async fn rolling_replacement(test: &'static str, ack_mode: &'static str, followers_first: bool) {
    let mut c = Cluster::start(test, ack_mode).await;
    let mut violations = Vec::new();

    let leader = c.wait_leader().await;
    let mut acked = c.append_acked(leader, "base", 5).await;
    c.wait_converged(5, SETTLE).await.expect("initial cluster");

    let mut left = vec![0, 1, 2];
    'replace: while !left.is_empty() {
        let head = acked
            .iter()
            .map(|(position, _)| position + 1)
            .max()
            .unwrap();
        let leader_before = c.wait_leader().await;
        let node = left
            .iter()
            .copied()
            .find(|node| (*node != leader_before) == followers_first)
            .unwrap_or(left[0]);
        left.retain(|other| *other != node);
        c.evidence.note(format!(
            "replacing node{} (leader is node{}); {}",
            node + 1,
            leader_before + 1,
            c.view().await
        ));
        c.kill(node);
        c.wipe(node);
        c.spawn(node);
        let started = Instant::now();
        if let Err(detail) = c.wait_converged(head, Duration::from_secs(90)).await {
            violations.push(format!(
                "after replacing node{} (leader was node{}) the cluster did not converge in 90s: \
                 {detail}",
                node + 1,
                leader_before + 1
            ));
            break 'replace;
        }
        c.evidence.note(format!(
            "node{} replaced and caught up in {:?}; {}",
            node + 1,
            started.elapsed(),
            c.view().await
        ));
        let leader = c.wait_leader().await;
        for _ in 0..3 {
            match c
                .append_one(leader, "rolling", Duration::from_secs(30))
                .await
            {
                Some(one) => acked.extend(one),
                None => {
                    violations.push(format!(
                        "an append after replacing node{} was never acknowledged",
                        node + 1
                    ));
                    break 'replace;
                }
            }
        }
    }

    let head = acked
        .iter()
        .map(|(position, _)| position + 1)
        .max()
        .unwrap();
    if let Err(detail) = c.wait_converged(head, SETTLE).await {
        violations.push(format!("the cluster did not end converged: {detail}"));
    }
    for node in 0..3 {
        let missing = c.missing_on(node, &acked).await;
        if !missing.is_empty() {
            violations.push(missing.join("; "));
        }
    }
    let panics = c.panics();
    if !panics.is_empty() {
        violations.push(format!("a server panicked: {}", panics.join(" || ")));
    }
    c.evidence.note(format!("at the end: {}", c.view().await));
    finish(&c, violations);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn rolling_replacement_leaders_first_loses_nothing_durable() {
    let name = "wiped_voter_rolling_leaders_durable";
    bounded(name, rolling_replacement(name, "durable", false)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn rolling_replacement_leaders_first_loses_nothing_written() {
    let name = "wiped_voter_rolling_leaders_written";
    bounded(name, rolling_replacement(name, "written", false)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn rolling_replacement_followers_first_loses_nothing_durable() {
    let name = "wiped_voter_rolling_followers_durable";
    bounded(name, rolling_replacement(name, "durable", true)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn rolling_replacement_followers_first_loses_nothing_written() {
    let name = "wiped_voter_rolling_followers_written";
    bounded(name, rolling_replacement(name, "written", true)).await;
}

// ── 4. Ready before caught up ───────────────────────────────────────────

/// Kubernetes takes the next pod of a rolling update, and counts a pod
/// towards a PodDisruptionBudget, as soon as it reports ready. A voter that
/// reports ready while it still holds none of the log lets the next voter be
/// taken down inside the window the loss sequence above needs.
async fn wiped_voter_ready_only_when_caught_up(test: &'static str) {
    const SEGMENT: u64 = 256 * 1024;
    let wiped = 1;
    let mut c = Cluster::start_with(test, "durable", Some(SEGMENT)).await;
    let mut violations = Vec::new();
    let leader = c.make_follower(wiped).await;

    // A log of well over a hundred segments.
    let context = connect(c.srvs[leader].listen)
        .await
        .expect("connect leader")
        .default_context();
    let mut head = 0;
    for batch in 0..160 {
        let events: Vec<NewEvent> = (0..32)
            .map(|i| {
                event(
                    format!("bulk-{batch}-{i}"),
                    1_714_000_000_000 + batch,
                    "Bulk",
                    "1.0",
                    vec![b'x'; 8 * 1024],
                    vec![(b"bulk".to_vec(), b"1".to_vec())],
                )
            })
            .collect();
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            match context.append(&events, None).await {
                Ok(Appended::Ok {
                    first_position,
                    count,
                    ..
                }) => {
                    head = first_position + count as u64;
                    break;
                }
                other => {
                    assert!(Instant::now() < deadline, "bulk append failed: {other:?}");
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        }
    }
    drop(context);
    c.wait_converged(head, SETTLE)
        .await
        .expect("cluster before the wipe");
    c.evidence
        .note(format!("acknowledged head {head}; {}", c.view().await));

    c.kill(wiped);
    c.wipe(wiped);
    c.spawn(wiped);
    let restarted = Instant::now();
    let mut ready_at: Option<(Duration, Option<u64>)> = None;
    let mut caught_up_at = None;
    let mut ready_behind = 0usize;
    let mut lowest_ready_head = u64::MAX;
    while restarted.elapsed() < Duration::from_secs(90) {
        let ready = c.is_ready(wiped).await;
        let now_head = c.head(wiped).await;
        let local_tail = c
            .metric(wiped, "kronosdb_local_tail_position{context=\"default\"}")
            .await;
        if ready {
            ready_at.get_or_insert((restarted.elapsed(), local_tail));
            // Read after `ready`, so a head that moved in between only
            // makes this look better than it was.
            if local_tail.is_none_or(|tail| tail < head) {
                ready_behind += 1;
                lowest_ready_head = lowest_ready_head.min(local_tail.unwrap_or(0));
            }
        }
        if local_tail.is_some_and(|tail| tail >= head) && now_head.is_some_and(|h| h >= head) {
            caught_up_at = Some(restarted.elapsed());
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    c.evidence.note(format!(
        "wiped node{}: first ready after {:?} holding {:?} of {head} events; caught up after \
         {caught_up_at:?}; samples ready while behind: {ready_behind}",
        wiped + 1,
        ready_at.map(|(at, _)| at),
        ready_at.and_then(|(_, tail)| tail),
    ));
    if caught_up_at.is_none() {
        violations.push(format!(
            "the wiped node did not catch up in 90s: {}",
            c.view().await
        ));
    }
    if ready_behind > 0 {
        violations.push(format!(
            "the wiped node reported ready while it held as few as {lowest_ready_head} of the \
             {head} acknowledged events ({ready_behind} samples; first ready {:?} after restart, \
             caught up {caught_up_at:?} after)",
            ready_at.map(|(at, _)| at)
        ));
    }
    finish(&c, violations);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn wiped_voter_is_not_ready_before_it_caught_up() {
    let name = "wiped_voter_ready";
    bounded(name, wiped_voter_ready_only_when_caught_up(name)).await;
}

// ── 5. Two voters wiped at once ─────────────────────────────────────────

/// Beyond what three voters tolerate: two lose their disks together and one
/// keeps the whole log. Nothing can be promised about availability, but the
/// two empty voters are a majority, and the safe outcome is still that they
/// do not open for writes without the acknowledged events, and that the
/// survivor's copy is not discarded.
async fn two_wiped_voters(test: &'static str) {
    let survivor = 2;
    let mut c = Cluster::start(test, "durable").await;
    let mut violations = Vec::new();
    // A cluster that has lived a little: without failovers the wiped
    // bootstrapper would lead again in term 1 and claim the same epoch as
    // before, which only looks like the survivor's.
    for _ in 0..2 {
        let leader = c.wait_leader().await;
        c.kill(leader);
        c.wait_leader().await;
        c.spawn(leader);
        c.wait_ready(leader).await;
    }
    // The survivor is a follower: a leader that restarts with its own
    // committed vote leads again at once, which decides the outcome before
    // the two empty voters have a say.
    let leader = c.make_follower(survivor).await;
    let acked = c.append_acked(leader, "base", 10).await;
    let acked_head = 10;
    c.wait_converged(acked_head, SETTLE)
        .await
        .expect("initial cluster");
    let survivor_log = c.disk_logs().swap_remove(survivor);

    for node in 0..3 {
        c.kill(node);
    }
    c.wipe(0);
    c.wipe(1);
    for node in 0..3 {
        c.spawn(node);
    }

    let writable = c.wait_writable(SETTLE).await;
    c.evidence.note(format!(
        "nodes 1 and 2 wiped, node 3 intact: writable={:?}; {}",
        writable.map(|node| node + 1),
        c.view().await
    ));
    match writable {
        Some(node) => {
            let head = c.head(node).await;
            if head.is_none_or(|head| head < acked_head) {
                violations.push(format!(
                    "node{} opened its write gate at head {head:?}, below the acknowledged head \
                     {acked_head}",
                    node + 1
                ));
            }
            // Give followers the time to be brought in line with the leader.
            tokio::time::sleep(Duration::from_secs(10)).await;
            for node in 0..3 {
                let missing = c.missing_on(node, &acked).await;
                if !missing.is_empty() {
                    violations.push(format!(
                        "{} acknowledged event(s) not served: {}",
                        missing.len(),
                        missing[0]
                    ));
                }
            }
        }
        None => c
            .evidence
            .note("nobody opened the write gate: unavailable, which is safe"),
    }
    let after = c.disk_logs().swap_remove(survivor);
    if relate_logs(&survivor_log, &after) == LogRelation::Diverged
        || relate_logs(&survivor_log, &after) == LogRelation::SecondIsPrefix
    {
        violations.push(format!(
            "the surviving voter's log was cut back or replaced: before {}, after {}",
            describe_log_tail(&survivor_log),
            describe_log_tail(&after)
        ));
    }
    let panics = c.panics();
    if !panics.is_empty() {
        violations.push(format!("a server panicked: {}", panics.join(" || ")));
    }
    c.evidence.note(format!("at the end: {}", c.view().await));
    finish(&c, violations);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn two_wiped_voters_do_not_open_for_writes_without_the_log() {
    let name = "wiped_voter_two_wiped";
    bounded(name, two_wiped_voters(name)).await;
}
