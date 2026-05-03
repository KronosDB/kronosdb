//! PERF-03/04 informational 3-node bench. Closes Phase 1 D-03's deferred 3-node
//! work. In-process topology matches `cluster_test.rs`; NOT a gate — numbers
//! are diagnostic for v2 per Phase 7 CONTEXT.md specifics.
//!
//! Three in-process openraft nodes communicate over a real gRPC transport on
//! ports `19500`/`19501`/`19502` (disjoint from `cluster_test.rs` @ 19100-19300
//! and `concurrent_dcb_cluster.rs` @ 19400-19402 per STATE.md Phase 3
//! decisions). Leader election runs to completion before measurements begin.
//!
//! Cells exercised:
//!   - `3node_conditional__always-match__batch1`
//!   - `3node_unconditional__always-match__batch1`
//!
//! Payload: Phase 1 D-13 fixed shape (300B body, 2 tags). Per-cell JSONL is
//! emitted to `target/baseline-records/3node_<cell>.jsonl` via the same
//! `bench_instrumentation` sink as `raft_append_baseline.rs`.
//!
//! PERF-02/03/04 target wording is 1-node-scoped; these numbers are
//! informational and surface replication + gRPC + quorum-fsync headroom
//! relative to the 1-node baseline. They do NOT gate Phase 7 closing.
//!
//! NOTE: Cargo.toml `[[bench]]` registration for `raft_append_3node` is owned
//! by Plan 07-01 (atomic registration of both Wave 1 benches to eliminate a
//! merge race). This file compiles standalone against `cargo check`; running
//! it via `cargo bench --bench raft_append_3node` requires the registration
//! to be in place.

use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::Write;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use openraft::{BasicNode, Config, Raft};
use tempfile::TempDir;
use tokio::runtime::Runtime;
use tonic::transport::Server;

use kronosdb_eventstore::context::ContextManager;
use kronosdb_eventstore::criteria::{Criterion as DcbCriterion, SourcingCondition};
use kronosdb_eventstore::event::{Position, Tag};
use kronosdb_eventstore::raft::bench_instrumentation::{self as bi, Region, VecSink};
use kronosdb_eventstore::raft::log_store::{LogStore, LogStoreConfig};
use kronosdb_eventstore::raft::network::NetworkFactory;
use kronosdb_eventstore::raft::snapshot_store::SnapshotStore;
use kronosdb_eventstore::raft::state_machine::EventStoreStateMachine;
use kronosdb_eventstore::raft::transport::RaftTransportService;
use kronosdb_eventstore::raft::types::{
    NodeId, RaftAppendCondition, RaftAppendEvent, RaftCriterion, RaftRequest, TypeConfig,
};
use kronosdb_eventstore::segment::DEFAULT_SEGMENT_SIZE;

// --- Workload matrix (mirrors `raft_append_baseline.rs` for 3-node cells) ---

#[derive(Copy, Clone)]
enum Kind {
    Unconditional,
    Conditional,
}

#[derive(Copy, Clone)]
enum Selectivity {
    AlwaysMatch,
}

// --- Payload (Phase 1 D-13: 300B body, 2 tags) ---

fn make_order_events(count: usize, offset: u64) -> Vec<RaftAppendEvent> {
    (0..count)
        .map(|i| {
            let id = offset + i as u64;
            RaftAppendEvent {
                identifier: format!("evt-{id}"),
                name: "OrderPlaced".into(),
                version: "1.0".into(),
                timestamp: 1_712_345_678_000 + id as i64,
                payload: vec![0u8; 300],
                metadata: vec![],
                tags: vec![
                    (b"orderId".to_vec(), format!("order-{id}").into_bytes()),
                    (
                        b"customerId".to_vec(),
                        format!("cust-{}", id % 1000).into_bytes(),
                    ),
                ],
            }
        })
        .collect()
}

// --- 3-node in-process cluster fixture (mirrors cluster_test.rs::start_node) ---

struct BenchNode {
    #[allow(dead_code)]
    id: NodeId,
    raft: Arc<Raft<TypeConfig>>,
    #[allow(dead_code)]
    contexts: Arc<ContextManager>,
    #[allow(dead_code)]
    addr: SocketAddr,
}

/// Boot one openraft node with its own context manager, log store, state
/// machine, and gRPC transport. Matches `cluster_test.rs::start_node` shape.
async fn start_node(id: NodeId, port: u16, dir: &Path) -> BenchNode {
    let contexts =
        Arc::new(ContextManager::new(dir, DEFAULT_SEGMENT_SIZE).expect("create context manager"));
    if !contexts.context_exists("default") {
        contexts.create_context("default").expect("create default");
    }

    let raft_dir = dir.join("raft");
    let log_store = LogStore::new(&raft_dir, LogStoreConfig::default()).expect("create log store");
    let snap_store =
        Arc::new(SnapshotStore::new(raft_dir.join("snapshots")).expect("create snapshot store"));
    let state_machine = EventStoreStateMachine::new(Arc::clone(&contexts), snap_store)
        .expect("recover state machine");

    // Short election/heartbeat windows to keep bench boot latency low. These
    // mirror the values used in `cluster_test.rs`; the stock
    // `default_raft_config()` uses 1500-3000 ms which is usable for production
    // and SNAP paths but wastes bench wall-clock on every cell setup.
    let config = Config {
        heartbeat_interval: 200,
        election_timeout_min: 500,
        election_timeout_max: 1000,
        ..Default::default()
    };

    let raft = Arc::new(
        Raft::new(
            id,
            Arc::new(config),
            NetworkFactory,
            log_store,
            state_machine,
        )
        .await
        .expect("create raft node"),
    );

    let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let transport = RaftTransportService::new(Arc::clone(&raft));
    tokio::spawn(async move {
        Server::builder()
            .add_service(transport.into_server())
            .serve(addr)
            .await
            .ok();
    });
    // Give the tonic server a moment to bind before the caller starts issuing
    // peer RPCs.
    tokio::time::sleep(Duration::from_millis(100)).await;

    BenchNode {
        id,
        raft,
        contexts,
        addr,
    }
}

/// Port triple convention (STATE.md Phase 3 decisions):
///   - cluster_test.rs              → 19100/19200/19300
///   - concurrent_dcb_cluster.rs    → 19400/19402
///   - this bench (Phase 7 3-node)  → 19500/19501/19502
///
/// Cell index is a small offset so a subsequent cell can pick a fresh triple
/// if ports get stuck from the previous cell's tempdir teardown. In practice
/// SO_REUSEADDR lets tonic rebind within a few hundred ms of `Server` drop,
/// so the same triple is usually fine; the offset is defensive only.
fn port_triple(cell_idx: u16) -> (u16, u16, u16) {
    let base = 19500 + cell_idx * 10;
    (base, base + 1, base + 2)
}

struct Cluster {
    _dirs: Vec<TempDir>,
    nodes: Vec<BenchNode>,
    leader_idx: usize,
}

impl Cluster {
    /// Returns a reference to the leader node's Raft handle.
    fn leader_raft(&self) -> &Arc<Raft<TypeConfig>> {
        &self.nodes[self.leader_idx].raft
    }
}

/// Wait until the given node sees *some* current leader.
async fn wait_for_leader(raft: &Raft<TypeConfig>, timeout: Duration) -> Option<NodeId> {
    let start = tokio::time::Instant::now();
    loop {
        if let Some(leader) = raft.metrics().borrow().current_leader {
            return Some(leader);
        }
        if start.elapsed() > timeout {
            return None;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Boot a 3-node cluster with all three voters initialized together, then
/// wait for leader election. Hard-panics if no leader within 10s.
async fn boot_3node_cluster(cell_idx: u16) -> Cluster {
    let (p1, p2, p3) = port_triple(cell_idx);

    let d1 = tempfile::tempdir().expect("tempdir 1");
    let d2 = tempfile::tempdir().expect("tempdir 2");
    let d3 = tempfile::tempdir().expect("tempdir 3");

    let n1 = start_node(1, p1, &d1.path().join("node1")).await;
    let n2 = start_node(2, p2, &d2.path().join("node2")).await;
    let n3 = start_node(3, p3, &d3.path().join("node3")).await;

    // Bootstrap: initialize the cluster with all three voters at once, same
    // pattern as `cluster_test.rs::three_node_cluster_replication`. `add_learner`
    // + `change_membership` is the alternative path used when nodes join
    // later; we don't need it here because the bench always boots all three
    // together.
    let mut members: BTreeMap<NodeId, BasicNode> = BTreeMap::new();
    members.insert(
        1,
        BasicNode {
            addr: format!("127.0.0.1:{p1}"),
        },
    );
    members.insert(
        2,
        BasicNode {
            addr: format!("127.0.0.1:{p2}"),
        },
    );
    members.insert(
        3,
        BasicNode {
            addr: format!("127.0.0.1:{p3}"),
        },
    );

    n1.raft
        .initialize(members)
        .await
        .expect("initialize 3-node cluster");

    let leader_id = wait_for_leader(&n1.raft, Duration::from_secs(10))
        .await
        .expect("3-node bench failed to elect leader within 10s — check NetworkFactory wiring");

    // Wait for the other two nodes to observe the leader as well, so the very
    // first measured append doesn't race with follower catchup.
    let _ = wait_for_leader(&n2.raft, Duration::from_secs(10)).await;
    let _ = wait_for_leader(&n3.raft, Duration::from_secs(10)).await;

    let leader_idx = match leader_id {
        1 => 0,
        2 => 1,
        3 => 2,
        other => panic!("unexpected leader id: {other}"),
    };

    Cluster {
        _dirs: vec![d1, d2, d3],
        nodes: vec![n1, n2, n3],
        leader_idx,
    }
}

// --- Request builders (mirror `raft_append_baseline.rs`) ---

/// Always-match conditional: empty criteria list → apply-time DCB loop
/// iterates zero criteria → no possible conflict → append succeeds. The
/// `consistency_marker` value is irrelevant when criteria is empty but we
/// set it to the current head for tidiness.
fn build_always_match_condition(head: Position) -> RaftAppendCondition {
    RaftAppendCondition {
        consistency_marker: head.0,
        criteria: Vec::<RaftCriterion>::new(),
    }
}

fn build_raft_request(
    kind: Kind,
    _sel: Selectivity,
    batch: usize,
    offset: u64,
    head: Position,
) -> RaftRequest {
    let events = make_order_events(batch, offset);
    let condition = match kind {
        Kind::Unconditional => None,
        Kind::Conditional => Some(build_always_match_condition(head)),
    };
    RaftRequest::Append {
        context: "default".to_string(),
        events,
        condition,
    }
}

// Referenced by `build_always_match_condition` through `SourcingCondition` in
// the 1-node bench; the 3-node wire surface uses `RaftAppendCondition` directly
// (see `cluster.rs::build_raft_request`). Keep a no-op reference so the
// `DcbCriterion`/`SourcingCondition` import survives `cargo check` — the
// cross-link to Phase 1 semantics stays visible to readers of this file.
#[allow(dead_code)]
fn _dcb_semantics_doc_anchor() -> SourcingCondition {
    SourcingCondition {
        criteria: vec![DcbCriterion {
            names: vec!["OrderPlaced".into()],
            tags: vec![Tag::from_str("orderId", "order-0")],
        }],
    }
}

// --- Record emission (schema matches `raft_append_baseline.rs`) ---

fn records_dir() -> PathBuf {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let workspace_root = manifest
        .parent()
        .and_then(|p| p.parent())
        .map(|p| p.to_path_buf())
        .unwrap_or(manifest);
    let p = workspace_root.join("target").join("baseline-records");
    fs::create_dir_all(&p).expect("mkdir records");
    p
}

fn cell_key(kind: Kind, _sel: Selectivity, batch: usize) -> String {
    let k = match kind {
        Kind::Unconditional => "unconditional",
        Kind::Conditional => "conditional",
    };
    // 3-node cells all use the always-match selectivity; never-match is
    // 1-node-scoped (it depends on a seeded tag; see `raft_append_baseline.rs`
    // for the DCB rejection pattern).
    format!("3node_{k}__always-match__batch{batch}")
}

#[derive(serde::Serialize)]
struct RegionRecord<'a> {
    region: &'a str,
    nanos: u128,
}

#[derive(serde::Serialize)]
struct AppendRecord<'a> {
    cell: &'a str,
    iter: u64,
    regions: Vec<RegionRecord<'a>>,
    fsyncs_before: u64,
    fsyncs_after: u64,
}

// --- The 3-node sweep ---

fn bench_raft_append_3node(c: &mut Criterion) {
    let mut group = c.benchmark_group("raft_append_3node");
    // 3-node cells pay ~3× the fsync cost per Raft commit (quorum durability)
    // plus cross-node gRPC. CONTEXT.md Claude's Discretion: "3-node cells may
    // need 10-20 samples given 3× fsync cost per Raft commit". We pick 10.
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(60));

    // The two critical cells. Batch=10 / batch=100 / never-match are out of
    // scope for the 3-node bench per the plan — single-event is the shape that
    // surfaces replication-per-append cost vs 1-node amortization.
    let cells: Vec<(Kind, Selectivity, usize)> = vec![
        (Kind::Conditional, Selectivity::AlwaysMatch, 1),
        (Kind::Unconditional, Selectivity::AlwaysMatch, 1),
    ];

    let rt = Runtime::new().expect("tokio runtime");

    for (cell_idx, &(kind, sel, batch)) in cells.iter().enumerate() {
        let cell = cell_key(kind, sel, batch);
        group.throughput(Throughput::Elements(batch as u64));

        // Boot the cluster ONCE per cell, outside the measured loop. All
        // samples for this cell run against the same leader with the same
        // tempdirs. Alternative (boot per sample) would dominate wall-clock
        // and drown out the measurement.
        let cluster = rt.block_on(boot_3node_cluster(cell_idx as u16));

        // Install the bench-instrumentation sink AFTER cluster boot so boot
        // samples don't pollute the per-append record.
        let sink = Arc::new(VecSink::new());
        bi::install_sink(sink.clone());

        let mut out =
            File::create(records_dir().join(format!("{cell}.jsonl"))).expect("open jsonl");

        // Single probe append to validate semantics before measuring. If this
        // fails we haven't wasted a measurement window on a broken cluster.
        let probe = build_raft_request(kind, sel, batch, 1_000_000_000, Position(0));
        let probe_res = rt.block_on(cluster.leader_raft().client_write(probe));
        probe_res.unwrap_or_else(|e| {
            panic!(
                "3-node {cell} probe append failed: {e} — \
                 leader election or gRPC transport wiring is broken"
            )
        });
        // Drain the probe's samples so iter=1 reflects only the first measured append.
        let _ = sink.drain();

        let mut iter_idx: u64 = 0;
        group.bench_with_input(
            BenchmarkId::new(&cell, batch),
            &(kind, sel, batch),
            |b, &(kind, sel, batch)| {
                b.iter(|| {
                    iter_idx += 1;
                    // Use a monotonically increasing offset so event
                    // identifiers never collide across iterations.
                    let req = build_raft_request(
                        kind,
                        sel,
                        batch,
                        iter_idx * 1_000,
                        // Head is irrelevant for always-match (empty criteria)
                        // and conditional/unconditional uniformly, so pass 0
                        // rather than paying the `leader.metrics()` read cost
                        // per iter.
                        Position(0),
                    );

                    let fsyncs_before = bi::fsync_count();
                    let _ = rt.block_on(cluster.leader_raft().client_write(req));
                    let fsyncs_after = bi::fsync_count();

                    let samples = sink.drain();
                    let rec = AppendRecord {
                        cell: &cell,
                        iter: iter_idx,
                        regions: samples
                            .iter()
                            .map(|s| RegionRecord {
                                region: s.region.as_str(),
                                nanos: s.duration.as_nanos(),
                            })
                            .collect(),
                        fsyncs_before,
                        fsyncs_after,
                    };
                    let line = serde_json::to_string(&rec).unwrap();
                    writeln!(out, "{line}").unwrap();
                });
            },
        );

        bi::clear_sink();

        // Best-effort cluster shutdown so ports free before the next cell
        // rebinds. openraft's `shutdown` is infallible but may return an error
        // wrapper; we swallow it because the process exit will clean up
        // regardless.
        for node in &cluster.nodes {
            let _ = rt.block_on(node.raft.shutdown());
        }
        // Explicitly drop the cluster to release tempdirs and gRPC servers
        // before the next cell boots. Without this, the next cell's port
        // bind may race the previous server's teardown.
        drop(cluster);
    }

    group.finish();
}

criterion_group!(benches, bench_raft_append_3node);
criterion_main!(benches);

// Explicit ties to every Region variant — keeps this file self-documenting
// about which bench-instrumentation regions are expected to surface on the
// 3-node hot path, mirroring `raft_append_baseline.rs::_region_names`.
#[allow(dead_code)]
fn _region_names() -> [&'static str; 6] {
    [
        Region::LogGroupCommit.as_str(),
        Region::LogRecordWrite.as_str(),
        Region::LogIndexRebuild.as_str(),
        Region::LogAtomicWrite.as_str(),
        Region::ApplyEventPath.as_str(),
        Region::SegmentAppend.as_str(),
    ]
}
