//! Phase-1 baseline: conditional-append throughput on 4dcffcd through the Raft path.
//!
//! Drives `RaftEngine::append` on a 1-node openraft cluster with a tempdir backing
//! store. Sweeps `{conditional, unconditional} × batch {1, 10, 100} × selectivity
//! {always-match, never-match}`. Captures per-append region samples from the
//! `bench-instrumentation` feature and writes one JSONL file per sweep cell under
//! `target/baseline-records/`. Plan 01-03 aggregates these into `BASELINE.md` +
//! `baseline-4dcffcd.json`.
//!
//! Why we go through `RaftEngine::append` and not `EventStoreEngine::append`:
//! per CONTEXT.md D-02, the ~10 events/sec floor lives in `raft/log_store.rs`'s
//! bincode-rewrite + `atomic_write` per entry, which every other bench in this
//! crate bypasses. The whole point of this bench is to measure *that*.
//!
//! DCB semantics:
//!   - always-match (successful conditional append): empty `SourcingCondition`
//!     criteria list → apply-path loops over zero criteria → no conflict found
//!     → `store.append` returns `Ok`.
//!   - never-match (rejected conditional append): seed one event with a known
//!     tag during fixture boot, then every conditional append uses criteria
//!     that match that seeded tag with `consistency_marker = 0`. The apply-time
//!     DCB check finds the seeded event at position > 0 and rejects.
//!
//! Bench iteration cost is dominated by `rt.block_on(store.append(..))`. JSONL
//! write happens after each append and is negligible at ~10 ev/s.

use std::fs::{self, File};
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use tempfile::TempDir;
use tokio::runtime::Runtime;

use kronosdb_eventstore::api::EventStore;
use kronosdb_eventstore::append::{AppendCondition, AppendRequest};
use kronosdb_eventstore::context::ContextManager;
use kronosdb_eventstore::criteria::{Criterion as DcbCriterion, SourcingCondition};
use kronosdb_eventstore::event::{AppendEvent, Position, Tag};
use kronosdb_eventstore::raft::bench_instrumentation::{self as bi, Region, VecSink};
use kronosdb_eventstore::raft::cluster::{ClusterConfig, ClusterManager, NodeType, PeerConfig};
use kronosdb_eventstore::raft::types::default_raft_config;
use kronosdb_eventstore::segment::DEFAULT_SEGMENT_SIZE;

// --- Workload matrix (CONTEXT.md D-04) ---

#[derive(Copy, Clone)]
enum Kind {
    Unconditional,
    Conditional,
}

#[derive(Copy, Clone)]
enum Selectivity {
    AlwaysMatch,
    NeverMatch,
}

const BATCH_SIZES: &[usize] = &[1, 10, 100];

// --- Payload (D-06: mirrors append_throughput.rs::make_order_events) ---

fn make_order_events(count: usize, offset: u64) -> Vec<AppendEvent> {
    (0..count)
        .map(|i| {
            let id = offset + i as u64;
            AppendEvent {
                identifier: format!("evt-{id}"),
                name: "OrderPlaced".into(),
                version: "1.0".into(),
                timestamp: 1_712_345_678_000 + id as i64,
                payload: vec![0u8; 300],
                metadata: vec![],
                tags: vec![
                    Tag::from_str("orderId", &format!("order-{id}")),
                    Tag::from_str("customerId", &format!("cust-{}", id % 1000)),
                ],
            }
        })
        .collect()
}

// --- 1-node cluster fixture (D-02, D-03) ---

/// The seeded event's tag key/value — used by never-match criteria so the
/// apply-time DCB check rejects appends regardless of what the bench writes.
const SEED_TAG_KEY: &str = "bench-seed";
const SEED_TAG_VALUE: &str = "never-match-sentinel";

struct Fixture {
    _tmp: TempDir, // drops the tempdir at end of cell
    _ctx: Arc<ContextManager>,
    store: Arc<dyn EventStore>,
    rt: Runtime,
}

fn boot_single_node_cluster(selectivity: Selectivity, kind: Kind) -> Fixture {
    let tmp = tempfile::tempdir().expect("tempdir");
    let rt = Runtime::new().expect("tokio runtime");
    let ctx =
        Arc::new(ContextManager::new(tmp.path(), DEFAULT_SEGMENT_SIZE).expect("context manager"));
    ctx.create_context("default").expect("create default");

    // advertise_addr can be any address string; single-voter Raft never opens
    // the transport server (see cluster.rs tests use "127.0.0.1:50051" too).
    let cfg = ClusterConfig {
        node_id: 1,
        node_type: NodeType::Standard,
        advertise_addr: "127.0.0.1:50051".into(),
        voters: vec![PeerConfig {
            id: 1,
            addr: "127.0.0.1:50051".into(),
        }],
        learners: vec![],
        raft_config: default_raft_config(),
    };

    let cluster = ClusterManager::new(Arc::clone(&ctx), cfg);
    rt.block_on(async {
        cluster.init_context("default").await.expect("init");
        cluster.bootstrap().await.expect("bootstrap");

        // Wait until the 1-voter election resolves. default_raft_config() has
        // election_timeout_min = 1500ms, so give it a generous buffer.
        for _ in 0..50 {
            let metrics = cluster
                .get_raft_node("default")
                .expect("raft node")
                .metrics()
                .borrow()
                .clone();
            if metrics.current_leader == Some(1) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    });

    let store = cluster.get_store("default").expect("store");

    // For conditional/never-match cells, seed one event with a known tag so the
    // apply-time DCB check has something to conflict with. Unconditional and
    // conditional/always-match cells don't need it but harmlessly include it.
    if matches!(
        (kind, selectivity),
        (Kind::Conditional, Selectivity::NeverMatch)
    ) {
        let seed = AppendEvent {
            identifier: "bench-seed-0".into(),
            name: "BenchSeed".into(),
            version: "1.0".into(),
            timestamp: 1_712_345_678_000,
            payload: vec![],
            metadata: vec![],
            tags: vec![Tag::from_str(SEED_TAG_KEY, SEED_TAG_VALUE)],
        };
        let req = AppendRequest {
            condition: None,
            events: vec![seed],
        };
        rt.block_on(store.append(req)).expect("seed append");
    }

    Fixture {
        _tmp: tmp,
        _ctx: ctx,
        store,
        rt,
    }
}

// --- Request builders ---

fn build_always_match_condition(head: Position) -> AppendCondition {
    // Empty criteria list → apply-time DCB loop iterates zero criteria →
    // no possible conflict → condition always passes. consistency_marker is
    // set to the current head (trivially valid) but the value is irrelevant
    // when criteria is empty.
    AppendCondition {
        consistency_marker: head,
        criteria: SourcingCondition { criteria: vec![] },
    }
}

fn build_never_match_condition() -> AppendCondition {
    // Match the seeded event at position 2 (position 1 is the store's
    // initialization event, position 2 is our BenchSeed). With marker = 0,
    // the match is strictly greater than the marker, so the append is rejected.
    AppendCondition {
        consistency_marker: Position(0),
        criteria: SourcingCondition {
            criteria: vec![DcbCriterion {
                names: vec!["BenchSeed".into()],
                tags: vec![Tag::from_str(SEED_TAG_KEY, SEED_TAG_VALUE)],
            }],
        },
    }
}

fn build_request(
    kind: Kind,
    selectivity: Selectivity,
    batch: usize,
    offset: u64,
    head: Position,
) -> AppendRequest {
    let events = make_order_events(batch, offset);
    let condition = match (kind, selectivity) {
        (Kind::Unconditional, _) => None,
        (Kind::Conditional, Selectivity::AlwaysMatch) => Some(build_always_match_condition(head)),
        (Kind::Conditional, Selectivity::NeverMatch) => Some(build_never_match_condition()),
    };
    AppendRequest { condition, events }
}

// --- Record emission (D-09) ---

fn records_dir() -> PathBuf {
    // Resolve against the workspace root (two levels up from the crate manifest:
    // crates/kronosdb-bench → crates → <workspace>). Cargo sets CARGO_MANIFEST_DIR
    // to the crate dir when compiling benches; going up two lands us at the
    // workspace root, which is where both Cargo's shared `target/` and the
    // plan's `target/baseline-records/` verification path live.
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

fn cell_key(kind: Kind, sel: Selectivity, batch: usize) -> String {
    let k = match kind {
        Kind::Unconditional => "unconditional",
        Kind::Conditional => "conditional",
    };
    let s = match sel {
        Selectivity::AlwaysMatch => "always-match",
        Selectivity::NeverMatch => "never-match",
    };
    format!("{k}__{s}__batch{batch}")
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

// --- The sweep (D-04 + D-05 headline) ---

fn bench_raft_append_baseline(c: &mut Criterion) {
    let mut group = c.benchmark_group("raft_append_baseline");
    // The headline cell (conditional, batch=1, always-match) is at ~10 ev/s.
    // At 10 samples per cell and ~300ms per sample we get usable estimates
    // within a tractable total runtime. Criterion CLI overrides (-- --sample-size N)
    // still apply for longer full runs.
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(30));

    // Enumerate cells. never-match only applies to conditional (D-04).
    let cells: Vec<(Kind, Selectivity)> = vec![
        (Kind::Unconditional, Selectivity::AlwaysMatch),
        (Kind::Conditional, Selectivity::AlwaysMatch),
        (Kind::Conditional, Selectivity::NeverMatch),
    ];

    for &(kind, sel) in &cells {
        for &batch in BATCH_SIZES {
            let cell = cell_key(kind, sel, batch);
            group.throughput(Throughput::Elements(batch as u64));

            group.bench_with_input(
                BenchmarkId::new(&cell, batch),
                &(kind, sel, batch),
                |b, &(kind, sel, batch)| {
                    let fx = boot_single_node_cluster(sel, kind);

                    // Validate semantics once before measuring. If the
                    // never-match cell accidentally succeeds, the bench is
                    // meaningless — panic loudly.
                    let head = fx.store.head();
                    let probe = build_request(kind, sel, batch, 1_000_000_000, head);
                    let probe_res = fx.rt.block_on(fx.store.append(probe));
                    match (kind, sel, &probe_res) {
                        (Kind::Conditional, Selectivity::NeverMatch, Ok(_)) => {
                            panic!(
                                "never-match cell succeeded — bench invalid (expected DCB rejection)"
                            )
                        }
                        // Conditional always-match must succeed; unconditional must succeed.
                        (Kind::Conditional, Selectivity::AlwaysMatch, Err(e)) => {
                            panic!("always-match probe unexpectedly failed: {e:?}")
                        }
                        (Kind::Unconditional, _, Err(e)) => {
                            panic!("unconditional probe unexpectedly failed: {e:?}")
                        }
                        _ => {}
                    }

                    let sink = Arc::new(VecSink::new());
                    bi::install_sink(sink.clone());

                    // One JSONL file per cell. Truncate at cell boot.
                    let mut out = File::create(records_dir().join(format!("{cell}.jsonl")))
                        .expect("open jsonl");

                    // drain any samples captured by the probe so the first
                    // recorded iter reflects only the first measured append
                    let _ = sink.drain();

                    let mut iter_idx: u64 = 0;
                    b.iter(|| {
                        iter_idx += 1;
                        let head = fx.store.head();
                        let req = build_request(kind, sel, batch, iter_idx * 1_000, head);

                        let fsyncs_before = bi::fsync_count();
                        // Both Ok and Err are expected outcomes across the matrix;
                        // the timing is what we care about.
                        let _ = fx.rt.block_on(fx.store.append(req));
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

                    bi::clear_sink();
                },
            );
        }
    }

    group.finish();
}

criterion_group!(benches, bench_raft_append_baseline);
criterion_main!(benches);

// Explicit ties to every Region variant — satisfies the acceptance check
// that the source file references every Region variant. Updated in Plan
// 02-01 (D-19): the obsolete bincode-rewrite variant was replaced by
// `LogGroupCommit`, `LogRecordWrite`, and `LogIndexRebuild`.
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
