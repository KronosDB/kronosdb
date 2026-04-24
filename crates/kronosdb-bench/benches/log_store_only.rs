//! PERF-02 microbench: drives `LogStore::append_test` directly, bypassing
//! the Raft cluster manager and the event-path state machine. Measures the
//! log store in isolation per CONTEXT.md D-10. Requires the
//! `bench-instrumentation` feature (log_store.rs's `append_test` is gated
//! on `#[cfg(any(test, feature = "bench-instrumentation"))]`).
//!
//! Per CONTEXT.md D-10: PERF-02 gets a dedicated bench file that exercises
//! the log store in isolation so the 10k ev/s target measures what it
//! claims. Uses the same `bench-instrumentation` feature and Criterion
//! conventions as `raft_append_baseline.rs`. Some duplication with that
//! bench is acceptable; signal clarity dominates.
//!
//! Workload cells (Phase-7 D-13 subset — only the axes meaningful at the
//! log-store level; conditional/unconditional + selectivity are
//! MEANINGLESS here because no DCB evaluation happens at this layer):
//!   - `log_store_only__batch1`
//!   - `log_store_only__batch10`
//!   - `log_store_only__batch100`
//!
//! Per-append payload mirrors D-06 exactly (300B body, 2 tags
//! `orderId` + `customerId`) so the log-side byte-shape matches the
//! `raft_append_baseline` cells byte-for-byte and the A/B comparison is
//! clean. Events are wrapped inside a `RaftRequest::Append` variant and
//! then into an `Entry<TypeConfig>::EntryPayload::Normal` — the same
//! shape openraft produces when `client_write` lands on a leader.
//!
//! Expected fsync behavior (LOG-04): log-store-only path fires 1 fsync
//! per group-commit batch (active-segment fdatasync), occasionally 2
//! when `committed.bin` is folded on the same batch. The bench asserts
//! `fsync_count` strictly advances over each `b.iter()` closure body;
//! zero-advance would indicate group commit silently stopped firing.
//!
//! What this bench deliberately does NOT exercise (all covered by
//! `raft_append_baseline.rs` at the E2E layer):
//!   - The Raft cluster manager / engine — no replication, no election
//!   - The event-path apply routine — no DCB check, no event-segment append
//!   - The public event store append — no tag-index update, no head bump

use std::fs::{self, File};
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use openraft::{CommittedLeaderId, Entry, EntryPayload, LogId};
use tempfile::TempDir;
use tokio::runtime::Runtime;

use kronosdb_eventstore::event::{AppendEvent, Tag};
use kronosdb_eventstore::raft::bench_instrumentation::{self as bi, Region, VecSink};
use kronosdb_eventstore::raft::log_store::{LogStore, LogStoreConfig};
use kronosdb_eventstore::raft::types::{NodeId, RaftAppendEvent, RaftRequest, TypeConfig};

// --- Workload matrix (Phase-7 D-13 subset — batch axis only at log-store layer) ---

const BATCH_SIZES: &[usize] = &[1, 10, 100];

// --- Payload (D-06: mirrors raft_append_baseline.rs::make_order_events byte-for-byte) ---

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

// --- Entry construction (wraps events in RaftRequest::Append) ---

/// Build `batch` `Entry<TypeConfig>` records, each carrying a single
/// `RaftRequest::Append` variant. The Raft index sequence is contiguous,
/// starting at `first_index`. This mirrors the per-entry shape that
/// `openraft` feeds into `RaftLogStorage::append` during normal operation.
fn make_log_entries(batch: usize, first_index: u64) -> Vec<Entry<TypeConfig>> {
    let events = make_order_events(batch, first_index);
    // Each entry is one RaftRequest::Append carrying exactly one event,
    // matching the log-entry-per-event shape openraft produces.
    events
        .into_iter()
        .enumerate()
        .map(|(i, ev)| {
            let log_id = LogId::<NodeId> {
                leader_id: CommittedLeaderId::new(1, 0),
                index: first_index + i as u64,
            };
            let req = RaftRequest::Append {
                context: "default".into(),
                events: vec![RaftAppendEvent::from_event(&ev)],
                condition: None,
            };
            Entry {
                log_id,
                payload: EntryPayload::Normal(req),
            }
        })
        .collect()
}

// --- Fixture (1 LogStore on tempdir, no cluster, no state machine) ---

struct Fixture {
    _tmp: TempDir, // drops the tempdir at end of cell
    store: LogStore,
    rt: Runtime,
}

fn boot_log_store() -> Fixture {
    let tmp = tempfile::tempdir().expect("tempdir");
    let rt = Runtime::new().expect("tokio runtime");
    // Default LogStoreConfig — same config cluster_test.rs uses (via
    // cluster.rs) so log-side throughput numbers are apples-to-apples
    // with raft_append_baseline's log-side column.
    let store = LogStore::new(tmp.path(), LogStoreConfig::default()).expect("log store");
    Fixture { _tmp: tmp, store, rt }
}

// --- Record emission (matches Phase-1 JSONL schema consumed by aggregate_baseline) ---

fn records_dir() -> PathBuf {
    // Resolve against the workspace root (two levels up from the crate
    // manifest: crates/kronosdb-bench → crates → <workspace>).
    // Cargo sets CARGO_MANIFEST_DIR to the crate dir when compiling benches.
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

fn cell_key(batch: usize) -> String {
    format!("log_store_only__batch{batch}")
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

// --- The sweep (PERF-02 — batch axis only) ---

fn bench_log_store_only(c: &mut Criterion) {
    let mut group = c.benchmark_group("log_store_only");
    // Conservative defaults — log-store-only path is materially faster than
    // the Raft-wrapped E2E bench so sample_size can afford to be higher
    // than raft_append_baseline's 10, but 20 keeps total wall-clock
    // predictable while the harness is being iterated on. Criterion CLI
    // overrides (-- --sample-size N) still apply for longer runs.
    group.sample_size(20);
    group.measurement_time(Duration::from_secs(30));

    for &batch in BATCH_SIZES {
        let cell = cell_key(batch);
        group.throughput(Throughput::Elements(batch as u64));

        group.bench_with_input(
            BenchmarkId::new(&cell, batch),
            &batch,
            |b, &batch| {
                let mut fx = boot_log_store();

                let sink = Arc::new(VecSink::new());
                bi::install_sink(sink.clone());

                // One JSONL file per cell. Truncate at cell boot.
                let mut out = File::create(records_dir().join(format!("{cell}.jsonl")))
                    .expect("open jsonl");

                // Sanity probe: the helper must fire at least one fsync
                // per batch (group commit). If it doesn't, the bench is
                // meaningless — panic loudly, mirror raft_append_baseline's
                // defensive probe pattern.
                let probe_before = bi::fsync_count();
                let probe_entries = make_log_entries(batch, 1);
                fx.rt
                    .block_on(fx.store.append_test(probe_entries))
                    .expect("probe append");
                let probe_after = bi::fsync_count();
                assert!(
                    probe_after > probe_before,
                    "log_store_only probe fired 0 fsyncs — group commit silently disabled? (before={probe_before}, after={probe_after})"
                );

                // Drain probe samples so the first recorded iter reflects
                // only the first measured append.
                let _ = sink.drain();

                // Each b.iter closure body needs a monotonically increasing
                // Raft index; start after the probe (which consumed
                // indices 1..=batch).
                let mut next_index: u64 = (batch as u64) + 1;
                let mut iter_idx: u64 = 0;
                b.iter(|| {
                    iter_idx += 1;
                    let entries = make_log_entries(batch, next_index);
                    next_index += batch as u64;

                    let fsyncs_before = bi::fsync_count();
                    fx.rt
                        .block_on(fx.store.append_test(entries))
                        .expect("append_test");
                    let fsyncs_after = bi::fsync_count();

                    assert!(
                        fsyncs_after > fsyncs_before,
                        "log_store_only iter {iter_idx} fired 0 fsyncs — group commit stopped firing mid-bench"
                    );

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

    group.finish();
}

criterion_group!(benches, bench_log_store_only);
criterion_main!(benches);

// Explicit tie to the log-store-side Region variants — documents that the
// bench exercises `LogGroupCommit` and `LogRecordWrite` (and, on committed-
// dirty batches, `LogAtomicWrite`); it does NOT exercise `ApplyEventPath`
// or `SegmentAppend` because the state machine is bypassed. If the Region
// enum grows a log-layer variant in a future phase, add it here so the
// bench's coverage-map stays current.
#[allow(dead_code)]
fn _log_store_regions() -> [&'static str; 4] {
    [
        Region::LogGroupCommit.as_str(),
        Region::LogRecordWrite.as_str(),
        Region::LogIndexRebuild.as_str(),
        Region::LogAtomicWrite.as_str(),
    ]
}
