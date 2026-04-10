//! Measures group commit throughput under concurrent writers.
//!
//! Without group commit, each writer pays its own fsync (~4ms on macOS).
//! With group commit, multiple writers share one fsync per interval.
//! This benchmark directly measures the production-relevant throughput
//! difference.

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use kronosdb_eventstore::append::AppendRequest;
use kronosdb_eventstore::event::{AppendEvent, Tag};
use kronosdb_eventstore::store::{EventStoreEngine, StoreOptions};
use std::hint::black_box;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use tempfile::tempdir;

fn make_event(id: u64) -> AppendEvent {
    AppendEvent {
        identifier: format!("evt-{id}"),
        name: "OrderPlaced".into(),
        version: "1.0".into(),
        timestamp: 1712345678000 + id as i64,
        payload: vec![0u8; 300],
        metadata: vec![],
        tags: vec![
            Tag::from_str("orderId", &format!("order-{id}")),
            Tag::from_str("customerId", &format!("cust-{}", id % 1000)),
        ],
    }
}

/// Measures throughput of N concurrent writers, with and without group commit.
fn concurrent_writers(c: &mut Criterion) {
    let mut group = c.benchmark_group("group_commit");
    group.sample_size(10);
    group.measurement_time(std::time::Duration::from_secs(8));

    for &num_writers in &[1, 4, 16] {
        // --- Without group commit (default: fsync per append) ---
        group.throughput(Throughput::Elements(num_writers as u64));

        group.bench_with_input(
            BenchmarkId::new("no_gc", num_writers),
            &num_writers,
            |b, &num_writers| {
                let dir = tempdir().unwrap();
                let store = Arc::new(EventStoreEngine::create(dir.path()).unwrap());
                let counter = Arc::new(AtomicU64::new(0));

                b.iter(|| {
                    let barrier = Arc::new(Barrier::new(num_writers + 1));
                    let mut handles = Vec::new();

                    for _ in 0..num_writers {
                        let s = Arc::clone(&store);
                        let b = Arc::clone(&barrier);
                        let c = Arc::clone(&counter);
                        handles.push(std::thread::spawn(move || {
                            b.wait();
                            let id = c.fetch_add(1, Ordering::Relaxed);
                            let _ = black_box(s.append(AppendRequest {
                                condition: None,
                                events: vec![make_event(id)],
                            }));
                        }));
                    }

                    barrier.wait(); // release all writers simultaneously
                    for h in handles {
                        h.join().unwrap();
                    }
                });
            },
        );

        // --- With group commit (2ms interval) ---
        group.bench_with_input(
            BenchmarkId::new("gc_2ms", num_writers),
            &num_writers,
            |b, &num_writers| {
                let dir = tempdir().unwrap();
                let opts = StoreOptions {
                    group_commit_interval_ms: 2,
                    ..Default::default()
                };
                let store = Arc::new(
                    EventStoreEngine::create_with_store_options(dir.path(), &opts).unwrap(),
                );
                let counter = Arc::new(AtomicU64::new(0));

                b.iter(|| {
                    let barrier = Arc::new(Barrier::new(num_writers + 1));
                    let mut handles = Vec::new();

                    for _ in 0..num_writers {
                        let s = Arc::clone(&store);
                        let b = Arc::clone(&barrier);
                        let c = Arc::clone(&counter);
                        handles.push(std::thread::spawn(move || {
                            b.wait();
                            let id = c.fetch_add(1, Ordering::Relaxed);
                            let _ = black_box(s.append(AppendRequest {
                                condition: None,
                                events: vec![make_event(id)],
                            }));
                        }));
                    }

                    barrier.wait();
                    for h in handles {
                        h.join().unwrap();
                    }
                });
            },
        );
    }

    group.finish();
}

/// Sustained throughput test: N writers appending continuously for the measurement window.
/// This shows the real-world steady-state throughput.
fn sustained_concurrent_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("sustained_throughput");
    group.sample_size(10);
    group.measurement_time(std::time::Duration::from_secs(5));

    for &num_writers in &[4, 16] {
        // Group commit with 2ms interval.
        group.bench_with_input(
            BenchmarkId::new("gc_2ms_writers", num_writers),
            &num_writers,
            |b, &num_writers| {
                let dir = tempdir().unwrap();
                let opts = StoreOptions {
                    group_commit_interval_ms: 2,
                    ..Default::default()
                };
                let store = Arc::new(
                    EventStoreEngine::create_with_store_options(dir.path(), &opts).unwrap(),
                );
                let counter = Arc::new(AtomicU64::new(0));

                b.iter(|| {
                    // Each "iteration" = all N writers fire one append each.
                    let barrier = Arc::new(Barrier::new(num_writers + 1));
                    let mut handles = Vec::new();

                    for _ in 0..num_writers {
                        let s = Arc::clone(&store);
                        let b = Arc::clone(&barrier);
                        let c = Arc::clone(&counter);
                        handles.push(std::thread::spawn(move || {
                            b.wait();
                            let id = c.fetch_add(1, Ordering::Relaxed);
                            let _ = black_box(s.append(AppendRequest {
                                condition: None,
                                events: vec![make_event(id)],
                            }));
                        }));
                    }

                    barrier.wait();
                    for h in handles {
                        h.join().unwrap();
                    }
                });
            },
        );
    }

    group.finish();
}

criterion_group!(benches, concurrent_writers, sustained_concurrent_throughput);
criterion_main!(benches);
