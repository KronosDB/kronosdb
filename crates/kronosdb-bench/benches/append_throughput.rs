//! Measures append (write) throughput.
//!
//! KronosDB is single-writer, so this measures the raw sequential throughput
//! of the write path including fdatasync. The key variables are:
//! - Batch size (1-500 events per append call)
//! - Segment rotation overhead (small segments force frequent rotation)

use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use kronosdb_eventstore::append::AppendRequest;
use kronosdb_eventstore::event::{AppendEvent, Tag};
use kronosdb_eventstore::store::{EventStoreEngine, StoreOptions};
use std::hint::black_box;
use tempfile::tempdir;

/// `count` events, numbered from `first`. Every append gets events of its
/// own: the same events again, identifiers included, are a retry of the
/// first append and are not written.
fn make_order_events(first: usize, count: usize) -> Vec<AppendEvent> {
    (first..first + count)
        .map(|i| AppendEvent {
            identifier: format!("evt-{i}"),
            name: "OrderPlaced".into(),
            version: "1.0".into(),
            timestamp: 1712345678000 + i as i64,
            payload: vec![0u8; 300],
            metadata: vec![],
            tags: vec![
                Tag::from_str("orderId", &format!("order-{i}")),
                Tag::from_str("customerId", &format!("cust-{}", i % 1000)),
            ],
        })
        .collect()
}

/// Tests how batch size affects throughput.
/// Real clients typically batch 1-100 events per append.
fn batch_size_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("append_batch_size");
    group.sample_size(20);

    for &batch_size in &[1, 10, 50, 100, 500] {
        group.throughput(Throughput::Elements(batch_size as u64));

        group.bench_with_input(
            BenchmarkId::new("events", batch_size),
            &batch_size,
            |b, &batch_size| {
                let dir = tempdir().unwrap();
                let store = EventStoreEngine::create(dir.path()).unwrap();
                let mut next = 0;

                b.iter_batched(
                    || {
                        next += batch_size;
                        make_order_events(next, batch_size)
                    },
                    |events| {
                        black_box(
                            store
                                .append(AppendRequest {
                                    condition: None,
                                    events,
                                })
                                .unwrap(),
                        );
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }
    group.finish();
}

/// Tests sustained throughput with segment rotation.
/// Small segments force rotation every ~600 events, measuring the overhead
/// of sealing a segment + writing its .idx file.
fn sustained_with_rotation(c: &mut Criterion) {
    let mut group = c.benchmark_group("append_sustained");
    group.sample_size(10);
    group.measurement_time(std::time::Duration::from_secs(10));

    let batch_size = 100;
    group.throughput(Throughput::Elements(batch_size as u64));

    group.bench_function("with_rotation", |b| {
        let dir = tempdir().unwrap();
        let opts = StoreOptions {
            max_segment_size: 256 * 1024, // 256KB — rotates every ~600 events
            ..Default::default()
        };
        let store = EventStoreEngine::create_with_store_options(dir.path(), &opts).unwrap();
        let mut next = 0;

        b.iter_batched(
            || {
                next += batch_size;
                make_order_events(next, batch_size)
            },
            |events| {
                black_box(
                    store
                        .append(AppendRequest {
                            condition: None,
                            events,
                        })
                        .unwrap(),
                );
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("no_rotation", |b| {
        let dir = tempdir().unwrap();
        let store = EventStoreEngine::create(dir.path()).unwrap(); // 256MB default
        let mut next = 0;

        b.iter_batched(
            || {
                next += batch_size;
                make_order_events(next, batch_size)
            },
            |events| {
                black_box(
                    store
                        .append(AppendRequest {
                            condition: None,
                            events,
                        })
                        .unwrap(),
                );
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

criterion_group!(benches, batch_size_throughput, sustained_with_rotation);
criterion_main!(benches);
