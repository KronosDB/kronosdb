//! Reads across many sealed segments, warm and from a freshly opened engine.
//!
//! Sealed segment indexes are mapped, not loaded into a heap cache, so the
//! difference between the two is mapping and first-touch checking, not
//! deserializing indexes.

use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use kronosdb_bench::*;
use kronosdb_eventstore::event::Position;
use kronosdb_eventstore::store::{EventStoreEngine, StoreOptions};
use std::hint::black_box;
use tempfile::tempdir;

fn cached_vs_cold_reads(c: &mut Criterion) {
    let mut group = c.benchmark_group("cache_impact");
    group.sample_size(30);

    let num_orders = 5_000; // 40K events
    let events_per_segment = 2_000; // ~20 sealed segments
    let condition = order_condition(42);

    // Warm: every index mapped and checked, segments mapped.
    group.bench_function("warm_cache", |b| {
        let dir = tempdir().unwrap();
        let store = create_multi_segment_store(dir.path(), num_orders, 500, events_per_segment);

        // Warm up.
        let _ = store.source(Position(1), &condition).unwrap();

        b.iter(|| {
            black_box(
                store
                    .source(black_box(Position(1)), black_box(&condition))
                    .unwrap(),
            );
        });
    });

    // Cold: the first read of a freshly opened engine.
    group.bench_function("fresh_engine", |b| {
        let dir = tempdir().unwrap();
        let store = create_multi_segment_store(dir.path(), num_orders, 500, events_per_segment);
        drop(store);

        b.iter_batched(
            || {
                EventStoreEngine::open_with_store_options(dir.path(), &StoreOptions::default())
                    .unwrap()
            },
            |store| {
                black_box(
                    store
                        .source(black_box(Position(1)), black_box(&condition))
                        .unwrap(),
                );
                store
            },
            BatchSize::PerIteration,
        );
    });

    group.finish();
}

criterion_group!(benches, cached_vs_cold_reads);
criterion_main!(benches);
