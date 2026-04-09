//! Measures the impact of cache sizes on read performance.
//!
//! Directly validates the cache optimizations by comparing warm vs cold caches
//! across multiple sealed segments. Also shows how cache configuration affects
//! latency — the vertical scaling story.

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
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

    // Warm cache: all segment metadata in memory.
    group.bench_function("warm_cache", |b| {
        let dir = tempdir().unwrap();
        let store = create_multi_segment_store(dir.path(), num_orders, 500, events_per_segment);

        // Warm up.
        let _ = store.source(Position(1), &condition).unwrap();

        b.iter(|| {
            black_box(store.source(black_box(Position(1)), black_box(&condition)).unwrap());
        });
    });

    // Cold cache: size=1, forces eviction every segment.
    group.bench_function("cold_cache_size_1", |b| {
        let dir = tempdir().unwrap();
        let _store = create_multi_segment_store(dir.path(), num_orders, 500, events_per_segment);
        drop(_store);

        let opts = StoreOptions {
            index_cache_size: 1,
            bloom_cache_size: 1,
            ..Default::default()
        };
        let store = EventStoreEngine::open_with_store_options(dir.path(), &opts).unwrap();

        b.iter(|| {
            black_box(store.source(black_box(Position(1)), black_box(&condition)).unwrap());
        });
    });

    group.finish();
}

/// Shows how cache configuration maps to read latency.
/// This is the graph you show to justify RAM provisioning.
fn cache_size_scaling(c: &mut Criterion) {
    let mut group = c.benchmark_group("cache_size_scaling");
    group.sample_size(20);

    let num_orders = 5_000;
    let events_per_segment = 2_000;
    let condition = order_condition(42);

    let dir = tempdir().unwrap();
    let store = create_multi_segment_store(dir.path(), num_orders, 500, events_per_segment);
    drop(store);

    for &(idx_cache, bloom_cache) in &[
        (1, 1),     // Minimal
        (5, 10),    // Small
        (25, 50),   // Medium
        (100, 200), // Default
    ] {
        let label = format!("idx{idx_cache}_bloom{bloom_cache}");

        group.bench_with_input(
            BenchmarkId::new("config", &label),
            &(idx_cache, bloom_cache),
            |b, &(idx, bloom)| {
                let opts = StoreOptions {
                    index_cache_size: idx,
                    bloom_cache_size: bloom,
                    ..Default::default()
                };
                let store = EventStoreEngine::open_with_store_options(dir.path(), &opts).unwrap();
                let _ = store.source(Position(1), &condition).unwrap();

                b.iter(|| {
                    black_box(store.source(black_box(Position(1)), black_box(&condition)).unwrap());
                });
            },
        );
    }

    group.finish();
}

criterion_group!(benches, cached_vs_cold_reads, cache_size_scaling);
criterion_main!(benches);
