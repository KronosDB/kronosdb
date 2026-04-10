//! Measures how the event store behaves as data volume grows.
//!
//! This is the most important benchmark for capacity planning. It answers:
//! - How does read latency degrade as you go from 100K to 10M events?
//! - At what point do cache misses start to dominate?
//! - Does the bloom filter keep non-existent entity queries constant-time?
//!
//! Models a realistic e-commerce workload where the store grows over time
//! but individual queries are always "source events for order X" (8 events).

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use kronosdb_bench::*;
use kronosdb_eventstore::event::Position;
use std::hint::black_box;
use tempfile::tempdir;

/// How read latency scales with total event count.
///
/// Each data point: N orders × 8 events, source one specific order.
/// The query always returns 8 events — what changes is how much data
/// the store has to skip past (sealed segments, bloom checks, etc.).
fn read_latency_vs_volume(c: &mut Criterion) {
    let mut group = c.benchmark_group("read_latency_vs_volume");
    group.sample_size(20);

    // Segment size ~5K events to create realistic segment counts.
    let events_per_segment = 5_000;

    for &num_orders in &[
        1_000,   //     8K events, ~2 segments
        10_000,  //    80K events, ~16 segments
        50_000,  //   400K events, ~80 segments
        125_000, //  1.0M events, ~200 segments
        250_000, //  2.0M events, ~400 segments
        625_000, //  5.0M events, ~1000 segments
    ] {
        let total_events = num_orders * 8;
        let label = if total_events >= 1_000_000 {
            format!("{}M", total_events / 1_000_000)
        } else {
            format!("{}K", total_events / 1_000)
        };

        // Pick an order in the middle of the store — not biased toward
        // the active segment (which is always fast via in-memory index).
        let target_order = num_orders / 2;
        let condition = order_condition(target_order);

        group.bench_with_input(
            BenchmarkId::new("events", &label),
            &num_orders,
            |b, &num_orders| {
                let dir = tempdir().unwrap();
                let store = create_multi_segment_store(
                    dir.path(),
                    num_orders,
                    num_orders / 10, // 10 orders per customer
                    events_per_segment,
                );

                // Warm up: one read to populate caches.
                let events = store.source(Position(1), &condition).unwrap();
                assert_eq!(events.len(), 8);

                b.iter(|| {
                    black_box(
                        store
                            .source(black_box(Position(1)), black_box(&condition))
                            .unwrap(),
                    );
                });
            },
        );
    }

    group.finish();
}

/// Bloom filter rejection at scale: query for non-existent entity.
/// Should stay ~constant regardless of store size (just N bloom checks).
fn bloom_rejection_vs_volume(c: &mut Criterion) {
    let mut group = c.benchmark_group("bloom_rejection_vs_volume");
    group.sample_size(30);

    let events_per_segment = 5_000;
    let condition = nonexistent_order_condition();

    for &num_orders in &[1_000, 10_000, 50_000, 125_000, 250_000] {
        let total_events = num_orders * 8;
        let label = if total_events >= 1_000_000 {
            format!("{}M", total_events / 1_000_000)
        } else {
            format!("{}K", total_events / 1_000)
        };

        group.bench_with_input(
            BenchmarkId::new("events", &label),
            &num_orders,
            |b, &num_orders| {
                let dir = tempdir().unwrap();
                let store = create_multi_segment_store(
                    dir.path(),
                    num_orders,
                    num_orders / 10,
                    events_per_segment,
                );

                // Warm blooms.
                let _ = store.source(Position(1), &condition).unwrap();

                b.iter(|| {
                    black_box(
                        store
                            .source(black_box(Position(1)), black_box(&condition))
                            .unwrap(),
                    );
                });
            },
        );
    }

    group.finish();
}

/// Cache thrashing at scale: what happens when the working set exceeds cache size?
///
/// With default cache sizes (50 indices, 200 blooms) and thousands of segments,
/// random-order queries force evictions. This models a production scenario where
/// many different entities are queried in rapid succession (e.g., a busy API).
fn cache_pressure_at_scale(c: &mut Criterion) {
    let mut group = c.benchmark_group("cache_pressure");
    group.sample_size(15);

    let num_orders = 125_000; // 1M events
    let events_per_segment = 5_000; // ~200 segments
    let num_customers = 12_500; // 10 orders per customer

    let dir = tempdir().unwrap();
    let store =
        create_multi_segment_store(dir.path(), num_orders, num_customers, events_per_segment);

    // Sequential access: query orders 0, 1, 2, ... (temporal locality, cache-friendly)
    group.bench_function("sequential_access", |b| {
        let mut i = 0usize;
        b.iter(|| {
            let condition = order_condition(i % num_orders);
            black_box(
                store
                    .source(black_box(Position(1)), black_box(&condition))
                    .unwrap(),
            );
            i += 1;
        });
    });

    // Strided access: query orders 0, 1000, 2000, ... (spans many segments, cache-hostile)
    group.bench_function("strided_access", |b| {
        let mut i = 0usize;
        let stride = num_orders / 50; // jump across ~4 segments per query
        b.iter(|| {
            let order_id = (i * stride) % num_orders;
            let condition = order_condition(order_id);
            black_box(
                store
                    .source(black_box(Position(1)), black_box(&condition))
                    .unwrap(),
            );
            i += 1;
        });
    });

    group.finish();
}

/// Append throughput at different store sizes.
/// Does the store slow down as it grows? It shouldn't — appends only touch
/// the active segment. But tag index growth could cause overhead.
fn append_at_scale(c: &mut Criterion) {
    let mut group = c.benchmark_group("append_at_scale");
    group.sample_size(20);

    let events_per_segment = 5_000;

    for &num_orders in &[1_000, 10_000, 50_000, 125_000] {
        let total_events = num_orders * 8;
        let label = if total_events >= 1_000_000 {
            format!("{}M", total_events / 1_000_000)
        } else {
            format!("{}K", total_events / 1_000)
        };

        group.bench_with_input(
            BenchmarkId::new("existing_events", &label),
            &num_orders,
            |b, &num_orders| {
                let dir = tempdir().unwrap();
                let store = create_multi_segment_store(
                    dir.path(),
                    num_orders,
                    num_orders / 10,
                    events_per_segment,
                );

                let new_event = kronosdb_eventstore::event::AppendEvent {
                    identifier: "bench-evt".into(),
                    name: "OrderPlaced".into(),
                    version: "1.0".into(),
                    timestamp: 9999999999999,
                    payload: vec![0u8; 300],
                    metadata: vec![],
                    tags: vec![
                        kronosdb_eventstore::event::Tag::from_str("orderId", "order-new"),
                        kronosdb_eventstore::event::Tag::from_str("customerId", "cust-new"),
                    ],
                };

                b.iter(|| {
                    black_box(
                        store
                            .append(kronosdb_eventstore::append::AppendRequest {
                                condition: None,
                                events: vec![new_event.clone()],
                            })
                            .unwrap(),
                    );
                });
            },
        );
    }

    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default().significance_level(0.05);
    targets = read_latency_vs_volume, bloom_rejection_vs_volume, cache_pressure_at_scale, append_at_scale
}
criterion_main!(benches);
