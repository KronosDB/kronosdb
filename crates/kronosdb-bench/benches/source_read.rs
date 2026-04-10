//! Measures source (read) performance with realistic DCB query patterns.
//!
//! In DCB event sourcing, the dominant read pattern is:
//! "Source all events for entity X" — highly selective (5-50 events from millions).
//!
//! This tests the full read path: segment list lookup → bloom filter check →
//! index load → bitmap intersection → mmap scan → event deserialization.

use criterion::{Criterion, criterion_group, criterion_main};
use kronosdb_bench::*;
use kronosdb_eventstore::event::Position;
use kronosdb_eventstore::store::StoreOptions;
use std::hint::black_box;
use tempfile::tempdir;

/// The bread-and-butter DCB query: source all events for one order.
/// Returns 8 events regardless of store size. This should be fast.
fn source_single_order(c: &mut Criterion) {
    let mut group = c.benchmark_group("source_single_order");
    group.sample_size(50);

    // 10K orders × 8 events = 80K events, 1000 customers.
    let dir = tempdir().unwrap();
    let store = create_ecommerce_store(dir.path(), 10_000, 1_000, &StoreOptions::default());

    let condition = order_condition(42);

    // Warm up caches.
    let events = store.source(Position(1), &condition).unwrap();
    assert_eq!(events.len(), 8, "expected full order lifecycle");

    group.bench_function("80K_events", |b| {
        b.iter(|| {
            black_box(
                store
                    .source(black_box(Position(1)), black_box(&condition))
                    .unwrap(),
            );
        });
    });

    group.finish();
}

/// Source by customer — broader query spanning multiple orders.
/// With 10 orders per customer, returns ~80 events.
fn source_by_customer(c: &mut Criterion) {
    let mut group = c.benchmark_group("source_by_customer");
    group.sample_size(50);

    let dir = tempdir().unwrap();
    let store = create_ecommerce_store(dir.path(), 10_000, 1_000, &StoreOptions::default());

    let condition = customer_condition(42);

    let events = store.source(Position(1), &condition).unwrap();
    assert_eq!(events.len(), 80, "expected 10 orders × 8 events");

    group.bench_function("80_matching_events", |b| {
        b.iter(|| {
            black_box(
                store
                    .source(black_box(Position(1)), black_box(&condition))
                    .unwrap(),
            );
        });
    });

    group.finish();
}

/// Decision model sourcing: "has this order been paid?"
/// Filters by event type AND tag. Returns 1 event.
fn source_decision_model(c: &mut Criterion) {
    let mut group = c.benchmark_group("source_decision_model");
    group.sample_size(50);

    let dir = tempdir().unwrap();
    let store = create_ecommerce_store(dir.path(), 10_000, 1_000, &StoreOptions::default());

    let condition = order_payment_condition(42);

    let events = store.source(Position(1), &condition).unwrap();
    assert_eq!(events.len(), 1, "expected exactly one PaymentReceived");

    group.bench_function("type_and_tag_filter", |b| {
        b.iter(|| {
            black_box(
                store
                    .source(black_box(Position(1)), black_box(&condition))
                    .unwrap(),
            );
        });
    });

    group.finish();
}

/// Bloom filter effectiveness: query for non-existent entity.
/// Should be near-instant — all segments rejected by bloom.
fn source_nonexistent(c: &mut Criterion) {
    let mut group = c.benchmark_group("source_nonexistent");
    group.sample_size(100);

    for &(num_orders, events_per_seg) in &[
        (10_000, 5_000), // ~16 segments
        (50_000, 5_000), // ~80 segments
    ] {
        let label = format!("{}K_orders", num_orders / 1_000);
        let dir = tempdir().unwrap();
        let store = create_multi_segment_store(dir.path(), num_orders, 1_000, events_per_seg);

        let condition = nonexistent_order_condition();
        // Warm blooms.
        let events = store.source(Position(1), &condition).unwrap();
        assert!(events.is_empty());

        group.bench_function(&label, |b| {
            b.iter(|| {
                black_box(
                    store
                        .source(black_box(Position(1)), black_box(&condition))
                        .unwrap(),
                );
            });
        });
    }

    group.finish();
}

/// Source from a recent position (catch-up after snapshot).
/// In production, projections often have snapshots and only need recent events.
fn source_from_recent(c: &mut Criterion) {
    let mut group = c.benchmark_group("source_from_recent");
    group.sample_size(50);

    let dir = tempdir().unwrap();
    let store = create_ecommerce_store(dir.path(), 10_000, 1_000, &StoreOptions::default());
    let condition = order_condition(9_999); // last order — events near the end

    // Full replay from start.
    group.bench_function("full_replay", |b| {
        b.iter(|| {
            black_box(
                store
                    .source(black_box(Position(1)), black_box(&condition))
                    .unwrap(),
            );
        });
    });

    // Catch-up from 90% through the store.
    let near_end = Position(70_000); // 80K total events, start at 70K
    group.bench_function("catch_up_last_10pct", |b| {
        b.iter(|| {
            black_box(
                store
                    .source(black_box(near_end), black_box(&condition))
                    .unwrap(),
            );
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    source_single_order,
    source_by_customer,
    source_decision_model,
    source_nonexistent,
    source_from_recent,
);
criterion_main!(benches);
