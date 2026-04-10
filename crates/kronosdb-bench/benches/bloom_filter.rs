//! Measures bloom filter effectiveness.
//!
//! The bloom filter is the first line of defense against unnecessary index loads.
//! A bloom rejection means: no .idx file loaded, no bitmap ops, no segment scan.
//! This benchmark quantifies that savings.

use criterion::{Criterion, criterion_group, criterion_main};
use kronosdb_bench::*;
use kronosdb_eventstore::criteria::{Criterion as QueryCriterion, SourcingCondition};
use kronosdb_eventstore::event::Position;
use std::hint::black_box;
use tempfile::tempdir;

fn bloom_filter_effectiveness(c: &mut Criterion) {
    let mut group = c.benchmark_group("bloom_filter");
    group.sample_size(50);

    // 5K orders × 8 events = 40K events, ~20 sealed segments.
    let dir = tempdir().unwrap();
    let store = create_multi_segment_store(dir.path(), 5_000, 500, 2_000);

    // Query for an order that exists — bloom passes on segments containing it.
    let hit_condition = order_condition(42);
    let _ = store.source(Position(1), &hit_condition).unwrap();

    group.bench_function("existing_entity", |b| {
        b.iter(|| {
            black_box(
                store
                    .source(black_box(Position(1)), black_box(&hit_condition))
                    .unwrap(),
            );
        });
    });

    // Query for an order that does NOT exist — bloom rejects ALL segments.
    let miss_condition = nonexistent_order_condition();
    let _ = store.source(Position(1), &miss_condition).unwrap();

    group.bench_function("nonexistent_entity", |b| {
        b.iter(|| {
            black_box(
                store
                    .source(black_box(Position(1)), black_box(&miss_condition))
                    .unwrap(),
            );
        });
    });

    // Query by event type only — bloom passes on ALL segments (low selectivity).
    let broad_condition = SourcingCondition {
        criteria: vec![QueryCriterion {
            names: vec!["OrderPlaced".into()],
            tags: vec![],
        }],
    };
    let _ = store.source(Position(1), &broad_condition).unwrap();

    group.bench_function("broad_event_type", |b| {
        b.iter(|| {
            black_box(
                store
                    .source(black_box(Position(1)), black_box(&broad_condition))
                    .unwrap(),
            );
        });
    });

    group.finish();
}

criterion_group!(benches, bloom_filter_effectiveness);
criterion_main!(benches);
