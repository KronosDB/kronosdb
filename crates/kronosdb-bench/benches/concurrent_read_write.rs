//! Measures read performance under concurrent write load.
//!
//! In production, reads and writes happen simultaneously. The writer holds
//! a Mutex but readers don't need it (they use RwLock for the tag index
//! and atomics for committed_position). This benchmark validates that
//! reads aren't significantly degraded by concurrent writes.

use criterion::{Criterion, criterion_group, criterion_main};
use kronosdb_bench::*;
use kronosdb_eventstore::append::AppendRequest;
use kronosdb_eventstore::event::{AppendEvent, Position, Tag};
use kronosdb_eventstore::store::StoreOptions;
use std::hint::black_box;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tempfile::tempdir;

fn reads_under_write_load(c: &mut Criterion) {
    let mut group = c.benchmark_group("concurrent_read_write");
    group.sample_size(30);
    group.measurement_time(std::time::Duration::from_secs(10));

    let num_orders = 10_000;
    let num_customers = 1_000;
    let condition = order_condition(42);

    // Baseline: reads with no concurrent writes.
    group.bench_function("read_only", |b| {
        let dir = tempdir().unwrap();
        let store = Arc::new(create_ecommerce_store(
            dir.path(), num_orders, num_customers, &StoreOptions::default(),
        ));

        b.iter(|| {
            black_box(store.source(black_box(Position(1)), black_box(&condition)).unwrap());
        });
    });

    // Reads with a background writer appending new orders continuously.
    group.bench_function("read_with_background_writes", |b| {
        let dir = tempdir().unwrap();
        let store = Arc::new(create_ecommerce_store(
            dir.path(), num_orders, num_customers, &StoreOptions::default(),
        ));

        let stop = Arc::new(AtomicBool::new(false));

        let writer_store = Arc::clone(&store);
        let writer_stop = Arc::clone(&stop);
        let writer_handle = std::thread::spawn(move || {
            let mut i = 0u64;
            while !writer_stop.load(Ordering::Relaxed) {
                let events = vec![AppendEvent {
                    identifier: format!("bg-{i}"),
                    name: "OrderPlaced".into(),
                    version: "1.0".into(),
                    timestamp: 1712345678000 + i as i64,
                    payload: vec![0u8; 300],
                    metadata: vec![],
                    tags: vec![
                        Tag::from_str("orderId", &format!("bg-order-{i}")),
                        Tag::from_str("customerId", &format!("bg-cust-{}", i % 100)),
                    ],
                }];
                let _ = writer_store.append(AppendRequest {
                    condition: None,
                    events,
                });
                i += 1;
            }
            i
        });

        b.iter(|| {
            black_box(store.source(black_box(Position(1)), black_box(&condition)).unwrap());
        });

        stop.store(true, Ordering::Relaxed);
        let writes = writer_handle.join().unwrap();
        eprintln!("Background writer completed {writes} appends during benchmark");
    });

    group.finish();
}

criterion_group!(benches, reads_under_write_load);
criterion_main!(benches);
