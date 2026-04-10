//! Measures sustained events/sec under concurrent command handler load.
//!
//! This is the production-relevant number: N threads continuously appending
//! events as fast as possible for a fixed duration, counting total events landed.

use kronosdb_eventstore::append::AppendRequest;
use kronosdb_eventstore::event::{AppendEvent, Tag};
use kronosdb_eventstore::store::{EventStoreEngine, StoreOptions};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};
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

fn run_throughput_test(label: &str, num_writers: usize, opts: StoreOptions, duration: Duration) {
    let dir = tempdir().unwrap();
    let store = Arc::new(EventStoreEngine::create_with_store_options(dir.path(), &opts).unwrap());
    let counter = Arc::new(AtomicU64::new(0));
    let total_appended = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));

    let mut handles = Vec::new();
    for _ in 0..num_writers {
        let s = Arc::clone(&store);
        let c = Arc::clone(&counter);
        let t = Arc::clone(&total_appended);
        let st = Arc::clone(&stop);
        handles.push(std::thread::spawn(move || {
            while !st.load(Ordering::Relaxed) {
                let id = c.fetch_add(1, Ordering::Relaxed);
                if s.append(AppendRequest {
                    condition: None,
                    events: vec![make_event(id)],
                })
                .is_ok()
                {
                    t.fetch_add(1, Ordering::Relaxed);
                }
            }
        }));
    }

    std::thread::sleep(duration);
    stop.store(true, Ordering::Relaxed);

    for h in handles {
        h.join().unwrap();
    }

    let total = total_appended.load(Ordering::Relaxed);
    let rate = total as f64 / duration.as_secs_f64();
    println!(
        "{label:<45} {total:>8} events in {:>4.1}s = {rate:>10.0} events/sec",
        duration.as_secs_f64()
    );
}

fn main() {
    let test_duration = Duration::from_secs(5);

    println!(
        "\n=== Sustained Write Throughput (single-event appends, {:.0}s each) ===\n",
        test_duration.as_secs_f64()
    );
    println!(
        "{:<45} {:>8}   {:>6}   {:>14}",
        "Configuration", "Total", "Time", "Throughput"
    );
    println!("{}", "-".repeat(85));

    // No group commit (default) — baseline
    for &writers in &[1, 4, 16] {
        let label = format!("no_group_commit / {} writer(s)", writers);
        run_throughput_test(&label, writers, StoreOptions::default(), test_duration);
    }

    println!();

    // Group commit with 2ms interval
    for &writers in &[1, 4, 16] {
        let label = format!("group_commit_2ms / {} writer(s)", writers);
        run_throughput_test(
            &label,
            writers,
            StoreOptions {
                group_commit_interval_ms: 2,
                ..Default::default()
            },
            test_duration,
        );
    }

    println!();

    // Group commit with batch size 10 (more realistic: command handlers append small batches)
    println!("--- Batch appends (10 events per append) ---\n");

    let dir = tempdir().unwrap();
    let opts = StoreOptions {
        group_commit_interval_ms: 2,
        ..Default::default()
    };
    let store = Arc::new(EventStoreEngine::create_with_store_options(dir.path(), &opts).unwrap());
    let counter = Arc::new(AtomicU64::new(0));
    let total_events = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));

    let num_writers = 16;
    let batch_size = 10;
    let mut handles = Vec::new();

    for _ in 0..num_writers {
        let s = Arc::clone(&store);
        let c = Arc::clone(&counter);
        let t = Arc::clone(&total_events);
        let st = Arc::clone(&stop);
        handles.push(std::thread::spawn(move || {
            while !st.load(Ordering::Relaxed) {
                let base_id = c.fetch_add(batch_size, Ordering::Relaxed);
                let events: Vec<_> = (0..batch_size).map(|i| make_event(base_id + i)).collect();
                if let Ok(r) = s.append(AppendRequest {
                    condition: None,
                    events,
                }) {
                    t.fetch_add(r.count as u64, Ordering::Relaxed);
                }
            }
        }));
    }

    let start = Instant::now();
    std::thread::sleep(test_duration);
    stop.store(true, Ordering::Relaxed);

    for h in handles {
        h.join().unwrap();
    }

    let elapsed = start.elapsed();
    let total = total_events.load(Ordering::Relaxed);
    let rate = total as f64 / elapsed.as_secs_f64();
    println!(
        "group_commit_2ms / 16 writers / batch=10      {total:>8} events in {:>4.1}s = {rate:>10.0} events/sec",
        elapsed.as_secs_f64()
    );
}
