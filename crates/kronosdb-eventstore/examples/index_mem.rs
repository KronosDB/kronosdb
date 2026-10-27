//! What a sealed segment's index costs in memory, measured with a counting
//! allocator against `SegmentIndex::heap_bytes()`.
//!
//!   index_mem <dir> <events-per-segment> [aggregates=50000]
//!
//! The events look like `native_bench prepop`: two tags per event (an order
//! id that repeats every `aggregates` events and a customer id) and one of
//! four event types. 32 MB segments of the benchmark's ~490-byte events hold
//! about 69k events; 256 MB segments about 550k.

use std::alloc::{GlobalAlloc, Layout, System};
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};

use kronosdb_eventstore::event::Tag;
use kronosdb_eventstore::segment::segment_index::SegmentIndex;

struct Counting;
static LIVE: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        LIVE.fetch_add(layout.size(), Ordering::Relaxed);
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        LIVE.fetch_add(new_size, Ordering::Relaxed);
        LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static ALLOC: Counting = Counting;

fn live() -> usize {
    LIVE.load(Ordering::Relaxed)
}

fn mb(n: usize) -> f64 {
    n as f64 / (1024.0 * 1024.0)
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let dir = Path::new(&args[1]);
    let events: u64 = args[2].parse().unwrap();
    let aggregates: u64 = args.get(3).map_or(50_000, |v| v.parse().unwrap());
    std::fs::create_dir_all(dir).unwrap();

    const NAMES: [&str; 4] = [
        "OrderPlaced",
        "ItemAdded",
        "PaymentReceived",
        "OrderShipped",
    ];
    let mut index = SegmentIndex::new(0);
    let mut offset = 13u64;
    for n in 0..events {
        let a = n % aggregates;
        let tags = [
            Tag::from_str("orderId", &format!("agg-{a}")),
            Tag::from_str("customerId", &format!("c-{}", a % 500)),
        ];
        index.insert_event(n, offset, NAMES[(n / aggregates % 4) as usize], &tags);
        offset += 488;
    }
    let building = index.heap_bytes();
    let seg = dir.join("00000000000000000000.seg");
    index.write_to_disk(&seg).unwrap();
    drop(index);

    let idx_file = std::fs::metadata(seg.with_extension("idx")).unwrap().len() as usize;
    let bloom_file = std::fs::metadata(seg.with_extension("bloom"))
        .unwrap()
        .len() as usize;

    let before = live();
    let loaded = SegmentIndex::read_idx(&seg.with_extension("idx")).unwrap();
    let actual = live() - before;
    let estimate = loaded.heap_bytes();

    let before = live();
    let bloom = SegmentIndex::read_bloom(&seg.with_extension("bloom")).unwrap();
    let bloom_actual = live() - before;
    drop(bloom);
    drop(loaded);

    println!(
        "events/segment={events} aggregates={aggregates}\n  .idx   file {:7.2} MB  heap {:7.2} MB  estimate {:7.2} MB  ({:.1} B/event heap)\n  .bloom file {:7.2} MB  heap {:7.2} MB\n  index built in memory (estimate) {:7.2} MB",
        mb(idx_file),
        mb(actual),
        mb(estimate),
        actual as f64 / events as f64,
        mb(bloom_file),
        mb(bloom_actual),
        mb(building),
    );
}
