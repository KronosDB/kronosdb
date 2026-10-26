//! Per-event cost of the append path's pieces, with no network and no engine:
//! the segment writer (frame + crc + write + index), the index insert alone,
//! the DCB check against the active index, and what the server does per
//! request to turn the client's records into something the writer takes:
//! decode them into `AppendEvent`s, or check them in place.
//!
//!   append_micro <dir> [events]
//!
//! Point `dir` at tmpfs to leave the disk out of it.

use std::hint::black_box;
use std::time::Instant;

use kronosdb_eventstore::criteria::{Criterion, SourcingCondition};
use kronosdb_eventstore::event::{AppendEvent, Position, Tag};
use kronosdb_eventstore::segment::segment_index::SegmentIndex;
use kronosdb_eventstore::segment::writer::SegmentWriter;
use kronosdb_eventstore::wire;

fn event(a: u64, n: u64) -> AppendEvent {
    const NAMES: [&str; 4] = [
        "OrderPlaced",
        "ItemAdded",
        "PaymentReceived",
        "OrderShipped",
    ];
    AppendEvent {
        identifier: format!("{a:010}-{n:016x}"),
        name: NAMES[(n % 4) as usize].into(),
        version: "1".into(),
        timestamp: 1_700_000_000_000,
        metadata: vec![],
        tags: vec![
            Tag::new(b"orderId".to_vec(), format!("agg-{a}").into_bytes()),
            Tag::new(
                b"customerId".to_vec(),
                format!("c-{}", a % 500).into_bytes(),
            ),
        ],
        payload: vec![b'x'; 300],
    }
}

fn xorshift(mut x: u64) -> u64 {
    x ^= x >> 33;
    x = x.wrapping_mul(0xff51afd7ed558ccd);
    x ^= x >> 33;
    x = x.wrapping_mul(0xc4ceb9fe1a85ec53);
    x ^ (x >> 33)
}

fn ns_per(total: std::time::Duration, n: usize) -> f64 {
    total.as_nanos() as f64 / n as f64
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let dir = std::path::PathBuf::from(&args[1]);
    let n: usize = args.get(2).map(|s| s.parse().unwrap()).unwrap_or(60_000);
    let events: Vec<AppendEvent> = (0..n as u64)
        .map(|i| event(xorshift(i) % 50_000, i))
        .collect();

    // 1. The writer, batch 1 and batch 100. Segments are big enough that
    // rotation (which seals and re-indexes) is not in the number.
    for batch in [1usize, 100] {
        let sub = dir.join(format!("w{batch}"));
        let _ = std::fs::remove_dir_all(&sub);
        let mut writer = SegmentWriter::new(&sub, Position(0), 256 * 1024 * 1024).unwrap();
        let t = Instant::now();
        for chunk in events.chunks(batch) {
            black_box(writer.write_events(chunk).unwrap());
        }
        println!(
            "writer.write_events   batch={batch:<4} {:>8.0} ns/event",
            ns_per(t.elapsed(), n)
        );
        drop(writer);
        let _ = std::fs::remove_dir_all(&sub);
    }

    // 1b. The same writes from a client's records: checked in place, then
    // copied behind their CRCs, indexed from the record bytes.
    for batch in [1usize, 100] {
        let sub = dir.join(format!("r{batch}"));
        let _ = std::fs::remove_dir_all(&sub);
        let mut writer = SegmentWriter::new(&sub, Position(0), 256 * 1024 * 1024).unwrap();
        let batches: Vec<_> = events
            .chunks(batch)
            .map(|chunk| {
                let records = wire::append_records(chunk).unwrap();
                let frame = bytes::Bytes::copy_from_slice(records.as_records().bytes());
                (records.len(), frame)
            })
            .map(|(count, frame)| {
                let records = kronosdb_protocol::Records::from_raw(count, &frame).unwrap();
                wire::record_batch(&records, &frame).unwrap()
            })
            .collect();
        let t = Instant::now();
        for b in &batches {
            black_box(writer.write_batch(b).unwrap());
        }
        println!(
            "writer.write_batch    batch={batch:<4} {:>8.0} ns/event",
            ns_per(t.elapsed(), n)
        );
        drop(writer);
        let _ = std::fs::remove_dir_all(&sub);
    }

    // 2. The index insert alone.
    let mut index = SegmentIndex::new(0);
    let t = Instant::now();
    for (i, e) in events.iter().enumerate() {
        index.insert_event(i as u64, 13 + i as u64 * 400, &e.name, &e.tags);
    }
    println!(
        "index.insert_event             {:>8.0} ns/event",
        ns_per(t.elapsed(), n)
    );

    // 3. The DCB check against the active index: a tag criterion, and a
    // type-plus-tag one (the type's bitmap holds a quarter of the segment).
    let probe = |names: Vec<String>| -> f64 {
        let reps = 20_000usize;
        let t = Instant::now();
        for i in 0..reps {
            let a = xorshift(i as u64 + 7) % 50_000;
            let condition = SourcingCondition {
                criteria: vec![Criterion {
                    names: names.clone(),
                    tags: vec![Tag::new(
                        b"orderId".to_vec(),
                        format!("agg-{a}").into_bytes(),
                    )],
                }],
            };
            black_box(index.has_match_after(&condition, (n as u64).saturating_sub(10)));
        }
        ns_per(t.elapsed(), reps)
    };
    println!(
        "dcb has_match_after tag        {:>8.0} ns/check",
        probe(vec![])
    );
    println!(
        "dcb has_match_after type+tag   {:>8.0} ns/check",
        probe(vec!["OrderPlaced".into(), "ItemAdded".into()])
    );

    // 4. Wire records to AppendEvent, per event.
    for batch in [1usize, 100] {
        let records: Vec<_> = events
            .chunks(batch)
            .take(2000 / batch.min(100) + 1)
            .map(|c| wire::append_records(c).unwrap())
            .collect();
        let t = Instant::now();
        let mut count = 0usize;
        for r in &records {
            let converted = wire::append_events(&r.as_records()).unwrap();
            count += converted.len();
            black_box(converted);
        }
        println!(
            "wire::append_events   batch={batch:<4} {:>8.0} ns/event",
            ns_per(t.elapsed(), count)
        );
        let frames: Vec<_> = records
            .iter()
            .map(|r| {
                (
                    r.len(),
                    bytes::Bytes::copy_from_slice(r.as_records().bytes()),
                )
            })
            .collect();
        let t = Instant::now();
        let mut count = 0usize;
        for (len, frame) in &frames {
            let received = kronosdb_protocol::Records::from_raw(*len, frame).unwrap();
            let checked = wire::record_batch(&received, frame).unwrap();
            count += checked.len() as usize;
            black_box(checked);
        }
        println!(
            "wire::record_batch    batch={batch:<4} {:>8.0} ns/event",
            ns_per(t.elapsed(), count)
        );
    }
}
