//! Record CRC verification: the `crc32c` crate against the CPU's CRC
//! instruction called directly.
//!
//!   crc_bench [record-bytes=488] [records=131]

use std::hint::black_box;
use std::time::Instant;

use kronosdb_eventstore::segment::crc::{Span, first_mismatch, record_crc};

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let len: usize = args.get(1).map_or(488, |v| v.parse().unwrap());
    let count: usize = args.get(2).map_or(131, |v| v.parse().unwrap());

    let mut seed = 0x9e37_79b9_7f4a_7c15u64;
    let mut data = Vec::new();
    let mut spans = Vec::new();
    for _ in 0..count {
        let start = data.len();
        for _ in 0..len {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            data.push((seed >> 24) as u8);
        }
        spans.push(Span {
            flags: 0,
            body_start: start,
            body_end: start + len,
            stored: record_crc(0, &data[start..]),
        });
    }

    let rounds = 200_000_000 / (len * count);
    let time = |name: &str, f: &dyn Fn() -> bool| {
        // Warm up, then take the best of five.
        assert!(f());
        let mut best = f64::MAX;
        for _ in 0..5 {
            let t = Instant::now();
            for _ in 0..rounds {
                black_box(f());
            }
            best = best.min(t.elapsed().as_secs_f64());
        }
        let records = (rounds * count) as f64;
        println!(
            "{name:<28} {:6.1} ns/record  {:5.2} GB/s",
            best / records * 1e9,
            records * len as f64 / best / 1e9
        );
    };

    println!("{count} records of {len} bytes");
    time("crc32c crate, one by one", &|| {
        spans.iter().all(|s| {
            crc32c::crc32c_append(crc32c::crc32c(&[s.flags]), &data[s.body_start..s.body_end])
                == s.stored
        })
    });
    time("hardware, one by one", &|| {
        spans
            .iter()
            .all(|s| record_crc(s.flags, &data[s.body_start..s.body_end]) == s.stored)
    });
    time("hardware, a page in a row", &|| {
        first_mismatch(black_box(&data), black_box(&spans)).is_none()
    });
}
