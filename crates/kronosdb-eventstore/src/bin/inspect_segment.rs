//! Segment inspector — dumps `.seg` file contents in a human-readable form.
//!
//! Usage:
//!   cargo run -p kronosdb-eventstore --bin inspect_segment -- <path/to/segment.seg>
//!   cargo run -p kronosdb-eventstore --bin inspect_segment -- <path> --summary
//!   cargo run -p kronosdb-eventstore --bin inspect_segment -- <path> --from 100 --to 110
//!
//! Walks version-3 event and native control records. `--overhead` accounts
//! separately for application data, event framing, control records, and unused
//! preallocated space.

use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use kronosdb_eventstore::segment::format::ControlRecord;
use kronosdb_eventstore::segment::reader::SegmentReader;
use kronosdb_eventstore::segment::{self, RECORD_HEADER_SIZE, SEGMENT_HEADER_SIZE};

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let opts = match parse_args(&args) {
        Ok(opts) => opts,
        Err(msg) => {
            eprintln!("error: {msg}");
            eprintln!();
            print_usage();
            return ExitCode::from(2);
        }
    };

    match inspect(&opts) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::FAILURE
        }
    }
}

struct Options {
    path: PathBuf,
    summary_only: bool,
    from: Option<u64>,
    to: Option<u64>,
    show_payload: bool,
    show_overhead: bool,
}

fn parse_args(args: &[String]) -> Result<Options, String> {
    let mut path: Option<PathBuf> = None;
    let mut summary_only = false;
    let mut from: Option<u64> = None;
    let mut to: Option<u64> = None;
    let mut show_payload = false;
    let mut show_overhead = false;

    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "-h" | "--help" => {
                print_usage();
                std::process::exit(0);
            }
            "--summary" => summary_only = true,
            "--payload" => show_payload = true,
            "--overhead" => show_overhead = true,
            "--from" => {
                i += 1;
                let v = args
                    .get(i)
                    .ok_or_else(|| "--from requires a value".to_string())?;
                from = Some(v.parse().map_err(|_| format!("invalid --from: {v}"))?);
            }
            "--to" => {
                i += 1;
                let v = args
                    .get(i)
                    .ok_or_else(|| "--to requires a value".to_string())?;
                to = Some(v.parse().map_err(|_| format!("invalid --to: {v}"))?);
            }
            arg if arg.starts_with('-') => {
                return Err(format!("unknown flag: {arg}"));
            }
            arg => {
                if path.is_some() {
                    return Err(format!("unexpected argument: {arg}"));
                }
                path = Some(PathBuf::from(arg));
            }
        }
        i += 1;
    }

    Ok(Options {
        path: path.ok_or_else(|| "missing segment path".to_string())?,
        summary_only,
        from,
        to,
        show_payload,
        show_overhead,
    })
}

fn print_usage() {
    eprintln!(
        "Usage: inspect_segment <path> [--summary] [--overhead] [--payload] [--from N] [--to N]"
    );
    eprintln!();
    eprintln!("  <path>        Path to a .seg file");
    eprintln!("  --summary     Only print the header and record count");
    eprintln!("  --overhead    Account for application, format, control, and unused bytes");
    eprintln!("  --payload     Also hex-dump each event's payload bytes");
    eprintln!("  --from N      Only show events with position >= N");
    eprintln!("  --to N        Only show events with position <= N");
}

fn inspect(opts: &Options) -> Result<(), String> {
    let reader = SegmentReader::open(&opts.path).map_err(|e| format!("{e}"))?;
    let file_size = std::fs::metadata(&opts.path).map(|m| m.len()).unwrap_or(0);

    println!("SEGMENT: {}", opts.path.display());
    println!("  base_position: {}", reader.base_position());
    println!(
        "  file_size:     {} ({} bytes)",
        human_bytes(file_size),
        file_size
    );
    println!();

    let mut total: u64 = 0;
    let mut first_pos: Option<u64> = None;
    let mut last_pos: Option<u64> = None;
    let mut total_payload_bytes: u64 = 0;
    let mut unique_names: std::collections::BTreeMap<String, u64> =
        std::collections::BTreeMap::new();

    if !opts.summary_only {
        println!("RECORDS:");
    }

    for result in reader.iter(None) {
        let event = result.map_err(|e| format!("read error: {e}"))?;
        let pos = event.position.0;

        if let Some(from) = opts.from
            && pos < from
        {
            continue;
        }
        if let Some(to) = opts.to
            && pos > to
        {
            break;
        }

        total += 1;
        first_pos.get_or_insert(pos);
        last_pos = Some(pos);
        total_payload_bytes += event.payload.len() as u64;
        *unique_names.entry(event.name.clone()).or_insert(0) += 1;

        if opts.summary_only {
            continue;
        }

        let tags = if event.tags.is_empty() {
            "[]".to_string()
        } else {
            let parts: Vec<String> = event
                .tags
                .iter()
                .map(|t| {
                    format!(
                        "{}={}",
                        String::from_utf8_lossy(&t.key),
                        String::from_utf8_lossy(&t.value)
                    )
                })
                .collect();
            format!("[{}]", parts.join(", "))
        };

        println!(
            "  pos={:>6} id={:<36} name={:<30} ts={} ver={} tags={} payload={}B",
            pos,
            truncate(&event.identifier, 36),
            truncate(&event.name, 30),
            event.timestamp,
            event.version,
            tags,
            event.payload.len()
        );

        if opts.show_payload {
            print!("    payload: ");
            for (i, byte) in event.payload.iter().enumerate() {
                if i > 0 && i % 16 == 0 {
                    print!("\n             ");
                }
                print!("{byte:02x} ");
            }
            println!();
        }
    }

    println!();
    println!("SUMMARY:");
    println!("  events:            {total}");
    if let (Some(first), Some(last)) = (first_pos, last_pos) {
        println!("  position_range:    [{first}, {last}]");
    }
    println!("  total_payload:     {total_payload_bytes} bytes");
    if let Some(avg) = total_payload_bytes.checked_div(total) {
        println!("  avg_payload:       {avg} bytes");
    }
    println!("  unique_event_types: {}", unique_names.len());
    if !unique_names.is_empty() && total > 0 {
        for (name, count) in unique_names.iter() {
            println!("    {name}: {count}");
        }
    }

    if opts.show_overhead {
        println!();
        print_overhead(&opts.path)?;
    }

    Ok(())
}

#[derive(Default)]
struct OverheadStats {
    occupied_bytes: u64,
    application_bytes: u64,
    event_format_bytes: u64,
    epoch_change_bytes: u64,
    watermark_checkpoint_bytes: u64,
    epoch_changes: u64,
    watermark_checkpoints: u64,
}

impl OverheadStats {
    fn control_bytes(&self) -> u64 {
        self.epoch_change_bytes + self.watermark_checkpoint_bytes
    }
}

fn print_overhead(path: &Path) -> Result<(), String> {
    let stats = account_segment(path)?;
    let file_size = std::fs::metadata(path)
        .map_err(|error| format!("read segment metadata: {error}"))?
        .len();
    let unused = file_size.saturating_sub(stats.occupied_bytes);
    let percent = |bytes: u64| {
        if stats.occupied_bytes == 0 {
            0.0
        } else {
            bytes as f64 * 100.0 / stats.occupied_bytes as f64
        }
    };

    println!("BYTE ACCOUNTING:");
    println!("  occupied:            {} bytes", stats.occupied_bytes);
    println!("  unused_preallocated:  {unused} bytes");
    println!(
        "  application_data:     {} bytes ({:.6}%)",
        stats.application_bytes,
        percent(stats.application_bytes)
    );
    println!(
        "  event_format:         {} bytes ({:.6}%)",
        stats.event_format_bytes,
        percent(stats.event_format_bytes)
    );
    println!(
        "  native_control:       {} bytes ({:.9}%)",
        stats.control_bytes(),
        percent(stats.control_bytes())
    );
    println!(
        "    epoch_change:       {} records, {} bytes",
        stats.epoch_changes, stats.epoch_change_bytes
    );
    println!(
        "    watermark:          {} records, {} bytes",
        stats.watermark_checkpoints, stats.watermark_checkpoint_bytes
    );

    for (label, extension) in [("index", "idx"), ("bloom", "bloom")] {
        let companion = path.with_extension(extension);
        let bytes = std::fs::metadata(&companion)
            .map(|metadata| metadata.len())
            .unwrap_or(0);
        println!("  {label}_companion:     {bytes} bytes");
    }

    Ok(())
}

fn account_segment(path: &Path) -> Result<OverheadStats, String> {
    let mut file = std::fs::File::open(path).map_err(|error| format!("open segment: {error}"))?;
    let mut segment_header = [0u8; SEGMENT_HEADER_SIZE];
    file.read_exact(&mut segment_header)
        .map_err(|error| format!("read segment header: {error}"))?;

    let mut stats = OverheadStats {
        occupied_bytes: SEGMENT_HEADER_SIZE as u64,
        event_format_bytes: SEGMENT_HEADER_SIZE as u64,
        ..Default::default()
    };

    loop {
        let mut bytes = [0u8; RECORD_HEADER_SIZE];
        let read = file
            .read(&mut bytes)
            .map_err(|error| format!("read record header: {error}"))?;
        if read == 0 {
            break;
        }
        if read != RECORD_HEADER_SIZE {
            return Err(format!("short record header: {read} bytes"));
        }
        let Some(header) =
            segment::record::parse_header(&bytes).map_err(|error| error.to_string())?
        else {
            break;
        };
        let mut payload = vec![0u8; header.payload_len];
        file.read_exact(&mut payload)
            .map_err(|error| format!("read record payload: {error}"))?;
        if !segment::record::validate_crc(header, &payload) {
            return Err(format!("CRC mismatch at byte {}", stats.occupied_bytes));
        }
        let physical_bytes = header.total_len() as u64;
        stats.occupied_bytes += physical_bytes;

        match segment::record::decode_native(header, &payload).map_err(|error| error.to_string())? {
            segment::record::NativeRecord::Event { .. } => {
                let (event, consumed) = segment::format::deserialize_event(&payload)
                    .map_err(|error| error.to_string())?;
                if consumed != payload.len() {
                    return Err("event record has trailing bytes".into());
                }
                let application_bytes = event.identifier.len()
                    + event.name.len()
                    + event.version.len()
                    + std::mem::size_of_val(&event.timestamp)
                    + event.payload.len()
                    + event
                        .metadata
                        .iter()
                        .map(|(key, value)| key.len() + value.len())
                        .sum::<usize>()
                    + event
                        .tags
                        .iter()
                        .map(|tag| tag.key.len() + tag.value.len())
                        .sum::<usize>();
                stats.application_bytes += application_bytes as u64;
                stats.event_format_bytes += physical_bytes - application_bytes as u64;
            }
            segment::record::NativeRecord::Control(ControlRecord::EpochChange { .. }) => {
                stats.epoch_changes += 1;
                stats.epoch_change_bytes += physical_bytes;
            }
            segment::record::NativeRecord::Control(ControlRecord::WatermarkCheckpoint {
                ..
            }) => {
                stats.watermark_checkpoints += 1;
                stats.watermark_checkpoint_bytes += physical_bytes;
            }
        }
    }

    Ok(stats)
}

fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        s.to_string()
    } else {
        format!("{}…", &s[..max.saturating_sub(1)])
    }
}

fn human_bytes(n: u64) -> String {
    const KIB: u64 = 1024;
    const MIB: u64 = 1024 * KIB;
    const GIB: u64 = 1024 * MIB;
    if n >= GIB {
        format!("{:.2} GiB", n as f64 / GIB as f64)
    } else if n >= MIB {
        format!("{:.2} MiB", n as f64 / MIB as f64)
    } else if n >= KIB {
        format!("{:.2} KiB", n as f64 / KIB as f64)
    } else {
        format!("{n} B")
    }
}
