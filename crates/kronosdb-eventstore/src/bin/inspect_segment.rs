//! Segment inspector — dumps `.seg` file contents in a human-readable form.
//!
//! Usage:
//!   cargo run -p kronosdb-eventstore --bin inspect_segment -- <path/to/segment.seg>
//!   cargo run -p kronosdb-eventstore --bin inspect_segment -- <path> --summary
//!   cargo run -p kronosdb-eventstore --bin inspect_segment -- <path> --from 100 --to 110
//!
//! After Phase 4 (log-as-state), this will also decode interleaved Raft entry
//! markers. For now it just walks event records.

use std::path::PathBuf;
use std::process::ExitCode;

use kronosdb_eventstore::segment::reader::SegmentReader;

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
}

fn parse_args(args: &[String]) -> Result<Options, String> {
    let mut path: Option<PathBuf> = None;
    let mut summary_only = false;
    let mut from: Option<u64> = None;
    let mut to: Option<u64> = None;
    let mut show_payload = false;

    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "-h" | "--help" => {
                print_usage();
                std::process::exit(0);
            }
            "--summary" => summary_only = true,
            "--payload" => show_payload = true,
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
    })
}

fn print_usage() {
    eprintln!("Usage: inspect_segment <path> [--summary] [--payload] [--from N] [--to N]");
    eprintln!();
    eprintln!("  <path>        Path to a .seg file");
    eprintln!("  --summary     Only print the header and record count");
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
    if total > 0 {
        println!("  avg_payload:       {} bytes", total_payload_bytes / total);
    }
    println!("  unique_event_types: {}", unique_names.len());
    if !unique_names.is_empty() && total > 0 {
        for (name, count) in unique_names.iter() {
            println!("    {name}: {count}");
        }
    }

    Ok(())
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
