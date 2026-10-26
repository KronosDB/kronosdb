//! Small-op workloads over the native protocol, mirroring the gRPC rustbench.
//!
//!   native_bench prepop <addr> <aggregates> <per>
//!   native_bench append|head|source|loop <addr> <concurrency> <secs> [aggregates] [batch] [connections]
//!   native_bench fanout <addr> <subscribers> <secs> <match-all|filtered|hit> [buckets]
//!   native_bench replay <addr> <concurrency> <secs>
//!   native_bench passes <addr> <concurrency> <passes>
//!
//! With `BENCH_SERVER_PID` set to the server's pid (Linux), every run also
//! reports the server's CPU per operation over the measured window.
//!
//! `append` writes `batch` events per request (default 1); its ops/s counts
//! requests, so events/s is ops/s times the batch. The workers spread over
//! `connections` connections (default 1): one connection is one reader and
//! one writer task on the server, which can cap a run before the engine does.
//!
//! `fanout` runs live subscriptions, one connection each, from the head
//! while 16 appenders write. Every written event carries a `bucket` tag
//! drawn from `buckets` values (default 20, so each value is on 5% of the
//! events). `filtered` subscriptions match nothing that is written; `hit`
//! subscriptions each follow one bucket (subscriber j follows bucket
//! j % buckets) and check that what arrives carries it. `replay` reads the
//! whole log from position 0, over and over. `passes` replays the log a
//! fixed number of times and prints each pass's wall time, so a cold first
//! pass can be told from the warm ones.
//!
//! `NB_WARMUP` (seconds, default 1) is how long the timed modes run before
//! they start measuring; 0 measures from a cold start.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use kronosdb_client::{
    Appended, Client, Condition, ConnectOptions, Criterion, Delivery, NewEvent, SubscribeOptions,
};

fn xorshift(mut x: u64) -> u64 {
    x ^= x >> 33;
    x = x.wrapping_mul(0xff51afd7ed558ccd);
    x ^= x >> 33;
    x = x.wrapping_mul(0xc4ceb9fe1a85ec53);
    x ^ (x >> 33)
}

fn warmup() -> Duration {
    let secs: f64 = std::env::var("NB_WARMUP")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1.0);
    Duration::from_secs_f64(secs)
}

/// CPU ticks (user + system) the server has used so far, when
/// `BENCH_SERVER_PID` names it.
fn server_ticks() -> Option<u64> {
    let pid = std::env::var("BENCH_SERVER_PID").ok()?;
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    let fields: Vec<&str> = stat.rsplit_once(')')?.1.split_whitespace().collect();
    // utime and stime are the 14th and 15th fields; `fields` starts at the 3rd.
    Some(fields.get(11)?.parse::<u64>().ok()? + fields.get(12)?.parse::<u64>().ok()?)
}

/// " srvCPU/op=12.34us" for `ops` operations since `since`, or nothing.
fn cpu_per_op(since: Option<u64>, ops: f64) -> String {
    match (since, server_ticks()) {
        (Some(a), Some(b)) if ops > 0.0 => {
            // 100 ticks a second.
            format!(" srvCPU/op={:.2}us", (b - a) as f64 * 10_000.0 / ops)
        }
        _ => String::new(),
    }
}

fn agg_event(a: u64, n: u64) -> NewEvent {
    const NAMES: [&str; 4] = [
        "OrderPlaced",
        "ItemAdded",
        "PaymentReceived",
        "OrderShipped",
    ];
    NewEvent {
        identifier: format!("{a:010}-{n:016x}"),
        name: NAMES[(n % 4) as usize].into(),
        version: "1".into(),
        timestamp: 1_700_000_000_000,
        metadata: vec![],
        tags: vec![
            (b"orderId".to_vec(), format!("agg-{a}").into_bytes()),
            (
                b"customerId".to_vec(),
                format!("c-{}", a % 500).into_bytes(),
            ),
        ],
        payload: vec![b'x'; 300].into(),
    }
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let arg = |i: usize, d: &str| args.get(i).cloned().unwrap_or_else(|| d.to_string());
    let mode = arg(1, "head");
    let addr = arg(2, "127.0.0.1:50052");
    let client = Client::connect(&addr, &ConnectOptions::default())
        .await
        .unwrap();
    let ctx = client.default_context();

    if mode == "prepop" {
        let aggs: u64 = arg(3, "50000").parse().unwrap();
        let per: u64 = arg(4, "20").parse().unwrap();
        let t0 = Instant::now();
        for r in 0..per {
            let mut batch = Vec::new();
            for a in 0..aggs {
                batch.push(agg_event(a, r));
                if batch.len() == 1000 {
                    ctx.append(&std::mem::take(&mut batch), None).await.unwrap();
                }
            }
            if !batch.is_empty() {
                ctx.append(&batch, None).await.unwrap();
            }
        }
        println!(
            "prepop {} events in {:.1}s",
            aggs * per,
            t0.elapsed().as_secs_f64()
        );
        return;
    }

    if mode == "fanout" {
        let subs: usize = arg(3, "64").parse().unwrap();
        let secs: u64 = arg(4, "4").parse().unwrap();
        let kind = match arg(5, "match-all").as_str() {
            "match-all" => Fan::MatchAll,
            "filtered" => Fan::Nothing,
            "hit" => Fan::Bucket,
            other => panic!("unknown fanout mode {other}"),
        };
        let buckets: u64 = arg(6, "20").parse().unwrap();
        return fanout(&addr, &client, subs, secs, kind, buckets).await;
    }
    if mode == "replay" {
        let conc: usize = arg(3, "1").parse().unwrap();
        let secs: u64 = arg(4, "4").parse().unwrap();
        return replay(&client, conc, secs).await;
    }

    if mode == "passes" {
        let conc: usize = arg(3, "1").parse().unwrap();
        let passes: usize = arg(4, "3").parse().unwrap();
        return passes_run(&client, conc, passes).await;
    }

    let conc: usize = arg(3, "1").parse().unwrap();
    let secs: u64 = arg(4, "4").parse().unwrap();
    let aggs: u64 = arg(5, "50000").parse().unwrap();
    let batch: u64 = arg(6, "1").parse().unwrap();
    let connections: usize = arg(7, "1").parse().unwrap();
    let mut contexts = vec![ctx.clone()];
    for _ in 1..connections {
        let extra = Client::connect(&addr, &ConnectOptions::default())
            .await
            .unwrap();
        contexts.push(extra.default_context());
        // Keep the connection alive for the run; the benchmark exits after.
        std::mem::forget(extra);
    }
    let stop = Arc::new(AtomicBool::new(false));
    let measuring = Arc::new(AtomicBool::new(false));
    let read = Arc::new(AtomicU64::new(0));
    let conflicts = Arc::new(AtomicU64::new(0));
    let mut tasks = Vec::new();
    for w in 0..conc {
        let (ctx, stop, measuring, read, conflicts, mode) = (
            contexts[w % contexts.len()].clone(),
            stop.clone(),
            measuring.clone(),
            read.clone(),
            conflicts.clone(),
            mode.clone(),
        );
        tasks.push(tokio::spawn(async move {
            let mut lat = Vec::new();
            let mut i = (w as u64) << 32;
            while !stop.load(Ordering::Relaxed) {
                i += 1;
                let t = Instant::now();
                match mode.as_str() {
                    "head" => {
                        ctx.head().await.unwrap();
                    }
                    "append" => {
                        let events: Vec<NewEvent> = (0..batch)
                            .map(|k| agg_event(xorshift(i * 1000 + k) % aggs, i * 1000 + k))
                            .collect();
                        ctx.append(&events, None).await.unwrap();
                    }
                    "source" | "loop" => {
                        let a = xorshift(i) % aggs;
                        let criteria = vec![Criterion::tag("orderId", format!("agg-{a}"))];
                        let (batches, marker) = ctx.source_all(0, &criteria).await.unwrap();
                        read.fetch_add(
                            batches.iter().map(|b| b.len() as u64).sum(),
                            Ordering::Relaxed,
                        );
                        if mode == "loop" {
                            let condition = Condition { marker, criteria };
                            let r = ctx
                                .append(&[agg_event(a, 1_000_000 + i)], Some(&condition))
                                .await
                                .unwrap();
                            if matches!(r, Appended::Conflict { .. }) {
                                conflicts.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    }
                    other => panic!("unknown mode {other}"),
                }
                if measuring.load(Ordering::Relaxed) {
                    lat.push(t.elapsed().as_micros() as u64);
                }
            }
            lat
        }));
    }
    tokio::time::sleep(warmup()).await;
    measuring.store(true, Ordering::Relaxed);
    let (c0, t0) = (server_ticks(), Instant::now());
    tokio::time::sleep(Duration::from_secs(secs)).await;
    measuring.store(false, Ordering::Relaxed);
    let el = t0.elapsed().as_secs_f64();
    let c1 = server_ticks();
    stop.store(true, Ordering::Relaxed);
    let mut all = Vec::new();
    for t in tasks {
        all.extend(t.await.unwrap());
    }
    all.sort_unstable();
    let n = all.len().max(1);
    let p = |q: f64| all[((n as f64 * q) as usize).min(n - 1)] as f64 / 1000.0;
    let cpu = match (c0, c1) {
        (Some(a), Some(b)) if !all.is_empty() => format!(
            " srvCPU/op={:.2}us",
            (b - a) as f64 * 10_000.0 / all.len() as f64
        ),
        _ => String::new(),
    };
    println!(
        "native {mode:<6} c{conc:<4} b{batch:<4} n{connections:<2} ops/s={:>9.0} p50={:>7.3}ms p90={:>7.3}ms p99={:>7.3}ms{cpu}",
        all.len() as f64 / el,
        p(0.5),
        p(0.9),
        p(0.99)
    );
    if mode == "source" || mode == "loop" {
        println!(
            "    events/read={:.1} conflicts={}",
            read.load(Ordering::Relaxed) as f64 / all.len().max(1) as f64,
            conflicts.load(Ordering::Relaxed)
        );
    }
}

#[derive(Clone, Copy, PartialEq)]
enum Fan {
    MatchAll,
    Nothing,
    Bucket,
}

fn bucket_name(b: u64) -> Vec<u8> {
    format!("b{b}").into_bytes()
}

async fn fanout(addr: &str, client: &Client, subs: usize, secs: u64, kind: Fan, buckets: u64) {
    let from = client.default_context().head().await.unwrap();
    let delivered = Arc::new(AtomicU64::new(0));
    let wrong = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let mut readers = Vec::new();
    for j in 0..subs {
        let sub_client = Client::connect(addr, &ConnectOptions::default())
            .await
            .unwrap();
        let want = bucket_name(j as u64 % buckets);
        let options = SubscribeOptions {
            criteria: match kind {
                Fan::MatchAll => vec![],
                Fan::Nothing => vec![Criterion::tag("orderId", "nobody-writes-this")],
                Fan::Bucket => vec![Criterion::tag("bucket", want.clone())],
            },
            ..SubscribeOptions::default()
        };
        let mut sub = sub_client
            .default_context()
            .subscribe(from, options)
            .await
            .unwrap();
        let (delivered, wrong, stop) = (delivered.clone(), wrong.clone(), stop.clone());
        readers.push(tokio::spawn(async move {
            let _keep = sub_client;
            let mut last = 0u64;
            while !stop.load(Ordering::Relaxed) {
                match tokio::time::timeout(Duration::from_millis(200), sub.next()).await {
                    Ok(Ok(Some(Delivery::Events(batch)))) => {
                        delivered.fetch_add(batch.len() as u64, Ordering::Relaxed);
                        if kind == Fan::Bucket {
                            // The first and last event of every batch: cheap, and
                            // enough to catch a record from the wrong subscriber.
                            let mut it = batch.iter();
                            let first = it.next();
                            for e in first.into_iter().chain(it.last()) {
                                let ok =
                                    e.tags.iter().any(|t| t.key == b"bucket" && t.value == want);
                                if !ok || e.position < last {
                                    wrong.fetch_add(1, Ordering::Relaxed);
                                }
                                last = e.position;
                            }
                        }
                    }
                    Ok(Ok(Some(Delivery::Progress(_)))) | Err(_) => {}
                    _ => return,
                }
            }
        }));
    }
    let appended = Arc::new(AtomicU64::new(0));
    let mut writers = Vec::new();
    for w in 0..16u64 {
        let (ctx, stop, appended) = (client.default_context(), stop.clone(), appended.clone());
        writers.push(tokio::spawn(async move {
            let mut i = w << 32;
            while !stop.load(Ordering::Relaxed) {
                i += 1;
                let mut event = agg_event(xorshift(i) % 50_000, i);
                event.tags.push((
                    b"bucket".to_vec(),
                    bucket_name(xorshift(i ^ 0x9e37) % buckets),
                ));
                ctx.append(&[event], None).await.unwrap();
                appended.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }
    tokio::time::sleep(warmup()).await;
    let (a0, d0, c0, t0) = (
        appended.load(Ordering::Relaxed),
        delivered.load(Ordering::Relaxed),
        server_ticks(),
        Instant::now(),
    );
    tokio::time::sleep(Duration::from_secs(secs)).await;
    let el = t0.elapsed().as_secs_f64();
    let (a1, d1) = (
        appended.load(Ordering::Relaxed),
        delivered.load(Ordering::Relaxed),
    );
    let cpu = cpu_per_op(c0, (a1 - a0) as f64);
    let cpu_per_delivery = cpu_per_op(c0, (d1 - d0) as f64).replace("srvCPU/op", "srvCPU/delivery");
    stop.store(true, Ordering::Relaxed);
    for t in writers.into_iter().chain(readers) {
        let _ = t.await;
    }
    println!(
        "native fanout subs={subs:<4} {} ops/s={:>9.0} delivered/s={:>10.0}{cpu}{cpu_per_delivery}",
        match kind {
            Fan::MatchAll => "match-all",
            Fan::Nothing => "filtered ",
            Fan::Bucket => "hit      ",
        },
        (a1 - a0) as f64 / el,
        (d1 - d0) as f64 / el
    );
    let wrong = wrong.load(Ordering::Relaxed);
    if wrong > 0 {
        println!("    WRONG deliveries: {wrong}");
        std::process::exit(1);
    }
}

async fn replay(client: &Client, conc: usize, secs: u64) {
    let read = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let mut tasks = Vec::new();
    for _ in 0..conc {
        let (ctx, read, stop) = (client.default_context(), read.clone(), stop.clone());
        tasks.push(tokio::spawn(async move {
            while !stop.load(Ordering::Relaxed) {
                let mut source = ctx.source(0, &[], 1024).await.unwrap();
                while let Some(batch) = source.next().await.unwrap() {
                    read.fetch_add(batch.len() as u64, Ordering::Relaxed);
                    if stop.load(Ordering::Relaxed) {
                        break;
                    }
                }
            }
        }));
    }
    tokio::time::sleep(warmup()).await;
    let (r0, c0, t0) = (read.load(Ordering::Relaxed), server_ticks(), Instant::now());
    tokio::time::sleep(Duration::from_secs(secs)).await;
    let el = t0.elapsed().as_secs_f64();
    let r1 = read.load(Ordering::Relaxed);
    let cpu = cpu_per_op(c0, (r1 - r0) as f64);
    stop.store(true, Ordering::Relaxed);
    for t in tasks {
        let _ = t.await;
    }
    // Ops are events here, so the server-CPU/op the harness prints is per
    // delivered event.
    println!(
        "native replay  c{conc:<4} ops/s={:>9.0} (events/s, all readers){cpu}",
        (r1 - r0) as f64 / el
    );
}

async fn passes_run(client: &Client, conc: usize, passes: usize) {
    for pass in 1..=passes {
        let read = Arc::new(AtomicU64::new(0));
        let t0 = Instant::now();
        let mut tasks = Vec::new();
        for _ in 0..conc {
            let (ctx, read) = (client.default_context(), read.clone());
            tasks.push(tokio::spawn(async move {
                let mut source = ctx.source(0, &[], 1024).await.unwrap();
                while let Some(batch) = source.next().await.unwrap() {
                    read.fetch_add(batch.len() as u64, Ordering::Relaxed);
                }
            }));
        }
        for t in tasks {
            t.await.unwrap();
        }
        let el = t0.elapsed().as_secs_f64();
        let n = read.load(Ordering::Relaxed);
        println!(
            "native passes c{conc} pass={pass} events={n} secs={el:.3} ops/s={:>9.0} (events/s, all readers)",
            n as f64 / el
        );
    }
}
