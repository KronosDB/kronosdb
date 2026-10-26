//! Command dispatch round trip over the native protocol: one echo handler on
//! its own connection, `concurrency` dispatchers sharing a second one.
//!
//!   messaging_bench <addr> <concurrency> <secs>

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use kronosdb_client::{Client, Command, ConnectOptions, Payload, Reply};

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let arg = |i: usize, d: &str| args.get(i).cloned().unwrap_or_else(|| d.to_string());
    let addr = arg(1, "127.0.0.1:50051");
    let conc: usize = arg(2, "1").parse().unwrap();
    let secs: u64 = arg(3, "5").parse().unwrap();

    // The handler: subscribe, then echo every command.
    let handler_conn = Client::connect(&addr, &ConnectOptions::default())
        .await
        .unwrap();
    let mut handlers = handler_conn
        .default_bus()
        .command_handlers("bench-handler", "bench", 65_536)
        .await
        .unwrap();
    handlers.subscribe("Echo", 100).await.unwrap();
    tokio::spawn(async move {
        while let Ok(Some(incoming)) = handlers.next().await {
            let reply = Reply::ok(
                incoming.command.message_id.clone(),
                incoming.command.payload.clone(),
            );
            if incoming.respond(&reply).await.is_err() {
                break;
            }
        }
    });
    tokio::time::sleep(Duration::from_millis(500)).await;

    let dispatcher = Client::connect(&addr, &ConnectOptions::default())
        .await
        .unwrap();
    let stop = Arc::new(AtomicBool::new(false));
    let measuring = Arc::new(AtomicBool::new(false));
    let mut tasks = Vec::new();
    for w in 0..conc {
        let (bus, stop, measuring) = (dispatcher.default_bus(), stop.clone(), measuring.clone());
        tasks.push(tokio::spawn(async move {
            let mut lat = Vec::new();
            let mut i = 0u64;
            let body = Bytes::from(vec![b'x'; 200]);
            while !stop.load(Ordering::Relaxed) {
                i += 1;
                let mut cmd = Command::new(
                    format!("w{w}-{i}"),
                    "Echo",
                    Payload::new("Echo", body.clone()),
                );
                cmd.client_id = "bench-dispatcher".into();
                cmd.component_name = "bench".into();
                let t = Instant::now();
                bus.dispatch_command(&cmd).await.unwrap();
                if measuring.load(Ordering::Relaxed) {
                    lat.push(t.elapsed().as_micros() as u64);
                }
            }
            lat
        }));
    }
    tokio::time::sleep(Duration::from_secs(1)).await;
    measuring.store(true, Ordering::Relaxed);
    let t0 = Instant::now();
    tokio::time::sleep(Duration::from_secs(secs)).await;
    measuring.store(false, Ordering::Relaxed);
    let el = t0.elapsed().as_secs_f64();
    stop.store(true, Ordering::Relaxed);
    let mut all = Vec::new();
    for t in tasks {
        all.extend(t.await.unwrap());
    }
    all.sort_unstable();
    let n = all.len().max(1);
    let p = |q: f64| all[((n as f64 * q) as usize).min(n - 1)] as f64 / 1000.0;
    println!(
        "native command c{conc:<4} ops/s={:>9.0} p50={:>7.3}ms p90={:>7.3}ms p99={:>7.3}ms",
        all.len() as f64 / el,
        p(0.5),
        p(0.9),
        p(0.99)
    );
}
