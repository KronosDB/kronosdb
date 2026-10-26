//! The runtime's own cost over real TCP: an empty request answered inline.
//!
//!   cargo run --release -p kronosdb-protocol --example runtime_bench -- server 127.0.0.1:50071
//!   cargo run --release -p kronosdb-protocol --example runtime_bench -- client 127.0.0.1:50071 <concurrency> <secs>

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use kronosdb_protocol::net::client::{self, ConnectOptions};
use kronosdb_protocol::net::server::{
    self, Accepted, BoxFuture, PeerInfo, ServerConfig, ServerStream, Service,
};
use kronosdb_protocol::net::{Failure, OwnedFrame};
use kronosdb_protocol::{Frame, GetHead, Head};

struct Empty;

impl Service for Empty {
    type Session = ();

    fn hello(&self, _: OwnedFrame, _: Arc<PeerInfo>) -> BoxFuture<Result<Accepted<()>, Failure>> {
        Box::pin(async {
            Ok(Accepted {
                session: Arc::new(()),
                principal: String::new(),
                features: 0,
                max_frame: None,
            })
        })
    }

    fn request(&self, _: &Arc<()>, stream: ServerStream) -> BoxFuture<()> {
        Box::pin(async move {
            let _ = stream.out.finish(&Frame::Head(Head { position: 1 })).await;
        })
    }
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let addr = args
        .get(2)
        .cloned()
        .unwrap_or_else(|| "127.0.0.1:50071".into());
    match args[1].as_str() {
        "server" => {
            let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();
            let (_stop, stop_rx) = tokio::sync::watch::channel(false);
            server::serve(
                listener,
                Arc::new(Empty),
                Arc::new(ServerConfig::default()),
                stop_rx,
            )
            .await;
        }
        "client" => {
            let conc: usize = args.get(3).map(|s| s.parse().unwrap()).unwrap_or(1);
            let secs: u64 = args.get(4).map(|s| s.parse().unwrap()).unwrap_or(4);
            let conn = client::connect(&addr, &ConnectOptions::default())
                .await
                .unwrap();
            let stop = Arc::new(AtomicBool::new(false));
            let measuring = Arc::new(AtomicBool::new(false));
            let mut tasks = Vec::new();
            for _ in 0..conc {
                let (conn, stop, measuring) = (conn.clone(), stop.clone(), measuring.clone());
                tasks.push(tokio::spawn(async move {
                    let mut lat = Vec::new();
                    while !stop.load(Ordering::Relaxed) {
                        let t = Instant::now();
                        conn.request(&Frame::GetHead(GetHead { context: 0 }))
                            .await
                            .unwrap();
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
            let p =
                |q: f64| all[((all.len() as f64 * q) as usize).min(all.len() - 1)] as f64 / 1000.0;
            println!(
                "native c{conc:<4} ops/s={:>9.0} p50={:.3}ms p99={:.3}ms",
                all.len() as f64 / el,
                p(0.5),
                p(0.99)
            );
        }
        _ => panic!("server|client"),
    }
}
