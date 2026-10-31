//! The runtime end to end over in-memory pipes, against a toy service.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::watch;

use super::client::{self, ConnectOptions, Credential};
use super::rules::{DEFAULT_WINDOW, MIN_WINDOW};
use super::server::{self, Accepted, BoxFuture, PeerInfo, ServerConfig, ServerStream, Service};
use super::{Failure, NetError, OwnedFrame, encode};
use crate::codec::{FLAG_END, PREFACE, Records, RecordsBuf};
use crate::generated::*;

/// What the toy's flooding Source did, for tests to look at.
#[derive(Default)]
struct Flood {
    sent: AtomicUsize,
    ended: AtomicUsize,
}

struct Toy {
    flood: Arc<Flood>,
}

struct ToySession {
    principal: String,
}

impl Service for Toy {
    type Session = ToySession;

    fn hello(
        &self,
        hello: OwnedFrame,
        _peer: Arc<PeerInfo>,
    ) -> BoxFuture<Result<Accepted<ToySession>, Failure>> {
        let credential = match hello.decode() {
            Ok(Frame::Hello(h)) => h.credential.to_string(),
            _ => unreachable!(),
        };
        Box::pin(async move {
            if credential == "wrong" {
                return Err(Failure::new(
                    ErrorCode::UNAUTHENTICATED,
                    "the token is not valid",
                ));
            }
            let principal = format!("token:{credential}");
            Ok(Accepted {
                session: Arc::new(ToySession {
                    principal: principal.clone(),
                }),
                principal,
                features: 0,
                max_frame: None,
            })
        })
    }

    fn request(&self, session: &Arc<ToySession>, stream: ServerStream) -> BoxFuture<()> {
        let session = Arc::clone(session);
        let self_flood = Arc::clone(&self.flood);
        Box::pin(async move {
            let out = stream.out.clone();
            match stream.request.decode().unwrap() {
                // Answered without waiting: runs inline on the reader.
                Frame::GetHead(r) => {
                    let position = r.context as u64 * 1000 + 7;
                    let _ = out.finish(&Frame::Head(Head { position })).await;
                }
                // A Source of `from` frames of `batch_size` payload bytes each,
                // sent as fast as the connection takes them; stops when cancelled.
                Frame::Source(s) if s.from > 0 => {
                    let payload = vec![7u8; s.batch_size as usize];
                    let mut records = RecordsBuf::new();
                    records
                        .push(&EventRecord {
                            position: 0,
                            identifier: "e",
                            name: "n",
                            version: "1",
                            timestamp: 0,
                            metadata: vec![],
                            tags: vec![],
                            payload: &payload,
                        })
                        .unwrap();
                    for n in 1..=s.from {
                        if stream.inbound.is_cancelled() {
                            break;
                        }
                        let events = Events {
                            events: records.as_records(),
                            marker: (n == s.from).then_some(99),
                        };
                        let sent = if n == s.from {
                            out.finish(&Frame::Events(events)).await
                        } else {
                            out.send(&Frame::Events(events)).await
                        };
                        if sent.is_err() {
                            break;
                        }
                        self_flood.sent.fetch_add(1, Ordering::Relaxed);
                    }
                    self_flood.ended.fetch_add(1, Ordering::Relaxed);
                }
                Frame::Source(_) => {
                    for _ in 0..3 {
                        let _ = out
                            .send(&Frame::Events(Events {
                                events: Records::from_raw(0, &[]).unwrap(),
                                marker: None,
                            }))
                            .await;
                    }
                    let _ = out
                        .finish(&Frame::Events(Events {
                            events: Records::from_raw(0, &[]).unwrap(),
                            marker: Some(99),
                        }))
                        .await;
                }
                // Progress after progress, as fast as the window lets it;
                // ends when cancelled.
                Frame::Subscribe(s) => {
                    let mut position = s.from;
                    loop {
                        position += 1;
                        if out
                            .send(&Frame::Progress(Progress { position }))
                            .await
                            .is_err()
                        {
                            return;
                        }
                    }
                }
                Frame::WhoAmI(_) => {
                    let _ = out
                        .finish(&Frame::Identity(Identity {
                            subject: &session.principal,
                            source: "token",
                            grants: vec![],
                            authentication_enabled: true,
                        }))
                        .await;
                }
                // Waits, so it is spawned instead of finishing inline.
                Frame::GetTail(_) => {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                    let _ = out.finish(&Frame::Tail(Tail { position: 3 })).await;
                }
                // Waits long enough to tell a connection that waits with it.
                Frame::GetPositionAt(_) => {
                    tokio::time::sleep(Duration::from_millis(300)).await;
                    let _ = out
                        .finish(&Frame::PositionAt(PositionAt { position: Some(1) }))
                        .await;
                }
                // Forgets to answer: the runtime answers for it.
                _ => {}
            }
        })
    }
}

struct Harness {
    flood: Arc<Flood>,
    conn: client::Connection,
    shutdown: watch::Sender<bool>,
    server: tokio::task::JoinHandle<Result<(), NetError>>,
}

async fn start(credential: &str, heartbeat: Duration) -> Result<Harness, NetError> {
    start_with(credential, heartbeat, DEFAULT_WINDOW).await
}

async fn start_with(
    credential: &str,
    heartbeat: Duration,
    window: u32,
) -> Result<Harness, NetError> {
    let flood = Arc::new(Flood::default());
    let (client_io, server_io) = tokio::io::duplex(1 << 16);
    let (shutdown, shutdown_rx) = watch::channel(false);
    let config = Arc::new(ServerConfig {
        server_name: "toy".into(),
        heartbeat,
        ..ServerConfig::default()
    });
    let peer = Arc::new(PeerInfo {
        addr: "127.0.0.1:1".parse().unwrap(),
        certificates: vec![],
    });
    let server = tokio::spawn(server::run(
        server_io,
        peer,
        Arc::new(Toy {
            flood: Arc::clone(&flood),
        }),
        config,
        shutdown_rx,
    ));
    let options = ConnectOptions {
        client_name: "test".into(),
        client_id: "test-1".into(),
        credential: Credential::Token(credential.into()),
        window,
        ..ConnectOptions::default()
    };
    let conn = client::handshake(client_io, &options).await?;
    Ok(Harness {
        flood,
        conn,
        shutdown,
        server,
    })
}

fn head(frame: &OwnedFrame) -> u64 {
    match frame.decode().unwrap() {
        Frame::Head(h) => h.position,
        other => panic!("expected Head, got {}", other.name()),
    }
}

#[tokio::test]
async fn handshake_reports_the_principal() {
    let h = start("ci", Duration::from_secs(10)).await.unwrap();
    assert_eq!(h.conn.welcome().principal, "token:ci");
    assert_eq!(h.conn.welcome().server_name, "toy");
    let who = h.conn.request(&Frame::WhoAmI(WhoAmI {})).await.unwrap();
    assert!(matches!(who.decode().unwrap(), Frame::Identity(i) if i.subject == "token:ci"));
}

#[tokio::test]
async fn refused_credentials_fail_the_connect() {
    let error = start("wrong", Duration::from_secs(10)).await.err().unwrap();
    let failure = error.failure().expect("a failure from the server");
    assert_eq!(failure.code, ErrorCode::UNAUTHENTICATED);
}

#[tokio::test]
async fn concurrent_requests_get_their_own_answers() {
    let h = start("ci", Duration::from_secs(10)).await.unwrap();
    let mut tasks = Vec::new();
    for i in 0..2000u16 {
        let conn = h.conn.clone();
        tasks.push(tokio::spawn(async move {
            let answer = conn
                .request(&Frame::GetHead(GetHead { context: i % 50 }))
                .await
                .unwrap();
            assert_eq!(head(&answer), (i % 50) as u64 * 1000 + 7);
        }));
    }
    // Some wait, so they run spawned, interleaved with the inline ones.
    for _ in 0..50 {
        let conn = h.conn.clone();
        tasks.push(tokio::spawn(async move {
            let answer = conn
                .request(&Frame::GetTail(GetTail { context: 0 }))
                .await
                .unwrap();
            assert!(matches!(answer.decode().unwrap(), Frame::Tail(t) if t.position == 3));
        }));
    }
    for t in tasks {
        t.await.unwrap();
    }
}

#[tokio::test]
async fn server_streams_end_with_their_last_frame() {
    let h = start("ci", Duration::from_secs(10)).await.unwrap();
    let mut stream = h
        .conn
        .open(
            &Frame::Source(Source {
                context: 0,
                from: 0,
                criteria: vec![],
                batch_size: 0,
            }),
            true,
        )
        .await
        .unwrap();
    let mut markers = Vec::new();
    while let Some(frame) = stream.next().await.unwrap() {
        match frame.decode().unwrap() {
            Frame::Events(e) => markers.push(e.marker),
            other => panic!("unexpected {}", other.name()),
        }
    }
    assert_eq!(markers, vec![None, None, None, Some(99)]);
}

fn subscribe(from: u64) -> Frame<'static> {
    Frame::Subscribe(Subscribe {
        context: 0,
        from,
        criteria: vec![],
        exclude_names: vec![],
        batch_size: 0,
        progress_interval: 0,
    })
}

#[tokio::test]
async fn subscriptions_follow_their_window_and_end_on_cancel() {
    let h = start_with("ci", Duration::from_secs(10), MIN_WINDOW)
        .await
        .unwrap();
    let mut stream = h.conn.open(&subscribe(100), true).await.unwrap();
    // Far more than one window's worth arrives, in order, as it is read.
    for expected in 101..=20_100 {
        let frame = stream.next().await.unwrap().unwrap();
        let Frame::Progress(p) = frame.decode().unwrap() else {
            panic!()
        };
        assert_eq!(p.position, expected);
    }

    // Cancel is confirmed with RequestFailed(cancelled), and the connection lives on.
    stream.send_end(&Frame::Cancel(Cancel {})).await.unwrap();
    let error = loop {
        match stream.next().await {
            Ok(Some(_)) => continue,
            Ok(None) => panic!("ended without confirming the cancel"),
            Err(e) => break e,
        }
    };
    assert_eq!(error.failure().unwrap().code, ErrorCode::CANCELLED);
    let answer = h
        .conn
        .request(&Frame::GetHead(GetHead { context: 1 }))
        .await
        .unwrap();
    assert_eq!(head(&answer), 1007);
}

#[tokio::test]
async fn a_handler_that_never_answers_is_answered_for() {
    let h = start("ci", Duration::from_secs(10)).await.unwrap();
    let error = h
        .conn
        .request(&Frame::GetTags(GetTags {
            context: 0,
            position: 0,
        }))
        .await
        .unwrap_err();
    assert_eq!(error.failure().unwrap().code, ErrorCode::INTERNAL);
}

#[tokio::test]
async fn idle_connections_stay_up_on_heartbeats() {
    let h = start("ci", Duration::from_millis(40)).await.unwrap();
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert!(!h.conn.is_closed(), "the client stopped answering pings");
    let answer = h
        .conn
        .request(&Frame::GetHead(GetHead { context: 2 }))
        .await
        .unwrap();
    assert_eq!(head(&answer), 2007);
}

#[tokio::test]
async fn shutdown_drains_open_requests_then_closes() {
    let h = start("ci", Duration::from_secs(10)).await.unwrap();
    let slow = {
        let conn = h.conn.clone();
        tokio::spawn(async move { conn.request(&Frame::GetTail(GetTail { context: 0 })).await })
    };
    tokio::time::sleep(Duration::from_millis(5)).await;
    h.shutdown.send(true).unwrap();
    // The request already open is finished, not dropped.
    assert!(slow.await.unwrap().is_ok());
    tokio::time::timeout(Duration::from_secs(1), h.conn.closed())
        .await
        .expect("the connection closes once drained");
    assert!(h.conn.is_draining());
    assert!(h.server.await.unwrap().is_ok());
}

#[tokio::test]
async fn dropping_every_handle_closes_the_connection() {
    let Harness {
        conn,
        shutdown,
        server,
        ..
    } = start("ci", Duration::from_secs(10)).await.unwrap();
    let mut stream = conn
        .open(&Frame::GetTail(GetTail { context: 0 }), true)
        .await
        .unwrap();
    assert!(stream.next().await.unwrap().is_some());
    drop(stream);
    drop(conn);
    // Nothing can send any more: the client closes, and the server follows.
    let ended = tokio::time::timeout(Duration::from_secs(1), server)
        .await
        .expect("the server saw the connection close");
    assert!(ended.unwrap().is_ok());
    drop(shutdown);
}

#[tokio::test]
async fn a_verified_peer_may_send_larger_frames() {
    struct Wide;
    impl Service for Wide {
        type Session = ();
        fn hello(
            &self,
            _: OwnedFrame,
            _: Arc<PeerInfo>,
        ) -> BoxFuture<Result<Accepted<()>, Failure>> {
            Box::pin(async {
                Ok(Accepted {
                    session: Arc::new(()),
                    principal: String::new(),
                    features: 0,
                    max_frame: Some(1 << 20),
                })
            })
        }
        fn request(&self, _: &Arc<()>, stream: ServerStream) -> BoxFuture<()> {
            Box::pin(async move {
                let _ = stream.out.finish(&Frame::Head(Head { position: 1 })).await;
            })
        }
    }
    let (client_io, server_io) = tokio::io::duplex(1 << 20);
    let (_shutdown, shutdown_rx) = watch::channel(false);
    let config = Arc::new(ServerConfig {
        max_frame: 1 << 10,
        ..ServerConfig::default()
    });
    let peer = Arc::new(PeerInfo {
        addr: "127.0.0.1:1".parse().unwrap(),
        certificates: vec![],
    });
    tokio::spawn(server::run(
        server_io,
        peer,
        Arc::new(Wide),
        config,
        shutdown_rx,
    ));
    let conn = client::handshake(client_io, &ConnectOptions::default())
        .await
        .unwrap();
    assert_eq!(conn.welcome().max_frame, 1 << 20);
    let big = vec![7u8; 100 * 1024];
    let answer = conn
        .request(&Frame::RaftVote(RaftVote { data: &big }))
        .await
        .unwrap();
    assert_eq!(head(&answer), 1);
}

#[tokio::test]
async fn a_protocol_violation_closes_the_connection() {
    let (mut client_io, server_io) = tokio::io::duplex(1 << 16);
    let (_shutdown, shutdown_rx) = watch::channel(false);
    let server = tokio::spawn(server::run(
        server_io,
        Arc::new(PeerInfo {
            addr: "127.0.0.1:1".parse().unwrap(),
            certificates: vec![],
        }),
        Arc::new(Toy {
            flood: Arc::default(),
        }),
        Arc::new(ServerConfig::default()),
        shutdown_rx,
    ));
    let mut bytes = hello_bytes();
    // A server frame from the client.
    bytes.extend_from_slice(&encode(5, FLAG_END, &Frame::Head(Head { position: 1 })).unwrap());
    client_io.write_all(&bytes).await.unwrap();
    let mut received = Vec::new();
    client_io.read_to_end(&mut received).await.unwrap();
    assert!(server.await.unwrap().is_err());
    // Welcome, then RequestFailed(protocol_violation) on stream 0.
    let first = crate::codec::split_frame(&received).unwrap().unwrap();
    assert_eq!(first.header.kind, Welcome::KIND);
    let second = crate::codec::split_frame(&received[first.len..])
        .unwrap()
        .unwrap();
    let Some(Frame::RequestFailed(f)) = Frame::decode(second.header.kind, second.body).unwrap()
    else {
        panic!("expected RequestFailed");
    };
    assert_eq!(f.code, ErrorCode::PROTOCOL_VIOLATION);
    assert_eq!(second.header.stream, 0);
}

fn flood_source(frames: u64, payload: u32) -> Frame<'static> {
    Frame::Source(Source {
        context: 0,
        from: frames,
        criteria: vec![],
        batch_size: payload,
    })
}

/// Many concurrent requests on `conn` all answered, within a few seconds.
async fn others_still_complete(conn: &client::Connection) {
    let mut tasks = Vec::new();
    for i in 0..500u16 {
        let conn = conn.clone();
        tasks.push(tokio::spawn(async move {
            let answer = conn
                .request(&Frame::GetHead(GetHead { context: i % 50 }))
                .await
                .unwrap();
            assert_eq!(head(&answer), (i % 50) as u64 * 1000 + 7);
        }));
    }
    tokio::time::timeout(Duration::from_secs(5), async {
        for t in tasks {
            t.await.unwrap();
        }
    })
    .await
    .expect("unrelated requests were held up by a stream nobody reads");
}

/// Waits until the toy's flood has stopped sending for a while; returns how
/// many frames it sent.
async fn settled(flood: &Flood) -> usize {
    let mut last = usize::MAX;
    loop {
        tokio::time::sleep(Duration::from_millis(30)).await;
        let sent = flood.sent.load(Ordering::Relaxed);
        if sent == last {
            return sent;
        }
        last = sent;
    }
}

#[tokio::test]
async fn an_unread_stream_holds_nothing_up() {
    let h = start("ci", Duration::from_secs(10)).await.unwrap();
    let mut stream = h.conn.open(&subscribe(0), true).await.unwrap();
    // The server sends a whole window while nobody reads it.
    others_still_complete(&h.conn).await;
    others_still_complete(&h.conn).await;

    // Every frame is there, in order, once the consumer reads.
    for expected in 1..=100_000 {
        let frame = stream.next().await.unwrap().unwrap();
        let Frame::Progress(p) = frame.decode().unwrap() else {
            panic!()
        };
        assert_eq!(p.position, expected);
    }
    stream.cancel().await;
}

#[tokio::test]
async fn a_stream_nobody_reads_waits_at_its_window_and_is_read_whole_however_late() {
    // Room for three frames of 30 kB and a fourth that overdraws the window.
    let h = start_with("ci", Duration::from_secs(10), 100_000)
        .await
        .unwrap();
    let mut stream = h.conn.open(&flood_source(200, 30_000), true).await.unwrap();
    others_still_complete(&h.conn).await;
    assert_eq!(
        settled(&h.flood).await,
        4,
        "the server sent past the window"
    );

    // The consumer comes back late: nothing was lost, and nothing failed.
    let mut markers = Vec::new();
    while let Some(frame) = stream.next().await.unwrap() {
        let Frame::Events(e) = frame.decode().unwrap() else {
            panic!()
        };
        markers.push(e.marker);
    }
    assert_eq!(markers.len(), 200);
    assert_eq!(markers.last().unwrap(), &Some(99));
    others_still_complete(&h.conn).await;
}

#[tokio::test]
async fn a_frame_bigger_than_the_window_still_arrives() {
    let h = start_with("ci", Duration::from_secs(10), MIN_WINDOW)
        .await
        .unwrap();
    let mut stream = h.conn.open(&flood_source(3, 100_000), true).await.unwrap();
    let mut frames = 0;
    while stream.next().await.unwrap().is_some() {
        frames += 1;
    }
    assert_eq!(frames, 3);
}

#[tokio::test]
async fn a_cancelled_stream_stops_at_once() {
    let h = start("ci", Duration::from_secs(10)).await.unwrap();
    let stream = h
        .conn
        .open(&flood_source(100_000, 30_000), true)
        .await
        .unwrap();
    let sent = settled(&h.flood).await;
    assert!(
        sent < 1_000,
        "the server sent {sent} frames past the window"
    );
    stream.cancel().await;
    tokio::time::timeout(Duration::from_secs(5), async {
        while h.flood.ended.load(Ordering::Relaxed) == 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("the server went on with a stream the client cancelled");
    assert_eq!(h.flood.sent.load(Ordering::Relaxed), sent);
    others_still_complete(&h.conn).await;
}

#[tokio::test]
async fn a_cancel_is_read_while_the_server_is_behind_on_writing() {
    // The client opens a stream and never reads: the server fills the pipe
    // and the stream's window and is behind. A Cancel still gets through.
    let (client_io, server_io) = tokio::io::duplex(4096);
    let (_shutdown, shutdown_rx) = watch::channel(false);
    let flood = Arc::new(Flood::default());
    let server = tokio::spawn(server::run(
        server_io,
        Arc::new(PeerInfo {
            addr: "127.0.0.1:1".parse().unwrap(),
            certificates: vec![],
        }),
        Arc::new(Toy {
            flood: Arc::clone(&flood),
        }),
        Arc::new(ServerConfig::default()),
        shutdown_rx,
    ));
    let mut bytes = hello_bytes();
    bytes.extend_from_slice(&encode(1, FLAG_END, &flood_source(100_000, 30_000)).unwrap());
    let (_read_half, mut write_half) = tokio::io::split(client_io);
    write_half.write_all(&bytes).await.unwrap();
    settled(&flood).await;
    assert_eq!(flood.ended.load(Ordering::Relaxed), 0);
    let cancel = encode(1, FLAG_END, &Frame::Cancel(Cancel {})).unwrap();
    tokio::time::timeout(Duration::from_secs(1), write_half.write_all(&cancel))
        .await
        .expect("the server stopped reading")
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while flood.ended.load(Ordering::Relaxed) == 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("the Cancel waited behind the replies it was meant to stop");
    server.abort();
}

#[tokio::test]
async fn an_older_client_is_told_to_upgrade() {
    let error = handshake_raw(PROTOCOL_VERSION - 1, DEFAULT_WINDOW).await;
    assert_eq!(error.code, ErrorCode::REJECTED);
    assert!(error.message.contains("upgrade the client"), "{error}");
}

#[tokio::test]
async fn a_window_below_the_minimum_is_refused() {
    let error = handshake_raw(PROTOCOL_VERSION, MIN_WINDOW - 1).await;
    assert_eq!(error.code, ErrorCode::INVALID_ARGUMENT);
}

/// A handshake the server refuses: what it said.
async fn handshake_raw(version: u16, window: u32) -> Failure {
    let (mut client_io, server_io) = tokio::io::duplex(1 << 16);
    let (_shutdown, shutdown_rx) = watch::channel(false);
    let server = tokio::spawn(server::run(
        server_io,
        Arc::new(PeerInfo {
            addr: "127.0.0.1:1".parse().unwrap(),
            certificates: vec![],
        }),
        Arc::new(Toy {
            flood: Arc::default(),
        }),
        Arc::new(ServerConfig::default()),
        shutdown_rx,
    ));
    let mut bytes = PREFACE.to_vec();
    bytes.extend_from_slice(&encode(0, 0, &hello(version, window)).unwrap());
    client_io.write_all(&bytes).await.unwrap();
    let mut received = Vec::new();
    client_io.read_to_end(&mut received).await.unwrap();
    assert!(server.await.unwrap().is_err());
    let first = crate::codec::split_frame(&received).unwrap().unwrap();
    let Some(Frame::RequestFailed(f)) = Frame::decode(first.header.kind, first.body).unwrap()
    else {
        panic!("expected RequestFailed");
    };
    Failure::new(f.code, f.message)
}

fn hello(version: u16, window: u32) -> Frame<'static> {
    Frame::Hello(Hello {
        version,
        features: 0,
        record_format: server::RECORD_FORMAT,
        client_name: "",
        client_id: "",
        credential_kind: CredentialKind::TOKEN,
        credential: "ci",
        window,
    })
}

/// The preface and a Hello, as a raw client sends them.
fn hello_bytes() -> Vec<u8> {
    let mut bytes = PREFACE.to_vec();
    bytes.extend_from_slice(&encode(0, 0, &hello(PROTOCOL_VERSION, DEFAULT_WINDOW)).unwrap());
    bytes
}

/// Hello and a run of GetHead requests, as one burst of bytes.
fn raw_requests(count: u32) -> Vec<u8> {
    let mut bytes = hello_bytes();
    for id in 1..=count {
        bytes.extend_from_slice(
            &encode(id, FLAG_END, &Frame::GetHead(GetHead { context: 0 })).unwrap(),
        );
    }
    bytes
}

/// Reads frames off `io` until `count` heads arrived, returning their streams.
async fn read_heads(io: &mut tokio::io::DuplexStream, count: usize) -> Vec<u32> {
    let mut buf = Vec::new();
    let mut streams = Vec::new();
    let mut chunk = vec![0u8; 16 * 1024];
    while streams.len() < count {
        while let Some(split) = crate::codec::split_frame(&buf).unwrap() {
            if split.header.kind == Head::KIND {
                streams.push(split.header.stream);
            }
            buf.drain(..split.len);
        }
        if streams.len() < count {
            let n = io.read(&mut chunk).await.unwrap();
            assert!(n > 0, "the server closed with answers missing");
            buf.extend_from_slice(&chunk[..n]);
        }
    }
    streams
}

#[tokio::test]
async fn a_peer_that_reads_late_still_gets_every_answer() {
    // The pipe holds a few kilobytes and the answers add up to far more, yet
    // the peer sends all its requests before it reads any answer. The server
    // has to keep reading while its writes are blocked, or this deadlocks.
    let (client_io, server_io) = tokio::io::duplex(4096);
    let (_shutdown, shutdown_rx) = watch::channel(false);
    let server = tokio::spawn(server::run(
        server_io,
        Arc::new(PeerInfo {
            addr: "127.0.0.1:1".parse().unwrap(),
            certificates: vec![],
        }),
        Arc::new(Toy {
            flood: Arc::new(Flood::default()),
        }),
        Arc::new(ServerConfig::default()),
        shutdown_rx,
    ));
    let count = 8_000u32;
    let bytes = raw_requests(count);
    let (read_half, mut write_half) = tokio::io::split(client_io);
    tokio::time::timeout(Duration::from_secs(5), write_half.write_all(&bytes))
        .await
        .expect("the server stopped reading while its writes were blocked")
        .unwrap();
    let mut client_io = read_half.unsplit(write_half);
    // Requests that cannot be answered at once run concurrently, so the
    // answers are not in order; each stream is answered once.
    let mut streams = read_heads(&mut client_io, count as usize).await;
    streams.sort_unstable();
    assert_eq!(streams, (1..=count).collect::<Vec<_>>());
    drop(client_io);
    assert!(server.await.unwrap().is_ok());
}

#[tokio::test]
async fn a_peer_that_never_reads_cannot_make_the_server_buffer_without_bound() {
    // Requests keep coming and no answer is ever read: the server stops
    // reading once it holds a fixed amount of unwritten answers.
    let (client_io, server_io) = tokio::io::duplex(4096);
    let (_shutdown, shutdown_rx) = watch::channel(false);
    let server = tokio::spawn(server::run(
        server_io,
        Arc::new(PeerInfo {
            addr: "127.0.0.1:1".parse().unwrap(),
            certificates: vec![],
        }),
        Arc::new(Toy {
            flood: Arc::new(Flood::default()),
        }),
        Arc::new(ServerConfig::default()),
        shutdown_rx,
    ));
    let bytes = raw_requests(200_000);
    let (_read_half, mut write_half) = tokio::io::split(client_io);
    // 200k requests would be 3.8 MB of answers; the writer must get stuck.
    let stuck = tokio::time::timeout(Duration::from_millis(500), write_half.write_all(&bytes))
        .await
        .is_err();
    assert!(stuck, "the server kept reading without writing");
    server.abort();
}

#[tokio::test]
async fn a_busy_connection_does_not_starve_a_quiet_one() {
    // One connection pipelines a burst of work; another does a single round
    // trip, on the same single-threaded runtime.
    let busy = start("busy", Duration::from_secs(10)).await.unwrap();
    let quiet = start("quiet", Duration::from_secs(10)).await.unwrap();
    let flood = {
        let conn = busy.conn.clone();
        tokio::spawn(async move {
            let mut tasks = Vec::new();
            for i in 0..20_000u32 {
                let conn = conn.clone();
                tasks.push(tokio::spawn(async move {
                    conn.request(&Frame::GetHead(GetHead {
                        context: (i % 7) as u16,
                    }))
                    .await
                    .unwrap()
                }));
            }
            for t in tasks {
                t.await.unwrap();
            }
        })
    };
    tokio::time::sleep(Duration::from_millis(5)).await;
    let started = std::time::Instant::now();
    let answer = tokio::time::timeout(
        Duration::from_secs(2),
        quiet.conn.request(&Frame::GetHead(GetHead { context: 3 })),
    )
    .await
    .expect("the quiet connection was starved")
    .unwrap();
    assert_eq!(head(&answer), 3007);
    assert!(started.elapsed() < Duration::from_secs(2));
    flood.await.unwrap();
}

#[tokio::test]
async fn a_request_that_waits_does_not_hold_up_its_neighbours() {
    let h = start("ci", Duration::from_secs(10)).await.unwrap();
    // GetPositionAt takes 300 ms; requests sent behind it on the same
    // connection are answered long before.
    let slow_done = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let slow = {
        let (conn, done) = (h.conn.clone(), slow_done.clone());
        tokio::spawn(async move {
            conn.request(&Frame::GetPositionAt(GetPositionAt {
                context: 0,
                timestamp: 0,
            }))
            .await
            .unwrap();
            done.store(true, std::sync::atomic::Ordering::SeqCst);
        })
    };
    tokio::time::sleep(Duration::from_millis(5)).await;
    for i in 0..10u16 {
        h.conn
            .request(&Frame::GetHead(GetHead { context: i }))
            .await
            .unwrap();
        assert!(
            !slow_done.load(std::sync::atomic::Ordering::SeqCst),
            "a request waited for the slow one in front of it"
        );
    }
    slow.await.unwrap();
}

/// Real sockets: TLS, and requests that run in parallel.
mod sockets {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use tokio::net::TcpListener;
    use tokio_rustls::rustls::pki_types::{PrivateKeyDer, ServerName};
    use tokio_rustls::rustls::{self, ClientConfig, RootCertStore};
    use tokio_rustls::{TlsAcceptor, TlsConnector};

    use super::*;

    /// How many handlers run at once, and the most that did.
    static ACTIVE: AtomicUsize = AtomicUsize::new(0);
    static MOST: AtomicUsize = AtomicUsize::new(0);

    /// Answers WhoAmI at once; GetTags takes a while to start, without
    /// yielding, as a request that reads a lot does.
    struct Busy;

    impl Service for Busy {
        type Session = ();

        fn hello(
            &self,
            _: OwnedFrame,
            _: Arc<PeerInfo>,
        ) -> BoxFuture<Result<Accepted<()>, Failure>> {
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
                if stream.request.kind() == GetTags::KIND {
                    let now = ACTIVE.fetch_add(1, Ordering::SeqCst) + 1;
                    MOST.fetch_max(now, Ordering::SeqCst);
                    let until = std::time::Instant::now() + Duration::from_millis(3);
                    while std::time::Instant::now() < until {}
                    ACTIVE.fetch_sub(1, Ordering::SeqCst);
                }
                let _ = stream
                    .out
                    .finish(&Frame::Identity(Identity {
                        subject: "ok",
                        source: "",
                        grants: vec![],
                        authentication_enabled: false,
                    }))
                    .await;
            })
        }
    }

    fn tls_pair() -> (TlsAcceptor, ConnectOptions) {
        let key = rcgen::KeyPair::generate().unwrap();
        let cert = rcgen::CertificateParams::new(vec!["localhost".to_string()])
            .unwrap()
            .self_signed(&key)
            .unwrap();
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let server = rustls::ServerConfig::builder_with_provider(Arc::clone(&provider))
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_no_client_auth()
            .with_single_cert(
                vec![cert.der().clone()],
                PrivateKeyDer::Pkcs8(key.serialize_der().into()),
            )
            .unwrap();
        let mut roots = RootCertStore::empty();
        roots.add(cert.der().clone()).unwrap();
        let client = ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_root_certificates(roots)
            .with_no_client_auth();
        let options = ConnectOptions {
            tls: Some((
                TlsConnector::from(Arc::new(client)),
                ServerName::try_from("localhost").unwrap(),
            )),
            ..ConnectOptions::default()
        };
        (TlsAcceptor::from(Arc::new(server)), options)
    }

    /// A listener serving every connection with `server::connection`.
    async fn listen(tls: Option<TlsAcceptor>) -> (std::net::SocketAddr, watch::Sender<bool>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (shutdown, shutdown_rx) = watch::channel(false);
        let config = Arc::new(ServerConfig {
            tls,
            ..ServerConfig::default()
        });
        tokio::spawn(async move {
            loop {
                let Ok((tcp, peer)) = listener.accept().await else {
                    return;
                };
                let _ = tcp.set_nodelay(true);
                tokio::spawn(server::connection(
                    tcp,
                    peer,
                    Arc::new(Busy),
                    Arc::clone(&config),
                    shutdown_rx.clone(),
                ));
            }
        });
        (addr, shutdown)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 3)]
    async fn a_request_right_behind_a_cancel_is_answered_at_once() {
        // A reader takes part of a stream, drops it and asks its next
        // question at once, as a reader that wants the first N events does.
        // The Cancel lets the server catch up; the request that came with it
        // must not wait for the peer to send anything more.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (_shutdown, shutdown_rx) = watch::channel(false);
        tokio::spawn(async move {
            let (tcp, peer) = listener.accept().await.unwrap();
            let _ = tcp.set_nodelay(true);
            server::connection(
                tcp,
                peer,
                Arc::new(Toy {
                    flood: Arc::default(),
                }),
                Arc::new(ServerConfig::default()),
                shutdown_rx,
            )
            .await;
        });
        let options = ConnectOptions {
            credential: Credential::Token("ci".into()),
            ..ConnectOptions::default()
        };
        let conn = client::connect(&addr.to_string(), &options).await.unwrap();
        for _ in 0..50 {
            let mut stream = conn
                .open(&flood_source(100_000, 30_000), true)
                .await
                .unwrap();
            for _ in 0..300 {
                stream.next().await.unwrap().unwrap();
            }
            drop(stream);
            let started = std::time::Instant::now();
            let answer = conn
                .request(&Frame::GetHead(GetHead { context: 1 }))
                .await
                .unwrap();
            assert_eq!(head(&answer), 1007);
            assert!(
                started.elapsed() < Duration::from_millis(500),
                "answered after {:?}",
                started.elapsed()
            );
        }
    }

    async fn who(conn: &client::Connection) {
        let answer = conn.request(&Frame::WhoAmI(WhoAmI {})).await.unwrap();
        assert!(matches!(answer.decode().unwrap(), Frame::Identity(i) if i.subject == "ok"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 3)]
    async fn tls_connections_serve_concurrent_requests_and_refuse_plain_ones() {
        let (acceptor, options) = tls_pair();
        let (addr, _shutdown) = listen(Some(acceptor)).await;
        let mut tasks = Vec::new();
        for _ in 0..4 {
            let options = options.clone();
            tasks.push(tokio::spawn(async move {
                let conn = client::connect(&addr.to_string(), &options).await.unwrap();
                who(&conn).await;
                // Many requests at once over the one encrypted stream.
                let mut inner = Vec::new();
                for _ in 0..500 {
                    let conn = conn.clone();
                    inner.push(tokio::spawn(async move { who(&conn).await }));
                }
                for t in inner {
                    t.await.unwrap();
                }
            }));
        }
        for t in tasks {
            t.await.unwrap();
        }
        let plain = client::connect(&addr.to_string(), &ConnectOptions::default()).await;
        assert!(plain.is_err(), "a plain client got into a TLS listener");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 3)]
    async fn requests_that_are_dear_to_start_run_in_parallel_on_one_connection() {
        let (addr, _shutdown) = listen(None).await;
        let conn = client::connect(&addr.to_string(), &ConnectOptions::default())
            .await
            .unwrap();
        MOST.store(0, Ordering::SeqCst);
        let mut tasks = Vec::new();
        for _ in 0..24 {
            let conn = conn.clone();
            tasks.push(tokio::spawn(async move {
                conn.request(&Frame::GetTags(GetTags {
                    context: 0,
                    position: 0,
                }))
                .await
                .unwrap()
            }));
        }
        for t in tasks {
            t.await.unwrap();
        }
        // The first ones run on the connection's task; once it has seen how
        // long they take, the rest go to other workers.
        assert!(
            MOST.load(Ordering::SeqCst) >= 2,
            "the requests ran one after another"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 3)]
    async fn a_busy_connection_does_not_starve_one_that_is_not() {
        let (addr, _shutdown) = listen(None).await;
        let busy = client::connect(&addr.to_string(), &ConnectOptions::default())
            .await
            .unwrap();
        let quiet = client::connect(&addr.to_string(), &ConnectOptions::default())
            .await
            .unwrap();
        let flood = {
            let busy = busy.clone();
            tokio::spawn(async move {
                let mut tasks = Vec::new();
                for _ in 0..4 {
                    let busy = busy.clone();
                    tasks.push(tokio::spawn(async move {
                        for _ in 0..20_000 {
                            who(&busy).await;
                        }
                    }));
                }
                for t in tasks {
                    t.await.unwrap();
                }
            })
        };
        tokio::time::sleep(Duration::from_millis(20)).await;
        for _ in 0..50 {
            tokio::time::timeout(Duration::from_secs(2), who(&quiet))
                .await
                .expect("the quiet connection was starved");
        }
        flood.await.unwrap();
    }
}
