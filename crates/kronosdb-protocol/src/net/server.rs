//! The server half of the connection runtime.
//!
//! [`serve`] accepts connections and runs each one as a single task: the
//! preface and Hello, then a loop that reads frames, routes them to streams
//! by the rules in `rules`, and writes the answers, coalesced. A new request
//! is handed to the [`Service`]; its future is polled once on the connection's
//! task, so a request answered without waiting costs no task switch and no
//! wake-up, and is spawned only if it has to wait.

use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

use bytes::Bytes;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, watch};
use tokio_rustls::TlsAcceptor;
use tokio_rustls::rustls::pki_types::CertificateDer;

use super::outbox::{OutTx, Outbox, WriteBuf};
use super::rules::{self, LivenessCheck, ServerRoute};
use super::{
    DEFAULT_MAX_FRAME, Failure, FrameReader, NetError, Out, OwnedFrame, encode, encode_sized,
    shared_header,
};
use crate::codec::{FLAG_END, PREFACE};
use crate::generated::{
    ErrorCode, Frame, GoAway, Hello, PROTOCOL_VERSION, Pong, ServerPing, Welcome,
};

pub type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send + 'static>>;

/// What the application decides about connections and requests.
pub trait Service: Send + Sync + 'static {
    /// Per-connection state: the principal, bound contexts.
    type Session: Send + Sync + 'static;

    /// Accepts or refuses a connection from its Hello.
    fn hello(
        &self,
        hello: OwnedFrame,
        peer: Arc<PeerInfo>,
    ) -> BoxFuture<Result<Accepted<Self::Session>, Failure>>;

    /// Serves one request. The future must answer through `stream.out`; if it
    /// ends without a final frame the runtime answers for it.
    fn request(&self, session: &Arc<Self::Session>, stream: ServerStream) -> BoxFuture<()>;
}

/// A connection the service accepted.
pub struct Accepted<S> {
    pub session: Arc<S>,
    /// Shown in Welcome.
    pub principal: String,
    /// Granted feature bits.
    pub features: u64,
    /// A frame size limit for this connection other than the server's, in
    /// both directions. Peers replicating a log trade frames far larger than
    /// a client's, and only a verified peer should be allowed to.
    pub max_frame: Option<u32>,
}

/// Who is on the other end, as far as the transport knows.
#[derive(Debug, Clone)]
pub struct PeerInfo {
    pub addr: SocketAddr,
    /// The TLS client certificate chain, leaf first, when one was presented.
    pub certificates: Vec<CertificateDer<'static>>,
}

#[derive(Clone)]
pub struct ServerConfig {
    pub server_name: String,
    pub max_frame: u32,
    /// Ping a connection after this long without traffic; close it after
    /// two pings go unanswered.
    pub heartbeat: Duration,
    /// How long a client has to send its preface and Hello.
    pub handshake_timeout: Duration,
    pub tls: Option<TlsAcceptor>,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            server_name: String::new(),
            max_frame: DEFAULT_MAX_FRAME,
            heartbeat: Duration::from_secs(10),
            handshake_timeout: Duration::from_secs(10),
            tls: None,
        }
    }
}

/// Answers one request. Cloneable, so a request can answer from several tasks.
#[derive(Clone)]
pub struct StreamSender {
    id: u32,
    tx: OutTx,
    ended: Arc<AtomicBool>,
}

impl StreamSender {
    pub fn id(&self) -> u32 {
        self.id
    }

    async fn push(&self, out: Out, end: bool) -> Result<(), NetError> {
        if end {
            if self.ended.swap(true, Ordering::AcqRel) {
                return Err(NetError::Closed);
            }
        } else if self.ended.load(Ordering::Acquire) {
            return Err(NetError::Closed);
        }
        self.tx.push(out).await
    }

    /// Sends a frame; more follow.
    pub async fn send(&self, frame: &Frame<'_>) -> Result<(), NetError> {
        let bytes = encode(self.id, 0, frame)?;
        self.push(Out::Frame(bytes), false).await
    }

    /// Sends a frame that is about `size` bytes long, such as one carrying a
    /// large payload: it is encoded into a buffer of that size, copied once.
    pub async fn send_sized(&self, size: usize, frame: &Frame<'_>) -> Result<(), NetError> {
        let bytes = encode_sized(self.id, 0, frame, size)?;
        self.push(Out::Frame(bytes), false).await
    }

    /// Sends the request's last frame.
    pub async fn finish(&self, frame: &Frame<'_>) -> Result<(), NetError> {
        let bytes = encode(self.id, FLAG_END, frame)?;
        self.push(Out::Frame(bytes), true).await
    }

    /// Ends the request with a failure.
    pub async fn fail(&self, failure: &Failure) -> Result<(), NetError> {
        self.finish(&Frame::RequestFailed(failure.to_frame())).await
    }

    /// Sends a frame whose body is already encoded and may be shared with
    /// other streams, so a batch sent to many subscribers is encoded once.
    pub async fn send_shared(&self, kind: u16, end: bool, body: Bytes) -> Result<(), NetError> {
        let flags = if end { FLAG_END } else { 0 };
        let header = shared_header(self.id, kind, flags, body.len());
        self.push(Out::Shared { header, body }, end).await
    }

    pub fn has_finished(&self) -> bool {
        self.ended.load(Ordering::Acquire)
    }
}

/// What the client sends on a request after its first frame.
pub struct Inbound {
    rx: Option<mpsc::Receiver<OwnedFrame>>,
    cancel: watch::Receiver<bool>,
}

impl Inbound {
    /// The next frame. `None` once the client has sent its last frame,
    /// cancelled, or gone away.
    pub async fn next(&mut self) -> Option<OwnedFrame> {
        let rx = self.rx.as_mut()?;
        tokio::select! {
            biased;
            _ = wait_cancelled(&mut self.cancel) => None,
            frame = rx.recv() => frame,
        }
    }

    /// Resolves once the client cancels the request or disconnects.
    pub async fn cancelled(&mut self) {
        wait_cancelled(&mut self.cancel).await
    }

    pub fn is_cancelled(&self) -> bool {
        *self.cancel.borrow() || self.cancel.has_changed().is_err()
    }
}

/// Resolves once `shutdown` turns true (or its sender is gone).
async fn stopping(shutdown: &mut watch::Receiver<bool>) {
    let _ = shutdown.wait_for(|stop| *stop).await;
}

async fn wait_cancelled(cancel: &mut watch::Receiver<bool>) {
    // A dropped sender is a gone connection: cancelled as well.
    let _ = cancel.wait_for(|cancelled| *cancelled).await;
}

/// One request: its first frame, what follows it, and where to answer.
pub struct ServerStream {
    pub request: OwnedFrame,
    pub inbound: Inbound,
    pub out: StreamSender,
}

/// How a request's handler is doing, for the connection and the handler.
#[derive(Default)]
struct Flags {
    /// The handler ended; the stream is closed whatever the table says.
    done: AtomicBool,
    /// The handler runs on its own task, so the connection has to be told
    /// when it ends; one that ended inline is already known to have.
    spawned: AtomicBool,
}

/// What the connection keeps per open stream.
struct Entry {
    inbound: Option<mpsc::Sender<OwnedFrame>>,
    cancel: watch::Sender<bool>,
    flags: Arc<Flags>,
}

/// A frame for a stream whose inbound queue was full: reading waits for room.
struct Stalled {
    id: u32,
    tx: mpsc::Sender<OwnedFrame>,
    frame: OwnedFrame,
}

/// How a connection's requests fare on their first poll, on average: how
/// long it takes, and how often it ends the request. Answering a queued
/// request on the spot only pays when both go well. One that has to wait (a
/// command waiting for its handler) is spawned anyway, so polling it first
/// gains nothing and puts the work of every queued request on this one task,
/// one after another, where spawned they would spread over the workers.
struct Cost {
    micros: f32,
    ends: f32,
}

impl Cost {
    /// Requests whose first poll takes longer than this are run apart from
    /// the connection when others are queued behind them.
    const LIGHT: f32 = 20.0;
    /// Nor are queued requests answered on the spot when fewer than this
    /// share of first polls end the request.
    const ENDING: f32 = 0.5;

    fn new() -> Cost {
        Cost {
            micros: 0.0,
            ends: 1.0,
        }
    }

    fn light(&self) -> bool {
        self.micros < Self::LIGHT && self.ends > Self::ENDING
    }

    fn record(&mut self, took: Duration, ended: bool) {
        self.micros += (took.as_secs_f32() * 1e6 - self.micros) * 0.125;
        self.ends += (if ended { 1.0 } else { 0.0 } - self.ends) * 0.125;
    }
}

const INBOUND_CAPACITY: usize = 64;
/// Frames routed per turn of the connection loop before it writes and lets
/// other tasks run.
const FRAMES_PER_TURN: usize = 128;
/// Time spent answering requests on the spot in one turn, at most, before
/// the connection writes and lets other tasks run.
const TURN_BUDGET: Duration = Duration::from_micros(500);
/// More queued replies are moved into the write buffer only below this.
const WRITE_TARGET: usize = 256 * 1024;
/// Reading pauses while this much is waiting to be written, so a peer that
/// does not read cannot make the server buffer without bound.
const READ_PAUSE: usize = 1024 * 1024;
/// How long a closing connection may take to flush its last replies.
const CLOSE_TIMEOUT: Duration = Duration::from_secs(30);

/// Whether `prefix`, the first bytes a client sent, open a native connection.
/// `None` until enough bytes arrived to tell.
pub fn is_native_preface(prefix: &[u8]) -> Option<bool> {
    let n = prefix.len().min(PREFACE.len());
    if prefix[..n] != PREFACE[..n] {
        Some(false)
    } else if n == PREFACE.len() {
        Some(true)
    } else {
        None
    }
}

/// Accepts connections until `shutdown` turns true, then asks each open
/// connection to drain.
pub async fn serve<S: Service>(
    listener: TcpListener,
    service: Arc<S>,
    config: Arc<ServerConfig>,
    mut shutdown: watch::Receiver<bool>,
) {
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let Ok((tcp, addr)) = accepted else { continue };
                let _ = tcp.set_nodelay(true);
                tokio::spawn(connection(tcp, addr, Arc::clone(&service), Arc::clone(&config), shutdown.clone()));
            }
            _ = stopping(&mut shutdown) => return,
        }
    }
}

/// Serves one accepted connection: TLS if configured, then the loop.
pub async fn connection<S: Service>(
    mut tcp: TcpStream,
    addr: SocketAddr,
    service: Arc<S>,
    config: Arc<ServerConfig>,
    shutdown: watch::Receiver<bool>,
) {
    let result = match config.tls.clone() {
        Some(acceptor) => {
            match tokio::time::timeout(config.handshake_timeout, acceptor.accept(tcp)).await {
                Ok(Ok(tls)) => {
                    let certificates = tls
                        .get_ref()
                        .1
                        .peer_certificates()
                        .map(|chain| chain.iter().map(|c| c.clone().into_owned()).collect())
                        .unwrap_or_default();
                    let peer = Arc::new(PeerInfo { addr, certificates });
                    run(tls, peer, service, config, shutdown).await
                }
                Ok(Err(e)) => Err(NetError::Tls(e.to_string())),
                Err(_) => Err(NetError::Timeout),
            }
        }
        None => {
            let peer = Arc::new(PeerInfo {
                addr,
                certificates: Vec::new(),
            });
            // Borrowed halves of the socket: no lock between them.
            let (r, w) = tcp.split();
            run_halves(r, w, peer, service, config, shutdown).await
        }
    };
    if let Err(error) = result {
        tracing::debug!(%addr, %error, "connection closed");
    }
}

/// Starts a request's handler and says whether it already ended. It is
/// polled once on the connection's task when nothing is queued behind it, or
/// when requests are cheap to start (see [`Cost`]), so a request answered
/// without waiting costs no task switch and no wake-up. One that has to wait
/// is spawned. One that is dear to start, with others queued behind it, is
/// spawned straight away, so concurrent requests on one connection run in
/// parallel instead of one after another on the connection's task.
fn start(fut: BoxFuture<()>, flags: &Flags, inline: bool) -> bool {
    let mut fut = fut;
    if inline {
        let mut cx = Context::from_waker(Waker::noop());
        if fut.as_mut().poll(&mut cx) == Poll::Ready(()) {
            return true;
        }
    }
    flags.spawned.store(true, Ordering::Release);
    // The spawned task polls it (again) with a real waker.
    tokio::spawn(fut);
    false
}

/// Queues a frame the connection itself sends. They are all short and fixed,
/// so encoding cannot fail; if it somehow did there would be nobody to tell.
fn control(outbox: &Outbox, stream: u32, flags: u8, frame: &Frame<'_>) {
    if let Ok(bytes) = encode(stream, flags, frame) {
        outbox.push_now(Out::Frame(bytes));
    }
}

fn go_away(outbox: &Outbox) {
    control(
        outbox,
        0,
        0,
        &Frame::GoAway(GoAway {
            code: ErrorCode::UNAVAILABLE,
            message: "the server is shutting down; reconnect to another node",
        }),
    );
}

/// Runs one connection to the end. Generic over the transport so TLS and
/// plain TCP share it, and tests can use in-memory pipes.
pub async fn run<S, IO>(
    io: IO,
    peer: Arc<PeerInfo>,
    service: Arc<S>,
    config: Arc<ServerConfig>,
    shutdown: watch::Receiver<bool>,
) -> Result<(), NetError>
where
    S: Service,
    IO: AsyncRead + AsyncWrite + Send + 'static,
{
    let (r, w) = tokio::io::split(io);
    run_halves(r, w, peer, service, config, shutdown).await
}

/// The connection's task: it reads and routes frames, runs the handlers that
/// can answer at once, and writes everything the handlers queued, in one
/// write per burst. Reading pauses while the peer is behind on reading
/// replies; writing never waits on reading.
async fn run_halves<S, R, W>(
    r: R,
    mut w: W,
    peer: Arc<PeerInfo>,
    service: Arc<S>,
    config: Arc<ServerConfig>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), NetError>
where
    S: Service,
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let outbox = Outbox::new();
    let mut wbuf = WriteBuf::new();
    let mut taken: VecDeque<Out> = VecDeque::new();
    let mut reader = FrameReader::new(r, config.max_frame);

    let handshake = handshake(&mut reader, &outbox, &service, peer, &config);
    let session = match tokio::time::timeout(config.handshake_timeout, handshake).await {
        Ok(Ok(accepted)) => accepted.session,
        Ok(Err(e)) => {
            // The refusal is queued: let it out before closing.
            let farewell = farewell(&outbox, &mut wbuf, &mut taken, &mut w);
            let _ = tokio::time::timeout(config.handshake_timeout, farewell).await;
            return Err(e);
        }
        Err(_) => return Err(NetError::Timeout),
    };

    let mut finished: Vec<u32> = Vec::new();
    let mut streams: HashMap<u32, Entry> = HashMap::new();
    let mut stalled: Option<Stalled> = None;
    let mut liveness = rules::Liveness::new(config.heartbeat, 2, Instant::now());
    let mut tick = tokio::time::interval(config.heartbeat.max(Duration::from_millis(10)) / 2);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut draining = *shutdown.borrow();
    let mut write_failed = false;
    let mut cost = Cost::new();

    let outcome: Result<(), NetError> = 'conn: loop {
        // Noticed here as well as in the wait below: a peer that never stops
        // sending would otherwise keep the connection from ever waiting.
        if !draining && *shutdown.borrow() {
            draining = true;
            go_away(&outbox);
        }
        // Replies still waiting from an earlier turn go out before the next
        // batch of requests is started: they have waited long enough.
        if wbuf.has_pending()
            && let Err(e) = flush_now(&mut wbuf, &mut w).await
        {
            write_failed = true;
            break Err(e.into());
        }
        // Everything already buffered first: one read often holds many frames.
        let mut over_budget = false;
        if stalled.is_none() {
            let mut budget = FRAMES_PER_TURN;
            let mut traffic = false;
            let mut spent = Duration::ZERO;
            loop {
                if budget == 0 || spent > TURN_BUDGET {
                    over_budget = reader.has_frame();
                    break;
                }
                let frame = match reader.buffered() {
                    Ok(Some(frame)) => frame,
                    Ok(None) => break,
                    Err(e) => break 'conn Err(e),
                };
                budget -= 1;
                if !traffic {
                    liveness.traffic(Instant::now());
                    traffic = true;
                }
                let id = frame.stream();
                // A stream whose handler already finished is closed, whatever the table says.
                let open = streams
                    .get(&id)
                    .is_some_and(|e| !e.flags.done.load(Ordering::Acquire));
                match rules::route_from_client(frame.kind(), id, open) {
                    ServerRoute::Pong => {
                        let nonce = match frame.decode() {
                            Ok(Frame::Ping(p)) => p.nonce,
                            _ => {
                                break 'conn Err(NetError::Unexpected {
                                    kind: frame.kind(),
                                    context: "as a ping",
                                });
                            }
                        };
                        control(&outbox, 0, 0, &Frame::Pong(Pong { nonce }));
                    }
                    ServerRoute::PongReceived | ServerRoute::Drop => {}
                    ServerRoute::Cancel => {
                        if let Some(entry) = streams.get_mut(&id) {
                            entry.inbound = None;
                            let _ = entry.cancel.send(true);
                        }
                    }
                    ServerRoute::Deliver => {
                        let end = frame.is_end();
                        let entry = streams.get_mut(&id).expect("routed as open");
                        if let Some(inbound) = &entry.inbound {
                            match inbound.try_send(frame) {
                                Ok(()) => {}
                                Err(mpsc::error::TrySendError::Closed(_)) => entry.inbound = None,
                                Err(mpsc::error::TrySendError::Full(frame)) => {
                                    // Reading waits until the handler takes
                                    // some; writing carries on meanwhile.
                                    stalled = Some(Stalled {
                                        id,
                                        tx: inbound.clone(),
                                        frame,
                                    });
                                    break;
                                }
                            }
                        }
                        if end {
                            entry.inbound = None;
                        }
                    }
                    ServerRoute::Open => {
                        if draining {
                            let failure = Failure::new(
                                ErrorCode::UNAVAILABLE,
                                "the server is shutting down; retry on another node",
                            );
                            control(
                                &outbox,
                                id,
                                FLAG_END,
                                &Frame::RequestFailed(failure.to_frame()),
                            );
                            continue;
                        }
                        let (inbound_tx, inbound_rx) = if frame.is_end() {
                            (None, None)
                        } else {
                            let (t, r) = mpsc::channel(INBOUND_CAPACITY);
                            (Some(t), Some(r))
                        };
                        let (cancel_tx, cancel_rx) = watch::channel(false);
                        let mut cancelled = cancel_tx.subscribe();
                        let flags = Arc::new(Flags::default());
                        let out = StreamSender {
                            id,
                            tx: outbox.sender(),
                            ended: Arc::new(AtomicBool::new(false)),
                        };
                        let stream = ServerStream {
                            request: frame,
                            inbound: Inbound {
                                rx: inbound_rx,
                                cancel: cancel_rx,
                            },
                            out: out.clone(),
                        };
                        let handler = service.request(&session, stream);
                        // The last request read is answered on the spot; so
                        // are the ones queued behind others while requests
                        // are cheap and end there. The rest run apart, in
                        // parallel.
                        let inline = !reader.has_frame() || cost.light();
                        let started = inline.then(Instant::now);
                        let handler_flags = Arc::clone(&flags);
                        let completed = start(
                            Box::pin(async move {
                                handler.await;
                                if !out.has_finished() {
                                    let was_cancelled = *cancelled.borrow_and_update()
                                        || cancelled.has_changed().is_err();
                                    let (code, message) = rules::unanswered_code(was_cancelled);
                                    let _ = out.fail(&Failure::new(code, message)).await;
                                }
                                handler_flags.done.store(true, Ordering::Release);
                                if handler_flags.spawned.load(Ordering::Acquire) {
                                    out.tx.finished(id);
                                }
                            }),
                            &flags,
                            inline,
                        );
                        if let Some(started) = started {
                            let took = started.elapsed();
                            cost.record(took, completed);
                            spent += took;
                        }
                        // One that ended inline never needs a table entry.
                        if !completed {
                            streams.insert(
                                id,
                                Entry {
                                    inbound: inbound_tx,
                                    cancel: cancel_tx,
                                    flags,
                                },
                            );
                        }
                    }
                    ServerRoute::Violation(why) => {
                        let failure = Failure::new(ErrorCode::PROTOCOL_VIOLATION, why);
                        control(
                            &outbox,
                            0,
                            FLAG_END,
                            &Frame::RequestFailed(failure.to_frame()),
                        );
                        break 'conn Err(NetError::Unexpected {
                            kind: frame.kind(),
                            context: why,
                        });
                    }
                }
            }
        }

        // What the handlers queued, and which of them ended.
        outbox.take(&mut taken, &mut finished);
        for id in finished.drain(..) {
            if streams
                .get(&id)
                .is_some_and(|e| e.flags.done.load(Ordering::Acquire))
            {
                streams.remove(&id);
            }
        }
        while wbuf.pending() < WRITE_TARGET {
            match taken.pop_front() {
                Some(out) => wbuf.add(out),
                None => break,
            }
        }
        if draining && streams.is_empty() && stalled.is_none() {
            break Ok(());
        }

        // Answers go out before the loop waits, whichever arm wakes it next:
        // a reply is never held back behind reading more requests.
        if wbuf.has_pending()
            && let Err(e) = flush_now(&mut wbuf, &mut w).await
        {
            write_failed = true;
            break Err(e.into());
        }
        if over_budget {
            // A deep pipeline: let other connections run, then carry on.
            tokio::task::yield_now().await;
            continue;
        }
        if !wbuf.has_pending() && !taken.is_empty() {
            // Written out, and more replies are waiting to be moved in.
            continue;
        }

        let can_read = stalled.is_none() && taken.is_empty() && wbuf.pending() < READ_PAUSE;
        // Not `biased`. Replies are already flushed above, so the write arm
        // is ready only while the socket is full. With it listed first, a
        // stream whose handler keeps the queue full and a peer that keeps
        // taking its frames leave it ready every turn, and the reads (a
        // Cancel, credits, the next request) wait for the stream to end. A
        // random start gives each arm its turn.
        tokio::select! {
            written = std::future::poll_fn(|cx| wbuf.poll_write(cx, &mut w)), if wbuf.has_pending() => {
                if let Err(e) = written {
                    write_failed = true;
                    break Err(e.into());
                }
            }
            // Handlers answered from elsewhere, or ended.
            _ = std::future::poll_fn(|cx| outbox.poll_ready(cx, false)), if taken.is_empty() => {}
            room = async { stalled.as_ref().expect("guarded").tx.clone().reserve_owned().await }, if stalled.is_some() => {
                let Stalled { id, frame, .. } = stalled.take().expect("guarded");
                let end = frame.is_end();
                if let Ok(permit) = room {
                    permit.send(frame);
                }
                if end && let Some(entry) = streams.get_mut(&id) {
                    entry.inbound = None;
                }
            }
            more = reader.fill(), if can_read => match more {
                Ok(true) => {}
                Ok(false) => break Ok(()),
                Err(e) => break Err(e),
            },
            _ = tick.tick() => match liveness.check(Instant::now()) {
                LivenessCheck::Fine => {}
                LivenessCheck::Ping(nonce) => {
                    control(&outbox, 0, 0, &Frame::ServerPing(ServerPing { nonce }));
                }
                LivenessCheck::Dead => break Err(NetError::Timeout),
            },
            _ = stopping(&mut shutdown), if !draining => {
                draining = true;
                go_away(&outbox);
            }
        }
    };

    // Dropping the table cancels every open request; once their handlers
    // let go of their senders, what is queued is flushed and the connection
    // closes.
    drop(streams);
    drop(stalled);
    if !write_failed {
        let farewell = farewell(&outbox, &mut wbuf, &mut taken, &mut w);
        let _ = tokio::time::timeout(CLOSE_TIMEOUT, farewell).await;
    }
    outbox.close();
    outcome
}

/// Writes what the socket takes without waiting; what it does not take stays
/// queued for the write arm of the loop to finish.
async fn flush_now<W: AsyncWrite + Unpin>(wbuf: &mut WriteBuf, w: &mut W) -> std::io::Result<()> {
    std::future::poll_fn(|cx| match wbuf.poll_write(cx, w) {
        Poll::Ready(result) => Poll::Ready(result),
        Poll::Pending => Poll::Ready(Ok(())),
    })
    .await
}

/// Writes out everything that is queued or still to be queued, until the last
/// handler has let go of its sender, then closes the write side.
async fn farewell<W: AsyncWrite + Unpin>(
    outbox: &Outbox,
    wbuf: &mut WriteBuf,
    taken: &mut VecDeque<Out>,
    w: &mut W,
) -> std::io::Result<()> {
    let mut finished = Vec::new();
    loop {
        outbox.take(taken, &mut finished);
        finished.clear();
        while wbuf.pending() < WRITE_TARGET {
            match taken.pop_front() {
                Some(out) => wbuf.add(out),
                None => break,
            }
        }
        if wbuf.has_pending() {
            std::future::poll_fn(|cx| wbuf.poll_write(cx, w)).await?;
        } else if taken.is_empty() {
            if outbox.is_drained() {
                break;
            }
            std::future::poll_fn(|cx| outbox.poll_ready(cx, true)).await;
        }
    }
    w.shutdown().await
}

async fn handshake<S: Service, R: AsyncRead + Unpin>(
    reader: &mut FrameReader<R>,
    outbox: &Outbox,
    service: &Arc<S>,
    peer: Arc<PeerInfo>,
    config: &ServerConfig,
) -> Result<Accepted<S::Session>, NetError> {
    let preface = reader.read_exact(PREFACE.len()).await?;
    if preface.as_ref() != PREFACE {
        return Err(NetError::Unexpected {
            kind: 0,
            context: "instead of the KRONOSDB preface",
        });
    }
    let hello = reader.next().await?.ok_or(NetError::Closed)?;
    if hello.kind() != Hello::KIND || hello.stream() != 0 {
        let failure = Failure::new(
            ErrorCode::PROTOCOL_VIOLATION,
            "the first frame must be Hello on stream 0",
        );
        control(
            outbox,
            0,
            FLAG_END,
            &Frame::RequestFailed(failure.to_frame()),
        );
        return Err(NetError::Unexpected {
            kind: hello.kind(),
            context: "before Hello",
        });
    }
    let (version, record_format) = match hello.decode()? {
        Frame::Hello(h) => (h.version, h.record_format),
        _ => unreachable!("checked the kind"),
    };
    let refuse = |failure: Failure| {
        control(
            outbox,
            0,
            FLAG_END,
            &Frame::RequestFailed(failure.to_frame()),
        );
        Err::<Accepted<S::Session>, NetError>(NetError::Failed(failure))
    };
    if version < PROTOCOL_VERSION {
        return refuse(Failure::new(
            ErrorCode::REJECTED,
            format!(
                "protocol version {version} is not supported; this server speaks {PROTOCOL_VERSION}"
            ),
        ));
    }
    if record_format != RECORD_FORMAT {
        return refuse(Failure::new(
            ErrorCode::REJECTED,
            format!(
                "record format {record_format} is not supported; this server stores format {RECORD_FORMAT}"
            ),
        ));
    }
    let accepted = match service.hello(hello, peer).await {
        Ok(accepted) => accepted,
        Err(failure) => return refuse(failure),
    };
    let max_frame = accepted.max_frame.unwrap_or(config.max_frame);
    reader.set_max_frame(max_frame);
    let welcome = Frame::Welcome(Welcome {
        version: PROTOCOL_VERSION,
        features: accepted.features,
        record_format: RECORD_FORMAT,
        server_name: &config.server_name,
        principal: &accepted.principal,
        max_frame,
        heartbeat: u32::try_from(config.heartbeat.as_millis()).unwrap_or(u32::MAX),
    });
    outbox.push_now(Out::Frame(encode(0, 0, &welcome)?));
    Ok(accepted)
}

/// The stored record layout this build reads and writes.
pub const RECORD_FORMAT: u8 = 3;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn requests_are_cheap_until_they_are_seen_to_be_dear() {
        let mut cost = Cost::new();
        assert!(cost.light());
        for _ in 0..100 {
            cost.record(Duration::from_micros(2), true);
        }
        assert!(cost.light());
        // A few dear ones tip it, and cheap ones bring it back.
        for _ in 0..4 {
            cost.record(Duration::from_micros(200), true);
        }
        assert!(!cost.light());
        for _ in 0..60 {
            cost.record(Duration::from_micros(2), true);
        }
        assert!(cost.light());
    }

    #[test]
    fn requests_that_wait_are_not_answered_on_the_spot() {
        // Commands: a quick first poll that routes them, then a wait for the
        // handler. Polling the queued ones first gains nothing.
        let mut cost = Cost::new();
        for _ in 0..10 {
            cost.record(Duration::from_micros(3), false);
        }
        assert!(!cost.light());
        for _ in 0..10 {
            cost.record(Duration::from_micros(3), true);
        }
        assert!(cost.light());
    }
}
