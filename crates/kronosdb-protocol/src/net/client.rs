//! The client half of the connection runtime: one connection, any number of
//! concurrent requests on it.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

use bytes::Bytes;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot, watch};
use tokio_rustls::TlsConnector;
use tokio_rustls::rustls::pki_types::ServerName;

use super::rules::{self, ClientRoute, LivenessCheck, Reply};
use super::server::RECORD_FORMAT;
use super::{
    DEFAULT_MAX_FRAME, Failure, FrameReader, NetError, Out, OwnedFrame, encode, write_loop,
};
use crate::codec::{FLAG_END, PREFACE};
use crate::generated::{
    Cancel, ClientPong, CredentialKind, Frame, Hello, PROTOCOL_VERSION, Welcome, Window,
};

pub use super::rules::DEFAULT_WINDOW;

/// How a client proves who it is.
#[derive(Debug, Clone)]
pub enum Credential {
    /// None, or the TLS client certificate.
    None,
    Token(String),
    Jwt(String),
}

#[derive(Clone)]
pub struct ConnectOptions {
    pub client_name: String,
    pub client_id: String,
    pub credential: Credential,
    pub tls: Option<(TlsConnector, ServerName<'static>)>,
    pub connect_timeout: Duration,
    /// Bytes of frames the server may send on each stream ahead of its
    /// consumer. A consumer that falls behind holds its own stream at most
    /// this far ahead, and no other. Bigger fills a link that is further
    /// away; the server never sends more than this on a stream nobody reads.
    pub window: u32,
}

impl Default for ConnectOptions {
    fn default() -> Self {
        Self {
            client_name: String::new(),
            client_id: String::new(),
            credential: Credential::None,
            tls: None,
            connect_timeout: Duration::from_secs(10),
            window: DEFAULT_WINDOW,
        }
    }
}

/// What the server said in Welcome.
#[derive(Debug, Clone)]
pub struct Welcomed {
    pub version: u16,
    pub features: u64,
    pub server_name: String,
    pub principal: String,
    pub max_frame: u32,
    pub heartbeat: Duration,
}

enum Slot {
    Unary(oneshot::Sender<Result<OwnedFrame, NetError>>),
    Stream(Sink),
}

/// The reader's end of a stream's queue. Unbounded, so handing a frame over
/// never waits: the stream's window bounds what the server sends into it.
struct Sink {
    tx: mpsc::UnboundedSender<OwnedFrame>,
}

struct Shared {
    out: mpsc::Sender<Out>,
    slots: Mutex<HashMap<u32, Slot>>,
    last_id: AtomicU32,
    closed: watch::Receiver<bool>,
    draining: AtomicBool,
    window: u32,
    welcome: Welcomed,
}

impl Shared {
    fn allocate(&self, slot: Slot) -> Result<u32, NetError> {
        if *self.closed.borrow() {
            return Err(NetError::Closed);
        }
        let mut slots = self.slots.lock().unwrap();
        let mut id = rules::next_stream_id(self.last_id.load(Ordering::Relaxed));
        while slots.contains_key(&id) {
            id = rules::next_stream_id(id);
        }
        self.last_id.store(id, Ordering::Relaxed);
        slots.insert(id, slot);
        Ok(id)
    }

    fn release(&self, id: u32) {
        self.slots.lock().unwrap().remove(&id);
    }

    async fn send(&self, stream: u32, end: bool, frame: &Frame<'_>) -> Result<(), NetError> {
        let bytes = encode(stream, if end { FLAG_END } else { 0 }, frame)?;
        self.out
            .send(Out::Frame(bytes))
            .await
            .map_err(|_| NetError::Closed)
    }
}

/// One connection to a server. Cheap to clone; every clone shares it.
#[derive(Clone)]
pub struct Connection {
    shared: Arc<Shared>,
}

/// Opens a connection to `addr` (`host:port`) and performs the handshake.
pub async fn connect(addr: &str, options: &ConnectOptions) -> Result<Connection, NetError> {
    let tcp = tokio::time::timeout(options.connect_timeout, TcpStream::connect(addr))
        .await
        .map_err(|_| NetError::Timeout)??;
    tcp.set_nodelay(true)?;
    match &options.tls {
        Some((connector, name)) => {
            let tls = tokio::time::timeout(
                options.connect_timeout,
                connector.connect(name.clone(), tcp),
            )
            .await
            .map_err(|_| NetError::Timeout)?
            .map_err(|e| NetError::Tls(e.to_string()))?;
            handshake(tls, options).await
        }
        None => handshake(tcp, options).await,
    }
}

/// Performs the handshake over an established transport and starts the
/// connection's reader and writer.
pub async fn handshake<IO>(io: IO, options: &ConnectOptions) -> Result<Connection, NetError>
where
    IO: AsyncRead + AsyncWrite + Send + 'static,
{
    let (r, w) = tokio::io::split(io);
    let (tx, rx) = mpsc::channel::<Out>(1024);
    tokio::spawn(write_loop(w, rx));
    let mut reader = FrameReader::new(r, DEFAULT_MAX_FRAME);

    let (kind, credential) = match &options.credential {
        Credential::None => (CredentialKind::NONE, ""),
        Credential::Token(t) => (CredentialKind::TOKEN, t.as_str()),
        Credential::Jwt(t) => (CredentialKind::JWT, t.as_str()),
    };
    let mut first = PREFACE.to_vec();
    first.extend_from_slice(&encode(
        0,
        0,
        &Frame::Hello(Hello {
            version: PROTOCOL_VERSION,
            features: 0,
            record_format: RECORD_FORMAT,
            client_name: &options.client_name,
            client_id: &options.client_id,
            credential_kind: kind,
            credential,
            window: options.window,
        }),
    )?);
    tx.send(Out::Frame(Bytes::from(first)))
        .await
        .map_err(|_| NetError::Closed)?;

    let reply = tokio::time::timeout(options.connect_timeout, reader.next())
        .await
        .map_err(|_| NetError::Timeout)??
        .ok_or(NetError::Closed)?;
    let welcome = match reply.decode()? {
        Frame::Welcome(w) => welcomed(&w),
        Frame::RequestFailed(f) => return Err(NetError::Failed(Failure::new(f.code, f.message))),
        _ => {
            return Err(NetError::Unexpected {
                kind: reply.kind(),
                context: "instead of Welcome",
            });
        }
    };
    reader.set_max_frame(welcome.max_frame);

    let (closed_tx, closed_rx) = watch::channel(false);
    let shared = Arc::new(Shared {
        out: tx,
        slots: Mutex::new(HashMap::new()),
        last_id: AtomicU32::new(0),
        closed: closed_rx,
        draining: AtomicBool::new(false),
        window: options.window,
        welcome,
    });
    // The reader holds the connection weakly: once every `Connection` and
    // open stream is gone nothing can send, the writer finishes and closes
    // the socket, and the server's end of it ends the reader in turn.
    tokio::spawn(read_loop(
        reader,
        Arc::downgrade(&shared),
        shared.welcome.heartbeat,
        closed_tx,
    ));
    Ok(Connection { shared })
}

fn welcomed(w: &Welcome<'_>) -> Welcomed {
    Welcomed {
        version: w.version,
        features: w.features,
        server_name: w.server_name.to_string(),
        principal: w.principal.to_string(),
        max_frame: w.max_frame,
        heartbeat: Duration::from_millis(w.heartbeat as u64),
    }
}

async fn read_loop<R: AsyncRead + Unpin>(
    mut reader: FrameReader<R>,
    shared: Weak<Shared>,
    heartbeat: Duration,
    closed: watch::Sender<bool>,
) {
    // The server pings a quiet connection every heartbeat; nothing at all
    // for a few of those means it is gone.
    let interval = heartbeat.max(Duration::from_secs(1));
    let mut liveness = rules::Liveness::new(interval * 3, 0, Instant::now());
    let mut tick = tokio::time::interval(interval);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let reason = loop {
        let frame = tokio::select! {
            frame = reader.next() => frame,
            _ = tick.tick() => match liveness.check(Instant::now()) {
                LivenessCheck::Dead => break NetError::Timeout,
                _ => continue,
            },
        };
        let frame = match frame {
            Ok(Some(frame)) => frame,
            Ok(None) => break NetError::Closed,
            Err(e) => break e,
        };
        liveness.traffic(Instant::now());
        let Some(shared) = shared.upgrade() else {
            break NetError::Closed;
        };
        let id = frame.stream();
        let open = id != 0 && shared.slots.lock().unwrap().contains_key(&id);
        match rules::route_from_server(frame.kind(), id, open) {
            ClientRoute::Pong => {
                // Best effort: the reader never waits for the writer. A
                // connection too busy to take a pong is showing the server
                // all the sign of life it needs.
                if let Ok(Frame::ServerPing(p)) = frame.decode()
                    && let Ok(pong) =
                        encode(0, 0, &Frame::ClientPong(ClientPong { nonce: p.nonce }))
                {
                    let _ = shared.out.try_send(Out::Frame(pong));
                }
            }
            ClientRoute::PongReceived | ClientRoute::Drop => {}
            ClientRoute::GoAway => shared.draining.store(true, Ordering::Release),
            ClientRoute::Fatal => {
                break frame
                    .failure()
                    .map(NetError::Failed)
                    .unwrap_or(NetError::Closed);
            }
            ClientRoute::Violation(why) => {
                break NetError::Unexpected {
                    kind: frame.kind(),
                    context: why,
                };
            }
            ClientRoute::Deliver => deliver(&shared, id, frame),
        }
    };
    let _ = closed.send(true);
    let Some(shared) = shared.upgrade() else {
        return;
    };
    let slots: Vec<Slot> = shared
        .slots
        .lock()
        .unwrap()
        .drain()
        .map(|(_, s)| s)
        .collect();
    for slot in slots {
        if let Slot::Unary(tx) = slot {
            let _ = tx.send(Err(clone_error(&reason)));
        }
    }
    tracing::debug!(error = %reason, "client connection closed");
}

/// Hands one frame to the request it answers. Never waits: the reader
/// serves every stream of the connection, so no stream's consumer may hold it.
fn deliver(shared: &Shared, id: u32, frame: OwnedFrame) {
    let last = rules::reply_kind(frame.kind(), frame.is_end()) == Reply::Last;
    let mut slots = shared.slots.lock().unwrap();
    match slots.get(&id) {
        Some(Slot::Unary(_)) => {
            let Some(Slot::Unary(tx)) = slots.remove(&id) else {
                unreachable!()
            };
            drop(slots);
            let result = match frame.failure() {
                Some(failure) => Err(NetError::Failed(failure)),
                None => Ok(frame),
            };
            let _ = tx.send(result);
        }
        Some(Slot::Stream(sink)) => {
            // A consumer that stopped reading has dropped its receiver.
            let _ = sink.tx.send(frame);
            if last {
                slots.remove(&id);
            }
        }
        None => {}
    }
}

fn clone_error(e: &NetError) -> NetError {
    match e {
        NetError::Failed(f) => NetError::Failed(f.clone()),
        NetError::Timeout => NetError::Timeout,
        _ => NetError::Closed,
    }
}

impl Connection {
    pub fn welcome(&self) -> &Welcomed {
        &self.shared.welcome
    }

    pub fn is_closed(&self) -> bool {
        *self.shared.closed.borrow()
    }

    /// Whether the server asked clients to move to another node.
    pub fn is_draining(&self) -> bool {
        self.shared.draining.load(Ordering::Acquire)
    }

    /// Resolves once the connection has closed.
    pub async fn closed(&self) {
        let mut closed = self.shared.closed.clone();
        let _ = closed.wait_for(|c| *c).await;
    }

    /// Sends a request that is answered by exactly one frame.
    pub async fn request(&self, frame: &Frame<'_>) -> Result<OwnedFrame, NetError> {
        let (tx, rx) = oneshot::channel();
        let id = self.shared.allocate(Slot::Unary(tx))?;
        if let Err(e) = self.shared.send(id, true, frame).await {
            self.shared.release(id);
            return Err(e);
        }
        rx.await.unwrap_or(Err(NetError::Closed))
    }

    /// Opens a stream with its first frame. With `end` the client says
    /// nothing more in it and only reads (a Source, a subscription); without
    /// it the client keeps sending (a handler's answers, a subscription
    /// query's permits). Either way the stream's window is granted back as
    /// its frames are read, and dropping it cancels it.
    pub async fn open(&self, frame: &Frame<'_>, end: bool) -> Result<ClientStream, NetError> {
        let (tx, rx) = mpsc::unbounded_channel();
        let id = self.shared.allocate(Slot::Stream(Sink { tx }))?;
        if let Err(e) = self.shared.send(id, end, frame).await {
            self.shared.release(id);
            return Err(e);
        }
        Ok(ClientStream {
            sender: StreamWriter {
                id,
                shared: Arc::clone(&self.shared),
            },
            rx,
            consumed: 0,
            finished: false,
        })
    }
}

/// The sending half of an open stream; cloneable, for answering from
/// several tasks while another reads.
#[derive(Clone)]
pub struct StreamWriter {
    id: u32,
    shared: Arc<Shared>,
}

impl StreamWriter {
    pub async fn send(&self, frame: &Frame<'_>) -> Result<(), NetError> {
        self.shared.send(self.id, false, frame).await
    }

    pub async fn send_end(&self, frame: &Frame<'_>) -> Result<(), NetError> {
        self.shared.send(self.id, true, frame).await
    }
}

/// An open stream: read the server's frames, send more of the client's.
/// Dropping it before the server finished cancels the request.
pub struct ClientStream {
    sender: StreamWriter,
    rx: mpsc::UnboundedReceiver<OwnedFrame>,
    /// Bytes read since the window was last granted back.
    consumed: u32,
    finished: bool,
}

impl ClientStream {
    pub fn writer(&self) -> StreamWriter {
        self.sender.clone()
    }

    /// The next frame from the server. `Ok(None)` after its last frame; a
    /// RequestFailed becomes `Err`.
    pub async fn next(&mut self) -> Result<Option<OwnedFrame>, NetError> {
        if self.finished {
            return Ok(None);
        }
        let Some(frame) = self.rx.recv().await else {
            self.finished = true;
            return Err(NetError::Closed);
        };
        if rules::reply_kind(frame.kind(), frame.is_end()) == Reply::Last {
            self.finished = true;
        } else {
            // The frame that ends a stream spends no window; the others are
            // granted back once half the window has been read.
            self.consumed = self.consumed.saturating_add(frame.bytes().len() as u32);
            if rules::grant_due(self.consumed, self.sender.shared.window) {
                self.grant();
            }
        }
        match frame.failure() {
            Some(failure) => Err(NetError::Failed(failure)),
            None => Ok(Some(frame)),
        }
    }

    /// Hands the server back the window this stream's consumer used. Never
    /// waits, so `next` stays safe to cancel: a full writer queue sends it
    /// from a task of its own.
    fn grant(&mut self) {
        let bytes = std::mem::take(&mut self.consumed);
        let Ok(window) = encode(self.sender.id, 0, &Frame::Window(Window { bytes })) else {
            return;
        };
        if let Err(mpsc::error::TrySendError::Full(window)) =
            self.sender.shared.out.try_send(Out::Frame(window))
        {
            let out = self.sender.shared.out.clone();
            tokio::spawn(async move {
                let _ = out.send(window).await;
            });
        }
    }

    pub async fn send(&self, frame: &Frame<'_>) -> Result<(), NetError> {
        self.sender.send(frame).await
    }

    pub async fn send_end(&self, frame: &Frame<'_>) -> Result<(), NetError> {
        self.sender.send_end(frame).await
    }

    /// Cancels the request and waits for the server to confirm.
    pub async fn cancel(mut self) {
        if !self.finished {
            let _ = self
                .sender
                .shared
                .send(self.sender.id, true, &Frame::Cancel(Cancel {}))
                .await;
            while let Ok(Some(_)) = self.next().await {}
            self.finished = true;
        }
    }
}

impl Drop for ClientStream {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        self.sender.shared.release(self.sender.id);
        if let Ok(bytes) = encode(self.sender.id, FLAG_END, &Frame::Cancel(Cancel {})) {
            // Best effort: a full queue means the connection is in trouble anyway.
            let _ = self.sender.shared.out.try_send(Out::Frame(bytes));
        }
    }
}
