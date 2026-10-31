//! The connection runtime: framing, multiplexing, the handshake and
//! liveness, shared by the server ([`server`]) and the client ([`client`]).
//!
//! A client connection has one task reading and routing each frame to its
//! stream and one writing, coalescing whatever frames are queued into one
//! write. A server connection is one task doing both ([`server`]), so a
//! request answered at once costs no hand-off. Everything else is the
//! application's.

pub mod client;
mod outbox;
pub mod rules;
pub mod server;

use std::fmt;
use std::io;

use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::mpsc;

use crate::codec::{
    DecodeError, EncodeError, FLAG_END, FRAME_HEADER_LEN, FrameHeader, encode_frame, split_frame,
};
use crate::generated::{ErrorCode, Frame, RequestFailed};

/// Frames larger than this are refused unless the connection agreed otherwise.
pub const DEFAULT_MAX_FRAME: u32 = 16 * 1024 * 1024;

/// A whole received frame, header included, sharing the read buffer.
#[derive(Clone)]
pub struct OwnedFrame {
    header: FrameHeader,
    bytes: Bytes,
}

impl OwnedFrame {
    /// Wraps one whole encoded frame, header included.
    pub fn from_bytes(bytes: Bytes) -> Result<Self, DecodeError> {
        let split = split_frame(&bytes)?.ok_or(DecodeError::Truncated {
            needed: FRAME_HEADER_LEN,
            at: 0,
        })?;
        Ok(Self {
            header: split.header,
            bytes,
        })
    }

    pub fn header(&self) -> FrameHeader {
        self.header
    }

    pub fn kind(&self) -> u16 {
        self.header.kind
    }

    pub fn stream(&self) -> u32 {
        self.header.stream
    }

    pub fn is_end(&self) -> bool {
        self.header.is_end()
    }

    pub fn body(&self) -> &[u8] {
        &self.bytes[FRAME_HEADER_LEN..]
    }

    /// The frame and its header, as received.
    pub fn bytes(&self) -> &Bytes {
        &self.bytes
    }

    /// The body as a sub-slice of the shared buffer, for keeping parts of a
    /// frame (an event batch, a payload) without copying them.
    pub fn body_bytes(&self) -> Bytes {
        self.bytes.slice(FRAME_HEADER_LEN..)
    }

    /// Decodes the frame. A kind this version does not know is an error here:
    /// callers that tolerate them check [`Frame::decode`] themselves.
    pub fn decode(&self) -> Result<Frame<'_>, NetError> {
        Frame::decode(self.kind(), self.body())?.ok_or(NetError::UnknownKind(self.kind()))
    }

    /// The failure this frame reports, if it is a RequestFailed.
    pub fn failure(&self) -> Option<Failure> {
        match self.decode() {
            Ok(Frame::RequestFailed(f)) => Some(Failure::new(f.code, f.message)),
            _ => None,
        }
    }
}

impl fmt::Debug for OwnedFrame {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OwnedFrame")
            .field("kind", &format_args!("{:#06x}", self.kind()))
            .field("stream", &self.stream())
            .field("flags", &self.header.flags)
            .field("len", &self.bytes.len())
            .finish()
    }
}

/// Encodes one frame into its own buffer.
pub fn encode(stream: u32, flags: u8, frame: &Frame<'_>) -> Result<Bytes, EncodeError> {
    encode_sized(stream, flags, frame, 64)
}

/// Encodes one frame into a buffer that starts at `capacity` bytes. A frame
/// that carries a large payload says so, and its bytes are copied once
/// instead of again each time the buffer outgrows itself.
pub fn encode_sized(
    stream: u32,
    flags: u8,
    frame: &Frame<'_>,
    capacity: usize,
) -> Result<Bytes, EncodeError> {
    let mut out = Vec::with_capacity(capacity);
    encode_frame(&mut out, stream, flags, frame)?;
    Ok(Bytes::from(out))
}

/// A request that failed, as RequestFailed reports it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Failure {
    pub code: ErrorCode,
    pub message: String,
}

impl Failure {
    pub fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }

    pub fn to_frame(&self) -> RequestFailed<'_> {
        RequestFailed {
            code: self.code,
            message: &self.message,
        }
    }
}

impl fmt::Display for Failure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.code.name() {
            Some(name) => write!(f, "{name}: {}", self.message),
            None => write!(f, "error {}: {}", self.code.0, self.message),
        }
    }
}

#[derive(Debug)]
pub enum NetError {
    Io(io::Error),
    Decode(DecodeError),
    Encode(EncodeError),
    /// The peer answered with RequestFailed.
    Failed(Failure),
    /// A frame kind this version does not know.
    UnknownKind(u16),
    /// A known frame where it makes no sense.
    Unexpected {
        kind: u16,
        context: &'static str,
    },
    /// A frame larger than the connection allows.
    FrameTooLarge {
        len: usize,
        max: u32,
    },
    /// The connection is closed, or closed before the answer arrived.
    Closed,
    Timeout,
    Tls(String),
}

impl NetError {
    /// The failure, when the peer reported one.
    pub fn failure(&self) -> Option<&Failure> {
        match self {
            NetError::Failed(f) => Some(f),
            _ => None,
        }
    }
}

impl fmt::Display for NetError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NetError::Io(e) => write!(f, "connection error: {e}"),
            NetError::Decode(e) => write!(f, "malformed frame: {e}"),
            NetError::Encode(e) => write!(f, "cannot encode: {e}"),
            NetError::Failed(failure) => failure.fmt(f),
            NetError::UnknownKind(kind) => write!(f, "unknown frame kind {kind:#06x}"),
            NetError::Unexpected { kind, context } => {
                write!(f, "unexpected frame {kind:#06x} {context}")
            }
            NetError::FrameTooLarge { len, max } => {
                write!(f, "frame of {len} bytes exceeds the {max}-byte limit")
            }
            NetError::Closed => write!(f, "connection closed"),
            NetError::Timeout => write!(f, "timed out"),
            NetError::Tls(e) => write!(f, "TLS: {e}"),
        }
    }
}

impl std::error::Error for NetError {}

impl From<io::Error> for NetError {
    fn from(e: io::Error) -> Self {
        NetError::Io(e)
    }
}

impl From<DecodeError> for NetError {
    fn from(e: DecodeError) -> Self {
        NetError::Decode(e)
    }
}

impl From<EncodeError> for NetError {
    fn from(e: EncodeError) -> Self {
        NetError::Encode(e)
    }
}

/// One queued write: a whole frame, or a header with a body shared by many
/// frames (the same event batch sent to many subscribers).
pub(crate) enum Out {
    Frame(Bytes),
    Shared {
        header: [u8; FRAME_HEADER_LEN],
        body: Bytes,
    },
}

impl Out {
    fn len(&self) -> usize {
        match self {
            Out::Frame(b) => b.len(),
            Out::Shared { body, .. } => FRAME_HEADER_LEN + body.len(),
        }
    }

    fn header(&self) -> &[u8] {
        match self {
            Out::Frame(b) => &b[..FRAME_HEADER_LEN],
            Out::Shared { header, .. } => header,
        }
    }

    /// Whether this is one of `stream`'s frames that a Cancel makes pointless:
    /// any but the one that ends the stream, which the client still waits for.
    fn cancelled_with(&self, stream: u32) -> bool {
        let h = self.header();
        u32::from_le_bytes(h[7..11].try_into().unwrap()) == stream && h[6] & FLAG_END == 0
    }
}

pub(crate) fn shared_header(
    stream: u32,
    kind: u16,
    flags: u8,
    body_len: usize,
) -> [u8; FRAME_HEADER_LEN] {
    let mut h = [0u8; FRAME_HEADER_LEN];
    h[0..4].copy_from_slice(&((FRAME_HEADER_LEN - 4 + body_len) as u32).to_le_bytes());
    h[4..6].copy_from_slice(&kind.to_le_bytes());
    h[6] = flags;
    h[7..11].copy_from_slice(&stream.to_le_bytes());
    h
}

/// Frames at least this big are written as they are instead of being copied
/// into the coalescing buffer.
const COPY_LIMIT: usize = 16 * 1024;
/// Stop coalescing once a write would be this big.
const WRITE_TARGET: usize = 256 * 1024;

/// Drains `rx` to `w`, one write for every burst of queued frames. Returns
/// when every sender is gone or the socket fails.
pub(crate) async fn write_loop<W: AsyncWrite + Unpin>(
    mut w: W,
    mut rx: mpsc::Receiver<Out>,
) -> io::Result<()> {
    let mut buf = BytesMut::with_capacity(64 * 1024);
    while let Some(first) = rx.recv().await {
        let mut next = Some(first);
        while let Some(out) = next.take() {
            if out.len() >= COPY_LIMIT {
                if !buf.is_empty() {
                    w.write_all(&buf).await?;
                    buf.clear();
                }
                match out {
                    Out::Frame(b) => w.write_all(&b).await?,
                    Out::Shared { header, body } => {
                        w.write_all(&header).await?;
                        w.write_all(&body).await?;
                    }
                }
            } else {
                match out {
                    Out::Frame(b) => buf.extend_from_slice(&b),
                    Out::Shared { header, body } => {
                        buf.extend_from_slice(&header);
                        buf.extend_from_slice(&body);
                    }
                }
            }
            if buf.len() < WRITE_TARGET {
                next = rx.try_recv().ok();
            }
        }
        if !buf.is_empty() {
            w.write_all(&buf).await?;
            buf.clear();
        }
        w.flush().await?;
    }
    w.shutdown().await
}

/// Reads frames off a connection, keeping partial input between calls so it
/// is safe to use inside `select!`.
pub(crate) struct FrameReader<R> {
    r: R,
    buf: BytesMut,
    max_frame: u32,
}

impl<R: AsyncRead + Unpin> FrameReader<R> {
    pub(crate) fn new(r: R, max_frame: u32) -> Self {
        Self {
            r,
            buf: BytesMut::with_capacity(64 * 1024),
            max_frame,
        }
    }

    pub(crate) fn set_max_frame(&mut self, max: u32) {
        self.max_frame = max;
    }

    /// Whether a whole frame is already buffered behind the current one.
    pub(crate) fn has_frame(&self) -> bool {
        self.buf.len() >= 4
            && self.buf.len() >= u32::from_le_bytes(self.buf[0..4].try_into().unwrap()) as usize + 4
    }

    /// The kind of the next frame, once all of it is buffered.
    pub(crate) fn peek_kind(&self) -> Option<u16> {
        (self.buf.len() >= 6 && self.has_frame())
            .then(|| u16::from_le_bytes(self.buf[4..6].try_into().unwrap()))
    }

    /// The next complete frame already buffered, if any.
    pub(crate) fn buffered(&mut self) -> Result<Option<OwnedFrame>, NetError> {
        if self.buf.len() < 4 {
            return Ok(None);
        }
        let len = u32::from_le_bytes(self.buf[0..4].try_into().unwrap()) as usize + 4;
        if len > self.max_frame as usize {
            return Err(NetError::FrameTooLarge {
                len,
                max: self.max_frame,
            });
        }
        if self.buf.len() < len {
            self.buf.reserve(len - self.buf.len());
            return Ok(None);
        }
        Ok(Some(OwnedFrame::from_bytes(
            self.buf.split_to(len).freeze(),
        )?))
    }

    /// Reads more bytes. `Ok(false)` at a clean end of input.
    pub(crate) async fn fill(&mut self) -> Result<bool, NetError> {
        if self.buf.capacity() - self.buf.len() < 4096 {
            self.buf.reserve(64 * 1024);
        }
        let n = self.r.read_buf(&mut self.buf).await?;
        if n == 0 {
            if self.buf.is_empty() {
                return Ok(false);
            }
            return Err(NetError::Io(io::ErrorKind::UnexpectedEof.into()));
        }
        Ok(true)
    }

    /// The next frame, reading as needed. `Ok(None)` at a clean end of input.
    pub(crate) async fn next(&mut self) -> Result<Option<OwnedFrame>, NetError> {
        loop {
            if let Some(frame) = self.buffered()? {
                return Ok(Some(frame));
            }
            if !self.fill().await? {
                return Ok(None);
            }
        }
    }

    /// Reads exactly `n` bytes that are not a frame (the preface).
    pub(crate) async fn read_exact(&mut self, n: usize) -> Result<Bytes, NetError> {
        while self.buf.len() < n {
            if !self.fill().await? {
                return Err(NetError::Closed);
            }
        }
        Ok(self.buf.split_to(n).freeze())
    }
}

#[cfg(test)]
mod tests;
