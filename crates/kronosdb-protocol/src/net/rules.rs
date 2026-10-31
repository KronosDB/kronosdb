//! The decisions a connection makes, as plain functions over plain data, so
//! they are tested without sockets. The drivers in `server` and `client` do
//! the I/O these decide on.

use std::time::{Duration, Instant};

use crate::generated::{
    Cancel, ClientPong, ErrorCode, GoAway, Hello, Permits, Ping, Pong, RequestFailed, ServerPing,
    Welcome, Window,
};

/// Each stream's window when the client does not choose one: enough for a
/// stream to fill a fast link a millisecond or two away, refilled at half.
pub const DEFAULT_WINDOW: u32 = 8 * 1024 * 1024;
/// The smallest window a client may grant; anything less would make every
/// stream crawl.
pub const MIN_WINDOW: u32 = 64 * 1024;

/// Whether a frame kind is one the server sends.
pub fn sent_by_server(kind: u16) -> bool {
    kind & 0x8000 != 0
}

/// What the server does with one frame from a client.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServerRoute {
    /// Answer a client Ping.
    Pong,
    /// A client answered a ServerPing.
    PongReceived,
    /// The first frame of a new request.
    Open,
    /// More input for a stream that is open.
    Deliver,
    /// The client gave up on a stream.
    Cancel,
    /// The client grants a stream more of its window.
    Window,
    /// Nothing to do: input for a stream that has already finished.
    Drop,
    /// The client broke the protocol; the connection closes.
    Violation(&'static str),
}

/// Routes a frame from a client, given whether its stream is open.
pub fn route_from_client(kind: u16, stream: u32, open: bool) -> ServerRoute {
    if sent_by_server(kind) {
        return ServerRoute::Violation("the client sent a server frame");
    }
    if stream == 0 {
        return match kind {
            Ping::KIND => ServerRoute::Pong,
            ClientPong::KIND => ServerRoute::PongReceived,
            Hello::KIND => ServerRoute::Violation("Hello may only be sent once"),
            _ => ServerRoute::Violation("only pings travel on stream 0 after the handshake"),
        };
    }
    match (kind, open) {
        (Cancel::KIND, true) => ServerRoute::Cancel,
        (Window::KIND, true) => ServerRoute::Window,
        // About a stream, never the start of one: what arrives after the
        // stream ended is late, not a request.
        (Cancel::KIND | Window::KIND | Permits::KIND, false) => ServerRoute::Drop,
        (_, true) => ServerRoute::Deliver,
        (_, false) => ServerRoute::Open,
    }
}

/// Whether the server acts on a frame of `kind` while it is behind on
/// writing. Reading otherwise pauses then, so a client that does not read
/// cannot make the server buffer without bound; these produce no more than
/// one short answer per open stream, and two of them are how a client gets
/// the server out of being behind: a Cancel drops what a stream queued, and
/// a Window lets a stream that waited on it go on.
pub fn read_while_behind(kind: u16) -> bool {
    matches!(kind, Cancel::KIND | Window::KIND | ClientPong::KIND)
}

/// What the client does with one frame from the server.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientRoute {
    /// Answer a ServerPing.
    Pong,
    /// The server answered a Ping.
    PongReceived,
    /// The server is draining.
    GoAway,
    /// The server refused the connection or closed it for a violation.
    Fatal,
    /// A frame for an open request.
    Deliver,
    /// A frame for a request that is no longer waited on.
    Drop,
    Violation(&'static str),
}

pub fn route_from_server(kind: u16, stream: u32, open: bool) -> ClientRoute {
    if !sent_by_server(kind) {
        return ClientRoute::Violation("the server sent a client frame");
    }
    if stream == 0 {
        return match kind {
            ServerPing::KIND => ClientRoute::Pong,
            Pong::KIND => ClientRoute::PongReceived,
            GoAway::KIND => ClientRoute::GoAway,
            RequestFailed::KIND => ClientRoute::Fatal,
            Welcome::KIND => ClientRoute::Violation("Welcome may only be sent once"),
            _ => ClientRoute::Violation("only connection frames travel on stream 0"),
        };
    }
    if open {
        ClientRoute::Deliver
    } else {
        ClientRoute::Drop
    }
}

/// What a request's answer means for its stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reply {
    /// More frames follow.
    More,
    /// The stream is finished.
    Last,
}

pub fn reply_kind(kind: u16, end: bool) -> Reply {
    if end || kind == RequestFailed::KIND {
        Reply::Last
    } else {
        Reply::More
    }
}

/// Whether a client that consumed `consumed` bytes of a stream since it last
/// granted any grants them back now: once it is half the window, so the
/// server keeps sending while the grant travels.
pub fn grant_due(consumed: u32, window: u32) -> bool {
    consumed >= window / 2
}

/// Liveness of a quiet connection: after `interval` without traffic send a
/// ping, and give up once `allowed_misses` pings in a row go unanswered.
#[derive(Debug, Clone)]
pub struct Liveness {
    interval: Duration,
    allowed_misses: u32,
    last_traffic: Instant,
    unanswered: u32,
    nonce: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LivenessCheck {
    Fine,
    Ping(u64),
    Dead,
}

impl Liveness {
    pub fn new(interval: Duration, allowed_misses: u32, now: Instant) -> Self {
        Self {
            interval,
            allowed_misses,
            last_traffic: now,
            unanswered: 0,
            nonce: 0,
        }
    }

    /// Any frame from the peer proves it is alive.
    pub fn traffic(&mut self, now: Instant) {
        self.last_traffic = now;
        self.unanswered = 0;
    }

    pub fn check(&mut self, now: Instant) -> LivenessCheck {
        if now.duration_since(self.last_traffic) < self.interval {
            return LivenessCheck::Fine;
        }
        if self.unanswered >= self.allowed_misses {
            return LivenessCheck::Dead;
        }
        self.unanswered += 1;
        self.nonce += 1;
        // Wait a whole interval for the answer before the next ping.
        self.last_traffic = now;
        LivenessCheck::Ping(self.nonce)
    }
}

/// Client stream ids: increasing, skipping 0, wrapping after `u32::MAX`, so
/// an id is never reused while the server may still hold it.
pub fn next_stream_id(current: u32) -> u32 {
    match current.wrapping_add(1) {
        0 => 1,
        id => id,
    }
}

/// The code a request gets when its handler ends without answering.
pub fn unanswered_code(cancelled: bool) -> (ErrorCode, &'static str) {
    if cancelled {
        (ErrorCode::CANCELLED, "the client cancelled the request")
    } else {
        (
            ErrorCode::INTERNAL,
            "the server ended the request without answering it",
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::generated::{Append, Appended, Events, GetHead, Head};

    #[test]
    fn server_routes_by_stream_and_state() {
        assert_eq!(route_from_client(Ping::KIND, 0, false), ServerRoute::Pong);
        assert_eq!(
            route_from_client(ClientPong::KIND, 0, false),
            ServerRoute::PongReceived
        );
        assert!(matches!(
            route_from_client(GetHead::KIND, 0, false),
            ServerRoute::Violation(_)
        ));
        assert_eq!(route_from_client(Append::KIND, 7, false), ServerRoute::Open);
        assert_eq!(
            route_from_client(Permits::KIND, 7, true),
            ServerRoute::Deliver
        );
        assert_eq!(
            route_from_client(Window::KIND, 7, true),
            ServerRoute::Window
        );
        assert_eq!(route_from_client(Window::KIND, 7, false), ServerRoute::Drop);
        assert_eq!(
            route_from_client(Permits::KIND, 7, false),
            ServerRoute::Drop
        );
        assert_eq!(
            route_from_client(Cancel::KIND, 7, true),
            ServerRoute::Cancel
        );
        assert_eq!(route_from_client(Cancel::KIND, 7, false), ServerRoute::Drop);
        assert!(matches!(
            route_from_client(Head::KIND, 7, true),
            ServerRoute::Violation(_)
        ));
    }

    #[test]
    fn client_routes_by_stream_and_state() {
        assert_eq!(
            route_from_server(ServerPing::KIND, 0, false),
            ClientRoute::Pong
        );
        assert_eq!(
            route_from_server(GoAway::KIND, 0, false),
            ClientRoute::GoAway
        );
        assert_eq!(
            route_from_server(RequestFailed::KIND, 0, false),
            ClientRoute::Fatal
        );
        assert_eq!(
            route_from_server(Appended::KIND, 3, true),
            ClientRoute::Deliver
        );
        assert_eq!(route_from_server(Events::KIND, 3, false), ClientRoute::Drop);
        assert!(matches!(
            route_from_server(Append::KIND, 3, true),
            ClientRoute::Violation(_)
        ));
    }

    #[test]
    fn replies_end_on_end_or_failure() {
        assert_eq!(reply_kind(Events::KIND, false), Reply::More);
        assert_eq!(reply_kind(Events::KIND, true), Reply::Last);
        assert_eq!(reply_kind(RequestFailed::KIND, false), Reply::Last);
    }

    #[test]
    fn only_frames_that_cut_work_are_read_while_behind() {
        assert!(read_while_behind(Cancel::KIND));
        assert!(read_while_behind(Window::KIND));
        assert!(read_while_behind(ClientPong::KIND));
        // A ping wants an answer queued behind everything else.
        assert!(!read_while_behind(Ping::KIND));
        assert!(!read_while_behind(Permits::KIND));
        assert!(!read_while_behind(Append::KIND));
    }

    #[test]
    fn grants_go_back_at_half_the_window() {
        assert!(!grant_due(0, 1000));
        assert!(!grant_due(499, 1000));
        assert!(grant_due(500, 1000));
    }

    #[test]
    fn liveness_pings_then_gives_up() {
        let t0 = Instant::now();
        let s = Duration::from_secs(1);
        let mut l = Liveness::new(s, 2, t0);
        assert_eq!(l.check(t0 + s / 2), LivenessCheck::Fine);
        assert_eq!(l.check(t0 + s), LivenessCheck::Ping(1));
        assert_eq!(l.check(t0 + s + s / 2), LivenessCheck::Fine);
        assert_eq!(l.check(t0 + 2 * s), LivenessCheck::Ping(2));
        assert_eq!(l.check(t0 + 3 * s), LivenessCheck::Dead);
        l.traffic(t0 + 3 * s);
        assert_eq!(l.check(t0 + 4 * s), LivenessCheck::Ping(3));
    }

    #[test]
    fn stream_ids_skip_zero() {
        assert_eq!(next_stream_id(0), 1);
        assert_eq!(next_stream_id(41), 42);
        assert_eq!(next_stream_id(u32::MAX), 1);
    }
}
