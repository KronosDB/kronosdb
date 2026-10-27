//! The decisions a connection makes, as plain functions over plain data, so
//! they are tested without sockets. The drivers in `server` and `client` do
//! the I/O these decide on.

use std::time::{Duration, Instant};

use crate::generated::{
    Cancel, ClientPong, ErrorCode, GoAway, Hello, OpenCommandHandlers, OpenPlatform,
    OpenQueryHandlers, OpenSubscriptionQuery, Ping, Pong, RelaySubscription, RequestFailed,
    ServerPing, Subscribe, TailOpen, Welcome,
};

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
        (Cancel::KIND, false) => ServerRoute::Drop,
        (_, true) => ServerRoute::Deliver,
        (_, false) => ServerRoute::Open,
    }
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

/// How much of a stream's replies the server may have in flight towards a
/// consumer that has not read them yet.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplyFlow {
    /// The server sends no more than the client granted (credits, a byte
    /// window), so what is queued is bounded by the client's own grant and
    /// the client keeps all of it. A platform stream belongs here too: its
    /// frames are heartbeats and instructions at the server's own pace.
    Granted,
    /// The server sends as fast as the connection takes it. The client keeps
    /// a byte budget per stream and fails the stream that outruns its
    /// consumer, so one slow reader cannot hold up the connection.
    Budgeted,
}

/// Which kind of replies the stream opened by a request of `kind` gets. A
/// request this does not know is treated as `Budgeted`, the safe side.
pub fn reply_flow(kind: u16) -> ReplyFlow {
    match kind {
        Subscribe::KIND
        | OpenSubscriptionQuery::KIND
        | RelaySubscription::KIND
        | OpenCommandHandlers::KIND
        | OpenQueryHandlers::KIND
        | OpenPlatform::KIND
        | TailOpen::KIND => ReplyFlow::Granted,
        _ => ReplyFlow::Budgeted,
    }
}

/// Whether a stream that already queues `queued` bytes takes another frame
/// of `len` bytes within `budget`. A stream with nothing queued always takes
/// one, so a frame bigger than the budget still gets through.
pub fn admits(queued: usize, len: usize, budget: usize) -> bool {
    queued == 0 || queued.saturating_add(len) <= budget
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
    use crate::generated::{Append, Appended, Credit, Events, GetHead, Head};

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
            route_from_client(Credit::KIND, 7, true),
            ServerRoute::Deliver
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
    fn only_granted_streams_are_unbudgeted() {
        use crate::generated::{DispatchQuery, SnapshottedSource, Source};
        assert_eq!(reply_flow(Subscribe::KIND), ReplyFlow::Granted);
        assert_eq!(reply_flow(OpenCommandHandlers::KIND), ReplyFlow::Granted);
        assert_eq!(reply_flow(TailOpen::KIND), ReplyFlow::Granted);
        assert_eq!(reply_flow(Source::KIND), ReplyFlow::Budgeted);
        assert_eq!(reply_flow(SnapshottedSource::KIND), ReplyFlow::Budgeted);
        assert_eq!(reply_flow(DispatchQuery::KIND), ReplyFlow::Budgeted);
        assert_eq!(reply_flow(0x7fff), ReplyFlow::Budgeted);
    }

    #[test]
    fn a_budget_admits_up_to_its_size_and_one_oversized_frame() {
        assert!(admits(0, 100, 10));
        assert!(admits(40, 60, 100));
        assert!(!admits(41, 60, 100));
        assert!(!admits(1, usize::MAX, 100));
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
