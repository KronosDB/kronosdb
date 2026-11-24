//! One bus: its engine, and the senders to the handlers connected to this
//! node. The engine decides who handles a message and holds the permits;
//! the links are how a message reaches them.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use dashmap::DashMap;
use kronosdb_messaging::engine::MessagingEngine;
use kronosdb_messaging::query::QueryResult;
use parking_lot::Mutex;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::{mpsc, oneshot};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use super::types::{Fault, FaultKind, ToCommandHandler, ToQueryHandler};

/// The way to one handler stream. `stream` tells a stream's own link from
/// the one that replaced it when a client reconnects.
pub(super) struct Link<M> {
    pub(super) stream: u64,
    pub(super) tx: mpsc::Sender<M>,
    /// Cancelled when something else ends the stream: a newer stream of the
    /// same client taking over, or the client going silent.
    pub(super) cancel: CancellationToken,
    /// When the stream opened.
    pub(super) opened: Instant,
    /// What the stream has subscribed, by message type, with the load
    /// factor it asked for. This is what the client has on this bus: the
    /// engine's subscriptions and the table's rows follow it.
    pub(super) subscribed: HashMap<String, i32>,
}

impl<M> Link<M> {
    pub(super) fn new(stream: u64, tx: mpsc::Sender<M>, cancel: CancellationToken) -> Self {
        Self {
            stream,
            tx,
            cancel,
            opened: Instant::now(),
            subscribed: HashMap::new(),
        }
    }
}

/// Where a query's results gather: the sender is the dispatcher's stream,
/// and the entry retires once every handler it went to has said it is done.
pub(super) struct Collector {
    pub(super) results: mpsc::Sender<Result<QueryResult, Fault>>,
    /// The handlers that have not sent QueryDone yet.
    pub(super) outstanding: Mutex<Vec<String>>,
    /// Whether any handler has sent a result yet.
    pub(super) answered: AtomicBool,
    /// Whether these handlers are the only ones the query went to, so that
    /// none of them answering is the whole answer.
    pub(super) alone: bool,
    /// The handlers whose streams have not taken the query yet. One that
    /// ends first never saw it.
    pub(super) unsent: Mutex<Vec<String>>,
}

pub struct Bus {
    pub name: Arc<str>,
    pub engine: Arc<MessagingEngine>,
    pub(super) command_links: DashMap<String, Link<ToCommandHandler>>,
    pub(super) query_links: DashMap<String, Link<ToQueryHandler>>,
    /// Queries in flight, by request id.
    pub(super) collectors: DashMap<String, Arc<Collector>>,
    /// Subscription queries waiting for their handler's initial result.
    pub(super) initials: DashMap<String, oneshot::Sender<QueryResult>>,
}

impl Bus {
    pub(super) fn new(name: Arc<str>, engine: Arc<MessagingEngine>) -> Self {
        Self {
            name,
            engine,
            command_links: DashMap::new(),
            query_links: DashMap::new(),
            collectors: DashMap::new(),
            initials: DashMap::new(),
        }
    }

    /// Hands a message to a command handler's stream.
    pub(super) async fn to_command_handler(
        &self,
        client_id: &str,
        message: ToCommandHandler,
    ) -> Result<(), Undelivered> {
        deliver(&self.command_links, client_id, message).await
    }

    pub(super) async fn to_query_handler(
        &self,
        client_id: &str,
        message: ToQueryHandler,
    ) -> Result<(), Undelivered> {
        deliver(&self.query_links, client_id, message).await
    }

    /// Like [`to_query_handler`](Bus::to_query_handler) for messages nobody
    /// should wait on (cancellations): dropped when the stream is full.
    pub(super) fn nudge_query_handler(&self, client_id: &str, message: ToQueryHandler) {
        if let Some(link) = self.query_links.get(client_id) {
            let _ = link.tx.try_send(message);
        }
    }

    /// Marks `client_id` done with a query, and retires the query once every
    /// handler it went to is.
    pub(super) fn handler_done(&self, client_id: &str, request_id: &str) {
        let retire = match self.collectors.get(request_id) {
            Some(collector) => {
                let mut outstanding = collector.outstanding.lock();
                outstanding.retain(|h| h != client_id);
                outstanding.is_empty()
            }
            None => false,
        };
        if retire {
            self.collectors.remove(request_id);
        }
    }

    /// The handler's stream took the query.
    pub(super) fn query_taken(&self, client_id: &str, request_id: &str) {
        if let Some(collector) = self.collectors.get(request_id) {
            collector.unsent.lock().retain(|h| h != client_id);
        }
    }

    /// A handler is gone: the queries waiting on it will never hear from it.
    pub(super) fn handler_gone(&self, client_id: &str) {
        let waiting: Vec<String> = self
            .collectors
            .iter()
            .filter(|c| c.outstanding.lock().iter().any(|h| h == client_id))
            .map(|c| c.key().clone())
            .collect();
        for request_id in waiting {
            self.handler_gone_for(client_id, &request_id);
        }
    }

    /// A handler is gone. A query that it never took, and that no one else
    /// is answering, ends with a fault that says no handler took it, which
    /// the caller can try again; any other waits no more.
    pub(super) fn handler_gone_for(&self, client_id: &str, request_id: &str) {
        let Some(collector) = self.collectors.get(request_id).map(|c| Arc::clone(&c)) else {
            return;
        };
        let never_took = collector.unsent.lock().iter().any(|h| h == client_id);
        let others = collector.outstanding.lock().iter().any(|h| h != client_id);
        if never_took && !others && collector.alone && !collector.answered.load(Ordering::Acquire) {
            let _ = collector.results.try_send(Err(Fault::new(
                FaultKind::NoHandler,
                "no handler available: the handler's stream ended before it was sent the query",
            )));
        }
        self.handler_done(client_id, request_id);
    }
}

/// The handler has no stream on this node, or its stream has ended.
#[derive(Debug)]
pub(super) struct Undelivered;

/// One lookup finds the sender; a message that fits goes without cloning it.
async fn deliver<M>(
    links: &DashMap<String, Link<M>>,
    client_id: &str,
    message: M,
) -> Result<(), Undelivered> {
    let (tx, message) = {
        let Some(link) = links.get(client_id) else {
            return Err(Undelivered);
        };
        match link.tx.try_send(message) {
            Ok(()) => return Ok(()),
            Err(TrySendError::Closed(_)) => return Err(Undelivered),
            // The map's guard cannot be held across the wait.
            Err(TrySendError::Full(message)) => (link.tx.clone(), message),
        }
    };
    tx.send(message).await.map_err(|_| Undelivered)
}

#[cfg(test)]
mod tests {
    use super::*;
    use kronosdb_messaging::engine::MessagingEngine;

    fn bus() -> Bus {
        Bus::new("default".into(), Arc::new(MessagingEngine::new()))
    }

    fn link(tx: mpsc::Sender<ToQueryHandler>, stream: u64) -> Link<ToQueryHandler> {
        Link::new(stream, tx, CancellationToken::new())
    }

    #[tokio::test]
    async fn delivery_finds_the_handlers_stream() {
        let bus = bus();
        let (tx, mut rx) = mpsc::channel(1);
        bus.query_links.insert("h1".into(), link(tx, 1));
        let cancel = || ToQueryHandler::CancelQuery {
            request_id: "q".into(),
        };
        assert!(bus.to_query_handler("h1", cancel()).await.is_ok());
        assert!(matches!(
            rx.recv().await,
            Some(ToQueryHandler::CancelQuery { .. })
        ));
        // No stream: the message comes back.
        assert!(bus.to_query_handler("nobody", cancel()).await.is_err());
    }

    #[tokio::test]
    async fn a_full_stream_makes_the_sender_wait_not_fail() {
        let bus = Arc::new(bus());
        let (tx, mut rx) = mpsc::channel(1);
        bus.query_links.insert("h1".into(), link(tx, 1));
        let msg = |n: &str| ToQueryHandler::CancelQuery {
            request_id: n.into(),
        };
        bus.to_query_handler("h1", msg("a")).await.unwrap();
        let second = {
            let bus = Arc::clone(&bus);
            tokio::spawn(async move { bus.to_query_handler("h1", msg("b")).await })
        };
        tokio::task::yield_now().await;
        assert!(!second.is_finished(), "waits for room");
        rx.recv().await.unwrap();
        assert!(second.await.unwrap().is_ok());
    }

    fn collector(
        handlers: &[&str],
    ) -> (Arc<Collector>, mpsc::Receiver<Result<QueryResult, Fault>>) {
        let (results, rx) = mpsc::channel(4);
        (
            Arc::new(Collector {
                results,
                outstanding: Mutex::new(handlers.iter().map(|h| h.to_string()).collect()),
                answered: AtomicBool::new(false),
                alone: true,
                unsent: Mutex::new(Vec::new()),
            }),
            rx,
        )
    }

    #[tokio::test]
    async fn a_query_retires_when_every_handler_is_done() {
        let bus = bus();
        let (c, mut rx) = collector(&["h1", "h2"]);
        bus.collectors.insert("q1".into(), c);
        bus.handler_done("h1", "q1");
        assert!(bus.collectors.contains_key("q1"), "h2 is still working");
        bus.handler_done("h2", "q1");
        assert!(bus.collectors.is_empty());
        // Every sender is gone, so the dispatcher's stream ends.
        assert!(rx.recv().await.is_none());
        // Late or unknown completions are harmless.
        bus.handler_done("h1", "q1");
    }

    #[tokio::test]
    async fn a_vanished_handler_does_not_hold_a_query_open() {
        let bus = bus();
        let (c, _rx) = collector(&["h1", "h2"]);
        bus.collectors.insert("q1".into(), c);
        let (c2, _rx2) = collector(&["h2"]);
        bus.collectors.insert("q2".into(), c2);
        bus.handler_gone("h2");
        assert!(bus.collectors.contains_key("q1"), "h1 is still working");
        assert!(!bus.collectors.contains_key("q2"));
    }
}
