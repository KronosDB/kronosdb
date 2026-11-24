//! Handler streams: a client serving commands or queries on a bus.
//!
//! A session is opened when the client opens its stream and ends when the
//! value is dropped, however the stream ended; dropping it is the one
//! cleanup path (remove its subscriptions, fail what was in flight to it,
//! tell the other clients).
//!
//! The registered stream of a client on a bus is its link there, and the
//! link holds what the stream has subscribed. Everything else follows the
//! link: the engine's subscriptions change together with it, under the
//! link's lock, and the replicated routing table is written to match (see
//! [`table`](super::table)). A stream that is no longer the registered link
//! changes nothing.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use dashmap::mapref::entry::Entry;
use kronosdb_eventstore::raft::handler_registry::HandlerKind;
use kronosdb_messaging::command::CommandResult;
use kronosdb_messaging::query::QueryResult;
use kronosdb_messaging::types::{ClientId, ComponentName};
use tokio::sync::mpsc;
use tokio::time::Instant;
use tokio_util::sync::{CancellationToken, WaitForCancellationFuture};

use super::Hub;
use super::bus::{Bus, Link};
use super::subscriptions::update_of;
use super::table::QUERY_LOAD_FACTOR;
use super::types::{
    SubscriptionEvent, ToCommandHandler, ToQueryHandler, TopologyChange, TopologyEvent, Who,
};

/// How many messages may queue for one handler stream. Absorbs dispatch
/// bursts without making the dispatcher wait.
const HANDLER_QUEUE: usize = 4096;

/// Which of a client's handler streams on a bus an ending is for.
#[derive(Clone, Copy)]
pub(super) enum Which {
    /// This stream, if it is still the registered one.
    Stream(u64),
    /// The registered stream, if it opened before the instant: a client
    /// silent since then has not opened it.
    OpenedBefore(Instant),
}

impl Which {
    fn covers<M>(self, link: &Link<M>) -> bool {
        match self {
            Which::Stream(stream) => link.stream == stream,
            Which::OpenedBefore(instant) => link.opened < instant,
        }
    }
}

/// What ending a handler stream took away.
pub(super) struct Ended {
    pub(super) subscribed: usize,
    /// Commands the handler had been sent, which fail as a lost handler.
    pub(super) cancelled_in_flight: usize,
    /// Commands the stream never took, which fail as no handler.
    pub(super) not_sent: usize,
}

impl Hub {
    fn without_component(who: &Who) -> Who {
        Who {
            component_name: String::new(),
            ..who.clone()
        }
    }

    fn announce(&self, event: TopologyEvent, kind: HandlerKind, message_type: &str, who: &Who) {
        self.clients.broadcast(TopologyChange {
            event,
            message_type: message_type.to_string(),
            kind,
            client_id: who.client_id.clone(),
            component_name: who.component_name.clone(),
        });
    }

    /// Ends the client's command stream on `bus`, if `which` covers it: it
    /// is cancelled and unregistered, its subscriptions leave the engine and
    /// the commands in flight to it fail. None when there was nothing of
    /// that kind to end (a newer stream replaced it, or it ended already).
    pub(super) fn end_command_stream(
        self: &Arc<Self>,
        bus: &Bus,
        client_id: &str,
        which: Which,
    ) -> Option<Ended> {
        let ended = match bus.command_links.entry(client_id.to_string()) {
            Entry::Occupied(entry) if which.covers(entry.get()) => {
                entry.get().cancel.cancel();
                // Under the link's lock: a stream opening now waits for it.
                let client = ClientId(client_id.to_string());
                let (lost, not_sent) = bus.engine.remove_command_client_as(&client);
                let link = entry.remove();
                Ended {
                    subscribed: link.subscribed.len(),
                    cancelled_in_flight: lost.len(),
                    not_sent: not_sent.len(),
                }
            }
            _ => return None,
        };
        self.sync_routing(client_id);
        self.announce(
            TopologyEvent::HandlerDeregistered,
            HandlerKind::Command,
            "",
            &Who {
                client_id: client_id.to_string(),
                component_name: String::new(),
            },
        );
        Some(ended)
    }

    /// Ends the client's query stream on `bus`, if `which` covers it, like
    /// [`end_command_stream`](Hub::end_command_stream). The subscriptions
    /// it served end, and the queries waiting on it stop waiting.
    pub(super) fn end_query_stream(
        self: &Arc<Self>,
        bus: &Bus,
        client_id: &str,
        which: Which,
    ) -> Option<Ended> {
        use kronosdb_messaging::api::{QueryDispatcher, SubscriptionQueryDispatcher};
        let ended = match bus.query_links.entry(client_id.to_string()) {
            Entry::Occupied(entry) if which.covers(entry.get()) => {
                entry.get().cancel.cancel();
                let client = ClientId(client_id.to_string());
                bus.engine.remove_query_client(&client);
                // Its subscribers would otherwise wait for updates that
                // cannot come, and queries waiting on this handler for
                // answers that cannot come.
                bus.engine.fail_handler_subscriptions(&client);
                bus.handler_gone(client_id);
                let link = entry.remove();
                Ended {
                    subscribed: link.subscribed.len(),
                    cancelled_in_flight: 0,
                    not_sent: 0,
                }
            }
            _ => return None,
        };
        self.sync_routing(client_id);
        self.announce(
            TopologyEvent::HandlerDeregistered,
            HandlerKind::Query,
            "",
            &Who {
                client_id: client_id.to_string(),
                component_name: String::new(),
            },
        );
        Some(ended)
    }

    /// Opens a command handler stream for `who` on `bus`. A newer stream of
    /// the same client takes over from an older one: it starts with nothing
    /// subscribed, and the commands in flight to the older one fail.
    pub fn open_command_handlers(
        self: &Arc<Self>,
        bus: Arc<Bus>,
        who: Who,
    ) -> (CommandHandlers, mpsc::Receiver<ToCommandHandler>) {
        let stream = self.next_stream();
        let cancel = CancellationToken::new();
        let (tx, rx) = mpsc::channel(HANDLER_QUEUE);
        let link = Link::new(stream, tx, cancel.clone());
        let older = match bus.command_links.entry(who.client_id.clone()) {
            Entry::Occupied(mut entry) => {
                // Nothing the older stream was sent can be answered now,
                // and what it subscribed is not the client's any more: both
                // go before this stream can be given anything of its own.
                let (lost, not_sent) = bus
                    .engine
                    .remove_command_client_as(&ClientId(who.client_id.clone()));
                if !lost.is_empty() || !not_sent.is_empty() {
                    tracing::info!(client_id = %who.client_id, lost = lost.len(), not_sent = not_sent.len(), "command handler takeover: failed the older stream's commands in flight");
                }
                Some(std::mem::replace(entry.get_mut(), link))
            }
            Entry::Vacant(entry) => {
                entry.insert(link);
                None
            }
        };
        if let Some(older) = older {
            older.cancel.cancel();
            self.sync_routing(&who.client_id);
            if !older.subscribed.is_empty() {
                self.announce(
                    TopologyEvent::HandlerDeregistered,
                    HandlerKind::Command,
                    "",
                    &Self::without_component(&who),
                );
            }
        }
        tracing::info!(bus = %bus.name, client_id = %who.client_id, stream, "command handler stream opened");
        (
            CommandHandlers {
                hub: Arc::clone(self),
                bus,
                who,
                stream,
                cancel,
            },
            rx,
        )
    }

    /// Opens a query handler stream, taking over from an older one like
    /// [`open_command_handlers`](Hub::open_command_handlers).
    pub fn open_query_handlers(
        self: &Arc<Self>,
        bus: Arc<Bus>,
        who: Who,
    ) -> (QueryHandlers, mpsc::Receiver<ToQueryHandler>) {
        use kronosdb_messaging::api::{QueryDispatcher, SubscriptionQueryDispatcher};
        let stream = self.next_stream();
        let cancel = CancellationToken::new();
        let (tx, rx) = mpsc::channel(HANDLER_QUEUE);
        let link = Link::new(stream, tx, cancel.clone());
        let older = match bus.query_links.entry(who.client_id.clone()) {
            Entry::Occupied(mut entry) => {
                // The subscriptions the older stream served get no more
                // updates, and the queries waiting on it no answers.
                let client = ClientId(who.client_id.clone());
                bus.engine.remove_query_client(&client);
                bus.engine.fail_handler_subscriptions(&client);
                bus.handler_gone(&who.client_id);
                Some(std::mem::replace(entry.get_mut(), link))
            }
            Entry::Vacant(entry) => {
                entry.insert(link);
                None
            }
        };
        if let Some(older) = older {
            older.cancel.cancel();
            self.sync_routing(&who.client_id);
            if !older.subscribed.is_empty() {
                self.announce(
                    TopologyEvent::HandlerDeregistered,
                    HandlerKind::Query,
                    "",
                    &Self::without_component(&who),
                );
            }
        }
        tracing::info!(bus = %bus.name, client_id = %who.client_id, stream, "query handler stream opened");
        (
            QueryHandlers {
                hub: Arc::clone(self),
                bus,
                who,
                stream,
                cancel,
            },
            rx,
        )
    }
}

/// A client's command handler stream on one bus.
pub struct CommandHandlers {
    hub: Arc<Hub>,
    bus: Arc<Bus>,
    who: Who,
    stream: u64,
    cancel: CancellationToken,
}

impl CommandHandlers {
    /// Resolves when something else ends this stream: its client went
    /// silent, or a newer stream of the client took over.
    pub fn cancelled(&self) -> WaitForCancellationFuture<'_> {
        self.cancel.cancelled()
    }

    /// The stream is sending the command to its client. Until it does, the
    /// command is one its handler never saw.
    pub fn taken(&self, message_id: &str) {
        self.bus.engine.command_bus().mark_sent(message_id);
    }

    /// Runs `change` on this stream's link, under its lock. None when this
    /// stream is not the registered one: it is not the client's any more,
    /// and nothing it does counts.
    fn with_link<R>(&self, change: impl FnOnce(&mut Link<ToCommandHandler>) -> R) -> Option<R> {
        let mut link = self.bus.command_links.get_mut(&self.who.client_id)?;
        (link.stream == self.stream).then(|| change(&mut link))
    }

    pub fn subscribe(&mut self, command: &str, load_factor: i32) {
        use kronosdb_messaging::api::CommandDispatcher;
        let held = self.with_link(|link| {
            self.bus.engine.subscribe_command(
                command.to_string(),
                ClientId(self.who.client_id.clone()),
                ComponentName(self.who.component_name.clone()),
                load_factor,
            );
            link.subscribed.insert(command.to_string(), load_factor);
        });
        if held.is_none() {
            tracing::debug!(client_id = %self.who.client_id, command, "subscribe on a replaced command handler stream ignored");
            return;
        }
        self.hub.sync_routing(&self.who.client_id);
        self.hub.announce(
            TopologyEvent::HandlerRegistered,
            HandlerKind::Command,
            command,
            &self.who,
        );
    }

    pub fn unsubscribe(&mut self, command: &str) {
        use kronosdb_messaging::api::CommandDispatcher;
        let held = self.with_link(|link| {
            self.bus
                .engine
                .unsubscribe_command(command, &ClientId(self.who.client_id.clone()));
            link.subscribed.remove(command);
        });
        if held.is_none() {
            return;
        }
        self.hub.sync_routing(&self.who.client_id);
        self.hub.announce(
            TopologyEvent::HandlerDeregistered,
            HandlerKind::Command,
            command,
            &Hub::without_component(&self.who),
        );
    }

    /// The client will take `permits` more commands.
    pub fn grant(&self, permits: i64) {
        use kronosdb_messaging::api::CommandDispatcher;
        self.with_link(|_| {
            self.bus
                .engine
                .grant_command_permits(&ClientId(self.who.client_id.clone()), permits);
        });
    }

    /// The handler answered a command.
    pub fn complete(&self, result: CommandResult) {
        use kronosdb_messaging::api::CommandDispatcher;
        // The bus owns the caller's response sender; completing takes the
        // entry, sends, and records the metrics.
        let request_id = result.request_id.clone();
        self.bus.engine.complete_command(&request_id, result);
    }
}

impl Drop for CommandHandlers {
    fn drop(&mut self) {
        let client = &self.who.client_id;
        match self
            .hub
            .end_command_stream(&self.bus, client, Which::Stream(self.stream))
        {
            Some(ended) => tracing::info!(
                bus = %self.bus.name,
                client_id = %client,
                stream = self.stream,
                commands = ended.subscribed,
                cancelled_in_flight = ended.cancelled_in_flight,
                not_sent = ended.not_sent,
                "command handler stream ended: unsubscribed and failed in-flight commands"
            ),
            None => tracing::info!(
                bus = %self.bus.name,
                client_id = %client,
                stream = self.stream,
                "command handler stream ended; a newer stream or the client's removal already took its place"
            ),
        }
    }
}

/// A client's query handler stream on one bus.
pub struct QueryHandlers {
    hub: Arc<Hub>,
    bus: Arc<Bus>,
    who: Who,
    stream: u64,
    cancel: CancellationToken,
}

impl QueryHandlers {
    pub fn cancelled(&self) -> WaitForCancellationFuture<'_> {
        self.cancel.cancelled()
    }

    /// The stream is sending the query to its client, like
    /// [`CommandHandlers::taken`].
    pub fn taken(&self, message_id: &str) {
        self.bus.query_taken(&self.who.client_id, message_id);
    }

    /// Like [`CommandHandlers::with_link`].
    fn with_link<R>(&self, change: impl FnOnce(&mut Link<ToQueryHandler>) -> R) -> Option<R> {
        let mut link = self.bus.query_links.get_mut(&self.who.client_id)?;
        (link.stream == self.stream).then(|| change(&mut link))
    }

    pub fn subscribe(&mut self, query: &str) {
        use kronosdb_messaging::api::QueryDispatcher;
        let held = self.with_link(|link| {
            self.bus.engine.subscribe_query(
                query.to_string(),
                ClientId(self.who.client_id.clone()),
                ComponentName(self.who.component_name.clone()),
            );
            link.subscribed.insert(query.to_string(), QUERY_LOAD_FACTOR);
        });
        if held.is_none() {
            tracing::debug!(client_id = %self.who.client_id, query, "subscribe on a replaced query handler stream ignored");
            return;
        }
        self.hub.sync_routing(&self.who.client_id);
        self.hub.announce(
            TopologyEvent::HandlerRegistered,
            HandlerKind::Query,
            query,
            &self.who,
        );
    }

    pub fn unsubscribe(&mut self, query: &str) {
        use kronosdb_messaging::api::QueryDispatcher;
        let held = self.with_link(|link| {
            self.bus
                .engine
                .unsubscribe_query(query, &ClientId(self.who.client_id.clone()));
            link.subscribed.remove(query);
        });
        if held.is_none() {
            return;
        }
        self.hub.sync_routing(&self.who.client_id);
        self.hub.announce(
            TopologyEvent::HandlerDeregistered,
            HandlerKind::Query,
            query,
            &Hub::without_component(&self.who),
        );
    }

    pub fn grant(&self, permits: i64) {
        use kronosdb_messaging::api::QueryDispatcher;
        self.with_link(|_| {
            self.bus
                .engine
                .grant_query_permits(&ClientId(self.who.client_id.clone()), permits);
        });
    }

    /// The handler produced one result for a query.
    pub fn handled(&self, result: QueryResult) {
        // No collector: the query already retired, timed out or was given up on.
        if let Some(collector) = self.bus.collectors.get(&result.request_id) {
            collector.answered.store(true, Ordering::Release);
        }
        if let Some(collector) = self.bus.collectors.get(&result.request_id)
            && let Err(e) = collector.results.try_send(Ok(result))
        {
            tracing::warn!(
                request_id = %collector.key(),
                reason = %e,
                "query response dropped: caller buffer full or closed"
            );
        }
    }

    /// The handler has sent every result for a query.
    pub fn done(&self, request_id: &str) {
        self.bus.handler_done(&self.who.client_id, request_id);
    }

    /// The handler's word on a subscription query it serves.
    pub fn subscription_event(&self, subscription_id: &str, event: SubscriptionEvent) {
        use kronosdb_messaging::api::SubscriptionQueryDispatcher;
        match event {
            // The initial result goes to the subscriber ahead of every
            // update, by its own path.
            SubscriptionEvent::Initial(result) => {
                if let Some((_, initial)) = self.bus.initials.remove(subscription_id) {
                    let _ = initial.send(result);
                } else {
                    tracing::warn!(
                        subscription_id,
                        "subscription initial result dropped: subscriber gone or already answered"
                    );
                }
            }
            event => {
                let terminal = event.is_terminal();
                let Some(update) = update_of(subscription_id, event) else {
                    return;
                };
                self.bus.engine.send_update(subscription_id, update);
                if terminal {
                    // The terminal message is in the subscriber's buffer;
                    // retire the subscription so its channel closes and
                    // nothing leaks.
                    self.bus.engine.complete_subscription(subscription_id);
                }
            }
        }
    }
}

impl Drop for QueryHandlers {
    fn drop(&mut self) {
        let client = &self.who.client_id;
        match self
            .hub
            .end_query_stream(&self.bus, client, Which::Stream(self.stream))
        {
            Some(ended) => tracing::info!(
                bus = %self.bus.name,
                client_id = %client,
                stream = self.stream,
                queries = ended.subscribed,
                "query handler stream ended: unsubscribed its queries"
            ),
            None => tracing::info!(
                bus = %self.bus.name,
                client_id = %client,
                stream = self.stream,
                "query handler stream ended; a newer stream or the client's removal already took its place"
            ),
        }
    }
}
