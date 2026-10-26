//! Handler streams: a client serving commands or queries on a bus.
//!
//! A session is opened when the client opens its stream and ends when the
//! value is dropped, however the stream ended; dropping it is the one
//! cleanup path (deregister in the replicated table, fail what was in
//! flight to it, tell the other clients).

use std::sync::Arc;
use std::time::Duration;

use kronosdb_eventstore::error::Error;
use kronosdb_eventstore::raft::handler_registry::{HandlerKind, HandlerRegistration};
use kronosdb_messaging::command::CommandResult;
use kronosdb_messaging::query::QueryResult;
use kronosdb_messaging::types::{ClientId, ComponentName};
use tokio::sync::mpsc;
use tokio_util::sync::{CancellationToken, WaitForCancellationFuture};

use super::Hub;
use super::bus::{Bus, Link};
use super::subscriptions::update_of;
use super::types::{
    SubscriptionEvent, ToCommandHandler, ToQueryHandler, TopologyChange, TopologyEvent, Who,
};

/// How many messages may queue for one handler stream. Absorbs dispatch
/// bursts without making the dispatcher wait.
const HANDLER_QUEUE: usize = 4096;

impl Hub {
    /// Writes a handler's row to the replicated routing table (ADR-0007),
    /// off the caller: dispatch falls back to the local path until it lands.
    /// A write the control plane cannot take yet (no leader: the node is
    /// starting, or an election is on) is retried until it lands or the
    /// handler stream ends — otherwise the handler would stay invisible to
    /// every other node.
    fn publish_handler(
        &self,
        bus: &Bus,
        kind: HandlerKind,
        message_type: &str,
        who: &Who,
        load_factor: i32,
        alive: &CancellationToken,
    ) {
        let registration = HandlerRegistration {
            bus: bus.name.to_string(),
            kind,
            message_type: message_type.to_string(),
            client_id: who.client_id.clone(),
            node_id: self.cluster.local_node_id(),
            load_factor,
        };
        let cluster = Arc::clone(&self.cluster);
        let alive = alive.clone();
        tokio::spawn(async move {
            let mut backoff = Duration::from_millis(50);
            loop {
                // A stream that ended has retracted its rows; writing one
                // now would leave a row nothing serves.
                if alive.is_cancelled() {
                    return;
                }
                match cluster.register_handler(registration.clone()).await {
                    Ok(()) => return,
                    Err(Error::Unavailable { message }) => {
                        tracing::debug!(%message, "fabric: handler registration waits for the control plane");
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "fabric: handler registration write failed");
                        return;
                    }
                }
                tokio::select! {
                    _ = alive.cancelled() => return,
                    _ = tokio::time::sleep(backoff) => {}
                }
                backoff = (backoff * 2).min(Duration::from_secs(2));
            }
        });
    }

    fn retract_handler(&self, bus: &Bus, kind: HandlerKind, message_type: &str, client_id: &str) {
        let cluster = Arc::clone(&self.cluster);
        let (bus, message_type, client_id) = (
            bus.name.to_string(),
            message_type.to_string(),
            client_id.to_string(),
        );
        tokio::spawn(async move {
            if let Err(e) = cluster
                .deregister_handler(&bus, kind, &message_type, &client_id)
                .await
            {
                tracing::warn!(error = %e, "fabric: handler deregistration write failed");
            }
        });
    }

    /// Drops every row the client has, on every bus.
    pub(super) fn retract_client(&self, client_id: &str) {
        let cluster = Arc::clone(&self.cluster);
        let client_id = client_id.to_string();
        tokio::spawn(async move {
            if let Err(e) = cluster.deregister_client_handlers(&client_id).await {
                tracing::warn!(error = %e, "fabric: client deregistration write failed");
            }
        });
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

    /// Opens a command handler stream for `who` on `bus`. A newer stream of
    /// the same client takes over from an older one.
    pub fn open_command_handlers(
        self: &Arc<Self>,
        bus: Arc<Bus>,
        who: Who,
    ) -> (CommandHandlers, mpsc::Receiver<ToCommandHandler>) {
        let stream = self.next_stream();
        let cancel = CancellationToken::new();
        let (tx, rx) = mpsc::channel(HANDLER_QUEUE);
        let link = Link {
            stream,
            tx,
            cancel: cancel.clone(),
        };
        if bus.command_links.contains_key(&who.client_id) {
            // Takeover. The older stream leaves its cleanup to this one, and
            // nothing it was sent can be answered now: fail its commands in
            // flight before this stream can be given any of its own.
            let failed = bus
                .engine
                .command_bus()
                .cancel_for_handler(&ClientId(who.client_id.clone()));
            if !failed.is_empty() {
                tracing::info!(client_id = %who.client_id, failed = failed.len(), "command handler takeover: failed the older stream's commands in flight");
            }
        }
        if let Some(older) = bus.command_links.insert(who.client_id.clone(), link) {
            older.cancel.cancel();
        }
        self.clients
            .track_handler_stream(&who.client_id, stream, cancel.clone());
        tracing::info!(bus = %bus.name, client_id = %who.client_id, "command handler stream opened");
        (
            CommandHandlers {
                hub: Arc::clone(self),
                bus,
                who,
                stream,
                subscribed: Vec::new(),
                cancel,
                alive: CancellationToken::new(),
            },
            rx,
        )
    }

    pub fn open_query_handlers(
        self: &Arc<Self>,
        bus: Arc<Bus>,
        who: Who,
    ) -> (QueryHandlers, mpsc::Receiver<ToQueryHandler>) {
        let stream = self.next_stream();
        let cancel = CancellationToken::new();
        let (tx, rx) = mpsc::channel(HANDLER_QUEUE);
        let link = Link {
            stream,
            tx,
            cancel: cancel.clone(),
        };
        if bus.query_links.contains_key(&who.client_id) {
            // Takeover: the subscriptions the older stream served get no
            // more updates. End them before this stream can serve any.
            use kronosdb_messaging::api::SubscriptionQueryDispatcher;
            bus.engine
                .fail_handler_subscriptions(&ClientId(who.client_id.clone()));
        }
        if let Some(older) = bus.query_links.insert(who.client_id.clone(), link) {
            older.cancel.cancel();
        }
        self.clients
            .track_handler_stream(&who.client_id, stream, cancel.clone());
        tracing::info!(bus = %bus.name, client_id = %who.client_id, "query handler stream opened");
        (
            QueryHandlers {
                hub: Arc::clone(self),
                bus,
                who,
                stream,
                subscribed: Vec::new(),
                cancel,
                alive: CancellationToken::new(),
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
    subscribed: Vec<String>,
    cancel: CancellationToken,
    /// Cancelled when this value drops, however the stream ended.
    alive: CancellationToken,
}

impl CommandHandlers {
    /// Resolves when something else ends this stream: the client's platform
    /// stream died, or a newer stream of the client took over.
    pub fn cancelled(&self) -> WaitForCancellationFuture<'_> {
        self.cancel.cancelled()
    }

    pub fn subscribe(&mut self, command: &str, load_factor: i32) {
        use kronosdb_messaging::api::CommandDispatcher;
        self.bus.engine.subscribe_command(
            command.to_string(),
            ClientId(self.who.client_id.clone()),
            ComponentName(self.who.component_name.clone()),
            load_factor,
        );
        if !self.subscribed.iter().any(|c| c == command) {
            self.subscribed.push(command.to_string());
        }
        self.hub.publish_handler(
            &self.bus,
            HandlerKind::Command,
            command,
            &self.who,
            load_factor,
            &self.alive,
        );
        self.hub.announce(
            TopologyEvent::HandlerRegistered,
            HandlerKind::Command,
            command,
            &self.who,
        );
    }

    pub fn unsubscribe(&mut self, command: &str) {
        use kronosdb_messaging::api::CommandDispatcher;
        self.bus
            .engine
            .unsubscribe_command(command, &ClientId(self.who.client_id.clone()));
        self.subscribed.retain(|c| c != command);
        self.hub.retract_handler(
            &self.bus,
            HandlerKind::Command,
            command,
            &self.who.client_id,
        );
        self.hub.announce(
            TopologyEvent::HandlerDeregistered,
            HandlerKind::Command,
            command,
            &Who {
                component_name: String::new(),
                ..self.who.clone()
            },
        );
    }

    /// The client will take `permits` more commands.
    pub fn grant(&self, permits: i64) {
        use kronosdb_messaging::api::CommandDispatcher;
        self.bus
            .engine
            .grant_command_permits(&ClientId(self.who.client_id.clone()), permits);
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
        use kronosdb_messaging::api::CommandDispatcher;
        self.alive.cancel();
        let client = ClientId(self.who.client_id.clone());
        let others = self
            .hub
            .clients
            .has_other_handler_streams(&self.who.client_id, self.stream);
        self.hub
            .clients
            .forget_handler_stream(&self.who.client_id, self.stream);
        let mine = self
            .bus
            .command_links
            .remove_if(&self.who.client_id, |_, link| link.stream == self.stream)
            .is_some();
        // A newer stream owns the client's registrations now. And a client
        // whose platform stream died was cleaned up by that stream's end.
        if !mine || self.cancel.is_cancelled() {
            tracing::info!(client_id = %client.0, "command handler stream ended; cleanup left to its successor");
            return;
        }
        // Removes the subscriptions and fails every command in flight to
        // this handler with KRONOSDB-4006.
        let cancelled = self.bus.engine.remove_command_client(&client);
        if others {
            for command in &self.subscribed {
                self.hub
                    .retract_handler(&self.bus, HandlerKind::Command, command, &client.0);
            }
        } else {
            self.hub.retract_client(&client.0);
        }
        tracing::info!(
            client_id = %client.0,
            commands = self.subscribed.len(),
            cancelled_in_flight = cancelled.len(),
            "command handler stream ended: unsubscribed and failed in-flight commands"
        );
        self.hub.announce(
            TopologyEvent::HandlerDeregistered,
            HandlerKind::Command,
            "",
            &Who {
                component_name: String::new(),
                ..self.who.clone()
            },
        );
    }
}

/// A client's query handler stream on one bus.
pub struct QueryHandlers {
    hub: Arc<Hub>,
    bus: Arc<Bus>,
    who: Who,
    stream: u64,
    subscribed: Vec<String>,
    cancel: CancellationToken,
    /// Cancelled when this value drops, however the stream ended.
    alive: CancellationToken,
}

impl QueryHandlers {
    pub fn cancelled(&self) -> WaitForCancellationFuture<'_> {
        self.cancel.cancelled()
    }

    pub fn subscribe(&mut self, query: &str) {
        use kronosdb_messaging::api::QueryDispatcher;
        self.bus.engine.subscribe_query(
            query.to_string(),
            ClientId(self.who.client_id.clone()),
            ComponentName(self.who.component_name.clone()),
        );
        if !self.subscribed.iter().any(|q| q == query) {
            self.subscribed.push(query.to_string());
        }
        // Query fan-out reads these rows; the load factor is unused.
        self.hub.publish_handler(
            &self.bus,
            HandlerKind::Query,
            query,
            &self.who,
            100,
            &self.alive,
        );
        self.hub.announce(
            TopologyEvent::HandlerRegistered,
            HandlerKind::Query,
            query,
            &self.who,
        );
    }

    pub fn unsubscribe(&mut self, query: &str) {
        use kronosdb_messaging::api::QueryDispatcher;
        self.bus
            .engine
            .unsubscribe_query(query, &ClientId(self.who.client_id.clone()));
        self.subscribed.retain(|q| q != query);
        self.hub
            .retract_handler(&self.bus, HandlerKind::Query, query, &self.who.client_id);
        self.hub.announce(
            TopologyEvent::HandlerDeregistered,
            HandlerKind::Query,
            query,
            &Who {
                component_name: String::new(),
                ..self.who.clone()
            },
        );
    }

    pub fn grant(&self, permits: i64) {
        use kronosdb_messaging::api::QueryDispatcher;
        self.bus
            .engine
            .grant_query_permits(&ClientId(self.who.client_id.clone()), permits);
    }

    /// The handler produced one result for a query.
    pub fn handled(&self, result: QueryResult) {
        // No collector: the query already retired, timed out or was given up on.
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
        use kronosdb_messaging::api::{QueryDispatcher, SubscriptionQueryDispatcher};
        self.alive.cancel();
        let client = ClientId(self.who.client_id.clone());
        let others = self
            .hub
            .clients
            .has_other_handler_streams(&self.who.client_id, self.stream);
        self.hub
            .clients
            .forget_handler_stream(&self.who.client_id, self.stream);
        let mine = self
            .bus
            .query_links
            .remove_if(&self.who.client_id, |_, link| link.stream == self.stream)
            .is_some();
        if !mine || self.cancel.is_cancelled() {
            tracing::info!(client_id = %client.0, "query handler stream ended; cleanup left to its successor");
            return;
        }
        self.bus.engine.remove_query_client(&client);
        // Its subscribers would otherwise wait for updates that cannot come.
        self.bus.engine.fail_handler_subscriptions(&client);
        // Queries waiting on this handler would otherwise wait out their
        // deadline for answers that cannot come.
        self.bus.handler_gone(&client.0);
        if others {
            for query in &self.subscribed {
                self.hub
                    .retract_handler(&self.bus, HandlerKind::Query, query, &client.0);
            }
        } else {
            self.hub.retract_client(&client.0);
        }
        tracing::info!(
            client_id = %client.0,
            queries = self.subscribed.len(),
            "query handler stream ended: unsubscribed its queries"
        );
        self.hub.announce(
            TopologyEvent::HandlerDeregistered,
            HandlerKind::Query,
            "",
            &Who {
                component_name: String::new(),
                ..self.who.clone()
            },
        );
    }
}
