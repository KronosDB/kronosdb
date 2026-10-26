//! Clients and their platform streams: who is connected, which processors
//! they run, and what the server may tell them.

use std::sync::Arc;
use std::time::Duration;

use kronosdb_messaging::api::MessagingPlatform;
use kronosdb_messaging::types::{ClientId, ComponentName};
use tokio::sync::mpsc;

use super::Hub;
use super::types::{ClientIdentity, Fault, NodeInfo, ProcessorReport, ToClient};

/// How many messages may queue for one platform stream.
const PLATFORM_QUEUE: usize = 256;

impl Hub {
    /// What this node tells a client about itself.
    pub fn node_info(&self) -> NodeInfo {
        NodeInfo {
            node_name: self.config.node_name.clone(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            contexts: (self.contexts)(),
        }
    }

    /// A client introduces itself ahead of opening its platform stream.
    pub fn register_client(&self, identity: ClientIdentity) -> Result<NodeInfo, Fault> {
        if identity.client_id.is_empty() {
            return Err(Fault::invalid("client_id is required"));
        }
        if identity.component_name.is_empty() {
            return Err(Fault::invalid("component_name is required"));
        }
        self.client_registry.register(
            ClientId(identity.client_id),
            ComponentName(identity.component_name),
            identity.version,
        );
        Ok(self.node_info())
    }

    /// Opens a client's platform stream. The first message on it is this
    /// node's info. A newer stream of the same client replaces an older one,
    /// whose receiver then ends.
    pub fn open_platform(
        self: &Arc<Self>,
        identity: ClientIdentity,
    ) -> Result<(PlatformSession, mpsc::Receiver<ToClient>), Fault> {
        if identity.client_id.is_empty() {
            return Err(Fault::invalid("client_id is required"));
        }
        let client = ClientId(identity.client_id);
        self.client_registry.register(
            client.clone(),
            ComponentName(identity.component_name),
            identity.version,
        );
        self.client_registry.set_stream_active(&client, true);
        let stream = self.next_stream();
        let (tx, rx) = mpsc::channel(PLATFORM_QUEUE);
        let _ = tx.try_send(ToClient::Node(self.node_info()));
        self.clients.register_platform(&client.0, stream, tx);
        Ok((
            PlatformSession {
                hub: Arc::clone(self),
                client,
                stream,
            },
            rx,
        ))
    }

    /// Cleans up after clients that registered and went silent: off every
    /// bus and out of the replicated routing table.
    pub fn spawn_reaper(self: &Arc<Self>) -> tokio::task::JoinHandle<()> {
        let hub = Arc::clone(self);
        let timeout = hub.config.heartbeat_timeout;
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(timeout.max(Duration::from_millis(10)));
            loop {
                interval.tick().await;
                for client in hub.client_registry.reap_dead_clients(timeout) {
                    hub.forget_client(&client);
                }
            }
        })
    }

    /// Takes a client off every bus and out of the replicated table.
    fn forget_client(&self, client: &ClientId) {
        for bus in self.all_buses() {
            bus.engine.remove_client(client);
            bus.handler_gone(&client.0);
        }
        self.retract_client(&client.0);
    }
}

/// A client's platform stream. Dropping it, however the stream ended, is the
/// client leaving: its handler streams end, and everything it registered
/// goes.
pub struct PlatformSession {
    hub: Arc<Hub>,
    client: ClientId,
    stream: u64,
}

impl PlatformSession {
    pub fn client(&self) -> &ClientId {
        &self.client
    }

    /// The client is alive.
    pub fn heartbeat(&self) {
        self.hub.client_registry.heartbeat(&self.client);
    }

    /// The client reports one of its event processors.
    pub fn report(&self, report: ProcessorReport) {
        self.hub.processors.report(&self.client.0, report);
    }
}

impl Drop for PlatformSession {
    fn drop(&mut self) {
        // A newer platform stream of this client owns it now: its
        // registrations are the new ones, not ours to remove.
        if !self
            .hub
            .clients
            .unregister_platform(&self.client.0, self.stream)
        {
            return;
        }
        // Handler streams end with it: they are cancelled, and this does
        // their cleanup, so a stale stream cannot clean up registrations the
        // client makes after it reconnects.
        self.hub.clients.close_handler_streams(&self.client.0);
        self.hub.processors.remove_client(&self.client.0);
        self.hub
            .client_registry
            .set_stream_active(&self.client, false);
        self.hub.client_registry.unregister(&self.client);
        self.hub.forget_client(&self.client);
        tracing::info!(client_id = %self.client.0, "platform stream ended: client removed");
    }
}
