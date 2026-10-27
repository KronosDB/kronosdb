//! The clients connected to this node: the way to each one's platform
//! stream, and the handler streams that must end with it.

use std::collections::HashMap;

use dashmap::DashMap;
use parking_lot::Mutex;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::types::{ProcessorInstruction, ToClient, TopologyChange};

struct PlatformLink {
    stream: u64,
    tx: mpsc::Sender<ToClient>,
}

#[derive(Default)]
pub struct ClientDirectory {
    platform: DashMap<String, PlatformLink>,
    /// A client's handler streams, by stream id. A client whose platform
    /// stream dies is gone, whatever its handler streams still say.
    handler_streams: Mutex<HashMap<String, Vec<(u64, CancellationToken)>>>,
}

impl ClientDirectory {
    pub fn new() -> Self {
        Self::default()
    }

    /// Makes `stream` the client's platform stream. The one it replaces
    /// loses its sender, which ends it.
    pub(super) fn register_platform(
        &self,
        client_id: &str,
        stream: u64,
        tx: mpsc::Sender<ToClient>,
    ) {
        self.platform
            .insert(client_id.to_string(), PlatformLink { stream, tx });
    }

    /// Forgets `stream` if it is still the client's platform stream. False
    /// when a newer one has taken over, which then owns the client.
    pub(super) fn unregister_platform(&self, client_id: &str, stream: u64) -> bool {
        self.platform
            .remove_if(client_id, |_, link| link.stream == stream)
            .is_some()
    }

    pub(super) fn track_handler_stream(
        &self,
        client_id: &str,
        stream: u64,
        token: CancellationToken,
    ) {
        self.handler_streams
            .lock()
            .entry(client_id.to_string())
            .or_default()
            .push((stream, token));
    }

    pub(super) fn forget_handler_stream(&self, client_id: &str, stream: u64) {
        let mut streams = self.handler_streams.lock();
        if let Some(list) = streams.get_mut(client_id) {
            list.retain(|(s, _)| *s != stream);
            if list.is_empty() {
                streams.remove(client_id);
            }
        }
    }

    /// Whether the client still has a handler stream other than `stream`.
    pub(super) fn has_other_handler_streams(&self, client_id: &str, stream: u64) -> bool {
        self.handler_streams
            .lock()
            .get(client_id)
            .is_some_and(|list| list.iter().any(|(s, _)| *s != stream))
    }

    /// Ends every handler stream of a client whose platform stream died.
    pub(super) fn close_handler_streams(&self, client_id: &str) {
        let closing = self.handler_streams.lock().remove(client_id);
        for (_, token) in closing.into_iter().flatten() {
            token.cancel();
        }
    }

    #[cfg(test)]
    pub fn is_connected(&self, client_id: &str) -> bool {
        self.platform.contains_key(client_id)
    }

    /// Asks every connected client to reconnect elsewhere (graceful shutdown).
    pub async fn request_reconnect_all(&self) {
        for tx in self.senders() {
            let _ = tx.send(ToClient::ReconnectRequested).await;
        }
    }

    /// Sends a processor instruction to one client. False when it is not
    /// connected or its stream is gone.
    pub async fn send_instruction(
        &self,
        client_id: &str,
        instruction: ProcessorInstruction,
    ) -> bool {
        let tx = self.platform.get(client_id).map(|link| link.tx.clone());
        match tx {
            Some(tx) => tx.send(ToClient::Instruction(instruction)).await.is_ok(),
            None => false,
        }
    }

    /// Tells every connected client about a topology change. A client whose
    /// queue is full is told once it has room, without holding up the rest
    /// or the caller.
    pub(super) fn broadcast(&self, change: TopologyChange) {
        for tx in self.senders() {
            let message = ToClient::Topology(change.clone());
            match tx.try_send(message) {
                Ok(()) | Err(mpsc::error::TrySendError::Closed(_)) => {}
                Err(mpsc::error::TrySendError::Full(message)) => {
                    tokio::spawn(async move {
                        let _ = tx.send(message).await;
                    });
                }
            }
        }
    }

    fn senders(&self) -> Vec<mpsc::Sender<ToClient>> {
        self.platform.iter().map(|link| link.tx.clone()).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hub::types::{ProcessorAction, TopologyEvent};
    use kronosdb_eventstore::raft::handler_registry::HandlerKind;

    fn change() -> TopologyChange {
        TopologyChange {
            event: TopologyEvent::HandlerRegistered,
            message_type: "Echo".into(),
            kind: HandlerKind::Command,
            client_id: "h1".into(),
            component_name: "svc".into(),
        }
    }

    #[tokio::test]
    async fn a_newer_platform_stream_owns_the_client() {
        let clients = ClientDirectory::new();
        let (old_tx, mut old_rx) = mpsc::channel(4);
        let (new_tx, _new_rx) = mpsc::channel(4);
        clients.register_platform("c", 1, old_tx);
        clients.register_platform("c", 2, new_tx);
        // The replaced stream's sender is gone, so its receiver ends.
        assert!(old_rx.recv().await.is_none());
        // Its late cleanup finds the client is no longer its own.
        assert!(!clients.unregister_platform("c", 1));
        assert!(clients.is_connected("c"));
        assert!(clients.unregister_platform("c", 2));
        assert!(!clients.is_connected("c"));
    }

    #[tokio::test]
    async fn broadcasts_and_instructions_reach_connected_clients() {
        let clients = ClientDirectory::new();
        let (tx, mut rx) = mpsc::channel(4);
        clients.register_platform("c", 1, tx);
        clients.broadcast(change());
        assert!(matches!(rx.recv().await, Some(ToClient::Topology(_))));

        let instruction = ProcessorInstruction {
            instruction_id: "i1".into(),
            action: ProcessorAction::Pause,
            processor_name: "proj".into(),
            segment: 0,
        };
        assert!(clients.send_instruction("c", instruction.clone()).await);
        assert!(!clients.send_instruction("nobody", instruction).await);
        assert!(matches!(rx.recv().await, Some(ToClient::Instruction(_))));

        clients.request_reconnect_all().await;
        assert!(matches!(
            rx.recv().await,
            Some(ToClient::ReconnectRequested)
        ));
    }

    #[tokio::test]
    async fn a_full_client_is_told_later_without_blocking_the_broadcast() {
        let clients = ClientDirectory::new();
        let (slow_tx, mut slow_rx) = mpsc::channel(1);
        let (fast_tx, mut fast_rx) = mpsc::channel(4);
        clients.register_platform("slow", 1, slow_tx);
        clients.register_platform("fast", 1, fast_tx);
        clients.broadcast(change());
        clients.broadcast(change());
        // The fast client has both at once; the slow one gets the second
        // when it makes room.
        assert!(fast_rx.recv().await.is_some() && fast_rx.recv().await.is_some());
        assert!(slow_rx.recv().await.is_some());
        assert!(slow_rx.recv().await.is_some());
    }

    #[test]
    fn handler_streams_end_with_their_platform_stream() {
        let clients = ClientDirectory::new();
        let (a, b) = (CancellationToken::new(), CancellationToken::new());
        clients.track_handler_stream("c", 10, a.clone());
        clients.track_handler_stream("c", 11, b.clone());
        assert!(clients.has_other_handler_streams("c", 10));
        clients.forget_handler_stream("c", 11);
        assert!(!clients.has_other_handler_streams("c", 10));
        clients.close_handler_streams("c");
        assert!(a.is_cancelled());
        assert!(
            !b.is_cancelled(),
            "a stream that already ended is left alone"
        );
    }
}
