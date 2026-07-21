use std::collections::HashMap;

use parking_lot::Mutex;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tonic::Status;

/// Shared registry of all active command/query handler gRPC streams.
///
/// When a client disconnects (platform stream dies), the platform service
/// calls `close_client_streams()` to:
/// 1. Cancel the cancellation token — this immediately breaks all server-side
///    handler task loops via `tokio::select!`, preventing stale tasks from
///    cleaning up NEW handler registrations after the client reconnects.
/// 2. Send CANCELLED status on all outbound channels — this tells the client
///    that its handler streams are dead and it must reconnect.
pub struct HandlerStreamRegistry {
    /// client_id → (cancellation token, list of handler stream senders).
    streams: Mutex<HashMap<String, ClientEntry>>,
}

struct ClientEntry {
    token: CancellationToken,
    closers: Vec<Box<dyn StreamCloser>>,
}

/// Trait for closing a handler stream — type-erased so we can store
/// command and query stream senders in the same registry.
pub trait StreamCloser: Send + Sync {
    fn close(&self);
}

/// Wraps an mpsc::Sender to implement StreamCloser.
/// Sending an error closes the stream with a status the client can detect.
struct MpscStreamCloser<T: Send + 'static> {
    tx: mpsc::Sender<Result<T, Status>>,
}

impl<T: Send + 'static> StreamCloser for MpscStreamCloser<T> {
    fn close(&self) {
        // Send a CANCELLED status — the client will see this as a stream error
        // and trigger its reconnection logic.
        let _ = self.tx.try_send(Err(Status::cancelled(
            "client disconnected from platform stream",
        )));
    }
}

impl HandlerStreamRegistry {
    pub fn new() -> Self {
        Self {
            streams: Mutex::new(HashMap::new()),
        }
    }

    /// Gets (or creates) the cancellation token for a client.
    /// Handler tasks use this token in `tokio::select!` to detect platform disconnect.
    pub fn get_cancellation_token(&self, client_id: &str) -> CancellationToken {
        let mut streams = self.streams.lock();
        streams
            .entry(client_id.to_string())
            .or_insert_with(|| ClientEntry {
                token: CancellationToken::new(),
                closers: Vec::new(),
            })
            .token
            .clone()
    }

    /// Registers a handler stream sender for a client.
    pub fn register<T: Send + 'static>(
        &self,
        client_id: &str,
        tx: mpsc::Sender<Result<T, Status>>,
    ) {
        let mut streams = self.streams.lock();
        streams
            .entry(client_id.to_string())
            .or_insert_with(|| ClientEntry {
                token: CancellationToken::new(),
                closers: Vec::new(),
            })
            .closers
            .push(Box::new(MpscStreamCloser { tx }));
    }

    /// Closes all handler streams for a client and removes them from the registry.
    /// Called when the platform stream dies — triggers the client's reconnection
    /// logic which will re-register all handlers.
    ///
    /// 1. Cancels the token — server-side handler tasks exit immediately
    /// 2. Sends CANCELLED to outbound channels — client sees stream error
    pub fn close_client_streams(&self, client_id: &str) {
        let mut streams = self.streams.lock();
        if let Some(entry) = streams.remove(client_id) {
            // Cancel the token first — this makes handler tasks exit before
            // they can run cleanup code that would remove new registrations.
            entry.token.cancel();
            for closer in &entry.closers {
                closer.close();
            }
        }
    }
}
