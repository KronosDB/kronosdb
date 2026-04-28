use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use dashmap::DashMap;
use parking_lot::RwLock;
use tokio::sync::oneshot;

use crate::error_codes;
use crate::handler::{HandlerRegistry, MessageTypeDetail, MessageTypeMetrics, MetricsSnapshot};
use crate::types::{
    ClientId, ComponentName, ErrorDetail, Metadata, MetadataValue, Payload, ProcessingInstruction,
    ProcessingKey, RoutingKey,
};

/// A command to be dispatched.
#[derive(Debug, Clone)]
pub struct Command {
    /// Unique message identifier.
    pub message_id: String,
    /// The command name, used for routing to the correct handler.
    pub name: String,
    /// Timestamp of command creation (millis since epoch).
    pub timestamp: i64,
    /// The command payload.
    pub payload: Payload,
    /// Metadata — opaque to KronosDB, transported losslessly.
    pub metadata: Metadata,
    /// Processing instructions (routing key, priority, timeout, etc.).
    pub processing_instructions: Vec<ProcessingInstruction>,
    /// Optional routing key for consistent hashing.
    pub routing_key: Option<RoutingKey>,
    /// The client that dispatched this command.
    pub client_id: ClientId,
    /// The component name that dispatched this command.
    pub component_name: ComponentName,
}

/// The result of command handler execution.
#[derive(Debug, Clone)]
pub struct CommandResult {
    /// Unique response identifier.
    pub message_id: String,
    /// The command this is a response to.
    pub request_id: String,
    /// Error code, if failed (top-level, for quick checks).
    pub error_code: Option<String>,
    /// Full error detail preserving message, location, details chain, and error code.
    pub error: Option<ErrorDetail>,
    /// Result payload, if any.
    pub payload: Option<Payload>,
    /// Response metadata — transported losslessly.
    pub metadata: Metadata,
    /// Processing instructions on the response.
    pub processing_instructions: Vec<ProcessingInstruction>,
}

/// Error dispatching a command.
#[derive(Debug)]
pub enum CommandError {
    /// No handler registered for this command type.
    NoHandlerAvailable { command_name: String },
    /// All handlers are at capacity (no permits).
    NoPermitsAvailable { command_name: String },
    /// The handler disconnected before responding.
    HandlerDisconnected,
    /// Timeout waiting for response.
    Timeout,
    /// A command with the same message_id is already in-flight.
    Duplicate { message_id: String },
    /// The in-flight command buffer is full.
    AtCapacity { capacity: usize },
}

impl std::fmt::Display for CommandError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoHandlerAvailable { command_name } => {
                write!(f, "no handler available for command '{command_name}'")
            }
            Self::NoPermitsAvailable { command_name } => {
                write!(f, "all handlers at capacity for command '{command_name}'")
            }
            Self::HandlerDisconnected => write!(f, "handler disconnected before responding"),
            Self::Timeout => write!(f, "timeout waiting for command response"),
            Self::Duplicate { message_id } => {
                write!(f, "command '{message_id}' is already in-flight")
            }
            Self::AtCapacity { capacity } => {
                write!(f, "in-flight command buffer full ({capacity} commands)")
            }
        }
    }
}

/// A command that has been assigned to a handler and is waiting for a response.
///
/// The response sender lives in [`CommandBus::in_flight`] — the gRPC layer
/// holds the receiver and the bus owns the sender so handler-disconnect
/// and timeout sweeps can fail callers without the gRPC layer maintaining
/// a parallel map.
pub struct PendingCommand {
    pub command: Command,
    /// The client_id of the handler this command was routed to.
    pub target_handler: ClientId,
    /// When this command was dispatched (for latency tracking).
    pub dispatched_at: Instant,
}

/// Configuration for the command bus.
pub struct CommandBusConfig {
    /// Soft capacity limit for in-flight commands. Low-priority commands
    /// (priority <= 0) are rejected when this is reached. Default: 10,000.
    pub capacity: usize,
    /// Hard capacity limit. ALL commands are rejected when this is reached.
    /// Default: 110% of capacity.
    pub hard_capacity: usize,
}

impl Default for CommandBusConfig {
    fn default() -> Self {
        Self::with_capacity(10_000)
    }
}

impl CommandBusConfig {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            capacity,
            hard_capacity: capacity + capacity / 10, // 110%
        }
    }
}

/// Tracks an in-flight command dispatched to a handler.
struct InFlightEntry {
    /// Sender for the caller's response. Consumed when the command
    /// completes, is cancelled, or is swept.
    response_tx: oneshot::Sender<CommandResult>,
    /// Which handler this command was sent to.
    target_handler: ClientId,
    /// When the command was dispatched.
    dispatched_at: Instant,
    /// The command name (for metrics on cancel/timeout paths).
    command_name: String,
}

/// The command bus. Routes commands to registered handlers.
///
/// Thread-safe. The handler registry stays behind a RwLock (subscribe/
/// unsubscribe are rare, dispatch reads are frequent and parallel — a
/// RwLock is the right fit). In-flight commands and per-type metrics
/// live in sharded DashMaps so concurrent dispatch/complete paths don't
/// contend on a single mutex.
pub struct CommandBus {
    handlers: RwLock<HandlerRegistry>,
    /// In-flight commands: message_id → tracking entry.
    /// Used for capacity limits, duplicate detection, and disconnect failover.
    /// Owns the response oneshot sender; entries are removed on completion,
    /// cancellation, or sweep.
    in_flight: DashMap<String, InFlightEntry>,
    /// Round-robin counter for load balancing.
    dispatch_counter: AtomicU64,
    /// Per-command-type dispatch metrics. Lock-free atomic counters
    /// inside the value; DashMap shards the key-level insert contention.
    metrics: DashMap<String, MessageTypeMetrics>,
    /// Configuration.
    config: CommandBusConfig,
}

impl Default for CommandBus {
    fn default() -> Self {
        Self::new()
    }
}

impl CommandBus {
    pub fn new() -> Self {
        Self::with_config(CommandBusConfig::default())
    }

    pub fn with_config(config: CommandBusConfig) -> Self {
        Self {
            handlers: RwLock::new(HandlerRegistry::new()),
            in_flight: DashMap::new(),
            dispatch_counter: AtomicU64::new(0),
            metrics: DashMap::new(),
            config,
        }
    }

    /// Registers a command handler.
    pub fn subscribe(
        &self,
        command_name: String,
        client_id: ClientId,
        component_name: ComponentName,
        load_factor: i32,
    ) {
        let mut handlers = self.handlers.write();
        handlers.subscribe(command_name, client_id, component_name, load_factor);
    }

    /// Unregisters a command handler.
    pub fn unsubscribe(&self, command_name: &str, client_id: &ClientId) {
        let mut handlers = self.handlers.write();
        handlers.unsubscribe(command_name, client_id);
    }

    /// Removes all subscriptions for a disconnected client and cancels any
    /// in-flight commands targeting that client (each receives a
    /// `KRONOSDB-4006` failure). Returns the message_ids that were cancelled,
    /// for logging.
    pub fn remove_client(&self, client_id: &ClientId) -> Vec<String> {
        {
            let mut handlers = self.handlers.write();
            handlers.remove_client(client_id);
        }
        self.cancel_for_handler(client_id)
    }

    /// Grants flow control permits to a client.
    pub fn grant_permits(&self, client_id: &ClientId, permits: i64) {
        let handlers = self.handlers.read();
        handlers.grant_permits(client_id, permits);
    }

    /// Dispatches a command to a handler.
    ///
    /// Returns a `PendingCommand` (to deliver to the handler) and a oneshot
    /// receiver for the response. The response sender is retained inside
    /// the bus's in-flight map so disconnect/timeout sweeps can fail the
    /// caller without the gRPC layer keeping a parallel map.
    ///
    /// Checks (in order): capacity (priority-aware) → duplicate → handler
    /// availability → permits.
    ///
    /// Load balancing: weighted round-robin based on load_factor.
    /// Routing key: if present, consistent hashing to the same handler.
    pub fn dispatch(
        &self,
        command: Command,
    ) -> Result<(PendingCommand, oneshot::Receiver<CommandResult>), CommandError> {
        let command_name = command.name.clone();
        let message_id = command.message_id.clone();

        // Priority-aware capacity check. High-priority commands (> 0) can
        // bypass the soft limit but are still rejected at the hard limit.
        let priority = extract_priority(&command.processing_instructions);
        let count = self.in_flight.len();
        if count >= self.config.hard_capacity {
            return Err(CommandError::AtCapacity {
                capacity: self.config.hard_capacity,
            });
        }
        if count >= self.config.capacity && priority <= 0 {
            return Err(CommandError::AtCapacity {
                capacity: self.config.capacity,
            });
        }
        if self.in_flight.contains_key(&message_id) {
            return Err(CommandError::Duplicate { message_id });
        }

        let handlers = self.handlers.read();
        let handler_list = handlers.get_handlers(&command.name).ok_or_else(|| {
            self.record_no_handler(&command_name);
            CommandError::NoHandlerAvailable {
                command_name: command.name.clone(),
            }
        })?;

        if handler_list.is_empty() {
            self.record_no_handler(&command_name);
            return Err(CommandError::NoHandlerAvailable {
                command_name: command.name.clone(),
            });
        }

        // Select a handler.
        let selected = if let Some(ref routing_key) = command.routing_key {
            let hash = simple_hash(&routing_key.0);
            let idx = (hash as usize) % handler_list.len();
            &handler_list[idx]
        } else {
            let counter = self.dispatch_counter.fetch_add(1, Ordering::Relaxed);
            let total_weight: i32 = handler_list.iter().map(|h| h.handler.load_factor).sum();
            let target = (counter % total_weight as u64) as i32;

            let mut cumulative = 0;
            let mut selected = &handler_list[0];
            for entry in handler_list {
                cumulative += entry.handler.load_factor;
                if target < cumulative {
                    selected = entry;
                    break;
                }
            }
            selected
        };

        // Check permits on the selected handler first; if none, fall back to
        // any handler with available permits before giving up.
        let selected = if selected.handler.try_acquire_permit() {
            selected
        } else {
            let fallback = handler_list.iter().find(|e| e.handler.try_acquire_permit());
            match fallback {
                Some(entry) => entry,
                None => {
                    self.record_no_permits(&command_name);
                    return Err(CommandError::NoPermitsAvailable {
                        command_name: command.name.clone(),
                    });
                }
            }
        };

        let target_handler = selected.handler.client_id.clone();
        let now = Instant::now();
        drop(handlers);

        self.record_dispatched(&command_name);

        let (response_tx, response_rx) = oneshot::channel();

        self.in_flight.insert(
            message_id,
            InFlightEntry {
                response_tx,
                target_handler: target_handler.clone(),
                dispatched_at: now,
                command_name: command_name.clone(),
            },
        );

        let pending = PendingCommand {
            command,
            target_handler,
            dispatched_at: now,
        };

        Ok((pending, response_rx))
    }

    /// Returns stats: command name → handler count.
    pub fn handler_stats(&self) -> Vec<(String, usize)> {
        self.handlers.read().handler_stats()
    }

    /// Returns detailed handler info + dispatch metrics per command type.
    pub fn handler_details(&self) -> Vec<MessageTypeDetail> {
        let handlers = self.handlers.read();
        let details = handlers.handler_details();

        details
            .into_iter()
            .map(|(name, handlers)| {
                let snapshot = self
                    .metrics
                    .get(&name)
                    .map(|m| m.value().snapshot())
                    .unwrap_or_else(MetricsSnapshot::empty);
                MessageTypeDetail {
                    name,
                    handlers,
                    metrics: snapshot,
                }
            })
            .collect()
    }

    /// Completes a pending command with a response from the handler.
    /// Removes the in-flight entry, sends the result to the caller, and
    /// records latency/success metrics.
    pub fn complete(&self, request_id: &str, result: CommandResult) {
        if let Some((_, entry)) = self.in_flight.remove(request_id) {
            let duration_us = entry.dispatched_at.elapsed().as_micros() as u64;
            let is_error = result.error_code.is_some();

            if let Some(m) = self.metrics.get(&entry.command_name) {
                m.total_duration_us
                    .fetch_add(duration_us, Ordering::Relaxed);
                if is_error {
                    m.failed.fetch_add(1, Ordering::Relaxed);
                } else {
                    m.succeeded.fetch_add(1, Ordering::Relaxed);
                }
            }

            let _ = entry.response_tx.send(result);
        }
    }

    /// Cancels a single in-flight command (e.g. caller-side timeout abandoned
    /// the receiver). Drops the response sender and records a failed metric.
    pub fn cancel_in_flight(&self, message_id: &str) {
        if let Some((_, entry)) = self.in_flight.remove(message_id) {
            let duration_us = entry.dispatched_at.elapsed().as_micros() as u64;
            if let Some(m) = self.metrics.get(&entry.command_name) {
                m.total_duration_us
                    .fetch_add(duration_us, Ordering::Relaxed);
                m.failed.fetch_add(1, Ordering::Relaxed);
            }
            // Sender is dropped; receiver (if still around) gets RecvError.
        }
    }

    /// Cancels all in-flight commands targeting a specific handler. Each
    /// cancelled command receives a `KRONOSDB-4006` failure. Called when a
    /// handler disconnects. Returns the cancelled message_ids for logging.
    pub fn cancel_for_handler(&self, handler_id: &ClientId) -> Vec<String> {
        let to_cancel: Vec<String> = self
            .in_flight
            .iter()
            .filter(|e| &e.value().target_handler == handler_id)
            .map(|e| e.key().clone())
            .collect();

        let mut cancelled = Vec::with_capacity(to_cancel.len());
        for msg_id in to_cancel {
            if let Some((_, entry)) = self.in_flight.remove(&msg_id) {
                self.fail_entry(
                    entry,
                    error_codes::CONNECTION_TO_HANDLER_LOST,
                    "handler disconnected before responding",
                    &msg_id,
                );
                cancelled.push(msg_id);
            }
        }
        cancelled
    }

    /// Removes in-flight entries older than `timeout`. Each swept entry
    /// receives a `KRONOSDB-4005` failure. Returns the swept message_ids.
    /// Safety net for any leak — the gRPC layer handles per-request timeouts.
    pub fn sweep_timeouts(&self, timeout: Duration) -> Vec<String> {
        let to_sweep: Vec<String> = self
            .in_flight
            .iter()
            .filter(|e| e.value().dispatched_at.elapsed() > timeout)
            .map(|e| e.key().clone())
            .collect();

        let mut swept = Vec::with_capacity(to_sweep.len());
        for msg_id in to_sweep {
            if let Some((_, entry)) = self.in_flight.remove(&msg_id) {
                self.fail_entry(
                    entry,
                    error_codes::COMMAND_TIMEOUT,
                    "command dispatch timed out",
                    &msg_id,
                );
                swept.push(msg_id);
            }
        }
        swept
    }

    /// Returns the number of in-flight commands.
    pub fn in_flight_count(&self) -> usize {
        self.in_flight.len()
    }

    fn fail_entry(&self, entry: InFlightEntry, error_code: &str, message: &str, request_id: &str) {
        let duration_us = entry.dispatched_at.elapsed().as_micros() as u64;
        if let Some(m) = self.metrics.get(&entry.command_name) {
            m.total_duration_us
                .fetch_add(duration_us, Ordering::Relaxed);
            m.failed.fetch_add(1, Ordering::Relaxed);
        }
        let result = CommandResult {
            message_id: String::new(),
            request_id: request_id.to_string(),
            error_code: Some(error_code.to_string()),
            error: Some(ErrorDetail {
                message: message.to_string(),
                location: String::new(),
                details: vec![],
                error_code: error_code.to_string(),
            }),
            payload: None,
            metadata: std::collections::HashMap::new(),
            processing_instructions: vec![],
        };
        let _ = entry.response_tx.send(result);
    }

    /// Records a command completion from the gRPC layer (success/failure + latency).
    /// Called when a response comes back from a handler — used for the
    /// connector-side dispatch path that doesn't go through `complete`.
    pub fn record_completion(&self, command_name: &str, is_error: bool, duration_us: u64) {
        self.get_or_create_metrics(command_name);
        if let Some(m) = self.metrics.get(command_name) {
            m.total_duration_us
                .fetch_add(duration_us, Ordering::Relaxed);
            if is_error {
                m.failed.fetch_add(1, Ordering::Relaxed);
            } else {
                m.succeeded.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    // ── Metrics helpers (cheap — just atomic increments) ────────────

    fn get_or_create_metrics(&self, name: &str) {
        self.metrics.entry(name.to_string()).or_default();
    }

    fn record_dispatched(&self, name: &str) {
        self.get_or_create_metrics(name);
        if let Some(m) = self.metrics.get(name) {
            m.dispatched.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn record_no_handler(&self, name: &str) {
        self.get_or_create_metrics(name);
        if let Some(m) = self.metrics.get(name) {
            m.no_handler.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn record_no_permits(&self, name: &str) {
        self.get_or_create_metrics(name);
        if let Some(m) = self.metrics.get(name) {
            m.no_permits.fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// Extracts the priority value from processing instructions. Returns 0 if not set.
fn extract_priority(instructions: &[ProcessingInstruction]) -> i64 {
    instructions
        .iter()
        .find(|pi| pi.key == ProcessingKey::Priority)
        .and_then(|pi| pi.value.as_ref())
        .map(|v| match v {
            MetadataValue::Number(n) => *n,
            _ => 0,
        })
        .unwrap_or(0)
}

/// Simple hash function for routing key consistent hashing.
fn simple_hash(s: &str) -> u64 {
    let mut hash: u64 = 5381;
    for byte in s.bytes() {
        hash = hash.wrapping_mul(33).wrapping_add(byte as u64);
    }
    hash
}

#[cfg(test)]
mod tests {
    use super::*;

    fn client(id: &str) -> ClientId {
        ClientId(id.to_string())
    }

    fn component(name: &str) -> ComponentName {
        ComponentName(name.to_string())
    }

    fn make_command(name: &str) -> Command {
        Command {
            message_id: format!("msg-{name}"),
            name: name.to_string(),
            timestamp: 0,
            payload: Payload {
                payload_type: name.to_string(),
                revision: "1".to_string(),
                data: vec![],
            },
            metadata: std::collections::HashMap::new(),
            processing_instructions: vec![],
            routing_key: None,
            client_id: client("dispatcher"),
            component_name: component("test"),
        }
    }

    #[test]
    fn dispatch_to_single_handler() {
        let bus = CommandBus::new();
        bus.subscribe(
            "CreateOrder".into(),
            client("node-1"),
            component("order-service"),
            100,
        );
        bus.grant_permits(&client("node-1"), 10);

        let cmd = make_command("CreateOrder");
        let result = bus.dispatch(cmd);
        assert!(result.is_ok());
    }

    #[test]
    fn no_handler_returns_error() {
        let bus = CommandBus::new();
        let cmd = make_command("NonExistent");
        let result = bus.dispatch(cmd);
        assert!(matches!(
            result,
            Err(CommandError::NoHandlerAvailable { .. })
        ));
    }

    #[test]
    fn no_permits_returns_error() {
        let bus = CommandBus::new();
        bus.subscribe(
            "CreateOrder".into(),
            client("node-1"),
            component("order-service"),
            100,
        );
        // No permits granted.

        let cmd = make_command("CreateOrder");
        let result = bus.dispatch(cmd);
        assert!(matches!(
            result,
            Err(CommandError::NoPermitsAvailable { .. })
        ));
    }

    #[test]
    fn load_balancing_round_robin() {
        let bus = CommandBus::new();
        bus.subscribe(
            "CreateOrder".into(),
            client("node-1"),
            component("order-service"),
            100,
        );
        bus.subscribe(
            "CreateOrder".into(),
            client("node-2"),
            component("order-service"),
            100,
        );
        bus.grant_permits(&client("node-1"), 100);
        bus.grant_permits(&client("node-2"), 100);

        for i in 0..100 {
            let mut cmd = make_command("CreateOrder");
            cmd.message_id = format!("cmd-{i}");
            let (_pending, _rx) = bus.dispatch(cmd).unwrap();
        }
    }

    #[test]
    fn routing_key_consistent() {
        let bus = CommandBus::new();
        bus.subscribe(
            "CreateOrder".into(),
            client("node-1"),
            component("order-service"),
            100,
        );
        bus.subscribe(
            "CreateOrder".into(),
            client("node-2"),
            component("order-service"),
            100,
        );
        bus.grant_permits(&client("node-1"), 100);
        bus.grant_permits(&client("node-2"), 100);

        let mut cmd1 = make_command("CreateOrder");
        cmd1.message_id = "cmd-1".into();
        cmd1.routing_key = Some(RoutingKey("order-123".into()));
        let (p1, _) = bus.dispatch(cmd1).unwrap();

        let mut cmd2 = make_command("CreateOrder");
        cmd2.message_id = "cmd-2".into();
        cmd2.routing_key = Some(RoutingKey("order-123".into()));
        let (p2, _) = bus.dispatch(cmd2).unwrap();

        assert_eq!(p1.target_handler, p2.target_handler);
    }

    #[test]
    fn remove_client_cleans_up() {
        let bus = CommandBus::new();
        bus.subscribe(
            "CreateOrder".into(),
            client("node-1"),
            component("order-service"),
            100,
        );
        bus.grant_permits(&client("node-1"), 10);

        bus.remove_client(&client("node-1"));

        let cmd = make_command("CreateOrder");
        let result = bus.dispatch(cmd);
        assert!(matches!(
            result,
            Err(CommandError::NoHandlerAvailable { .. })
        ));
    }

    #[test]
    fn duplicate_detection() {
        let bus = CommandBus::new();
        bus.subscribe(
            "CreateOrder".into(),
            client("node-1"),
            component("order-service"),
            100,
        );
        bus.grant_permits(&client("node-1"), 10);

        let cmd = make_command("CreateOrder");
        let (_pending, _rx) = bus.dispatch(cmd).unwrap();

        let cmd2 = make_command("CreateOrder");
        let result = bus.dispatch(cmd2);
        assert!(matches!(result, Err(CommandError::Duplicate { .. })));
    }

    #[test]
    fn capacity_limit_rejects_at_hard_cap() {
        let config = CommandBusConfig::with_capacity(2);
        let bus = CommandBus::with_config(config);
        bus.subscribe(
            "CreateOrder".into(),
            client("node-1"),
            component("order-service"),
            100,
        );
        bus.grant_permits(&client("node-1"), 100);

        let mut cmd1 = make_command("CreateOrder");
        cmd1.message_id = "cmd-1".into();
        let (_p1, _r1) = bus.dispatch(cmd1).unwrap();

        let mut cmd2 = make_command("CreateOrder");
        cmd2.message_id = "cmd-2".into();
        let (_p2, _r2) = bus.dispatch(cmd2).unwrap();

        assert_eq!(bus.in_flight_count(), 2);

        // Third should be rejected (capacity=2, hard=2 since 2/10=0).
        let mut cmd3 = make_command("CreateOrder");
        cmd3.message_id = "cmd-3".into();
        let result = bus.dispatch(cmd3);
        assert!(matches!(result, Err(CommandError::AtCapacity { .. })));
    }

    #[test]
    fn complete_frees_slot_and_delivers_response() {
        let config = CommandBusConfig::with_capacity(1);
        let bus = CommandBus::with_config(config);
        bus.subscribe(
            "CreateOrder".into(),
            client("node-1"),
            component("order-service"),
            100,
        );
        bus.grant_permits(&client("node-1"), 100);

        let cmd = make_command("CreateOrder");
        let msg_id = cmd.message_id.clone();
        let (_pending, rx) = bus.dispatch(cmd).unwrap();
        assert_eq!(bus.in_flight_count(), 1);

        let response = CommandResult {
            message_id: "resp-1".into(),
            request_id: msg_id.clone(),
            error_code: None,
            error: None,
            payload: None,
            metadata: std::collections::HashMap::new(),
            processing_instructions: vec![],
        };
        bus.complete(&msg_id, response);
        assert_eq!(bus.in_flight_count(), 0);
        // The receiver got the result.
        let got = rx.blocking_recv().expect("response delivered");
        assert!(got.error_code.is_none());

        // Slot freed; dispatch again.
        let mut cmd2 = make_command("CreateOrder");
        cmd2.message_id = "cmd-2".into();
        assert!(bus.dispatch(cmd2).is_ok());
    }

    #[test]
    fn cancel_for_handler_on_disconnect_fails_callers() {
        let bus = CommandBus::new();
        bus.subscribe(
            "CreateOrder".into(),
            client("node-1"),
            component("order-service"),
            100,
        );
        bus.subscribe(
            "ProcessPayment".into(),
            client("node-2"),
            component("payment-service"),
            100,
        );
        bus.grant_permits(&client("node-1"), 100);
        bus.grant_permits(&client("node-2"), 100);

        let mut cmd1 = make_command("CreateOrder");
        cmd1.message_id = "cmd-1".into();
        let (_p1, rx1) = bus.dispatch(cmd1).unwrap();

        let mut cmd2 = make_command("ProcessPayment");
        cmd2.message_id = "cmd-2".into();
        let (_p2, _rx2) = bus.dispatch(cmd2).unwrap();

        assert_eq!(bus.in_flight_count(), 2);

        let cancelled = bus.cancel_for_handler(&client("node-1"));
        assert_eq!(cancelled.len(), 1);
        assert_eq!(cancelled[0], "cmd-1");
        assert_eq!(bus.in_flight_count(), 1);

        let result = rx1.blocking_recv().expect("rx delivered cancellation");
        assert_eq!(
            result.error_code.as_deref(),
            Some(error_codes::CONNECTION_TO_HANDLER_LOST)
        );
    }

    #[test]
    fn remove_client_cancels_in_flight() {
        let bus = CommandBus::new();
        bus.subscribe(
            "CreateOrder".into(),
            client("node-1"),
            component("order-service"),
            100,
        );
        bus.grant_permits(&client("node-1"), 10);

        let cmd = make_command("CreateOrder");
        let (_pending, _rx) = bus.dispatch(cmd).unwrap();
        assert_eq!(bus.in_flight_count(), 1);

        let cancelled = bus.remove_client(&client("node-1"));
        assert_eq!(cancelled.len(), 1);
        assert_eq!(bus.in_flight_count(), 0);
    }

    #[test]
    fn priority_bypasses_soft_limit() {
        // soft=10, hard=11
        let config = CommandBusConfig::with_capacity(10);
        let bus = CommandBus::with_config(config);
        bus.subscribe(
            "CreateOrder".into(),
            client("node-1"),
            component("order-service"),
            100,
        );
        bus.grant_permits(&client("node-1"), 100);

        for i in 0..10 {
            let mut cmd = make_command("CreateOrder");
            cmd.message_id = format!("cmd-{i}");
            bus.dispatch(cmd).unwrap();
        }

        // Low-priority rejected at soft limit.
        let mut cmd_low = make_command("CreateOrder");
        cmd_low.message_id = "cmd-low".into();
        assert!(matches!(
            bus.dispatch(cmd_low),
            Err(CommandError::AtCapacity { .. })
        ));

        // High-priority passes the soft limit.
        let mut cmd_high = make_command("CreateOrder");
        cmd_high.message_id = "cmd-high".into();
        cmd_high.processing_instructions = vec![ProcessingInstruction {
            key: ProcessingKey::Priority,
            value: Some(MetadataValue::Number(10)),
        }];
        assert!(bus.dispatch(cmd_high).is_ok());

        // Now at hard limit (11) — even high-priority is rejected.
        let mut cmd_hard = make_command("CreateOrder");
        cmd_hard.message_id = "cmd-hard".into();
        cmd_hard.processing_instructions = vec![ProcessingInstruction {
            key: ProcessingKey::Priority,
            value: Some(MetadataValue::Number(10)),
        }];
        assert!(matches!(
            bus.dispatch(cmd_hard),
            Err(CommandError::AtCapacity { .. })
        ));
    }

    #[test]
    fn sweep_timeouts_drains_old_entries() {
        let bus = CommandBus::new();
        bus.subscribe(
            "CreateOrder".into(),
            client("node-1"),
            component("order-service"),
            100,
        );
        bus.grant_permits(&client("node-1"), 10);

        let cmd = make_command("CreateOrder");
        let (_pending, rx) = bus.dispatch(cmd).unwrap();

        // Long timeout — nothing swept.
        let swept = bus.sweep_timeouts(Duration::from_secs(3600));
        assert!(swept.is_empty());
        assert_eq!(bus.in_flight_count(), 1);

        // Zero timeout — everything swept and caller fails.
        let swept = bus.sweep_timeouts(Duration::ZERO);
        assert_eq!(swept.len(), 1);
        assert_eq!(bus.in_flight_count(), 0);

        let result = rx.blocking_recv().expect("rx delivered timeout error");
        assert_eq!(
            result.error_code.as_deref(),
            Some(error_codes::COMMAND_TIMEOUT)
        );
    }

    #[test]
    fn cancel_in_flight_drops_sender() {
        let bus = CommandBus::new();
        bus.subscribe(
            "CreateOrder".into(),
            client("node-1"),
            component("order-service"),
            100,
        );
        bus.grant_permits(&client("node-1"), 10);

        let cmd = make_command("CreateOrder");
        let msg_id = cmd.message_id.clone();
        let (_pending, rx) = bus.dispatch(cmd).unwrap();
        assert_eq!(bus.in_flight_count(), 1);

        bus.cancel_in_flight(&msg_id);
        assert_eq!(bus.in_flight_count(), 0);
        // Sender dropped → receiver gets RecvError.
        assert!(rx.blocking_recv().is_err());
    }
}
