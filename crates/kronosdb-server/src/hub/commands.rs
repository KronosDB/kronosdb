//! Dispatching a command to its handler and waiting for the answer.

use std::time::{Duration, Instant};

use kronosdb_messaging::command::{Command, CommandError, CommandResult};
use kronosdb_messaging::engine::MessagingEngine;
use kronosdb_messaging::error_codes;
use kronosdb_messaging::types::{ClientId, ErrorDetail};

use super::Hub;
use super::bus::Bus;
use super::routing::{CommandRoute, effective_timeout};
use super::types::{Fault, FaultKind, ToCommandHandler};

/// Frees a command's in-flight slot if the dispatch is abandoned before the
/// handler answers: the caller gave up, or the future was dropped.
struct InFlight<'a> {
    engine: &'a MessagingEngine,
    message_id: Option<String>,
}

impl InFlight<'_> {
    /// The command is settled; nothing is left to free.
    fn settled(&mut self) {
        self.message_id = None;
    }
}

impl Drop for InFlight<'_> {
    fn drop(&mut self) {
        if let Some(id) = &self.message_id {
            self.engine.command_bus().cancel_in_flight(id);
        }
    }
}

/// Why the bus would not take a command, as a fault.
pub(super) fn command_fault(error: CommandError) -> Fault {
    let kind = match &error {
        CommandError::NoHandlerAvailable { .. } | CommandError::NoPermitsAvailable { .. } => {
            FaultKind::Unavailable
        }
        CommandError::Duplicate { .. } => FaultKind::AlreadyExists,
        CommandError::AtCapacity { .. } => FaultKind::ResourceExhausted,
    };
    Fault::new(kind, error.to_string())
}

/// What a caller gets when the handler does not answer in time. The same
/// result the bus's own sweep gives a leaked command.
pub(super) fn timed_out(request_id: &str) -> CommandResult {
    CommandResult {
        message_id: String::new(),
        request_id: request_id.to_string(),
        error_code: Some(error_codes::COMMAND_TIMEOUT.to_string()),
        error: Some(ErrorDetail {
            message: "command dispatch timed out".to_string(),
            location: String::new(),
            details: vec![],
            error_code: error_codes::COMMAND_TIMEOUT.to_string(),
        }),
        payload: None,
        metadata: Default::default(),
        processing_instructions: vec![],
    }
}

impl Hub {
    /// Sends a command to the handler its routing key or the bus's balance
    /// picks — here, or on the node that has it — and returns the handler's
    /// result. A handler that fails, vanishes or takes too long is an `Ok`
    /// result with the error code that says so; a fault means the command
    /// never reached one.
    pub async fn dispatch_command(
        &self,
        bus: &Bus,
        command: Command,
    ) -> Result<CommandResult, Fault> {
        let timeout = effective_timeout(
            &command.processing_instructions,
            self.config.command_timeout,
        );
        // Location-aware routing (ADR-0007): selection runs against the
        // replicated handler table. A remote owner means the command never
        // touches the local bus; the owning node acquires the permit and
        // delivers.
        let started = Instant::now();
        let route = self
            .router
            .command_settled(
                &bus.name,
                &command.name,
                command.routing_key.as_ref().map(|k| k.0.as_str()),
                tokio::time::Instant::from_std(started) + timeout,
            )
            .await
            .ok_or_else(|| {
                Fault::unavailable(format!(
                    "no handler for keyed command '{}' is in the routing table yet; \
                     retry once its registration has replicated",
                    command.name
                ))
            })?;
        if let CommandRoute::Remote { client_id, node_id } = route {
            let timeout = timeout.saturating_sub(started.elapsed());
            return self
                .peers
                .forward_command(node_id, &bus.name, client_id, command, timeout)
                .await;
        }
        let message_id = command.message_id.clone();
        // Selects a handler, takes a permit, and files the in-flight entry
        // that holds the caller's response sender. A saturated handler makes
        // this wait (bounded by the command timeout) for a flow-control
        // grant instead of failing at once.
        let commands = bus.engine.command_bus();
        let timeout_left = timeout.saturating_sub(started.elapsed());
        let dispatched = match route {
            // A keyed command whose ring owner is local goes to exactly that
            // handler: the global ring's decision, not a local re-selection.
            CommandRoute::Local { client_id } => {
                commands
                    .dispatch_to_wait(command, &ClientId(client_id), timeout_left)
                    .await
            }
            _ => commands.dispatch_wait(command, timeout_left).await,
        };
        let (pending, response_rx) = dispatched.map_err(command_fault)?;
        self.await_handler(bus, pending, response_rx, message_id, timeout, started)
            .await
    }

    /// A command another node chose this node's `target` handler for. The
    /// choice is made; this node only takes the permit and delivers.
    pub async fn dispatch_forwarded_command(
        &self,
        bus: &Bus,
        target: ClientId,
        command: Command,
        budget: Duration,
    ) -> Result<CommandResult, Fault> {
        let started = Instant::now();
        let message_id = command.message_id.clone();
        let (pending, response_rx) = bus
            .engine
            .command_bus()
            .dispatch_to_wait(command, &target, budget)
            .await
            .map_err(command_fault)?;
        self.await_handler(bus, pending, response_rx, message_id, budget, started)
            .await
    }

    /// Delivers a dispatched command and waits for its result. The timeout
    /// is one budget for the whole dispatch, permit wait included.
    async fn await_handler(
        &self,
        bus: &Bus,
        pending: kronosdb_messaging::command::PendingCommand,
        response_rx: tokio::sync::oneshot::Receiver<CommandResult>,
        message_id: String,
        timeout: Duration,
        started: Instant,
    ) -> Result<CommandResult, Fault> {
        let mut in_flight = InFlight {
            engine: &bus.engine,
            message_id: Some(message_id),
        };
        let handler = pending.target_handler;
        if bus
            .to_command_handler(&handler.0, ToCommandHandler::Command(pending.command))
            .await
            .is_err()
        {
            return Err(Fault::unavailable(format!(
                "handler '{}' has no stream on this node",
                handler.0
            )));
        }
        let remaining = timeout.saturating_sub(started.elapsed());
        match tokio::time::timeout(remaining, response_rx).await {
            Ok(Ok(result)) => {
                in_flight.settled();
                Ok(result)
            }
            // The bus dropped the sender without an answer: the entry was
            // taken out from under us.
            Ok(Err(_)) => {
                in_flight.settled();
                Err(Fault::unavailable("handler disconnected before responding"))
            }
            Err(_) => {
                let id = in_flight.message_id.clone().unwrap_or_default();
                // Dropping the guard frees the slot.
                Ok(timed_out(&id))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bus_refusals_are_faults_of_the_right_kind() {
        let kind = |e| command_fault(e).kind;
        assert_eq!(
            kind(CommandError::NoHandlerAvailable {
                command_name: "x".into()
            }),
            FaultKind::Unavailable
        );
        assert_eq!(
            kind(CommandError::NoPermitsAvailable {
                command_name: "x".into()
            }),
            FaultKind::Unavailable
        );
        assert_eq!(
            kind(CommandError::Duplicate {
                message_id: "x".into()
            }),
            FaultKind::AlreadyExists
        );
        assert_eq!(
            kind(CommandError::AtCapacity { capacity: 1 }),
            FaultKind::ResourceExhausted
        );
    }

    #[test]
    fn a_timed_out_command_carries_the_timeout_code() {
        let result = timed_out("cmd-1");
        assert_eq!(result.request_id, "cmd-1");
        assert_eq!(
            result.error_code.as_deref(),
            Some(error_codes::COMMAND_TIMEOUT)
        );
        assert!(result.error.is_some() && result.payload.is_none());
    }
}
