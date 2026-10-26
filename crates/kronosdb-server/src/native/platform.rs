//! The platform: client registration, heartbeats, processor status and the
//! instructions the server sends a client.

use std::time::Duration;

use kronosdb_eventstore::raft::handler_registry::HandlerKind;
use kronosdb_protocol as p;
use kronosdb_protocol::ErrorCode;
use kronosdb_protocol::net::Failure;
use kronosdb_protocol::net::server::ServerStream;
use tokio::time::Instant;

use super::Ctx;
use super::convert::invalid;
use super::wire;
use crate::hub::{ClientIdentity, ProcessorReport, SegmentReport, ToClient, TopologyEvent};

fn identity(client: &p::ClientIdentity<'_>) -> ClientIdentity {
    ClientIdentity {
        client_id: client.client_id.to_string(),
        component_name: client.component_name.to_string(),
        version: client.version.to_string(),
    }
}

fn report(status: &p::ProcessorStatus<'_>) -> ProcessorReport {
    ProcessorReport {
        processor_name: status.processor_name.to_string(),
        mode: status.mode.to_string(),
        running: status.running,
        error: status.error,
        streaming: status.streaming,
        token_store_identifier: status.token_store_identifier.to_string(),
        segments: status
            .segments
            .iter()
            .map(|s| SegmentReport {
                segment_id: s.segment_id,
                caught_up: s.caught_up,
                replaying: s.replaying,
                one_part_of: s.one_part_of,
                token_position: s.token_position,
                error_state: s.error_state.to_string(),
            })
            .collect(),
    }
}

pub async fn get_platform_info(ctx: Ctx, stream: ServerStream) -> Result<(), Failure> {
    let Ok(p::Frame::GetPlatformInfo(req)) = stream.request.decode() else {
        return Err(invalid("malformed GetPlatformInfo"));
    };
    let info = ctx
        .deps
        .hub
        .register_client(identity(&req.client))
        .map_err(wire::failure)?;
    let contexts = info.contexts.iter().map(String::as_str).collect();
    let _ = stream
        .out
        .finish(&p::Frame::PlatformInfo(p::PlatformInfo {
            node_name: &info.node_name,
            version: &info.version,
            contexts,
        }))
        .await;
    Ok(())
}

/// The platform stream: the server sends heartbeats, topology changes and
/// processor instructions; the client answers the heartbeats and reports its
/// processors. A client that goes quiet for the heartbeat timeout is gone.
pub async fn open_platform(ctx: Ctx, stream: ServerStream) -> Result<(), Failure> {
    let ServerStream {
        request,
        mut inbound,
        out,
    } = stream;
    let Ok(p::Frame::OpenPlatform(open)) = request.decode() else {
        return Err(invalid("malformed OpenPlatform"));
    };
    let hub = &ctx.deps.hub;
    let (session, mut queue) = hub
        .open_platform(identity(&open.client))
        .map_err(wire::failure)?;
    let client_id = session.client().0.clone();

    let timeout = hub.config.heartbeat_timeout;
    let mut ticker =
        tokio::time::interval(hub.config.heartbeat_interval.max(Duration::from_millis(10)));
    let silence = tokio::time::sleep(timeout);
    tokio::pin!(silence);
    loop {
        tokio::select! {
            message = queue.recv() => {
                let Some(message) = message else {
                    return Err(Failure::new(
                        ErrorCode::UNAVAILABLE,
                        "a newer platform stream of this client replaced this one",
                    ));
                };
                if send(&out, &message).await.is_err() {
                    return Ok(());
                }
            }
            _ = ticker.tick() => {
                if out.send(&p::Frame::ServerHeartbeat(p::ServerHeartbeat {})).await.is_err() {
                    return Ok(());
                }
            }
            frame = inbound.next() => {
                let Some(frame) = frame else {
                    tracing::info!(client_id = %client_id, "platform stream: client closed");
                    return Ok(());
                };
                silence.as_mut().reset(Instant::now() + timeout);
                match frame.decode() {
                    Ok(p::Frame::PlatformHeartbeat(_)) => session.heartbeat(),
                    Ok(p::Frame::ProcessorStatus(status)) => session.report(report(&status)),
                    Ok(p::Frame::InstructionResult(result)) => {
                        tracing::debug!(
                            client_id = %client_id,
                            instruction_id = %result.instruction_id,
                            success = result.success,
                            "platform stream: instruction result"
                        );
                    }
                    Ok(p::Frame::ClientAck(_)) => {}
                    _ => {
                        return Err(invalid(
                            "a platform stream accepts PlatformHeartbeat, ProcessorStatus, \
                             InstructionResult and ClientAck",
                        ));
                    }
                }
            }
            _ = &mut silence => {
                tracing::warn!(
                    client_id = %client_id,
                    timeout_secs = ?timeout,
                    "platform stream: heartbeat timeout; removing the client"
                );
                return Err(Failure::new(
                    ErrorCode::UNAVAILABLE,
                    "no heartbeat within the timeout; reconnect",
                ));
            }
        }
    }
}

async fn send(
    out: &kronosdb_protocol::net::server::StreamSender,
    message: &ToClient,
) -> Result<(), kronosdb_protocol::net::NetError> {
    match message {
        ToClient::Node(info) => {
            out.send(&p::Frame::NodeInfo(p::NodeInfo {
                node_name: &info.node_name,
                version: &info.version,
                contexts: info.contexts.iter().map(String::as_str).collect(),
            }))
            .await
        }
        ToClient::Topology(change) => {
            let change_type = match change.event {
                TopologyEvent::HandlerRegistered => "handler_registered",
                TopologyEvent::HandlerDeregistered => "handler_deregistered",
            };
            let handler_kind = match change.kind {
                HandlerKind::Command => "command",
                HandlerKind::Query => "query",
            };
            out.send(&p::Frame::TopologyChange(p::TopologyChange {
                change_type,
                message_type: &change.message_type,
                handler_kind,
                client_id: &change.client_id,
                component_name: &change.component_name,
            }))
            .await
        }
        ToClient::Instruction(instruction) => {
            out.send(&p::Frame::ProcessorInstruction(p::ProcessorInstruction {
                instruction_id: &instruction.instruction_id,
                action: wire::wire_action(instruction.action),
                processor_name: &instruction.processor_name,
                segment: instruction.segment,
            }))
            .await
        }
        ToClient::ReconnectRequested => {
            out.send(&p::Frame::RequestReconnect(p::RequestReconnect {}))
                .await
        }
    }
}
