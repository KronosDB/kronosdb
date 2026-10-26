//! Commands, queries and subscription queries over the native protocol.
//!
//! These handlers only translate: a frame becomes a hub type (`wire`), the
//! hub does the work, and its answer becomes frames. Handler streams are
//! the one place a request stays open for good: they select over what the
//! hub sends the handler and what the client sends the server, and the
//! hub's session (dropped when the function returns) does the cleanup.

use kronosdb_protocol as p;
use kronosdb_protocol::ErrorCode;
use kronosdb_protocol::net::Failure;
use kronosdb_protocol::net::server::{Inbound, ServerStream, StreamSender};

use super::Ctx;
use super::convert::invalid;
use super::wire::{self, Source};
use crate::auth::policy::Access;
use crate::hub::{QueryStream, SubscriptionFeed, ToCommandHandler, ToQueryHandler, Who};

/// The ack a client asked for with an instruction id. Instructions without
/// one are not acknowledged.
async fn ack(out: &StreamSender, instruction_id: &str) {
    if instruction_id.is_empty() {
        return;
    }
    let _ = out
        .send(&p::Frame::ServerAck(p::ServerAck {
            ack: p::Ack {
                instruction_id,
                success: true,
                error: None,
            },
        }))
        .await;
}

/// Why a handler stream the server ended says it ended.
const ENDED_BY_SERVER: &str = "this handler stream ended because its client's platform stream \
     did, or a newer handler stream of the same client replaced it";

fn load_factor(wire: u32) -> i32 {
    wire.min(i32::MAX as u32) as i32
}

// ── Commands ───────────────────────────────────────────────────────────

pub async fn dispatch_command(ctx: Ctx, stream: ServerStream) -> Result<(), Failure> {
    let ServerStream {
        request,
        mut inbound,
        out,
    } = stream;
    let Ok(p::Frame::DispatchCommand(req)) = request.decode() else {
        return Err(invalid("malformed DispatchCommand"));
    };
    let bus = ctx.bus(req.bus, Access::Write)?;
    if req.command.message_id.is_empty() || req.command.name.is_empty() {
        return Err(invalid("a command needs a message_id and a name"));
    }
    let command = wire::command(Source(request.bytes()), &req.command);
    // The caller going away abandons the dispatch, which frees its slot.
    let result = tokio::select! {
        result = ctx.deps.hub.dispatch_command(&bus, command) => result.map_err(wire::failure)?,
        _ = inbound.cancelled() => return Ok(()),
    };
    let _ = out
        .finish(&p::Frame::CommandResponse(p::CommandResponse {
            result: wire::wire_command_result(&result),
        }))
        .await;
    Ok(())
}

pub async fn open_command_handlers(ctx: Ctx, stream: ServerStream) -> Result<(), Failure> {
    let ServerStream {
        request,
        mut inbound,
        out,
    } = stream;
    let Ok(p::Frame::OpenCommandHandlers(open)) = request.decode() else {
        return Err(invalid("malformed OpenCommandHandlers"));
    };
    if open.client_id.is_empty() {
        return Err(invalid("a handler stream needs a client_id"));
    }
    let bus = ctx.bus(open.bus, Access::Write)?;
    let who = Who {
        client_id: open.client_id.to_string(),
        component_name: open.component_name.to_string(),
    };
    let (mut handlers, mut queue) = ctx.deps.hub.open_command_handlers(bus, who);
    loop {
        tokio::select! {
            _ = handlers.cancelled() => {
                return Err(Failure::new(ErrorCode::UNAVAILABLE, ENDED_BY_SERVER));
            }
            message = queue.recv() => match message {
                Some(ToCommandHandler::Command(command)) => {
                    let frame = p::Frame::HandleCommand(p::HandleCommand {
                        command: wire::wire_command(&command),
                    });
                    if out.send(&frame).await.is_err() {
                        return Ok(());
                    }
                }
                // Replaced by a newer stream of the same client.
                None => return Err(Failure::new(ErrorCode::UNAVAILABLE, ENDED_BY_SERVER)),
            },
            frame = inbound.next() => {
                let Some(frame) = frame else { return Ok(()) };
                match frame.decode() {
                    Ok(p::Frame::SubscribeCommand(s)) => {
                        if s.command.is_empty() {
                            return Err(invalid("a subscription needs a command name"));
                        }
                        handlers.subscribe(s.command, load_factor(s.load_factor));
                        ack(&out, s.instruction_id).await;
                    }
                    Ok(p::Frame::UnsubscribeCommand(u)) => {
                        handlers.unsubscribe(u.command);
                        ack(&out, u.instruction_id).await;
                    }
                    Ok(p::Frame::Credit(c)) => handlers.grant(c.credits as i64),
                    Ok(p::Frame::CommandHandled(h)) => {
                        handlers.complete(wire::command_result(Source(frame.bytes()), &h.result));
                    }
                    Ok(p::Frame::ClientAck(_)) => {}
                    _ => {
                        return Err(invalid(
                            "a command handler stream accepts SubscribeCommand, \
                             UnsubscribeCommand, Credit, CommandHandled and ClientAck",
                        ));
                    }
                }
            }
        }
    }
}

// ── Queries ────────────────────────────────────────────────────────────

pub async fn dispatch_query(ctx: Ctx, stream: ServerStream) -> Result<(), Failure> {
    let ServerStream {
        request,
        mut inbound,
        out,
    } = stream;
    let Ok(p::Frame::DispatchQuery(req)) = request.decode() else {
        return Err(invalid("malformed DispatchQuery"));
    };
    let bus = ctx.bus(req.bus, Access::Read)?;
    if req.query.message_id.is_empty() || req.query.name.is_empty() {
        return Err(invalid("a query needs a message_id and a name"));
    }
    let query = wire::query(Source(request.bytes()), &req.query);
    let results = tokio::select! {
        results = ctx.deps.hub.dispatch_query(&bus, query) => results.map_err(wire::failure)?,
        _ = inbound.cancelled() => return Ok(()),
    };
    send_results(&out, &mut inbound, results).await
}

/// Streams a query's results to the dispatcher until they are all in. The
/// dispatcher cancelling drops the results, which tells the handlers that
/// are still working to stop.
pub async fn send_results(
    out: &StreamSender,
    inbound: &mut Inbound,
    mut results: QueryStream,
) -> Result<(), Failure> {
    loop {
        tokio::select! {
            item = results.next() => match item {
                Some(Ok(result)) => {
                    let frame = p::Frame::QueryResponse(p::QueryResponse {
                        result: wire::wire_query_result(&result),
                    });
                    if out.send(&frame).await.is_err() {
                        return Ok(());
                    }
                }
                Some(Err(fault)) => return Err(wire::failure(fault)),
                None => {
                    let _ = out.finish(&p::Frame::QueryEnd(p::QueryEnd {})).await;
                    return Ok(());
                }
            },
            _ = inbound.cancelled() => return Ok(()),
        }
    }
}

pub async fn open_query_handlers(ctx: Ctx, stream: ServerStream) -> Result<(), Failure> {
    let ServerStream {
        request,
        mut inbound,
        out,
    } = stream;
    let Ok(p::Frame::OpenQueryHandlers(open)) = request.decode() else {
        return Err(invalid("malformed OpenQueryHandlers"));
    };
    if open.client_id.is_empty() {
        return Err(invalid("a handler stream needs a client_id"));
    }
    let bus = ctx.bus(open.bus, Access::Write)?;
    let who = Who {
        client_id: open.client_id.to_string(),
        component_name: open.component_name.to_string(),
    };
    let (mut handlers, mut queue) = ctx.deps.hub.open_query_handlers(bus, who);
    loop {
        tokio::select! {
            _ = handlers.cancelled() => {
                return Err(Failure::new(ErrorCode::UNAVAILABLE, ENDED_BY_SERVER));
            }
            message = queue.recv() => {
                let Some(message) = message else {
                    return Err(Failure::new(ErrorCode::UNAVAILABLE, ENDED_BY_SERVER));
                };
                let sent = match &message {
                    ToQueryHandler::Query(query) => {
                        out.send(&p::Frame::HandleQuery(p::HandleQuery {
                            query: wire::wire_query(query),
                        }))
                        .await
                    }
                    ToQueryHandler::CancelQuery { request_id } => {
                        out.send(&p::Frame::CancelQuery(p::CancelQuery { request_id })).await
                    }
                    ToQueryHandler::Subscription(subscription) => {
                        out.send(&p::Frame::HandleSubscriptionQuery(p::HandleSubscriptionQuery {
                            subscription: wire::wire_subscription(subscription),
                        }))
                        .await
                    }
                    ToQueryHandler::SubscriptionPermits { subscription_id, permits } => {
                        out.send(&p::Frame::SubscriptionQueryPermits(p::SubscriptionQueryPermits {
                            subscription_id,
                            permits: *permits,
                        }))
                        .await
                    }
                    ToQueryHandler::CancelSubscription { subscription_id } => {
                        out.send(&p::Frame::CancelSubscriptionQuery(p::CancelSubscriptionQuery {
                            subscription_id,
                        }))
                        .await
                    }
                };
                if sent.is_err() {
                    return Ok(());
                }
            }
            frame = inbound.next() => {
                let Some(frame) = frame else { return Ok(()) };
                match frame.decode() {
                    Ok(p::Frame::SubscribeQuery(s)) => {
                        if s.query.is_empty() {
                            return Err(invalid("a subscription needs a query name"));
                        }
                        handlers.subscribe(s.query);
                        ack(&out, s.instruction_id).await;
                    }
                    Ok(p::Frame::UnsubscribeQuery(u)) => {
                        handlers.unsubscribe(u.query);
                        ack(&out, u.instruction_id).await;
                    }
                    Ok(p::Frame::Credit(c)) => handlers.grant(c.credits as i64),
                    Ok(p::Frame::QueryHandled(h)) => {
                        handlers.handled(wire::query_result(Source(frame.bytes()), &h.result));
                    }
                    Ok(p::Frame::QueryDone(d)) => handlers.done(d.request_id),
                    Ok(p::Frame::SubscriptionQueryHandled(h)) => {
                        let event = wire::subscription_event(
                            Source(frame.bytes()),
                            h.subscription_id,
                            &h.event,
                        );
                        handlers.subscription_event(h.subscription_id, event);
                    }
                    Ok(p::Frame::ClientAck(_)) => {}
                    _ => {
                        return Err(invalid(
                            "a query handler stream accepts SubscribeQuery, UnsubscribeQuery, \
                             Credit, QueryHandled, QueryDone, SubscriptionQueryHandled and ClientAck",
                        ));
                    }
                }
            }
        }
    }
}

// ── Subscription queries ───────────────────────────────────────────────

pub async fn open_subscription_query(ctx: Ctx, stream: ServerStream) -> Result<(), Failure> {
    let ServerStream {
        request,
        mut inbound,
        out,
    } = stream;
    let Ok(p::Frame::OpenSubscriptionQuery(req)) = request.decode() else {
        return Err(invalid("malformed OpenSubscriptionQuery"));
    };
    let bus = ctx.bus(req.bus, Access::Read)?;
    if req.subscription.subscription_id.is_empty() || req.subscription.query.name.is_empty() {
        return Err(invalid(
            "a subscription query needs a subscription_id and a query name",
        ));
    }
    let subscription = wire::subscription(Source(request.bytes()), &req.subscription);
    let id = subscription.subscription_id.clone();
    let feed = tokio::select! {
        feed = ctx.deps.hub.open_subscription(&bus, subscription) => feed.map_err(wire::failure)?,
        _ = inbound.cancelled() => return Ok(()),
    };
    send_events(&out, &mut inbound, feed, &id).await
}

/// Streams a subscription's events to its subscriber, and passes the update
/// credits it grants on to the feed. Ends after the terminal event, or when
/// the subscriber cancels (which drops the feed and so cancels the
/// subscription).
pub async fn send_events(
    out: &StreamSender,
    inbound: &mut Inbound,
    mut feed: SubscriptionFeed,
    subscription_id: &str,
) -> Result<(), Failure> {
    loop {
        tokio::select! {
            event = feed.next() => match event {
                Some(Ok(event)) => {
                    let terminal = event.is_terminal();
                    let frame = p::Frame::SubscriptionQueryEvent(p::SubscriptionQueryEvent {
                        subscription_id,
                        event: wire::wire_subscription_event(&event),
                    });
                    if terminal {
                        let _ = out.finish(&frame).await;
                        return Ok(());
                    }
                    if out.send(&frame).await.is_err() {
                        return Ok(());
                    }
                }
                Some(Err(fault)) => return Err(wire::failure(fault)),
                // The feed closed without a terminal event: the end.
                None => {
                    let end = crate::hub::SubscriptionEvent::Complete;
                    let _ = out
                        .finish(&p::Frame::SubscriptionQueryEvent(p::SubscriptionQueryEvent {
                            subscription_id,
                            event: wire::wire_subscription_event(&end),
                        }))
                        .await;
                    return Ok(());
                }
            },
            frame = inbound.next() => {
                let Some(frame) = frame else { return Ok(()) };
                match frame.decode() {
                    Ok(p::Frame::Credit(c)) => feed.grant(c.credits as u64),
                    _ => return Err(invalid("a subscription query accepts only Credit and Cancel")),
                }
            }
        }
    }
}
