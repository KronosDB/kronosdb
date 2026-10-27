//! Scheduler requests: schedules are `$` events in the context's own log.

use kronosdb_eventstore::error::Error;
use kronosdb_eventstore::scheduler::{self, ScheduleSpec, ScheduledEvent};
use kronosdb_protocol as p;
use kronosdb_protocol::ErrorCode;
use kronosdb_protocol::net::Failure;
use kronosdb_protocol::net::server::ServerStream;

use super::Ctx;
use super::convert::{self, invalid};
use crate::auth::policy::{Access, Scope};

fn worker_failed(e: tokio::task::JoinError) -> Failure {
    convert::failure(Error::Internal {
        message: format!("scheduler worker panicked: {e}"),
    })
}

/// The engine of a context the session may write, on the writable leader.
fn writable(
    ctx: &Ctx,
    context: u16,
) -> Result<std::sync::Arc<kronosdb_eventstore::store::EventStoreEngine>, Failure> {
    let name = ctx
        .session
        .resolve(Scope::Context, context, Access::Write)?;
    if !ctx.deps.cluster.is_writable_leader() {
        return Err(Failure::new(
            ErrorCode::UNAVAILABLE,
            "scheduling must be directed to the leader node",
        ));
    }
    ctx.deps
        .cluster
        .context_manager()
        .get_context(&name)
        .map_err(convert::failure)
}

pub async fn schedule_append(ctx: Ctx, stream: ServerStream) -> Result<(), Failure> {
    let frame = stream.request;
    let Ok(p::Frame::ScheduleAppend(req)) = frame.decode() else {
        return Err(invalid("malformed ScheduleAppend"));
    };
    let engine = writable(&ctx, req.context)?;
    let event = req
        .event
        .event()
        .map_err(|e| invalid(format!("the scheduled record is not an event: {e}")))?;
    let token = match req.token.trim() {
        "" => uuid::Uuid::new_v4().to_string(),
        supplied => supplied.to_string(),
    };
    let spec = ScheduleSpec {
        due_ms: req.due,
        target: ScheduledEvent {
            identifier: event.identifier.to_string(),
            name: event.name.to_string(),
            version: event.version.to_string(),
            payload: event.payload.to_vec(),
            metadata: event
                .metadata
                .iter()
                .map(|m| (m.key.to_string(), m.value.to_string()))
                .collect(),
            tags: event
                .tags
                .iter()
                .map(|t| (t.key.to_vec(), t.value.to_vec()))
                .collect(),
        },
    };
    let task_token = token.clone();
    tokio::task::spawn_blocking(move || scheduler::schedule(&engine, &task_token, &spec))
        .await
        .map_err(worker_failed)?
        .map_err(|e| match e {
            Error::ConsistencyConditionViolated { .. } => Failure::new(
                ErrorCode::ALREADY_EXISTS,
                "a schedule with this token already exists",
            ),
            other => convert::failure(other),
        })?;
    let _ = stream
        .out
        .finish(&p::Frame::Scheduled(p::Scheduled { token: &token }))
        .await;
    Ok(())
}

pub async fn cancel_schedule(ctx: Ctx, stream: ServerStream) -> Result<(), Failure> {
    let frame = stream.request;
    let Ok(p::Frame::CancelSchedule(req)) = frame.decode() else {
        return Err(invalid("malformed CancelSchedule"));
    };
    let engine = writable(&ctx, req.context)?;
    let token = req.token.to_string();
    tokio::task::spawn_blocking(move || scheduler::cancel(&engine, &token))
        .await
        .map_err(worker_failed)?
        .map_err(|e| match e {
            Error::ConsistencyConditionViolated { .. } => Failure::new(
                ErrorCode::REJECTED,
                "this schedule has already fired or been cancelled",
            ),
            Error::SnapshotNotFound { .. } => {
                Failure::new(ErrorCode::NOT_FOUND, "no such schedule")
            }
            other => convert::failure(other),
        })?;
    let _ = stream
        .out
        .finish(&p::Frame::ScheduleCancelled(p::ScheduleCancelled {}))
        .await;
    Ok(())
}

pub async fn list_schedules(ctx: Ctx, stream: ServerStream) -> Result<(), Failure> {
    let Ok(p::Frame::ListSchedules(req)) = stream.request.decode() else {
        return Err(invalid("malformed ListSchedules"));
    };
    let name = ctx
        .session
        .resolve(Scope::Context, req.context, Access::Read)?;
    let engine = ctx
        .deps
        .cluster
        .context_manager()
        .get_context(&name)
        .map_err(convert::failure)?;
    let projections = std::sync::Arc::clone(&ctx.deps.schedule_projections);
    let schedules = tokio::task::spawn_blocking(move || {
        let mut projections = projections.lock().expect("projection map poisoned");
        let projection = projections.entry(name.to_string()).or_default();
        projection.advance(&engine).map(|()| projection.list())
    })
    .await
    .map_err(worker_failed)?
    .map_err(convert::failure)?;
    let schedules = schedules
        .iter()
        .map(|live| p::Schedule {
            token: &live.token,
            due: live.due_ms,
            event_name: &live.target.name,
        })
        .collect();
    let _ = stream
        .out
        .finish(&p::Frame::Schedules(p::Schedules { schedules }))
        .await;
    Ok(())
}
