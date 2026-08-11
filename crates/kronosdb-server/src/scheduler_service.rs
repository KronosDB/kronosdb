//! gRPC surface for scheduled events (ADR-0003).
//!
//! Thin by design: the durability, the exactly-once guard, and the firing all
//! live in `kronosdb_eventstore::scheduler`. This layer routes to a context,
//! checks that the node may write, and translates errors.

use std::sync::Arc;

use tonic::{Request, Response, Status};

use kronosdb_eventstore::error::Error;
use kronosdb_eventstore::raft::cluster::ClusterManager;
use kronosdb_eventstore::scheduler::{self, ScheduleSpec, ScheduledEvent};
use kronosdb_eventstore::store::EventStoreEngine;

use crate::proto::kronosdb::scheduler as pb;

const DEFAULT_CONTEXT: &str = "default";
const CONTEXT_HEADER: &str = "kronosdb-context";

pub struct SchedulerServiceImpl {
    cluster: Arc<ClusterManager>,
    /// Per-context projections, advanced incrementally on each list call.
    /// Without this every ListSchedules would refold the whole log from
    /// position zero.
    projections: Arc<std::sync::Mutex<std::collections::HashMap<String, scheduler::Projection>>>,
}

impl SchedulerServiceImpl {
    pub fn new(cluster: Arc<ClusterManager>) -> Self {
        Self {
            cluster,
            projections: Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
        }
    }

    fn extract_context<T>(request: &Request<T>) -> String {
        request
            .metadata()
            .get(CONTEXT_HEADER)
            .and_then(|value| value.to_str().ok())
            .unwrap_or(DEFAULT_CONTEXT)
            .to_string()
    }

    /// Resolves the local engine, refusing when this node may not write.
    ///
    /// Schedule bookkeeping is appended straight to the local engine, which
    /// skips the routed path's leader forwarding — so unlike a client append,
    /// this cannot be served from a follower. Clients follow the same topology
    /// notifications they already use to find the leader.
    // tonic's `Status` fixes the large error type.
    #[allow(clippy::result_large_err)]
    fn writable_engine<T>(&self, request: &Request<T>) -> Result<Arc<EventStoreEngine>, Status> {
        if !self.cluster.is_writable_leader() {
            // UNAVAILABLE, not FAILED_PRECONDITION: "wrong node, try another"
            // is transient and clients fail over on it — and it must stay
            // distinguishable from cancel's "already resolved", which IS
            // FAILED_PRECONDITION and means the race has a winner.
            return Err(Status::unavailable(
                "scheduling must be directed to the leader node",
            ));
        }
        let context = Self::extract_context(request);
        self.cluster
            .context_manager()
            .get_context(&context)
            .map_err(to_status)
    }
}

#[tonic::async_trait]
impl pb::scheduler_service_server::SchedulerService for SchedulerServiceImpl {
    async fn schedule_append(
        &self,
        request: Request<pb::ScheduleAppendRequest>,
    ) -> Result<Response<pb::ScheduleAppendResponse>, Status> {
        let engine = self.writable_engine(&request)?;
        let request = request.into_inner();

        let tagged = request
            .event
            .ok_or_else(|| Status::invalid_argument("event is required"))?;
        let event = tagged
            .event
            .ok_or_else(|| Status::invalid_argument("event.event is required"))?;

        // Generated when the caller does not supply one; supplying it is what
        // makes retrying a timed-out call safe.
        let token = match request.token.trim() {
            "" => uuid::Uuid::new_v4().to_string(),
            supplied => supplied.to_string(),
        };

        let spec = ScheduleSpec {
            due_ms: request.due_ms,
            target: ScheduledEvent {
                identifier: event.identifier,
                name: event.name,
                version: event.version,
                payload: event.payload,
                metadata: event.metadata.into_iter().collect(),
                tags: tagged
                    .tags
                    .into_iter()
                    .map(|tag| (tag.key, tag.value))
                    .collect(),
            },
        };

        let token_for_task = token.clone();
        tokio::task::spawn_blocking(move || scheduler::schedule(&engine, &token_for_task, &spec))
            .await
            .map_err(|error| Status::internal(format!("scheduler worker panicked: {error}")))?
            .map_err(|error| match error {
                // The token is already in use — the idempotency guard.
                Error::ConsistencyConditionViolated { .. } => {
                    Status::already_exists("a schedule with this token already exists")
                }
                other => to_status(other),
            })?;

        Ok(Response::new(pb::ScheduleAppendResponse { token }))
    }

    async fn cancel_schedule(
        &self,
        request: Request<pb::CancelScheduleRequest>,
    ) -> Result<Response<pb::CancelScheduleResponse>, Status> {
        let engine = self.writable_engine(&request)?;
        let token = request.into_inner().token;

        tokio::task::spawn_blocking(move || scheduler::cancel(&engine, &token))
            .await
            .map_err(|error| Status::internal(format!("scheduler worker panicked: {error}")))?
            .map_err(|error| match error {
                // Already resolved — fired, or cancelled by an earlier call.
                // The guard cannot tell which, so the message does not
                // pretend to; either way nothing was cancelled here, and
                // saying so beats reporting a success that did nothing.
                Error::ConsistencyConditionViolated { .. } => {
                    Status::failed_precondition("this schedule has already fired or been cancelled")
                }
                Error::SnapshotNotFound { .. } => Status::not_found("no such schedule"),
                other => to_status(other),
            })?;

        Ok(Response::new(pb::CancelScheduleResponse {}))
    }

    async fn list_schedules(
        &self,
        request: Request<pb::ListSchedulesRequest>,
    ) -> Result<Response<pb::ListSchedulesResponse>, Status> {
        let context = Self::extract_context(&request);
        let engine = self
            .cluster
            .context_manager()
            .get_context(&context)
            .map_err(to_status)?;

        // Reads are local, so any node can answer.
        let projections = Arc::clone(&self.projections);
        let schedules = tokio::task::spawn_blocking(move || {
            let mut projections = projections.lock().expect("projection map poisoned");
            let projection = projections.entry(context).or_default();
            projection.advance(&engine).map(|()| projection.list())
        })
        .await
        .map_err(|error| Status::internal(format!("scheduler worker panicked: {error}")))?
        .map_err(to_status)?;

        Ok(Response::new(pb::ListSchedulesResponse {
            schedules: schedules
                .into_iter()
                .map(|live| pb::Schedule {
                    token: live.token,
                    due_ms: live.due_ms,
                    event_name: live.target.name,
                })
                .collect(),
        }))
    }
}

// tonic's `Status` fixes the large error type.
#[allow(clippy::result_large_err)]
fn to_status(error: Error) -> Status {
    match error {
        Error::ContextNotFound { name } => Status::not_found(format!("context not found: {name}")),
        Error::ReservedNamespace { detail } => {
            Status::invalid_argument(format!("reserved namespace: {detail}"))
        }
        other => Status::internal(other.to_string()),
    }
}
