//! The native protocol server: KronosDB's services behind the connection
//! runtime from `kronosdb-protocol`.
//!
//! A connection authenticates once, in its Hello, into a [`Session`]. Every
//! request is then routed by the kind of its first frame to a handler: a
//! plain async function of the connection's context and the request's
//! stream. Authorization is decided here and in the session, so handlers only
//! do their work.

mod convert;
mod eventstore;
mod fabric;
mod internode;
mod messaging;
pub mod peers;
mod platform;
mod scheduler;
mod session;
pub mod tls;
mod wire;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use kronosdb_eventstore::api::EventStore;
use kronosdb_eventstore::raft::cluster::ClusterManager;
use kronosdb_eventstore::raft::transport::RaftNode;
use kronosdb_eventstore::replication::PEER_MAX_FRAME;
use kronosdb_eventstore::replication::service::SegmentReplicationService;
use kronosdb_eventstore::scheduler::Projection;
use kronosdb_protocol as p;
use kronosdb_protocol::ErrorCode;
use kronosdb_protocol::net::server::{Accepted, BoxFuture, PeerInfo, ServerStream, Service};
use kronosdb_protocol::net::{Failure, OwnedFrame};

use crate::auth::policy::{self, Access, Scope};
use crate::auth::{AuthError, Authenticator, Credentials, Principal};
use crate::hub::{Bus, Hub};
pub use session::Session;

/// What the handlers share for the life of the server.
pub struct Deps {
    pub cluster: Arc<ClusterManager>,
    pub auth: Arc<Authenticator>,
    /// The metadata Raft group, for peers' Raft RPCs.
    pub raft: Arc<RaftNode>,
    /// The leader side of segment replication, for peers' Tail streams,
    /// forwarded appends and cursor queries.
    pub replication: Arc<SegmentReplicationService>,
    pub max_snapshot_size: u64,
    /// Commands, queries and the platform.
    pub hub: Arc<Hub>,
    /// The scheduler's per-context view of live schedules, advanced lazily.
    pub schedule_projections: Arc<Mutex<HashMap<String, Projection>>>,
}

/// What every handler gets: the server's dependencies and the connection.
#[derive(Clone)]
pub struct Ctx {
    pub deps: Arc<Deps>,
    pub session: Arc<Session>,
}

impl Ctx {
    /// The store a request names, if the session may use it with `access`.
    pub fn store(&self, context: u16, access: Access) -> Result<Arc<dyn EventStore>, Failure> {
        let name = self.session.resolve(Scope::Context, context, access)?;
        self.deps.cluster.get_store(&name).map_err(convert::failure)
    }

    /// The bus a request names, if the session may use it with `access`.
    pub fn bus(&self, bus: u16, access: Access) -> Result<Arc<Bus>, Failure> {
        let name = self.session.resolve(Scope::Bus, bus, access)?;
        Ok(self.deps.hub.bus(&name))
    }
}

pub struct Native {
    deps: Arc<Deps>,
}

impl Native {
    pub fn new(deps: Deps) -> Self {
        Self {
            deps: Arc::new(deps),
        }
    }
}

type Handled = BoxFuture<Result<(), Failure>>;

/// The handler for a request, by its first frame's kind.
fn route(kind: u16, ctx: Ctx, stream: ServerStream) -> Option<Handled> {
    Some(match kind {
        p::Append::KIND => Box::pin(eventstore::append(ctx, stream)),
        p::Source::KIND => Box::pin(eventstore::source(ctx, stream)),
        p::Subscribe::KIND => Box::pin(eventstore::subscribe(ctx, stream)),
        p::GetHead::KIND => Box::pin(eventstore::get_head(ctx, stream)),
        p::GetTail::KIND => Box::pin(eventstore::get_tail(ctx, stream)),
        p::GetTags::KIND => Box::pin(eventstore::get_tags(ctx, stream)),
        p::GetPositionAt::KIND => Box::pin(eventstore::get_position_at(ctx, stream)),
        p::AppendSnapshot::KIND => Box::pin(eventstore::append_snapshot(ctx, stream)),
        p::GetSnapshot::KIND => Box::pin(eventstore::get_snapshot(ctx, stream)),
        p::SnapshottedSource::KIND => Box::pin(eventstore::snapshotted_source(ctx, stream)),
        p::ScheduleAppend::KIND => Box::pin(scheduler::schedule_append(ctx, stream)),
        p::CancelSchedule::KIND => Box::pin(scheduler::cancel_schedule(ctx, stream)),
        p::ListSchedules::KIND => Box::pin(scheduler::list_schedules(ctx, stream)),
        p::DispatchCommand::KIND => Box::pin(messaging::dispatch_command(ctx, stream)),
        p::OpenCommandHandlers::KIND => Box::pin(messaging::open_command_handlers(ctx, stream)),
        p::DispatchQuery::KIND => Box::pin(messaging::dispatch_query(ctx, stream)),
        p::OpenQueryHandlers::KIND => Box::pin(messaging::open_query_handlers(ctx, stream)),
        p::OpenSubscriptionQuery::KIND => Box::pin(messaging::open_subscription_query(ctx, stream)),
        p::GetPlatformInfo::KIND => Box::pin(platform::get_platform_info(ctx, stream)),
        p::OpenPlatform::KIND => Box::pin(platform::open_platform(ctx, stream)),
        p::ForwardCommand::KIND => Box::pin(fabric::forward_command(ctx, stream)),
        p::ForwardQuery::KIND => Box::pin(fabric::forward_query(ctx, stream)),
        p::RelaySubscription::KIND => Box::pin(fabric::relay_subscription(ctx, stream)),
        p::BindContext::KIND => Box::pin(bind(ctx, stream, Scope::Context)),
        p::BindBus::KIND => Box::pin(bind(ctx, stream, Scope::Bus)),
        p::Reauthenticate::KIND => Box::pin(reauthenticate(ctx, stream)),
        p::WhoAmI::KIND => Box::pin(who_am_i(ctx, stream)),
        p::RaftVote::KIND
        | p::RaftAppendEntries::KIND
        | p::RaftInstallSnapshot::KIND
        | p::ForwardWrite::KIND => Box::pin(internode::raft(ctx, stream)),
        p::TailOpen::KIND => Box::pin(internode::tail(ctx, stream)),
        p::ForwardAppend::KIND => Box::pin(internode::forward_append(ctx, stream)),
        p::GetCursors::KIND => Box::pin(internode::get_cursors(ctx, stream)),
        _ => return None,
    })
}

impl Service for Native {
    type Session = Session;

    fn hello(
        &self,
        hello: OwnedFrame,
        peer: Arc<PeerInfo>,
    ) -> BoxFuture<Result<Accepted<Session>, Failure>> {
        let deps = Arc::clone(&self.deps);
        Box::pin(async move {
            let Ok(p::Frame::Hello(h)) = hello.decode() else {
                return Err(convert::invalid("malformed Hello"));
            };
            let cert = peer.certificates.first().map(|c| c.as_ref());
            let principal = authenticate(&deps.auth, h.credential_kind, h.credential, cert).await?;
            // Peers replicate frames of tens of megabytes; clients do not.
            let max_frame = principal
                .allows(Access::Peer, Scope::Global, "")
                .then_some(PEER_MAX_FRAME);
            Ok(Accepted {
                principal: format!("{}:{}", principal.source, principal.subject),
                session: Arc::new(Session::new(principal)),
                features: 0,
                max_frame,
            })
        })
    }

    fn request(&self, session: &Arc<Session>, stream: ServerStream) -> BoxFuture<()> {
        let ctx = Ctx {
            deps: Arc::clone(&self.deps),
            session: Arc::clone(session),
        };
        let kind = stream.request.kind();
        let out = stream.out.clone();
        let auth = Arc::clone(&ctx.deps.auth);
        // Requests scoped to a context or bus are checked when the handler
        // resolves its id; the rest are checked here.
        let (access, scope) = policy::demand(kind);
        let checked = if scope == Scope::Global {
            ctx.session.check_global(access)
        } else {
            Ok(())
        };
        let handled = match checked {
            Ok(()) => route(kind, ctx, stream),
            Err(failure) => Some(Box::pin(async move { Err(failure) }) as Handled),
        };
        Box::pin(async move {
            let result = match handled {
                Some(handled) => handled.await,
                None => Err(Failure::new(
                    ErrorCode::INVALID_ARGUMENT,
                    format!("frame kind {kind:#06x} does not start a request"),
                )),
            };
            if let Err(failure) = result {
                if failure.code == ErrorCode::PERMISSION_DENIED {
                    auth.record_denied();
                }
                let _ = out.fail(&failure).await;
            }
        })
    }
}

/// Resolves Hello's or Reauthenticate's credentials into a principal.
async fn authenticate(
    auth: &Authenticator,
    kind: p::CredentialKind,
    credential: &str,
    client_cert: Option<&[u8]>,
) -> Result<Arc<Principal>, Failure> {
    if let Some(open) = auth.open_principal() {
        return Ok(open);
    }
    let token = match kind {
        p::CredentialKind::NONE => None,
        p::CredentialKind::TOKEN | p::CredentialKind::JWT => Some(credential),
        other => {
            return Err(convert::invalid(format!(
                "credential kind {} is not supported",
                other.0
            )));
        }
    };
    auth.authenticate(Credentials { token, client_cert })
        .await
        .map_err(|AuthError::Unauthenticated(m)| Failure::new(ErrorCode::UNAUTHENTICATED, m))
}

async fn bind(ctx: Ctx, stream: ServerStream, scope: Scope) -> Result<(), Failure> {
    let reply = match (scope, stream.request.decode()) {
        (Scope::Context, Ok(p::Frame::BindContext(b))) => {
            let rights = ctx.session.rights(scope, b.name)?;
            // Contexts must exist; buses are created on first use.
            ctx.deps
                .cluster
                .get_store(b.name)
                .map_err(convert::failure)?;
            ctx.session.bind(scope, b.id, b.name, rights)?;
            p::Frame::ContextBound(p::ContextBound { id: b.id })
        }
        (Scope::Bus, Ok(p::Frame::BindBus(b))) => {
            let rights = ctx.session.rights(scope, b.name)?;
            ctx.session.bind(scope, b.id, b.name, rights)?;
            p::Frame::BusBound(p::BusBound { id: b.id })
        }
        _ => return Err(convert::invalid("malformed bind request")),
    };
    let _ = stream.out.finish(&reply).await;
    Ok(())
}

async fn reauthenticate(ctx: Ctx, stream: ServerStream) -> Result<(), Failure> {
    let Ok(p::Frame::Reauthenticate(r)) = stream.request.decode() else {
        return Err(convert::invalid("malformed Reauthenticate"));
    };
    // A certificate identity is the connection's and cannot be replaced.
    let principal = authenticate(&ctx.deps.auth, r.credential_kind, r.credential, None).await?;
    let shown = format!("{}:{}", principal.source, principal.subject);
    ctx.session.reauthenticate(principal);
    let _ = stream
        .out
        .finish(&p::Frame::Reauthenticated(p::Reauthenticated {
            principal: &shown,
        }))
        .await;
    Ok(())
}

async fn who_am_i(ctx: Ctx, stream: ServerStream) -> Result<(), Failure> {
    let principal = ctx.session.principal();
    let roles: Vec<Vec<&str>> = principal
        .grants
        .iter()
        .map(|g| g.roles.iter().map(|r| r.as_str()).collect())
        .collect();
    let grants = principal
        .grants
        .iter()
        .zip(&roles)
        .map(|(g, roles)| p::AccessGrant {
            roles: roles.clone(),
            contexts: g.contexts.iter().flatten().map(String::as_str).collect(),
            all_contexts: g.contexts.is_none(),
            buses: g.buses.iter().flatten().map(String::as_str).collect(),
            all_buses: g.buses.is_none(),
        })
        .collect();
    let _ = stream
        .out
        .finish(&p::Frame::Identity(p::Identity {
            subject: &principal.subject,
            source: &principal.source,
            grants,
            authentication_enabled: principal.source != "anonymous",
        }))
        .await;
    Ok(())
}
