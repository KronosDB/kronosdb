//! Authorization: what each gRPC method needs, and whether a principal's
//! grants cover it.

use super::config::Role;

/// What a gRPC method demands of its caller.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Access {
    /// No credential needed (health probes).
    Public,
    /// Any verified caller, grants or not (`WhoAmI`).
    Authenticated,
    Read,
    Write,
    Admin,
    /// Internode traffic only.
    Peer,
}

/// Which routing header scopes the method.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scope {
    /// `kronosdb-context` (event store, scheduler).
    Context,
    /// `kronosdb-bus` (commands, queries).
    Bus,
    /// Not tied to a context or bus.
    Global,
}

/// Classifies a gRPC path (`/package.Service/Method`). Anything unrecognized
/// demands `admin`: a service added later is closed until it is classified.
pub fn classify(path: &str) -> (Access, Scope) {
    let mut parts = path.trim_start_matches('/').splitn(2, '/');
    let service = parts.next().unwrap_or("");
    let method = parts.next().unwrap_or("");

    match service {
        "grpc.health.v1.Health" => (Access::Public, Scope::Global),
        "kronosdb.eventstore.EventStore" => match method {
            "Append" | "AppendSnapshot" => (Access::Write, Scope::Context),
            "Source" | "Stream" | "GetHead" | "GetTail" | "GetTags" | "GetSequenceAt"
            | "SnapshottedSource" | "GetSnapshot" => (Access::Read, Scope::Context),
            _ => (Access::Admin, Scope::Context),
        },
        "kronosdb.scheduler.SchedulerService" => match method {
            "ListSchedules" => (Access::Read, Scope::Context),
            "ScheduleAppend" | "CancelSchedule" => (Access::Write, Scope::Context),
            _ => (Access::Admin, Scope::Context),
        },
        "kronosdb.command.CommandService" => match method {
            // Handling commands and dispatching them both change state.
            "OpenStream" | "Dispatch" => (Access::Write, Scope::Bus),
            _ => (Access::Admin, Scope::Bus),
        },
        "kronosdb.query.QueryService" => match method {
            "Query" | "Subscription" => (Access::Read, Scope::Bus),
            // Answering queries is serving data to others, not reading it.
            "OpenStream" => (Access::Write, Scope::Bus),
            _ => (Access::Admin, Scope::Bus),
        },
        "kronosdb.platform.PlatformService" => match method {
            "WhoAmI" => (Access::Authenticated, Scope::Global),
            // Lifecycle/heartbeats: any client with a grant.
            _ => (Access::Read, Scope::Global),
        },
        "kronosdb.raft.RaftTransport"
        | "kronosdb.replication.SegmentReplication"
        | "kronosdb.fabric.MessagingFabric" => (Access::Peer, Scope::Global),
        _ => (Access::Admin, Scope::Global),
    }
}

fn role_satisfies(role: Role, access: Access) -> bool {
    match access {
        Access::Public | Access::Authenticated => true,
        Access::Read => matches!(role, Role::Read | Role::Write | Role::Admin),
        Access::Write => matches!(role, Role::Write | Role::Admin),
        Access::Admin => role == Role::Admin,
        Access::Peer => role == Role::Peer,
    }
}

/// Roles plus the contexts/buses they apply to (`None` = everywhere).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Grant {
    pub roles: Vec<Role>,
    pub contexts: Option<Vec<String>>,
    pub buses: Option<Vec<String>>,
}

impl Grant {
    pub fn unscoped(roles: Vec<Role>) -> Self {
        Self {
            roles,
            contexts: None,
            buses: None,
        }
    }

    /// `target` is the context or bus name the request addresses.
    pub fn allows(&self, access: Access, scope: Scope, target: &str) -> bool {
        if !self.roles.iter().any(|r| role_satisfies(*r, access)) {
            return false;
        }
        let patterns = match scope {
            Scope::Context => &self.contexts,
            Scope::Bus => &self.buses,
            // A grant narrowed to some contexts still lets its holder
            // connect; `admin` and `peer` are cluster-wide powers and must
            // come from a grant that isn't narrowed.
            Scope::Global => {
                return match access {
                    Access::Admin | Access::Peer => self.contexts.is_none() && self.buses.is_none(),
                    _ => true,
                };
            }
        };
        match patterns {
            None => true,
            Some(patterns) => patterns.iter().any(|p| glob_match(p, target)),
        }
    }
}

/// `*` matches any run of characters (including none); everything else is
/// literal. Enough for `orders-*`, `*@karma.life`, `spiffe://prod/*`.
pub fn glob_match(pattern: &str, text: &str) -> bool {
    let (p, t) = (pattern.as_bytes(), text.as_bytes());
    let (mut pi, mut ti) = (0, 0);
    let mut backtrack: Option<(usize, usize)> = None;
    while ti < t.len() {
        if pi < p.len() && p[pi] == b'*' {
            backtrack = Some((pi, ti));
            pi += 1;
        } else if pi < p.len() && p[pi] == t[ti] {
            pi += 1;
            ti += 1;
        } else if let Some((star, matched)) = backtrack {
            // Let the last `*` swallow one more byte and retry.
            backtrack = Some((star, matched + 1));
            pi = star + 1;
            ti = matched + 1;
        } else {
            return false;
        }
    }
    p[pi..].iter().all(|&b| b == b'*')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn glob() {
        assert!(glob_match("*", ""));
        assert!(glob_match("*", "anything"));
        assert!(glob_match("orders-*", "orders-eu"));
        assert!(glob_match("orders-*", "orders-"));
        assert!(!glob_match("orders-*", "billing-eu"));
        assert!(glob_match("*@karma.life", "theo@karma.life"));
        assert!(!glob_match("*@karma.life", "theo@karma.life.evil.io"));
        assert!(glob_match(
            "spiffe://prod/*/sa/orders",
            "spiffe://prod/ns/x/sa/orders"
        ));
        assert!(glob_match("a*b*c", "aXXbYYbZc"));
        assert!(!glob_match("a*b*c", "aXXbYY"));
        assert!(glob_match("exact", "exact"));
        assert!(!glob_match("exact", "exact-not"));
        assert!(!glob_match("", "x"));
    }

    #[test]
    fn classification() {
        assert_eq!(
            classify("/kronosdb.eventstore.EventStore/Append"),
            (Access::Write, Scope::Context)
        );
        assert_eq!(
            classify("/kronosdb.eventstore.EventStore/Source"),
            (Access::Read, Scope::Context)
        );
        assert_eq!(
            classify("/kronosdb.query.QueryService/Query"),
            (Access::Read, Scope::Bus)
        );
        assert_eq!(
            classify("/kronosdb.raft.RaftTransport/Vote"),
            (Access::Peer, Scope::Global)
        );
        assert_eq!(
            classify("/kronosdb.platform.PlatformService/WhoAmI").0,
            Access::Authenticated
        );
        assert_eq!(
            classify("/grpc.health.v1.Health/Check"),
            (Access::Public, Scope::Global)
        );
        // Unknown services and unknown methods on known services stay closed.
        assert_eq!(classify("/some.New/Thing").0, Access::Admin);
        assert_eq!(
            classify("/kronosdb.eventstore.EventStore/Truncate").0,
            Access::Admin
        );
    }

    #[test]
    fn role_hierarchy() {
        let read = Grant::unscoped(vec![Role::Read]);
        let write = Grant::unscoped(vec![Role::Write]);
        let admin = Grant::unscoped(vec![Role::Admin]);
        let peer = Grant::unscoped(vec![Role::Peer]);

        assert!(read.allows(Access::Read, Scope::Context, "default"));
        assert!(!read.allows(Access::Write, Scope::Context, "default"));
        assert!(write.allows(Access::Read, Scope::Context, "default"));
        assert!(admin.allows(Access::Write, Scope::Bus, "default"));
        // peer and admin never imply each other.
        assert!(!admin.allows(Access::Peer, Scope::Global, ""));
        assert!(!peer.allows(Access::Read, Scope::Context, "default"));
        assert!(peer.allows(Access::Peer, Scope::Global, ""));
    }

    #[test]
    fn scoped_grants() {
        let grant = Grant {
            roles: vec![Role::Admin],
            contexts: Some(vec!["orders-*".into()]),
            buses: Some(vec![]),
        };
        assert!(grant.allows(Access::Write, Scope::Context, "orders-eu"));
        assert!(!grant.allows(Access::Write, Scope::Context, "billing"));
        assert!(!grant.allows(Access::Read, Scope::Bus, "default"));
        // Can connect, but a narrowed grant confers no cluster-wide power.
        assert!(grant.allows(Access::Read, Scope::Global, ""));
        assert!(!grant.allows(Access::Admin, Scope::Global, ""));
    }
}
