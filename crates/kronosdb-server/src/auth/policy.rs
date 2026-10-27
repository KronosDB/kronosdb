//! Authorization: what each request needs, and whether a principal's
//! grants cover it.

use super::config::Role;

/// What a request demands of its caller.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Access {
    /// Any verified caller, grants or not (`WhoAmI`).
    Authenticated,
    Read,
    Write,
    Admin,
    /// Internode traffic only.
    Peer,
}

/// What the request is scoped to: the context or bus it is bound to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scope {
    /// An event store context (event store, scheduler).
    Context,
    /// A messaging bus (commands, queries).
    Bus,
    /// Not tied to a context or bus.
    Global,
}

/// What a request needs, by the kind of its first frame. Unknown
/// kinds demand `admin`: a request added later is closed until it is
/// classified here.
pub fn demand(kind: u16) -> (Access, Scope) {
    use kronosdb_protocol as p;
    match kind {
        p::Append::KIND | p::AppendSnapshot::KIND => (Access::Write, Scope::Context),
        p::Source::KIND
        | p::Subscribe::KIND
        | p::GetHead::KIND
        | p::GetTail::KIND
        | p::GetTags::KIND
        | p::GetPositionAt::KIND
        | p::GetSnapshot::KIND
        | p::SnapshottedSource::KIND
        | p::ListSchedules::KIND => (Access::Read, Scope::Context),
        p::ScheduleAppend::KIND | p::CancelSchedule::KIND => (Access::Write, Scope::Context),
        // Binding checks the scope's own rights; the request itself only
        // needs a verified caller.
        p::BindContext::KIND | p::BindBus::KIND | p::Reauthenticate::KIND | p::WhoAmI::KIND => {
            (Access::Authenticated, Scope::Global)
        }
        // Handling commands and dispatching them both change state.
        p::DispatchCommand::KIND | p::OpenCommandHandlers::KIND => (Access::Write, Scope::Bus),
        p::DispatchQuery::KIND | p::OpenSubscriptionQuery::KIND => (Access::Read, Scope::Bus),
        // Answering queries is serving data to others, not reading it.
        p::OpenQueryHandlers::KIND => (Access::Write, Scope::Bus),
        // Lifecycle and processor status: any client with a grant.
        p::GetPlatformInfo::KIND | p::OpenPlatform::KIND => (Access::Read, Scope::Global),
        p::RaftVote::KIND
        | p::RaftAppendEntries::KIND
        | p::RaftInstallSnapshot::KIND
        | p::ForwardWrite::KIND
        | p::TailOpen::KIND
        | p::ForwardAppend::KIND
        | p::GetCursors::KIND
        | p::ForwardCommand::KIND
        | p::ForwardQuery::KIND
        | p::RelaySubscription::KIND => (Access::Peer, Scope::Global),
        _ => (Access::Admin, Scope::Global),
    }
}

fn role_satisfies(role: Role, access: Access) -> bool {
    match access {
        Access::Authenticated => true,
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
/// literal. Enough for `orders-*`, `*@example.com`, `spiffe://prod/*`.
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
        assert!(glob_match("*@example.com", "theo@example.com"));
        assert!(!glob_match("*@example.com", "theo@example.com.evil.io"));
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
        use kronosdb_protocol as p;
        assert_eq!(demand(p::Append::KIND), (Access::Write, Scope::Context));
        assert_eq!(demand(p::Source::KIND), (Access::Read, Scope::Context));
        assert_eq!(demand(p::DispatchQuery::KIND), (Access::Read, Scope::Bus));
        assert_eq!(
            demand(p::ForwardCommand::KIND),
            (Access::Peer, Scope::Global)
        );
        assert_eq!(demand(p::WhoAmI::KIND).0, Access::Authenticated);
        // A kind nothing classified stays closed.
        assert_eq!(demand(0x7ffe).0, Access::Admin);
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
