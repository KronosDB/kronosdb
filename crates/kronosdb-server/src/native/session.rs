//! One connection's state: who it is, and the contexts and buses it bound.
//!
//! Rights are worked out when a scope is bound (and again on
//! reauthentication), so a request checks a flag instead of matching the
//! principal's grants every call.

use std::sync::Arc;

use kronosdb_protocol::ErrorCode;
use kronosdb_protocol::net::Failure;
use parking_lot::RwLock;

use crate::auth::Principal;
use crate::auth::policy::{Access, Scope};

/// What the principal may do in one bound context or bus.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Rights {
    pub read: bool,
    pub write: bool,
    pub admin: bool,
}

impl Rights {
    pub fn of(principal: &Principal, scope: Scope, name: &str) -> Self {
        Self {
            read: principal.allows(Access::Read, scope, name),
            write: principal.allows(Access::Write, scope, name),
            admin: principal.allows(Access::Admin, scope, name),
        }
    }

    pub fn allow(&self, access: Access) -> bool {
        match access {
            Access::Authenticated => true,
            Access::Read => self.read,
            Access::Write => self.write,
            Access::Admin => self.admin,
            Access::Peer => false,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Binding {
    pub name: Arc<str>,
    pub rights: Rights,
}

/// Ids name contexts and buses on the wire; a table indexed by id resolves
/// them without hashing.
#[derive(Debug, Default)]
struct Bindings {
    slots: Vec<Option<Binding>>,
}

impl Bindings {
    fn get(&self, id: u16) -> Option<&Binding> {
        self.slots.get(id as usize).and_then(Option::as_ref)
    }

    fn bind(&mut self, id: u16, binding: Binding) -> Result<(), Failure> {
        let slot = id as usize;
        if self.slots.len() <= slot {
            self.slots.resize(slot + 1, None);
        }
        match &self.slots[slot] {
            Some(existing) if existing.name != binding.name => Err(Failure::new(
                ErrorCode::INVALID_ARGUMENT,
                format!(
                    "id {id} is already bound to {:?}; bind {:?} to another id",
                    existing.name, binding.name
                ),
            )),
            _ => {
                self.slots[slot] = Some(binding);
                Ok(())
            }
        }
    }

    fn recompute(&mut self, principal: &Principal, scope: Scope) {
        for binding in self.slots.iter_mut().flatten() {
            binding.rights = Rights::of(principal, scope, &binding.name);
        }
    }
}

pub struct Session {
    principal: RwLock<Arc<Principal>>,
    contexts: RwLock<Bindings>,
    buses: RwLock<Bindings>,
}

pub const DEFAULT_SCOPE: &str = "default";

impl Session {
    pub fn new(principal: Arc<Principal>) -> Self {
        let mut contexts = Bindings::default();
        let mut buses = Bindings::default();
        let _ = contexts.bind(
            0,
            Binding {
                name: DEFAULT_SCOPE.into(),
                rights: Rights::of(&principal, Scope::Context, DEFAULT_SCOPE),
            },
        );
        let _ = buses.bind(
            0,
            Binding {
                name: DEFAULT_SCOPE.into(),
                rights: Rights::of(&principal, Scope::Bus, DEFAULT_SCOPE),
            },
        );
        Self {
            principal: RwLock::new(principal),
            contexts: RwLock::new(contexts),
            buses: RwLock::new(buses),
        }
    }

    pub fn principal(&self) -> Arc<Principal> {
        Arc::clone(&self.principal.read())
    }

    /// Replaces the principal and recomputes every bound scope's rights.
    pub fn reauthenticate(&self, principal: Arc<Principal>) {
        self.contexts.write().recompute(&principal, Scope::Context);
        self.buses.write().recompute(&principal, Scope::Bus);
        *self.principal.write() = principal;
    }

    fn bindings(&self, scope: Scope) -> &RwLock<Bindings> {
        match scope {
            Scope::Bus => &self.buses,
            _ => &self.contexts,
        }
    }

    /// The principal's rights on `name`, if it may read there at all.
    /// Checked before anything else about the scope, so a caller without
    /// rights learns nothing, not even whether it exists.
    pub fn rights(&self, scope: Scope, name: &str) -> Result<Rights, Failure> {
        let principal = self.principal();
        let rights = Rights::of(&principal, scope, name);
        if rights.read {
            Ok(rights)
        } else {
            Err(denied(&principal, Access::Read, scope, name))
        }
    }

    /// Binds `id` to `name` in `scope` with rights from [`Session::rights`].
    pub fn bind(&self, scope: Scope, id: u16, name: &str, rights: Rights) -> Result<(), Failure> {
        self.bindings(scope).write().bind(
            id,
            Binding {
                name: name.into(),
                rights,
            },
        )
    }

    /// The name bound to `id`, if the principal has `access` there.
    pub fn resolve(&self, scope: Scope, id: u16, access: Access) -> Result<Arc<str>, Failure> {
        let bindings = self.bindings(scope).read();
        let Some(binding) = bindings.get(id) else {
            let what = if scope == Scope::Bus {
                "bus"
            } else {
                "context"
            };
            return Err(Failure::new(
                ErrorCode::INVALID_ARGUMENT,
                format!("no {what} is bound to id {id}; bind it first"),
            ));
        };
        if binding.rights.allow(access) {
            Ok(Arc::clone(&binding.name))
        } else {
            Err(denied(&self.principal(), access, scope, &binding.name))
        }
    }

    /// Whether the principal has `access` outside any context or bus.
    pub fn check_global(&self, access: Access) -> Result<(), Failure> {
        let principal = self.principal();
        if principal.allows(access, Scope::Global, "") {
            Ok(())
        } else {
            Err(denied(&principal, access, Scope::Global, ""))
        }
    }
}

fn denied(principal: &Principal, access: Access, scope: Scope, name: &str) -> Failure {
    let on = match scope {
        Scope::Context => format!(" on context {name:?}"),
        Scope::Bus => format!(" on bus {name:?}"),
        Scope::Global => String::new(),
    };
    Failure::new(
        ErrorCode::PERMISSION_DENIED,
        format!(
            "{} ({}) lacks {access:?} access{on}",
            principal.subject, principal.source
        ),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::config::Role;
    use crate::auth::policy::Grant;

    fn principal(roles: Vec<Role>, contexts: Option<Vec<String>>) -> Arc<Principal> {
        Arc::new(Principal {
            subject: "ci".into(),
            source: "token".into(),
            grants: vec![Grant {
                roles,
                contexts,
                buses: None,
            }],
        })
    }

    #[test]
    fn rights_follow_the_bound_scope() {
        let session = Session::new(principal(vec![Role::Write], Some(vec!["orders-*".into()])));
        // `default` is bound from the start but this principal cannot use it.
        assert!(session.resolve(Scope::Context, 0, Access::Read).is_err());
        let rights = session.rights(Scope::Context, "orders-eu").unwrap();
        session
            .bind(Scope::Context, 1, "orders-eu", rights)
            .unwrap();
        assert_eq!(
            &*session.resolve(Scope::Context, 1, Access::Write).unwrap(),
            "orders-eu"
        );
        let denied = session
            .resolve(Scope::Context, 1, Access::Admin)
            .unwrap_err();
        assert_eq!(denied.code, ErrorCode::PERMISSION_DENIED);
        let unbound = session
            .resolve(Scope::Context, 2, Access::Read)
            .unwrap_err();
        assert_eq!(unbound.code, ErrorCode::INVALID_ARGUMENT);
        assert!(session.rights(Scope::Context, "billing").is_err());
        let other = session.rights(Scope::Context, "orders-us").unwrap();
        assert!(
            session.bind(Scope::Context, 1, "orders-us", other).is_err(),
            "no rebinding"
        );
        assert!(
            session.bind(Scope::Context, 1, "orders-eu", rights).is_ok(),
            "same name is fine"
        );
    }

    #[test]
    fn reauthentication_recomputes_rights() {
        let session = Session::new(principal(vec![Role::Read], None));
        assert!(session.resolve(Scope::Context, 0, Access::Write).is_err());
        session.reauthenticate(principal(vec![Role::Write], None));
        assert!(session.resolve(Scope::Context, 0, Access::Write).is_ok());
    }
}
