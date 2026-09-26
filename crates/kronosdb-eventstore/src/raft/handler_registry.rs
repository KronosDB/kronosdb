//! Replicated messaging-handler registry (ADR-0007 Tier 2).
//!
//! The routing table is the applied state of `RegisterHandler` /
//! `DeregisterHandler` / `DeregisterClient` / `ClearNodeHandlers` control-
//! plane entries: which handler instances exist for a (bus, kind, message
//! type), and which node each is connected to. Every node applies the same
//! entries in the same order, so every node holds an identical, linearizable
//! view — the property that makes cross-node dispatch deterministic.
//!
//! This module is deliberately messaging-agnostic: rows are opaque strings
//! to the control plane. Ring construction and handler selection live in
//! the server, on top of `lookup()` + `generation()`.
//!
//! Registrations are ephemeral by design. They are cleaned by:
//! - explicit deregistration (unsubscribe, client disconnect),
//! - `ClearNodeHandlers`, written by each node at startup so rows stranded
//!   by a crash never outlive the restart, and
//! - membership diffs — rows owned by a node that left the cluster drop
//!   when the membership entry applies.

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

use super::types::NodeId;

/// Which bus a registration belongs to: command handlers and query handlers
/// are separate namespaces even for the same message-type string.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum HandlerKind {
    Command,
    Query,
}

/// One replicated handler registration, as carried in Raft entries and
/// metadata snapshots.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HandlerRegistration {
    pub bus: String,
    pub kind: HandlerKind,
    pub message_type: String,
    pub client_id: String,
    pub node_id: NodeId,
    pub load_factor: i32,
}

/// A handler as seen by dispatch: who can process the message and where
/// they are connected.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegisteredHandler {
    pub client_id: String,
    pub node_id: NodeId,
    pub load_factor: i32,
}

/// The handlers registered for one message type, sorted by client id and
/// shared: a lookup hands out the `Arc`, a mutation replaces it. Every
/// dispatch reads rows; registrations are rare.
pub type HandlerRows = Arc<[RegisteredHandler]>;

#[derive(Default)]
struct TypeRows {
    command: Option<HandlerRows>,
    query: Option<HandlerRows>,
}

impl TypeRows {
    fn get(&self, kind: HandlerKind) -> Option<&HandlerRows> {
        match kind {
            HandlerKind::Command => self.command.as_ref(),
            HandlerKind::Query => self.query.as_ref(),
        }
    }

    fn slot(&mut self, kind: HandlerKind) -> &mut Option<HandlerRows> {
        match kind {
            HandlerKind::Command => &mut self.command,
            HandlerKind::Query => &mut self.query,
        }
    }

    fn is_empty(&self) -> bool {
        self.command.is_none() && self.query.is_none()
    }
}

fn sorted_rows(mut rows: Vec<RegisteredHandler>) -> Option<HandlerRows> {
    if rows.is_empty() {
        return None;
    }
    rows.sort_by(|a, b| a.client_id.cmp(&b.client_id));
    Some(Arc::from(rows))
}

fn empty_rows() -> HandlerRows {
    static EMPTY: std::sync::OnceLock<HandlerRows> = std::sync::OnceLock::new();
    Arc::clone(EMPTY.get_or_init(|| Arc::from(Vec::new())))
}

/// The applied routing table. Written only by the Raft state machine
/// (single apply thread), read concurrently by dispatch paths.
#[derive(Default)]
pub struct HandlerRoutingTable {
    /// bus → message type → rows per kind. Two borrowed lookups per
    /// dispatch, no key allocated.
    inner: RwLock<HashMap<String, HashMap<String, TypeRows>>>,
    /// Bumped on every mutation. Readers cache derived structures (rings)
    /// keyed by this.
    generation: AtomicU64,
}

impl HandlerRoutingTable {
    pub fn new() -> Self {
        Self::default()
    }

    /// Monotonic mutation counter for derived-structure caching.
    pub fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }

    fn bump(&self) {
        self.generation.fetch_add(1, Ordering::Release);
    }

    /// Applies a registration. Replaces any existing row for the same
    /// (bus, kind, message_type, client_id) regardless of node — a client
    /// reconnecting through a different node moves its row.
    pub fn apply_register(&self, reg: HandlerRegistration) {
        let handler = RegisteredHandler {
            client_id: reg.client_id,
            node_id: reg.node_id,
            load_factor: reg.load_factor,
        };
        let mut table = self.inner.write();
        let slot = table
            .entry(reg.bus)
            .or_default()
            .entry(reg.message_type)
            .or_default()
            .slot(reg.kind);
        let mut rows: Vec<RegisteredHandler> =
            slot.as_deref().map(<[_]>::to_vec).unwrap_or_default();
        if let Some(existing) = rows.iter_mut().find(|r| r.client_id == handler.client_id) {
            *existing = handler;
        } else {
            rows.push(handler);
        }
        *slot = sorted_rows(rows);
        drop(table);
        self.bump();
    }

    pub fn apply_deregister(
        &self,
        bus: &str,
        kind: HandlerKind,
        message_type: &str,
        client_id: &str,
        node_id: NodeId,
    ) {
        let mut table = self.inner.write();
        if let Some(types) = table.get_mut(bus) {
            if let Some(type_rows) = types.get_mut(message_type) {
                let slot = type_rows.slot(kind);
                if let Some(rows) = slot.as_deref() {
                    let kept: Vec<RegisteredHandler> = rows
                        .iter()
                        .filter(|r| !(r.client_id == client_id && r.node_id == node_id))
                        .cloned()
                        .collect();
                    *slot = sorted_rows(kept);
                }
                if type_rows.is_empty() {
                    types.remove(message_type);
                }
            }
            if types.is_empty() {
                table.remove(bus);
            }
        }
        drop(table);
        self.bump();
    }

    /// Keeps only the rows `keep` accepts, in every bus and type.
    fn retain_rows(&self, keep: impl Fn(&RegisteredHandler) -> bool) {
        let mut table = self.inner.write();
        table.retain(|_, types| {
            types.retain(|_, type_rows| {
                for kind in [HandlerKind::Command, HandlerKind::Query] {
                    let slot = type_rows.slot(kind);
                    if let Some(rows) = slot.as_deref()
                        && !rows.iter().all(&keep)
                    {
                        let kept: Vec<RegisteredHandler> =
                            rows.iter().filter(|r| keep(r)).cloned().collect();
                        *slot = sorted_rows(kept);
                    }
                }
                !type_rows.is_empty()
            });
            !types.is_empty()
        });
        drop(table);
        self.bump();
    }

    pub fn apply_deregister_client(&self, client_id: &str, node_id: NodeId) {
        self.retain_rows(|r| !(r.client_id == client_id && r.node_id == node_id));
    }

    pub fn apply_clear_node(&self, node_id: NodeId) {
        self.retain_rows(|r| r.node_id != node_id);
    }

    pub fn retain_nodes(&self, live: &BTreeSet<NodeId>) {
        self.retain_rows(|r| live.contains(&r.node_id));
    }

    /// The handlers for a message type, sorted by client id. Shared, not
    /// copied: the hot path of every dispatch.
    pub fn lookup(&self, bus: &str, kind: HandlerKind, message_type: &str) -> HandlerRows {
        self.inner
            .read()
            .get(bus)
            .and_then(|types| types.get(message_type))
            .and_then(|type_rows| type_rows.get(kind))
            .cloned()
            .unwrap_or_else(empty_rows)
    }

    pub fn rows(&self) -> Vec<HandlerRegistration> {
        let table = self.inner.read();
        let mut rows: Vec<HandlerRegistration> = Vec::new();
        for (bus, types) in table.iter() {
            for (message_type, type_rows) in types {
                for kind in [HandlerKind::Command, HandlerKind::Query] {
                    for h in type_rows.get(kind).map(|r| r.iter()).into_iter().flatten() {
                        rows.push(HandlerRegistration {
                            bus: bus.clone(),
                            kind,
                            message_type: message_type.clone(),
                            client_id: h.client_id.clone(),
                            node_id: h.node_id,
                            load_factor: h.load_factor,
                        });
                    }
                }
            }
        }
        rows.sort_by(|a, b| {
            (&a.bus, a.kind, &a.message_type, &a.client_id).cmp(&(
                &b.bus,
                b.kind,
                &b.message_type,
                &b.client_id,
            ))
        });
        rows
    }

    /// Replaces the whole table from snapshot rows (restart / snapshot
    /// install). Restored rows are provisional: each node's startup
    /// `ClearNodeHandlers` and membership diffs remove any that are stale.
    pub fn restore(&self, rows: Vec<HandlerRegistration>) {
        let mut grouped: HashMap<String, HashMap<String, [Vec<RegisteredHandler>; 2]>> =
            HashMap::new();
        for reg in rows {
            let kind_index = match reg.kind {
                HandlerKind::Command => 0,
                HandlerKind::Query => 1,
            };
            grouped
                .entry(reg.bus)
                .or_default()
                .entry(reg.message_type)
                .or_default()[kind_index]
                .push(RegisteredHandler {
                    client_id: reg.client_id,
                    node_id: reg.node_id,
                    load_factor: reg.load_factor,
                });
        }
        let table = grouped
            .into_iter()
            .map(|(bus, types)| {
                let types = types
                    .into_iter()
                    .map(|(message_type, [command, query])| {
                        (
                            message_type,
                            TypeRows {
                                command: sorted_rows(command),
                                query: sorted_rows(query),
                            },
                        )
                    })
                    .collect();
                (bus, types)
            })
            .collect();
        *self.inner.write() = table;
        self.bump();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reg(bus: &str, message_type: &str, client: &str, node: NodeId) -> HandlerRegistration {
        HandlerRegistration {
            bus: bus.into(),
            kind: HandlerKind::Command,
            message_type: message_type.into(),
            client_id: client.into(),
            node_id: node,
            load_factor: 100,
        }
    }

    #[test]
    fn register_and_lookup() {
        let table = HandlerRoutingTable::new();
        table.apply_register(reg("main", "CreateOrder", "c1", 1));
        table.apply_register(reg("main", "CreateOrder", "c2", 2));

        let rows = table.lookup("main", HandlerKind::Command, "CreateOrder");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].client_id, "c1");
        assert_eq!(rows[1].node_id, 2);
        assert!(
            table
                .lookup("other", HandlerKind::Command, "CreateOrder")
                .is_empty()
        );
        assert!(
            table
                .lookup("main", HandlerKind::Query, "CreateOrder")
                .is_empty()
        );
    }

    #[test]
    fn reregistration_moves_row_to_new_node() {
        let table = HandlerRoutingTable::new();
        table.apply_register(reg("main", "CreateOrder", "c1", 1));
        table.apply_register(reg("main", "CreateOrder", "c1", 3));

        let rows = table.lookup("main", HandlerKind::Command, "CreateOrder");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].node_id, 3);
    }

    #[test]
    fn stale_disconnect_does_not_remove_moved_row() {
        let table = HandlerRoutingTable::new();
        table.apply_register(reg("main", "CreateOrder", "c1", 1));
        // Client reconnects through node 3, then node 1's disconnect lands.
        table.apply_register(reg("main", "CreateOrder", "c1", 3));
        table.apply_deregister_client("c1", 1);

        let rows = table.lookup("main", HandlerKind::Command, "CreateOrder");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].node_id, 3);
    }

    #[test]
    fn clear_node_drops_only_that_node() {
        let table = HandlerRoutingTable::new();
        table.apply_register(reg("main", "CreateOrder", "c1", 1));
        table.apply_register(reg("main", "CreateOrder", "c2", 2));
        table.apply_register(reg("main", "Ship", "c3", 1));
        table.apply_clear_node(1);

        assert_eq!(
            table
                .lookup("main", HandlerKind::Command, "CreateOrder")
                .len(),
            1
        );
        assert!(
            table
                .lookup("main", HandlerKind::Command, "Ship")
                .is_empty()
        );
    }

    #[test]
    fn membership_diff_drops_departed_nodes() {
        let table = HandlerRoutingTable::new();
        table.apply_register(reg("main", "CreateOrder", "c1", 1));
        table.apply_register(reg("main", "CreateOrder", "c2", 2));
        table.retain_nodes(&BTreeSet::from([2, 3]));

        let rows = table.lookup("main", HandlerKind::Command, "CreateOrder");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].node_id, 2);
    }

    #[test]
    fn snapshot_roundtrip() {
        let table = HandlerRoutingTable::new();
        table.apply_register(reg("main", "CreateOrder", "c1", 1));
        table.apply_register(reg("shared", "Ship", "c2", 2));

        let restored = HandlerRoutingTable::new();
        restored.restore(table.rows());
        assert_eq!(restored.rows(), table.rows());
    }

    #[test]
    fn generation_bumps_on_mutation() {
        let table = HandlerRoutingTable::new();
        let g0 = table.generation();
        table.apply_register(reg("main", "CreateOrder", "c1", 1));
        assert!(table.generation() > g0);
    }
}
