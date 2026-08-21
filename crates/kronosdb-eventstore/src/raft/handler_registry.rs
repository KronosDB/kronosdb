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

type TableKey = (String, HandlerKind, String);

/// The applied routing table. Written only by the Raft state machine
/// (single apply thread), read concurrently by dispatch paths.
#[derive(Default)]
pub struct HandlerRoutingTable {
    inner: RwLock<HashMap<TableKey, Vec<RegisteredHandler>>>,
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
        let key = (reg.bus, reg.kind, reg.message_type);
        let handler = RegisteredHandler {
            client_id: reg.client_id,
            node_id: reg.node_id,
            load_factor: reg.load_factor,
        };
        let mut table = self.inner.write();
        let rows = table.entry(key).or_default();
        if let Some(existing) = rows.iter_mut().find(|r| r.client_id == handler.client_id) {
            *existing = handler;
        } else {
            rows.push(handler);
        }
        drop(table);
        self.bump();
    }

    /// Applies an explicit unsubscribe. Filtered by node so a stale
    /// deregistration from a node the client already left cannot remove
    /// the client's newer registration through another node.
    pub fn apply_deregister(
        &self,
        bus: &str,
        kind: HandlerKind,
        message_type: &str,
        client_id: &str,
        node_id: NodeId,
    ) {
        let key = (bus.to_string(), kind, message_type.to_string());
        let mut table = self.inner.write();
        if let Some(rows) = table.get_mut(&key) {
            rows.retain(|r| !(r.client_id == client_id && r.node_id == node_id));
            if rows.is_empty() {
                table.remove(&key);
            }
        }
        drop(table);
        self.bump();
    }

    /// Applies a client disconnect: removes every row for the client that
    /// still points at the node it disconnected from.
    pub fn apply_deregister_client(&self, client_id: &str, node_id: NodeId) {
        let mut table = self.inner.write();
        table.retain(|_, rows| {
            rows.retain(|r| !(r.client_id == client_id && r.node_id == node_id));
            !rows.is_empty()
        });
        drop(table);
        self.bump();
    }

    /// Drops every row owned by a node (startup crash-cleanup, node death).
    pub fn apply_clear_node(&self, node_id: NodeId) {
        let mut table = self.inner.write();
        table.retain(|_, rows| {
            rows.retain(|r| r.node_id != node_id);
            !rows.is_empty()
        });
        drop(table);
        self.bump();
    }

    /// Drops rows owned by nodes outside the live membership set.
    pub fn retain_nodes(&self, live: &BTreeSet<NodeId>) {
        let mut table = self.inner.write();
        table.retain(|_, rows| {
            rows.retain(|r| live.contains(&r.node_id));
            !rows.is_empty()
        });
        drop(table);
        self.bump();
    }

    /// Handlers for a (bus, kind, message_type), sorted by client_id so
    /// every node derives identical rings from identical generations.
    pub fn lookup(
        &self,
        bus: &str,
        kind: HandlerKind,
        message_type: &str,
    ) -> Vec<RegisteredHandler> {
        let key = (bus.to_string(), kind, message_type.to_string());
        let mut rows = self.inner.read().get(&key).cloned().unwrap_or_default();
        rows.sort_by(|a, b| a.client_id.cmp(&b.client_id));
        rows
    }

    /// All rows, for snapshotting. Sorted for deterministic snapshots.
    pub fn rows(&self) -> Vec<HandlerRegistration> {
        let table = self.inner.read();
        let mut rows: Vec<HandlerRegistration> = table
            .iter()
            .flat_map(|((bus, kind, message_type), handlers)| {
                handlers.iter().map(move |h| HandlerRegistration {
                    bus: bus.clone(),
                    kind: *kind,
                    message_type: message_type.clone(),
                    client_id: h.client_id.clone(),
                    node_id: h.node_id,
                    load_factor: h.load_factor,
                })
            })
            .collect();
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
        let mut table: HashMap<TableKey, Vec<RegisteredHandler>> = HashMap::new();
        for reg in rows {
            table
                .entry((reg.bus, reg.kind, reg.message_type))
                .or_default()
                .push(RegisteredHandler {
                    client_id: reg.client_id,
                    node_id: reg.node_id,
                    load_factor: reg.load_factor,
                });
        }
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
