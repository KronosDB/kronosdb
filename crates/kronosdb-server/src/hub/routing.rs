//! Where a message goes (ADR-0007 Tier 2): decisions over the replicated
//! handler table, as plain functions of the rows, and the small pure helpers
//! that read a message's processing instructions.
//!
//! Selection happens on the dispatching node. The ring and the weighted
//! round-robin run against the replicated table, which every node holds
//! identically, so any node resolves the same owner for the same key. The
//! forward then names the chosen handler explicitly; the owning node only
//! acquires the permit and delivers.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use kronosdb_eventstore::raft::handler_registry::{
    HandlerKind, HandlerRoutingTable, HandlerRows, RegisteredHandler,
};
use kronosdb_messaging::ring::Ring;
use kronosdb_messaging::types::{
    MetadataValue, ProcessingInstruction, ProcessingKey, RoutingKey, instruction_number,
};
use parking_lot::RwLock;

/// Where a command should go, per the replicated routing table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommandRoute {
    /// No replicated rows for this (bus, type) — use the node-local path.
    /// Covers single-node bootstrap and registration-write lag.
    LocalFallback,
    /// Keyed command whose ring owner is connected to this node.
    Local { client_id: String },
    /// The selected handler is connected to a remote node.
    Remote { client_id: String, node_id: u64 },
    /// Unkeyed command with at least one local handler: let the local bus
    /// load-balance (locality-preferred).
    LocalAny,
}

/// Where a query should fan out, per the replicated routing table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryFanout {
    /// Attempt local dispatch (local handlers exist, or no replicated rows
    /// at all — the bootstrap fallback).
    pub local: bool,
    /// Remote deliveries: owning node → handler instances on that node.
    pub remote: Vec<(u64, Vec<String>)>,
}

/// The row a weighted round-robin counter lands on.
pub fn weighted_pick(rows: &[RegisteredHandler], counter: u64) -> &RegisteredHandler {
    let total: u64 = rows.iter().map(|r| r.load_factor.max(1) as u64).sum();
    let target = counter % total.max(1);
    let mut cumulative = 0u64;
    for row in rows {
        cumulative += row.load_factor.max(1) as u64;
        if target < cumulative {
            return row;
        }
    }
    &rows[0]
}

/// Routes a command given the rows for its type (never empty here), the
/// ring built from them, and a round-robin counter value.
pub fn command_route(
    rows: &[RegisteredHandler],
    local: u64,
    routing_key: Option<&str>,
    ring: impl FnOnce() -> Arc<Ring>,
    counter: u64,
) -> CommandRoute {
    if rows.is_empty() {
        return CommandRoute::LocalFallback;
    }
    match routing_key {
        Some(key) => {
            let idx = ring().lookup(key).unwrap_or(0);
            // In bounds for a ring built from `rows`, which is the only
            // kind the router hands in; the clamp keeps a caller's own
            // ring from taking dispatch down.
            let row = rows.get(idx).unwrap_or(&rows[0]);
            if row.node_id == local {
                CommandRoute::Local {
                    client_id: row.client_id.clone(),
                }
            } else {
                CommandRoute::Remote {
                    client_id: row.client_id.clone(),
                    node_id: row.node_id,
                }
            }
        }
        None => {
            if rows.iter().any(|r| r.node_id == local) {
                return CommandRoute::LocalAny;
            }
            let chosen = weighted_pick(rows, counter);
            CommandRoute::Remote {
                client_id: chosen.client_id.clone(),
                node_id: chosen.node_id,
            }
        }
    }
}

/// Fan-out plan for a query. Scatter-gather goes to every node with
/// registered handlers (local delivery for local rows); point-to-point is
/// locality-preferred, falling back to a weighted pick over remote
/// instances when no local handler exists.
pub fn query_fanout(
    rows: &[RegisteredHandler],
    local: u64,
    point_to_point: bool,
    counter: u64,
) -> QueryFanout {
    if rows.is_empty() {
        return QueryFanout {
            local: true,
            remote: Vec::new(),
        };
    }
    let has_local = rows.iter().any(|r| r.node_id == local);

    if point_to_point {
        if has_local {
            return QueryFanout {
                local: true,
                remote: Vec::new(),
            };
        }
        let chosen = weighted_pick(rows, counter);
        return QueryFanout {
            local: false,
            remote: vec![(chosen.node_id, vec![chosen.client_id.clone()])],
        };
    }

    let mut remote: HashMap<u64, Vec<String>> = HashMap::new();
    for row in rows.iter().filter(|r| r.node_id != local) {
        remote
            .entry(row.node_id)
            .or_default()
            .push(row.client_id.clone());
    }
    let mut remote: Vec<(u64, Vec<String>)> = remote.into_iter().collect();
    remote.sort_by_key(|(node, _)| *node);
    QueryFanout {
        // With remote nodes present, dispatch locally only when local
        // handlers are known; a local NoHandler is tolerated either way.
        local: has_local,
        remote,
    }
}

/// A ring and the rows it was built from.
struct CachedRing {
    rows: HandlerRows,
    ring: Arc<Ring>,
}

/// Routing decisions against the replicated handler table, with a
/// per-(bus, type) ring cache.
///
/// A ring is only valid for the rows it was built from: it names handlers
/// by their index in them. The cache therefore remembers those rows, and a
/// ring is reused only for the very same rows (the table hands out one
/// shared allocation per change), never for rows that merely carry the same
/// version number. The table's generation cannot stand in for that: it is
/// bumped after the rows change, so a dispatch can read the new rows under
/// the old generation, pair them with a ring built for the old rows, and
/// send a key to a handler no ring would pick.
pub struct Router {
    table: Arc<HandlerRoutingTable>,
    local_node: u64,
    /// bus → message type → ring for some rows. Two borrowed lookups on a
    /// hit; nothing allocated on the dispatch path.
    rings: RwLock<HashMap<String, HashMap<String, CachedRing>>>,
    rr_counter: AtomicU64,
}

impl Router {
    pub fn new(table: Arc<HandlerRoutingTable>, local_node: u64) -> Self {
        Self {
            table,
            local_node,
            rings: RwLock::new(HashMap::new()),
            rr_counter: AtomicU64::new(0),
        }
    }

    pub fn command(
        &self,
        bus: &str,
        message_type: &str,
        routing_key: Option<&str>,
    ) -> CommandRoute {
        let rows = self.table.lookup(bus, HandlerKind::Command, message_type);
        command_route(
            &rows,
            self.local_node,
            routing_key,
            || self.ring_for(bus, message_type, &rows),
            self.rr_counter.fetch_add(1, Ordering::Relaxed),
        )
    }

    /// [`command`](Self::command), except that a keyed command waits (until
    /// `deadline`) for its type's rows instead of taking the local fallback.
    /// A routing key promises one handler per key, which only the replicated
    /// table can keep: every node sees its changes in the same order, while
    /// the local bus knows only its own handlers, so a key routed locally
    /// first would move as soon as the rows land. `None` when no row
    /// appeared in time.
    pub async fn command_settled(
        &self,
        bus: &str,
        message_type: &str,
        routing_key: Option<&str>,
        deadline: tokio::time::Instant,
    ) -> Option<CommandRoute> {
        loop {
            let changed = self.table.changed();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let route = self.command(bus, message_type, routing_key);
            if routing_key.is_none() || route != CommandRoute::LocalFallback {
                return Some(route);
            }
            tokio::time::timeout_at(deadline, changed).await.ok()?;
        }
    }

    pub fn query(&self, bus: &str, message_type: &str, point_to_point: bool) -> QueryFanout {
        let rows = self.table.lookup(bus, HandlerKind::Query, message_type);
        query_fanout(
            &rows,
            self.local_node,
            point_to_point,
            self.rr_counter.fetch_add(1, Ordering::Relaxed),
        )
    }

    /// Ring over the sorted global handler rows. Rows come pre-sorted by
    /// client_id from the table, and the hash is deterministic across
    /// builds, so every node derives the same ring from the same rows.
    fn ring_for(&self, bus: &str, message_type: &str, rows: &HandlerRows) -> Arc<Ring> {
        {
            let rings = self.rings.read();
            if let Some(cached) = rings.get(bus).and_then(|types| types.get(message_type))
                && Arc::ptr_eq(&cached.rows, rows)
            {
                return Arc::clone(&cached.ring);
            }
        }
        let ring = Arc::new(Ring::build(
            rows.iter().map(|r| (r.client_id.as_str(), r.load_factor)),
        ));
        self.rings
            .write()
            .entry(bus.to_string())
            .or_default()
            .insert(
                message_type.to_string(),
                CachedRing {
                    rows: Arc::clone(rows),
                    ring: Arc::clone(&ring),
                },
            );
        ring
    }
}

/// The text of a command's routing-key instruction, if it has one.
pub fn routing_key_of(instructions: &[ProcessingInstruction]) -> Option<RoutingKey> {
    instructions
        .iter()
        .find_map(|pi| match (&pi.key, &pi.value) {
            (ProcessingKey::RoutingKey, Some(MetadataValue::Text(key))) => {
                Some(RoutingKey(key.clone()))
            }
            _ => None,
        })
}

/// Effective request deadline: the client's `Timeout` instruction
/// (milliseconds, clamped to [1s, 1h]) when present, else the server default.
pub fn effective_timeout(
    instructions: &[ProcessingInstruction],
    server_default: Duration,
) -> Duration {
    instruction_number(instructions, ProcessingKey::Timeout)
        .map(|ms| Duration::from_millis(ms.clamp(1_000, 3_600_000) as u64))
        .unwrap_or(server_default)
}

/// Expected result count from the `NrOfResults` instruction. `1` selects
/// point-to-point routing; anything else (absent, 0, negative, or >1) is
/// scatter-gather (`-1`).
pub fn expected_results(instructions: &[ProcessingInstruction]) -> i32 {
    match instruction_number(instructions, ProcessingKey::NrOfResults) {
        Some(1) => 1,
        _ => -1,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kronosdb_eventstore::raft::handler_registry::HandlerRegistration;

    fn row(client: &str, node: u64, load: i32) -> RegisteredHandler {
        RegisteredHandler {
            client_id: client.into(),
            node_id: node,
            load_factor: load,
        }
    }

    fn ring_of(rows: &[RegisteredHandler]) -> Arc<Ring> {
        Arc::new(Ring::build(
            rows.iter().map(|r| (r.client_id.as_str(), r.load_factor)),
        ))
    }

    #[test]
    fn no_rows_fall_back_to_the_local_bus() {
        assert_eq!(
            command_route(&[], 1, None, || ring_of(&[]), 0),
            CommandRoute::LocalFallback
        );
        let fanout = query_fanout(&[], 1, false, 0);
        assert!(fanout.local && fanout.remote.is_empty());
    }

    #[test]
    fn unkeyed_commands_prefer_a_local_handler() {
        let rows = [row("a", 1, 100), row("b", 2, 100)];
        assert_eq!(
            command_route(&rows, 1, None, || ring_of(&rows), 5),
            CommandRoute::LocalAny
        );
        let remote_only = [row("b", 2, 100), row("c", 3, 100)];
        assert!(matches!(
            command_route(&remote_only, 1, None, || ring_of(&remote_only), 0),
            CommandRoute::Remote { .. }
        ));
    }

    #[test]
    fn keyed_commands_follow_the_ring_to_the_same_owner_everywhere() {
        let rows = [row("a", 1, 100), row("b", 2, 100), row("c", 3, 100)];
        let owner = |local: u64, key: &str| match command_route(
            &rows,
            local,
            Some(key),
            || ring_of(&rows),
            0,
        ) {
            CommandRoute::Local { client_id } | CommandRoute::Remote { client_id, .. } => client_id,
            other => panic!("unexpected {other:?}"),
        };
        for key in ["order-1", "order-2", "order-3", "order-4"] {
            // Whichever node asks, the same handler owns the key.
            assert_eq!(owner(1, key), owner(2, key));
            assert_eq!(owner(2, key), owner(3, key));
            // And the answer says Local exactly on the owner's node.
            let owner_node = rows
                .iter()
                .find(|r| r.client_id == owner(1, key))
                .unwrap()
                .node_id;
            assert!(matches!(
                command_route(&rows, owner_node, Some(key), || ring_of(&rows), 0),
                CommandRoute::Local { .. }
            ));
        }
    }

    #[test]
    fn weighted_pick_follows_the_weights() {
        let rows = [row("a", 1, 1), row("b", 2, 3)];
        let picks: Vec<&str> = (0..8)
            .map(|n| weighted_pick(&rows, n).client_id.as_str())
            .collect();
        assert_eq!(picks.iter().filter(|c| **c == "a").count(), 2);
        assert_eq!(picks.iter().filter(|c| **c == "b").count(), 6);
    }

    #[test]
    fn point_to_point_queries_stay_local_when_they_can() {
        let rows = [row("a", 1, 100), row("b", 2, 100)];
        let fanout = query_fanout(&rows, 1, true, 0);
        assert!(fanout.local && fanout.remote.is_empty());
        let fanout = query_fanout(&rows, 3, true, 0);
        assert!(!fanout.local);
        assert_eq!(fanout.remote.len(), 1);
        assert_eq!(fanout.remote[0].1.len(), 1);
    }

    #[test]
    fn scatter_gather_reaches_every_remote_node_once() {
        let rows = [
            row("a", 1, 100),
            row("b", 2, 100),
            row("c", 2, 100),
            row("d", 3, 100),
        ];
        let fanout = query_fanout(&rows, 1, false, 0);
        assert!(fanout.local);
        assert_eq!(
            fanout.remote,
            vec![(2, vec!["b".into(), "c".into()]), (3, vec!["d".into()])]
        );
    }

    #[test]
    fn the_router_reads_the_table_and_caches_rings_by_rows() {
        let table = Arc::new(HandlerRoutingTable::new());
        let router = Router::new(Arc::clone(&table), 1);
        assert_eq!(
            router.command("default", "Echo", Some("k")),
            CommandRoute::LocalFallback
        );
        for (client, node) in [("a", 1), ("b", 2)] {
            table.apply_register(HandlerRegistration {
                bus: "default".into(),
                kind: HandlerKind::Command,
                message_type: "Echo".into(),
                client_id: client.into(),
                node_id: node,
                load_factor: 100,
            });
        }
        let first = router.command("default", "Echo", Some("k"));
        assert_eq!(first, router.command("default", "Echo", Some("k")));
        assert!(!matches!(first, CommandRoute::LocalFallback));
        // Another bus has no rows.
        assert_eq!(
            router.command("other", "Echo", Some("k")),
            CommandRoute::LocalFallback
        );
    }

    fn number(key: ProcessingKey, n: i64) -> ProcessingInstruction {
        ProcessingInstruction {
            key,
            value: Some(MetadataValue::Number(n)),
        }
    }

    #[test]
    fn instructions_decide_timeout_and_result_count() {
        let default = Duration::from_secs(30);
        assert_eq!(effective_timeout(&[], default), default);
        assert_eq!(
            effective_timeout(&[number(ProcessingKey::Timeout, 5_000)], default),
            Duration::from_secs(5)
        );
        // Clamped to [1s, 1h].
        assert_eq!(
            effective_timeout(&[number(ProcessingKey::Timeout, 1)], default),
            Duration::from_secs(1)
        );
        assert_eq!(
            effective_timeout(&[number(ProcessingKey::Timeout, i64::MAX)], default),
            Duration::from_secs(3600)
        );
        assert_eq!(expected_results(&[]), -1);
        assert_eq!(
            expected_results(&[number(ProcessingKey::NrOfResults, 1)]),
            1
        );
        assert_eq!(
            expected_results(&[number(ProcessingKey::NrOfResults, 5)]),
            -1
        );
    }

    #[test]
    fn only_text_routing_keys_count() {
        let text = ProcessingInstruction {
            key: ProcessingKey::RoutingKey,
            value: Some(MetadataValue::Text("order-7".into())),
        };
        assert_eq!(routing_key_of(&[text]).unwrap().0, "order-7");
        assert!(routing_key_of(&[number(ProcessingKey::RoutingKey, 7)]).is_none());
        assert!(routing_key_of(&[]).is_none());
    }

    /// While the table's rows change under it, a router only ever names a
    /// handler that a ring built from some version of the rows picks for the
    /// key. The versions are chosen so that a ring paired with the rows of
    /// the other version names "b", which no version's ring picks for this
    /// key. Before the cache was keyed by the rows, a dispatch that read the
    /// new rows just before the table's generation moved did exactly that.
    #[test]
    fn a_route_never_pairs_a_ring_with_rows_it_was_not_built_from() {
        let reg = |client: &str| HandlerRegistration {
            bus: "default".into(),
            kind: HandlerKind::Command,
            message_type: "Keyed".into(),
            client_id: client.into(),
            node_id: 1,
            load_factor: 100,
        };
        let owner = |rows: &[&str], key: &str| {
            Ring::build(rows.iter().map(|c| (*c, 100)))
                .lookup(key)
                .unwrap()
        };
        let (first, second) = (["a", "b"], ["b", "c"]);
        let key = (0..100_000)
            .map(|i| format!("k{i}"))
            .find(|k| owner(&first, k) == 0 && owner(&second, k) == 1)
            .expect("a key that moves from a to c");

        let table = Arc::new(HandlerRoutingTable::new());
        table.restore(first.iter().map(|c| reg(c)).collect());
        let router = Router::new(Arc::clone(&table), 1);
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let writer = {
            let (table, stop) = (Arc::clone(&table), Arc::clone(&stop));
            std::thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    table.restore(second.iter().map(|c| reg(c)).collect());
                    table.restore(first.iter().map(|c| reg(c)).collect());
                }
            })
        };
        let until = std::time::Instant::now() + Duration::from_secs(1);
        let mut routed = 0u64;
        while std::time::Instant::now() < until {
            for _ in 0..1000 {
                routed += 1;
                match router.command("default", "Keyed", Some(&key)) {
                    CommandRoute::Local { client_id } => {
                        assert!(
                            client_id == "a" || client_id == "c",
                            "{client_id} after {routed} routes"
                        )
                    }
                    other => panic!("unexpected {other:?}"),
                }
            }
        }
        stop.store(true, Ordering::Relaxed);
        writer.join().unwrap();
    }

    #[tokio::test]
    async fn a_keyed_command_waits_for_its_rows_instead_of_routing_locally() {
        let table = Arc::new(HandlerRoutingTable::new());
        let router = Router::new(Arc::clone(&table), 1);
        let soon = || tokio::time::Instant::now() + std::time::Duration::from_secs(5);

        // Unkeyed commands keep the local fallback.
        assert_eq!(
            router
                .command_settled("default", "Echo", None, soon())
                .await,
            Some(CommandRoute::LocalFallback)
        );
        // A keyed one with no rows gives up at its deadline.
        let now = tokio::time::Instant::now();
        assert_eq!(
            router
                .command_settled("default", "Echo", Some("k"), now)
                .await,
            None
        );
        // And routes over the replicated table as soon as a row lands.
        let writer = {
            let table = Arc::clone(&table);
            tokio::spawn(async move {
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                table.apply_register(HandlerRegistration {
                    bus: "default".into(),
                    kind: HandlerKind::Command,
                    message_type: "Echo".into(),
                    client_id: "a".into(),
                    node_id: 1,
                    load_factor: 100,
                });
            })
        };
        assert_eq!(
            router
                .command_settled("default", "Echo", Some("k"), soon())
                .await,
            Some(CommandRoute::Local {
                client_id: "a".into()
            })
        );
        writer.await.unwrap();
    }
}
