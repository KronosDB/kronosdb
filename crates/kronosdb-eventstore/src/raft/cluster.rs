//! `ClusterManager` — every node is always a Raft node.
//!
//! A single-node deployment is simply a one-voter Raft cluster that instantly
//! self-elects. When peers join, the cluster scales up without any mode switch.
//!
//! The gRPC layer always calls `cluster_manager.get_store(context)` and gets
//! back `Arc<dyn EventStore>` backed by `RaftEngine`.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use openraft::{BasicNode, Config, Membership, Raft, StoredMembership};
use parking_lot::RwLock;
use tonic::transport::Channel;

use crate::api::EventStore;
use crate::append::{AppendRequest, AppendResponse};
use crate::context::ContextManager;
use crate::criteria::SourcingCondition;
use crate::error::Error;
use crate::event::{Position, SequencedEvent, Tag};
use crate::store::EventStoreEngine;
use crate::stream::EventStream;

use super::log_store::{LogStore, LogStoreConfig};
use super::network::NetworkFactory;
use super::proto;
use super::proto::raft_transport_client::RaftTransportClient;
use super::snapshot_store::SnapshotStore;
use super::state_machine::{EventStoreStateMachine, synthesize_rescue_snapshot};
use super::types::{
    NodeId, RaftAppendCondition, RaftAppendEvent, RaftCriterion, RaftRejectReason, RaftRequest,
    RaftResponse, TypeConfig,
};

/// Node type determines how a node participates in the cluster.
#[derive(Debug, Clone, PartialEq)]
pub enum NodeType {
    /// Full Raft voter + candidate. Participates in consensus, stores events.
    Standard,
    /// Raft learner. Receives log entries but doesn't vote. Read-only event store.
    PassiveBackup,
}

/// Configuration for a cluster peer.
#[derive(Debug, Clone)]
pub struct PeerConfig {
    pub id: NodeId,
    pub addr: String,
}

/// Cluster configuration.
#[derive(Debug, Clone)]
pub struct ClusterConfig {
    /// This node's ID.
    pub node_id: NodeId,
    /// This node's type.
    pub node_type: NodeType,
    /// This node's advertised address (for peers to connect to).
    pub advertise_addr: String,
    /// All voter peers (including self).
    pub voters: Vec<PeerConfig>,
    /// All learner peers (including self if passive backup).
    pub learners: Vec<PeerConfig>,
    /// Raft configuration.
    pub raft_config: Config,
    /// Serve appends directly from the local engine when this node is a
    /// standalone single-voter deployment (one voter, no learners). Skips the
    /// consensus round-trip — and its extra Raft-log fsync — per append. The
    /// write path is fixed for the process lifetime, so exactly one append
    /// path is ever active (the invariant that prevents dual-write bugs).
    /// Ignored whenever peers or learners are configured.
    pub single_node_fast_path: bool,
    /// Deferred group-commit interval for the Raft log store. `None` = fsync
    /// inline on every append (strict, serializes consensus appends).
    /// Typically mirrors the segment group-commit interval.
    pub log_group_commit: Option<std::time::Duration>,
}

/// Manages event store access — always backed by Raft consensus.
///
/// Every node is a Raft node. A single-node deployment starts as a
/// one-voter cluster that instantly self-elects as leader.
pub struct ClusterManager {
    context_manager: Arc<ContextManager>,
    /// Per-context store facades. On the consensus path each wraps the
    /// node's single shared Raft group (the target context travels inside
    /// `RaftRequest::Append`); on the single-node fast path this is the
    /// local engine directly.
    raft_stores: RwLock<HashMap<String, Arc<dyn EventStore>>>,
    /// The node's single Raft node. One consensus group per node governs all
    /// contexts — the state machine routes each applied entry to its context.
    raft: RwLock<Option<Arc<Raft<TypeConfig>>>>,
    cluster_config: ClusterConfig,
}

impl ClusterManager {
    /// Creates a new cluster manager. Every node is always a Raft node.
    pub fn new(context_manager: Arc<ContextManager>, cluster_config: ClusterConfig) -> Self {
        Self {
            context_manager,
            raft_stores: RwLock::new(HashMap::new()),
            raft: RwLock::new(None),
            cluster_config,
        }
    }

    /// Returns true if this node has multiple voters (multi-node cluster).
    pub fn is_multi_node(&self) -> bool {
        self.cluster_config.voters.len() > 1
    }

    /// Returns true when appends bypass Raft: fast path requested AND the
    /// deployment is genuinely standalone (one voter, no learners). Any
    /// configured peer disables it regardless of the flag.
    pub fn is_fast_path(&self) -> bool {
        self.cluster_config.single_node_fast_path
            && self.cluster_config.voters.len() <= 1
            && self.cluster_config.learners.is_empty()
    }

    /// Initializes the node's single Raft group. Idempotent — subsequent
    /// calls are no-ops.
    ///
    /// The Raft log store lives under `<data_dir>/default/raft` — the same
    /// location it historically occupied when it was the default context's
    /// per-context log — so existing data directories keep working without
    /// migration. The log is node-wide: entries for every context flow
    /// through it, and the state machine (which wraps the whole
    /// `ContextManager`) routes each entry to its target context.
    pub async fn init_raft(&self) -> Result<(), Error> {
        if self.raft.read().is_some() {
            return Ok(());
        }

        // Node-wide Raft log store (kept under default/ for back-compat).
        let raft_dir = self.context_manager.data_dir().join("default").join("raft");
        let log_store = LogStore::new(
            &raft_dir,
            LogStoreConfig {
                group_commit_interval: self.cluster_config.log_group_commit,
                ..Default::default()
            },
        )
        .map_err(Error::Io)?;

        // On-disk snapshot store. Lives in `<raft_dir>/snapshots`. Used to
        // persist `last_membership` across restart — without this, the
        // cluster-init Membership log entry being purged leaves no durable
        // carrier of voter set and the node defaults to Learner on restart.
        let snapshot_store =
            Arc::new(SnapshotStore::new(raft_dir.join("snapshots")).map_err(Error::Io)?);

        // Legacy recovery for data dirs written before membership.bin
        // existed: a node that restarted cleanly pre-first-snapshot has its
        // Membership entry inside the applied log region, which openraft
        // never rescans — the state machine would hydrate an empty voter set
        // and the node could never elect a leader again. If the entry is
        // still in the (unpurged) log, recover it from there and persist it.
        if snapshot_store
            .load_membership()
            .map_err(Error::Io)?
            .is_none()
            && snapshot_store
                .load_latest_meta()
                .map_err(Error::Io)?
                .is_none()
        {
            if let Some(last) = log_store.last_log_id() {
                let first = log_store.last_purged().map(|p| p.index + 1).unwrap_or(0);
                let mut found: Option<StoredMembership<NodeId, BasicNode>> = None;
                for idx in first..=last.index {
                    if let Some(entry) = log_store.entry_at(idx).map_err(Error::Io)? {
                        if let openraft::EntryPayload::Membership(m) = &entry.payload {
                            found = Some(StoredMembership::new(
                                Some(*openraft::RaftLogId::get_log_id(&entry)),
                                m.clone(),
                            ));
                        }
                    }
                }
                if let Some(m) = found {
                    tracing::info!(
                        target: "raft.recovery",
                        voter_count = m.membership().voter_ids().count(),
                        "recovered membership from log scan (legacy data dir without membership.bin)"
                    );
                    snapshot_store.save_membership(&m).map_err(Error::Io)?;
                }
            }
        }

        // Rescue shim for pre-fix data dirs. Trigger conditions:
        //   1. No on-disk snapshot exists (`load_latest_meta` is None)
        //   2. The Raft log was purged at some point (`last_purged` is Some)
        //
        // Together these imply the node ran a server build that built
        // snapshots only in memory. The cluster-init Membership entry at
        // index 1 was purged but never persisted into a snapshot, so on
        // restart `applied_state()` would return an empty `last_membership`
        // and openraft would default the node to Learner — writes hang
        // forever (see `restart_after_snapshot_single_node.rs`).
        //
        // We refuse to rescue if this node isn't in `cluster_config.voters`
        // — silently writing a membership that excludes us would corrupt
        // the cluster's voter set in a way that's much harder to recover
        // from than the original bug.
        let needs_rescue = snapshot_store
            .load_latest_meta()
            .map_err(Error::Io)?
            .is_none()
            && log_store.last_purged().is_some();
        if needs_rescue {
            let cfg = &self.cluster_config;
            let self_in_voters = cfg.voters.iter().any(|p| p.id == cfg.node_id);
            if !self_in_voters {
                return Err(Error::Corrupted {
                    message: format!(
                        "rescue refused: node {} is not in cluster_config.voters {:?}; \
                         cannot synthesize a snapshot whose membership excludes the running node. \
                         This data dir was created by a pre-fix server (snapshots in-memory only) \
                         and the cluster-init Membership log entry has been purged. Either start \
                         this node under its original voter id or restore from backup.",
                        cfg.node_id,
                        cfg.voters.iter().map(|p| p.id).collect::<Vec<_>>(),
                    ),
                });
            }

            let mut nodes: BTreeMap<NodeId, BasicNode> = BTreeMap::new();
            for p in &cfg.voters {
                nodes.insert(
                    p.id,
                    BasicNode {
                        addr: p.addr.clone(),
                    },
                );
            }
            let voter_ids: std::collections::BTreeSet<NodeId> =
                cfg.voters.iter().map(|p| p.id).collect();
            let membership = Membership::new(vec![voter_ids], nodes);
            let stored_membership = StoredMembership::new(None, membership);

            // Use the marker-recovered last_applied. The sentinel `node_id=0`
            // would normally be fine inside the state machine (reconcile
            // rewrites it from the real log entry), but the snapshot meta is
            // checked against `last_purged` by openraft's startup with a
            // strict lexicographic `(term, node_id, index)` comparison —
            // a sentinel `node_id=0` here would fail the
            // `purge_upto <= snapshot_last_log_id` invariant when
            // `last_purged` carries the real `node_id`. Read the actual log
            // entry at the marker's index and use its real `LogId` for the
            // snapshot meta. If the log has no entry there (impossible in
            // this rescue path because `last_purged.is_some()` implies log
            // entries exist below that index, and markers are only written
            // alongside log entries), fall back to the log's own
            // `last_log_id` so the comparison still succeeds.
            let marker_applied_index = self
                .context_manager
                .max_applied_log_id()?
                .map(|applied| applied.index);
            let rescue_last_log_id = match marker_applied_index {
                Some(idx) => match log_store.entry_at(idx).map_err(Error::Io)? {
                    Some(entry) => Some(*openraft::RaftLogId::get_log_id(&entry)),
                    None => log_store.last_log_id(),
                },
                None => None,
            };

            tracing::warn!(
                target: "raft.recovery",
                node_id = cfg.node_id,
                voter_count = cfg.voters.len(),
                rescue_last_log_id = ?rescue_last_log_id,
                last_purged = ?log_store.last_purged(),
                "rescuing pre-fix data dir: synthesizing membership snapshot from cluster_config \
                 (no on-disk snapshot, but log was purged — cluster-init Membership entry is gone)"
            );

            synthesize_rescue_snapshot(
                &self.context_manager,
                &snapshot_store,
                rescue_last_log_id,
                stored_membership,
            )?;
        }

        // Create state machine wrapping the context manager. `new` recovers
        // `last_applied` from segment markers so the post-restart state matches
        // what the log store will report as committed (Option D), and hydrates
        // `last_membership` from the latest on-disk snapshot (or the rescue
        // snapshot we just synthesized above).
        let mut state_machine =
            EventStoreStateMachine::new(Arc::clone(&self.context_manager), snapshot_store)?;

        // Reconciliation pass: bring `state_machine.last_applied` and
        // `log_store.committed` into a consistent shape before handing both
        // to openraft. Without this pass, `last_applied`'s sentinel
        // `node_id=0` from marker-only recovery produces two CRASH-02
        // failure shapes (see `state_machine::reconcile_with_log` for the
        // full root-cause writeup). This pass is idempotent on a clean
        // shutdown and is the only new I/O added to the startup path
        // beyond the pre-fix recovery (no fsync, two in-memory lookups).
        let log_last = log_store.last_log_id();
        let log_committed = log_store.committed();
        let report = state_machine.reconcile_with_log(log_last, log_committed, |idx| {
            let entry = log_store.entry_at(idx).map_err(Error::Io)?;
            Ok(entry.map(|e| *openraft::RaftLogId::get_log_id(&e)))
        })?;
        if let Some(new_committed) = report.committed_promoted_to {
            log_store.promote_committed(new_committed);
        }

        // Create Raft node.
        let log_bytes = log_store.bytes_since_purge_handle();
        let raft = Raft::new(
            self.cluster_config.node_id,
            Arc::new(self.cluster_config.raft_config.clone()),
            NetworkFactory,
            log_store,
            state_machine,
        )
        .await
        .map_err(|e| Error::Corrupted {
            message: format!("failed to create raft node: {e}"),
        })?;

        let raft = Arc::new(raft);

        // Byte-aware snapshot trigger. openraft's snapshot policy counts
        // ENTRIES (LogsSinceLast), which is blind to entry size: with
        // coalesced multi-MB append entries, bulk ingest can retain tens of
        // GB of raft log before the entry-count policy fires. This task
        // triggers a snapshot (and thus purge) once the log grows past a
        // byte threshold, whichever comes first.
        {
            const LOG_BYTES_SNAPSHOT_THRESHOLD: u64 = 256 * 1024 * 1024;
            let raft = Arc::clone(&raft);
            let log_bytes = Arc::clone(&log_bytes);
            tokio::spawn(async move {
                let mut tick = tokio::time::interval(std::time::Duration::from_secs(15));
                tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                loop {
                    tick.tick().await;
                    if log_bytes.load(std::sync::atomic::Ordering::Relaxed)
                        < LOG_BYTES_SNAPSHOT_THRESHOLD
                    {
                        continue;
                    }
                    tracing::info!(
                        target: "raft.snapshot",
                        threshold_bytes = LOG_BYTES_SNAPSHOT_THRESHOLD,
                        "raft log exceeded byte threshold; triggering snapshot"
                    );
                    // Fatal error => raft is shutting down; end the task.
                    if raft.trigger().snapshot().await.is_err() {
                        return;
                    }
                    // Snapshot+purge run asynchronously; back off so we don't
                    // re-trigger against a counter that hasn't reset yet.
                    tokio::time::sleep(std::time::Duration::from_secs(60)).await;
                }
            });
        }

        *self.raft.write() = Some(raft);

        Ok(())
    }

    /// Registers a context's store facade. Idempotent. On the consensus path
    /// this wraps the shared Raft group (requires `init_raft()`); on the
    /// single-node fast path the local engine is served directly.
    pub fn register_context(&self, context_name: &str) -> Result<(), Error> {
        if self.raft_stores.read().contains_key(context_name) {
            return Ok(());
        }
        let local_engine = self.context_manager.get_context(context_name)?;
        let store: Arc<dyn EventStore> = if self.is_fast_path() {
            local_engine
        } else {
            let raft = self.raft_node().ok_or_else(|| Error::Corrupted {
                message: "register_context called before init_raft".into(),
            })?;
            Arc::new(RaftEngine::new(
                raft,
                local_engine,
                context_name.to_string(),
            ))
        };
        self.raft_stores
            .write()
            .insert(context_name.to_string(), store);
        Ok(())
    }

    /// Initializes the shared Raft group (if needed) and registers a context.
    pub async fn init_context(&self, context_name: &str) -> Result<(), Error> {
        self.init_raft().await?;
        self.register_context(context_name)
    }

    /// Creates a context through Raft consensus so every node in the cluster
    /// applies it. Idempotent (replayed/duplicate creates succeed). Forwards
    /// to the leader when called on a follower.
    ///
    /// The name is validated BEFORE proposing: `apply` treats an invalid name
    /// as a fatal state-machine error, so it must never reach the log.
    pub async fn create_context_replicated(&self, name: &str) -> Result<(), Error> {
        crate::context::validate_context_name(name)?;

        // Fast path: no peers to replicate to — create locally, with the
        // same idempotent semantics as a replicated CreateContext.
        if self.is_fast_path() {
            return match self.context_manager.create_context(name) {
                Ok(()) | Err(Error::ContextAlreadyExists { .. }) => Ok(()),
                Err(e) => Err(e),
            };
        }

        let raft = self.raft_node().ok_or_else(|| Error::Corrupted {
            message: "create_context_replicated called before init_raft".into(),
        })?;

        let raft_req = RaftRequest::CreateContext {
            name: name.to_string(),
        };
        let response = submit_raft_request(&raft, raft_req).await?;

        match response {
            RaftResponse::ContextCreated => Ok(()),
            other => Err(Error::Corrupted {
                message: format!("unexpected raft response for create_context: {other:?}"),
            }),
        }
    }

    /// Gets an event store for a context (always Raft-backed).
    ///
    /// Registers the facade lazily when the context exists in the
    /// `ContextManager` but no `RaftEngine` has been built yet — this is how
    /// contexts created at runtime (via a replicated `CreateContext` applied
    /// on this node) become servable without a restart.
    pub fn get_store(&self, context_name: &str) -> Result<Arc<dyn EventStore>, Error> {
        if let Some(store) = self.raft_stores.read().get(context_name) {
            return Ok(Arc::clone(store));
        }
        if self.context_manager.context_exists(context_name)
            && (self.raft_node().is_some() || self.is_fast_path())
        {
            self.register_context(context_name)?;
            if let Some(store) = self.raft_stores.read().get(context_name) {
                return Ok(Arc::clone(store));
            }
        }
        Err(Error::ContextNotFound {
            name: context_name.to_string(),
        })
    }

    /// Gets the underlying ContextManager (for admin, snapshot store access, etc.).
    pub fn context_manager(&self) -> &Arc<ContextManager> {
        &self.context_manager
    }

    /// Gets the node's shared Raft node (for the transport service and
    /// membership operations). `None` before `init_raft()`.
    pub fn raft_node(&self) -> Option<Arc<Raft<TypeConfig>>> {
        self.raft.read().clone()
    }

    /// Returns the cluster config.
    pub fn cluster_config(&self) -> &ClusterConfig {
        &self.cluster_config
    }

    /// Bootstraps the Raft cluster.
    ///
    /// For single-node: initializes with just this node → instant leader.
    /// For multi-node: the lowest-numbered voter node bootstraps.
    ///
    /// Safe to call multiple times — if already initialized, this is a no-op.
    pub async fn bootstrap(&self) -> Result<(), Error> {
        let config = &self.cluster_config;

        // Only the lowest-ID voter bootstraps.
        let min_voter_id = config
            .voters
            .iter()
            .map(|p| p.id)
            .min()
            .unwrap_or(config.node_id);
        if config.node_id != min_voter_id {
            return Ok(()); // Not our job to bootstrap.
        }

        // Build initial membership from voter configs.
        let mut members = BTreeMap::new();
        for peer in &config.voters {
            members.insert(
                peer.id,
                BasicNode {
                    addr: peer.addr.clone(),
                },
            );
        }

        // Initialize the node's shared Raft group.
        if let Some(raft) = self.raft_node() {
            match raft.initialize(members).await {
                Ok(_) => {}
                Err(e) => {
                    // Already initialized is not an error: openraft raises a
                    // typed `InitializeError::NotAllowed` whenever the log or
                    // vote is non-empty (fresh-leader re-init AND post-crash
                    // restart both land here). Matching the typed variant —
                    // instead of the error's display string — survives
                    // openraft upgrades that rephrase the message.
                    let already_initialized = matches!(
                        e.api_error(),
                        Some(openraft::error::InitializeError::NotAllowed(_))
                    );
                    if !already_initialized {
                        return Err(Error::Corrupted {
                            message: format!("failed to bootstrap cluster: {e}"),
                        });
                    }
                }
            }
        }

        Ok(())
    }

    /// Adds a learner to the cluster (for passive backup nodes).
    pub async fn add_learner(&self, id: NodeId, addr: String) -> Result<(), Error> {
        let raft = self.raft_node().ok_or_else(|| Error::Corrupted {
            message: "add_learner called before init_raft".into(),
        })?;

        raft.add_learner(id, BasicNode { addr }, true)
            .await
            .map_err(|e| Error::Corrupted {
                message: format!("failed to add learner: {e}"),
            })?;

        Ok(())
    }

    /// Changes the voter membership (for dynamic membership changes).
    pub async fn change_membership(&self, voter_ids: Vec<NodeId>) -> Result<(), Error> {
        let raft = self.raft_node().ok_or_else(|| Error::Corrupted {
            message: "change_membership called before init_raft".into(),
        })?;

        let members: BTreeMap<NodeId, BasicNode> = voter_ids
            .into_iter()
            .map(|id| {
                let addr = self
                    .cluster_config
                    .voters
                    .iter()
                    .find(|p| p.id == id)
                    .map(|p| p.addr.clone())
                    .unwrap_or_default();
                (id, BasicNode { addr })
            })
            .collect();

        raft.change_membership(
            members
                .keys()
                .copied()
                .collect::<std::collections::BTreeSet<_>>(),
            false,
        )
        .await
        .map_err(|e| Error::Corrupted {
            message: format!("failed to change membership: {e}"),
        })?;

        Ok(())
    }
}

// ─── RaftEngine ─────────────────────────────────────────────────────

/// A cluster-aware event store backed by Raft consensus.
///
/// - **Appends** go through Raft consensus (proposed to leader, replicated, then applied).
/// - **Reads** go directly to the local engine (eventually consistent on followers).
/// - **Subscribes** attach to the local engine (events appear after Raft applies them).
///
/// If this node is not the leader, appends are forwarded to the leader
/// via the ForwardWrite RPC. Clients never need to know who the leader is.
pub struct RaftEngine {
    raft: Arc<Raft<TypeConfig>>,
    local_engine: Arc<EventStoreEngine>,
    /// Hands appends to the proposer task, which coalesces everything that
    /// queued up during the previous consensus round into one log entry.
    proposer_tx: tokio::sync::mpsc::UnboundedSender<ProposeItem>,
}

type ProposeItem = (
    AppendRequest,
    tokio::sync::oneshot::Sender<Result<AppendResponse, Error>>,
);

/// Caps per coalesced entry: comfortably under the marker's u16 event
/// budget, and — critically — bounded in BYTES. An event count alone is not
/// a size bound (16k × ~430B events ≈ 7MB entries, which blew through the
/// raft transport's message limit and wedged replication; see the cluster
/// bench). The byte cap also bounds per-entry log fsync latency.
const MAX_BATCH_ITEMS: usize = 512;
const MAX_BATCH_EVENTS: usize = 16 * 1024;
const MAX_BATCH_BYTES: usize = 2 * 1024 * 1024;

/// Approximate wire size of one event inside a raft entry.
fn estimate_event_bytes(e: &crate::event::AppendEvent) -> usize {
    let mut n = 64 // framing + fixed fields
        + e.identifier.len()
        + e.name.len()
        + e.version.len()
        + e.payload.len();
    for (k, v) in &e.metadata {
        n += k.len() + v.len() + 8;
    }
    for t in &e.tags {
        n += t.key.len() + t.value.len() + 8;
    }
    n
}

fn estimate_request_bytes(r: &AppendRequest) -> usize {
    r.events.iter().map(estimate_event_bytes).sum()
}

impl RaftEngine {
    pub fn new(
        raft: Arc<Raft<TypeConfig>>,
        local_engine: Arc<EventStoreEngine>,
        context_name: String,
    ) -> Self {
        let (proposer_tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<ProposeItem>();

        // Proposer: self-clocking append coalescing. openraft 0.9 blocks its
        // core loop on every log entry's fsync, so consensus throughput is
        // entries/sec, not appends/sec — the fix is fewer, fatter entries.
        // While one client_write round-trip is in flight, new appends queue
        // here; the next round proposes them all as ONE AppendBatch entry.
        // A lone append still goes out immediately as a plain Append entry
        // (no added latency, wire-identical to older nodes).
        {
            let raft = Arc::clone(&raft);
            let context = context_name.clone();
            tokio::spawn(async move {
                while let Some(first) = rx.recv().await {
                    let mut event_count = first.0.events.len();
                    let mut byte_count = estimate_request_bytes(&first.0);
                    let mut batch = vec![first];
                    while batch.len() < MAX_BATCH_ITEMS
                        && event_count < MAX_BATCH_EVENTS
                        && byte_count < MAX_BATCH_BYTES
                    {
                        match rx.try_recv() {
                            Ok(item) => {
                                event_count += item.0.events.len();
                                byte_count += estimate_request_bytes(&item.0);
                                batch.push(item);
                            }
                            Err(_) => break,
                        }
                    }
                    propose_appends(&raft, &context, batch).await;
                }
            });
        }

        Self {
            raft,
            local_engine,
            proposer_tx,
        }
    }

    pub fn raft(&self) -> &Raft<TypeConfig> {
        &self.raft
    }
}

fn to_raft_append(context: &str, request: &AppendRequest) -> RaftRequest {
    RaftRequest::Append {
        context: context.to_string(),
        events: request
            .events
            .iter()
            .map(RaftAppendEvent::from_event)
            .collect(),
        condition: convert_condition(request),
    }
}

fn convert_condition(request: &AppendRequest) -> Option<RaftAppendCondition> {
    request.condition.as_ref().map(|c| RaftAppendCondition {
        consistency_marker: c.consistency_marker.0,
        criteria: c
            .criteria
            .criteria
            .iter()
            .map(|cr| RaftCriterion {
                names: cr.names.clone(),
                tags: cr
                    .tags
                    .iter()
                    .map(|t| (t.key.clone(), t.value.clone()))
                    .collect(),
            })
            .collect(),
    })
}

/// Submits a request through consensus, forwarding to the leader when this
/// node is a follower.
async fn submit_raft_request(
    raft: &Raft<TypeConfig>,
    raft_req: RaftRequest,
) -> Result<RaftResponse, Error> {
    match raft.client_write(raft_req.clone()).await {
        Ok(resp) => Ok(resp.data),
        Err(e) => {
            let err_str = format!("{e}");
            if err_str.contains("forward request to") || err_str.contains("ForwardToLeader") {
                forward_write_to_leader(raft, &raft_req).await
            } else {
                Err(Error::Corrupted {
                    message: format!("raft write failed: {e}"),
                })
            }
        }
    }
}

fn map_append_response(response: RaftResponse) -> Result<AppendResponse, Error> {
    match response {
        RaftResponse::Append {
            first_position,
            count,
            consistency_marker,
        } => Ok(AppendResponse {
            first_position: Position(first_position),
            count,
            consistency_marker: Position(consistency_marker),
        }),
        RaftResponse::AppendRejected {
            reason:
                RaftRejectReason::ConsistencyConditionViolated {
                    conflicting_position,
                },
        } => Err(Error::ConsistencyConditionViolated {
            conflicting_position: Position(conflicting_position),
        }),
        _ => Err(Error::Corrupted {
            message: "unexpected raft response type for append".into(),
        }),
    }
}

/// Proposes one coalesced round of appends and fans the per-item results
/// back out to their waiting callers.
async fn propose_appends(raft: &Raft<TypeConfig>, context: &str, batch: Vec<ProposeItem>) {
    let (requests, senders): (Vec<AppendRequest>, Vec<_>) = batch.into_iter().unzip();

    let raft_req = if requests.len() == 1 {
        to_raft_append(context, &requests[0])
    } else {
        RaftRequest::AppendBatch {
            context: context.to_string(),
            items: requests
                .iter()
                .map(|request| super::types::BatchAppendItem {
                    events: request
                        .events
                        .iter()
                        .map(RaftAppendEvent::from_event)
                        .collect(),
                    condition: convert_condition(request),
                })
                .collect(),
        }
    };

    match submit_raft_request(raft, raft_req).await {
        Ok(RaftResponse::AppendBatch { results }) if results.len() == senders.len() => {
            for (sender, result) in senders.into_iter().zip(results) {
                let mapped = match result {
                    super::types::BatchAppendResult::Append {
                        first_position,
                        count,
                        consistency_marker,
                    } => Ok(AppendResponse {
                        first_position: Position(first_position),
                        count,
                        consistency_marker: Position(consistency_marker),
                    }),
                    super::types::BatchAppendResult::Rejected {
                        reason:
                            RaftRejectReason::ConsistencyConditionViolated {
                                conflicting_position,
                            },
                    } => Err(Error::ConsistencyConditionViolated {
                        conflicting_position: Position(conflicting_position),
                    }),
                };
                let _ = sender.send(mapped);
            }
        }
        Ok(response) if senders.len() == 1 => {
            let mapped = map_append_response(response);
            if let Some(sender) = senders.into_iter().next() {
                let _ = sender.send(mapped);
            }
        }
        Ok(other) => {
            for sender in senders {
                let _ = sender.send(Err(Error::Corrupted {
                    message: format!("unexpected raft response for append batch: {other:?}"),
                }));
            }
        }
        Err(e) => {
            let message = e.to_string();
            for sender in senders {
                let _ = sender.send(Err(Error::Corrupted {
                    message: message.clone(),
                }));
            }
        }
    }
}

/// Forwards a write to the current leader via the ForwardWrite RPC. Shared by
/// `RaftEngine::append` and `ClusterManager::create_context_replicated`.
async fn forward_write_to_leader(
    raft: &Raft<TypeConfig>,
    raft_req: &RaftRequest,
) -> Result<RaftResponse, Error> {
    let metrics = raft.metrics().borrow().clone();
    let leader_id = metrics.current_leader.ok_or_else(|| Error::Corrupted {
        message: "no leader available, try again later".into(),
    })?;

    let leader_node = metrics
        .membership_config
        .membership()
        .get_node(&leader_id)
        .ok_or_else(|| Error::Corrupted {
            message: format!("leader {leader_id} address not found in membership"),
        })?;

    let endpoint = format!("http://{}", leader_node.addr);
    let channel = Channel::from_shared(endpoint.clone())
        .map_err(|e| Error::Corrupted {
            message: format!("invalid leader endpoint: {e}"),
        })?
        .connect()
        .await
        .map_err(|e| Error::Corrupted {
            message: format!("connect to leader at {endpoint}: {e}"),
        })?;

    let mut client = RaftTransportClient::new(channel);

    let data = bincode::serialize(raft_req).map_err(|e| Error::Corrupted {
        message: format!("serialize forward request: {e}"),
    })?;

    let resp = client
        .forward_write(proto::ForwardWriteRequest { data })
        .await
        .map_err(|e| Error::Corrupted {
            message: format!("forward write to leader: {e}"),
        })?;

    let raft_resp: RaftResponse =
        bincode::deserialize(&resp.into_inner().data).map_err(|e| Error::Corrupted {
            message: format!("deserialize leader response: {e}"),
        })?;

    Ok(raft_resp)
}

#[async_trait::async_trait]
impl EventStore for RaftEngine {
    async fn append(&self, request: AppendRequest) -> Result<AppendResponse, Error> {
        // Hand off to the proposer, which coalesces concurrent appends into
        // one consensus round. DCB rejections come back as the same typed
        // error the direct (non-Raft) path returns (D-02) — wire contract
        // unchanged for connectors.
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.proposer_tx
            .send((request, tx))
            .map_err(|_| Error::Corrupted {
                message: "raft append proposer stopped".into(),
            })?;
        rx.await.map_err(|_| Error::Corrupted {
            message: "raft append proposer dropped the response".into(),
        })?
    }

    fn source(
        &self,
        from_position: Position,
        condition: &SourcingCondition,
    ) -> Result<Vec<SequencedEvent>, Error> {
        self.local_engine.source(from_position, condition)
    }

    fn source_page(
        &self,
        from_position: Position,
        condition: &SourcingCondition,
        up_to: Position,
        limit: usize,
    ) -> Result<Vec<SequencedEvent>, Error> {
        self.local_engine
            .source_page(from_position, condition, up_to, limit)
    }

    fn subscribe(&self, from_position: Position, condition: SourcingCondition) -> EventStream {
        self.local_engine.subscribe(from_position, condition)
    }

    fn head(&self) -> Position {
        self.local_engine.head()
    }

    fn tail(&self) -> Position {
        self.local_engine.tail()
    }

    fn get_tags(&self, position: Position) -> Result<Vec<Tag>, Error> {
        self.local_engine.get_tags(position)
    }

    fn get_sequence_at(&self, timestamp_millis: i64) -> Result<Option<Position>, Error> {
        self.local_engine.get_sequence_at(timestamp_millis)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::segment::DEFAULT_SEGMENT_SIZE;

    fn make_contexts(dir: &std::path::Path) -> Arc<ContextManager> {
        let ctx = Arc::new(ContextManager::new(dir, DEFAULT_SEGMENT_SIZE).unwrap());
        ctx.create_context("default").unwrap();
        ctx
    }

    fn single_node_config(addr: &str) -> ClusterConfig {
        // fast path OFF so these tests keep exercising the consensus path.
        ClusterConfig {
            node_id: 1,
            node_type: NodeType::Standard,
            advertise_addr: addr.into(),
            voters: vec![PeerConfig {
                id: 1,
                addr: addr.into(),
            }],
            learners: vec![],
            raft_config: super::super::types::default_raft_config(),
            single_node_fast_path: false,
            log_group_commit: None,
        }
    }

    fn fast_path_config(addr: &str) -> ClusterConfig {
        ClusterConfig {
            single_node_fast_path: true,
            ..single_node_config(addr)
        }
    }

    #[tokio::test]
    async fn single_node_init_and_get_store() {
        let dir = tempfile::tempdir().unwrap();
        let contexts = make_contexts(dir.path());

        let cluster =
            ClusterManager::new(Arc::clone(&contexts), single_node_config("127.0.0.1:50051"));
        assert!(!cluster.is_multi_node());

        cluster.init_context("default").await.unwrap();
        cluster.bootstrap().await.unwrap();

        let store = cluster.get_store("default").unwrap();
        assert_eq!(store.head(), Position(0));
    }

    #[tokio::test]
    async fn context_not_found_before_init() {
        let dir = tempfile::tempdir().unwrap();
        let contexts = make_contexts(dir.path());

        let cluster =
            ClusterManager::new(Arc::clone(&contexts), single_node_config("127.0.0.1:50051"));

        let result = cluster.get_store("default");
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn raft_node_returns_node_after_init() {
        let dir = tempfile::tempdir().unwrap();
        let contexts = make_contexts(dir.path());

        let cluster =
            ClusterManager::new(Arc::clone(&contexts), single_node_config("127.0.0.1:50051"));

        assert!(cluster.raft_node().is_none());

        cluster.init_context("default").await.unwrap();

        assert!(cluster.raft_node().is_some());
    }

    #[tokio::test]
    async fn non_default_context_append_goes_through_shared_raft() {
        let dir = tempfile::tempdir().unwrap();
        let contexts = make_contexts(dir.path());
        contexts.create_context("orders").unwrap();

        let cluster =
            ClusterManager::new(Arc::clone(&contexts), single_node_config("127.0.0.1:50051"));
        cluster.init_context("default").await.unwrap();
        cluster.init_context("orders").await.unwrap();
        cluster.bootstrap().await.unwrap();

        let store = cluster.get_store("orders").unwrap();
        let resp = store
            .append(AppendRequest {
                condition: None,
                events: vec![crate::event::AppendEvent {
                    identifier: "e1".into(),
                    timestamp: 1,
                    name: "TestEvent".into(),
                    version: "1.0".into(),
                    payload: vec![1],
                    metadata: Default::default(),
                    tags: vec![Tag {
                        key: b"orderId".to_vec(),
                        value: b"o1".to_vec(),
                    }],
                }],
            })
            .await
            .unwrap();
        assert_eq!(resp.count, 1);

        // The default context is untouched by the orders append.
        let default_store = cluster.get_store("default").unwrap();
        assert_eq!(default_store.head(), Position(0));
    }

    #[tokio::test]
    async fn runtime_created_context_is_writable_without_restart() {
        let dir = tempfile::tempdir().unwrap();
        let contexts = make_contexts(dir.path());

        let cluster =
            ClusterManager::new(Arc::clone(&contexts), single_node_config("127.0.0.1:50051"));
        cluster.init_context("default").await.unwrap();
        cluster.bootstrap().await.unwrap();

        // Created AFTER bootstrap, through consensus — like the admin API.
        cluster.create_context_replicated("late-ctx").await.unwrap();

        // get_store lazily registers the facade; the append must succeed.
        let store = cluster.get_store("late-ctx").unwrap();
        let resp = store
            .append(AppendRequest {
                condition: None,
                events: vec![crate::event::AppendEvent {
                    identifier: "e1".into(),
                    timestamp: 1,
                    name: "TestEvent".into(),
                    version: "1.0".into(),
                    payload: vec![1],
                    metadata: Default::default(),
                    tags: vec![],
                }],
            })
            .await
            .unwrap();
        assert_eq!(resp.count, 1);
    }

    /// Reopens a data dir after a "restart", waiting for the previous
    /// instance's fencing lock to release (the Raft core task drops its
    /// ContextManager Arc asynchronously after shutdown).
    async fn reopen_contexts(dir: &std::path::Path) -> Arc<ContextManager> {
        for _ in 0..100 {
            match ContextManager::new(dir, crate::segment::DEFAULT_SEGMENT_SIZE) {
                Ok(c) => return Arc::new(c),
                Err(_) => tokio::time::sleep(std::time::Duration::from_millis(50)).await,
            }
        }
        panic!("data dir fencing lock not released within 5s");
    }

    async fn wait_for_leader(cluster: &ClusterManager) {
        let raft = cluster.raft_node().expect("raft initialized");
        for _ in 0..100 {
            if raft.metrics().borrow().current_leader.is_some() {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        panic!("no leader elected within 10s");
    }

    fn test_append() -> AppendRequest {
        AppendRequest {
            condition: None,
            events: vec![crate::event::AppendEvent {
                identifier: "e1".into(),
                timestamp: 1,
                name: "TestEvent".into(),
                version: "1.0".into(),
                payload: vec![1],
                metadata: Default::default(),
                tags: vec![],
            }],
        }
    }

    /// Regression: a clean restart BEFORE the first Raft snapshot must not
    /// lose the voter set. Without membership.bin, the cluster-init
    /// Membership entry sits in the applied log region (never rescanned by
    /// openraft), the state machine hydrates an empty membership, and the
    /// node comes back as a Learner that can never elect — writes fail with
    /// "no leader available" forever.
    #[tokio::test]
    async fn clean_restart_before_first_snapshot_keeps_write_availability() {
        let dir = tempfile::tempdir().unwrap();

        {
            let contexts = make_contexts(dir.path());
            let cluster =
                ClusterManager::new(Arc::clone(&contexts), single_node_config("127.0.0.1:50051"));
            cluster.init_context("default").await.unwrap();
            cluster.bootstrap().await.unwrap();
            wait_for_leader(&cluster).await;
            let store = cluster.get_store("default").unwrap();
            store.append(test_append()).await.unwrap();
            let _ = cluster.raft_node().unwrap().shutdown().await;
        }

        // Restart: rediscover contexts from disk (no create_context).
        let contexts = reopen_contexts(dir.path()).await;
        let cluster =
            ClusterManager::new(Arc::clone(&contexts), single_node_config("127.0.0.1:50051"));
        cluster.init_context("default").await.unwrap();
        cluster.bootstrap().await.unwrap();
        wait_for_leader(&cluster).await;

        let store = cluster.get_store("default").unwrap();
        let resp = store.append(test_append()).await.unwrap();
        assert_eq!(resp.count, 1);
    }

    /// Same as above but for legacy data dirs written before membership.bin
    /// existed: the voter set must be recovered by scanning the unpurged log
    /// for the last Membership entry.
    #[tokio::test]
    async fn legacy_dir_without_membership_bin_recovers_via_log_scan() {
        let dir = tempfile::tempdir().unwrap();

        {
            let contexts = make_contexts(dir.path());
            let cluster =
                ClusterManager::new(Arc::clone(&contexts), single_node_config("127.0.0.1:50051"));
            cluster.init_context("default").await.unwrap();
            cluster.bootstrap().await.unwrap();
            wait_for_leader(&cluster).await;
            let store = cluster.get_store("default").unwrap();
            store.append(test_append()).await.unwrap();
            let _ = cluster.raft_node().unwrap().shutdown().await;
        }

        // Simulate a pre-fix data dir: membership.bin does not exist.
        let membership_file = dir
            .path()
            .join("default")
            .join("raft")
            .join("snapshots")
            .join("membership.bin");
        assert!(
            membership_file.exists(),
            "run 1 should have persisted membership.bin"
        );
        std::fs::remove_file(&membership_file).unwrap();

        let contexts = reopen_contexts(dir.path()).await;
        let cluster =
            ClusterManager::new(Arc::clone(&contexts), single_node_config("127.0.0.1:50051"));
        cluster.init_context("default").await.unwrap();
        cluster.bootstrap().await.unwrap();
        wait_for_leader(&cluster).await;

        let store = cluster.get_store("default").unwrap();
        let resp = store.append(test_append()).await.unwrap();
        assert_eq!(resp.count, 1);
    }

    /// Fast path: appends work WITHOUT Raft ever being initialized or
    /// bootstrapped — direct proof consensus is bypassed.
    #[tokio::test]
    async fn fast_path_appends_without_raft() {
        let dir = tempfile::tempdir().unwrap();
        let contexts = make_contexts(dir.path());

        let cluster =
            ClusterManager::new(Arc::clone(&contexts), fast_path_config("127.0.0.1:50051"));
        assert!(cluster.is_fast_path());
        // No init_raft, no bootstrap — get_store registers the local engine.
        let store = cluster.get_store("default").unwrap();
        let resp = store.append(test_append()).await.unwrap();
        assert_eq!(resp.count, 1);

        // Runtime creation works too, locally and idempotently.
        cluster.create_context_replicated("orders").await.unwrap();
        cluster.create_context_replicated("orders").await.unwrap();
        let orders = cluster.get_store("orders").unwrap();
        assert_eq!(orders.append(test_append()).await.unwrap().count, 1);
    }

    /// Any configured peer or learner disables the fast path, even when the
    /// flag is set — replication must never be silently skipped.
    #[test]
    fn fast_path_disabled_when_peers_configured() {
        let dir = tempfile::tempdir().unwrap();
        let contexts = make_contexts(dir.path());

        let mut cfg = fast_path_config("127.0.0.1:50051");
        cfg.voters.push(PeerConfig {
            id: 2,
            addr: "127.0.0.1:50052".into(),
        });
        let cluster = ClusterManager::new(Arc::clone(&contexts), cfg);
        assert!(!cluster.is_fast_path());

        let mut cfg = fast_path_config("127.0.0.1:50051");
        cfg.learners.push(PeerConfig {
            id: 3,
            addr: "127.0.0.1:50053".into(),
        });
        let cluster = ClusterManager::new(Arc::clone(&contexts), cfg);
        assert!(!cluster.is_fast_path());
    }

    /// Not a correctness test — prints a rough single-node timing comparison
    /// between the consensus path and the fast path. Run manually:
    /// `cargo test -p kronosdb-eventstore --lib raft::cluster -- --ignored --nocapture`
    #[tokio::test]
    #[ignore = "timing measurement, run manually"]
    async fn fast_path_vs_raft_timing() {
        const N: usize = 300;

        let dir = tempfile::tempdir().unwrap();
        let contexts = make_contexts(dir.path());
        let cluster =
            ClusterManager::new(Arc::clone(&contexts), single_node_config("127.0.0.1:50051"));
        cluster.init_context("default").await.unwrap();
        cluster.bootstrap().await.unwrap();
        wait_for_leader(&cluster).await;
        let store = cluster.get_store("default").unwrap();
        let start = std::time::Instant::now();
        for _ in 0..N {
            store.append(test_append()).await.unwrap();
        }
        let raft_elapsed = start.elapsed();

        let dir2 = tempfile::tempdir().unwrap();
        let contexts2 = make_contexts(dir2.path());
        let cluster2 =
            ClusterManager::new(Arc::clone(&contexts2), fast_path_config("127.0.0.1:50051"));
        let store2 = cluster2.get_store("default").unwrap();
        let start = std::time::Instant::now();
        for _ in 0..N {
            store2.append(test_append()).await.unwrap();
        }
        let fast_elapsed = start.elapsed();

        println!(
            "raft path: {N} appends in {raft_elapsed:?} ({:.0} appends/s)",
            N as f64 / raft_elapsed.as_secs_f64()
        );
        println!(
            "fast path: {N} appends in {fast_elapsed:?} ({:.0} appends/s)",
            N as f64 / fast_elapsed.as_secs_f64()
        );
    }

    /// Concurrent variant: 64 writers with the server-default 2ms group
    /// commit, where one fsync is amortized across every writer in the
    /// window. Run manually:
    /// `cargo test -p kronosdb-eventstore --release --lib fast_path_vs_raft_timing_concurrent -- --ignored --nocapture`
    #[tokio::test(flavor = "multi_thread", worker_threads = 32)]
    #[ignore = "timing measurement, run manually"]
    async fn fast_path_vs_raft_timing_concurrent_group_commit() {
        const WRITERS: usize = 64;
        const PER_WRITER: usize = 50;

        fn gc_contexts(dir: &std::path::Path) -> Arc<ContextManager> {
            let opts = crate::store::StoreOptions {
                group_commit_interval_ms: 2,
                ..Default::default()
            };
            let ctx = Arc::new(ContextManager::with_options(dir, opts).unwrap());
            ctx.create_context("default").unwrap();
            ctx
        }

        async fn run(store: Arc<dyn EventStore>) -> std::time::Duration {
            let start = std::time::Instant::now();
            let mut handles = Vec::new();
            for _ in 0..WRITERS {
                let s = Arc::clone(&store);
                handles.push(tokio::spawn(async move {
                    for _ in 0..PER_WRITER {
                        s.append(test_append()).await.unwrap();
                    }
                }));
            }
            for h in handles {
                h.await.unwrap();
            }
            start.elapsed()
        }

        let total = WRITERS * PER_WRITER;

        let dir = tempfile::tempdir().unwrap();
        let contexts = gc_contexts(dir.path());
        let cluster =
            ClusterManager::new(Arc::clone(&contexts), single_node_config("127.0.0.1:50051"));
        cluster.init_context("default").await.unwrap();
        cluster.bootstrap().await.unwrap();
        wait_for_leader(&cluster).await;
        let raft_elapsed = run(cluster.get_store("default").unwrap()).await;

        let dir2 = tempfile::tempdir().unwrap();
        let contexts2 = gc_contexts(dir2.path());
        let cluster2 =
            ClusterManager::new(Arc::clone(&contexts2), fast_path_config("127.0.0.1:50051"));
        let fast_elapsed = run(cluster2.get_store("default").unwrap()).await;

        println!(
            "raft path (group commit, {WRITERS} writers): {total} appends in {raft_elapsed:?} ({:.0} appends/s)",
            total as f64 / raft_elapsed.as_secs_f64()
        );
        println!(
            "fast path (group commit, {WRITERS} writers): {total} appends in {fast_elapsed:?} ({:.0} appends/s)",
            total as f64 / fast_elapsed.as_secs_f64()
        );
    }

    #[tokio::test]
    async fn create_context_replicated_rejects_invalid_name_before_proposing() {
        let dir = tempfile::tempdir().unwrap();
        let contexts = make_contexts(dir.path());

        let cluster =
            ClusterManager::new(Arc::clone(&contexts), single_node_config("127.0.0.1:50051"));
        cluster.init_context("default").await.unwrap();
        cluster.bootstrap().await.unwrap();

        // An invalid name must be rejected client-side — reaching the state
        // machine would be a fatal StorageError.
        assert!(cluster.create_context_replicated("bad/name").await.is_err());

        // The raft group stays healthy afterwards.
        cluster
            .create_context_replicated("good-name")
            .await
            .unwrap();
        assert!(cluster.get_store("good-name").is_ok());
    }

    #[tokio::test]
    async fn context_manager_accessible() {
        let dir = tempfile::tempdir().unwrap();
        let contexts = make_contexts(dir.path());
        let cluster =
            ClusterManager::new(Arc::clone(&contexts), single_node_config("127.0.0.1:50051"));

        assert!(cluster.context_manager().context_exists("default"));
    }

    /// DCB-03 regression guard: `conditional_propose_lock` was the propose-time
    /// TOCTOU guard added in 04e0cbf. This worktree started from 4dcffcd so the
    /// identifier is absent. Apply-time authority (Phase 3) makes the lock dead
    /// weight; reintroducing it would re-install the bug it was defending against.
    /// This test fails loudly if the identifier ever reappears in this file.
    #[test]
    fn dcb_03_guard_no_conditional_propose_lock_in_cluster_rs() {
        const SOURCE: &str = include_str!("cluster.rs");
        // We count occurrences of the identifier and require the count to equal
        // the known-good count for this test's own mentions (docstring, name,
        // assertion message). Any occurrence outside this test bumps the count
        // and fails the guard — which is the intended contract.
        let occurrences = SOURCE.matches("conditional_propose_lock").count();
        // This constant must be updated ONLY when changing this test's body.
        // The identifier appears here in the docstring, the function name, the
        // match-string literal, and the assertion message — total 5 mentions.
        const EXPECTED_SELF_MENTIONS: usize = 5;
        assert_eq!(
            occurrences, EXPECTED_SELF_MENTIONS,
            "DCB-03 regression: 'conditional_propose_lock' should appear exactly \
             {EXPECTED_SELF_MENTIONS} times (all inside dcb_03_guard_no_conditional_propose_lock_in_cluster_rs); \
             found {occurrences}. Apply-time authority makes this lock unnecessary — \
             do not reintroduce it."
        );
    }
}
