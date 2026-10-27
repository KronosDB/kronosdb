//! Native replication coordination and fencing.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use openraft::{BasicNode, RaftMetrics};

use crate::error::Error;
use crate::event::Position;
use crate::replication::client::{TailClientConfig, catch_up_to, run_tail_session};
use crate::wire;
use kronosdb_protocol as p;

use super::cluster::{ClusterManager, PeerConfig};
use super::types::{NodeId, RaftRequest, RaftResponse};

impl ClusterManager {
    /// Starts the node-wide coordinator that turns openraft elections into
    /// fenced native replication sessions. Idempotent.
    pub fn start_replication(self: &Arc<Self>) -> Result<(), Error> {
        if self.raft_node().is_none() {
            return Err(Error::Unavailable {
                message: "start_replication called before init_raft".into(),
            });
        }
        if self.coordinator_started.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        let manager = Arc::clone(self);
        tokio::spawn(async move {
            manager.run_replication_coordinator().await;
        });
        let checkpoint_manager = Arc::clone(self);
        tokio::spawn(async move {
            checkpoint_manager.run_watermark_checkpoints().await;
        });
        Ok(())
    }

    /// Starts the ADR-0002 stage-1 backup loop. Runs on every node but only
    /// the claimed leader uploads; after failover the new leader continues
    /// from the shared manifest. Object-store failures are logged and
    /// retried next interval — they can never touch the write path.
    pub fn start_backup(self: &Arc<Self>, config: crate::tier::TierConfig) -> Result<(), Error> {
        let archiver = crate::tier::Archiver::from_url(&config.url)?;
        let manager = Arc::clone(self);
        tokio::spawn(async move {
            manager.run_backup_loop(archiver, config.interval).await;
        });
        Ok(())
    }

    async fn run_backup_loop(self: Arc<Self>, archiver: crate::tier::Archiver, interval: Duration) {
        loop {
            tokio::time::sleep(interval).await;
            let Some(claim) = self.control.claim() else {
                continue;
            };
            if !claim.writable || claim.leader_id != self.cluster_config.node_id {
                continue;
            }
            for context in self.context_manager.list_contexts() {
                let Ok(engine) = self.context_manager.get_context(&context) else {
                    continue;
                };
                match archiver.run_backup_pass(&context, &engine).await {
                    Ok(report) if report.uploaded > 0 => {
                        tracing::info!(
                            context,
                            uploaded = report.uploaded,
                            archived_total = report.uploaded + report.already_archived,
                            "backup pass shipped sealed segments"
                        );
                    }
                    Ok(_) => {}
                    Err(error) => {
                        tracing::warn!(%error, context, "backup pass failed; retrying next interval");
                    }
                }
            }
        }
    }

    /// Starts the ADR-0003 scheduler. Every node folds the schedule
    /// projection from its local log, but only the claimed leader fires, so
    /// failover needs no recovery pass — the new leader's view is already
    /// current and it simply starts firing. A fire that a dying leader
    /// already committed is refused by its own DCB guard, so the handover
    /// cannot double-fire.
    pub fn start_scheduler(self: &Arc<Self>, tick: Duration) {
        let manager = Arc::clone(self);
        tokio::spawn(async move {
            manager.run_scheduler_loop(tick).await;
        });
    }

    async fn run_scheduler_loop(self: Arc<Self>, tick: Duration) {
        let mut projections: HashMap<String, crate::scheduler::Projection> = HashMap::new();
        loop {
            tokio::time::sleep(tick).await;

            let leading = self.is_writable_leader();

            for context in self.context_manager.list_contexts() {
                let Ok(engine) = self.context_manager.get_context(&context) else {
                    continue;
                };
                let projection = projections.entry(context.clone()).or_default();

                // Folded on every node so a new leader starts warm.
                if let Err(error) = projection.advance(&engine) {
                    tracing::warn!(%error, context, "schedule projection stalled; retrying");
                    continue;
                }
                if !leading {
                    continue;
                }

                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_millis() as i64)
                    .unwrap_or(0);

                for live in projection.due(now) {
                    match crate::scheduler::fire(&engine, &live) {
                        Ok(()) => tracing::debug!(
                            context,
                            token = live.token,
                            event = live.target.name,
                            "scheduled event fired"
                        ),
                        // The schedule resolved under us — cancelled, or
                        // already fired by the leader we took over from.
                        Err(Error::ConsistencyConditionViolated { .. }) => tracing::debug!(
                            context,
                            token = live.token,
                            "scheduled event already resolved; skipping"
                        ),
                        // Transient: the guard makes a retry next tick safe.
                        Err(error) => tracing::warn!(
                            %error, context, token = live.token,
                            "scheduled event failed to fire; retrying next tick"
                        ),
                    }
                }
            }
        }
    }

    async fn run_watermark_checkpoints(self: Arc<Self>) {
        let mut last = HashMap::new();
        loop {
            tokio::time::sleep(Duration::from_secs(5)).await;
            let Some(claim) = self.control.claim() else {
                continue;
            };
            if !claim.writable
                || claim.leader_id != self.cluster_config.node_id
                || self.control.voters().len() == 1
            {
                continue;
            }
            for context in self.context_manager.list_contexts() {
                let Ok(engine) = self.context_manager.get_context(&context) else {
                    continue;
                };
                let watermark = engine.head().0;
                if last.get(&context).copied().unwrap_or(0) >= watermark {
                    continue;
                }
                match tokio::task::spawn_blocking(move || engine.persist_watermark_checkpoint())
                    .await
                {
                    Ok(Ok(())) => {
                        last.insert(context, watermark);
                    }
                    Ok(Err(error)) => tracing::debug!(%error, "watermark checkpoint deferred"),
                    Err(error) => tracing::warn!(%error, "watermark checkpoint worker panicked"),
                }
            }
        }
    }

    /// Mirrors OpenRaft's committed uniform membership into the native data
    /// plane on every node. Joint consensus cannot be represented as one
    /// majority set, so the native write gate remains closed until OpenRaft
    /// publishes the final uniform configuration.
    async fn sync_native_membership(
        &self,
        metrics: &RaftMetrics<NodeId, BasicNode>,
    ) -> Result<bool, Error> {
        let stored = metrics.membership_config.as_ref();
        let membership = stored.membership();
        match membership.get_joint_config().len() {
            0 => {
                self.control.close_gate();
                return Ok(false);
            }
            1 => {}
            _ => {
                self.control.close_gate();
                return Ok(false);
            }
        }

        let voters = stored
            .voter_ids()
            .map(|id| {
                membership
                    .get_node(&id)
                    .map(|node| PeerConfig {
                        id,
                        addr: node.addr.clone(),
                    })
                    .ok_or_else(|| Error::Unavailable {
                        message: format!("active voter {id} has no peer address"),
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
        if voters.is_empty() {
            self.control.close_gate();
            return Ok(false);
        }

        // Learners are mirrored so the replication service admits their Tail
        // sessions; they never enter quorum math or the voter generation.
        let learner_ids: Vec<u64> = membership.learner_ids().collect();
        self.control.set_learners(learner_ids);

        let _topology_guard = self.topology_lock.lock().await;
        if *self.active_voters.read() != voters {
            let voter_ids = voters.iter().map(|peer| peer.id).collect();
            {
                let mut known = self.known_peers.write();
                for peer in &voters {
                    known.insert(peer.id, peer.clone());
                }
            }
            *self.active_voters.write() = voters;
            self.control.set_voters(voter_ids);
        }
        Ok(true)
    }

    async fn run_replication_coordinator(self: Arc<Self>) {
        let Some(raft) = self.raft_node() else {
            return;
        };
        let mut metrics = raft.metrics();
        let mut applied = self.control_updates.subscribe();
        let mut follower_key: Option<(u64, u64, Vec<String>)> = None;
        let mut follower_tasks = Vec::new();

        loop {
            let current = metrics.borrow().clone();
            let committed_claim = applied.borrow().leader_claim.clone();
            if let Some(claim) = committed_claim.as_ref() {
                self.control
                    .observe_claim(claim.epoch, claim.node_id, claim.term);
            }
            let membership_ready = match self.sync_native_membership(&current).await {
                Ok(ready) => ready,
                Err(error) => {
                    self.control.close_gate();
                    tracing::warn!(%error, "failed to synchronize native voter membership");
                    false
                }
            };
            if !membership_ready {
                abort_tasks(&mut follower_tasks).await;
                follower_key = None;
                tokio::select! {
                    changed = metrics.changed() => {
                        if changed.is_err() {
                            break;
                        }
                    }
                    changed = applied.changed() => {
                        if changed.is_err() {
                            break;
                        }
                    }
                    () = tokio::time::sleep(Duration::from_millis(250)) => {}
                }
                continue;
            }

            let leader_id = current.current_leader;
            let term = current.current_term;

            if leader_id == Some(self.cluster_config.node_id) {
                abort_tasks(&mut follower_tasks).await;
                follower_key = None;
                let claimed_contexts_ready = committed_claim
                    .as_ref()
                    .map(|claim| {
                        claim.node_id == self.cluster_config.node_id
                            && claim.term == term
                            && claim.voters == self.control.voters()
                            && self
                                .context_manager
                                .list_contexts()
                                .iter()
                                .all(|context| claim.per_context_tails.contains_key(context))
                    })
                    .unwrap_or(false);
                let active = self.control.claim().map(|claim| {
                    claim.writable
                        && claim.leader_id == self.cluster_config.node_id
                        && claim.term == term
                        && claim.voter_generation == self.control.voter_generation()
                        && claimed_contexts_ready
                });
                if active != Some(true) {
                    self.control.close_gate();
                    if let Err(error) = self.establish_local_claim(term).await {
                        self.control.close_gate();
                        tracing::warn!(%error, term, "native leader claim not yet ready");
                    }
                }
            } else {
                self.control.close_gate();
                match committed_claim {
                    Some(claim)
                        if claim.node_id != self.cluster_config.node_id
                            && leader_id == Some(claim.node_id)
                            && claim.term == term
                            && claim.voters == self.control.voters() =>
                    {
                        let mut contexts = self.context_manager.list_contexts();
                        contexts.sort();
                        let key = (claim.epoch, claim.node_id, contexts.clone());
                        if follower_key.as_ref() != Some(&key) {
                            abort_tasks(&mut follower_tasks).await;
                            self.control
                                .observe_claim(claim.epoch, claim.node_id, claim.term);
                            follower_tasks =
                                self.start_follower_sessions(claim.epoch, claim.node_id, contexts);
                            follower_key = Some(key);
                        }
                    }
                    _ => {
                        abort_tasks(&mut follower_tasks).await;
                        follower_key = None;
                    }
                }
            }

            tokio::select! {
                changed = metrics.changed() => {
                    if changed.is_err() {
                        break;
                    }
                }
                changed = applied.changed() => {
                    if changed.is_err() {
                        break;
                    }
                }
                () = tokio::time::sleep(Duration::from_millis(500)) => {}
            }
        }

        abort_tasks(&mut follower_tasks).await;
        self.control.close_gate();
    }

    fn start_follower_sessions(
        &self,
        epoch: u64,
        leader_id: u64,
        contexts: Vec<String>,
    ) -> Vec<tokio::task::JoinHandle<()>> {
        let active_voters = self.active_voters.read().clone();
        let Some(leader) = active_voters
            .iter()
            .find(|peer| peer.id == leader_id)
            .cloned()
        else {
            tracing::error!(leader_id, "claimed leader is absent from voter config");
            return Vec::new();
        };
        let voters: Vec<_> = active_voters.iter().map(|peer| peer.id).collect();

        contexts
            .into_iter()
            .filter_map(|context| {
                let engine = match self.context_manager.get_context(&context) {
                    Ok(engine) => engine,
                    Err(error) => {
                        tracing::warn!(%error, %context, "cannot start follower Tail session");
                        return None;
                    }
                };
                let config = TailClientConfig {
                    context: context.clone(),
                    follower_id: self.cluster_config.node_id,
                    leader_id,
                    leader_addr: leader.addr.clone(),
                    epoch,
                    voters: voters.clone(),
                    recovery_claim: None,
                    peers: Arc::clone(&self.peers),
                };
                let control = Arc::clone(&self.control);
                Some(tokio::spawn(async move {
                    loop {
                        let current = control.claim();
                        if current
                            .map(|claim| claim.epoch != epoch || claim.leader_id != leader_id)
                            .unwrap_or(true)
                        {
                            return;
                        }
                        if let Err(error) = run_tail_session(Arc::clone(&engine), config.clone()).await {
                            tracing::debug!(%error, %context, leader_id, epoch, "Tail session reconnecting");
                        }
                        tokio::time::sleep(Duration::from_millis(250)).await;
                    }
                }))
            })
            .collect()
    }

    async fn establish_local_claim(&self, term: u64) -> Result<(), Error> {
        let topology_guard = self.topology_lock.lock().await;
        let active_voters = self.active_voters.read().clone();
        let voter_ids: Vec<_> = active_voters.iter().map(|peer| peer.id).collect();
        let voter_generation = self.control.voter_generation();
        if !voter_ids.contains(&self.cluster_config.node_id) {
            return Err(Error::Unavailable {
                message: "local node is not an active voter".into(),
            });
        }
        let raft = self.raft_node().ok_or_else(|| Error::Unavailable {
            message: "metadata control plane is not initialized".into(),
        })?;
        let current = raft.metrics().borrow().clone();
        if current.current_leader != Some(self.cluster_config.node_id)
            || current.current_term != term
        {
            return Err(Error::Unavailable {
                message: "leadership changed before native fencing claim".into(),
            });
        }

        let contexts = self.context_manager.list_contexts();
        let committed = self.control_updates.borrow().leader_claim.clone();
        let resumable = committed.as_ref().filter(|claim| {
            claim.node_id == self.cluster_config.node_id
                && claim.term == term
                && claim.voters == voter_ids
                && contexts
                    .iter()
                    .all(|context| claim.per_context_tails.contains_key(context))
        });
        let prior_epoch = resumable
            .map(|claim| claim.prior_epoch)
            .unwrap_or_else(|| committed.as_ref().map(|claim| claim.epoch).unwrap_or(0));
        let resumed_epoch = resumable.map(|claim| claim.epoch);

        for context in &contexts {
            let engine = self.context_manager.get_context(context)?;
            engine.drain_pending()?;
            let engine_epoch = engine.replication_epoch();
            if engine_epoch < prior_epoch {
                engine.begin_replication_epoch(prior_epoch, voter_ids.clone())?;
            } else if engine_epoch > resumed_epoch.unwrap_or(prior_epoch) {
                return Err(Error::Corrupted {
                    message: format!(
                        "context {context} recovered epoch {engine_epoch} beyond active claim {}",
                        resumed_epoch.unwrap_or(prior_epoch)
                    ),
                });
            }
        }

        // Reuse a same-term claim after transient failure or restart. Allocating
        // another metadata epoch would discard the only durable description of
        // which prior epoch is being reconciled.
        let epoch = if let Some(epoch) = resumed_epoch {
            epoch
        } else {
            // Commit the new claim before reading data cursors. Once a voter has
            // observed this metadata entry it has stopped acknowledging the prior
            // leader, closing the election/cursor-query race.
            let preliminary_tails = contexts
                .iter()
                .map(|context| {
                    self.context_manager
                        .get_context(context)
                        .map(|engine| (context.clone(), engine.durable_tail().0))
                })
                .collect::<Result<BTreeMap<_, _>, _>>()?;
            let response = self
                .submit_control_request(RaftRequest::LeaderClaim {
                    node_id: self.cluster_config.node_id,
                    term,
                    prior_epoch,
                    voters: voter_ids.clone(),
                    per_context_tails: preliminary_tails,
                })
                .await?;
            match response {
                RaftResponse::LeaderClaimed { epoch } => epoch,
                other => {
                    return Err(Error::Internal {
                        message: format!("unexpected leader-claim response: {other:?}"),
                    });
                }
            }
        };
        self.control
            .observe_claim(epoch, self.cluster_config.node_id, term);
        drop(topology_guard);

        let quorum = active_voters.len() / 2 + 1;
        let snapshots = loop {
            let mut snapshots = self.collect_voter_cursors(&active_voters).await?;
            snapshots.retain(|snapshot| snapshot.control_epoch == epoch);
            if snapshots.len() >= quorum {
                break snapshots;
            }

            let current = raft.metrics().borrow().clone();
            if current.current_leader != Some(self.cluster_config.node_id)
                || current.current_term != term
            {
                return Err(Error::Unavailable {
                    message: "leadership changed while fencing prior Tail sessions".into(),
                });
            }
            if !self.sync_native_membership(&current).await?
                || self.control.voters() != voter_ids
                || self.control.voter_generation() != voter_generation
            {
                return Err(Error::Unavailable {
                    message: "voter membership changed while fencing prior Tail sessions".into(),
                });
            }
            tracing::debug!(
                epoch,
                responses = snapshots.len(),
                quorum,
                "waiting for voters to observe native fencing claim"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        };

        let mut safe_tails = BTreeMap::new();
        for context in &contexts {
            let safe_tail = self
                .reconcile_context_for_claim(
                    context,
                    prior_epoch,
                    epoch,
                    &snapshots,
                    &active_voters,
                )
                .await?;
            safe_tails.insert(context.clone(), safe_tail);
        }

        let current = raft.metrics().borrow().clone();
        if current.current_leader != Some(self.cluster_config.node_id)
            || current.current_term != term
        {
            return Err(Error::Unavailable {
                message: "leadership changed during native catch-up".into(),
            });
        }

        // Membership cannot change between installing the epoch in engines and
        // publishing their exact voter sets. The guard is released before the
        // potentially unbounded fresh-quorum wait below.
        let epoch_guard = self.topology_lock.lock().await;
        if *self.active_voters.read() != active_voters
            || self.control.voters() != voter_ids
            || self.control.voter_generation() != voter_generation
        {
            return Err(Error::Unavailable {
                message: "voter membership changed before persisting EpochChange".into(),
            });
        }
        for context in &contexts {
            let engine = self.context_manager.get_context(context)?;
            match engine.replication_epoch().cmp(&epoch) {
                std::cmp::Ordering::Less => {
                    engine.begin_replication_epoch(epoch, voter_ids.clone())?;
                    engine.persist_epoch_change(epoch, self.cluster_config.node_id)?;
                }
                std::cmp::Ordering::Equal => {}
                std::cmp::Ordering::Greater => {
                    return Err(Error::Corrupted {
                        message: format!(
                            "context {context} epoch {} exceeds active claim {epoch}",
                            engine.replication_epoch()
                        ),
                    });
                }
            }
            engine.acknowledge_replica(self.cluster_config.node_id, epoch, engine.durable_tail());
        }
        drop(epoch_guard);

        // The longest live prefix may have existed on only one old voter. It
        // is safe to preserve, but it does not become externally visible in
        // the new epoch until Tail acknowledgements put it on a fresh quorum.
        for context in &contexts {
            let engine = self.context_manager.get_context(context)?;
            let target = Position(safe_tails[context]);
            while engine.head().0 < target.0 {
                let current = raft.metrics().borrow().clone();
                if current.current_leader != Some(self.cluster_config.node_id)
                    || current.current_term != term
                {
                    return Err(Error::Unavailable {
                        message: "leadership changed while confirming native data quorum".into(),
                    });
                }
                if !self.sync_native_membership(&current).await?
                    || self.control.voters() != voter_ids
                    || self.control.voter_generation() != voter_generation
                {
                    return Err(Error::Unavailable {
                        message: "voter membership changed while confirming native data quorum"
                            .into(),
                    });
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }

        let current = raft.metrics().borrow().clone();
        if current.current_leader != Some(self.cluster_config.node_id)
            || current.current_term != term
        {
            return Err(Error::Unavailable {
                message: "leadership changed while activating native claim".into(),
            });
        }
        if !self
            .control
            .activate_local(epoch, term, voter_ids, voter_generation)
        {
            return Err(Error::Unavailable {
                message: "voter membership changed while activating native leader claim".into(),
            });
        }
        tracing::info!(epoch, term, "native leader gate opened");
        Ok(())
    }

    async fn collect_voter_cursors(
        &self,
        active_voters: &[PeerConfig],
    ) -> Result<Vec<VoterCursors>, Error> {
        let mut snapshots = Vec::new();
        let mut local_contexts = HashMap::new();
        for context in self.context_manager.list_contexts() {
            let engine = self.context_manager.get_context(&context)?;
            local_contexts.insert(
                context,
                CursorSnapshot {
                    position: engine.local_tail().0,
                    log_epoch: engine.log_epoch(),
                },
            );
        }
        snapshots.push(VoterCursors {
            node_id: self.cluster_config.node_id,
            control_epoch: self.control.claim().map(|claim| claim.epoch).unwrap_or(0),
            contexts: local_contexts,
        });

        // Every peer at once: failover waits for the slowest peer (or one
        // 3 s timeout), not for the sum of them.
        let queries = active_voters
            .iter()
            .filter(|peer| peer.id != self.cluster_config.node_id)
            .map(|peer| async move {
                let result = async {
                    let reply = self
                        .peers
                        .request(&peer.addr, &p::Frame::GetCursors(p::GetCursors {}))
                        .await
                        .map_err(|error| {
                            wire::with_context(
                                error,
                                format!("query native cursors from node {}", peer.id),
                            )
                        })?;
                    let Ok(p::Frame::Cursors(response)) = reply.decode() else {
                        return Err(Error::Internal {
                            message: format!(
                                "node {} answered a cursor query with something else",
                                peer.id
                            ),
                        });
                    };
                    if response.node_id != peer.id {
                        return Err(Error::Unavailable {
                            message: format!(
                                "cursor endpoint for node {} identified itself as {}",
                                peer.id, response.node_id
                            ),
                        });
                    }
                    Ok(VoterCursors {
                        node_id: response.node_id,
                        control_epoch: response.control_epoch,
                        contexts: response
                            .contexts
                            .iter()
                            .map(|cursor| {
                                (
                                    cursor.context.to_string(),
                                    CursorSnapshot {
                                        position: cursor.durable_position,
                                        log_epoch: cursor.log_epoch,
                                    },
                                )
                            })
                            .collect(),
                    })
                };
                match tokio::time::timeout(Duration::from_secs(3), result).await {
                    Ok(Ok(snapshot)) => Some(snapshot),
                    Ok(Err(error)) => {
                        tracing::debug!(%error, node_id = peer.id, "voter cursor query failed");
                        None
                    }
                    Err(_) => {
                        tracing::debug!(node_id = peer.id, "voter cursor query timed out");
                        None
                    }
                }
            });
        snapshots.extend(
            futures_util::future::join_all(queries)
                .await
                .into_iter()
                .flatten(),
        );
        Ok(snapshots)
    }

    async fn reconcile_context_for_claim(
        &self,
        context: &str,
        prior_epoch: u64,
        claim_epoch: u64,
        snapshots: &[VoterCursors],
        active_voters: &[PeerConfig],
    ) -> Result<u64, Error> {
        // A context already carrying this claim's EpochChange completed its
        // prior-epoch reconciliation before that record was written. Keep that
        // durable decision on resume; later responders may expose a longer but
        // necessarily unacknowledged old suffix.
        let engine = self.context_manager.get_context(context)?;
        if engine.replication_epoch() == claim_epoch {
            return Ok(engine.durable_tail().0);
        }
        if engine.replication_epoch() != prior_epoch {
            return Err(Error::Corrupted {
                message: format!(
                    "context {context} is at epoch {}, expected {prior_epoch} or {claim_epoch}",
                    engine.replication_epoch()
                ),
            });
        }

        // Any live voter quorum intersects the quorum that made every prior
        // client acknowledgement durable, so some responder holds every
        // committed byte. Which one is the one whose log is newest the way
        // Raft ranks logs: by the epoch its last EpochChange names, then by
        // length. Tails of different epochs are not comparable: a deposed
        // leader's unacknowledged suffix is longer than the acknowledged
        // events that replaced it. The claimant keeps its own log when it is
        // the best, and otherwise takes the best voter's (cutting back its own
        // suffix to where its epoch ends there, never below its watermark),
        // then re-replicates it to a fresh quorum before opening writes.
        let candidate = |node_id: NodeId, cursor: Option<&CursorSnapshot>| ClaimCandidate {
            node_id,
            log_epoch: cursor.map(|cursor| cursor.log_epoch).unwrap_or(0),
            tail: cursor.map(|cursor| cursor.position).unwrap_or(0),
        };
        let local = ClaimCandidate {
            node_id: self.cluster_config.node_id,
            log_epoch: engine.log_epoch(),
            tail: engine.local_tail().0,
        };
        let peers: Vec<_> = snapshots
            .iter()
            .filter(|snapshot| snapshot.node_id != self.cluster_config.node_id)
            .map(|snapshot| candidate(snapshot.node_id, snapshot.contexts.get(context)))
            .collect();
        let source = match choose_claim_source(local, &peers) {
            ClaimSource::Local => return Ok(local.tail),
            ClaimSource::Peer(source) => source,
        };
        let peer = active_voters
            .iter()
            .find(|peer| peer.id == source.node_id)
            .ok_or_else(|| Error::Unavailable {
                message: format!(
                    "cursor source {} is absent from voter config",
                    source.node_id
                ),
            })?;
        tracing::info!(
            context,
            source = source.node_id,
            source_epoch = source.log_epoch,
            source_tail = source.tail,
            local_epoch = local.log_epoch,
            local_tail = local.tail,
            "claimant is not the newest log; taking the best voter's before claiming"
        );
        catch_up_to(
            Arc::clone(&engine),
            TailClientConfig {
                context: context.to_string(),
                follower_id: self.cluster_config.node_id,
                leader_id: source.node_id,
                leader_addr: peer.addr.clone(),
                epoch: prior_epoch,
                voters: active_voters.iter().map(|peer| peer.id).collect(),
                recovery_claim: Some((claim_epoch, self.cluster_config.node_id)),
                peers: Arc::clone(&self.peers),
            },
            Position(source.tail),
        )
        .await?;
        Ok(source.tail)
    }
}

/// One voter's log for a context, as far as ranking it goes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ClaimCandidate {
    node_id: NodeId,
    /// The epoch of the newest `EpochChange` in its log.
    log_epoch: u64,
    /// Its next event position.
    tail: u64,
}

#[derive(Debug, PartialEq, Eq)]
enum ClaimSource {
    /// The claimant's own log is the newest (or tied): it keeps it.
    Local,
    /// Another voter's log is newer: take it.
    Peer(ClaimCandidate),
}

/// Picks the log a claimant builds on: the greatest `(log_epoch, tail)`,
/// which is Raft's "last term, then index" rule with epochs for terms. A
/// voter in a later epoch wins over one with a longer tail, because that
/// tail is a suffix its epoch's successor has already replaced. Ties keep the
/// claimant's own log, then the lowest node id, so the choice is stable.
fn choose_claim_source(local: ClaimCandidate, peers: &[ClaimCandidate]) -> ClaimSource {
    let rank = |candidate: &ClaimCandidate| (candidate.log_epoch, candidate.tail);
    let best = peers
        .iter()
        .copied()
        .min_by_key(|peer| (std::cmp::Reverse(rank(peer)), peer.node_id));
    match best {
        Some(best) if rank(&best) > rank(&local) => ClaimSource::Peer(best),
        _ => ClaimSource::Local,
    }
}

async fn abort_tasks(tasks: &mut Vec<tokio::task::JoinHandle<()>>) {
    let tasks: Vec<_> = std::mem::take(tasks);
    for task in &tasks {
        task.abort();
    }
    for task in tasks {
        let _ = task.await;
    }
}

struct VoterCursors {
    node_id: NodeId,
    control_epoch: u64,
    contexts: HashMap<String, CursorSnapshot>,
}

struct CursorSnapshot {
    position: u64,
    log_epoch: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn voter(node_id: NodeId, log_epoch: u64, tail: u64) -> ClaimCandidate {
        ClaimCandidate {
            node_id,
            log_epoch,
            tail,
        }
    }

    #[test]
    fn the_claimant_keeps_its_log_when_nobody_is_ahead() {
        let local = voter(1, 4, 10);
        assert_eq!(
            choose_claim_source(local, &[voter(2, 4, 8), voter(3, 3, 12)]),
            ClaimSource::Local,
            "a longer tail in an older epoch is a suffix that was replaced"
        );
        assert_eq!(choose_claim_source(local, &[]), ClaimSource::Local);
    }

    #[test]
    fn a_voter_with_a_longer_tail_in_the_same_epoch_is_ahead() {
        let local = voter(1, 4, 10);
        assert_eq!(
            choose_claim_source(local, &[voter(2, 4, 12), voter(3, 4, 11)]),
            ClaimSource::Peer(voter(2, 4, 12))
        );
    }

    #[test]
    fn a_voter_in_a_later_epoch_wins_over_a_longer_tail() {
        // The deposed leader holds events 5 and 6 nobody acknowledged; the
        // survivor holds one event the next leader acknowledged.
        let deposed = voter(1, 1, 7);
        let survivor = voter(2, 4, 6);
        assert_eq!(
            choose_claim_source(deposed, &[survivor]),
            ClaimSource::Peer(survivor)
        );
    }

    #[test]
    fn a_longer_tail_does_not_beat_a_later_epoch_among_peers_either() {
        let local = voter(1, 2, 5);
        assert_eq!(
            choose_claim_source(local, &[voter(2, 3, 5), voter(3, 2, 50)]),
            ClaimSource::Peer(voter(2, 3, 5))
        );
    }

    #[test]
    fn ties_keep_the_claimants_log_and_then_the_lowest_node() {
        let local = voter(2, 4, 10);
        assert_eq!(
            choose_claim_source(local, &[voter(1, 4, 10), voter(3, 4, 10)]),
            ClaimSource::Local
        );
        let local = voter(1, 3, 1);
        assert_eq!(
            choose_claim_source(local, &[voter(3, 4, 10), voter(2, 4, 10)]),
            ClaimSource::Peer(voter(2, 4, 10))
        );
    }

    #[test]
    fn a_voter_without_the_context_ranks_last() {
        let local = voter(1, 0, 0);
        assert_eq!(
            choose_claim_source(local, &[voter(2, 0, 0)]),
            ClaimSource::Local
        );
    }
}
