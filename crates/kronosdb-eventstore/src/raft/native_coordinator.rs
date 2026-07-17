//! Native replication coordination and fencing.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use openraft::{BasicNode, RaftMetrics};

use crate::error::Error;
use crate::event::Position;
use crate::replication::client::{TailClientConfig, catch_up_to, run_tail_session};
use crate::replication::proto as replication_proto;
use crate::replication::proto::segment_replication_client::SegmentReplicationClient;

use super::cluster::{ClusterManager, PeerConfig};
use super::types::{NodeId, RaftRequest, RaftResponse};

impl ClusterManager {
    /// Starts the node-wide coordinator that turns openraft elections into
    /// fenced native replication sessions. Idempotent.
    pub fn start_replication(self: &Arc<Self>) -> Result<(), Error> {
        if self.raft_node().is_none() {
            return Err(Error::Corrupted {
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
                    .ok_or_else(|| Error::Corrupted {
                        message: format!("active voter {id} has no peer address"),
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
        if voters.is_empty() {
            self.control.close_gate();
            return Ok(false);
        }

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
                    transport: self.cluster_config.peer_transport.clone(),
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
            return Err(Error::Corrupted {
                message: "local node is not an active voter".into(),
            });
        }
        let raft = self.raft_node().ok_or_else(|| Error::Corrupted {
            message: "metadata control plane is not initialized".into(),
        })?;
        let current = raft.metrics().borrow().clone();
        if current.current_leader != Some(self.cluster_config.node_id)
            || current.current_term != term
        {
            return Err(Error::Corrupted {
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
                    return Err(Error::Corrupted {
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
                return Err(Error::Corrupted {
                    message: "leadership changed while fencing prior Tail sessions".into(),
                });
            }
            if !self.sync_native_membership(&current).await?
                || self.control.voters() != voter_ids
                || self.control.voter_generation() != voter_generation
            {
                return Err(Error::Corrupted {
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
            return Err(Error::Corrupted {
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
            return Err(Error::Corrupted {
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
                    return Err(Error::Corrupted {
                        message: "leadership changed while confirming native data quorum".into(),
                    });
                }
                if !self.sync_native_membership(&current).await?
                    || self.control.voters() != voter_ids
                    || self.control.voter_generation() != voter_generation
                {
                    return Err(Error::Corrupted {
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
            return Err(Error::Corrupted {
                message: "leadership changed while activating native claim".into(),
            });
        }
        if !self
            .control
            .activate_local(epoch, term, voter_ids, voter_generation)
        {
            return Err(Error::Corrupted {
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
                },
            );
        }
        snapshots.push(VoterCursors {
            node_id: self.cluster_config.node_id,
            control_epoch: self.control.claim().map(|claim| claim.epoch).unwrap_or(0),
            contexts: local_contexts,
        });

        for peer in active_voters
            .iter()
            .filter(|peer| peer.id != self.cluster_config.node_id)
        {
            let result = async {
                let channel = self.cached_channel(&peer.addr).await?;
                let mut client = SegmentReplicationClient::new(channel)
                    .max_decoding_message_size(crate::replication::PEER_MAX_MESSAGE_BYTES)
                    .max_encoding_message_size(crate::replication::PEER_MAX_MESSAGE_BYTES);
                let request = self
                    .cluster_config
                    .peer_transport
                    .request(replication_proto::GetCursorsRequest {})?;
                let response = client
                    .get_cursors(request)
                    .await
                    .map_err(|error| Error::Corrupted {
                        message: format!("query native cursors from node {}: {error}", peer.id),
                    })?
                    .into_inner();
                if response.node_id != peer.id {
                    return Err(Error::Corrupted {
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
                        .into_iter()
                        .map(|cursor| {
                            (
                                cursor.context,
                                CursorSnapshot {
                                    position: cursor.durable_position,
                                },
                            )
                        })
                        .collect(),
                })
            };
            match tokio::time::timeout(Duration::from_secs(3), result).await {
                Ok(Ok(snapshot)) => snapshots.push(snapshot),
                Ok(Err(error)) => {
                    tracing::debug!(%error, node_id = peer.id, "voter cursor query failed")
                }
                Err(_) => tracing::debug!(node_id = peer.id, "voter cursor query timed out"),
            }
        }
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
        // client acknowledgement durable. Therefore at least one response has
        // every committed byte. Pull the longest same-epoch prefix, then, after
        // claiming, re-replicate it to a fresh quorum before opening writes.
        let safe_tail = snapshots
            .iter()
            .map(|snapshot| {
                snapshot
                    .contexts
                    .get(context)
                    .map(|cursor| cursor.position)
                    .unwrap_or(0)
            })
            .max()
            .unwrap_or(0);
        let local_tail = engine.local_tail().0;
        if local_tail == safe_tail || safe_tail == 0 {
            return Ok(safe_tail);
        }
        if local_tail > safe_tail {
            return Err(Error::Corrupted {
                message: format!(
                    "local context {context} tail {local_tail} exceeds the longest voter tail {safe_tail} in epoch {prior_epoch}"
                ),
            });
        }

        let source = snapshots
            .iter()
            .find(|snapshot| {
                snapshot.node_id != self.cluster_config.node_id
                    && snapshot
                        .contexts
                        .get(context)
                        .map(|cursor| cursor.position)
                        .unwrap_or(0)
                        >= safe_tail
            })
            .ok_or_else(|| Error::Corrupted {
                message: format!("no voter can source context {context} through {safe_tail}"),
            })?;
        let peer = active_voters
            .iter()
            .find(|peer| peer.id == source.node_id)
            .ok_or_else(|| Error::Corrupted {
                message: format!(
                    "cursor source {} is absent from voter config",
                    source.node_id
                ),
            })?;
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
                transport: self.cluster_config.peer_transport.clone(),
            },
            Position(safe_tail),
        )
        .await?;
        Ok(safe_tail)
    }
}

async fn abort_tasks(tasks: &mut Vec<tokio::task::JoinHandle<()>>) {
    let tasks: Vec<_> = tasks.drain(..).collect();
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
}
