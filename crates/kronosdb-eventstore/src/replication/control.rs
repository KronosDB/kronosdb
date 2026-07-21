//! Shared claimed-leader gate for the native data plane.

use std::sync::Arc;

use parking_lot::RwLock;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ActiveClaim {
    pub epoch: u64,
    pub leader_id: u64,
    pub term: u64,
    /// Local membership generation under which this claim was observed.
    pub voter_generation: u64,
    /// True only after the leader has caught up, committed its claim, and
    /// durably written EpochChange in every context.
    pub writable: bool,
}

/// One node-wide view of the metadata control plane. Event appends and
/// forwarding are fenced against this gate, not against openraft's transient
/// `current_leader` alone.
pub struct ReplicationControl {
    node_id: u64,
    voters: RwLock<Vec<u64>>,
    /// Non-voting cluster members. Learners run full Tail sessions and ack
    /// cursors, but never count toward the watermark quorum and never change
    /// the voter generation or the write gate.
    learners: RwLock<Vec<u64>>,
    voter_generation: std::sync::atomic::AtomicU64,
    claim: tokio::sync::watch::Sender<Option<ActiveClaim>>,
    /// Latest durably-acked replication cursor per (context, follower),
    /// recorded on the claimed leader for every session including learners.
    /// Informational: promotion gating and operator visibility, never quorum.
    progress: RwLock<std::collections::BTreeMap<(String, u64), u64>>,
}

impl ReplicationControl {
    pub fn new(node_id: u64, mut voters: Vec<u64>) -> Arc<Self> {
        voters.sort_unstable();
        voters.dedup();
        let (claim, _) = tokio::sync::watch::channel(None);
        Arc::new(Self {
            node_id,
            voters: RwLock::new(voters),
            learners: RwLock::new(Vec::new()),
            voter_generation: std::sync::atomic::AtomicU64::new(0),
            claim,
            progress: RwLock::new(std::collections::BTreeMap::new()),
        })
    }

    pub fn node_id(&self) -> u64 {
        self.node_id
    }

    pub fn voters(&self) -> Vec<u64> {
        self.voters.read().clone()
    }

    pub fn set_voters(&self, mut voters: Vec<u64>) {
        voters.sort_unstable();
        voters.dedup();
        let mut current = self.voters.write();
        if *current == voters {
            return;
        }
        *current = voters;
        self.voter_generation
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        drop(current);
        self.close_gate();
    }

    pub fn learners(&self) -> Vec<u64> {
        self.learners.read().clone()
    }

    /// Replaces the learner set. Unlike `set_voters` this neither bumps the
    /// voter generation nor closes the write gate: learners are invisible to
    /// quorum math, so their arrival or departure must not fence sessions.
    pub fn set_learners(&self, mut learners: Vec<u64>) {
        learners.sort_unstable();
        learners.dedup();
        let mut current = self.learners.write();
        if *current == learners {
            return;
        }
        *current = learners;
    }

    /// True when the node is a cluster member of either kind — the set
    /// allowed to open Tail replication sessions.
    pub fn is_replica(&self, node_id: u64) -> bool {
        self.voters.read().contains(&node_id) || self.learners.read().contains(&node_id)
    }

    /// Records a follower's latest durably-acked cursor for one context.
    pub fn record_progress(&self, context: &str, node_id: u64, position: u64) {
        self.progress
            .write()
            .insert((context.to_string(), node_id), position);
    }

    /// The latest recorded cursor per context for one follower.
    pub fn progress_of(&self, node_id: u64) -> std::collections::BTreeMap<String, u64> {
        self.progress
            .read()
            .iter()
            .filter(|((_, node), _)| *node == node_id)
            .map(|((context, _), position)| (context.clone(), *position))
            .collect()
    }

    pub fn voter_generation(&self) -> u64 {
        self.voter_generation
            .load(std::sync::atomic::Ordering::Acquire)
    }

    pub fn claim(&self) -> Option<ActiveClaim> {
        *self.claim.borrow()
    }

    pub fn claim_updates(&self) -> tokio::sync::watch::Receiver<Option<ActiveClaim>> {
        self.claim.subscribe()
    }

    pub fn close_gate(&self) {
        self.claim.send_if_modified(|current| {
            let Some(claim) = current.as_mut() else {
                return false;
            };
            if !claim.writable {
                return false;
            }
            claim.writable = false;
            true
        });
    }

    pub fn observe_claim(&self, epoch: u64, leader_id: u64, term: u64) {
        let voter_generation = self.voter_generation();
        self.claim.send_if_modified(|current| {
            if current.map(|claim| claim.epoch >= epoch).unwrap_or(false) {
                return false;
            }
            *current = Some(ActiveClaim {
                epoch,
                leader_id,
                term,
                voter_generation,
                writable: false,
            });
            true
        });
    }

    /// Opens the local write gate only if the membership used to establish the
    /// claim is still the active voter set. `set_voters` and this method take
    /// the locks in the same order, so a concurrent topology update either
    /// wins first or closes a gate opened just before it.
    pub fn activate_local(
        &self,
        epoch: u64,
        term: u64,
        mut expected_voters: Vec<u64>,
        expected_generation: u64,
    ) -> bool {
        expected_voters.sort_unstable();
        expected_voters.dedup();
        let voters = self.voters.read();
        if *voters != expected_voters || self.voter_generation() != expected_generation {
            return false;
        }
        self.claim.send_replace(Some(ActiveClaim {
            epoch,
            leader_id: self.node_id,
            term,
            voter_generation: expected_generation,
            writable: true,
        }));
        true
    }

    pub fn is_local_writable(&self, epoch: u64, leader_id: u64) -> bool {
        self.claim()
            .map(|claim| {
                claim.writable
                    && claim.epoch == epoch
                    && claim.leader_id == leader_id
                    && leader_id == self.node_id
            })
            .unwrap_or(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn membership_change_fences_an_open_gate() {
        let control = ReplicationControl::new(1, vec![1, 2, 3]);
        let generation = control.voter_generation();
        control.observe_claim(7, 1, 4);
        assert!(control.activate_local(7, 4, vec![1, 2, 3], generation));
        assert!(control.is_local_writable(7, 1));

        control.set_voters(vec![1, 2]);
        assert!(!control.is_local_writable(7, 1));
        assert!(control.voter_generation() > generation);
    }

    #[test]
    fn stale_generation_cannot_reopen_gate_after_aba_membership() {
        let control = ReplicationControl::new(1, vec![1, 2, 3]);
        let original = control.voter_generation();
        control.observe_claim(7, 1, 4);
        control.set_voters(vec![1, 2]);
        control.set_voters(vec![1, 2, 3]);

        assert!(!control.activate_local(7, 4, vec![1, 2, 3], original));
    }

    #[test]
    fn claim_watch_observes_fencing_change() {
        let control = ReplicationControl::new(1, vec![1]);
        let mut updates = control.claim_updates();
        control.observe_claim(8, 1, 5);
        assert!(updates.has_changed().unwrap());
        assert_eq!(updates.borrow_and_update().as_ref().unwrap().epoch, 8);
    }

    #[test]
    fn learners_are_replicas_but_never_fence_the_gate() {
        let control = ReplicationControl::new(1, vec![1]);
        let generation = control.voter_generation();
        control.observe_claim(3, 1, 2);
        assert!(control.activate_local(3, 2, vec![1], generation));

        control.set_learners(vec![2, 4]);
        assert!(control.is_replica(1), "voter is a replica");
        assert!(control.is_replica(2), "learner is a replica");
        assert!(!control.is_replica(3), "stranger is not a replica");
        assert_eq!(
            control.voter_generation(),
            generation,
            "learner changes must not bump the voter generation"
        );
        assert!(
            control.is_local_writable(3, 1),
            "learner changes must not close the write gate"
        );
    }

    #[test]
    fn progress_tracks_latest_cursor_per_context_and_node() {
        let control = ReplicationControl::new(1, vec![1, 2]);
        control.record_progress("orders", 2, 10);
        control.record_progress("orders", 2, 25);
        control.record_progress("billing", 2, 5);
        control.record_progress("orders", 3, 99);

        let progress = control.progress_of(2);
        assert_eq!(progress.get("orders"), Some(&25));
        assert_eq!(progress.get("billing"), Some(&5));
        assert_eq!(progress.len(), 2);
        assert_eq!(control.progress_of(4).len(), 0);
    }
}
