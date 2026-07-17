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
    voter_generation: std::sync::atomic::AtomicU64,
    claim: tokio::sync::watch::Sender<Option<ActiveClaim>>,
}

impl ReplicationControl {
    pub fn new(node_id: u64, mut voters: Vec<u64>) -> Arc<Self> {
        voters.sort_unstable();
        voters.dedup();
        let (claim, _) = tokio::sync::watch::channel(None);
        Arc::new(Self {
            node_id,
            voters: RwLock::new(voters),
            voter_generation: std::sync::atomic::AtomicU64::new(0),
            claim,
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
}
