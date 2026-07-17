use std::collections::BTreeMap;

use openraft::BasicNode;
use openraft::Config;
use openraft::Entry;
use openraft::SnapshotPolicy;
use serde::{Deserialize, Serialize};

/// Node ID type — simple u64.
pub type NodeId = u64;

/// Node info — address for gRPC transport.
pub type Node = BasicNode;

// The openraft type config for KronosDB's metadata control plane.
openraft::declare_raft_types!(
    pub TypeConfig:
        D = RaftRequest,
        R = RaftResponse,
        NodeId = NodeId,
        Node = Node,
        Entry = Entry<TypeConfig>,
        SnapshotData = tokio::fs::File,
);

/// Commands replicated by the metadata-only Raft control plane.
///
/// Event data never belongs in this type. Segments are replicated by the
/// native data plane; Raft carries only context and leadership metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RaftRequest {
    /// Create a context on every node. Applying this more than once succeeds.
    CreateContext { name: String },

    /// Claim native data-plane leadership.
    ///
    /// The fencing epoch is deliberately absent: the state machine derives it
    /// from the applied entry's `log_id.index`, which is globally ordered and
    /// cannot be chosen or reused by the claimant.
    LeaderClaim {
        node_id: NodeId,
        term: u64,
        prior_epoch: u64,
        voters: Vec<NodeId>,
        per_context_tails: BTreeMap<String, u64>,
    },
}

/// Application response returned after applying a control-plane entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RaftResponse {
    ContextCreated,
    /// The epoch allocated from the applied LeaderClaim's log index.
    LeaderClaimed {
        epoch: u64,
    },
    /// Blank or membership entry applied.
    Ok,
}

/// Applied leader metadata retained by the state machine and its snapshots.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LeaderClaim {
    pub epoch: u64,
    pub node_id: NodeId,
    pub term: u64,
    pub prior_epoch: u64,
    pub voters: Vec<NodeId>,
    pub per_context_tails: BTreeMap<String, u64>,
}

/// Default cadence for metadata snapshots. The control-plane log entries are
/// tiny, so an entry-count policy is sufficient and bounds restart replay.
pub const RAFT_SNAPSHOT_LOGS_SINCE_LAST: u64 = 10_000;

/// Helper to build a Raft config with sensible defaults.
pub fn default_raft_config() -> Config {
    Config {
        heartbeat_interval: 500,
        election_timeout_min: 1500,
        election_timeout_max: 3000,
        snapshot_policy: SnapshotPolicy::LogsSinceLast(RAFT_SNAPSHOT_LOGS_SINCE_LAST),
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn leader_claim_response_roundtrips() {
        let response = RaftResponse::LeaderClaimed { epoch: 42 };
        let bytes = bincode::serialize(&response).expect("serialize");
        let decoded: RaftResponse = bincode::deserialize(&bytes).expect("deserialize");
        assert!(matches!(decoded, RaftResponse::LeaderClaimed { epoch: 42 }));
    }

    #[test]
    fn default_raft_config_enables_snapshot_policy() {
        let cfg = default_raft_config();
        assert!(matches!(
            cfg.snapshot_policy,
            openraft::SnapshotPolicy::LogsSinceLast(RAFT_SNAPSHOT_LOGS_SINCE_LAST)
        ));
    }
}
