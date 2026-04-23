use std::io::Cursor;

use openraft::BasicNode;
use openraft::Config;
use openraft::Entry;
use serde::{Deserialize, Serialize};

use crate::criteria::SourcingCondition;
use crate::event::AppendEvent;
use crate::event::Position;

/// Node ID type — simple u64.
pub type NodeId = u64;

/// Node info — address for gRPC transport.
pub type Node = BasicNode;

// The openraft type config for KronosDB.
openraft::declare_raft_types!(
    pub TypeConfig:
        D = RaftRequest,
        R = RaftResponse,
        NodeId = NodeId,
        Node = Node,
        Entry = Entry<TypeConfig>,
        SnapshotData = Cursor<Vec<u8>>,
);

/// Commands that can be proposed to the Raft cluster.
///
/// This is the "D" (application data) type in openraft.
/// Each variant becomes a Raft log entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RaftRequest {
    /// Append events to the event store.
    Append {
        /// The context to append to.
        context: String,
        /// Serialized events (we serialize AppendEvents to avoid trait object issues).
        events: Vec<RaftAppendEvent>,
        /// Optional DCB consistency condition.
        condition: Option<RaftAppendCondition>,
    },
    /// Create a new context.
    CreateContext { name: String },
}

/// Application response returned after applying a Raft log entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RaftResponse {
    /// Response from an append operation.
    Append {
        first_position: u64,
        count: u32,
        consistency_marker: u64,
    },
    /// Context was created.
    ContextCreated,
    /// No-op / membership change applied.
    Ok,
    /// Apply-time rejection of an append (e.g. DCB consistency violation).
    ///
    /// Returned by the state machine when `apply` cannot honour the
    /// `RaftRequest` against the current committed state (D-01). Downstream
    /// (cluster.rs) maps this back to the existing `Error` taxonomy for the
    /// client-facing surface (D-02) so connector wire contracts stay stable.
    AppendRejected { reason: RaftRejectReason },
}

/// Reason an apply-time append was rejected.
///
/// Extensible: future rejection classes add variants here rather than
/// changing `RaftResponse`'s shape (D-01, D-03). `u64` (not `event::Position`)
/// on the wire keeps this serde surface decoupled from the event-layer type.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RaftRejectReason {
    /// The DCB consistency condition was violated at apply time.
    /// `conflicting_position` is the `u64` form of `event::Position` of
    /// the event that caused the rejection.
    ConsistencyConditionViolated { conflicting_position: u64 },
}

/// Serializable version of AppendEvent (for Raft log entries).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaftAppendEvent {
    pub identifier: String,
    pub name: String,
    pub version: String,
    pub timestamp: i64,
    pub payload: Vec<u8>,
    pub metadata: Vec<(String, String)>,
    pub tags: Vec<(Vec<u8>, Vec<u8>)>,
}

/// Serializable version of AppendCondition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaftAppendCondition {
    pub consistency_marker: u64,
    pub criteria: Vec<RaftCriterion>,
}

/// Serializable version of Criterion.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaftCriterion {
    pub names: Vec<String>,
    pub tags: Vec<(Vec<u8>, Vec<u8>)>,
}

// --- Conversions between Raft types and eventstore types ---

impl RaftAppendEvent {
    pub fn from_event(e: &AppendEvent) -> Self {
        Self {
            identifier: e.identifier.clone(),
            name: e.name.clone(),
            version: e.version.clone(),
            timestamp: e.timestamp,
            payload: e.payload.clone(),
            metadata: e.metadata.clone(),
            tags: e
                .tags
                .iter()
                .map(|t| (t.key.clone(), t.value.clone()))
                .collect(),
        }
    }

    pub fn to_event(&self) -> AppendEvent {
        AppendEvent {
            identifier: self.identifier.clone(),
            name: self.name.clone(),
            version: self.version.clone(),
            timestamp: self.timestamp,
            payload: self.payload.clone(),
            metadata: self.metadata.clone(),
            tags: self
                .tags
                .iter()
                .map(|(k, v)| crate::event::Tag {
                    key: k.clone(),
                    value: v.clone(),
                })
                .collect(),
        }
    }
}

impl RaftAppendCondition {
    pub fn to_condition(&self) -> crate::append::AppendCondition {
        crate::append::AppendCondition {
            consistency_marker: Position(self.consistency_marker),
            criteria: SourcingCondition {
                criteria: self.criteria.iter().map(|c| c.to_criterion()).collect(),
            },
        }
    }
}

impl RaftCriterion {
    pub fn to_criterion(&self) -> crate::criteria::Criterion {
        crate::criteria::Criterion {
            names: self.names.clone(),
            tags: self
                .tags
                .iter()
                .map(|(k, v)| crate::event::Tag {
                    key: k.clone(),
                    value: v.clone(),
                })
                .collect(),
        }
    }
}

/// Helper to build a Raft config with sensible defaults.
pub fn default_raft_config() -> Config {
    Config {
        heartbeat_interval: 500,
        election_timeout_min: 1500,
        election_timeout_max: 3000,
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn append_rejected_roundtrip_bincode() {
        let r = RaftResponse::AppendRejected {
            reason: RaftRejectReason::ConsistencyConditionViolated {
                conflicting_position: 42,
            },
        };
        let bytes = bincode::serialize(&r).expect("serialize");
        let decoded: RaftResponse = bincode::deserialize(&bytes).expect("deserialize");
        match decoded {
            RaftResponse::AppendRejected {
                reason:
                    RaftRejectReason::ConsistencyConditionViolated {
                        conflicting_position,
                    },
            } => assert_eq!(conflicting_position, 42),
            other => panic!("expected AppendRejected, got {other:?}"),
        }
    }

    #[test]
    fn existing_variants_still_roundtrip() {
        let cases = vec![
            RaftResponse::Append {
                first_position: 1,
                count: 2,
                consistency_marker: 3,
            },
            RaftResponse::ContextCreated,
            RaftResponse::Ok,
        ];
        for r in cases {
            let bytes = bincode::serialize(&r).expect("serialize");
            let decoded: RaftResponse = bincode::deserialize(&bytes).expect("deserialize");
            // Debug-string equality is sufficient — no PartialEq on RaftResponse today.
            assert_eq!(format!("{r:?}"), format!("{decoded:?}"));
        }
    }
}
