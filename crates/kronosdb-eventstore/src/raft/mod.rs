//! Raft consensus integration for cluster-aware event stores.
//!
//! Every KronosDB node runs Raft — a single-node cluster when alone,
//! expanding to multi-node when peers join. This module contains:
//!
//! - `cluster` — `ClusterManager` that wraps event stores with Raft consensus
//! - `types` — Raft request/response types and openraft type configuration
//! - `log_store` — Persistent Raft log storage
//! - `state_machine` — Applies committed Raft entries to the event store
//! - `network` — gRPC network factory for peer connections
//! - `transport` — Inbound gRPC service for Raft RPCs

pub mod cluster;
pub mod log_store;
pub mod network;
pub mod state_machine;
pub mod transport;
pub mod types;

/// Generated protobuf types for Raft transport.
pub mod proto {
    tonic::include_proto!("kronosdb.raft");
}
