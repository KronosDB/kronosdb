//! Metadata consensus and native data-plane coordination.
//!
//! OpenRaft owns elections, membership, context metadata, and fencing-epoch
//! allocation. Event records never enter its journal; `native_coordinator` and
//! `routed_engine` connect committed claims to byte-exact segment replication.

pub mod cluster;
pub mod log_store;
mod native_coordinator;
pub mod network;
mod routed_engine;
pub mod snapshot_format;
pub mod snapshot_store;
pub mod state_machine;
pub mod transport;
pub mod types;

#[cfg(feature = "bench-instrumentation")]
pub mod bench_instrumentation;

/// Generated protobuf types for Raft transport.
pub mod proto {
    tonic::include_proto!("kronosdb.raft");
}
