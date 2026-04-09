pub mod types;
pub mod log_store;
pub mod state_machine;
pub mod network;
pub mod transport;
pub mod node;
pub mod cluster;

/// Generated protobuf types for Raft transport.
pub mod proto {
    tonic::include_proto!("kronosdb.raft");
}
