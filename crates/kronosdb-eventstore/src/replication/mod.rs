//! Native authoritative-log replication.
//!
//! The group-commit wave is the replication unit. At wave seal, the leader
//! races local fdatasync against streaming byte-exact segment records to
//! followers; append acknowledgements release only when the durable quorum
//! watermark reaches their next-exclusive position.

pub const PEER_MAX_MESSAGE_BYTES: usize = 64 * 1024 * 1024;
pub const MAX_RECORD_BYTES_PER_FRAME: usize = 60 * 1024 * 1024;

pub mod client;
pub mod control;
pub mod dispatcher;
pub mod peer;
pub mod service;
pub mod watermark;

/// Generated protobuf types for the native segment Tail protocol.
pub mod proto {
    // Generated service methods return Result<_, tonic::Status>; clippy 1.98
    // flags the Status variant's size in code we don't author.
    #![allow(clippy::result_large_err)]
    tonic::include_proto!("kronosdb.replication");
}
