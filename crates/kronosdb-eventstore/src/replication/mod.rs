//! Native authoritative-log replication.
//!
//! The group-commit wave is the replication unit. At wave seal, the leader
//! races local fdatasync against streaming byte-exact segment records to
//! followers; append acknowledgements release only when the durable quorum
//! watermark reaches their next-exclusive position.

pub const PEER_MAX_MESSAGE_BYTES: usize = 64 * 1024 * 1024;
/// The largest frame peers exchange: a message of up to
/// `PEER_MAX_MESSAGE_BYTES` plus the header and fields around it.
pub const PEER_MAX_FRAME: u32 = PEER_MAX_MESSAGE_BYTES as u32 + 1024 * 1024;
pub const MAX_RECORD_BYTES_PER_FRAME: usize = 60 * 1024 * 1024;

pub mod client;
pub mod control;
pub mod dispatcher;
pub mod epochs;
pub mod peer;
pub mod service;
pub mod watermark;
