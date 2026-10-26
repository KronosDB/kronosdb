//! The KronosDB native wire protocol.
//!
//! Every frame, structure and enum is declared once in
//! `protocol/kronosdb.schema`; `generated.rs` is produced from it by
//! `just protocol`, along with the TypeScript codec and the golden fixtures
//! every implementation is tested against. `protocol/README.md` is the spec.
//!
//! [`net`] is the connection runtime both the server and the clients use:
//! framing, multiplexing, the handshake and liveness.
//!
//! Decoding borrows from the input: strings are `&str`, bytes are `&[u8]`,
//! and event records stay in their stored layout — a batch read from disk is
//! sent without being decoded or re-encoded.

mod codec;
mod generated;
pub mod net;

pub use codec::*;
pub use generated::*;

#[cfg(test)]
mod tests;
