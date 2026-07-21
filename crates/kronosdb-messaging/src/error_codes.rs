//! Error codes emitted on command/query dispatch failures.
//!
//! Format: "KRONOSDB-XXXX". Only codes the server actually emits live here —
//! add new ones when the code path that emits them lands.

/// Command dispatch timed out waiting for a handler response.
pub const COMMAND_TIMEOUT: &str = "KRONOSDB-4005";

/// The handler the command was routed to disconnected before responding.
pub const CONNECTION_TO_HANDLER_LOST: &str = "KRONOSDB-4006";
