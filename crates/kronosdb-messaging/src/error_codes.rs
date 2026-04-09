/// Standard error codes for command/query dispatch and handler responses.
/// These align with Axon Server conventions so the connector can map them
/// to the correct exception types on the client side.
///
/// Format: "KRONOSDB-XXXX" where the number range indicates the category.
/// 4000-4099: Command errors
/// 4100-4199: Query errors
/// 4200-4299: Event store errors
/// 5000-5099: Server internal errors

// ── Command errors ─────────────────────────────────────────────────

/// No handler is registered for the given command type.
pub const NO_HANDLER_FOR_COMMAND: &str = "KRONOSDB-4000";

/// A handler is registered but command dispatch failed (routing error, permits, etc.).
pub const COMMAND_DISPATCH_ERROR: &str = "KRONOSDB-4001";

/// The handler processed the command but returned a transient (retryable) error.
pub const COMMAND_EXECUTION_ERROR: &str = "KRONOSDB-4002";

/// The handler processed the command but returned a non-transient (permanent) error.
pub const COMMAND_EXECUTION_NON_TRANSIENT_ERROR: &str = "KRONOSDB-4003";

/// A concurrency/version conflict occurred (e.g., DCB condition check failed).
pub const CONCURRENCY_EXCEPTION: &str = "KRONOSDB-4004";

/// Command dispatch timed out waiting for a handler response.
pub const COMMAND_TIMEOUT: &str = "KRONOSDB-4005";

// ── Query errors ───────────────────────────────────────────────────

/// No handler is registered for the given query type.
pub const NO_HANDLER_FOR_QUERY: &str = "KRONOSDB-4100";

/// Query dispatch failed.
pub const QUERY_DISPATCH_ERROR: &str = "KRONOSDB-4101";

/// Query execution failed with a transient error.
pub const QUERY_EXECUTION_ERROR: &str = "KRONOSDB-4102";

/// Query response timed out.
pub const QUERY_TIMEOUT: &str = "KRONOSDB-4105";

// ── Event store errors ─────────────────────────────────────────────

/// The append condition (DCB consistency check) was not met.
pub const APPEND_CONDITION_FAILED: &str = "KRONOSDB-4200";

/// The requested context does not exist.
pub const CONTEXT_NOT_FOUND: &str = "KRONOSDB-4201";

/// Invalid event data (corrupt payload, missing required fields).
pub const INVALID_EVENT: &str = "KRONOSDB-4202";

// ── Server errors ──────────────────────────────────────────────────

/// Unexpected internal server error.
pub const INTERNAL_ERROR: &str = "KRONOSDB-5000";

/// The server is not the leader (clustered mode) — client should reconnect.
pub const NOT_LEADER: &str = "KRONOSDB-5001";

/// Authentication failed — invalid or missing token.
pub const AUTHENTICATION_FAILED: &str = "KRONOSDB-5002";
