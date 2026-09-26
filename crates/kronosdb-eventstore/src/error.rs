use crate::event::Position;

/// Errors that can occur during event store operations.
#[derive(Debug)]
pub enum Error {
    /// The DCB consistency condition was violated.
    /// Another event matching the condition's query was found after the consistency marker.
    ConsistencyConditionViolated {
        /// The position of the conflicting event that caused the rejection.
        conflicting_position: Position,
    },

    /// An I/O error occurred during storage operations.
    Io(std::io::Error),

    /// The event store data is corrupted (e.g., CRC mismatch).
    /// Stored or replicated bytes failed an integrity check. Reserved for
    /// that: availability and state problems have their own variants below,
    /// because "data corrupted" must never be a false alarm.
    Corrupted { message: String },

    /// The node can't serve this right now — it is starting, an election or
    /// failover is in progress, the leader is unreachable. Nothing is wrong
    /// with the data and the same request is expected to succeed on retry.
    Unavailable { message: String },

    /// A well-formed request that the current cluster state or configuration
    /// doesn't allow. Retrying unchanged won't help.
    Rejected { message: String },

    /// The server broke one of its own invariants. Not the caller's fault.
    Internal { message: String },

    /// There is no event at this position (past the head, or truncated away).
    PositionNotFound { position: u64 },

    /// The requested context was not found.
    ContextNotFound { name: String },

    /// A context with this name already exists.
    ContextAlreadyExists { name: String },

    /// The context name is invalid.
    InvalidContextName { name: String, reason: String },

    /// Snapshot not found.
    SnapshotNotFound { key: String },

    /// A client request used the `$` namespace, which is reserved for events
    /// the server writes about itself. See `docs/system-events.md`.
    ReservedNamespace { detail: String },
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Error::Io(err)
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::ConsistencyConditionViolated {
                conflicting_position,
            } => {
                write!(
                    f,
                    "consistency condition violated: conflicting event at position {}",
                    conflicting_position.0
                )
            }
            Error::Io(err) => write!(f, "I/O error: {err}"),
            Error::Corrupted { message } => write!(f, "data corrupted: {message}"),
            Error::Unavailable { message } => write!(f, "temporarily unavailable: {message}"),
            Error::Rejected { message } => write!(f, "rejected: {message}"),
            Error::Internal { message } => write!(f, "internal error: {message}"),
            Error::PositionNotFound { position } => {
                write!(f, "no event at position {position}")
            }
            Error::ContextNotFound { name } => write!(f, "context not found: {name}"),
            Error::ContextAlreadyExists { name } => {
                write!(f, "context already exists: {name}")
            }
            Error::InvalidContextName { name, reason } => {
                write!(f, "invalid context name '{name}': {reason}")
            }
            Error::SnapshotNotFound { key } => write!(f, "snapshot not found: {key}"),
            Error::ReservedNamespace { detail } => {
                write!(f, "reserved namespace: {detail}")
            }
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Io(err) => Some(err),
            _ => None,
        }
    }
}
