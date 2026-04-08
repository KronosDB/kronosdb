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
    Corrupted {
        message: String,
    },

    /// The requested context was not found.
    ContextNotFound {
        name: String,
    },

    /// A context with this name already exists.
    ContextAlreadyExists {
        name: String,
    },

    /// The context name is invalid.
    InvalidContextName {
        name: String,
        reason: String,
    },
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Error::Io(err)
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::ConsistencyConditionViolated { conflicting_position } => {
                write!(
                    f,
                    "consistency condition violated: conflicting event at position {}",
                    conflicting_position.0
                )
            }
            Error::Io(err) => write!(f, "I/O error: {err}"),
            Error::Corrupted { message } => write!(f, "data corrupted: {message}"),
            Error::ContextNotFound { name } => write!(f, "context not found: {name}"),
            Error::ContextAlreadyExists { name } => {
                write!(f, "context already exists: {name}")
            }
            Error::InvalidContextName { name, reason } => {
                write!(f, "invalid context name '{name}': {reason}")
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
