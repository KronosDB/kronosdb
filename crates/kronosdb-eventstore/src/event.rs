use crate::tag::Tag;

/// Global position in the event log. Monotonically increasing, gapless.
/// Position 0 means "no events" / "beginning of log".
/// First event has position 1.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Position(pub u64);

impl Position {
    pub const ZERO: Position = Position(0);

    pub fn next(self) -> Position {
        Position(self.0 + 1)
    }
}

/// An event as provided by the client for appending.
/// Does not include a position — that's assigned by the writer.
pub struct AppendEvent {
    /// Client-provided unique identifier.
    pub identifier: String,
    /// Event type name (e.g., "OrderPlaced", "PaymentReceived").
    pub name: String,
    /// Schema version of the event payload.
    pub version: String,
    /// Timestamp in milliseconds since epoch. Client-provided, not assigned by server.
    pub timestamp: i64,
    /// Opaque payload bytes. The server does not interpret this.
    pub payload: Vec<u8>,
    /// Arbitrary key-value metadata.
    pub metadata: Vec<(String, String)>,
    /// Tags to index this event with.
    pub tags: Vec<Tag>,
}

/// An event as returned by read operations (Source, Stream).
/// Does NOT include tags — tags live in the index and are queried separately.
pub struct SequencedEvent {
    /// Global position in the log.
    pub position: Position,
    /// Client-provided unique identifier.
    pub identifier: String,
    /// Event type name.
    pub name: String,
    /// Schema version.
    pub version: String,
    /// Client-provided timestamp.
    pub timestamp: i64,
    /// Opaque payload.
    pub payload: Vec<u8>,
    /// Arbitrary metadata.
    pub metadata: Vec<(String, String)>,
}

/// An event as stored on disk. Includes tags so the tag index
/// can be fully rebuilt from segments on recovery.
pub struct StoredEvent {
    /// Global position in the log.
    pub position: Position,
    /// Client-provided unique identifier.
    pub identifier: String,
    /// Event type name.
    pub name: String,
    /// Schema version.
    pub version: String,
    /// Client-provided timestamp.
    pub timestamp: i64,
    /// Opaque payload.
    pub payload: Vec<u8>,
    /// Arbitrary metadata.
    pub metadata: Vec<(String, String)>,
    /// Tags that were provided at append time.
    pub tags: Vec<Tag>,
}

impl StoredEvent {
    /// Converts to a SequencedEvent by dropping the tags.
    pub fn into_sequenced(self) -> SequencedEvent {
        SequencedEvent {
            position: self.position,
            identifier: self.identifier,
            name: self.name,
            version: self.version,
            timestamp: self.timestamp,
            payload: self.payload,
            metadata: self.metadata,
        }
    }
}
