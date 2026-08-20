use crate::append::{AppendRequest, AppendResponse};
use crate::criteria::SourcingCondition;
use crate::error::Error;
use crate::event::{Position, SequencedEvent, Tag};
use crate::snapshot::Snapshot;
use crate::stream::EventStream;

/// The event store interface.
///
/// This trait defines the contract between the server layer and the event store engine.
/// Extensions (PII encryption, audit logging, metrics, etc.) can decorate this trait
/// by wrapping an inner implementation and delegating, adding behavior before/after calls.
///
/// The gRPC service layer programs against this trait, not the concrete `EventStore` type.
///
/// `append` is async because clustered nodes may forward it to the claimed leader.
/// All read methods are sync because they operate on local mmap'd data.
#[async_trait::async_trait]
pub trait EventStore: Send + Sync {
    /// Appends events to the store, optionally with a DCB consistency condition.
    ///
    /// In cluster mode, the claimed leader writes the group-commit wave and
    /// acknowledges after native segment cursors reach quorum durability.
    async fn append(&self, request: AppendRequest) -> Result<AppendResponse, Error>;

    /// Reads events matching a sourcing condition from `from_position` up to the current head.
    fn source(
        &self,
        from_position: Position,
        condition: &SourcingCondition,
    ) -> Result<Vec<SequencedEvent>, Error>;

    /// Reads up to `limit` matching events with position in `[from_position, up_to)`.
    ///
    /// Chunked-streaming building block: freeze `up_to` at `head()` once,
    /// then advance `from_position` past the last returned event. Memory
    /// stays bounded by `limit` and abandoning the read stops the work.
    fn source_page(
        &self,
        from_position: Position,
        condition: &SourcingCondition,
        up_to: Position,
        limit: usize,
    ) -> Result<Vec<SequencedEvent>, Error>;

    /// Creates a live event stream subscription.
    fn subscribe(&self, from_position: Position, condition: SourcingCondition) -> EventStream;

    /// Returns the current head position (next position to be assigned).
    ///
    /// Counts every position, including events clients cannot read. Use
    /// `visible_head` for anything a client compares its own cursor against.
    fn head(&self) -> Position;

    /// The head as clients see it: the position after the last readable
    /// event. Equal to `head` unless system events trail the log.
    fn visible_head(&self) -> Position;

    /// Returns the tail position (first event in the store).
    fn tail(&self) -> Position;

    /// Gets tags for an event at the given position.
    fn get_tags(&self, position: Position) -> Result<Vec<Tag>, Error>;

    /// Returns the position of the first event with timestamp >= the given millis-since-epoch.
    /// Returns `None` if no such event exists (empty store or all events are older).
    fn get_sequence_at(&self, timestamp_millis: i64) -> Result<Option<Position>, Error>;

    /// Stores an opaque client snapshot under a client-composed key,
    /// returning the log position of its record (ADR-0005).
    ///
    /// Rides the same replicated append path as events: async because
    /// clustered nodes forward it to the claimed leader.
    async fn append_snapshot(
        &self,
        key: Vec<u8>,
        state: Vec<u8>,
        fold_position: Position,
    ) -> Result<Position, Error>;

    /// The latest snapshot for a key whose record landed strictly below
    /// `below` (`None` = no bound). A miss returns `Ok(None)` — always legal.
    /// Local read, like `source`.
    fn get_snapshot(&self, key: &[u8], below: Option<Position>) -> Result<Option<Snapshot>, Error>;
}
