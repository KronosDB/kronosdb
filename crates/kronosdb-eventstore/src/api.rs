use crate::append::{AppendRequest, AppendResponse};
use crate::criteria::SourcingCondition;
use crate::error::Error;
use crate::event::{Position, SequencedEvent, Tag};
use crate::stream::EventStream;

/// The event store interface.
///
/// This trait defines the contract between the server layer and the event store engine.
/// Extensions (PII encryption, audit logging, metrics, etc.) can decorate this trait
/// by wrapping an inner implementation and delegating, adding behavior before/after calls.
///
/// The gRPC service layer programs against this trait, not the concrete `EventStore` type.
///
/// `append` is async because it may involve Raft consensus (network I/O).
/// All read methods are sync because they operate on local mmap'd data.
#[async_trait::async_trait]
pub trait EventStore: Send + Sync {
    /// Appends events to the store, optionally with a DCB consistency condition.
    ///
    /// In cluster mode, this goes through Raft consensus before the events
    /// are committed to the local store.
    async fn append(&self, request: AppendRequest) -> Result<AppendResponse, Error>;

    /// Reads events matching a sourcing condition from `from_position` up to the current head.
    fn source(
        &self,
        from_position: Position,
        condition: &SourcingCondition,
    ) -> Result<Vec<SequencedEvent>, Error>;

    /// Creates a live event stream subscription.
    fn subscribe(&self, from_position: Position, condition: SourcingCondition) -> EventStream;

    /// Returns the current head position (next position to be assigned).
    fn head(&self) -> Position;

    /// Returns the tail position (first event in the store).
    fn tail(&self) -> Position;

    /// Gets tags for an event at the given position.
    fn get_tags(&self, position: Position) -> Result<Vec<Tag>, Error>;

    /// Returns the position of the first event with timestamp >= the given millis-since-epoch.
    /// Returns `None` if no such event exists (empty store or all events are older).
    fn get_sequence_at(&self, timestamp_millis: i64) -> Result<Option<Position>, Error>;
}
