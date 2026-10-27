use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::criteria::SourcingCondition;
use crate::event::Position;
use crate::store::LogView;
use crate::subscription::{CommitFeed, LiveChunk, LivePick, Shared};

/// A live event stream subscription.
///
/// The server drives this by calling `wait_for_new_events()` in a loop,
/// which returns a bound when the log may hold events for the stream between
/// its cursor and that bound. The server then reads `[cursor, bound)` from
/// the store, sends what it finds, and advances the cursor.
///
/// A stream is woken by the commit's own content: the context's dispatcher
/// matches every newly committed event against all live streams once, and
/// only streams that an event matched are woken. A stream behind the head is
/// not woken separately; it reads page after page (`cursor < bound`) and
/// only asks to sleep again once it has caught up.
///
/// Reliability guarantees:
/// - Events are NEVER missed: every committed event is either read by the
///   stream itself (it asks for everything below the watermark after it
///   registers) or matched by the dispatcher after it registered.
/// - Events are never delivered twice: a wake is a hint, and every read
///   starts at the stream's cursor, which only the caller advances.
/// - Events are always delivered in position order.
/// - Delivery is watermark-bounded: only quorum-committed events are
///   visible, so a delivered event can never be truncated away later.
pub struct EventStream {
    /// The criteria to filter events.
    pub condition: SourcingCondition,
    /// The next position to source from. Advanced by the caller after processing.
    pub cursor: Position,
    /// The bound of the last wake: while `cursor` is below it, the stream is
    /// still reading what that wake announced.
    owed: u64,
    /// The stream wants every event, so it can send shared batches.
    follows_everything: bool,
    /// The position from which the dispatcher matches events against this
    /// stream: chunks that start there or later hold what it picked.
    since: u64,
    feed: Arc<CommitFeed>,
    shared: Arc<Shared>,
    slot: u32,
    /// Shared watermark — next-exclusive, the source of truth for what's available.
    watermark: Arc<AtomicU64>,
}

impl EventStream {
    pub(crate) fn new(
        condition: SourcingCondition,
        cursor: Position,
        exclude_names: &[String],
        feed: Arc<CommitFeed>,
        view: &LogView,
    ) -> Self {
        // Registered before anything is read: see the subscription module.
        let (slot, shared, since) = feed.register(view, &condition, exclude_names);
        let follows_everything = exclude_names.is_empty()
            && condition
                .criteria
                .iter()
                .any(|c| c.names.is_empty() && c.tags.is_empty());
        Self {
            condition,
            cursor,
            owed: 0,
            follows_everything,
            since,
            feed,
            shared,
            slot,
            watermark: view.watermark_handle(),
        }
    }

    /// Waits until the log may hold events for this stream at or after the
    /// cursor, and returns the bound to read up to (`bound > cursor`).
    ///
    /// The caller reads `[cursor, bound)` with the stream's condition and
    /// advances the cursor; while it is below the bound this returns at once,
    /// so a stream behind the head reads straight through. Once the cursor
    /// reaches the bound the stream sleeps until an event matches it. A wake
    /// can be spurious (an empty read), never missing.
    ///
    /// The bound is the end of a stretch of the log the dispatcher has
    /// matched, not necessarily the head: it may be below the watermark, and
    /// the events past it wake the stream when they are matched.
    ///
    /// If the store is dropped it returns the current watermark, which does
    /// not exceed the cursor once the stream has drained, to let the caller
    /// clean up.
    ///
    /// Cancel-safe: dropping the future loses no wake.
    pub async fn wait_for_new_events(&mut self) -> u64 {
        loop {
            if self.cursor.0 < self.owed {
                return self.owed;
            }
            if self.feed.is_closed() {
                return self.watermark.load(Ordering::Acquire);
            }
            // Taken before anything is read: an event matched after this is
            // past the bound, and wakes the stream again.
            let mark = self.shared.take();
            if mark != 0 {
                if mark > self.cursor.0 {
                    self.owed = mark;
                }
                continue;
            }
            self.shared.woken().await;
        }
    }

    /// The shared batch that starts exactly at the cursor, when this stream
    /// wants every event and the batch holds at most `max_events`. Sending
    /// its `body` as an `Events` frame (and then advancing the cursor to its
    /// `to`) is the same as reading `[cursor, to)`, without the read.
    ///
    /// `None` means read through the store as usual: the stream filters (see
    /// `live_picks`), is catching up, or the batch is gone from the ring.
    pub fn live_batch(&self, max_events: usize) -> Option<Arc<LiveChunk>> {
        if !self.follows_everything || self.cursor.0 >= self.owed {
            return None;
        }
        self.feed
            .batch_at(self.cursor.0)
            .filter(|batch| batch.count as usize <= max_events)
    }

    /// What a filtering stream at the live tail takes from the dispatcher's
    /// chunk covering its cursor: an `Events` body of the events that chunk
    /// holds for it from the cursor on, at most `max_events` of them, and
    /// where the cursor goes afterwards. Sending the body (if any) and
    /// advancing the cursor to `next` is the same as reading the stream's
    /// condition over `[cursor, next)`, without the read: the dispatcher
    /// already matched, checked and copied those records.
    ///
    /// `None` means read through the store as usual: the stream wants every
    /// event (see `live_batch`), has no wake to serve, or the chunk is gone
    /// from the ring or was made before the stream existed.
    pub fn live_picks(&self, max_events: usize) -> Option<LivePick> {
        if self.follows_everything || self.cursor.0 >= self.owed {
            return None;
        }
        self.feed
            .pick_at(self.slot, self.since, self.cursor.0, max_events)
    }

    /// The position below which this stream has nothing left to send: every
    /// matching event under it was read, or was seen by the dispatcher and
    /// matched nothing. What a Progress frame may report. Never below the
    /// cursor.
    pub fn frontier(&self) -> Position {
        if self.cursor.0 < self.owed {
            return self.cursor;
        }
        // The mark first, the flag second: the dispatcher flags a chunk's
        // matches before it publishes the mark, so a stream that sees the
        // mark and no flag has no match below it that it has not read.
        let dispatched = self.feed.dispatched();
        if self.shared.is_pending() {
            return self.cursor;
        }
        Position(self.cursor.0.max(dispatched))
    }

    /// Advances the cursor to the given position.
    /// Call this after processing events returned by source().
    pub fn advance_cursor(&mut self, position: Position) {
        self.cursor = position;
    }
}

impl Drop for EventStream {
    fn drop(&mut self) {
        self.feed.unregister(self.slot);
    }
}
