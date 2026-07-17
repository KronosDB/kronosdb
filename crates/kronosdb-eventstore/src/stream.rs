use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::sync::broadcast;

use crate::criteria::SourcingCondition;
use crate::event::Position;

/// Notification sent when the watermark advances (new events are committed
/// and externally visible). This is a wake-up signal only — the watermark
/// atomic is the source of truth, not this notification.
#[derive(Debug, Clone)]
pub struct CommitNotification {
    /// The watermark after this advance (next-exclusive).
    pub watermark: u64,
}

/// A live event stream subscription.
///
/// The gRPC layer drives this by calling `wait_for_new_events()` in a loop,
/// which returns the head position when there are new events. The gRPC layer
/// then calls `store.source()` for that range and sends the results to the client.
///
/// Reliability guarantees:
/// - Events are NEVER missed. The watermark atomic is the source of truth,
///   not the broadcast notification. If a notification is dropped (slow
///   subscriber), the next wait will catch up by checking the atomic.
/// - Events are always delivered in sequence order.
/// - The cursor only advances after the caller has processed the events.
/// - Delivery is watermark-bounded: only quorum-committed events are
///   visible, so a delivered event can never be truncated away later.
pub struct EventStream {
    /// The criteria to filter events.
    pub condition: SourcingCondition,
    /// The next position to source from. Advanced by the caller after processing.
    pub cursor: Position,
    /// Receiver for commit notifications from the writer.
    receiver: broadcast::Receiver<CommitNotification>,
    /// Shared watermark — next-exclusive, the source of truth for what's available.
    watermark: Arc<AtomicU64>,
}

impl EventStream {
    pub(crate) fn new(
        condition: SourcingCondition,
        cursor: Position,
        receiver: broadcast::Receiver<CommitNotification>,
        watermark: Arc<AtomicU64>,
    ) -> Self {
        Self {
            condition,
            cursor,
            receiver,
            watermark,
        }
    }

    /// Waits until there are new events to process.
    ///
    /// Returns the head position when at least one event at or after the
    /// cursor exists (i.e. `head > cursor`). The caller should then call
    /// `store.source(cursor, &condition)` to get the events.
    ///
    /// After processing the events, the caller should update `self.cursor`
    /// to the position after the last event processed.
    ///
    /// This method handles missed notifications gracefully — if the
    /// broadcast channel lags, it falls back to checking the atomic head
    /// position directly.
    pub async fn wait_for_new_events(&mut self) -> u64 {
        loop {
            // First check if there are already events at or beyond our cursor.
            let current = self.watermark.load(Ordering::Acquire);
            if current > self.cursor.0 {
                return current;
            }

            // Wait for a notification from the writer.
            match self.receiver.recv().await {
                Ok(notification) => {
                    if notification.watermark > self.cursor.0 {
                        return notification.watermark;
                    }
                    // Notification was for positions behind our cursor — keep waiting.
                }
                Err(broadcast::error::RecvError::Lagged(_)) => {
                    // We missed some notifications. No problem — check the atomic
                    // directly. This is the catch-up path.
                    let current = self.watermark.load(Ordering::Acquire);
                    if current > self.cursor.0 {
                        return current;
                    }
                    // Still nothing ahead of our cursor — keep waiting.
                }
                Err(broadcast::error::RecvError::Closed) => {
                    // The store was dropped. Return the current watermark to
                    // let the caller clean up gracefully.
                    return self.watermark.load(Ordering::Acquire);
                }
            }
        }
    }

    /// Advances the cursor to the given position.
    /// Call this after processing events returned by source().
    pub fn advance_cursor(&mut self, position: Position) {
        self.cursor = position;
    }
}
