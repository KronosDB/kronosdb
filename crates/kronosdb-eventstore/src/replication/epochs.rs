//! Where each epoch begins in a node's log.
//!
//! Every leader writes an `EpochChange` as the first record of a fresh
//! segment, at the event position it takes over from, so an epoch owns the
//! events from its `start_position` up to the next epoch's. Two logs that both
//! contain an epoch's `EpochChange` hold the same bytes for that epoch up to the
//! shorter of the two (one leader wrote them, and followers copy them byte for
//! byte). That is Raft's log-matching property, and it is what lets a leader
//! find where a returning node's history stops agreeing with its own without
//! comparing the two logs.
//!
//! The index is kept as `EpochChange` records are written, applied from a
//! leader, recovered at open, or truncated away, so asking where an epoch ends
//! never scans the log.

/// One `EpochChange` in a log.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EpochStart {
    pub epoch: u64,
    /// The event position the epoch's leader took over at.
    pub start_position: u64,
}

/// Where an epoch's events end in a log.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EpochExtent {
    /// The log holds no `EpochChange` for this epoch: it was never seen here,
    /// was truncated away, or has been purged. Nothing can be said about it.
    Unknown,
    /// The newest epoch in the log: its events run to the log's tail.
    Open,
    /// A later epoch took over at this position, so the epoch's events are
    /// those below it.
    EndsAt(u64),
}

/// The `EpochChange` records of one log, oldest first.
#[derive(Debug, Default, Clone)]
pub struct EpochIndex {
    starts: Vec<EpochStart>,
}

impl EpochIndex {
    /// Notes an `EpochChange` that now sits at the end of the log. Epochs in a
    /// log only grow, so one that does not is a redelivery (or damage the
    /// record's own checks will report) and is left out. Returns whether it
    /// was added.
    pub fn record(&mut self, epoch: u64, start_position: u64) -> bool {
        if self.starts.last().is_some_and(|last| last.epoch >= epoch) {
            return false;
        }
        self.starts.push(EpochStart {
            epoch,
            start_position,
        });
        true
    }

    /// Forgets the epochs that began at or after `position`: the log was cut
    /// back there, and the `EpochChange` of an epoch that took over at the cut
    /// lies in a segment that is gone.
    pub fn truncate_from(&mut self, position: u64) {
        self.starts.retain(|start| start.start_position < position);
    }

    /// The newest epoch whose `EpochChange` is in the log.
    pub fn last_epoch(&self) -> Option<u64> {
        self.starts.last().map(|start| start.epoch)
    }

    /// Where `epoch` ends in this log.
    pub fn extent(&self, epoch: u64) -> EpochExtent {
        match self
            .starts
            .binary_search_by_key(&epoch, |start| start.epoch)
        {
            Err(_) => EpochExtent::Unknown,
            Ok(index) => match self.starts.get(index + 1) {
                Some(next) => EpochExtent::EndsAt(next.start_position),
                None => EpochExtent::Open,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn index(starts: &[(u64, u64)]) -> EpochIndex {
        let mut index = EpochIndex::default();
        for &(epoch, start) in starts {
            assert!(index.record(epoch, start));
        }
        index
    }

    #[test]
    fn an_epoch_ends_where_the_next_one_takes_over() {
        let index = index(&[(1, 0), (2, 5), (4, 9)]);
        assert_eq!(index.extent(1), EpochExtent::EndsAt(5));
        assert_eq!(index.extent(2), EpochExtent::EndsAt(9));
        assert_eq!(index.last_epoch(), Some(4));
    }

    #[test]
    fn the_newest_epoch_runs_to_the_tail() {
        let index = index(&[(1, 0), (2, 5)]);
        assert_eq!(index.extent(2), EpochExtent::Open);
    }

    #[test]
    fn epochs_the_log_never_saw_are_unknown() {
        // Epoch 3 was claimed by a node whose EpochChange never reached this
        // log: it is neither before, after, nor between known ones.
        let index = index(&[(1, 0), (2, 5), (4, 9)]);
        assert_eq!(index.extent(3), EpochExtent::Unknown);
        assert_eq!(index.extent(0), EpochExtent::Unknown);
        assert_eq!(index.extent(5), EpochExtent::Unknown);
        assert_eq!(EpochIndex::default().extent(1), EpochExtent::Unknown);
        assert_eq!(EpochIndex::default().last_epoch(), None);
    }

    #[test]
    fn a_purged_prefix_leaves_its_epochs_unknown() {
        // A log that starts at epoch 3 (older segments gone) says nothing
        // about epochs 1 and 2, but still knows where 3 ends.
        let index = index(&[(3, 100), (4, 140)]);
        assert_eq!(index.extent(2), EpochExtent::Unknown);
        assert_eq!(index.extent(3), EpochExtent::EndsAt(140));
    }

    #[test]
    fn redelivered_or_stale_records_are_not_added() {
        let mut index = index(&[(1, 0), (2, 5)]);
        assert!(!index.record(2, 5), "the same record again");
        assert!(!index.record(1, 0), "an older epoch");
        assert_eq!(index.last_epoch(), Some(2));
        assert_eq!(index.extent(1), EpochExtent::EndsAt(5));
    }

    #[test]
    fn truncating_drops_the_epochs_that_began_in_the_cut_segments() {
        let mut index = index(&[(1, 0), (2, 5), (3, 8)]);

        // Cutting at 8 removes the segment epoch 3 began.
        index.truncate_from(8);
        assert_eq!(index.last_epoch(), Some(2));
        assert_eq!(index.extent(2), EpochExtent::Open);
        assert_eq!(index.extent(3), EpochExtent::Unknown);

        // Cutting inside epoch 2 keeps epoch 2, which began lower.
        index.truncate_from(6);
        assert_eq!(index.last_epoch(), Some(2));

        // Cutting at its start position removes it.
        index.truncate_from(5);
        assert_eq!(index.last_epoch(), Some(1));
        assert_eq!(index.extent(1), EpochExtent::Open);

        // A later epoch can then be recorded again.
        assert!(index.record(4, 5));
        assert_eq!(index.extent(1), EpochExtent::EndsAt(5));
    }
}
