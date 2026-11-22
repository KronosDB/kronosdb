//! The events at the end of a log, by identifier: how a retried append is
//! recognised.
//!
//! A client that does not hear whether its append was committed (the leader
//! changed, the connection dropped before the answer arrived) sends the same
//! append again. The log already holds every event's identifier, so each node
//! keeps a small index over the most recent events of its own copy: an append
//! whose events are all there, in the same order and with the same content, is
//! the same append, and is answered with the positions it already has instead
//! of being written a second time.
//!
//! Nothing is stored for this and nothing is replicated: every node builds the
//! index from the log it has, as it writes, as it applies what the leader
//! sends, and from its open segment when it starts. So it survives a
//! failover exactly as far as the events themselves did: a new leader that
//! holds the first attempt's events recognises the retry, and one that does
//! not writes it, correctly, as new.
//!
//! The window is a number of events, not a time. A retry that arrives after
//! that many newer events is written again. A full window takes about 58
//! bytes an event (66 MiB for 1,200,000); it grows as events are written, so
//! a quiet context holds little. After a restart the window
//! starts as the open segment's events: a first attempt whose segment was
//! sealed before the node stopped is not recognised.

use std::collections::{HashMap, VecDeque};
use std::hash::{BuildHasher, BuildHasherDefault, Hasher, RandomState};
use std::sync::OnceLock;

/// What identifies one event to the index: a keyed hash of its identifier,
/// and a check value over everything else it holds (its position excluded).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Fingerprint {
    id: u64,
    content: u64,
}

/// The process's hash keys. Fingerprints never leave the process, so each
/// node has its own; random keys keep a client from choosing identifiers
/// that collide.
fn keys() -> &'static RandomState {
    static KEYS: OnceLock<RandomState> = OnceLock::new();
    KEYS.get_or_init(RandomState::new)
}

impl Fingerprint {
    /// The fingerprint of an event record's body (`position u64 |
    /// identifier_len u16 | identifier | …`), or `None` for an event without
    /// an identifier, which can never be recognised as a retry.
    pub fn of(body: &[u8]) -> Option<Self> {
        let id_len = u16::from_le_bytes(body.get(8..10)?.try_into().ok()?) as usize;
        if id_len == 0 {
            return None;
        }
        let identifier = body.get(10..10 + id_len)?;
        // Zero is the queue's "no identifier" mark.
        let id = keys().hash_one(identifier).max(1);
        let rest = &body[8..];
        let content = u64::from(crc32c::crc32c(rest)) | ((rest.len() as u64) << 32);
        Some(Self { id, content })
    }
}

/// A map keyed by values that are already hashes.
#[derive(Default)]
struct PassThrough(u64);

impl Hasher for PassThrough {
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, _: &[u8]) {
        unreachable!("keys are u64");
    }

    fn write_u64(&mut self, value: u64) {
        self.0 = value;
    }
}

/// The most recent events of one log, by identifier.
pub struct RecentEvents {
    /// Identifier hash → the newest event with it: position, content check.
    by_id: HashMap<u64, (u64, u64), BuildHasherDefault<PassThrough>>,
    /// The identifier hash of each event in the window, oldest first; 0 for
    /// an event without one.
    order: VecDeque<u64>,
    /// The position of `order[0]`.
    first: u64,
    /// Most events held; 0 turns the index off.
    capacity: usize,
}

impl RecentEvents {
    pub fn new(capacity: usize) -> Self {
        Self {
            by_id: HashMap::default(),
            order: VecDeque::new(),
            first: 0,
            capacity,
        }
    }

    /// Most events held.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Events held now.
    pub fn len(&self) -> usize {
        self.order.len()
    }

    pub fn is_empty(&self) -> bool {
        self.order.is_empty()
    }

    /// The position after the newest event held.
    fn end(&self) -> u64 {
        self.first + self.order.len() as u64
    }

    /// Records the event written at `position`. Positions arrive in order;
    /// one that does not follow the last (a log that restarted elsewhere)
    /// starts the window again from there.
    pub fn note(&mut self, position: u64, fingerprint: Option<Fingerprint>) {
        if self.capacity == 0 {
            return;
        }
        if position != self.end() {
            self.by_id.clear();
            self.order.clear();
            self.first = position;
        }
        match fingerprint {
            Some(fingerprint) => {
                self.by_id
                    .insert(fingerprint.id, (position, fingerprint.content));
                self.order.push_back(fingerprint.id);
            }
            None => self.order.push_back(0),
        }
        while self.order.len() > self.capacity {
            let id = self.order.pop_front().expect("longer than the capacity");
            self.forget(id, self.first);
            self.first += 1;
        }
    }

    /// Drops `id`'s entry if it still points at `position`: a newer event
    /// with the same identifier keeps its own.
    fn forget(&mut self, id: u64, position: u64) {
        if id != 0 && self.by_id.get(&id).is_some_and(|(at, _)| *at == position) {
            self.by_id.remove(&id);
        }
    }

    /// Forgets the events at and after `position`: the log was cut back there.
    pub fn truncate_from(&mut self, position: u64) {
        while self.end() > position && !self.order.is_empty() {
            let id = self.order.pop_back().expect("not empty");
            self.forget(id, self.end());
        }
    }

    /// The position the events with these fingerprints already have, if all
    /// of them are in the window, in this order, one after the other, with the
    /// same content. Anything else (an event without an identifier, one that
    /// is not there, one whose content differs) is not a retry.
    pub fn position_of(&self, fingerprints: &[Option<Fingerprint>]) -> Option<u64> {
        let first = (*fingerprints.first()?)?;
        let (start, content) = *self.by_id.get(&first.id)?;
        if content != first.content {
            return None;
        }
        for (offset, fingerprint) in fingerprints.iter().enumerate().skip(1) {
            let fingerprint = (*fingerprint)?;
            let (position, content) = *self.by_id.get(&fingerprint.id)?;
            if position != start + offset as u64 || content != fingerprint.content {
                return None;
            }
        }
        Some(start)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A record body: position, identifier, then whatever follows.
    fn body(identifier: &str, rest: &[u8]) -> Vec<u8> {
        let mut out = 0u64.to_le_bytes().to_vec();
        out.extend_from_slice(&(identifier.len() as u16).to_le_bytes());
        out.extend_from_slice(identifier.as_bytes());
        out.extend_from_slice(rest);
        out
    }

    fn print(identifier: &str, rest: &[u8]) -> Option<Fingerprint> {
        Fingerprint::of(&body(identifier, rest))
    }

    #[test]
    fn the_same_events_in_the_same_order_are_found_where_they_are() {
        let mut recent = RecentEvents::new(100);
        recent.note(0, print("x", b"other"));
        recent.note(1, print("a", b"one"));
        recent.note(2, print("b", b"two"));
        assert_eq!(
            recent.position_of(&[print("a", b"one"), print("b", b"two")]),
            Some(1)
        );
        assert_eq!(recent.position_of(&[print("b", b"two")]), Some(2));
    }

    #[test]
    fn anything_else_is_not_a_retry() {
        let mut recent = RecentEvents::new(100);
        recent.note(0, print("a", b"one"));
        recent.note(1, print("b", b"two"));
        recent.note(2, print("c", b"three"));
        // Unknown identifier.
        assert_eq!(recent.position_of(&[print("z", b"one")]), None);
        // Same identifier, other content.
        assert_eq!(recent.position_of(&[print("a", b"changed")]), None);
        // Known events, but not next to each other, or in another order.
        assert_eq!(
            recent.position_of(&[print("a", b"one"), print("c", b"three")]),
            None
        );
        assert_eq!(
            recent.position_of(&[print("b", b"two"), print("a", b"one")]),
            None
        );
        // Part of it is new.
        assert_eq!(
            recent.position_of(&[print("c", b"three"), print("d", b"four")]),
            None
        );
        // Nothing to look for.
        assert_eq!(recent.position_of(&[]), None);
    }

    #[test]
    fn an_event_without_an_identifier_is_never_a_retry() {
        assert_eq!(print("", b"one"), None);
        let mut recent = RecentEvents::new(100);
        recent.note(0, print("", b"one"));
        recent.note(1, print("a", b"two"));
        assert_eq!(recent.position_of(&[print("", b"one")]), None);
        assert_eq!(recent.position_of(&[print("a", b"two")]), Some(1));
    }

    #[test]
    fn the_window_holds_the_newest_events_only() {
        let mut recent = RecentEvents::new(3);
        for position in 0..5u64 {
            recent.note(position, print(&format!("e{position}"), b"x"));
        }
        assert_eq!(recent.len(), 3);
        assert_eq!(recent.position_of(&[print("e1", b"x")]), None);
        assert_eq!(recent.position_of(&[print("e2", b"x")]), Some(2));
        assert_eq!(recent.position_of(&[print("e4", b"x")]), Some(4));
    }

    #[test]
    fn a_reused_identifier_points_at_its_newest_event_and_survives_the_older_ones_leaving() {
        let mut recent = RecentEvents::new(2);
        recent.note(0, print("a", b"first"));
        recent.note(1, print("a", b"second"));
        assert_eq!(recent.position_of(&[print("a", b"first")]), None);
        assert_eq!(recent.position_of(&[print("a", b"second")]), Some(1));
        // Position 0 leaves the window; the entry belongs to position 1.
        recent.note(2, print("b", b"x"));
        assert_eq!(recent.position_of(&[print("a", b"second")]), Some(1));
    }

    #[test]
    fn a_cut_log_forgets_what_was_cut() {
        let mut recent = RecentEvents::new(100);
        for position in 0..4u64 {
            recent.note(position, print(&format!("e{position}"), b"x"));
        }
        recent.truncate_from(2);
        assert_eq!(recent.position_of(&[print("e1", b"x")]), Some(1));
        assert_eq!(recent.position_of(&[print("e2", b"x")]), None);
        // What is written there next is found at its own position.
        recent.note(2, print("new", b"y"));
        assert_eq!(recent.position_of(&[print("new", b"y")]), Some(2));
    }

    #[test]
    fn a_log_that_continues_elsewhere_starts_the_window_again() {
        let mut recent = RecentEvents::new(100);
        recent.note(0, print("a", b"x"));
        recent.note(50, print("b", b"x"));
        assert_eq!(recent.position_of(&[print("a", b"x")]), None);
        assert_eq!(recent.position_of(&[print("b", b"x")]), Some(50));
    }

    #[test]
    fn a_capacity_of_zero_holds_nothing() {
        let mut recent = RecentEvents::new(0);
        recent.note(0, print("a", b"x"));
        assert!(recent.is_empty());
        assert_eq!(recent.position_of(&[print("a", b"x")]), None);
    }
}
