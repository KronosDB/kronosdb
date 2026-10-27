//! Live subscriptions, driven by what a commit contains.
//!
//! A subscription used to wake on every commit, ask the index whether the new
//! range held anything for it, and go back to sleep: with a few hundred
//! filtered subscribers that was most of the server's CPU, spent saying "no".
//! Now one dispatcher per context matches each newly committed event against
//! every live subscription at once and wakes only those it matched.
//!
//! ```text
//! commit ──► poke ──► dispatcher thread
//!                       │  reads [dispatched, watermark) from the log,
//!                       │  matches each event (name, tags) in the Matcher
//!                       ▼
//!                  wake the subscriptions that matched
//!                       │
//!                       ▼
//!          subscription reads [cursor, watermark) as before
//! ```
//!
//! # Why nothing is missed or repeated
//!
//! A wake is a hint, never data: a woken subscription reads from its own
//! cursor, so it cannot receive an event twice, and a spurious wake costs one
//! empty read. Missing one needs a matching event that neither the
//! subscription's own reads nor the dispatcher covered:
//!
//! - The subscription registers in the matcher *before* its first read, and
//!   every read loads the watermark after it takes the wake flag.
//! - The dispatcher loads the watermark *inside* the matcher lock, and
//!   matches the range it names against the registrations present at that
//!   moment.
//!
//! The lock orders every dispatch against every registration. A dispatch
//! that ran first loaded a watermark the registrant's later read will see, so
//! the registrant reads those events itself; one that ran after saw the
//! registration and wakes it. There is no separate catch-up state to hand
//! over from: a subscription far behind the head simply ignores wakes (they
//! fold into one mark) until it has read up to it.
//!
//! The mark a wake carries is the end of the dispatch chunk that held the
//! match. A subscription reads up to it, not to the watermark: everything
//! below the mark has been matched (or is read by the subscription itself),
//! and it keeps the cursor on chunk boundaries.
//!
//! # The live tail: chunks
//!
//! Re-reading what the dispatcher has just read, checked and matched would
//! be waste, so the dispatcher keeps what it read. Each dispatch chunk
//! `[from, to)` becomes a [`LiveChunk`] in a small ring: the client-visible
//! records of the range, copied once after one CRC check.
//!
//! - While a subscription wants every event (no filter, no exclusions) the
//!   chunk holds all the range's visible events as a ready-made `Events`
//!   frame body. A subscription whose cursor sits exactly at its start sends
//!   it as it is, shared with every other such subscriber.
//! - Otherwise the chunk holds the events some filter matched. For every
//!   filtering subscription the dispatcher records which of the chunk's
//!   records it matched (the matcher already knows: it is what woke it), and
//!   the subscription builds its own `Events` body from just those records.
//!   Exclusions are the matcher's business like any other criterion; system
//!   events are in no chunk, and system tags are stripped as the record is
//!   copied.
//!
//! What a chunk says about a subscription is only true if the dispatcher
//! matched the range against it, which it did for every chunk starting at or
//! after the position the subscription registered at (`since`). Anything
//! else (catching up through its past, a chunk already evicted, a cursor the
//! ring does not cover) reads through the index as before, which gives the
//! same records byte for byte.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering, fence};
use std::sync::{Arc, Once, OnceLock};
use std::thread::Thread;

use bytes::Bytes;
use parking_lot::{Mutex, RwLock};
use tokio::sync::Notify;

use crate::criteria::SourcingCondition;
use crate::error::Error;
use crate::store::{LogView, copy_client_record};

/// Most events the dispatcher matches while holding the registry lock; a
/// longer backlog is worked off in chunks so registering never waits long.
const CHUNK: u64 = 4096;

/// A shared batch ends at this many events, like a read's default page.
const BATCH_EVENTS: u32 = 1024;
/// ... or once its records pass this many bytes, like a read's byte cap.
const BATCH_BYTES: usize = 1024 * 1024;
/// Batches kept for subscribers that are a little behind the dispatcher.
const RING_BATCHES: usize = 256;
/// ... and the most bytes those batches may hold between them.
const RING_BYTES: usize = 16 * 1024 * 1024;

/// An event type or tag part, as stored.
type Key = Box<[u8]>;

/// Keys are client-supplied tag values and event types, so the hasher is
/// seeded per map (foldhash's `fast::RandomState`), as in the segment index:
/// cheap on short keys and not predictable from outside.
type KeyMap<V> = HashMap<Key, V, foldhash::fast::RandomState>;

struct Criterion {
    names: Box<[Key]>,
    tags: Box<[(Key, Key)]>,
}

struct Sub<H> {
    handle: H,
    criteria: Box<[Criterion]>,
    exclude: Box<[Key]>,
    /// Wants every event and excludes none: served from the whole chunk, so
    /// matching events one by one never needs to name it.
    plain: bool,
}

impl<H> Sub<H> {
    fn excludes(&self, name: &[u8]) -> bool {
        self.exclude.iter().any(|n| **n == *name)
    }
}

/// Where a criterion is filed: a candidate is only a candidate, `matches`
/// verifies the rest of the criterion.
#[derive(Clone, Copy)]
struct Entry {
    slot: u32,
    criterion: u32,
}

/// Which subscriptions an event belongs to, found without visiting the ones
/// it cannot belong to.
///
/// Each criterion is filed under one anchor: its first tag, else (no tags)
/// each of its event types, else it matches everything. An event looks up its
/// own name and each of its tags, so the work is the event's tag count plus
/// the candidates those lookups find, whatever the number of subscriptions.
pub(crate) struct Matcher<H> {
    subs: Vec<Option<Sub<H>>>,
    free: Vec<u32>,
    by_tag: KeyMap<KeyMap<Vec<Entry>>>,
    by_name: KeyMap<Vec<Entry>>,
    /// Subscriptions with a match-everything criterion and no exclusions:
    /// one event wakes them all, so a chunk wakes them once.
    everything: Vec<u32>,
    /// Match-everything subscriptions that exclude some event types and so
    /// must be asked per event.
    everything_excluding: Vec<u32>,
    live: usize,
}

/// Working set for matching one event: which subscriptions it matched, each
/// once.
#[derive(Default)]
pub(crate) struct Scratch {
    epoch: u32,
    stamp: Vec<u32>,
    hits: Vec<u32>,
}

impl Scratch {
    /// Starts matching an event against a matcher with `slots` slots.
    fn begin(&mut self, slots: usize) {
        self.epoch = self.epoch.wrapping_add(1);
        if self.epoch == 0 {
            self.stamp.iter_mut().for_each(|s| *s = 0);
            self.epoch = 1;
        }
        if self.stamp.len() < slots {
            self.stamp.resize(slots, 0);
        }
        self.hits.clear();
    }

    fn seen(&self, slot: u32) -> bool {
        self.stamp[slot as usize] == self.epoch
    }

    fn hit(&mut self, slot: u32) {
        if !self.seen(slot) {
            self.stamp[slot as usize] = self.epoch;
            self.hits.push(slot);
        }
    }

    /// The slots matched since `begin`, each once.
    pub(crate) fn hits(&self) -> &[u32] {
        &self.hits
    }
}

fn key(b: &[u8]) -> Key {
    b.to_vec().into_boxed_slice()
}

impl<H> Matcher<H> {
    pub(crate) fn new() -> Self {
        Self {
            subs: Vec::new(),
            free: Vec::new(),
            by_tag: KeyMap::default(),
            by_name: KeyMap::default(),
            everything: Vec::new(),
            everything_excluding: Vec::new(),
            live: 0,
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.live == 0
    }

    /// Whether some subscription wants every event, so shared batches pay.
    pub(crate) fn wants_shared_tail(&self) -> bool {
        !self.everything.is_empty()
    }

    /// Whether some subscription filters (or excludes), so each event it
    /// matches is recorded for it.
    pub(crate) fn wants_picks(&self) -> bool {
        self.live > self.everything.len()
    }

    /// The subscriptions that want every event: `matches` leaves them out,
    /// since one visible event in a chunk matches all of them.
    pub(crate) fn everything(&self) -> &[u32] {
        &self.everything
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.live
    }

    /// The number of slots ever handed out: what a `Scratch` must cover.
    pub(crate) fn slots(&self) -> usize {
        self.subs.len()
    }

    pub(crate) fn handle(&self, slot: u32) -> Option<&H> {
        self.subs.get(slot as usize)?.as_ref().map(|s| &s.handle)
    }

    /// Every live slot.
    pub(crate) fn live_slots(&self) -> impl Iterator<Item = u32> + '_ {
        self.subs
            .iter()
            .enumerate()
            .filter_map(|(i, s)| s.as_ref().map(|_| i as u32))
    }

    /// Files a subscription. Event types in `exclude` never match it. A
    /// condition with no criteria matches nothing, as it does when sourcing.
    pub(crate) fn insert(
        &mut self,
        condition: &SourcingCondition,
        exclude: &[String],
        handle: H,
    ) -> u32 {
        let criteria: Box<[Criterion]> = condition
            .criteria
            .iter()
            .map(|c| Criterion {
                names: c.names.iter().map(|n| key(n.as_bytes())).collect(),
                tags: c
                    .tags
                    .iter()
                    .map(|t| (key(&t.key), key(&t.value)))
                    .collect(),
            })
            .collect();
        let plain = exclude.is_empty()
            && criteria
                .iter()
                .any(|c| c.names.is_empty() && c.tags.is_empty());
        let sub = Sub {
            handle,
            criteria,
            exclude: exclude.iter().map(|n| key(n.as_bytes())).collect(),
            plain,
        };
        let slot = match self.free.pop() {
            Some(slot) => slot,
            None => {
                self.subs.push(None);
                (self.subs.len() - 1) as u32
            }
        };
        let mut everything = false;
        for (i, criterion) in sub.criteria.iter().enumerate() {
            let entry = Entry {
                slot,
                criterion: i as u32,
            };
            if let Some((key, value)) = criterion.tags.first() {
                self.by_tag
                    .entry(key.clone())
                    .or_default()
                    .entry(value.clone())
                    .or_default()
                    .push(entry);
            } else if !criterion.names.is_empty() {
                for name in criterion.names.iter() {
                    let list = self.by_name.entry(name.clone()).or_default();
                    if !list.iter().any(|e| e.slot == slot) {
                        list.push(entry);
                    }
                }
            } else {
                everything = true;
            }
        }
        if everything {
            if sub.exclude.is_empty() {
                self.everything.push(slot);
            } else {
                self.everything_excluding.push(slot);
            }
        }
        self.subs[slot as usize] = Some(sub);
        self.live += 1;
        slot
    }

    /// Unfiles a subscription, returning its handle.
    pub(crate) fn remove(&mut self, slot: u32) -> Option<H> {
        let sub = self.subs.get_mut(slot as usize)?.take()?;
        for criterion in sub.criteria.iter() {
            if let Some((key, value)) = criterion.tags.first() {
                if let Some(values) = self.by_tag.get_mut(key) {
                    if let Some(list) = values.get_mut(value) {
                        list.retain(|e| e.slot != slot);
                        if list.is_empty() {
                            values.remove(value);
                        }
                    }
                    if values.is_empty() {
                        self.by_tag.remove(key);
                    }
                }
            } else {
                for name in criterion.names.iter() {
                    if let Some(list) = self.by_name.get_mut(name) {
                        list.retain(|e| e.slot != slot);
                        if list.is_empty() {
                            self.by_name.remove(name);
                        }
                    }
                }
            }
        }
        self.everything.retain(|&s| s != slot);
        self.everything_excluding.retain(|&s| s != slot);
        self.free.push(slot);
        self.live -= 1;
        Some(sub.handle)
    }

    /// Records in `scratch` every subscription the event (`name`, `tags`)
    /// belongs to, except the ones that want every event (`everything`).
    /// `tags` is walked more than once, so it must be cheap to clone.
    pub(crate) fn matches<'a>(
        &self,
        scratch: &mut Scratch,
        name: &[u8],
        tags: impl Iterator<Item = (&'a [u8], &'a [u8])> + Clone,
    ) {
        if let Some(list) = self.by_name.get(name) {
            for entry in list {
                // Filed by name alone: the name matching is the whole criterion.
                if !scratch.seen(entry.slot) {
                    let sub = self.sub(entry.slot);
                    if !sub.plain && !sub.excludes(name) {
                        scratch.hit(entry.slot);
                    }
                }
            }
        }
        if !self.by_tag.is_empty() {
            for (key, value) in tags.clone() {
                let Some(list) = self.by_tag.get(key).and_then(|values| values.get(value)) else {
                    continue;
                };
                for entry in list {
                    if scratch.seen(entry.slot) {
                        continue;
                    }
                    let sub = self.sub(entry.slot);
                    if !sub.plain
                        && !sub.excludes(name)
                        && criterion_matches(&sub.criteria[entry.criterion as usize], name, &tags)
                    {
                        scratch.hit(entry.slot);
                    }
                }
            }
        }
        for &slot in &self.everything_excluding {
            if !scratch.seen(slot) && !self.sub(slot).excludes(name) {
                scratch.hit(slot);
            }
        }
    }

    fn sub(&self, slot: u32) -> &Sub<H> {
        self.subs[slot as usize]
            .as_ref()
            .expect("an indexed slot is live")
    }
}

fn criterion_matches<'a>(
    criterion: &Criterion,
    name: &[u8],
    tags: &(impl Iterator<Item = (&'a [u8], &'a [u8])> + Clone),
) -> bool {
    (criterion.names.is_empty() || criterion.names.iter().any(|n| **n == *name))
        && criterion
            .tags
            .iter()
            .all(|(key, value)| tags.clone().any(|(k, v)| k == &**key && v == &**value))
}

/// What the dispatcher shares with one subscription.
pub(crate) struct Shared {
    /// The end of the newest dispatch chunk that held an event for this
    /// subscription, or zero: the subscription owes the log a read up to it.
    /// Wakes fold into the highest mark.
    mark: AtomicU64,
    notify: Notify,
}

impl Shared {
    /// Takes the mark. Zero when no read is owed.
    pub(crate) fn take(&self) -> u64 {
        self.mark.swap(0, Ordering::AcqRel)
    }

    /// Whether a read is owed, without taking it.
    pub(crate) fn is_pending(&self) -> bool {
        self.mark.load(Ordering::Acquire) != 0
    }

    /// Waits for a wake. Cancel-safe: a wake that lands while nobody waits
    /// is kept for the next call.
    pub(crate) async fn woken(&self) {
        self.notify.notified().await;
    }

    /// Owes a read up to `to`.
    fn wake(&self, to: u64) {
        if self.mark.fetch_max(to, Ordering::AcqRel) == 0 {
            self.notify.notify_one();
        }
    }
}

/// The body of an `Events` frame with no events and no marker.
const EMPTY_BODY: [u8; 5] = [0; 5];

/// A run of committed events `[from, to)`, encoded once for every subscriber
/// that wants some of them.
///
/// The records are the ones a client reads (`len | flags | body`, system tags
/// stripped), copied out of the log once, after one CRC check. Which of them
/// the chunk holds depends on who was subscribed when it was made:
///
/// - while a subscription wanted every event, all of the range's visible
///   events (`complete`), and `body` is a ready-made `Events` frame body;
/// - otherwise just the events some subscription matched.
///
/// For every filtering subscription the dispatcher also records which of the
/// chunk's records matched it, so it can build its own `Events` body from
/// them without touching the log.
pub struct LiveChunk {
    /// The first position the chunk covers.
    pub from: u64,
    /// The position after the last it covers.
    pub to: u64,
    /// Events in `body`: system events are left out, so this can be fewer
    /// than `to - from`, even none.
    pub count: u32,
    /// `count u32 LE | records | 0u8`: the records as a client reads them
    /// (`len | flags | body`), then the absent-marker byte. Cloning is a
    /// reference count.
    pub body: Bytes,
    /// `body` holds every event a client can see in `[from, to)`.
    complete: bool,
    /// Where record `i` starts in `body`; one more entry marks the end of the
    /// last. Empty when nobody picks.
    starts: Box<[u32]>,
    /// Record `i`'s position, relative to `from`.
    positions: Box<[u32]>,
    picks: Picks,
}

/// Which records of a chunk matched which subscription: slot `pick_slots[i]`
/// matched `recs[ends[i - 1]..ends[i]]` (record indexes, ascending).
#[derive(Default)]
struct Picks {
    slots: Box<[u32]>,
    ends: Box<[u32]>,
    recs: Box<[u32]>,
}

impl LiveChunk {
    /// A range in which nothing was recorded.
    fn empty(from: u64, to: u64, complete: bool) -> Self {
        Self {
            from,
            to,
            count: 0,
            body: Bytes::from_static(&EMPTY_BODY),
            complete,
            starts: Box::default(),
            positions: Box::default(),
            picks: Picks::default(),
        }
    }

    fn position(&self, record: u32) -> u64 {
        self.from + self.positions[record as usize] as u64
    }

    /// The records (ascending indexes) that matched `slot`'s subscription.
    /// Only meaningful for a subscription that was registered before the
    /// chunk was made.
    fn picked(&self, slot: u32) -> &[u32] {
        match self.picks.slots.binary_search(&slot) {
            Ok(i) => {
                let start = if i == 0 {
                    0
                } else {
                    self.picks.ends[i - 1] as usize
                };
                &self.picks.recs[start..self.picks.ends[i] as usize]
            }
            Err(_) => &[],
        }
    }

    /// An `Events` body (no marker) with the given records, in order.
    fn body_of(&self, records: &[u32]) -> Bytes {
        let start = |r: u32| self.starts[r as usize] as usize;
        let end = |r: u32| self.starts[r as usize + 1] as usize;
        let size: usize = records.iter().map(|&r| end(r) - start(r)).sum();
        let mut body = Vec::with_capacity(4 + size + 1);
        body.extend_from_slice(&(records.len() as u32).to_le_bytes());
        // Neighbouring records are neighbours in the chunk: copy runs.
        let mut run: Option<(usize, usize)> = None;
        for (i, &r) in records.iter().enumerate() {
            match run {
                Some((from, _)) if i > 0 && records[i - 1] + 1 == r => run = Some((from, end(r))),
                Some((from, to)) => {
                    body.extend_from_slice(&self.body[from..to]);
                    run = Some((start(r), end(r)));
                }
                None => run = Some((start(r), end(r))),
            }
        }
        if let Some((from, to)) = run {
            body.extend_from_slice(&self.body[from..to]);
        }
        body.push(0);
        Bytes::from(body)
    }
}

/// What a filtering subscription takes from a chunk.
pub struct LivePick {
    /// `count u32 | records | 0u8`, or `None` when the chunk holds nothing
    /// more for the subscription.
    pub body: Option<Bytes>,
    /// Events in `body`.
    pub count: u32,
    /// Where the subscription's cursor goes: everything before it that
    /// matches has been sent.
    pub next: u64,
}

/// Builds a chunk record by record.
struct ChunkBuilder {
    body: Vec<u8>,
    count: u32,
    scratch: Vec<u8>,
    from: u64,
    /// Remember where each record is, for subscriptions that pick.
    track: bool,
    starts: Vec<u32>,
    positions: Vec<u32>,
}

impl ChunkBuilder {
    fn new(from: u64, track: bool) -> Self {
        let mut body = Vec::with_capacity(8 * 1024);
        body.extend_from_slice(&[0; 4]);
        Self {
            body,
            count: 0,
            scratch: Vec::new(),
            from,
            track,
            starts: Vec::new(),
            positions: Vec::new(),
        }
    }

    /// Copies the record in; returns its index in the chunk.
    fn push(
        &mut self,
        position: u64,
        record: &crate::segment::reader::StoredRecord<'_>,
        system_tags: bool,
    ) -> Result<u32, Error> {
        if self.track {
            self.starts.push(self.body.len() as u32);
            self.positions.push((position - self.from) as u32);
        }
        copy_client_record(&mut self.body, record, system_tags, &mut self.scratch)?;
        self.count += 1;
        Ok(self.count - 1)
    }

    fn full(&self) -> bool {
        self.count >= BATCH_EVENTS || self.body.len() >= BATCH_BYTES
    }

    fn finish(mut self, from: u64, to: u64, complete: bool, picks: Picks) -> LiveChunk {
        self.body[0..4].copy_from_slice(&self.count.to_le_bytes());
        if self.track {
            self.starts.push(self.body.len() as u32);
        }
        self.body.push(0);
        LiveChunk {
            from,
            to,
            count: self.count,
            body: Bytes::from(self.body),
            complete,
            starts: self.starts.into_boxed_slice(),
            positions: self.positions.into_boxed_slice(),
            picks,
        }
    }
}

/// Which records each filtering subscription matched, while a chunk is built.
#[derive(Default)]
struct PickScratch {
    by_slot: Vec<Vec<u32>>,
    touched: Vec<u32>,
}

impl PickScratch {
    fn begin(&mut self, slots: usize) {
        if self.by_slot.len() < slots {
            self.by_slot.resize_with(slots, Vec::new);
        }
    }

    fn add(&mut self, slot: u32, record: u32) {
        let list = &mut self.by_slot[slot as usize];
        if list.is_empty() {
            self.touched.push(slot);
        }
        list.push(record);
    }

    /// The picks of the chunk just built; leaves the scratch empty.
    fn take(&mut self) -> Picks {
        self.touched.sort_unstable();
        let mut ends = Vec::with_capacity(self.touched.len());
        let mut recs = Vec::new();
        for &slot in &self.touched {
            let list = &mut self.by_slot[slot as usize];
            recs.extend_from_slice(list);
            list.clear();
            ends.push(recs.len() as u32);
        }
        Picks {
            slots: std::mem::take(&mut self.touched).into_boxed_slice(),
            ends: ends.into_boxed_slice(),
            recs: recs.into_boxed_slice(),
        }
    }

    fn clear(&mut self) {
        for &slot in &self.touched {
            self.by_slot[slot as usize].clear();
        }
        self.touched.clear();
    }
}

/// The most recent chunks, oldest first, contiguous except where the
/// dispatcher could not read the log or nobody was subscribed.
struct Ring {
    chunks: VecDeque<Arc<LiveChunk>>,
    bytes: usize,
    max_chunks: usize,
    max_bytes: usize,
}

impl Default for Ring {
    fn default() -> Self {
        Self {
            chunks: VecDeque::new(),
            bytes: 0,
            max_chunks: RING_BATCHES,
            max_bytes: RING_BYTES,
        }
    }
}

impl Ring {
    /// Adds the chunk, which starts where the last one ended or later.
    /// Returns it, unless it was folded into the last chunk (only an empty
    /// one is, and an empty chunk has nothing to pick).
    fn push(&mut self, chunk: LiveChunk) -> Option<Arc<LiveChunk>> {
        // Quiet stretches would otherwise push the useful chunks out: a run
        // of empty ones is one empty chunk.
        if chunk.count == 0
            && let Some(last) = self.chunks.back_mut()
            && last.count == 0
            && last.complete == chunk.complete
            && last.to == chunk.from
            && let Some(last) = Arc::get_mut(last)
        {
            last.to = chunk.to;
            return None;
        }
        let chunk = Arc::new(chunk);
        self.bytes += chunk.body.len();
        self.chunks.push_back(Arc::clone(&chunk));
        while self.chunks.len() > self.max_chunks
            || (self.bytes > self.max_bytes && self.chunks.len() > 1)
        {
            if let Some(old) = self.chunks.pop_front() {
                self.bytes -= old.body.len();
            }
        }
        Some(chunk)
    }

    /// The chunk whose range holds `position`.
    fn covering(&self, position: u64) -> Option<&Arc<LiveChunk>> {
        // A stream at the live tail is nearly always in the newest chunk.
        if let Some(last) = self.chunks.back()
            && last.from <= position
        {
            return (position < last.to).then_some(last);
        }
        let i = self.chunks.partition_point(|c| c.to <= position);
        self.chunks.get(i).filter(|c| c.from <= position)
    }
}

struct FeedState {
    matcher: Matcher<Arc<Shared>>,
    scratch: Scratch,
    picks: PickScratch,
}

/// The dispatcher's wake-up: a flag and the thread to unpark.
struct Poke {
    pending: AtomicBool,
    thread: OnceLock<Thread>,
}

impl Poke {
    fn fire(&self) {
        if !self.pending.swap(true, Ordering::AcqRel)
            && let Some(thread) = self.thread.get()
        {
            thread.unpark();
        }
    }
}

/// One context's live subscriptions and the dispatcher that serves them.
pub(crate) struct CommitFeed {
    state: Mutex<FeedState>,
    /// Live subscriptions: lets a commit with none skip the dispatcher.
    subs: AtomicUsize,
    /// Events below this have been matched against every subscription
    /// registered at the time (see `frontier` on `EventStream`).
    dispatched: AtomicU64,
    poke: Poke,
    closed: AtomicBool,
    started: Once,
    /// Recent chunks; see the module docs.
    ring: RwLock<Ring>,
}

impl CommitFeed {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(FeedState {
                matcher: Matcher::new(),
                scratch: Scratch::default(),
                picks: PickScratch::default(),
            }),
            subs: AtomicUsize::new(0),
            dispatched: AtomicU64::new(0),
            poke: Poke {
                pending: AtomicBool::new(false),
                thread: OnceLock::new(),
            },
            closed: AtomicBool::new(false),
            started: Once::new(),
            ring: RwLock::new(Ring::default()),
        })
    }

    /// The watermark advanced. Called by every path that advances it, after
    /// the advance; costs one fence and a load when nobody subscribes.
    pub(crate) fn committed(&self) {
        // Pairs with the fence in `register`: either this sees the new
        // subscription and pokes, or the subscription's first read sees the
        // new watermark.
        fence(Ordering::SeqCst);
        if self.subs.load(Ordering::Relaxed) > 0 {
            self.poke.fire();
        }
    }

    /// Files a subscription. It owes its first read already. Returns its
    /// slot, what the dispatcher shares with it, and the position from which
    /// the dispatcher matches events against it (chunks starting there or
    /// later hold its picks).
    pub(crate) fn register(
        self: &Arc<Self>,
        view: &LogView,
        condition: &SourcingCondition,
        exclude: &[String],
    ) -> (u32, Arc<Shared>, u64) {
        self.ensure_started(view);
        let (slot, shared, since) = {
            let mut state = self.state.lock();
            if state.matcher.is_empty() {
                // Nobody was listening, so nothing between the old mark and
                // now was matched, and nothing needs to be: the newcomer
                // reads its own past.
                let now = view.watermark();
                if now > self.dispatched.load(Ordering::Relaxed) {
                    self.dispatched.store(now, Ordering::Release);
                }
            }
            // Everything below the dispatched mark was matched without this
            // subscription, so it owes a read up to there; everything from
            // there on, the dispatcher matches against it.
            let since = self.dispatched.load(Ordering::Relaxed);
            let shared = Arc::new(Shared {
                mark: AtomicU64::new(since),
                notify: Notify::new(),
            });
            let slot = state.matcher.insert(condition, exclude, shared.clone());
            self.subs.fetch_add(1, Ordering::SeqCst);
            (slot, shared, since)
        };
        fence(Ordering::SeqCst);
        (slot, shared, since)
    }

    pub(crate) fn unregister(&self, slot: u32) {
        let mut state = self.state.lock();
        if state.matcher.remove(slot).is_some() {
            self.subs.fetch_sub(1, Ordering::SeqCst);
        }
    }

    /// Events below this have been matched against every subscription that
    /// was registered when they were.
    pub(crate) fn dispatched(&self) -> u64 {
        self.dispatched.load(Ordering::Acquire)
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    /// Stops the dispatcher and wakes every subscription so it can notice.
    pub(crate) fn close(&self) {
        self.closed.store(true, Ordering::Release);
        self.poke.fire();
        if let Some(thread) = self.poke.thread.get() {
            thread.unpark();
        }
        let state = self.state.lock();
        for slot in state.matcher.live_slots() {
            if let Some(shared) = state.matcher.handle(slot) {
                shared.wake(u64::MAX);
            }
        }
    }

    /// The complete chunk that starts exactly at `from`, if the ring has it.
    pub(crate) fn batch_at(&self, from: u64) -> Option<Arc<LiveChunk>> {
        let ring = self.ring.read();
        ring.covering(from)
            .filter(|c| c.from == from && c.complete)
            .cloned()
    }

    /// What the subscription in `slot`, registered at `since`, takes from the
    /// ring at `cursor`: its matches in the chunk covering the cursor, at
    /// most `max_events` of them. `None` when no chunk covers the cursor, or
    /// the one that does was made before the subscription existed.
    pub(crate) fn pick_at(
        &self,
        slot: u32,
        since: u64,
        cursor: u64,
        max_events: usize,
    ) -> Option<LivePick> {
        if max_events == 0 {
            return None;
        }
        let chunk = {
            let ring = self.ring.read();
            ring.covering(cursor).filter(|c| c.from >= since)?.clone()
        };
        let picked = chunk.picked(slot);
        let first = picked.partition_point(|&r| chunk.position(r) < cursor);
        let picked = &picked[first..];
        let take = picked.len().min(max_events);
        let next = if take == picked.len() {
            chunk.to
        } else {
            chunk.position(picked[take - 1]) + 1
        };
        Some(LivePick {
            body: (take > 0).then(|| chunk.body_of(&picked[..take])),
            count: take as u32,
            next,
        })
    }

    /// Limits what the ring keeps: a test hook for subscribers that fall
    /// behind it.
    pub(crate) fn limit_ring(&self, chunks: usize, bytes: usize) {
        let mut ring = self.ring.write();
        ring.max_chunks = chunks.max(1);
        ring.max_bytes = bytes;
    }

    fn ensure_started(self: &Arc<Self>, view: &LogView) {
        self.started.call_once(|| {
            let feed = Arc::clone(self);
            let view = view.clone();
            let handle = std::thread::Builder::new()
                .name("kronosdb-subs".into())
                .spawn(move || run(feed, view))
                .expect("spawn subscription dispatcher");
            let _ = self.poke.thread.set(handle.thread().clone());
        });
    }

    /// Matches the next stretch of committed events. True when more remain.
    fn dispatch_chunk(&self, view: &LogView) -> bool {
        let mut guard = self.state.lock();
        // Loaded inside the lock: see the module docs.
        let watermark = view.watermark();
        let from = self.dispatched.load(Ordering::Relaxed);
        if guard.matcher.is_empty() {
            if watermark > from {
                self.dispatched.store(watermark, Ordering::Release);
            }
            return false;
        }
        if watermark <= from {
            return false;
        }
        let limit = watermark.min(from + CHUNK);
        let FeedState {
            matcher,
            scratch,
            picks,
        } = &mut *guard;
        // Every record that anyone wants goes in the chunk: all of them while
        // a subscription follows everything, else the ones some filter matched.
        let complete = matcher.wants_shared_tail();
        let track = matcher.wants_picks();
        picks.begin(matcher.slots());
        let mut batch: Option<ChunkBuilder> = None;
        let mut visible = false;
        let mut failed = None;
        let visited = view.visit_events(from, limit, |position, name, tags, record| {
            // System events are invisible to clients, so they wake nobody
            // and are in no chunk.
            if crate::system::is_system_name_bytes(name) {
                return true;
            }
            visible = true;
            scratch.begin(matcher.slots());
            matcher.matches(scratch, name, tags.clone());
            if !complete && scratch.hits().is_empty() {
                return true;
            }
            let batch = batch.get_or_insert_with(|| ChunkBuilder::new(from, track));
            let system_tags = tags
                .clone()
                .any(|(k, _)| crate::system::is_system_name_bytes(k));
            match batch.push(position, record, system_tags) {
                Ok(index) => {
                    for &slot in scratch.hits() {
                        picks.add(slot, index);
                    }
                    !batch.full()
                }
                Err(error) => {
                    failed = Some(error);
                    false
                }
            }
        });
        // Where this chunk ends: `limit`, or sooner when a chunk filled up.
        let visited = match (visited, failed) {
            (Ok(to), None) => Ok(to),
            (Ok(_), Some(error)) | (Err(error), _) => Err(error),
        };
        let to = match visited {
            Ok(to) => {
                let chunk = match batch {
                    Some(batch) => batch.finish(from, to, complete, picks.take()),
                    None => LiveChunk::empty(from, to, complete),
                };
                // In the ring before anyone is woken: a woken subscription
                // looks for this chunk.
                if let Some(chunk) = self.ring.write().push(chunk) {
                    for &slot in chunk.picks.slots.iter() {
                        if let Some(shared) = matcher.handle(slot) {
                            shared.wake(to);
                        }
                    }
                }
                if visible {
                    for &slot in matcher.everything() {
                        if let Some(shared) = matcher.handle(slot) {
                            shared.wake(to);
                        }
                    }
                }
                to
            }
            Err(error) => {
                // Cannot tell what the range holds: let every subscription
                // read it and find out (it reports the same error if the log
                // really is unreadable).
                tracing::warn!(
                    %error, from, limit,
                    "subscription dispatch could not read the log; waking every subscription"
                );
                picks.clear();
                for slot in matcher.live_slots() {
                    if let Some(shared) = matcher.handle(slot) {
                        shared.wake(limit);
                    }
                }
                limit
            }
        };
        // After the wakes: a reader that sees this mark also sees them.
        self.dispatched.store(to, Ordering::Release);
        to < watermark
    }
}

/// The dispatcher thread: sleeps until a commit pokes it, then matches
/// everything committed since its last pass.
fn run(feed: Arc<CommitFeed>, view: LogView) {
    loop {
        loop {
            if feed.is_closed() {
                return;
            }
            if feed.poke.pending.swap(false, Ordering::AcqRel) {
                break;
            }
            std::thread::park();
        }
        while feed.dispatch_chunk(&view) {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::criteria::Criterion as Crit;
    use crate::event::Tag;

    fn crit(names: &[&str], tags: &[(&str, &str)]) -> Crit {
        Crit {
            names: names.iter().map(|n| n.to_string()).collect(),
            tags: tags.iter().map(|(k, v)| Tag::from_str(k, v)).collect(),
        }
    }

    fn cond(criteria: Vec<Crit>) -> SourcingCondition {
        SourcingCondition { criteria }
    }

    /// Slots matched by one event, sorted.
    fn matched(m: &Matcher<&'static str>, name: &str, tags: &[(&str, &str)]) -> Vec<u32> {
        let mut scratch = Scratch::default();
        scratch.begin(m.slots());
        let tags: Vec<(&[u8], &[u8])> = tags
            .iter()
            .map(|(k, v)| (k.as_bytes(), v.as_bytes()))
            .collect();
        m.matches(&mut scratch, name.as_bytes(), tags.iter().copied());
        // The subscriptions that want everything are not matched event by
        // event; any visible event is theirs.
        for &slot in m.everything() {
            scratch.hit(slot);
        }
        let mut hits = scratch.hits().to_vec();
        hits.sort_unstable();
        hits
    }

    #[test]
    fn a_tag_criterion_matches_events_carrying_the_tag() {
        let mut m = Matcher::new();
        let a = m.insert(&cond(vec![crit(&[], &[("order", "1")])]), &[], "a");
        let b = m.insert(&cond(vec![crit(&[], &[("order", "2")])]), &[], "b");
        assert_eq!(matched(&m, "E", &[("order", "1")]), vec![a]);
        assert_eq!(matched(&m, "E", &[("order", "2"), ("x", "y")]), vec![b]);
        assert_eq!(matched(&m, "E", &[("order", "3")]), Vec::<u32>::new());
        assert_eq!(matched(&m, "E", &[]), Vec::<u32>::new());
    }

    #[test]
    fn every_tag_of_a_criterion_must_be_present() {
        let mut m = Matcher::new();
        let a = m.insert(
            &cond(vec![crit(&[], &[("order", "1"), ("customer", "c")])]),
            &[],
            "a",
        );
        assert_eq!(matched(&m, "E", &[("order", "1")]), Vec::<u32>::new());
        assert_eq!(matched(&m, "E", &[("customer", "c")]), Vec::<u32>::new());
        // The anchor is the first tag, not a privileged one: order of the
        // event's tags is irrelevant.
        assert_eq!(
            matched(&m, "E", &[("customer", "c"), ("order", "1")]),
            vec![a]
        );
        // Same key, wrong value for the second tag.
        assert_eq!(
            matched(&m, "E", &[("order", "1"), ("customer", "d")]),
            Vec::<u32>::new()
        );
    }

    #[test]
    fn names_alone_match_by_event_type() {
        let mut m = Matcher::new();
        let a = m.insert(&cond(vec![crit(&["Placed", "Shipped"], &[])]), &[], "a");
        assert_eq!(matched(&m, "Placed", &[("t", "1")]), vec![a]);
        assert_eq!(matched(&m, "Shipped", &[]), vec![a]);
        assert_eq!(matched(&m, "Other", &[]), Vec::<u32>::new());
    }

    #[test]
    fn names_narrow_a_tag_criterion() {
        let mut m = Matcher::new();
        let a = m.insert(&cond(vec![crit(&["Placed"], &[("order", "1")])]), &[], "a");
        assert_eq!(matched(&m, "Placed", &[("order", "1")]), vec![a]);
        assert_eq!(matched(&m, "Shipped", &[("order", "1")]), Vec::<u32>::new());
        assert_eq!(matched(&m, "Placed", &[("order", "2")]), Vec::<u32>::new());
    }

    #[test]
    fn criteria_are_alternatives() {
        let mut m = Matcher::new();
        let a = m.insert(
            &cond(vec![
                crit(&["Placed"], &[("order", "1")]),
                crit(&["Shipped"], &[]),
            ]),
            &[],
            "a",
        );
        assert_eq!(matched(&m, "Placed", &[("order", "1")]), vec![a]);
        assert_eq!(matched(&m, "Shipped", &[("order", "9")]), vec![a]);
        assert_eq!(matched(&m, "Placed", &[("order", "9")]), Vec::<u32>::new());
    }

    #[test]
    fn an_empty_criterion_matches_everything_and_no_criteria_nothing() {
        let mut m = Matcher::new();
        let all = m.insert(&cond(vec![crit(&[], &[])]), &[], "all");
        let none = m.insert(&cond(vec![]), &[], "none");
        let got = matched(&m, "Anything", &[("k", "v")]);
        assert_eq!(got, vec![all]);
        assert!(!got.contains(&none));
    }

    #[test]
    fn exclusions_hide_event_types_from_every_kind_of_criterion() {
        let mut m = Matcher::new();
        let ex = vec!["Noise".to_string()];
        let all = m.insert(&cond(vec![crit(&[], &[])]), &ex, "all");
        let by_tag = m.insert(&cond(vec![crit(&[], &[("o", "1")])]), &ex, "tag");
        let by_name = m.insert(&cond(vec![crit(&["Noise", "Signal"], &[])]), &ex, "name");
        assert_eq!(
            matched(&m, "Signal", &[("o", "1")]),
            vec![all, by_tag, by_name]
        );
        assert_eq!(matched(&m, "Noise", &[("o", "1")]), Vec::<u32>::new());
    }

    #[test]
    fn a_subscription_is_reported_once_however_many_ways_it_matches() {
        let mut m = Matcher::new();
        let a = m.insert(
            &cond(vec![
                crit(&[], &[("o", "1")]),
                crit(&["E"], &[]),
                crit(&[], &[]),
            ]),
            &[],
            "a",
        );
        assert_eq!(matched(&m, "E", &[("o", "1"), ("o", "1")]), vec![a]);
    }

    #[test]
    fn identical_subscriptions_are_all_found() {
        let mut m = Matcher::new();
        let slots: Vec<u32> = (0..5)
            .map(|_| m.insert(&cond(vec![crit(&[], &[("o", "1")])]), &[], "s"))
            .collect();
        assert_eq!(matched(&m, "E", &[("o", "1")]), slots);
        assert_eq!(m.len(), 5);
    }

    #[test]
    fn removal_unfiles_everything_and_reuses_the_slot() {
        let mut m = Matcher::new();
        let a = m.insert(
            &cond(vec![crit(&["E"], &[]), crit(&[], &[("o", "1")])]),
            &[],
            "a",
        );
        let b = m.insert(&cond(vec![crit(&[], &[])]), &[], "b");
        assert_eq!(m.remove(a), Some("a"));
        assert_eq!(m.remove(a), None);
        assert_eq!(matched(&m, "E", &[("o", "1")]), vec![b]);
        assert_eq!(m.remove(b), Some("b"));
        assert!(m.is_empty());
        assert!(m.by_tag.is_empty() && m.by_name.is_empty());
        assert_eq!(matched(&m, "E", &[("o", "1")]), Vec::<u32>::new());
        let c = m.insert(&cond(vec![crit(&["E"], &[])]), &[], "c");
        assert_eq!(c, b, "the most recently freed slot is reused");
        assert_eq!(matched(&m, "E", &[]), vec![c]);
    }

    #[test]
    fn scratch_passes_do_not_leak_into_each_other() {
        let mut m = Matcher::new();
        let a = m.insert(&cond(vec![crit(&["E"], &[])]), &[], "a");
        let mut scratch = Scratch::default();
        for _ in 0..3 {
            scratch.begin(m.slots());
            m.matches(&mut scratch, b"E", std::iter::empty());
            m.matches(&mut scratch, b"E", std::iter::empty());
            assert_eq!(scratch.hits(), &[a]);
        }
        scratch.begin(m.slots());
        m.matches(&mut scratch, b"F", std::iter::empty());
        assert!(scratch.hits().is_empty());
    }

    #[test]
    fn tags_are_raw_bytes() {
        let mut m = Matcher::new();
        let a = m.insert(
            &cond(vec![Crit {
                names: vec![],
                tags: vec![Tag::new(vec![0xff, 0x00], vec![0x01, 0xfe])],
            }]),
            &[],
            "a",
        );
        let mut scratch = Scratch::default();
        scratch.begin(m.slots());
        let tags = [(&[0xffu8, 0x00][..], &[0x01u8, 0xfe][..])];
        m.matches(&mut scratch, b"E", tags.iter().copied());
        assert_eq!(scratch.hits(), &[a]);
    }
    /// A stored record for `position`, framed as the log holds it.
    fn stored(position: u64, name: &str, tag: (&str, &str)) -> (Vec<u8>, Vec<u8>) {
        use crate::event::{AppendEvent, Position};
        use crate::segment::format;

        let event = AppendEvent {
            identifier: format!("e{position}"),
            name: name.into(),
            version: "1".into(),
            timestamp: 0,
            payload: vec![1, 2, 3],
            metadata: vec![],
            tags: vec![Tag::from_str(tag.0, tag.1)],
        };
        let mut body = Vec::new();
        format::serialize_append_event(Position(position), &event, &mut body);
        let mut framed = ((1 + body.len()) as u32).to_le_bytes().to_vec();
        framed.push(0);
        framed.extend_from_slice(&body);
        (body, framed)
    }

    fn positions_of(body: &[u8]) -> Vec<u64> {
        use kronosdb_protocol as p;
        let Some(p::Frame::Events(events)) = p::Frame::decode(p::Events::KIND, body).unwrap()
        else {
            panic!("not an Events frame");
        };
        assert_eq!(events.marker, None);
        events
            .events
            .iter()
            .map(|r| r.event().unwrap().position)
            .collect()
    }

    fn build(positions: &[u64], from: u64, to: u64, picks: Picks) -> LiveChunk {
        use crate::segment::reader::StoredRecord;
        let mut batch = ChunkBuilder::new(from, true);
        for &position in positions {
            let (body, framed) = stored(position, "E", ("k", "v"));
            batch
                .push(
                    position,
                    &StoredRecord {
                        flags: 0,
                        body: &body,
                        framed: &framed,
                    },
                    false,
                )
                .unwrap();
        }
        batch.finish(from, to, true, picks)
    }

    #[test]
    fn a_chunk_body_is_an_events_frame_of_the_visible_records() {
        let chunk = build(&[4, 5, 6], 4, 7, Picks::default());
        assert_eq!((chunk.from, chunk.to, chunk.count), (4, 7, 3));
        assert_eq!(positions_of(&chunk.body), vec![4, 5, 6]);
        assert_eq!(chunk.starts.len(), 4);
        assert_eq!(chunk.starts[0], 4, "records follow the count");
        assert!(chunk.starts.windows(2).all(|w| w[0] < w[1]));
        assert_eq!(chunk.starts[3] as usize, chunk.body.len() - 1);
        assert_eq!(&*chunk.positions, &[0, 1, 2]);
    }

    #[test]
    fn a_pick_body_holds_just_the_picked_records_in_order() {
        // Records 0..8 at positions 10, 11, 13, 14, 15, 18, 19, 21.
        let at = [10u64, 11, 13, 14, 15, 18, 19, 21];
        let chunk = build(&at, 10, 22, Picks::default());
        for picked in [
            vec![],
            vec![0],
            vec![7],
            vec![0, 1],
            vec![1, 2, 3, 4],
            vec![0, 2, 3, 5, 6, 7],
            (0..8).collect::<Vec<u32>>(),
        ] {
            let body = chunk.body_of(&picked);
            let want: Vec<u64> = picked.iter().map(|&r| at[r as usize]).collect();
            assert_eq!(positions_of(&body), want, "records {picked:?}");
        }
    }

    #[test]
    fn picks_are_found_by_slot() {
        let mut scratch = PickScratch::default();
        scratch.begin(8);
        scratch.add(5, 0);
        scratch.add(2, 0);
        scratch.add(5, 2);
        scratch.add(2, 3);
        scratch.add(5, 3);
        let picks = scratch.take();
        assert!(scratch.touched.is_empty() && scratch.by_slot.iter().all(Vec::is_empty));
        let chunk = build(&[0, 1, 2, 3], 0, 4, picks);
        assert_eq!(chunk.picked(2), &[0, 3]);
        assert_eq!(chunk.picked(5), &[0, 2, 3]);
        assert_eq!(chunk.picked(0), &[] as &[u32]);
        assert_eq!(chunk.picked(7), &[] as &[u32]);
        // A second chunk starts from nothing.
        scratch.add(1, 1);
        assert_eq!(scratch.take().slots.as_ref(), &[1]);
    }

    fn ring_chunk(from: u64, to: u64, bytes: usize) -> LiveChunk {
        let mut chunk = LiveChunk::empty(from, to, true);
        if bytes > 0 {
            chunk.count = 1;
            chunk.body = Bytes::from(vec![0; bytes]);
        }
        chunk
    }

    #[test]
    fn the_ring_keeps_recent_chunks_within_its_bounds() {
        let mut ring = Ring::default();
        for from in 0..(RING_BATCHES as u64 + 10) {
            ring.push(ring_chunk(from, from + 1, 8));
        }
        assert_eq!(ring.chunks.len(), RING_BATCHES);
        assert_eq!(ring.chunks.front().unwrap().from, 10);

        // Bytes bound the ring too, but the newest chunk always stays.
        let mut ring = Ring::default();
        ring.push(ring_chunk(0, 1, RING_BYTES));
        ring.push(ring_chunk(1, 2, RING_BYTES));
        assert_eq!(
            ring.chunks.iter().map(|c| c.from).collect::<Vec<_>>(),
            vec![1]
        );
        assert_eq!(ring.bytes, RING_BYTES);
    }

    #[test]
    fn the_ring_finds_the_chunk_covering_a_position() {
        let mut ring = Ring::default();
        // 0..10, 10..14, a gap, 20..30
        ring.push(ring_chunk(0, 10, 8));
        ring.push(ring_chunk(10, 14, 8));
        ring.push(ring_chunk(20, 30, 8));
        let covering = |p: u64| ring.covering(p).map(|c| (c.from, c.to));
        assert_eq!(covering(0), Some((0, 10)));
        assert_eq!(covering(9), Some((0, 10)));
        assert_eq!(covering(10), Some((10, 14)));
        assert_eq!(covering(13), Some((10, 14)));
        assert_eq!(covering(14), None, "in the gap");
        assert_eq!(covering(19), None, "in the gap");
        assert_eq!(covering(20), Some((20, 30)));
        assert_eq!(covering(29), Some((20, 30)));
        assert_eq!(covering(30), None, "not dispatched yet");
    }

    #[test]
    fn quiet_stretches_fold_into_one_empty_chunk() {
        let mut ring = Ring::default();
        assert!(ring.push(ring_chunk(0, 5, 8)).is_some());
        for from in 5..105 {
            assert!(ring.push(ring_chunk(from, from + 1, 0)).is_none() || from == 5);
        }
        assert_eq!(ring.chunks.len(), 2, "one full chunk, one empty run");
        assert_eq!(
            ring.covering(77).map(|c| (c.from, c.to, c.count)),
            Some((5, 105, 0))
        );
        // A chunk with events is never folded in.
        assert!(ring.push(ring_chunk(105, 106, 8)).is_some());
        assert_eq!(ring.chunks.len(), 3);
        // Nor is an empty one that is not complete into a complete one.
        let mut partial = LiveChunk::empty(106, 107, false);
        partial.count = 0;
        assert!(ring.push(partial).is_some());
        assert_eq!(ring.chunks.len(), 4);
    }

    #[test]
    fn only_plain_match_everything_subscriptions_want_shared_batches() {
        let mut m = Matcher::new();
        assert!(!m.wants_shared_tail());
        m.insert(&cond(vec![crit(&["E"], &[("k", "v")])]), &[], "filtered");
        m.insert(
            &cond(vec![crit(&[], &[])]),
            &["Noise".to_string()],
            "excluding",
        );
        assert!(!m.wants_shared_tail());
        let all = m.insert(&cond(vec![crit(&[], &[])]), &[], "all");
        assert!(m.wants_shared_tail());
        m.remove(all);
        assert!(!m.wants_shared_tail());
    }
}
