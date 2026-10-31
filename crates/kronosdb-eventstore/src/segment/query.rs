//! Answering a sourcing condition from a source of keys.
//!
//! Every event is indexed under one key per tag and one for its type (the
//! "forward keys"); each key maps to the positions holding it. A source of
//! keys covers a range of positions: the active segment's in-memory index, a
//! sealed segment's `.keys`, or a merged table over many segments (ADR-0010).
//! A read covers its range with disjoint sources and asks each the same
//! questions, answered here once over [`KeySource`].
//!
//! A query against a source is resolved first ([`Resolved::new`]): one
//! lookup per key, leaving each criterion's keys in hand. The resolved query
//! then answers windowed questions (the first match at or after a position,
//! the matches in a range, the last match before a position) touching only
//! the parts of each key's positions they need, so a source spanning
//! billions of positions costs what the answer costs.

use roaring::RoaringBitmap;

use crate::criteria::SourcingCondition;
use crate::error::Error;
use crate::segment::key_table::key_hash;
use crate::segment::mapped_set::MappedSet;

/// Reserved tag key for event type names.
pub(crate) const EVENT_TYPE_TAG_KEY: &[u8] = b"__kronosdb_event_type__";

/// The forward key of `(key, value)`: the key's length, the key, the value.
pub(crate) fn forward_key(key: &[u8], value: &[u8]) -> Vec<u8> {
    let mut k = Vec::with_capacity(4 + key.len() + value.len());
    write_forward_key(&mut k, key, value);
    k
}

/// Writes the forward key of `(key, value)` into `out`, replacing it.
pub(crate) fn write_forward_key(out: &mut Vec<u8>, key: &[u8], value: &[u8]) {
    out.clear();
    out.extend_from_slice(&(key.len() as u32).to_le_bytes());
    out.extend_from_slice(key);
    out.extend_from_slice(value);
}

/// One key a condition asks about: its forward key and its hash.
pub struct ProbeKey {
    pub(crate) bytes: Vec<u8>,
    pub(crate) hash: u64,
}

impl ProbeKey {
    fn new(key: &[u8], value: &[u8]) -> Self {
        let bytes = forward_key(key, value);
        let hash = key_hash(&bytes);
        Self { bytes, hash }
    }
}

pub(crate) struct ProbeCriterion {
    /// Event types, any one of which satisfies the criterion. Empty: any type.
    pub(crate) names: Vec<ProbeKey>,
    /// Tags, all of which the event must carry.
    pub(crate) tags: Vec<ProbeKey>,
}

impl ProbeCriterion {
    fn takes_everything(&self) -> bool {
        self.names.is_empty() && self.tags.is_empty()
    }
}

/// A sourcing condition prepared for asking sources about: its forward keys
/// and their hashes, built once per query instead of once per source.
pub struct Probe {
    pub(crate) criteria: Vec<ProbeCriterion>,
}

impl Probe {
    pub fn new(condition: &SourcingCondition) -> Self {
        Self {
            criteria: condition
                .criteria
                .iter()
                .map(|criterion| ProbeCriterion {
                    names: criterion
                        .names
                        .iter()
                        .map(|name| ProbeKey::new(EVENT_TYPE_TAG_KEY, name.as_bytes()))
                        .collect(),
                    tags: criterion
                        .tags
                        .iter()
                        .map(|tag| ProbeKey::new(&tag.key, &tag.value))
                        .collect(),
                })
                .collect(),
        }
    }

    /// Whether some criterion matches every event, which makes every other
    /// criterion redundant.
    pub fn takes_everything(&self) -> bool {
        self.criteria.iter().any(ProbeCriterion::takes_everything)
    }
}

/// Positions in one source: a set relative to its base.
#[derive(Debug, Clone, PartialEq)]
pub struct PositionSet {
    base: u64,
    bits: RoaringBitmap,
}

impl PositionSet {
    pub(crate) fn new(base: u64, bits: RoaringBitmap) -> Self {
        Self { base, bits }
    }

    /// Every position in `[from, to)` of a source that starts at `base`.
    pub(crate) fn range(base: u64, from: u64, to: u64) -> Self {
        let mut bits = RoaringBitmap::new();
        if from < to {
            bits.insert_range(relative(base, from)..relative(base, to));
        }
        Self { base, bits }
    }

    pub fn is_empty(&self) -> bool {
        self.bits.is_empty()
    }

    pub fn len(&self) -> u64 {
        self.bits.len()
    }

    pub fn contains(&self, position: u64) -> bool {
        position >= self.base
            && position - self.base <= u64::from(u32::MAX)
            && self.bits.contains((position - self.base) as u32)
    }

    pub fn max(&self) -> Option<u64> {
        self.bits.max().map(|p| self.base + u64::from(p))
    }

    /// The positions, ascending.
    pub fn iter(&self) -> impl Iterator<Item = u64> + '_ {
        self.bits.iter().map(move |p| self.base + u64::from(p))
    }

    /// Drops every position below `from`.
    pub fn remove_below(&mut self, from: u64) {
        if from > self.base {
            self.bits.remove_range(..relative(self.base, from));
        }
    }

    /// Drops every position at or past `bound`.
    pub fn remove_from(&mut self, bound: u64) {
        if bound <= self.base {
            self.bits.clear();
        } else if bound - self.base <= u64::from(u32::MAX) {
            self.bits.remove_range(relative(self.base, bound)..);
        }
    }
}

/// `position` relative to `base`, clamped to the 32-bit span a source's
/// sets cover.
fn relative(base: u64, position: u64) -> u32 {
    position.saturating_sub(base).min(u64::from(u32::MAX)) as u32
}

/// The smallest member of `bitmap` at or after `after`, without walking the
/// members below it: `rank` counts them, `select` skips them.
pub fn first_at_or_after(bitmap: &RoaringBitmap, after: u32) -> Option<u32> {
    if after == 0 {
        return bitmap.min();
    }
    let below = bitmap.rank(after - 1);
    bitmap.select(u32::try_from(below).ok()?)
}

/// One key's positions in a source, relative to its base: a set in memory,
/// or one read in place from a mapped table.
pub enum Positions<'a> {
    Memory(&'a RoaringBitmap),
    Mapped(MappedSet<'a>),
}

impl Positions<'_> {
    pub fn len(&self) -> u64 {
        match self {
            Positions::Memory(set) => set.len(),
            Positions::Mapped(set) => set.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The smallest member at or after `value`.
    pub fn first_at_or_after(&self, value: u32) -> Result<Option<u32>, Error> {
        match self {
            Positions::Memory(set) => Ok(first_at_or_after(set, value)),
            Positions::Mapped(set) => set.first_at_or_after(value),
        }
    }

    /// The largest member below `bound` (up to 2^32).
    pub fn last_before(&self, bound: u64) -> Result<Option<u32>, Error> {
        match self {
            Positions::Memory(set) => Ok(if bound > u64::from(u32::MAX) {
                set.max()
            } else {
                set.range(..bound as u32).next_back()
            }),
            Positions::Mapped(set) => set.last_before(bound),
        }
    }

    /// The members in `[from, to)`, `to` up to 2^32.
    pub fn range(&self, from: u32, to: u64) -> Result<RoaringBitmap, Error> {
        match self {
            Positions::Memory(set) => Ok(if u64::from(from) >= to {
                RoaringBitmap::new()
            } else if to > u64::from(u32::MAX) {
                if from == 0 {
                    (*set).clone()
                } else {
                    RoaringBitmap::from_sorted_iter(set.range(from..)).unwrap()
                }
            } else {
                RoaringBitmap::from_sorted_iter(set.range(from..to as u32)).unwrap()
            }),
            Positions::Mapped(set) => set.range(from, to),
        }
    }
}

/// A source of keys over the positions `[base, base + span)`.
pub trait KeySource: Send + Sync {
    /// The first position the source covers.
    fn base(&self) -> u64;
    /// How many positions it covers.
    fn span(&self) -> u64;
    /// The positions holding `key`, or `None` when none does.
    fn positions(&self, key: &ProbeKey) -> Result<Option<Positions<'_>>, Error>;
}

/// Where a segment's events are: its byte offsets, by position. The active
/// segment's in-memory index, or a sealed segment's mapped `.idx`, which
/// checks its bytes as it reads them, so every answer can be
/// `Error::Corrupted`.
pub trait Locator: Send + Sync {
    /// The segment's first position.
    fn base_position(&self) -> u64;
    /// How many events it holds: positions `base..base + count`.
    fn event_count(&self) -> u64;
    /// The positions the segment holds.
    fn position_range(&self) -> std::ops::Range<u64> {
        self.base_position()..self.base_position() + self.event_count()
    }
    /// Byte offset of the record of the event at `position`, if the
    /// segment holds it.
    fn offset_of(&self, position: u64) -> Result<Option<u64>, Error>;
    /// The exact length of that record, header included. `None` when the
    /// segment holds no such event, or it is the last one of a segment
    /// still being written, whose end the index does not know.
    fn record_len(&self, position: u64) -> Result<Option<u32>, Error>;
}

/// A criterion with its keys looked up in one source.
struct ResolvedCriterion<'a> {
    /// Every one must hold the event.
    tags: Vec<Positions<'a>>,
    /// One must hold the event, when `named` (the criterion names types).
    names: Vec<Positions<'a>>,
    named: bool,
}

/// A probe resolved against one source: each criterion that can match
/// there, with its keys' positions in hand. A criterion missing a tag, or
/// naming only types the source lacks, cannot match and is dropped.
pub struct Resolved<'a> {
    base: u64,
    span: u64,
    everything: bool,
    criteria: Vec<ResolvedCriterion<'a>>,
}

impl<'a> Resolved<'a> {
    /// Looks up every key of `probe` in `source`, once.
    pub fn new(source: &'a (impl KeySource + ?Sized), probe: &Probe) -> Result<Self, Error> {
        let mut resolved = Self {
            base: source.base(),
            span: source.span(),
            everything: probe.takes_everything(),
            criteria: Vec::new(),
        };
        if resolved.everything {
            return Ok(resolved);
        }
        'criteria: for criterion in &probe.criteria {
            let mut tags = Vec::with_capacity(criterion.tags.len());
            for tag in &criterion.tags {
                match source.positions(tag)? {
                    Some(positions) => tags.push(positions),
                    None => continue 'criteria,
                }
            }
            let mut names = Vec::new();
            for name in &criterion.names {
                if let Some(positions) = source.positions(name)? {
                    names.push(positions);
                }
            }
            let named = !criterion.names.is_empty();
            if named && names.is_empty() {
                continue;
            }
            // The rarest tag first: it bounds every intersection.
            tags.sort_by_key(Positions::len);
            resolved
                .criteria
                .push(ResolvedCriterion { tags, names, named });
        }
        Ok(resolved)
    }

    /// Whether no criterion can match anywhere in the source.
    pub fn is_dead(&self) -> bool {
        !self.everything && self.criteria.is_empty()
    }

    /// The positions the source covers.
    pub fn range(&self) -> std::ops::Range<u64> {
        self.base..self.base + self.span
    }

    /// `[from, to)` clipped to the source, relative to its base.
    fn clip(&self, from: u64, to: u64) -> Option<(u32, u64)> {
        let from = from.max(self.base);
        let to = to.min(self.base + self.span);
        (from < to).then(|| ((from - self.base) as u32, to - self.base))
    }

    /// The first matching position at or after `after`.
    pub fn first_at_or_after(&self, after: u64) -> Result<Option<u64>, Error> {
        let Some((from, _)) = self.clip(after, u64::MAX) else {
            return Ok(None);
        };
        if self.everything {
            return Ok(Some(self.base + u64::from(from)));
        }
        let mut first: Option<u32> = None;
        for criterion in &self.criteria {
            if let Some(found) = first_in(criterion, from)? {
                first = Some(first.map_or(found, |seen| seen.min(found)));
            }
        }
        Ok(first.map(|p| self.base + u64::from(p)))
    }

    /// The last matching position below `below`.
    pub fn last_before(&self, below: u64) -> Result<Option<u64>, Error> {
        let Some((_, to)) = self.clip(self.base, below) else {
            return Ok(None);
        };
        if self.everything {
            return Ok(Some(self.base + to - 1));
        }
        let mut last: Option<u32> = None;
        for criterion in &self.criteria {
            if let Some(found) = last_in(criterion, to)? {
                last = Some(last.map_or(found, |seen| seen.max(found)));
            }
        }
        Ok(last.map(|p| self.base + u64::from(p)))
    }

    /// Whether any position in `[from, to)` matches.
    pub fn any_in(&self, from: u64, to: u64) -> Result<bool, Error> {
        Ok(self.first_at_or_after(from)?.is_some_and(|p| p < to))
    }

    /// The matching positions in `[from, to)`, or `None` when there are
    /// none. Reads only the parts of each key's positions in the range.
    pub fn matching_in(&self, from: u64, to: u64) -> Result<Option<PositionSet>, Error> {
        let Some((lo, hi)) = self.clip(from, to) else {
            return Ok(None);
        };
        if self.everything {
            return Ok(Some(PositionSet::range(
                self.base,
                self.base + u64::from(lo),
                self.base + hi,
            )));
        }
        let mut result: Option<RoaringBitmap> = None;
        for criterion in &self.criteria {
            let mut parts = criterion.tags.iter();
            let mut set = match parts.next() {
                Some(first) => Some(first.range(lo, hi)?),
                None => None,
            };
            for tag in parts {
                if set.as_ref().is_some_and(RoaringBitmap::is_empty) {
                    break;
                }
                let other = tag.range(lo, hi)?;
                if let Some(set) = &mut set {
                    *set &= other;
                }
            }
            if criterion.named && !set.as_ref().is_some_and(RoaringBitmap::is_empty) {
                let mut names = RoaringBitmap::new();
                for name in &criterion.names {
                    names |= name.range(lo, hi)?;
                }
                set = Some(match set {
                    Some(set) => set & names,
                    None => names,
                });
            }
            if let Some(set) = set.filter(|set| !set.is_empty()) {
                match &mut result {
                    Some(result) => *result |= set,
                    None => result = Some(set),
                }
            }
        }
        Ok(result.map(|bits| PositionSet::new(self.base, bits)))
    }
}

/// The first position at or after `after` that satisfies `criterion`: every
/// tag, and one of its event types if it names any. A leapfrog over the
/// members' positions, each probed in place, so the cost follows the
/// matches at or after `after` (usually none), not the size of the sets.
fn first_in(criterion: &ResolvedCriterion<'_>, after: u32) -> Result<Option<u32>, Error> {
    let mut cursor = after;
    loop {
        let mut highest = cursor;
        let mut agreed = true;
        let mut first_seen: Option<u32> = None;
        let mut see = |found: u32| {
            if first_seen.is_some_and(|seen| seen != found) {
                agreed = false;
            }
            first_seen = Some(found);
            highest = highest.max(found);
        };
        for tag in &criterion.tags {
            match tag.first_at_or_after(cursor)? {
                Some(found) => see(found),
                None => return Ok(None),
            }
        }
        if criterion.named {
            let mut lowest: Option<u32> = None;
            for name in &criterion.names {
                if let Some(found) = name.first_at_or_after(cursor)? {
                    lowest = Some(lowest.map_or(found, |seen| seen.min(found)));
                }
            }
            match lowest {
                Some(found) => see(found),
                None => return Ok(None),
            }
        }
        if agreed {
            return Ok(first_seen);
        }
        cursor = highest;
    }
}

/// The last position below `bound` that satisfies `criterion`: the mirror
/// of [`first_in`].
fn last_in(criterion: &ResolvedCriterion<'_>, bound: u64) -> Result<Option<u32>, Error> {
    let mut cursor = bound;
    loop {
        let mut lowest = u32::MAX;
        let mut agreed = true;
        let mut first_seen: Option<u32> = None;
        let mut see = |found: u32| {
            if first_seen.is_some_and(|seen| seen != found) {
                agreed = false;
            }
            first_seen = Some(found);
            lowest = lowest.min(found);
        };
        for tag in &criterion.tags {
            match tag.last_before(cursor)? {
                Some(found) => see(found),
                None => return Ok(None),
            }
        }
        if criterion.named {
            let mut highest: Option<u32> = None;
            for name in &criterion.names {
                if let Some(found) = name.last_before(cursor)? {
                    highest = Some(highest.map_or(found, |seen| seen.max(found)));
                }
            }
            match highest {
                Some(found) => see(found),
                None => return Ok(None),
            }
        }
        if agreed {
            return Ok(first_seen);
        }
        cursor = u64::from(lowest) + 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::criteria::Criterion;
    use crate::event::Tag;
    use std::collections::HashMap;

    /// An in-memory source for exercising the query logic alone.
    struct Memory {
        base: u64,
        span: u64,
        keys: HashMap<Vec<u8>, RoaringBitmap>,
    }

    impl KeySource for Memory {
        fn base(&self) -> u64 {
            self.base
        }
        fn span(&self) -> u64 {
            self.span
        }
        fn positions(&self, key: &ProbeKey) -> Result<Option<Positions<'_>>, Error> {
            Ok(self.keys.get(&key.bytes).map(Positions::Memory))
        }
    }

    struct Rng(u64);
    impl Rng {
        fn below(&mut self, n: u64) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0 % n
        }
    }

    /// Events with a type and some tags, as (type, tags) by relative position.
    fn events(rng: &mut Rng, count: u64) -> Vec<(String, Vec<Tag>)> {
        (0..count)
            .map(|_| {
                let name = ["A", "B", "C"][rng.below(3) as usize].to_string();
                let mut tags = vec![Tag::from_str("k", &rng.below(7).to_string())];
                if rng.below(2) == 0 {
                    tags.push(Tag::from_str("j", &rng.below(3).to_string()));
                }
                (name, tags)
            })
            .collect()
    }

    fn source(base: u64, events: &[(String, Vec<Tag>)]) -> Memory {
        let mut keys: HashMap<Vec<u8>, RoaringBitmap> = HashMap::new();
        for (i, (name, tags)) in events.iter().enumerate() {
            keys.entry(forward_key(EVENT_TYPE_TAG_KEY, name.as_bytes()))
                .or_default()
                .insert(i as u32);
            for tag in tags {
                keys.entry(forward_key(&tag.key, &tag.value))
                    .or_default()
                    .insert(i as u32);
            }
        }
        Memory {
            base,
            span: events.len() as u64,
            keys,
        }
    }

    fn matches(criterion: &Criterion, (name, tags): &(String, Vec<Tag>)) -> bool {
        (criterion.names.is_empty() || criterion.names.contains(name))
            && criterion.tags.iter().all(|t| tags.contains(t))
    }

    fn random_condition(rng: &mut Rng) -> SourcingCondition {
        let criteria = (0..rng.below(3))
            .map(|_| Criterion {
                names: ["A", "B", "Z"]
                    .iter()
                    .filter(|_| rng.below(3) == 0)
                    .map(|n| n.to_string())
                    .collect(),
                tags: (0..rng.below(3))
                    .map(|i| {
                        let key = if i == 0 { "k" } else { "j" };
                        Tag::from_str(key, &rng.below(8).to_string())
                    })
                    .collect(),
            })
            .collect();
        SourcingCondition { criteria }
    }

    /// Every windowed answer equals the one read off the events directly.
    #[test]
    fn resolved_answers_agree_with_the_events() {
        let mut rng = Rng(0x5EED);
        const BASE: u64 = 1_000;
        for count in [0u64, 1, 50, 3_000] {
            let events = events(&mut rng, count);
            let source = source(BASE, &events);
            for _ in 0..200 {
                let condition = random_condition(&mut rng);
                let probe = Probe::new(&condition);
                let resolved = Resolved::new(&source, &probe).unwrap();
                let truth: Vec<u64> = events
                    .iter()
                    .enumerate()
                    .filter(|(_, e)| condition.criteria.iter().any(|c| matches(c, e)))
                    .map(|(i, _)| BASE + i as u64)
                    .collect();
                if resolved.is_dead() {
                    assert!(truth.is_empty(), "{condition:?} ruled out but matches");
                }
                for _ in 0..10 {
                    let from = BASE + rng.below(count + 20) - 10;
                    let to = from + rng.below(count / 2 + 2);
                    let expected: Vec<u64> = truth
                        .iter()
                        .copied()
                        .filter(|p| (from..to).contains(p))
                        .collect();
                    let got: Vec<u64> = resolved
                        .matching_in(from, to)
                        .unwrap()
                        .map(|set| set.iter().collect())
                        .unwrap_or_default();
                    assert_eq!(got, expected, "{condition:?} in {from}..{to}");
                    assert_eq!(resolved.any_in(from, to).unwrap(), !expected.is_empty());
                    assert_eq!(
                        resolved.first_at_or_after(from).unwrap(),
                        truth.iter().copied().find(|&p| p >= from)
                    );
                    assert_eq!(
                        resolved.last_before(to).unwrap(),
                        truth.iter().copied().rev().find(|&p| p < to)
                    );
                }
            }
        }
    }
}
