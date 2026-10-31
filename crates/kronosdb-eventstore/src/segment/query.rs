//! Answering a sourcing condition from one segment's keys.
//!
//! A segment indexes each event under one key per tag and one for its type
//! (the "forward keys"); each key maps to the set of positions holding it,
//! relative to the segment's base. The active segment keeps those sets in a
//! hash map, a sealed one in its mapped `.idx`; the questions asked of them
//! are the same and are answered here, once, over [`KeyBitmaps`].

use std::borrow::Cow;

use roaring::RoaringBitmap;

use crate::criteria::SourcingCondition;
use crate::error::Error;
use crate::event::Position;
use crate::segment::filter::key_hash;

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

/// One key a condition asks about: its forward key and its filter hash.
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

/// A sourcing condition prepared for asking segments about: its forward keys
/// and their hashes, built once per query instead of once per segment.
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

    /// Whether any criterion might match in a segment whose keys pass
    /// `might_hold`: each of its tags, and one of its types if it names any.
    /// No criteria match nothing; an empty criterion matches everything.
    pub(crate) fn might_match(&self, mut might_hold: impl FnMut(&ProbeKey) -> bool) -> bool {
        self.criteria.iter().any(|criterion| {
            (criterion.names.is_empty() || criterion.names.iter().any(&mut might_hold))
                && criterion.tags.iter().all(&mut might_hold)
        })
    }
}

/// Positions in one segment: a set relative to its base.
#[derive(Debug, Clone, PartialEq)]
pub struct PositionSet {
    base: u64,
    bits: RoaringBitmap,
}

impl PositionSet {
    pub(crate) fn new(base: u64, bits: RoaringBitmap) -> Self {
        Self { base, bits }
    }

    /// Every position in `[from, to)` of a segment that starts at `base`.
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

/// `position` relative to `base`, clamped to the 32-bit span a segment's
/// sets cover.
fn relative(base: u64, position: u64) -> u32 {
    position.saturating_sub(base).min(u64::from(u32::MAX)) as u32
}

/// What a read asks of one segment's index, whether it is the active
/// segment's in memory or a sealed segment's mapped `.idx`. A sealed index
/// checks its bytes as it reads them, so every answer can be
/// `Error::Corrupted`.
pub trait SegmentKeys: Send + Sync {
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
    /// Whether the segment might hold a match: `false` is certain, `true`
    /// is not (a sealed segment answers from its key filter).
    fn might_match(&self, probe: &Probe) -> Result<bool, Error>;
    /// The positions matching `probe`, or `None` when there are none.
    fn matching(&self, probe: &Probe) -> Result<Option<PositionSet>, Error>;
    /// Whether any event in `[from, to)` could match, from ranks alone:
    /// never a false negative, a `true` still needs `matching`.
    fn any_match_in(&self, probe: &Probe, from: u64, to: u64) -> Result<bool, Error>;
    /// The first matching position at or after `after` (the DCB check).
    fn first_match_after(&self, probe: &Probe, after: u64) -> Result<Option<Position>, Error>;
}

/// One segment's keys and the positions holding each.
pub(crate) trait KeyBitmaps {
    /// The segment's first position.
    fn base(&self) -> u64;
    /// How many events it holds: its positions are `base..base + count`.
    fn count(&self) -> u64;
    /// The positions holding `key`, relative to the base. `None` when no
    /// event in the segment carries it.
    fn bitmap(&self, key: &ProbeKey) -> Result<Option<Cow<'_, RoaringBitmap>>, Error>;
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

/// The positions matching `probe`, or `None` when there are none.
pub(crate) fn matching(
    keys: &impl KeyBitmaps,
    probe: &Probe,
) -> Result<Option<PositionSet>, Error> {
    if probe.takes_everything() {
        return Ok((keys.count() > 0)
            .then(|| PositionSet::range(keys.base(), keys.base(), keys.base() + keys.count())));
    }
    let mut result: Option<RoaringBitmap> = None;
    for criterion in &probe.criteria {
        if let Some(bitmap) = resolve_criterion(keys, criterion)? {
            match &mut result {
                Some(existing) => *existing |= &bitmap,
                None => result = Some(bitmap),
            }
        }
    }
    Ok(result.map(|bits| PositionSet::new(keys.base(), bits)))
}

fn resolve_criterion(
    keys: &impl KeyBitmaps,
    criterion: &ProbeCriterion,
) -> Result<Option<RoaringBitmap>, Error> {
    let mut parts: Vec<Cow<'_, RoaringBitmap>> = Vec::with_capacity(criterion.tags.len() + 1);
    for tag in &criterion.tags {
        match keys.bitmap(tag)? {
            Some(bitmap) => parts.push(bitmap),
            None => return Ok(None),
        }
    }
    if !criterion.names.is_empty() {
        let mut names: Option<RoaringBitmap> = None;
        for name in &criterion.names {
            if let Some(bitmap) = keys.bitmap(name)? {
                match &mut names {
                    Some(existing) => *existing |= bitmap.as_ref(),
                    None => names = Some(bitmap.into_owned()),
                }
            }
        }
        match names {
            Some(names) => parts.push(Cow::Owned(names)),
            None => return Ok(None),
        }
    }
    // Start from the smallest set: the copy is of the most selective part,
    // and every AND after it can only shrink it.
    parts.sort_by_key(|bitmap| bitmap.len());
    let mut parts = parts.into_iter();
    let Some(first) = parts.next() else {
        // An empty criterion: `matching` answered it as a range.
        return Ok(None);
    };
    let mut result = first.into_owned();
    for part in parts {
        result &= part.as_ref();
    }
    Ok((!result.is_empty()).then_some(result))
}

/// Whether any event in `[from, to)` could match `probe`, decided from
/// ranks alone: no bitmap is combined. A criterion is possible when each of
/// its tags has a position in the range and, if it names event types, at
/// least one of them does. Never a false negative; a true answer still needs
/// `matching` to confirm.
pub(crate) fn any_match_in(
    keys: &impl KeyBitmaps,
    probe: &Probe,
    from: u64,
    to: u64,
) -> Result<bool, Error> {
    let (base, end) = (keys.base(), keys.base() + keys.count());
    let (from, to) = (from.max(base), to.min(end));
    if from >= to {
        return Ok(false);
    }
    let (from, to) = (relative(base, from), relative(base, to));
    let in_range = |bitmap: &RoaringBitmap| first_at_or_after(bitmap, from).is_some_and(|p| p < to);
    for criterion in &probe.criteria {
        if criterion.takes_everything() {
            return Ok(true);
        }
        let mut possible = true;
        for tag in &criterion.tags {
            if !keys.bitmap(tag)?.is_some_and(|bitmap| in_range(&bitmap)) {
                possible = false;
                break;
            }
        }
        if !possible {
            continue;
        }
        if criterion.names.is_empty() {
            return Ok(true);
        }
        for name in &criterion.names {
            if keys.bitmap(name)?.is_some_and(|bitmap| in_range(&bitmap)) {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

/// The first position at or after `after` matching `probe`: what
/// `matching(..)` then "first member at or after" would give, without
/// building the matching set. `after` is the next-exclusive marker a DCB
/// check validated against.
pub(crate) fn first_match_after(
    keys: &impl KeyBitmaps,
    probe: &Probe,
    after: u64,
) -> Result<Option<Position>, Error> {
    let (base, end) = (keys.base(), keys.base() + keys.count());
    if after >= end {
        return Ok(None);
    }
    let after = relative(base, after);
    let mut first: Option<u32> = None;
    for criterion in &probe.criteria {
        if let Some(found) = first_match_in(keys, criterion, after)? {
            first = Some(first.map_or(found, |seen| seen.min(found)));
        }
    }
    Ok(first.map(|p| Position(base + u64::from(p))))
}

/// The first position at or after `after` that satisfies `criterion`: every
/// tag, and one of its event types if it names any. A leapfrog over the
/// member bitmaps, each probed by rank and select, so the cost follows the
/// matches at or after `after` (usually none), not the size of the bitmaps.
fn first_match_in(
    keys: &impl KeyBitmaps,
    criterion: &ProbeCriterion,
    after: u32,
) -> Result<Option<u32>, Error> {
    if criterion.takes_everything() {
        return Ok((u64::from(after) < keys.count()).then_some(after));
    }
    let mut tags = Vec::with_capacity(criterion.tags.len());
    for tag in &criterion.tags {
        match keys.bitmap(tag)? {
            Some(bitmap) => tags.push(bitmap),
            None => return Ok(None),
        }
    }
    let mut types = Vec::with_capacity(criterion.names.len());
    for name in &criterion.names {
        if let Some(bitmap) = keys.bitmap(name)? {
            types.push(bitmap);
        }
    }
    if !criterion.names.is_empty() && types.is_empty() {
        return Ok(None);
    }

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
        for bitmap in &tags {
            match first_at_or_after(bitmap, cursor) {
                Some(found) => see(found),
                None => return Ok(None),
            }
        }
        if !types.is_empty() {
            match types
                .iter()
                .filter_map(|bitmap| first_at_or_after(bitmap, cursor))
                .min()
            {
                Some(found) => see(found),
                None => return Ok(None),
            }
        }
        if agreed {
            return Ok(Some(highest));
        }
        cursor = highest;
    }
}
