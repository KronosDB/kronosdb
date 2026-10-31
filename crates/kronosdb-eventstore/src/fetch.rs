//! Records fetched by byte range: the read path for sealed segments that are
//! not mapped.
//!
//! A read is planned from the index alone (ADR-0010): for each matching
//! position, the segment's locator gives the record's byte offset and exact
//! length. Mapping a segment to read a few of those records costs an open, a
//! map and fresh page faults, and past the mapping budget it also unmaps
//! another segment that the next read maps again. So a segment the budget
//! has no room for is read the way the plan describes it: the records a
//! chunk of the read needs, with neighbours closer than `COALESCE_GAP`
//! fetched together, one `pread` per run.
//!
//! Every record is CRC-checked as it is served, and its length checked
//! against the locator's, as the mapped path checks it. A file shorter than
//! its locator says is `Error::Corrupted`.

use std::fs::File;
use std::sync::Arc;

use crate::error::Error;
use crate::segment::blocks::read_exact_at;
use crate::segment::query::Locator;
use crate::segment::reader::{StoredRecord, record_in};

/// Records of one read closer than this are fetched in one run: below it, a
/// second `pread` costs more than copying the bytes between them.
pub const COALESCE_GAP: u64 = 16 * 1024;

/// The most one run fetches. A dense read of a segment becomes runs of this
/// size rather than one buffer as large as the segment.
pub const MAX_RUN: u64 = 1024 * 1024;

/// The records of one sealed segment that a read asked for, read from its
/// file.
pub struct FetchedRecords {
    file: Arc<File>,
    locator: Arc<dyn Locator>,
    /// What the last `load` fetched: disjoint byte ranges of the segment in
    /// ascending order, as (start offset, bytes).
    runs: Vec<(u64, Vec<u8>)>,
    /// A record asked for outside every run, read on its own.
    single: Vec<u8>,
}

impl FetchedRecords {
    /// A reader over the segment `locator` describes, whose file is `file`.
    pub fn new(file: Arc<File>, locator: Arc<dyn Locator>) -> Self {
        Self {
            file,
            locator,
            runs: Vec::new(),
            single: Vec::new(),
        }
    }

    /// Fetches the records at `records` (position, byte offset), in
    /// ascending order, replacing whatever the last load fetched.
    pub fn load(&mut self, records: &[(u64, u64)]) -> Result<(), Error> {
        self.runs.clear();
        let mut ranges: Vec<(u64, u64)> = Vec::new();
        for &(position, offset) in records {
            let end = offset + u64::from(self.len_of(position)?);
            match ranges.last_mut() {
                Some((start, run_end))
                    if offset >= *start
                        && offset <= *run_end + COALESCE_GAP
                        && end - *start <= MAX_RUN =>
                {
                    *run_end = (*run_end).max(end);
                }
                _ => ranges.push((offset, end)),
            }
        }
        for (start, end) in ranges {
            let bytes = self.read(start, end - start)?;
            self.runs.push((start, bytes));
        }
        Ok(())
    }

    /// The record of the event at `position`, which starts at `offset`:
    /// from the runs the last `load` fetched, or read on its own.
    pub fn record_at(&mut self, position: u64, offset: u64) -> Result<StoredRecord<'_>, Error> {
        let len = u64::from(self.len_of(position)?);
        let at = self.runs.partition_point(|(start, _)| *start <= offset);
        let in_run = at.checked_sub(1).filter(|&run| {
            let (start, bytes) = &self.runs[run];
            offset + len <= start + bytes.len() as u64
        });
        let record = match in_run {
            Some(run) => {
                let (start, bytes) = &self.runs[run];
                let from = (offset - start) as usize;
                &bytes[from..from + len as usize]
            }
            None => {
                self.single = self.read(offset, len)?;
                &self.single[..]
            }
        };
        let stored = record_in(record, 0)?;
        // `framed` is the record without its 4-byte CRC.
        if stored.framed.len() as u64 + 4 != len {
            return Err(Error::Corrupted {
                message: format!(
                    "record at offset {offset} of segment {} is {} bytes, its index says {len}",
                    self.locator.base_position(),
                    stored.framed.len() + 4
                ),
            });
        }
        Ok(stored)
    }

    fn len_of(&self, position: u64) -> Result<u32, Error> {
        self.locator
            .record_len(position)?
            .ok_or_else(|| Error::Internal {
                message: format!(
                    "the index of sealed segment {} has no record length for position {position}",
                    self.locator.base_position()
                ),
            })
    }

    fn read(&self, offset: u64, len: u64) -> Result<Vec<u8>, Error> {
        read_exact_at(&self.file, offset, len as usize).map_err(|error| match error.kind() {
            std::io::ErrorKind::UnexpectedEof => Error::Corrupted {
                message: format!(
                    "segment {} ends before bytes {offset}..{} its index places records in",
                    self.locator.base_position(),
                    offset + len
                ),
            },
            _ => error.into(),
        })
    }
}
