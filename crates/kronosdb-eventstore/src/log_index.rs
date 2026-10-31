//! The log-wide index on disk (ADR-0010): opening it, and the background
//! merges that keep it to a few large tables.
//!
//! A merge builds one table and switches it in:
//!
//! 1. the table is written under a temporary name, synced, renamed;
//! 2. the manifest naming the new set of tables is written the same way and
//!    the directory synced: the commit point;
//! 3. the in-memory layout swaps the table in and the tables and segment
//!    `.keys` it covers out (readers holding them keep their mappings);
//! 4. the superseded files are deleted.
//!
//! Opening finishes or undoes whatever step a crash interrupted: the
//! manifest says which tables are live, anything else in `index/` goes, and
//! so do the `.keys` of segments a live table covers.

use std::collections::BTreeMap;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use parking_lot::RwLock;

use crate::error::Error;
use crate::segment::key_table::KeyTableFile;
use crate::segment::merge::{self, INDEX_DIR, ManifestEntry, MergeCost, Throttle};
use crate::segment::sealed_index::SealedIndex;
use crate::segment::segment_index::SegmentIndex;
use crate::segment::writer::sync_dir;
use crate::segment::{self};
use crate::store::{SegmentKeys, SegmentList};

/// What opening found: the live merged tables, the own key tables of the
/// sealed segments they do not cover, and every sealed segment's locator.
pub(crate) struct OpenedIndex {
    pub tables: Vec<Arc<KeyTableFile>>,
    pub keys: BTreeMap<u64, SegmentKeys>,
    pub locators: Vec<(u64, Arc<SealedIndex>)>,
    /// Index files rebuilt from their segments (missing or damaged ones).
    pub rebuilt: usize,
}

fn remove_if_exists(path: &Path) -> Result<(), Error> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

/// Whether `error` means "rebuild this file from its segment".
fn rebuildable(error: Error, path: &Path) -> Result<(), Error> {
    match &error {
        Error::Io(io) if io.kind() == io::ErrorKind::NotFound => {
            tracing::info!(path = %path.display(), "sealed segment index file is missing; rebuilding it");
            Ok(())
        }
        Error::Corrupted { message } => {
            tracing::warn!(
                path = %path.display(),
                reason = %message,
                "sealed segment index file is unusable; rebuilding it from the segment"
            );
            Ok(())
        }
        _ => Err(error),
    }
}

/// Opens the index of the log in `dir`, whose segments start at `bases`
/// (the last one, `active_base`, being written). Reads headers and small
/// sections only; a file is rebuilt from its segment only when it is
/// missing or damaged.
pub(crate) fn open(dir: &Path, bases: &[u64], active_base: u64) -> Result<OpenedIndex, Error> {
    let index_dir = dir.join(INDEX_DIR);
    std::fs::create_dir_all(&index_dir)?;

    let sealed: Vec<u64> = bases.iter().copied().filter(|&b| b < active_base).collect();
    let is_bound =
        |position: u64| sealed.binary_search(&position).is_ok() || position == active_base;

    let (listed, mut dirty) = match merge::read_manifest(dir) {
        Ok(Some(listed)) => (listed, false),
        // A store from before merged tables existed, or one whose first
        // manifest a crash prevented: it gets one now.
        Ok(None) => (Vec::new(), true),
        Err(Error::Corrupted { message }) => {
            tracing::warn!(reason = %message, "index manifest is unusable; merged tables will be rebuilt");
            (Vec::new(), true)
        }
        Err(error) => return Err(error),
    };
    let mut tables = Vec::new();
    for entry in listed {
        let path = merge::table_path(dir, entry.base, entry.end);
        let opened = KeyTableFile::open(&path).and_then(|table| {
            let aligned = is_bound(entry.base) && is_bound(entry.end) && entry.end <= active_base;
            if ManifestEntry::of(&table) == entry && aligned {
                Ok(table)
            } else {
                Err(Error::Corrupted {
                    message: "does not match the manifest or the segments".into(),
                })
            }
        });
        match opened {
            Ok(table) => tables.push(Arc::new(table)),
            Err(error) => {
                tracing::warn!(
                    path = %path.display(),
                    %error,
                    "merged index table is unusable; the segments it covered are indexed on their own until it is rebuilt"
                );
                dirty = true;
            }
        }
    }
    if dirty {
        let entries: Vec<ManifestEntry> = tables.iter().map(|t| ManifestEntry::of(t)).collect();
        merge::write_manifest(dir, &entries)?;
    }
    // Anything the manifest does not name is left from a merge a crash
    // interrupted, or superseded by one that finished.
    let live: std::collections::HashSet<String> = tables
        .iter()
        .map(|t| merge::table_file_name(t.base(), t.end()))
        .chain(["MANIFEST".to_string()])
        .collect();
    for entry in std::fs::read_dir(&index_dir)? {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().into_owned();
        if !live.contains(&name) {
            remove_if_exists(&entry.path())?;
        }
    }

    let covered = |base: u64| {
        let at = tables.partition_point(|t| t.base() <= base);
        at > 0 && base < tables[at - 1].end()
    };
    let mut keys = BTreeMap::new();
    let mut locators = Vec::new();
    let mut rebuilt = 0;
    for &base in &sealed {
        let seg_path = segment::segment_path(dir, base);
        // Leftovers of interrupted writes.
        remove_if_exists(&seg_path.with_extension("idx.tmp"))?;
        remove_if_exists(&seg_path.with_extension("keys.tmp"))?;
        let mut built: Option<SegmentIndex> = None;
        let build = |built: &mut Option<SegmentIndex>| -> Result<(), Error> {
            if built.is_none() {
                *built = Some(SegmentIndex::build_from_segment(&seg_path)?);
            }
            Ok(())
        };

        let idx_path = seg_path.with_extension("idx");
        let locator = match SealedIndex::open(&idx_path, base) {
            Ok(locator) => locator,
            Err(error) => {
                rebuildable(error, &idx_path)?;
                build(&mut built)?;
                built.as_ref().unwrap().write_locator(&seg_path)?;
                rebuilt += 1;
                SealedIndex::open(&idx_path, base)?
            }
        };
        locators.push((base, Arc::new(locator)));

        let keys_path = seg_path.with_extension("keys");
        if covered(base) {
            remove_if_exists(&keys_path)?;
            continue;
        }
        let own = KeyTableFile::open(&keys_path).and_then(|table| {
            if table.base() == base && table.span() == locator_count(&locators) {
                Ok(table)
            } else {
                Err(Error::Corrupted {
                    message: "is not this segment's key table".into(),
                })
            }
        });
        let table = match own {
            Ok(table) => table,
            Err(error) => {
                rebuildable(error, &keys_path)?;
                build(&mut built)?;
                built.as_ref().unwrap().write_keys(&seg_path)?;
                rebuilt += 1;
                KeyTableFile::open(&keys_path)?
            }
        };
        keys.insert(base, SegmentKeys::Mapped(Arc::new(table)));
    }
    Ok(OpenedIndex {
        tables,
        keys,
        locators,
        rebuilt,
    })
}

/// Events of the segment whose locator was opened last.
fn locator_count(locators: &[(u64, Arc<SealedIndex>)]) -> u64 {
    use crate::segment::query::Locator;
    locators
        .last()
        .map_or(0, |(_, locator)| locator.event_count())
}

/// Where a merge may be stopped, to test that a crash there leaves a
/// consistent index.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MergeStep {
    /// The new table is in place; the manifest does not name it yet.
    TableWritten,
    /// The manifest names it; nothing in memory or superseded has changed.
    ManifestWritten,
    /// Readers use it; the superseded files are still there.
    Swapped,
    /// Half of the superseded files are gone.
    PartlyCleaned,
}

/// What a context's merges have cost so far.
#[derive(Debug, Clone, Copy, Default)]
pub struct MergeTotals {
    pub tables_built: u64,
    pub bytes_read: u64,
    pub bytes_written: u64,
    pub elapsed: Duration,
}

impl MergeTotals {
    fn add(&mut self, cost: &MergeCost) {
        self.tables_built += 1;
        self.bytes_read += cost.read;
        self.bytes_written += cost.written;
        self.elapsed += cost.elapsed;
    }
}

/// What a merge works on, shared with the engine.
pub(crate) struct MergeContext {
    pub dir: PathBuf,
    pub segments: Arc<RwLock<SegmentList>>,
    pub watermark: Arc<AtomicU64>,
    /// Bytes per second a merge may write; 0 is unlimited.
    pub rate: u64,
    stop: AtomicBool,
    /// One merge at a time, background or asked for.
    running: Mutex<()>,
    /// What every finished merge cost, apart from the lock above so asking
    /// never waits for a merge.
    totals: Mutex<MergeTotals>,
    wake: Mutex<bool>,
    woken: Condvar,
}

impl MergeContext {
    pub(crate) fn new(
        dir: PathBuf,
        segments: Arc<RwLock<SegmentList>>,
        watermark: Arc<AtomicU64>,
        rate: u64,
    ) -> Arc<Self> {
        Arc::new(Self {
            dir,
            segments,
            watermark,
            rate,
            stop: AtomicBool::new(false),
            running: Mutex::new(()),
            totals: Mutex::new(MergeTotals::default()),
            wake: Mutex::new(false),
            woken: Condvar::new(),
        })
    }

    /// Tells the background merger there may be work (a seal completed).
    pub(crate) fn poke(&self) {
        *self.wake.lock().unwrap() = true;
        self.woken.notify_one();
    }

    /// Stops the background merger at its next key and wakes it.
    pub(crate) fn stop(&self) {
        self.stop.store(true, Ordering::Release);
        self.poke();
    }

    pub(crate) fn totals(&self) -> MergeTotals {
        *self.totals.lock().unwrap()
    }

    /// Builds and switches in the next table the final segments should
    /// have. `Ok(None)` when there is nothing to do (or the merger is
    /// stopping). `at` is told each step and may stop the merge there,
    /// which is what a crash would leave.
    pub(crate) fn step(
        &self,
        mut at: impl FnMut(MergeStep) -> bool,
    ) -> Result<Option<MergeCost>, Error> {
        let _running = self.running.lock().unwrap();
        let list = self.segments.read().clone();
        let watermark = self.watermark.load(Ordering::Acquire);
        // Final segments: sealed, own keys on disk (or already merged), and
        // ending strictly below the watermark (see ADR-0010 for why not
        // "at or below").
        let mut finals = 0;
        while finals < list.sealed_count {
            let (base, end) = (list.bases[finals], list.bases[finals + 1]);
            let indexed = list.table_at(base).is_some()
                || matches!(list.keys.get(&base), Some(SegmentKeys::Mapped(_)));
            if end >= watermark || !indexed {
                break;
            }
            finals += 1;
        }
        if finals == 0 {
            return Ok(None);
        }
        let job = merge::next_job(&list.bases[..=finals], &list.tables, |base| {
            match list.keys.get(&base) {
                Some(SegmentKeys::Mapped(table)) => Some(Arc::clone(table)),
                _ => None,
            }
        });
        let Some(job) = job else {
            return Ok(None);
        };
        let path = merge::table_path(&self.dir, job.base, job.end);
        let throttle = Throttle::new(self.rate);
        let built = merge::build(&path, &job, |written| {
            throttle.pace(written);
            !self.stop.load(Ordering::Acquire)
        })?;
        let Some((_, cost)) = built else {
            return Ok(None);
        };
        let index_dir = self.dir.join(INDEX_DIR);
        sync_dir(&index_dir)?;
        let table = Arc::new(KeyTableFile::open(&path)?);
        if !at(MergeStep::TableWritten) {
            return Ok(Some(cost));
        }

        let inside = |base: u64| (job.base..job.end).contains(&base);
        let mut tables: Vec<Arc<KeyTableFile>> = list
            .tables
            .iter()
            .filter(|t| !inside(t.base()))
            .cloned()
            .collect();
        let superseded: Vec<PathBuf> = list
            .tables
            .iter()
            .filter(|t| inside(t.base()))
            .map(|t| merge::table_path(&self.dir, t.base(), t.end()))
            .chain(
                list.keys
                    .keys()
                    .filter(|&&base| inside(base))
                    .map(|&base| segment::segment_path(&self.dir, base).with_extension("keys")),
            )
            .collect();
        tables.push(Arc::clone(&table));
        tables.sort_by_key(|t| t.base());
        let entries: Vec<ManifestEntry> = tables.iter().map(|t| ManifestEntry::of(t)).collect();
        merge::write_manifest(&self.dir, &entries)?;
        if !at(MergeStep::ManifestWritten) {
            return Ok(Some(cost));
        }

        {
            // Applied to the layout as it is now: rotations and truncations
            // since the snapshot touch only what lies past the final
            // segments, never what the table covers.
            let mut layout = self.segments.write();
            let mut now: Vec<Arc<KeyTableFile>> = layout
                .tables
                .iter()
                .filter(|t| !inside(t.base()))
                .cloned()
                .collect();
            now.push(Arc::clone(&table));
            now.sort_by_key(|t| t.base());
            layout.tables = Arc::new(now);
            let mut keys = (*layout.keys).clone();
            keys.retain(|&base, _| !inside(base));
            layout.keys = Arc::new(keys);
        }
        if !at(MergeStep::Swapped) {
            return Ok(Some(cost));
        }

        for (i, path) in superseded.iter().enumerate() {
            if i == superseded.len() / 2 && !at(MergeStep::PartlyCleaned) {
                return Ok(Some(cost));
            }
            remove_if_exists(path)?;
        }
        self.totals.lock().unwrap().add(&cost);
        Ok(Some(cost))
    }

    /// Merges until the final segments have every table they should.
    pub(crate) fn run_to_completion(&self) -> Result<MergeTotals, Error> {
        let mut done = MergeTotals::default();
        while let Some(cost) = self.step(|_| true)? {
            done.add(&cost);
            if self.stop.load(Ordering::Acquire) {
                break;
            }
        }
        Ok(done)
    }
}

/// Starts the background merger of one context: it wakes when a seal
/// completes, and once a second, and builds every table that is due.
pub(crate) fn spawn(context: Arc<MergeContext>) -> std::thread::JoinHandle<()> {
    std::thread::Builder::new()
        .name("kronosdb-merge".into())
        .spawn(move || {
            let mut backoff = Duration::from_secs(1);
            while !context.stop.load(Ordering::Acquire) {
                {
                    let woken = context.wake.lock().unwrap();
                    let (mut woken, _) = context
                        .woken
                        .wait_timeout_while(woken, backoff, |woken| {
                            !*woken && !context.stop.load(Ordering::Acquire)
                        })
                        .unwrap();
                    *woken = false;
                }
                match context.run_to_completion() {
                    Ok(_) => backoff = Duration::from_secs(1),
                    Err(error) => {
                        tracing::warn!(
                            dir = %context.dir.display(),
                            %error,
                            "index merge failed; reads use the tables already in place and the merge is retried"
                        );
                        backoff = (backoff * 2).min(Duration::from_secs(60));
                    }
                }
            }
        })
        .expect("spawn the index merge thread")
}
