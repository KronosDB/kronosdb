//! Feature-gated bench instrumentation (phase 1 baseline).
//!
//! Compiled only when the `bench-instrumentation` Cargo feature is on.
//! Exists to measure the 4dcffcd ~10 events/sec floor by region; designed
//! to be ripped out in phase 8 once the new log store is in place.
//!
//! Design notes:
//! - No `tracing` — we do not want the subscriber in the measurement path.
//! - A single process-global `RecordSink` is used, chosen because the bench
//!   harness is single-threaded per sample. Plan 01-02 installs a sink; when
//!   no sink is installed, timer output is dropped (zero-alloc fast path).
//! - Fsync counter is a plain `AtomicU64`; callers bump it immediately after
//!   a successful fsync / fdatasync / rename-with-dir-fsync.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

/// The four named hotspots. Names MUST match the CSV/JSON schema plan 01-02 writes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Region {
    /// log_store::append — bincode rewrite of the full BTreeMap log.
    LogBincodeRewrite,
    /// log_store::atomic_write — tmp write + fsync + rename + dir fsync.
    LogAtomicWrite,
    /// state_machine::apply_request — the event-path match arm (Append variant).
    ApplyEventPath,
    /// segment::writer::append — group-commit append + fsync.
    SegmentAppend,
}

impl Region {
    pub fn as_str(self) -> &'static str {
        match self {
            Region::LogBincodeRewrite => "log_bincode_rewrite",
            Region::LogAtomicWrite => "log_atomic_write",
            Region::ApplyEventPath => "apply_event_path",
            Region::SegmentAppend => "segment_append",
        }
    }
}

/// Process-global fsync syscall counter.
pub static FSYNC_COUNTER: AtomicU64 = AtomicU64::new(0);

#[inline]
pub fn bump_fsync() {
    FSYNC_COUNTER.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub fn fsync_count() -> u64 {
    FSYNC_COUNTER.load(Ordering::Relaxed)
}

#[inline]
pub fn reset_fsync() {
    FSYNC_COUNTER.store(0, Ordering::Relaxed);
}

/// One timing sample produced by a scoped Timer::drop.
#[derive(Debug, Clone, Copy)]
pub struct Sample {
    pub region: Region,
    pub duration: Duration,
}

/// Per-append record aggregating multiple region samples for a single append call.
///
/// Plan 01-02's bench harness may construct these from drained samples when
/// emitting CSV/JSONL output. Kept here so the schema lives with the regions.
#[derive(Debug, Clone, Default)]
pub struct AppendRecord {
    pub log_bincode_rewrite_ns: u64,
    pub log_atomic_write_ns: u64,
    pub apply_event_path_ns: u64,
    pub segment_append_ns: u64,
    pub fsync_count: u64,
}

/// Accumulates per-append timing samples. Plan 01-02's bench installs a sink
/// before each b.iter() closure and drains it afterwards.
pub trait RecordSink: Send + Sync {
    fn record(&self, sample: Sample);
}

/// An in-memory sink that buffers samples in a Vec. Simple default used by
/// plan 01-02's harness; planner is free to swap for a CSV-stream sink.
pub struct VecSink {
    inner: Mutex<Vec<Sample>>,
}

impl VecSink {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(Vec::new()),
        }
    }
    pub fn drain(&self) -> Vec<Sample> {
        std::mem::take(&mut *self.inner.lock().unwrap())
    }
}

impl Default for VecSink {
    fn default() -> Self {
        Self::new()
    }
}

impl RecordSink for VecSink {
    fn record(&self, sample: Sample) {
        self.inner.lock().unwrap().push(sample);
    }
}

/// A fsync-counter counter wrapper kept as a type for parity with the plan's
/// exported surface. The actual counter is the static `FSYNC_COUNTER`; this
/// newtype is a convenience handle the bench can use to capture deltas.
pub struct FsyncCounter;

impl FsyncCounter {
    pub fn snapshot() -> u64 {
        fsync_count()
    }
    pub fn reset() {
        reset_fsync();
    }
}

static SINK: OnceLock<Mutex<Option<Arc<dyn RecordSink>>>> = OnceLock::new();

fn sink_slot() -> &'static Mutex<Option<Arc<dyn RecordSink>>> {
    SINK.get_or_init(|| Mutex::new(None))
}

/// Install the process-global sink. Plan 01-02 calls this before b.iter().
pub fn install_sink(sink: Arc<dyn RecordSink>) {
    *sink_slot().lock().unwrap() = Some(sink);
}

/// Clear the process-global sink.
pub fn clear_sink() {
    *sink_slot().lock().unwrap() = None;
}

fn emit(sample: Sample) {
    if let Some(s) = sink_slot().lock().unwrap().as_ref() {
        s.record(sample);
    }
}

/// RAII scoped timer. Emits one Sample on drop if a sink is installed.
/// Use via the `bench_time!(Region::X)` macro below for clarity at call sites.
pub struct Timer {
    region: Region,
    start: Instant,
}

impl Timer {
    pub fn new(region: Region) -> Self {
        Self {
            region,
            start: Instant::now(),
        }
    }
}

impl Drop for Timer {
    fn drop(&mut self) {
        emit(Sample {
            region: self.region,
            duration: self.start.elapsed(),
        });
    }
}

/// Convenience macro — call sites write `let _t = bench_time!(Region::LogAtomicWrite);`.
#[macro_export]
macro_rules! bench_time {
    ($region:expr) => {
        $crate::raft::bench_instrumentation::Timer::new($region)
    };
}
