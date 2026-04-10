//! Lock-free internal metrics for the event store engine.
//!
//! Every counter is a plain AtomicU64 — one cache-line-aligned atomic increment
//! per operation. No locks, no allocations, no overhead beyond the increment itself.
//!
//! These are internal counters, not Prometheus metrics. The server layer reads them
//! (via snapshot()) and can expose them however it wants: admin console, HTTP endpoint,
//! Prometheus exporter sidecar, etc.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

/// Relaxed ordering for metrics — we don't need happens-before guarantees
/// for approximate counters. This compiles to a plain ADD on x86.
const ORD: Ordering = Ordering::Relaxed;

/// Atomic counters for one event store engine (one context).
pub struct StoreMetrics {
    // ── Writes ──
    /// Total append() calls (successful).
    pub appends: AtomicU64,
    /// Total events appended (across all append calls).
    pub events_appended: AtomicU64,
    /// Total append() calls that failed due to DCB condition violation.
    pub dcb_violations: AtomicU64,
    /// Cumulative append duration in microseconds (divide by appends for average).
    pub append_duration_us: AtomicU64,

    // ── Reads ──
    /// Total source() calls.
    pub source_queries: AtomicU64,
    /// Total events returned by source() calls.
    pub events_sourced: AtomicU64,
    /// Cumulative source duration in microseconds.
    pub source_duration_us: AtomicU64,

    // ── Index cache ──
    pub index_cache_hits: AtomicU64,
    pub index_cache_misses: AtomicU64,

    // ── Bloom filter ──
    pub bloom_checks: AtomicU64,
    /// Segments skipped because bloom said "definitely not".
    pub bloom_rejections: AtomicU64,

    // ── Mmap cache ──
    pub mmap_cache_hits: AtomicU64,
    pub mmap_cache_misses: AtomicU64,

    // ── Segments ──
    /// Total segment rotations since startup.
    pub segment_rotations: AtomicU64,

    // ── Direct seeks ──
    /// Source queries that used direct offset seeks (v2 index).
    pub direct_seek_reads: AtomicU64,
    /// Source queries that fell back to sequential scan (v1 index or active segment).
    pub sequential_scan_reads: AtomicU64,
}

impl StoreMetrics {
    pub fn new() -> Self {
        Self {
            appends: AtomicU64::new(0),
            events_appended: AtomicU64::new(0),
            dcb_violations: AtomicU64::new(0),
            append_duration_us: AtomicU64::new(0),
            source_queries: AtomicU64::new(0),
            events_sourced: AtomicU64::new(0),
            source_duration_us: AtomicU64::new(0),
            index_cache_hits: AtomicU64::new(0),
            index_cache_misses: AtomicU64::new(0),
            bloom_checks: AtomicU64::new(0),
            bloom_rejections: AtomicU64::new(0),
            mmap_cache_hits: AtomicU64::new(0),
            mmap_cache_misses: AtomicU64::new(0),
            segment_rotations: AtomicU64::new(0),
            direct_seek_reads: AtomicU64::new(0),
            sequential_scan_reads: AtomicU64::new(0),
        }
    }

    /// Takes a point-in-time snapshot of all counters.
    /// The server layer can diff two snapshots to compute rates.
    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            appends: self.appends.load(ORD),
            events_appended: self.events_appended.load(ORD),
            dcb_violations: self.dcb_violations.load(ORD),
            append_duration_us: self.append_duration_us.load(ORD),
            source_queries: self.source_queries.load(ORD),
            events_sourced: self.events_sourced.load(ORD),
            source_duration_us: self.source_duration_us.load(ORD),
            index_cache_hits: self.index_cache_hits.load(ORD),
            index_cache_misses: self.index_cache_misses.load(ORD),
            bloom_checks: self.bloom_checks.load(ORD),
            bloom_rejections: self.bloom_rejections.load(ORD),
            mmap_cache_hits: self.mmap_cache_hits.load(ORD),
            mmap_cache_misses: self.mmap_cache_misses.load(ORD),
            segment_rotations: self.segment_rotations.load(ORD),
            direct_seek_reads: self.direct_seek_reads.load(ORD),
            sequential_scan_reads: self.sequential_scan_reads.load(ORD),
        }
    }

    // ── Increment helpers (inline for zero overhead) ──

    #[inline]
    pub fn record_append(&self, event_count: u32, duration_us: u64) {
        self.appends.fetch_add(1, ORD);
        self.events_appended.fetch_add(event_count as u64, ORD);
        self.append_duration_us.fetch_add(duration_us, ORD);
    }

    #[inline]
    pub fn record_dcb_violation(&self) {
        self.dcb_violations.fetch_add(1, ORD);
    }

    #[inline]
    pub fn record_source(&self, event_count: usize, duration_us: u64) {
        self.source_queries.fetch_add(1, ORD);
        self.events_sourced.fetch_add(event_count as u64, ORD);
        self.source_duration_us.fetch_add(duration_us, ORD);
    }

    #[inline]
    pub fn record_index_cache_hit(&self) {
        self.index_cache_hits.fetch_add(1, ORD);
    }

    #[inline]
    pub fn record_index_cache_miss(&self) {
        self.index_cache_misses.fetch_add(1, ORD);
    }

    #[inline]
    pub fn record_bloom_check(&self, rejected: bool) {
        self.bloom_checks.fetch_add(1, ORD);
        if rejected {
            self.bloom_rejections.fetch_add(1, ORD);
        }
    }

    #[inline]
    pub fn record_mmap_cache_hit(&self) {
        self.mmap_cache_hits.fetch_add(1, ORD);
    }

    #[inline]
    pub fn record_mmap_cache_miss(&self) {
        self.mmap_cache_misses.fetch_add(1, ORD);
    }

    #[inline]
    pub fn record_segment_rotation(&self) {
        self.segment_rotations.fetch_add(1, ORD);
    }

    #[inline]
    pub fn record_direct_seek(&self) {
        self.direct_seek_reads.fetch_add(1, ORD);
    }

    #[inline]
    pub fn record_sequential_scan(&self) {
        self.sequential_scan_reads.fetch_add(1, ORD);
    }
}

/// A frozen point-in-time copy of all metrics. Cheap to clone and pass around.
/// Diff two snapshots to compute rates (e.g., events/sec over the last 5s).
#[derive(Debug, Clone)]
pub struct MetricsSnapshot {
    pub appends: u64,
    pub events_appended: u64,
    pub dcb_violations: u64,
    pub append_duration_us: u64,
    pub source_queries: u64,
    pub events_sourced: u64,
    pub source_duration_us: u64,
    pub index_cache_hits: u64,
    pub index_cache_misses: u64,
    pub bloom_checks: u64,
    pub bloom_rejections: u64,
    pub mmap_cache_hits: u64,
    pub mmap_cache_misses: u64,
    pub segment_rotations: u64,
    pub direct_seek_reads: u64,
    pub sequential_scan_reads: u64,
}

impl MetricsSnapshot {
    /// Average append latency in microseconds, or 0 if no appends.
    pub fn avg_append_us(&self) -> u64 {
        if self.appends == 0 {
            0
        } else {
            self.append_duration_us / self.appends
        }
    }

    /// Average source latency in microseconds, or 0 if no queries.
    pub fn avg_source_us(&self) -> u64 {
        if self.source_queries == 0 {
            0
        } else {
            self.source_duration_us / self.source_queries
        }
    }

    /// Index cache hit rate as a percentage (0.0 - 100.0).
    pub fn index_cache_hit_rate(&self) -> f64 {
        let total = self.index_cache_hits + self.index_cache_misses;
        if total == 0 {
            0.0
        } else {
            (self.index_cache_hits as f64 / total as f64) * 100.0
        }
    }

    /// Bloom filter rejection rate as a percentage.
    pub fn bloom_rejection_rate(&self) -> f64 {
        if self.bloom_checks == 0 {
            0.0
        } else {
            (self.bloom_rejections as f64 / self.bloom_checks as f64) * 100.0
        }
    }

    /// Mmap cache hit rate as a percentage.
    pub fn mmap_cache_hit_rate(&self) -> f64 {
        let total = self.mmap_cache_hits + self.mmap_cache_misses;
        if total == 0 {
            0.0
        } else {
            (self.mmap_cache_hits as f64 / total as f64) * 100.0
        }
    }

    /// Fraction of reads using direct seeks vs sequential scans.
    pub fn direct_seek_rate(&self) -> f64 {
        let total = self.direct_seek_reads + self.sequential_scan_reads;
        if total == 0 {
            0.0
        } else {
            (self.direct_seek_reads as f64 / total as f64) * 100.0
        }
    }
}

/// Utility for timing operations. Usage:
/// ```ignore
/// let timer = Timer::start();
/// // ... do work ...
/// metrics.record_source(count, timer.elapsed_us());
/// ```
pub struct Timer(Instant);

impl Timer {
    #[inline]
    pub fn start() -> Self {
        Self(Instant::now())
    }

    #[inline]
    pub fn elapsed_us(&self) -> u64 {
        self.0.elapsed().as_micros() as u64
    }
}
