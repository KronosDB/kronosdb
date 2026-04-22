//! Aggregates per-append JSONL records from target/baseline-records/ into
//! per-cell summaries. See .planning/phases/01-baseline/01-CONTEXT.md D-09.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Deserialize)]
pub struct RegionRecord {
    pub region: String,
    pub nanos: u128,
}

#[derive(Deserialize)]
pub struct AppendRecord {
    pub cell: String,
    pub iter: u64,
    pub regions: Vec<RegionRecord>,
    pub fsyncs_before: u64,
    pub fsyncs_after: u64,
}

#[derive(Serialize, Debug, Clone)]
pub struct RegionStats {
    pub p50_us: f64,
    pub p95_us: f64,
    pub mean_us: f64,
    pub max_us: f64,
    pub count: u64,
}

#[derive(Serialize, Debug, Clone)]
pub struct CellSummary {
    pub events_per_sec: f64,
    pub total_latency_us: RegionStats, // per-append total (sum of regions, or wall-clock proxy)
    pub fsyncs_per_append: f64,
    pub regions: BTreeMap<String, RegionStats>,
    pub samples: u64,
    pub batch_size: u32,
}

#[derive(Serialize, Debug)]
pub struct Summary {
    pub commit: String,
    pub generated_at: String,
    pub host: HostInfo,
    pub headline: Headline,
    pub cells: BTreeMap<String, CellSummary>,
}

#[derive(Serialize, Debug)]
pub struct Headline {
    pub cell: String,
    pub events_per_sec: f64,
    pub mean_latency_us: f64,
    pub fsyncs_per_append: f64,
}

#[derive(Serialize, Debug)]
pub struct HostInfo {
    pub os: String,
    pub arch: String,
}

/// Parse cell key like "conditional__always-match__batch10" → batch size.
pub fn parse_batch(cell: &str) -> Option<u32> {
    cell.rsplit("__batch").next().and_then(|s| s.parse().ok())
}

pub fn percentile(sorted_us: &[f64], p: f64) -> f64 {
    if sorted_us.is_empty() {
        return 0.0;
    }
    let idx = ((sorted_us.len() as f64 - 1.0) * p).round() as usize;
    sorted_us[idx.min(sorted_us.len() - 1)]
}

pub fn region_stats(mut samples_us: Vec<f64>) -> RegionStats {
    let count = samples_us.len() as u64;
    if count == 0 {
        return RegionStats {
            p50_us: 0.0,
            p95_us: 0.0,
            mean_us: 0.0,
            max_us: 0.0,
            count: 0,
        };
    }
    samples_us.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let mean = samples_us.iter().sum::<f64>() / count as f64;
    let max = *samples_us.last().unwrap();
    RegionStats {
        p50_us: percentile(&samples_us, 0.50),
        p95_us: percentile(&samples_us, 0.95),
        mean_us: mean,
        max_us: max,
        count,
    }
}

/// Aggregate a stream of records for a single cell into a CellSummary.
pub fn aggregate_cell(_cell: &str, records: &[AppendRecord], batch: u32) -> CellSummary {
    // Gather per-region samples in microseconds.
    let mut by_region: BTreeMap<String, Vec<f64>> = BTreeMap::new();
    let mut totals_us: Vec<f64> = Vec::new();
    let mut fsyncs_total: u64 = 0;

    for r in records {
        let mut sum_ns: u128 = 0;
        for reg in &r.regions {
            let us = reg.nanos as f64 / 1_000.0;
            by_region.entry(reg.region.clone()).or_default().push(us);
            sum_ns += reg.nanos;
        }
        totals_us.push(sum_ns as f64 / 1_000.0);
        fsyncs_total += r.fsyncs_after.saturating_sub(r.fsyncs_before);
    }

    let samples = records.len() as u64;
    let total = region_stats(totals_us);
    let fsyncs_per_append = if samples == 0 {
        0.0
    } else {
        fsyncs_total as f64 / samples as f64
    };

    // Throughput = batch / mean per-append latency
    let events_per_sec = if total.mean_us > 0.0 {
        (batch as f64) * 1_000_000.0 / total.mean_us
    } else {
        0.0
    };

    let regions = by_region
        .into_iter()
        .map(|(name, samples_us)| (name, region_stats(samples_us)))
        .collect();

    CellSummary {
        events_per_sec,
        total_latency_us: total,
        fsyncs_per_append,
        regions,
        samples,
        batch_size: batch,
    }
}

/// Aggregate all cells into a full Summary.
pub fn aggregate_records(
    commit: &str,
    per_cell_records: &BTreeMap<String, Vec<AppendRecord>>,
) -> Summary {
    let mut cells = BTreeMap::new();
    for (cell, records) in per_cell_records {
        let batch = parse_batch(cell).unwrap_or(1);
        cells.insert(cell.clone(), aggregate_cell(cell, records, batch));
    }

    // Headline: conditional/always-match/batch=1 — the PERF-03 cell.
    let headline_key = "conditional__always-match__batch1".to_string();
    let headline = cells
        .get(&headline_key)
        .map(|c| Headline {
            cell: headline_key.clone(),
            events_per_sec: c.events_per_sec,
            mean_latency_us: c.total_latency_us.mean_us,
            fsyncs_per_append: c.fsyncs_per_append,
        })
        .unwrap_or(Headline {
            cell: headline_key,
            events_per_sec: 0.0,
            mean_latency_us: 0.0,
            fsyncs_per_append: 0.0,
        });

    Summary {
        commit: commit.to_string(),
        generated_at: chrono::Utc::now().to_rfc3339(),
        host: HostInfo {
            os: std::env::consts::OS.to_string(),
            arch: std::env::consts::ARCH.to_string(),
        },
        headline,
        cells,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn percentile_basic() {
        let xs = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        assert_eq!(percentile(&xs, 0.50), 3.0);
    }

    #[test]
    fn region_stats_from_samples() {
        let s = region_stats(vec![1.0, 2.0, 3.0, 4.0, 5.0]);
        assert!((s.mean_us - 3.0).abs() < 1e-9);
        assert_eq!(s.max_us, 5.0);
        assert_eq!(s.count, 5);
    }

    #[test]
    fn aggregate_cell_throughput() {
        let rs = vec![RegionRecord {
            region: "log_atomic_write".into(),
            nanos: 100_000_000,
        }]; // 100ms
        let rec = AppendRecord {
            cell: "conditional__always-match__batch1".into(),
            iter: 1,
            regions: rs,
            fsyncs_before: 0,
            fsyncs_after: 2,
        };
        let cs = aggregate_cell("conditional__always-match__batch1", &[rec], 1);
        // 100ms per append → 10 events/sec
        assert!((cs.events_per_sec - 10.0).abs() < 0.5);
        assert_eq!(cs.fsyncs_per_append, 2.0);
    }

    #[test]
    fn parse_batch_works() {
        assert_eq!(parse_batch("conditional__always-match__batch1"), Some(1));
        assert_eq!(
            parse_batch("unconditional__always-match__batch100"),
            Some(100)
        );
    }
}
