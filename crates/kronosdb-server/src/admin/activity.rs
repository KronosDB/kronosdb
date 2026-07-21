//! Hourly append-activity tracking for the admin console.
//!
//! The engine keeps no time-series metrics, so the console samples each
//! context's head position once a minute and buckets the deltas into
//! hourly counts. Purely in-memory, bounded to the last 24 hours —
//! restarts lose history, which is fine for an ops glance chart.

use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use parking_lot::RwLock;

use super::AdminState;

/// Hours of history retained (also the chart's x-axis width).
pub const HOURS: usize = 24;

const SAMPLE_INTERVAL: Duration = Duration::from_secs(60);

#[derive(Default)]
struct ContextActivity {
    /// Head position at the previous sample.
    last_head: u64,
    /// (hour epoch, events appended in that hour), oldest first.
    hours: Vec<(u64, u64)>,
}

#[derive(Default)]
pub struct ActivityTracker {
    contexts: RwLock<HashMap<String, ContextActivity>>,
}

impl ActivityTracker {
    pub fn new() -> Self {
        Self::default()
    }

    /// Samples every context's head and buckets the delta into the current
    /// hour. First sight of a context sets the baseline without counting
    /// pre-existing events as this hour's activity.
    fn sample(&self, state: &AdminState) {
        let hour = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs() / 3600)
            .unwrap_or(0);

        let mut contexts = self.contexts.write();
        for name in state.contexts.list_contexts() {
            let Ok(store) = state.contexts.get_context(&name) else {
                continue;
            };
            let head = store.head().0;
            let entry = contexts.entry(name).or_insert_with(|| ContextActivity {
                last_head: head,
                hours: Vec::new(),
            });

            let delta = head.saturating_sub(entry.last_head);
            entry.last_head = head;
            if delta > 0 {
                match entry.hours.last_mut() {
                    Some((h, count)) if *h == hour => *count += delta,
                    _ => entry.hours.push((hour, delta)),
                }
            }
            let cutoff = hour.saturating_sub(HOURS as u64);
            entry.hours.retain(|(h, _)| *h > cutoff);
        }
    }

    /// Total events per hour across all contexts for the last [`HOURS`]
    /// hours, oldest first. The final bucket is the current (partial) hour.
    pub fn hourly_totals(&self) -> [u64; HOURS] {
        let hour = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs() / 3600)
            .unwrap_or(0);
        let mut totals = [0u64; HOURS];
        let contexts = self.contexts.read();
        for activity in contexts.values() {
            for (h, count) in &activity.hours {
                let age = hour.saturating_sub(*h) as usize;
                if age < HOURS {
                    totals[HOURS - 1 - age] += count;
                }
            }
        }
        totals
    }
}

/// Spawns the once-a-minute sampler. An immediate first sample sets the
/// per-context baselines so the first minute isn't misattributed.
pub fn spawn_sampler(state: AdminState) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(SAMPLE_INTERVAL);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            interval.tick().await;
            state.activity.sample(&state);
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hourly_totals_empty() {
        let tracker = ActivityTracker::new();
        assert_eq!(tracker.hourly_totals(), [0; HOURS]);
    }
}
