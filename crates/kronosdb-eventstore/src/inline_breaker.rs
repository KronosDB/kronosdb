//! A circuit breaker for staging appends on an async worker.
//!
//! The cheapest place to stage an append is the async worker that received
//! it, as long as the work there is a few microseconds of memory writes. If
//! the write stalls (the page cache throttling dirty pages, a filesystem
//! journal commit), every connection pinned to that worker stalls with it.
//!
//! The breaker times each inline staging. One that takes at least
//! [`STALL_LIMIT`] shuts the inline route for [`COOLDOWN`]; appends then go
//! to the staging thread, which can block without freezing anyone else. When
//! the cooldown ends the next append tries inline again, and a repeat stall
//! shuts the route once more.
//!
//! State is one atomic deadline plus two for the warning's rate limit: the
//! hot path is a load and a compare, with no lock. Time is passed in as
//! nanoseconds on the breaker's own clock, so tests drive it directly.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// An inline staging at least this long counts as a stall.
pub(crate) const STALL_LIMIT: Duration = Duration::from_millis(2);
/// How long the inline route stays shut after a stall.
pub(crate) const COOLDOWN: Duration = Duration::from_secs(1);
/// At most one warning per this long; the stalls in between are counted into it.
pub(crate) const WARN_EVERY: Duration = Duration::from_secs(10);

/// What `finished` made of one inline staging.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Verdict {
    Fine,
    /// It stalled and the route is shut until the cooldown ends. `warn` is
    /// set when the caller should log: how many stalls there have been since
    /// the last warning, this one included.
    Stalled {
        elapsed_ns: u64,
        warn: Option<u64>,
    },
}

pub(crate) struct InlineBreaker {
    epoch: Instant,
    limit_ns: AtomicU64,
    cooldown_ns: AtomicU64,
    warn_every_ns: AtomicU64,
    /// The inline route is shut until this time. Moves only forward.
    shut_until: AtomicU64,
    /// When the last warning went out; 0 until the first.
    last_warn: AtomicU64,
    /// Stalls since the last warning.
    unreported: AtomicU64,
}

impl InlineBreaker {
    pub(crate) fn new() -> Self {
        Self::with_limits(STALL_LIMIT, COOLDOWN, WARN_EVERY)
    }

    pub(crate) fn with_limits(limit: Duration, cooldown: Duration, warn_every: Duration) -> Self {
        Self {
            epoch: Instant::now(),
            limit_ns: AtomicU64::new(limit.as_nanos() as u64),
            cooldown_ns: AtomicU64::new(cooldown.as_nanos() as u64),
            warn_every_ns: AtomicU64::new(warn_every.as_nanos() as u64),
            shut_until: AtomicU64::new(0),
            last_warn: AtomicU64::new(0),
            unreported: AtomicU64::new(0),
        }
    }

    /// Nanoseconds on this breaker's clock. Starts at 1 so that 0 can mean
    /// "never" in the state above.
    #[inline]
    pub(crate) fn now(&self) -> u64 {
        self.epoch.elapsed().as_nanos() as u64 + 1
    }

    /// Whether the inline route is open at `now`.
    #[inline]
    pub(crate) fn allows(&self, now: u64) -> bool {
        now >= self.shut_until.load(Ordering::Relaxed)
    }

    /// Judges one inline staging that ran from `started` to `ended`, and shuts
    /// the route when it stalled. Stagings that began before the route shut
    /// and end after it may push the deadline out again, which is intended:
    /// the disk is still slow.
    pub(crate) fn finished(&self, started: u64, ended: u64) -> Verdict {
        let elapsed_ns = ended.saturating_sub(started);
        if elapsed_ns < self.limit_ns.load(Ordering::Relaxed) {
            return Verdict::Fine;
        }
        let until = ended.saturating_add(self.cooldown_ns.load(Ordering::Relaxed));
        self.shut_until.fetch_max(until, Ordering::Relaxed);
        self.unreported.fetch_add(1, Ordering::Relaxed);

        // One caller wins the right to warn for this interval.
        let last = self.last_warn.load(Ordering::Relaxed);
        let due =
            last == 0 || ended.saturating_sub(last) >= self.warn_every_ns.load(Ordering::Relaxed);
        let warn = if due
            && self
                .last_warn
                .compare_exchange(last, ended, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
        {
            Some(self.unreported.swap(0, Ordering::Relaxed).max(1))
        } else {
            None
        };
        Verdict::Stalled { elapsed_ns, warn }
    }

    /// The cooldown, for the log line.
    pub(crate) fn cooldown(&self) -> Duration {
        Duration::from_nanos(self.cooldown_ns.load(Ordering::Relaxed))
    }

    #[cfg(test)]
    pub(crate) fn set_limits(&self, limit: Duration, cooldown: Duration) {
        self.limit_ns
            .store(limit.as_nanos() as u64, Ordering::Relaxed);
        self.cooldown_ns
            .store(cooldown.as_nanos() as u64, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MS: u64 = 1_000_000;

    fn breaker() -> InlineBreaker {
        InlineBreaker::with_limits(
            Duration::from_millis(2),
            Duration::from_secs(1),
            Duration::from_secs(10),
        )
    }

    #[test]
    fn a_new_breaker_is_open() {
        let b = breaker();
        assert!(b.allows(1));
        assert!(b.allows(b.now()));
    }

    #[test]
    fn fast_stagings_never_shut_the_route() {
        let b = breaker();
        for i in 0..1000u64 {
            let start = 100 + i * 10_000;
            assert_eq!(b.finished(start, start + 5_000), Verdict::Fine);
            assert!(b.allows(start + 5_000));
        }
        // Just under the limit is still fine.
        assert_eq!(b.finished(1_000, 1_000 + 2 * MS - 1), Verdict::Fine);
    }

    #[test]
    fn a_stall_shuts_the_route_for_the_cooldown_then_reopens() {
        let b = breaker();
        let start = 10 * MS;
        let end = start + 3 * MS;
        let Verdict::Stalled { elapsed_ns, .. } = b.finished(start, end) else {
            panic!("a 3 ms staging must count as a stall");
        };
        assert_eq!(elapsed_ns, 3 * MS);

        assert!(!b.allows(end));
        assert!(!b.allows(end + 999 * MS));
        assert!(b.allows(end + 1000 * MS), "open again at the deadline");
        assert!(b.allows(end + 5000 * MS));
    }

    #[test]
    fn exactly_the_limit_is_a_stall() {
        let b = breaker();
        assert!(matches!(b.finished(0, 2 * MS), Verdict::Stalled { .. }));
    }

    #[test]
    fn a_repeat_stall_after_reopening_shuts_it_again() {
        let b = breaker();
        b.finished(0, 5 * MS);
        let reopened = 5 * MS + 1000 * MS;
        assert!(b.allows(reopened));
        b.finished(reopened, reopened + 4 * MS);
        assert!(!b.allows(reopened + 4 * MS + 500 * MS));
        assert!(b.allows(reopened + 4 * MS + 1000 * MS));
    }

    #[test]
    fn a_late_straggler_extends_the_deadline_but_never_shortens_it() {
        let b = breaker();
        b.finished(0, 5 * MS); // shut until 1005 ms
        // A staging that started before the trip finishes later and also stalled.
        b.finished(2 * MS, 600 * MS); // shut until 1600 ms
        assert!(!b.allows(1500 * MS));
        assert!(b.allows(1600 * MS));
        // A slow one that ends earlier than the current deadline cannot pull it in.
        b.finished(10 * MS, 20 * MS); // would be 1020 ms
        assert!(!b.allows(1500 * MS));
    }

    #[test]
    fn warnings_are_rate_limited_and_count_what_they_cover() {
        let b = breaker();
        // The first stall warns, covering itself.
        assert_eq!(
            b.finished(0, 3 * MS),
            Verdict::Stalled {
                elapsed_ns: 3 * MS,
                warn: Some(1)
            }
        );
        // Further stalls inside the interval stay quiet.
        for i in 1..=4u64 {
            let start = i * 1500 * MS;
            let Verdict::Stalled { warn, .. } = b.finished(start, start + 3 * MS) else {
                panic!("stall expected");
            };
            // 1.5 s apart: well inside the 10 s window.
            assert_eq!(warn, None, "stall {i}");
        }
        // Past the interval the next stall warns and reports the four it hid
        // plus itself.
        let start = 11_000 * MS;
        assert_eq!(
            b.finished(start, start + 3 * MS),
            Verdict::Stalled {
                elapsed_ns: 3 * MS,
                warn: Some(5)
            }
        );
        // And the window restarts from that warning.
        let start = 12_000 * MS;
        let Verdict::Stalled { warn, .. } = b.finished(start, start + 3 * MS) else {
            panic!("stall expected");
        };
        assert_eq!(warn, None);
    }

    #[test]
    fn set_limits_changes_what_counts() {
        let b = breaker();
        assert_eq!(b.finished(0, MS), Verdict::Fine);
        b.set_limits(Duration::ZERO, Duration::from_secs(3600));
        assert!(matches!(b.finished(0, 1), Verdict::Stalled { .. }));
        assert!(!b.allows(3000 * 1000 * MS));
        assert_eq!(b.cooldown(), Duration::from_secs(3600));
    }

    #[test]
    fn concurrent_stalls_leave_a_sane_deadline_and_one_warning() {
        let b = std::sync::Arc::new(breaker());
        let warnings = std::sync::Arc::new(AtomicU64::new(0));
        let threads: Vec<_> = (0..8u64)
            .map(|t| {
                let (b, warnings) = (b.clone(), warnings.clone());
                std::thread::spawn(move || {
                    for i in 0..1000u64 {
                        let start = (t * 1000 + i) * MS;
                        if let Verdict::Stalled { warn: Some(_), .. } =
                            b.finished(start, start + 3 * MS)
                        {
                            warnings.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                })
            })
            .collect();
        for thread in threads {
            thread.join().unwrap();
        }
        // The latest end across all threads set the deadline.
        let latest_end = (7 * 1000 + 999) * MS + 3 * MS;
        assert!(!b.allows(latest_end + 999 * MS));
        assert!(b.allows(latest_end + 1000 * MS));
        // Ends span 8 s of breaker time, under the 10 s interval; only the
        // first caller to pass the gate may have warned.
        assert_eq!(warnings.load(Ordering::Relaxed), 1);
    }
}
