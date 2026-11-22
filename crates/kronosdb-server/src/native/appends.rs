//! What this node holds for appends at once, and how long one may wait.
//!
//! An append keeps its records in memory from the moment its request is read
//! until it is answered, and it is answered only once it is durable and, in a
//! cluster, acknowledged by a quorum. With a slow disk or unreachable followers
//! that can take arbitrarily long, so two limits apply to every append this
//! node answers:
//!
//! - A cap on the bytes of appends waiting at once. One that would go past it
//!   is refused before anything is written, as `unavailable`.
//! - A deadline. An append not confirmed by then is answered as
//!   `unavailable`, with its outcome unknown: it was (or may have been)
//!   written, and may still end up committed. The client sends the same append
//!   again, which the store recognises (`kronosdb_eventstore::recent`).

use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use kronosdb_eventstore::error::Error;

pub struct AppendBudget {
    max_pending_bytes: u64,
    deadline: Duration,
    pending: AtomicU64,
    refused: AtomicU64,
    timed_out: AtomicU64,
}

impl AppendBudget {
    pub fn new(max_pending_bytes: u64, deadline: Duration) -> Self {
        Self {
            max_pending_bytes,
            deadline,
            pending: AtomicU64::new(0),
            refused: AtomicU64::new(0),
            timed_out: AtomicU64::new(0),
        }
    }

    /// Bytes of appends waiting now.
    pub fn pending_bytes(&self) -> u64 {
        self.pending.load(Ordering::Relaxed)
    }

    /// Appends refused at the cap, since start.
    pub fn refused(&self) -> u64 {
        self.refused.load(Ordering::Relaxed)
    }

    /// Appends answered at the deadline, since start.
    pub fn timed_out(&self) -> u64 {
        self.timed_out.load(Ordering::Relaxed)
    }

    /// Runs one append of `bytes` request bytes under both limits.
    ///
    /// Refused, as `Unavailable`, when the waiting appends and this one would
    /// pass the cap; one append alone is always let through, however large,
    /// so a cap below the frame limit cannot refuse a request forever.
    /// Answered as `UnknownOutcome` if not confirmed by the deadline. Its
    /// room is given back however it ends, cancelled included.
    pub async fn run<T>(
        &self,
        bytes: u64,
        append: impl Future<Output = Result<T, Error>>,
    ) -> Result<T, Error> {
        let _room = self.reserve(bytes)?;
        match tokio::time::timeout(self.deadline, append).await {
            Ok(result) => result,
            Err(_) => {
                self.timed_out.fetch_add(1, Ordering::Relaxed);
                Err(Error::UnknownOutcome {
                    message: format!(
                        "the append was not confirmed as committed within {} s ([timeouts] \
                         append): the disk is not keeping up, or no quorum acknowledges it",
                        self.deadline.as_secs()
                    ),
                })
            }
        }
    }

    fn reserve(&self, bytes: u64) -> Result<Room<'_>, Error> {
        let mut pending = self.pending.load(Ordering::Relaxed);
        loop {
            if pending > 0 && pending.saturating_add(bytes) > self.max_pending_bytes {
                self.refused.fetch_add(1, Ordering::Relaxed);
                return Err(Error::Unavailable {
                    message: format!(
                        "this node has {pending} bytes of appends waiting to be made durable or \
                         acknowledged by a quorum, and this one ({bytes} bytes) would pass its \
                         limit of {} ([limits] max-pending-append-bytes); nothing was written, \
                         retry shortly",
                        self.max_pending_bytes
                    ),
                });
            }
            match self.pending.compare_exchange_weak(
                pending,
                pending + bytes,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    return Ok(Room {
                        budget: self,
                        bytes,
                    });
                }
                Err(now) => pending = now,
            }
        }
    }
}

/// Bytes reserved for one append, given back when dropped.
struct Room<'a> {
    budget: &'a AppendBudget,
    bytes: u64,
}

impl Drop for Room<'_> {
    fn drop(&mut self) {
        self.budget.pending.fetch_sub(self.bytes, Ordering::AcqRel);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MIB: u64 = 1024 * 1024;

    fn never() -> impl Future<Output = Result<(), Error>> {
        std::future::pending()
    }

    #[tokio::test]
    async fn an_append_past_the_cap_is_refused_as_unavailable_and_room_comes_back() {
        let budget = std::sync::Arc::new(AppendBudget::new(10 * MIB, Duration::from_secs(60)));
        let (release, released) = tokio::sync::oneshot::channel::<()>();
        let waiting = {
            let budget = std::sync::Arc::clone(&budget);
            tokio::spawn(async move {
                budget
                    .run(8 * MIB, async move {
                        let _ = released.await;
                        Ok(())
                    })
                    .await
            })
        };
        while budget.pending_bytes() == 0 {
            tokio::task::yield_now().await;
        }

        let refused = budget.run(4 * MIB, async { Ok(()) }).await.unwrap_err();
        assert!(matches!(refused, Error::Unavailable { .. }), "{refused}");
        assert!(
            refused.to_string().contains("max-pending-append-bytes"),
            "{refused}"
        );
        assert_eq!(budget.refused(), 1);
        budget.run(2 * MIB, async { Ok(()) }).await.unwrap();

        release.send(()).unwrap();
        waiting.await.unwrap().unwrap();
        assert_eq!(budget.pending_bytes(), 0);
        budget.run(9 * MIB, async { Ok(()) }).await.unwrap();
    }

    #[tokio::test]
    async fn one_append_larger_than_the_cap_goes_through_alone() {
        let budget = AppendBudget::new(MIB, Duration::from_secs(60));
        budget.run(16 * MIB, async { Ok(()) }).await.unwrap();
        assert_eq!(budget.pending_bytes(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn an_append_not_confirmed_by_the_deadline_has_an_unknown_outcome() {
        let budget = AppendBudget::new(10 * MIB, Duration::from_secs(20));
        let late = budget.run(MIB, never()).await.unwrap_err();
        assert!(matches!(late, Error::UnknownOutcome { .. }), "{late}");
        assert!(late.to_string().contains("20 s"), "{late}");
        assert_eq!(budget.timed_out(), 1);
        assert_eq!(budget.pending_bytes(), 0);
    }

    #[tokio::test]
    async fn a_failed_append_gives_its_room_back() {
        let budget = AppendBudget::new(10 * MIB, Duration::from_secs(60));
        let failed: Result<(), Error> = budget
            .run(MIB, async {
                Err(Error::Rejected {
                    message: "no".into(),
                })
            })
            .await;
        assert!(failed.is_err());
        assert_eq!(budget.pending_bytes(), 0);
    }
}
