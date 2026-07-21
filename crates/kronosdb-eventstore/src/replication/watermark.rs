//! `WatermarkState` — the quorum-commit ledger.
//!
//! The watermark is the quorum-committed position (next-exclusive): the
//! source of truth for what may be acked, read, streamed, or used as a
//! consistency marker. Each voter (leader included) reports a durable
//! cursor — the position its segment log has fsynced up to — and the
//! watermark is the highest position that a quorum of cursors has passed.
//!
//! On a single node the quorum is one, so the watermark is simply the
//! leader's own durable cursor through the same code path.
//!
//! Waiters come in two kinds, both exact-wakeup (no thundering herd):
//! synchronous callers park on a rendezvous channel; the gRPC append path
//! awaits a oneshot instead, so no thread is pinned for the fsync duration.

use std::collections::{BTreeMap, BTreeSet};
use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, SyncSender, sync_channel};
use std::sync::{Arc, Mutex};

use tokio::sync::oneshot;

use crate::error::Error;

/// Identifies a voter in the replication group. Matches the Raft `NodeId`
/// used by the control plane.
pub type NodeId = u64;

/// Leadership epoch allocated by the metadata control plane.
pub type Epoch = u64;

/// A waiter's wakeup payload: `Ok` when the watermark passed its position,
/// `Err(reason)` when the ledger was poisoned (fsync failure, epoch loss,
/// shutdown) and the write must NOT be treated as durable.
type WakeResult = Result<(), String>;

/// One registered ack waiter. Both kinds receive exactly one verdict.
enum Waiter {
    /// A thread parked on a rendezvous channel (`wait_for`).
    Sync(SyncSender<WakeResult>),
    /// A task awaiting a oneshot (`wait_for_async`). A dropped receiver
    /// (client went away) makes the send a no-op.
    Async(oneshot::Sender<WakeResult>),
}

impl Waiter {
    fn wake(self, verdict: WakeResult) {
        match self {
            Waiter::Sync(tx) => {
                let _ = tx.send(verdict);
            }
            Waiter::Async(tx) => {
                let _ = tx.send(verdict);
            }
        }
    }
}

struct Inner {
    /// Epoch whose acknowledgements are eligible for quorum calculation.
    epoch: Epoch,
    /// Exact voter set for this epoch. Unknown node ids never count.
    voters: BTreeSet<NodeId>,
    /// Majority of `voters`; recomputed atomically with epoch changes.
    quorum: usize,
    /// Durable cursor per voter in the current epoch.
    cursors: BTreeMap<NodeId, u64>,
    /// Waiters keyed by position: released when the watermark passes them.
    ledger: BTreeMap<u64, Vec<Waiter>>,
    /// Latched for the current epoch on fsync failure / epoch loss / shutdown.
    /// `begin_epoch` clears it after aborting old waiters.
    poisoned: Option<String>,
}

/// Quorum-commit state: exact current voters, their durable cursors, the
/// derived watermark, and the ledger of ack waiters.
pub struct WatermarkState {
    /// The watermark (next-exclusive). Lock-free read handle shared with
    /// read paths and event streams; every write happens under `inner` so
    /// ledger drains can never miss a bump.
    watermark: Arc<AtomicU64>,
    inner: Mutex<Inner>,
}

impl WatermarkState {
    /// Creates state for one explicit voter configuration. The watermark
    /// never regresses across later epoch changes.
    pub fn new(epoch: Epoch, voters: impl IntoIterator<Item = NodeId>, initial: u64) -> Self {
        let voters: BTreeSet<_> = voters.into_iter().collect();
        assert!(!voters.is_empty(), "voter set must not be empty");
        let quorum = voters.len() / 2 + 1;
        Self {
            watermark: Arc::new(AtomicU64::new(initial)),
            inner: Mutex::new(Inner {
                epoch,
                voters,
                quorum,
                cursors: BTreeMap::new(),
                ledger: BTreeMap::new(),
                poisoned: None,
            }),
        }
    }

    /// Atomically fences the old epoch and installs its exact voter set.
    /// Every old waiter receives a retryable error; the committed watermark
    /// itself is retained because it may never regress.
    pub fn begin_epoch(
        &self,
        epoch: Epoch,
        voters: impl IntoIterator<Item = NodeId>,
    ) -> Result<(), Error> {
        let voters: BTreeSet<_> = voters.into_iter().collect();
        if voters.is_empty() {
            return Err(Error::Io(io::Error::other("voter set must not be empty")));
        }
        let mut inner = self.inner.lock().unwrap();
        if epoch <= inner.epoch {
            return Err(Error::Io(io::Error::other(format!(
                "epoch {epoch} does not advance current epoch {}",
                inner.epoch
            ))));
        }
        abort_locked(&mut inner, &format!("leadership epoch advanced to {epoch}"));
        inner.epoch = epoch;
        inner.quorum = voters.len() / 2 + 1;
        inner.voters = voters;
        inner.cursors.clear();
        inner.poisoned = None;
        Ok(())
    }

    pub fn epoch(&self) -> Epoch {
        self.inner.lock().unwrap().epoch
    }

    /// The current watermark.
    pub fn get(&self) -> u64 {
        self.watermark.load(Ordering::Acquire)
    }

    /// Shared lock-free read handle, for read paths and event streams.
    pub fn handle(&self) -> Arc<AtomicU64> {
        Arc::clone(&self.watermark)
    }

    /// Records a current voter's durable cursor and recomputes the quorum
    /// commit point. Stale/future epochs and unknown node ids are ignored.
    pub fn advance(&self, node: NodeId, epoch: Epoch, pos: u64) -> Option<u64> {
        let mut inner = self.inner.lock().unwrap();
        if epoch != inner.epoch || !inner.voters.contains(&node) || inner.poisoned.is_some() {
            return None;
        }
        let cursor = inner.cursors.entry(node).or_insert(pos);
        *cursor = (*cursor).max(pos);

        if inner.cursors.len() < inner.quorum {
            return None;
        }
        let mut positions: Vec<u64> = inner.cursors.values().copied().collect();
        positions.sort_unstable_by(|a, b| b.cmp(a));
        let candidate = positions[inner.quorum - 1];
        self.bump_locked(&mut inner, candidate)
    }

    /// Adopts the leader-computed watermark on a follower. The epoch must be
    /// current and the value remains monotonic. Followers never run quorum
    /// math for remote cursors; only the claimed leader does.
    pub fn adopt(&self, epoch: Epoch, pos: u64) -> Option<u64> {
        let mut inner = self.inner.lock().unwrap();
        if epoch != inner.epoch || inner.poisoned.is_some() {
            return None;
        }
        self.bump_locked(&mut inner, pos)
    }

    /// Bumps the watermark to `candidate` if that moves it forward and
    /// drains the ledger prefix. Callers hold the `inner` lock, which is
    /// what orders the bump against waiter registration.
    fn bump_locked(&self, inner: &mut Inner, candidate: u64) -> Option<u64> {
        let current = self.watermark.load(Ordering::Acquire);
        if candidate <= current {
            return None;
        }
        self.watermark.store(candidate, Ordering::Release);

        // Release every waiter at a position the watermark now covers.
        // `split_off` keeps the strictly-greater suffix; the ledger key is
        // the position a waiter needs the watermark to REACH (next-exclusive
        // tail of its write), so a waiter at key K is released when
        // watermark >= K.
        let still_waiting = inner.ledger.split_off(&(candidate + 1));
        let released = std::mem::replace(&mut inner.ledger, still_waiting);
        for waiter in released.into_values().flatten() {
            // A wake only fails if the waiter gave up (dropped); fine.
            waiter.wake(Ok(()));
        }
        Some(candidate)
    }

    /// Blocks until the watermark reaches `pos`. Returns an error — the
    /// write is NOT durable — if the ledger is poisoned before that happens.
    pub fn wait_for(&self, pos: u64) -> Result<(), Error> {
        let rx: Receiver<WakeResult> = {
            let mut inner = self.inner.lock().unwrap();
            if let Some(msg) = &inner.poisoned {
                return Err(poison_error(msg));
            }
            if self.watermark.load(Ordering::Acquire) >= pos {
                return Ok(());
            }
            let (tx, rx) = sync_channel(1);
            inner.ledger.entry(pos).or_default().push(Waiter::Sync(tx));
            rx
        };
        match rx.recv() {
            Ok(Ok(())) => Ok(()),
            Ok(Err(msg)) => Err(poison_error(&msg)),
            // Sender dropped without a verdict — the state was torn down.
            Err(_) => Err(poison_error("watermark state dropped")),
        }
    }

    /// Awaits the watermark reaching `pos` without pinning a thread. Same
    /// verdicts and poisoning semantics as `wait_for`; only where the caller
    /// waits differs.
    pub async fn wait_for_async(&self, pos: u64) -> Result<(), Error> {
        let rx: oneshot::Receiver<WakeResult> = {
            let mut inner = self.inner.lock().unwrap();
            if let Some(msg) = &inner.poisoned {
                return Err(poison_error(msg));
            }
            if self.watermark.load(Ordering::Acquire) >= pos {
                return Ok(());
            }
            let (tx, rx) = oneshot::channel();
            inner.ledger.entry(pos).or_default().push(Waiter::Async(tx));
            rx
        };
        match rx.await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(msg)) => Err(poison_error(&msg)),
            // Sender dropped without a verdict — the state was torn down.
            Err(_) => Err(poison_error("watermark state dropped")),
        }
    }

    /// Poisons the ledger: every current waiter is woken with an error, and
    /// every future `wait_for` fails fast. Used on fsync failure (the write
    /// may never become durable), epoch loss, and shutdown. The first reason
    /// wins; later calls keep it.
    pub fn abort_all(&self, reason: &str) {
        let mut inner = self.inner.lock().unwrap();
        if inner.poisoned.is_none() {
            inner.poisoned = Some(reason.to_string());
        }
        abort_locked(&mut inner, reason);
    }

    /// True once `abort_all` has latched a failure.
    pub fn is_poisoned(&self) -> bool {
        self.inner.lock().unwrap().poisoned.is_some()
    }
}

fn abort_locked(inner: &mut Inner, reason: &str) {
    let msg = inner.poisoned.clone().unwrap_or_else(|| reason.to_string());
    let ledger = std::mem::take(&mut inner.ledger);
    for waiter in ledger.into_values().flatten() {
        waiter.wake(Err(msg.clone()));
    }
}

fn poison_error(msg: &str) -> Error {
    Error::Io(io::Error::other(format!(
        "append not committed (watermark aborted): {msg}"
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn quorum_of_one_advances_on_own_cursor() {
        let wm = WatermarkState::new(0, [0], 0);
        assert_eq!(wm.get(), 0);
        assert_eq!(wm.advance(0, 0, 5), Some(5));
        assert_eq!(wm.get(), 5);
        // Regression is ignored.
        assert_eq!(wm.advance(0, 0, 3), None);
        assert_eq!(wm.get(), 5);
    }

    #[test]
    fn quorum_of_two_needs_second_cursor() {
        let wm = WatermarkState::new(0, [0, 1, 2], 0);
        // Leader alone can't commit.
        assert_eq!(wm.advance(0, 0, 10), None);
        assert_eq!(wm.get(), 0);
        // A slow follower commits up to ITS cursor (2nd-highest).
        assert_eq!(wm.advance(1, 0, 4), Some(4));
        assert_eq!(wm.get(), 4);
        // Third cursor beyond the leader: watermark = 2nd-highest = 10.
        assert_eq!(wm.advance(2, 0, 12), Some(10));
        assert_eq!(wm.get(), 10);
    }

    #[test]
    fn stale_epoch_cursor_is_ignored() {
        let wm = WatermarkState::new(5, [0], 0);
        assert_eq!(wm.advance(0, 5, 10), Some(10));
        // Reports from any other epoch must not disturb the cursor.
        assert_eq!(wm.advance(0, 4, 99), None);
        assert_eq!(wm.advance(0, 6, 99), None);
        assert_eq!(wm.get(), 10);
        // A newer epoch must be explicitly installed, which fences old acks.
        wm.begin_epoch(6, [0]).unwrap();
        assert_eq!(wm.advance(0, 6, 11), Some(11));
    }

    #[test]
    fn waiter_released_when_watermark_passes() {
        let wm = Arc::new(WatermarkState::new(1, [0], 0));
        let waiter = {
            let wm = Arc::clone(&wm);
            std::thread::spawn(move || wm.wait_for(3))
        };
        std::thread::sleep(Duration::from_millis(20));
        // Not enough yet.
        wm.advance(0, 1, 2);
        std::thread::sleep(Duration::from_millis(20));
        assert!(!waiter.is_finished());
        // Passes the waiter's position.
        wm.advance(0, 1, 3);
        assert!(waiter.join().unwrap().is_ok());
    }

    #[test]
    fn wait_for_already_committed_returns_immediately() {
        let wm = WatermarkState::new(0, [0], 7);
        assert!(wm.wait_for(7).is_ok());
        assert!(wm.wait_for(0).is_ok());
    }

    #[test]
    fn unknown_voter_never_counts_toward_quorum() {
        let wm = WatermarkState::new(3, [1, 2, 3], 0);
        assert_eq!(wm.advance(1, 3, 10), None);
        assert_eq!(wm.advance(99, 3, 10), None);
        assert_eq!(wm.get(), 0);
        assert_eq!(wm.advance(2, 3, 7), Some(7));
    }

    #[test]
    fn epoch_change_aborts_old_waiters_without_regressing_watermark() {
        let wm = Arc::new(WatermarkState::new(3, [1, 2, 3], 5));
        let waiter = {
            let wm = Arc::clone(&wm);
            std::thread::spawn(move || wm.wait_for(9))
        };
        std::thread::sleep(Duration::from_millis(20));
        wm.begin_epoch(4, [1, 2, 3]).unwrap();

        assert!(waiter.join().unwrap().is_err());
        assert_eq!(wm.get(), 5);
        assert_eq!(wm.advance(1, 3, 99), None);
        assert_eq!(wm.advance(1, 4, 8), None);
        assert_eq!(wm.advance(2, 4, 8), Some(8));
    }

    #[test]
    fn abort_wakes_waiters_with_error_and_poisons() {
        let wm = Arc::new(WatermarkState::new(1, [0], 0));
        let waiter = {
            let wm = Arc::clone(&wm);
            std::thread::spawn(move || wm.wait_for(10))
        };
        std::thread::sleep(Duration::from_millis(20));
        wm.abort_all("simulated fsync failure");

        let err = waiter
            .join()
            .unwrap()
            .expect_err("waiter must NOT be told the write is durable");
        assert!(err.to_string().contains("simulated fsync failure"), "{err}");

        // Poisoned: future waits fail fast, even for committed positions.
        assert!(wm.is_poisoned());
        assert!(wm.wait_for(0).is_err());
    }
}
