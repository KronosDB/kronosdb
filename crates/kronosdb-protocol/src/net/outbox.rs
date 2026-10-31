//! What a server connection's task shares with the handlers that answer on
//! it: a mailbox they push frames and finished stream ids into, and the
//! buffer the connection writes from.
//!
//! The connection is one task that reads, runs handlers that can answer at
//! once, and writes. A handler answering from inside that task must not wake
//! it (it is running), and one answering from elsewhere wakes it only if it
//! is parked, so the mailbox is a mutexed queue with a waker that exists only
//! while the task waits, not a channel whose every send signals its receiver.

use std::collections::VecDeque;
use std::io::{self, IoSlice};
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard};
use std::task::{Context, Poll, Waker, ready};

use bytes::{Buf, Bytes, BytesMut};
use tokio::io::AsyncWrite;

use super::{NetError, Out};

/// Frames a connection queues before the handlers producing them wait.
pub(crate) const OUTBOUND_CAPACITY: usize = 1024;

pub(crate) struct Outbox {
    state: Mutex<State>,
}

#[derive(Default)]
struct State {
    queue: VecDeque<Out>,
    /// Streams whose handler ended while it ran apart from the connection.
    finished: Vec<u32>,
    /// The connection task, while it waits for the mailbox.
    rx_waker: Option<Waker>,
    /// Handlers waiting for room in a full queue.
    tx_wakers: Vec<Waker>,
    /// Live [`OutTx`] handles.
    senders: usize,
    /// The connection is closing and waits for the senders to go.
    closing: bool,
    /// The connection is gone: pushes fail.
    closed: bool,
}

impl Outbox {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(State::default()),
        })
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        // Nothing under the lock can panic; a poisoned lock is still sound.
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// A handle handlers answer through.
    pub(crate) fn sender(self: &Arc<Self>) -> OutTx {
        self.lock().senders += 1;
        OutTx(Arc::clone(self))
    }

    /// Queues a frame the connection itself sends (a pong, a refusal). These
    /// ignore the capacity: the connection stops reading while it is behind.
    pub(crate) fn push_now(&self, out: Out) {
        let mut s = self.lock();
        if !s.closed {
            s.queue.push_back(out);
        }
    }

    /// Takes the queued frames into `queue` (when it is empty, so what was
    /// taken earlier is written first) and the finished ids into `finished`.
    /// The connection is running, so it needs no wake-up for what arrives
    /// until it waits again.
    pub(crate) fn take(&self, queue: &mut VecDeque<Out>, finished: &mut Vec<u32>) {
        let mut s = self.lock();
        s.rx_waker = None;
        if queue.is_empty() && !s.queue.is_empty() {
            let was_full = s.queue.len() >= OUTBOUND_CAPACITY;
            std::mem::swap(queue, &mut s.queue);
            if was_full {
                for w in s.tx_wakers.drain(..) {
                    w.wake();
                }
            }
        }
        if !s.finished.is_empty() {
            finished.append(&mut s.finished);
        }
    }

    /// Drops what a cancelled stream queued and has not been taken, except
    /// the frame that ends it.
    pub(crate) fn discard(&self, stream: u32) {
        let mut s = self.lock();
        let was_full = s.queue.len() >= OUTBOUND_CAPACITY;
        s.queue.retain(|out| !out.cancelled_with(stream));
        if was_full && s.queue.len() < OUTBOUND_CAPACITY {
            for w in s.tx_wakers.drain(..) {
                w.wake();
            }
        }
    }

    /// Ready when [`take`](Self::take) has something; otherwise registers the
    /// task to be woken. With `closing`, also ready once no sender remains.
    pub(crate) fn poll_ready(&self, cx: &mut Context<'_>, closing: bool) -> Poll<()> {
        let mut s = self.lock();
        s.closing = closing;
        if !s.queue.is_empty() || !s.finished.is_empty() || (closing && s.senders == 0) {
            return Poll::Ready(());
        }
        if !s.rx_waker.as_ref().is_some_and(|w| w.will_wake(cx.waker())) {
            s.rx_waker = Some(cx.waker().clone());
        }
        Poll::Pending
    }

    /// Whether every sender is gone and nothing is left to take.
    pub(crate) fn is_drained(&self) -> bool {
        let s = self.lock();
        s.senders == 0 && s.queue.is_empty()
    }

    /// Fails every later push and releases handlers waiting for room.
    pub(crate) fn close(&self) {
        let mut s = self.lock();
        s.closed = true;
        s.queue.clear();
        for w in s.tx_wakers.drain(..) {
            w.wake();
        }
    }
}

/// A handler's way to answer. Counted, so the connection knows when the last
/// handler is done with it.
pub(crate) struct OutTx(Arc<Outbox>);

impl Clone for OutTx {
    fn clone(&self) -> Self {
        self.0.sender()
    }
}

impl Drop for OutTx {
    fn drop(&mut self) {
        let waker = {
            let mut s = self.0.lock();
            s.senders -= 1;
            if s.senders == 0 && s.closing {
                s.rx_waker.take()
            } else {
                None
            }
        };
        if let Some(w) = waker {
            w.wake();
        }
    }
}

impl OutTx {
    /// Queues a frame, waiting while the queue is full.
    pub(crate) async fn push(&self, out: Out) -> Result<(), NetError> {
        let mut out = Some(out);
        std::future::poll_fn(|cx| {
            let mut s = self.0.lock();
            if s.closed {
                return Poll::Ready(Err(NetError::Closed));
            }
            if s.queue.len() >= OUTBOUND_CAPACITY {
                if !s.tx_wakers.iter().any(|w| w.will_wake(cx.waker())) {
                    s.tx_wakers.push(cx.waker().clone());
                }
                return Poll::Pending;
            }
            s.queue
                .push_back(out.take().expect("polled after completing"));
            let waker = s.rx_waker.take();
            drop(s);
            if let Some(w) = waker {
                w.wake();
            }
            Poll::Ready(Ok(()))
        })
        .await
    }

    /// Tells the connection a stream's handler ended.
    pub(crate) fn finished(&self, id: u32) {
        let waker = {
            let mut s = self.0.lock();
            s.finished.push(id);
            s.rx_waker.take()
        };
        if let Some(w) = waker {
            w.wake();
        }
    }
}

/// Frames at least this big are written as they are instead of being copied
/// into the coalescing buffer.
const COPY_LIMIT: usize = 16 * 1024;
/// Slices handed to one vectored write.
const MAX_IOV: usize = 16;
/// A coalescing buffer that grew past this is replaced once drained.
const KEEP_CAPACITY: usize = 4 * 1024 * 1024;

enum Span {
    /// The next `len` bytes of the coalescing buffer.
    Buf(usize),
    /// A big frame, written without a copy.
    Big(Bytes),
}

/// Everything a connection has to write: small frames copied back to back
/// into one buffer, big ones kept apart, sent with as few writes as possible.
pub(crate) struct WriteBuf {
    buf: BytesMut,
    /// How much of `buf` is already written.
    done: usize,
    spans: VecDeque<Span>,
    /// Bytes added and not yet written.
    pending: usize,
    flush_due: bool,
}

impl WriteBuf {
    pub(crate) fn new() -> Self {
        Self {
            buf: BytesMut::with_capacity(16 * 1024),
            done: 0,
            spans: VecDeque::new(),
            pending: 0,
            flush_due: false,
        }
    }

    /// Bytes waiting to be written.
    pub(crate) fn pending(&self) -> usize {
        self.pending
    }

    pub(crate) fn has_pending(&self) -> bool {
        !self.spans.is_empty() || self.flush_due
    }

    pub(crate) fn add(&mut self, out: Out) {
        if self.spans.is_empty() {
            self.buf.clear();
            self.done = 0;
            if self.buf.capacity() > KEEP_CAPACITY {
                self.buf = BytesMut::with_capacity(16 * 1024);
            }
        }
        self.pending += out.len();
        let big = out.len() >= COPY_LIMIT;
        match out {
            Out::Frame(bytes) if big => self.spans.push_back(Span::Big(bytes)),
            Out::Frame(bytes) => self.small(&bytes),
            Out::Shared { header, body } => {
                self.small(&header);
                if big {
                    self.spans.push_back(Span::Big(body));
                } else {
                    self.small(&body);
                }
            }
        }
    }

    fn small(&mut self, bytes: &[u8]) {
        self.buf.extend_from_slice(bytes);
        match self.spans.back_mut() {
            Some(Span::Buf(len)) => *len += bytes.len(),
            _ => self.spans.push_back(Span::Buf(bytes.len())),
        }
    }

    fn advance(&mut self, mut n: usize) {
        self.pending -= n;
        while n > 0 {
            match self.spans.front_mut() {
                Some(Span::Buf(len)) if n >= *len => {
                    n -= *len;
                    self.done += *len;
                    self.spans.pop_front();
                }
                Some(Span::Buf(len)) => {
                    *len -= n;
                    self.done += n;
                    n = 0;
                }
                Some(Span::Big(bytes)) if n >= bytes.len() => {
                    n -= bytes.len();
                    self.spans.pop_front();
                }
                Some(Span::Big(bytes)) => {
                    bytes.advance(n);
                    n = 0;
                }
                None => unreachable!("wrote more than was queued"),
            }
        }
    }

    /// Writes what is queued, then flushes. Ready once all of it is out.
    pub(crate) fn poll_write<W: AsyncWrite + Unpin>(
        &mut self,
        cx: &mut Context<'_>,
        w: &mut W,
    ) -> Poll<io::Result<()>> {
        while !self.spans.is_empty() {
            let written = {
                let mut slices = [IoSlice::new(&[]); MAX_IOV];
                let mut count = 0;
                let mut at = self.done;
                for span in self.spans.iter().take(MAX_IOV) {
                    slices[count] = match span {
                        Span::Buf(len) => {
                            let slice = IoSlice::new(&self.buf[at..at + len]);
                            at += len;
                            slice
                        }
                        Span::Big(bytes) => IoSlice::new(bytes),
                    };
                    count += 1;
                }
                ready!(Pin::new(&mut *w).poll_write_vectored(cx, &slices[..count]))?
            };
            if written == 0 {
                return Poll::Ready(Err(io::ErrorKind::WriteZero.into()));
            }
            self.flush_due = true;
            self.advance(written);
        }
        if self.flush_due {
            ready!(Pin::new(w).poll_flush(cx))?;
            self.flush_due = false;
        }
        Poll::Ready(Ok(()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncReadExt;

    fn frame(len: usize, fill: u8) -> Out {
        Out::Frame(Bytes::from(vec![fill; len]))
    }

    #[tokio::test]
    async fn small_and_big_frames_arrive_in_order() {
        let (mut a, mut b) = tokio::io::duplex(1 << 20);
        let mut wbuf = WriteBuf::new();
        let mut expected = Vec::new();
        for (len, fill) in [
            (10, 1u8),
            (20, 2),
            (40_000, 3),
            (5, 4),
            (100_000, 5),
            (7, 6),
        ] {
            wbuf.add(frame(len, fill));
            expected.extend(std::iter::repeat_n(fill, len));
        }
        wbuf.add(Out::Shared {
            header: [9; crate::codec::FRAME_HEADER_LEN],
            body: Bytes::from(vec![7; 50_000]),
        });
        expected.extend([9; crate::codec::FRAME_HEADER_LEN]);
        expected.extend(vec![7; 50_000]);
        assert_eq!(wbuf.pending(), expected.len());
        std::future::poll_fn(|cx| wbuf.poll_write(cx, &mut a))
            .await
            .unwrap();
        assert_eq!(wbuf.pending(), 0);
        assert!(!wbuf.has_pending());
        drop(a);
        let mut got = Vec::new();
        b.read_to_end(&mut got).await.unwrap();
        assert!(got == expected, "bytes differ or are reordered");
    }

    #[tokio::test]
    async fn partial_writes_resume_where_they_stopped() {
        // A pipe smaller than the data forces short writes mid-frame.
        let (mut a, mut b) = tokio::io::duplex(1000);
        let mut wbuf = WriteBuf::new();
        let mut expected = Vec::new();
        for i in 0..20u8 {
            let len = if i % 3 == 0 { 20_000 } else { 300 };
            wbuf.add(frame(len, i));
            expected.extend(std::iter::repeat_n(i, len));
        }
        let reader = tokio::spawn(async move {
            let mut got = Vec::new();
            b.read_to_end(&mut got).await.unwrap();
            got
        });
        std::future::poll_fn(|cx| wbuf.poll_write(cx, &mut a))
            .await
            .unwrap();
        drop(a);
        assert!(reader.await.unwrap() == expected);
    }

    #[tokio::test]
    async fn a_full_mailbox_holds_senders_until_taken() {
        let outbox = Outbox::new();
        let tx = outbox.sender();
        for _ in 0..OUTBOUND_CAPACITY {
            tx.push(frame(1, 0)).await.unwrap();
        }
        let blocked = tokio::spawn({
            let tx = tx.clone();
            async move { tx.push(frame(1, 1)).await }
        });
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        assert!(!blocked.is_finished());
        let (mut queue, mut finished) = (VecDeque::new(), Vec::new());
        outbox.take(&mut queue, &mut finished);
        assert_eq!(queue.len(), OUTBOUND_CAPACITY);
        blocked.await.unwrap().unwrap();
        outbox.close();
        assert!(matches!(tx.push(frame(1, 2)).await, Err(NetError::Closed)));
    }

    #[test]
    fn a_cancel_discards_its_streams_frames_but_the_last() {
        use crate::codec::FLAG_END;
        use crate::generated::{Frame, Progress, RequestFailed};
        let progress = |stream, flags| {
            Out::Frame(
                super::super::encode(stream, flags, &Frame::Progress(Progress { position: 1 }))
                    .unwrap(),
            )
        };
        let outbox = Outbox::new();
        outbox.push_now(progress(1, 0));
        outbox.push_now(progress(2, 0));
        outbox.push_now(progress(1, 0));
        let failed = RequestFailed {
            code: crate::generated::ErrorCode::CANCELLED,
            message: "",
        };
        outbox.push_now(Out::Frame(
            super::super::encode(1, FLAG_END, &Frame::RequestFailed(failed)).unwrap(),
        ));
        outbox.discard(1);
        let (mut queue, mut finished) = (VecDeque::new(), Vec::new());
        outbox.take(&mut queue, &mut finished);
        let left: Vec<(u32, bool)> = queue
            .iter()
            .map(|out| {
                let h = out.header();
                (
                    u32::from_le_bytes(h[7..11].try_into().unwrap()),
                    h[6] & FLAG_END != 0,
                )
            })
            .collect();
        assert_eq!(left, vec![(2, false), (1, true)]);
    }

    #[tokio::test]
    async fn the_connection_wakes_for_pushes_and_for_the_last_sender() {
        let outbox = Outbox::new();
        let tx = outbox.sender();
        let waiter = tokio::spawn({
            let outbox = Arc::clone(&outbox);
            async move { std::future::poll_fn(|cx| outbox.poll_ready(cx, true)).await }
        });
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        assert!(!waiter.is_finished());
        tx.finished(7);
        waiter.await.unwrap();
        let (mut queue, mut finished) = (VecDeque::new(), Vec::new());
        outbox.take(&mut queue, &mut finished);
        assert_eq!(finished, vec![7]);

        let waiter = tokio::spawn({
            let outbox = Arc::clone(&outbox);
            async move { std::future::poll_fn(|cx| outbox.poll_ready(cx, true)).await }
        });
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        assert!(!waiter.is_finished());
        drop(tx);
        waiter.await.unwrap();
        assert!(outbox.is_drained());
    }
}
