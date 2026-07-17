//! Wave-to-frame dispatch for native segment replication.
//!
//! The sync thread seals a wave under the writer lock, hands a descriptor to
//! this dispatcher, then immediately starts its local fdatasync. A dedicated
//! dispatcher thread preads the bounded byte ranges from the page cache and
//! publishes them to Tail sessions, so replication and leader durability run
//! concurrently.

use std::fs::File;
use std::io;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{SyncSender, TrySendError, sync_channel};

use bytes::Bytes;
use tokio::sync::broadcast;

/// Maximum raw record bytes waiting between the sync thread and dispatcher.
/// The channel itself is descriptor-bounded; follower sessions apply their
/// own 64 MiB unacked window.
const DISPATCH_QUEUE_CAPACITY: usize = 128;
const LIVE_FRAME_CAPACITY: usize = 256;

/// A contiguous byte range from one segment covered by a sealed wave.
#[derive(Debug, Clone)]
pub struct WaveSlice {
    pub path: PathBuf,
    pub segment_base: u64,
    pub byte_start: u64,
    pub byte_end: u64,
    /// Follower cursor expected before applying this slice.
    pub first_position: u64,
    /// Follower cursor after applying this slice.
    pub next_position: u64,
}

/// Immutable descriptor captured at the writer-lock seal barrier.
#[derive(Debug, Clone)]
pub struct WaveDescriptor {
    pub wave_id: u64,
    pub epoch: u64,
    pub previous_segment_base: u64,
    pub first_position: u64,
    pub next_position: u64,
    pub slices: Vec<WaveSlice>,
}

/// Live dispatcher output consumed by leader-side Tail sessions.
#[derive(Debug, Clone)]
pub enum LiveFrame {
    Records {
        epoch: u64,
        segment_base: u64,
        byte_start: u64,
        byte_end: u64,
        first_position: u64,
        next_position: u64,
        data: Bytes,
        /// Monotonic count of raw record bytes published on this engine.
        stream_bytes_end: u64,
    },
    Rotate {
        epoch: u64,
        new_segment_base: u64,
    },
    /// Forces every active Tail session to reconnect from its durable cursor.
    /// Emitted when a live descriptor cannot be sourced or queued.
    Reset {
        epoch: u64,
    },
}

/// Non-blocking handle owned by an engine. With no Tail subscribers, wave
/// publication is a single atomic subscriber-count check and no descriptor is
/// queued or bytes read — the native path has no replication work until a
/// follower subscribes.
pub struct WavePublisher {
    descriptor_tx: SyncSender<WaveDescriptor>,
    live_tx: broadcast::Sender<LiveFrame>,
    stream_bytes: Arc<AtomicU64>,
}

impl WavePublisher {
    pub fn new() -> Arc<Self> {
        let (descriptor_tx, descriptor_rx) = sync_channel(DISPATCH_QUEUE_CAPACITY);
        let (live_tx, _) = broadcast::channel(LIVE_FRAME_CAPACITY);
        let stream_bytes = Arc::new(AtomicU64::new(0));
        let publisher = Arc::new(Self {
            descriptor_tx,
            live_tx: live_tx.clone(),
            stream_bytes: Arc::clone(&stream_bytes),
        });

        std::thread::Builder::new()
            .name("kronosdb-replication-dispatch".into())
            .spawn(move || {
                while let Ok(descriptor) = descriptor_rx.recv() {
                    if live_tx.receiver_count() == 0 {
                        continue;
                    }
                    let epoch = descriptor.epoch;
                    if let Err(error) = dispatch_descriptor(&live_tx, &stream_bytes, descriptor) {
                        tracing::error!(%error, "failed to source sealed wave for replication");
                        let _ = live_tx.send(LiveFrame::Reset { epoch });
                    }
                }
            })
            .expect("spawn replication dispatcher");

        publisher
    }

    pub fn subscribe(&self) -> broadcast::Receiver<LiveFrame> {
        self.live_tx.subscribe()
    }

    pub fn has_subscribers(&self) -> bool {
        self.live_tx.receiver_count() > 0
    }

    /// Hands a sealed wave to the dispatcher without blocking the local fsync.
    /// A full queue drops this live hint; Tail sessions detect broadcast lag,
    /// reconnect from their durable cursor, and use catch-up sourcing. The
    /// durable segment log remains the source of truth.
    pub fn try_publish(&self, descriptor: WaveDescriptor) {
        if !self.has_subscribers() || descriptor.slices.is_empty() {
            return;
        }
        match self.descriptor_tx.try_send(descriptor) {
            Ok(()) => {}
            Err(TrySendError::Full(descriptor)) => {
                tracing::warn!(
                    wave = descriptor.wave_id,
                    "replication dispatcher queue full; resetting Tail sessions"
                );
                let _ = self.live_tx.send(LiveFrame::Reset {
                    epoch: descriptor.epoch,
                });
            }
            Err(TrySendError::Disconnected(descriptor)) => {
                tracing::error!("replication dispatcher stopped; resetting live Tail sessions");
                let _ = self.live_tx.send(LiveFrame::Reset {
                    epoch: descriptor.epoch,
                });
            }
        }
    }

    pub fn stream_bytes(&self) -> u64 {
        self.stream_bytes.load(Ordering::Acquire)
    }
}

fn dispatch_descriptor(
    tx: &broadcast::Sender<LiveFrame>,
    stream_bytes: &AtomicU64,
    descriptor: WaveDescriptor,
) -> Result<(), io::Error> {
    let mut previous_base = Some(descriptor.previous_segment_base);
    for slice in descriptor.slices {
        if previous_base != Some(slice.segment_base) {
            let _ = tx.send(LiveFrame::Rotate {
                epoch: descriptor.epoch,
                new_segment_base: slice.segment_base,
            });
        }
        previous_base = Some(slice.segment_base);

        if slice.byte_end <= slice.byte_start {
            continue;
        }
        let len = usize::try_from(slice.byte_end - slice.byte_start)
            .map_err(|_| io::Error::other("replication wave slice exceeds address space"))?;
        let mut data = vec![0u8; len];
        let file = File::open(&slice.path)?;
        read_exact_at(&file, &mut data, slice.byte_start)?;
        let end = stream_bytes.fetch_add(len as u64, Ordering::AcqRel) + len as u64;
        let _ = tx.send(LiveFrame::Records {
            epoch: descriptor.epoch,
            segment_base: slice.segment_base,
            byte_start: slice.byte_start,
            byte_end: slice.byte_end,
            first_position: slice.first_position,
            next_position: slice.next_position,
            data: Bytes::from(data),
            stream_bytes_end: end,
        });
    }
    Ok(())
}

#[cfg(unix)]
fn read_exact_at(file: &File, mut buf: &mut [u8], mut offset: u64) -> io::Result<()> {
    use std::os::unix::fs::FileExt;
    while !buf.is_empty() {
        let read = file.read_at(buf, offset)?;
        if read == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "segment ended inside sealed wave range",
            ));
        }
        offset += read as u64;
        buf = &mut buf[read..];
    }
    Ok(())
}

#[cfg(windows)]
fn read_exact_at(file: &File, mut buf: &mut [u8], mut offset: u64) -> io::Result<()> {
    use std::os::windows::fs::FileExt;
    while !buf.is_empty() {
        let read = file.seek_read(buf, offset)?;
        if read == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "segment ended inside sealed wave range",
            ));
        }
        offset += read as u64;
        buf = &mut buf[read..];
    }
    Ok(())
}
