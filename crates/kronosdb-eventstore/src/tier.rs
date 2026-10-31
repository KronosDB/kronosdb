//! Tiered log storage, stage 1: the backup uploader (ADR-0002).
//!
//! The claimed leader periodically ships sealed, watermark-covered segments
//! to an object store (`file://`, `s3://`, ...) together with a per-context
//! JSON manifest. Segments below the watermark are immutable and
//! byte-identical on every node, so uploads are idempotent and the archive
//! has exactly one canonical byte representation.
//!
//! The manifest is the commit point: it is written only after every segment
//! it references uploaded and passed a size check. A crashed pass leaves at
//! worst orphan segment objects that the next pass re-uploads or adopts.
//!
//! Strictly write-only: no eviction, no read-path changes (stages 3–4).

use std::collections::BTreeSet;
use std::time::Duration;

use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload, WriteMultipart};
use serde::{Deserialize, Serialize};
use tokio::io::AsyncReadExt;

use crate::error::Error;
use crate::segment::sealed_index::SealedIndex;
use crate::store::{ArchivableSegment, EventStoreEngine};

/// Upload chunk size for segment multipart uploads.
const UPLOAD_CHUNK: usize = 8 * 1024 * 1024;

#[derive(Debug, Clone)]
pub struct TierConfig {
    /// Object store URL, e.g. `file:///backups/kronosdb` or `s3://bucket/prefix`.
    /// Cloud credentials come from the environment (standard SDK variables).
    pub url: String,
    /// Delay between backup passes on the claimed leader.
    pub interval: Duration,
}

/// Per-context archive manifest. Segments absent from the manifest do not
/// exist for readers, whatever objects may be present.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Manifest {
    pub context: String,
    /// Quorum watermark observed when this manifest revision was written.
    pub watermark: u64,
    pub segments: Vec<ManifestSegment>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestSegment {
    pub base: u64,
    /// End position (exclusive).
    pub end: u64,
    /// Size of the `.seg` object in bytes.
    pub size: u64,
    /// blake3 of the `.seg` bytes, computed while uploading.
    pub blake3: String,
    /// Companion objects uploaded alongside (the "idx").
    pub companions: Vec<String>,
}

#[derive(Debug, Default)]
pub struct BackupPassReport {
    pub uploaded: usize,
    pub already_archived: usize,
}

pub struct Archiver {
    store: Box<dyn ObjectStore>,
    prefix: ObjectPath,
}

impl Archiver {
    /// Builds an archiver from an object-store URL. `file://` and `memory://`
    /// need no credentials; `s3://` reads the standard AWS environment.
    pub fn from_url(raw_url: &str) -> Result<Self, Error> {
        let url = url::Url::parse(raw_url).map_err(|error| Error::Rejected {
            message: format!("invalid backup url {raw_url}: {error}"),
        })?;
        let (store, prefix) = object_store::parse_url(&url).map_err(|error| Error::Rejected {
            message: format!("unsupported backup url {raw_url}: {error}"),
        })?;
        Ok(Self { store, prefix })
    }

    fn context_path(&self, context: &str, parts: &[&str]) -> ObjectPath {
        let mut path = self.prefix.clone().join(context);
        for part in parts {
            path = path.join(*part);
        }
        path
    }

    pub async fn load_manifest(&self, context: &str) -> Result<Manifest, Error> {
        let path = self.context_path(context, &["manifest.json"]);
        match self.store.get(&path).await {
            Ok(result) => {
                let bytes = result.bytes().await.map_err(tier_err)?;
                serde_json::from_slice(&bytes).map_err(|error| Error::Corrupted {
                    message: format!("corrupt backup manifest for context {context}: {error}"),
                })
            }
            Err(object_store::Error::NotFound { .. }) => Ok(Manifest::default()),
            Err(error) => Err(tier_err(error)),
        }
    }

    /// Uploads every archivable segment absent from the manifest, then
    /// commits the updated manifest. Idempotent; safe to re-run after any
    /// crash or leader change.
    pub async fn run_backup_pass(
        &self,
        context: &str,
        engine: &EventStoreEngine,
    ) -> Result<BackupPassReport, Error> {
        let mut manifest = self.load_manifest(context).await?;
        let archived: BTreeSet<u64> = manifest.segments.iter().map(|s| s.base).collect();

        let mut report = BackupPassReport::default();
        for segment in engine.archivable_segments() {
            if archived.contains(&segment.base) {
                report.already_archived += 1;
                continue;
            }
            // Segments go up in log order, so the manifest stays a prefix
            // of the log: one whose seal has not reached the disk yet holds
            // back the ones after it until the next pass.
            let Some(entry) = self.upload_segment(context, &segment).await? else {
                break;
            };
            manifest.segments.push(entry);
            report.uploaded += 1;
        }

        if report.uploaded > 0 {
            manifest.segments.sort_by_key(|s| s.base);
            manifest.context = context.to_string();
            manifest.watermark = engine.head().0;
            let body = serde_json::to_vec_pretty(&manifest).map_err(|error| Error::Internal {
                message: format!("encode backup manifest: {error}"),
            })?;
            self.store
                .put(
                    &self.context_path(context, &["manifest.json"]),
                    PutPayload::from(body),
                )
                .await
                .map_err(tier_err)?;
        }
        Ok(report)
    }

    /// Streams one final segment's data bytes (plus its `.idx`) into the
    /// object store, hashing while uploading, and verifies the stored
    /// object's size before the caller commits the manifest. `None` when the
    /// segment's `.idx` is not on disk yet: its seal is still in flight.
    async fn upload_segment(
        &self,
        context: &str,
        segment: &ArchivableSegment,
    ) -> Result<Option<ManifestSegment>, Error> {
        let file_name = segment
            .path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| Error::Internal {
                message: format!("segment path has no name: {}", segment.path.display()),
            })?;
        let idx_path = segment.path.with_extension("idx");
        let idx_bytes = match tokio::fs::read(&idx_path).await {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        // The segment's data ends where its locator says. The file may run
        // on past that (preallocated space a crash kept from being trimmed),
        // and those bytes are not part of the log.
        let data_len = SealedIndex::open(&idx_path, segment.base)?.data_len();

        let object = self.context_path(context, &["segments", file_name]);
        let file = tokio::fs::File::open(&segment.path).await?;
        let file_len = file.metadata().await?.len();
        if file_len < data_len {
            return Err(Error::Corrupted {
                message: format!(
                    "segment {} is {file_len} bytes, but its index says its data runs to \
                     {data_len}",
                    segment.path.display()
                ),
            });
        }
        let mut data = file.take(data_len);
        let upload = self.store.put_multipart(&object).await.map_err(tier_err)?;
        let mut writer = WriteMultipart::new(upload);
        let mut hasher = blake3::Hasher::new();
        let mut total: u64 = 0;
        let mut chunk = vec![0u8; UPLOAD_CHUNK];
        loop {
            let read = data.read(&mut chunk).await?;
            if read == 0 {
                break;
            }
            hasher.update(&chunk[..read]);
            writer.write(&chunk[..read]);
            total += read as u64;
        }
        writer.finish().await.map_err(tier_err)?;
        if total != data_len {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                format!(
                    "segment {} ended at {total} bytes while uploading its {data_len}",
                    segment.path.display()
                ),
            )));
        }

        // Verify what the store now holds before the manifest may reference it.
        let head = self.store.head(&object).await.map_err(tier_err)?;
        if head.size != total {
            return Err(Error::Corrupted {
                message: format!(
                    "backup verification failed for {object}: uploaded {total} bytes, \
                     store reports {}",
                    head.size
                ),
            });
        }

        let idx_name = format!("{}.idx", file_name.trim_end_matches(".seg"));
        self.store
            .put(
                &self.context_path(context, &["segments", &idx_name]),
                PutPayload::from(idx_bytes),
            )
            .await
            .map_err(tier_err)?;

        Ok(Some(ManifestSegment {
            base: segment.base,
            end: segment.end,
            size: total,
            blake3: hasher.finalize().to_hex().to_string(),
            companions: vec!["idx".to_string()],
        }))
    }
}

fn tier_err(error: object_store::Error) -> Error {
    Error::Io(std::io::Error::other(format!("object store: {error}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::append::AppendRequest;
    use crate::event::{AppendEvent, Tag};
    use crate::store::EventStoreEngine;
    use std::sync::Arc;

    const SEGMENT_SIZE: u64 = 16 * 1024;

    fn make_event(index: usize) -> AppendEvent {
        AppendEvent {
            identifier: format!("id-{index}"),
            name: "TierTested".into(),
            version: "1.0".into(),
            timestamp: 1712345678000,
            payload: vec![b'x'; 512],
            metadata: vec![],
            tags: vec![Tag::from_str("agg", &format!("agg-{}", index % 4))],
        }
    }

    fn seal_segments(engine: &EventStoreEngine, minimum_sealed: usize) {
        let mut index = 0;
        while engine.archivable_segments().len() < minimum_sealed {
            engine
                .append(AppendRequest {
                    condition: None,
                    events: (0..8)
                        .map(|_| {
                            index += 1;
                            make_event(index)
                        })
                        .collect(),
                })
                .expect("append");
            assert!(index < 100_000, "segments never sealed");
        }
        engine.wait_for_seals();
    }

    async fn archiver_for(dir: &std::path::Path) -> Archiver {
        let url = url::Url::from_directory_path(dir).unwrap();
        Archiver::from_url(url.as_str()).expect("archiver")
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn backup_pass_uploads_sealed_watermark_covered_segments() {
        let data = tempfile::tempdir().expect("data dir");
        let tier = tempfile::tempdir().expect("tier dir");
        let engine = Arc::new(
            EventStoreEngine::create_with_options(data.path(), SEGMENT_SIZE).expect("engine"),
        );
        {
            let engine = Arc::clone(&engine);
            tokio::task::spawn_blocking(move || seal_segments(&engine, 2))
                .await
                .expect("seal");
        }

        let archiver = archiver_for(tier.path()).await;
        let report = archiver
            .run_backup_pass("default", &engine)
            .await
            .expect("backup pass");
        assert!(report.uploaded >= 2, "expected uploads, got {report:?}");
        assert_eq!(report.already_archived, 0);

        // Manifest matches the local files byte-for-byte.
        let manifest = archiver.load_manifest("default").await.expect("manifest");
        assert_eq!(manifest.segments.len(), report.uploaded);
        for entry in &manifest.segments {
            assert!(entry.end <= engine.head().0, "archived past the watermark");
            let local = crate::segment::segment_path(data.path(), entry.base);
            let bytes = std::fs::read(&local).expect("read local segment");
            assert_eq!(entry.size, bytes.len() as u64);
            assert_eq!(entry.blake3, blake3::hash(&bytes).to_hex().to_string());
            // The uploaded object holds the exact same bytes.
            let object = tier
                .path()
                .join("default")
                .join("segments")
                .join(format!("{:020}.seg", entry.base));
            assert_eq!(std::fs::read(&object).expect("read object"), bytes);
        }

        // The active segment is never archived.
        let active_base = *manifest.segments.iter().map(|s| &s.base).max().unwrap();
        let local_segments = crate::segment::list_segment_files(data.path()).expect("list");
        assert!(
            local_segments.iter().max().unwrap() > &active_base,
            "active segment must stay local-only"
        );
    }

    #[test]
    fn a_segment_ending_at_the_watermark_is_not_final() {
        let data = tempfile::tempdir().expect("data dir");
        let engine =
            EventStoreEngine::create_with_options(data.path(), SEGMENT_SIZE).expect("engine");
        engine
            .append(AppendRequest {
                condition: None,
                events: vec![make_event(0)],
            })
            .expect("append");
        // Checkpoints alone until one no longer fits: the segment seals
        // with control records after its last event, and the new active
        // segment holds no event, so the watermark sits at the sealed
        // segment's end.
        let segments = || crate::segment::list_segment_files(data.path()).expect("list");
        while segments().len() < 2 {
            engine.persist_watermark_checkpoint().expect("checkpoint");
        }
        let sealed_end = segments()[1];
        assert_eq!(engine.head().0, sealed_end);
        assert!(
            engine.archivable_segments().is_empty(),
            "a segment ending at the watermark can still be reopened"
        );

        engine
            .append(AppendRequest {
                condition: None,
                events: vec![make_event(1)],
            })
            .expect("append");
        let archivable = engine.archivable_segments();
        assert_eq!(archivable.len(), 1);
        assert_eq!(archivable[0].end, sealed_end);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn only_the_data_bytes_of_an_untrimmed_segment_are_archived() {
        let data = tempfile::tempdir().expect("data dir");
        let tier = tempfile::tempdir().expect("tier dir");
        let engine = Arc::new(
            EventStoreEngine::create_with_options(data.path(), SEGMENT_SIZE).expect("engine"),
        );
        {
            let engine = Arc::clone(&engine);
            tokio::task::spawn_blocking(move || seal_segments(&engine, 1))
                .await
                .expect("seal");
        }
        // What a crash between rotation and the seal's sync leaves: the
        // sealed file still at its preallocated length.
        let segment = engine.archivable_segments().remove(0);
        let data_bytes = std::fs::read(&segment.path).expect("read segment");
        std::fs::OpenOptions::new()
            .write(true)
            .open(&segment.path)
            .and_then(|file| file.set_len(SEGMENT_SIZE * 2))
            .expect("pad segment");

        let archiver = archiver_for(tier.path()).await;
        archiver
            .run_backup_pass("default", &engine)
            .await
            .expect("backup pass");
        let manifest = archiver.load_manifest("default").await.expect("manifest");
        let entry = &manifest.segments[0];
        assert_eq!(entry.base, segment.base);
        assert_eq!(entry.size, data_bytes.len() as u64);
        assert_eq!(entry.blake3, blake3::hash(&data_bytes).to_hex().to_string());
        let object = tier
            .path()
            .join("default")
            .join("segments")
            .join(format!("{:020}.seg", entry.base));
        assert_eq!(std::fs::read(&object).expect("read object"), data_bytes);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn backup_pass_is_incremental_and_idempotent() {
        let data = tempfile::tempdir().expect("data dir");
        let tier = tempfile::tempdir().expect("tier dir");
        let engine = Arc::new(
            EventStoreEngine::create_with_options(data.path(), SEGMENT_SIZE).expect("engine"),
        );
        {
            let engine = Arc::clone(&engine);
            tokio::task::spawn_blocking(move || seal_segments(&engine, 2))
                .await
                .expect("seal");
        }

        let archiver = archiver_for(tier.path()).await;
        let first = archiver
            .run_backup_pass("default", &engine)
            .await
            .expect("first pass");
        assert!(first.uploaded >= 2);

        // Immediate re-run: nothing new.
        let second = archiver
            .run_backup_pass("default", &engine)
            .await
            .expect("second pass");
        assert_eq!(second.uploaded, 0);
        assert_eq!(second.already_archived, first.uploaded);

        // Seal more segments; only the delta uploads.
        let sealed_before = first.uploaded;
        {
            let engine = Arc::clone(&engine);
            let target = sealed_before + 2;
            tokio::task::spawn_blocking(move || seal_segments(&engine, target))
                .await
                .expect("seal more");
        }
        let third = archiver
            .run_backup_pass("default", &engine)
            .await
            .expect("third pass");
        assert!(third.uploaded >= 2, "expected delta uploads, got {third:?}");
        assert_eq!(third.already_archived, sealed_before);

        let manifest = archiver.load_manifest("default").await.expect("manifest");
        assert_eq!(
            manifest.segments.len(),
            third.already_archived + third.uploaded
        );
        let bases: Vec<u64> = manifest.segments.iter().map(|s| s.base).collect();
        let mut sorted = bases.clone();
        sorted.sort_unstable();
        assert_eq!(bases, sorted, "manifest must stay sorted by base");
    }
}
