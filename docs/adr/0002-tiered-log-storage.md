# ADR-0002: Tiered log storage (sealed segments in object storage)

Date: 2026-07-21
Status: Accepted; implementation staged (stage 1 lands first)
Deciders: Theo

## Context

Two operational gaps share one answer:

1. **No backup story.** Replicas (voters, learners) protect against node
   loss, not against the bad write itself: a corrupting bug, an operator
   mistake, or a compromised client replicates to every node — including
   analytics learners — within milliseconds. Real backup requires an
   off-cluster, append-only copy with point-in-time restore.
2. **Unbounded local growth.** An event store never deletes. Without
   tiering, every voter and learner stores every context forever, so total
   retention is capped by the smallest node's disk. This is also the main
   remaining driver for Axon-style replication groups (see Non-goals).

Two properties of the ADR-0001 architecture make both cheap to solve at
once:

- **Sealed segments fully below the quorum watermark are immutable.**
  Failover truncation only ever removes bytes above the watermark, and the
  byte-exact healing protocol keeps everything below it identical across
  the cluster.
- **Segments are byte-identical on every node** (asserted by the
  crash/cold-join/stale-leader test suites). There is exactly one
  canonical byte representation of any archived segment, so any node can
  upload it and any node can consume it.

Together: *"sealed segment whose end position is ≤ the quorum watermark"*
is a perfect archival unit — content-addressable, uploaded once, never
rewritten.

## Decision

Adopt **tiered log storage**: an object store (S3/GCS/Azure/local
filesystem via the `object_store` crate) holds the canonical archive of
watermark-covered sealed segments plus a per-context manifest. The same
artifact serves as backup (stage 1–2) and as the cold tier (stage 3–4).

Object layout, per context:

```
<prefix>/<context>/segments/<base>.seg      # exact local bytes
<prefix>/<context>/segments/<base>.idx      # rebuildable; uploaded to skip rebuild on restore
<prefix>/<context>/segments/<base>.bloom
<prefix>/<context>/manifest.json            # commit point: segment list + blake3 + watermark floor
```

The manifest is written last after its segments upload; a segment absent
from the manifest does not exist for readers. Object-store PUTs are atomic
per object, so a crashed uploader leaves at worst orphan segment objects
that the next pass reconciles (idempotent re-upload or manifest adoption
after checksum verification).

**Upload eligibility:** a segment is archivable when it is sealed (a
newer base exists) AND its end position (the next segment's base) is ≤ the
current quorum watermark. Only the claimed data-plane leader uploads —
segments are byte-identical everywhere, so leader-only is a coordination
convenience, not a correctness requirement; after failover the new leader
continues, and re-uploads are harmless (same bytes, verified by checksum).

### Stages

1. **Backup uploader** (this change): background leader task ships
   eligible segments + manifest. Write-only; no read-path or eviction
   changes. Ships "we have backups".
2. **Restore/bootstrap**: start a node or cluster from a manifest,
   optionally truncated at position P (point-in-time restore = a prefix of
   the log, consistent by construction).
3. **Local eviction + read-through cache**: evict archived segments
   locally by retention policy; cold reads download into a local LRU cache
   and mmap from there. `.idx`/`.bloom` always stay local, so bloom-filter
   negative lookups never touch the tier.
4. **Catch-up from tier**: cold-joining learners and far-behind followers
   bulk-fetch sealed segments from the object store and only tail the hot
   suffix from the leader. This is the real resolution of the follower
   `NeedSnapshot` dead-end.

## Consequences

- Backup exists after stage 1 alone; restore after stage 2.
- After stages 3–4, local disk holds only the hot tail + cache: the
  storage-scaling wall disappears, and cold-join stops loading the leader.
- The seal-time watermark checkpoint (byte/seal-triggered checkpoint work)
  doubles as the archival-eligibility marker.
- New failure surface: object-store outages must never affect the write
  path (uploader is strictly asynchronous and lag-tolerant); stage 3 makes
  cold *reads* depend on the tier — mitigated by the local cache and by
  never evicting below a configured local retention floor.
- Restore trusts the manifest's blake3 checksums; the uploader verifies
  after upload before committing the manifest.

## Non-goals

- **Replication groups** (Axon-style per-context replica sets/roles) stay
  off the roadmap. The analytics/backup-replica need is served by
  learners; the storage-capacity driver is served by this ADR. Revisit
  triggers: total data outgrowing what every node can hold even with
  tiering, genuine multi-tenancy with placement/isolation requirements, or
  per-context durability tuning. Standing guard until then: keep
  `LeaderClaim` per-context-shaped so the eventual refactor (global
  metadata quorum + per-context replica sets — never raft-per-group) stays
  contained.
- No plugin API for storage backends: the `object_store` crate's built-in
  backends (S3, GCS, Azure, local filesystem) are configured by URL;
  anything else is a fork.
- No compaction/deletion of archived history.
