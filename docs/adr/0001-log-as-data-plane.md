# ADR-0001: Log-as-data-plane replication (segments ARE the replicated log)

Date: 2026-07-17
Status: Accepted (direction); implementation staged post-0.5.0
Deciders: Theo

## Context

KronosDB replicates writes through openraft: every append is proposed as a
Raft log entry, fsynced into the Raft log (`raft/log-*.bin`), replicated,
and then applied — where the state machine serializes the same events a
second time into the event segments (`.seg`), the actual durable state.

This is the standard consensus shape for state machines whose state is a
*different shape* than the log (a KV store materializes edits). Our state
machine's state **is an ordered log of events**. Running a log to replicate
a log costs, per event:

- **2 writes** (bincoded Raft entry + segment record) and **2 fsyncs**
  (log fsync backs the client ack; segment fsync via group commit)
- **2 serializations** through two different byte formats
- **per-entry RPC framing** on replication instead of streaming bytes
- openraft 0.9 additionally serializes its core loop on each entry's log
  fsync (no pipelining), making consensus throughput entries/sec-bound

Measured on 2026-07-17 (Docker/Linux, fdatasync, `benchmarks/harness`,
batch=1000 conc=16 appends): single-node fast path 204k events/s; the same
node through single-node Raft 113k (−45% with zero network); 3-node cluster
with 2ms simulated inter-AZ RTT 65k (p50 212ms). The consensus round-trip
itself accounts for only part of the gap; the double-write/double-fsync and
the unpipelined per-entry loop account for the rest. Bulk ingest also
retained 409MB of Raft log for 1M events (~0.5GB/M events of pure
duplication) before snapshot purge; a byte-aware snapshot trigger now
bounds this, but the duplication itself remains.

Two prior design decisions already lean toward unification:

1. **Raft markers live inside segments** (`segment/format.rs::RaftMarker`):
   every applied entry writes a `(term, index, event_count)` marker record
   interleaved with its events; boot recovery reconstructs `last_applied`
   by scanning markers (no sidecar). Segments already carry consensus
   epochs.
2. **Marker-authoritative recovery** already truncates incomplete
   marker-groups from segment tails — mechanically the same operation a
   follower needs on leader change.

## Decision

Adopt **log-as-data-plane** replication as the target architecture:

- The event segments become THE replicated log. The leader evaluates DCB
  conditions **at write time** (before replication), assigns positions,
  writes each accepted batch once, fsyncs once, and streams the same
  segment bytes to followers.
- Followers append the streamed bytes to their own segments, fsync, and
  ack a byte/position cursor. The **watermark** (commit point) advances
  when a quorum of cursors passes a position; client acks release at the
  watermark. Followers serve reads/subscriptions **clamped to the
  watermark** (their local tail may run ahead of it, unacked).
- Consensus (openraft) is retained for the **control plane only**:
  membership, leader election, context metadata, and fencing-epoch allocation —
  the KRaft shape: consensus for metadata, streaming for data. Watermark
  checkpoints live only in the segment log. Every replicated frame carries the
  leader's epoch; followers reject frames from stale epochs.
- On failover, the control plane elects the most-caught-up eligible
  follower (election restriction analog); the new leader's first act is an
  epoch-change record; followers **truncate any suffix past the watermark**
  that diverges from the new leader's stream. Truncated events were never
  client-acked, so no acked data is ever lost.

## Consequences

**Wins** (estimates grounded in the measured loss decomposition):
- Single write + single fsync per event on every node; one serialization.
- Replication becomes sequential byte streaming (pipelined by
  construction, zero-copy potential), bounded by network/disk bandwidth
  rather than consensus round-trips.
- Estimated single-node retention rises from ~55% to 85–95% of fast path;
  clustered throughput 2–3× today's (~150–180k events/s at batch), with
  latency floor unchanged (quorum RTT + fsync — physics).
- DCB evaluation happens once, at the leader's serialization point,
  instead of deterministically on every node at apply.

**Costs / dragons** (why this is staged, not in 0.5.0):
- Leader fencing becomes load-bearing for **consistency**, not just
  durability: a stale leader evaluating DCB against a stale index would
  corrupt consistency boundaries, not merely lose data. Epoch checks must
  be airtight on every frame.
- Follower segments lose append-only immutability: failover can truncate
  an unacked suffix and re-write the same positions with different events.
  Recovery invariants, the active-segment index, and subscription cursors
  must all tolerate this.
- We own the data-plane protocol (tail sessions, ack cursors, catch-up,
  flow control) instead of inheriting openraft's proven machinery. The
  crash-test suites (single-node, three-node convergence, cold-join,
  restart-after-purge) must be extended to cover the new failure shapes
  before it ships.

**Read-path filtering** (concern raised in review): segment readers
already skip non-event records via the record flags byte
(`SegmentIterator` skips `LEGACY_RAFT_MARKER` records); control records reuse
this mechanism. No API or client change.

**Control-record overhead budget** (concern raised in review): today's
marker is ~32 bytes (9B record header + ~23B payload) per *applied batch*
(up to 512 requests / 16k events / 2MB after coalescing) — worst case
(single 1-event appends, ~200–500B events) ~10–15% of segment bytes,
negligible at any realistic batching. The new design's control records
(epoch changes, periodic watermark checkpoints) are rarer than per-batch
markers. Budget: **< 1% of segment bytes at batch ≥ 10**, asserted by a
bench-time check.

## Alternatives considered

- **Status quo (openraft on the data path)**: rejected and removed. Its
  measured cost was the 45% single-node haircut, double storage, and an
  entries/sec ceiling.
- **Wait for openraft log-I/O pipelining**: recovers part of the gap
  (the per-entry core-loop stall) with zero new safety surface; does NOT
  remove the double-write/double-fsync/double-serialize. Worth taking
  when upstream ships it regardless of this ADR — it is the cheap
  waypoint, not the destination.
- **Chain replication / CRAQ**: better throughput under some regimes but
  worse tail latency on failure, and a poorer fit for quorum-of-2-in-3
  cross-AZ deployments.
- **Build full custom consensus (data + control)**: strictly more risk
  than keeping proven consensus where it is cheap (control plane) and
  replacing it only where it is expensive (data plane).

## References

- Measured baselines: `benchmarks/vs-umadb/results/` + `benchmarks/kronosdb-cluster/results/`
  (20260717-101436 fast path; 20260717-115106 single-node raft;
  20260717-095454 + rerun cluster), memory notes
  `project_umadb_bench_fixes`, `project_raft_cluster_bench`.
- Coalescing/livelock fix: `raft/cluster.rs` (MAX_BATCH_BYTES),
  `raft/network.rs`/`raft/transport.rs` (RAFT_MAX_MESSAGE_BYTES).
- Marker format: `segment/format.rs`; marker-authoritative recovery:
  `segment/writer.rs::recover_segment`.
- Implementation plan and protocol: `plan.md`.
