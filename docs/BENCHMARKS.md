# KronosDB Benchmarks

Last updated: 2026-07-21. All numbers below were measured by us, on our own
hardware, with the raw result files preserved. Read the
[honesty and limitations](#honesty-and-limitations) section before quoting
anything — every benchmark here is real, and every benchmark here has a
boundary beyond which it stops meaning what you want it to mean.

**Environment for all runs:** Apple M1 Max (10 cores, 16 GiB VM), Linux
containers under OrbStack, ARM64. Durability is always strict unless
explicitly noted: an append is acknowledged only after `fdatasync` and the
quorum watermark. One measured backend ran at a time.

---

## TL;DR

At equal durability, on identical hardware, through each system's official
client:

- **Single-event durable appends:** ~1.2× PostgreSQL throughput at lower
  latency; **2× at batch 10; 5–19× at batch 100+** (client-inclusive).
- **Concurrency scaling:** KronosDB reaches ~34,000 durable single-event
  appends/s at 256 connections and was still climbing; PostgreSQL peaks
  around 10,600 at 64 and *degrades* to ~8,200 at 256 — after raising
  `max_connections`, without which it refuses connections at 100.
- **Wide DCB criteria** (20 event types OR'd, the multi-entity invariant
  case): 2.4–3.3× PostgreSQL, because bitmap resolution is nearly free while
  SQL OR-branch planning is not.
- **Aggregate rehydration:** faster than PostgreSQL at every measured depth
  (10 → 10,000 events).
- **Multi-context vertical scaling:** +53% bulk ingest with 4 contexts on
  one node (201k → 308k events/s) — with an honest caveat: it *hurts*
  fsync-bound tiny writes on a single disk.
- **Hard-kill restart to ready: ≤ 0.37 s** with 100k/100k events readable
  and writes resuming (Axon Server SE: 15.46 s on the same machine).
- **Bulk ingest needs almost no CPU:** a 2-core server sustained ~239,000
  durable events/s; the measurement client saturated before the server did.

---

## 1. KronosDB vs PostgreSQL 16 — through the official TypeScript clients

This is the comparison a product team actually experiences: the full path
through `@kronos-ts/kronosdb` (gRPC) and `@kronos-ts/postgres` (the
production DCB adapter: advisory-lock taxonomy, conflict checks, `NOTIFY`)
— client serialization included.

**Setup:** both servers in Linux containers capped at 4 CPUs / 4 GiB.
KronosDB dev build (post-0.6.0: event-driven group commit, unary append,
async ack, cached active-segment mmap). PostgreSQL 16-alpine,
`fsync=on`, `synchronous_commit=on`, `pg` (node-postgres) adapter.
5 samples per cell, medians reported, every run cross-verified for
correctness (exact counts, unique IDs, ordinal continuity, payload
checksums). Harness:
`kronos-ts/integrationtests/src/benchmarks/kronosdb-postgres/`.

### Durable append throughput (serial client)

| Batch | KronosDB | PostgreSQL | Multiplier |
|---|---|---|---|
| 1 | 661 ev/s, p50 1.38 ms | 547 ev/s, p50 1.73 ms | 1.2× |
| 10 | 6,218 ev/s, p50 1.56 ms | 3,137 ev/s, p50 2.99 ms | 2.0× |
| 100 | 25,108 ev/s, p50 3.82 ms | 5,142 ev/s | 4.9× |
| 1,000 | 87,334 ev/s | 4,575 ev/s | 19× |

The batch-100/1000 multipliers are **client-inclusive**: PostgreSQL's bulk
numbers through this adapter are limited by the adapter's per-row work, not
the engine — see [section 2](#2-engine-level-rust-clients) where PostgreSQL
ingests 177k ev/s through a Rust client. Both claims are stated because both
layers are real; pick the one that matches your stack.

### Concurrent single-event appends (4-CPU containers)

| Concurrency | KronosDB | PostgreSQL |
|---|---|---|
| 1 | 687 ev/s, p50 1.26 ms | 561 ev/s, p50 1.65 ms |
| 4 | 2,050 ev/s, p50 1.8 ms | 1,551 ev/s, p50 2.5 ms |
| 16 | 5,922 ev/s, p50 2.4 ms | 2,824 ev/s, p50 5.4 ms |
| 64 | 10,722 ev/s, p50 5.2 ms | 2,825 ev/s, p50 21.5 ms |

Both ceilings here are **TypeScript-client-bound**, not server-bound (a
single Bun process saturates near 11k msg/s). The shape is what matters:
KronosDB's group-commit waves keep scaling; PostgreSQL's WAL serialization
plateaus at concurrency 16 and only its latency grows after that.

### Event-sourced command loop (source → reduce → conditional append)

The canonical "handle one command" cycle against a ~20-event aggregate,
using consistency markers / DCB conditions on both stores.

| Concurrency | KronosDB | PostgreSQL |
|---|---|---|
| 1 | 235–297 cmd/s, p50 3.4–4.0 ms | 316–321 cmd/s, p50 2.9–3.0 ms |
| 4 | ~1,040 cmd/s, p50 3.5 ms | ~735 cmd/s, p50 5.2 ms |
| 16 | ~1,100 cmd/s, p50 13–15 ms | ~1,100 cmd/s, p50 14–15 ms |

Honest call: **serial command handling is parity, with PostgreSQL slightly
ahead on the median.** This scenario showed ±20% run-to-run spread on
KronosDB (warmup-sensitive) while PostgreSQL was rock-stable; we report the
observed band, not the best run. At concurrency 4+ KronosDB leads; at 16
both are client-CPU-bound. Both stores' individual legs (read, append) are
faster on KronosDB — the remaining serial gap is TypeScript client work per
command, not the server.

### Wide DCB criteria — the multi-entity invariant case

Same command loop, but the consistency boundary spans **20 event types,
each restricted to its own tag key, OR'd into one criteria**, against 4,000
unrelated noise events. This is the workload DCB systems exist for.

| Concurrency | KronosDB | PostgreSQL | Multiplier |
|---|---|---|---|
| 1 | 246 cmd/s, p50 3.8 ms | 102 cmd/s, p50 9.7 ms | 2.4× |
| 4 | 1,113 cmd/s, p50 3.2 ms | 337 cmd/s, p50 11.3 ms | 3.3× |

Widening the criteria from 1 branch to 20 cost KronosDB ~4% (297 → 246
cmd/s serial). It cost PostgreSQL **3×** (316 → 102). Roaring-bitmap
OR/AND resolution over the tag index is nearly free; multi-branch SQL with
per-branch advisory locking is not.

### Aggregate rehydration (source + reduce, p50 per read)

| History depth | KronosDB | PostgreSQL |
|---|---|---|
| 10 | 0.68 ms | 0.73 ms |
| 100 | 0.96 ms | 1.05 ms |
| 1,000 | 4.54 ms | 5.02 ms |
| 10,000 | 42.9 ms | 45.0 ms |

### Tracking processors (measured on v0.6.0, 2 ms group commit)

| Scenario | KronosDB | PostgreSQL |
|---|---|---|
| Catch-up, framework default batch | 692 ev/s | 401 ev/s |
| Catch-up, batch 100 | 47,661 ev/s | 402 ev/s |
| Live append→handler, serial p50 | 6.7 ms | 7.3 ms |
| Live handled/s @ concurrency 16 | 2,921 | 2,064 |

The PostgreSQL catch-up ceiling (~400 ev/s) is an adapter artifact — its
streaming path pages 100 rows per 250 ms safety poll — not an engine limit.
Also noted for fairness: with Bun's built-in SQL driver instead of `pg`,
PostgreSQL live latency degrades to ~250 ms p50 (fallback polling); the
canonical comparison uses `pg` with native LISTEN precisely to avoid
crediting KronosDB for a competitor's bad adapter path.

---

## 2. Engine-level (Rust clients)

To remove the TypeScript client from the picture, both stores were driven
by a Rust harness (`benchmarks/harness/`): tonic for KronosDB,
tokio-postgres for PostgreSQL — the PostgreSQL store mirrors the TS
adapter's transaction semantics (shared advisory lock, conflict check for
conditional appends, multi-row insert, post-commit `NOTIFY`, GIN-indexed
JSONB tags). Uncapped CPUs, one connection per worker, realistic ~300-byte
order-fulfillment events.

### Single-event durable appends

| Concurrency | KronosDB | PostgreSQL |
|---|---|---|
| 1 | 473 ev/s, p50 2.0 ms | 382 ev/s, p50 2.5 ms |
| 4 | 2,240 ev/s, p50 1.7 ms | 1,967 ev/s, p50 1.9 ms |
| 16 | 8,231 ev/s, p50 1.9 ms | 4,790 ev/s, p50 2.8 ms |
| 64 | 14,039 ev/s, p50 4.4 ms | 10,610 ev/s, p50 5.6 ms |
| 256 | **34,376 ev/s, p50 7.1 ms** | 8,231 ev/s, p50 28.3 ms | 

Two findings worth naming:

- PostgreSQL's first 256-connection attempt failed outright:
  `FATAL: sorry, too many clients already` (default `max_connections=100`).
  The number above required raising the limit to 400. Connection-per-worker
  does not scale on PostgreSQL without pooling middleware; KronosDB held
  256 concurrent gRPC streams without configuration.
- Splitting the load generator across processes moved KronosDB's total by
  only +9% (37.5k with 2×128) — the measurement client is approximately
  honest, and the server was still not saturated at 256 workers.

### Bulk ingest (batch 1,000 × 16 workers)

| Store | Events/s |
|---|---|
| KronosDB, 1 context | 201,500 |
| KronosDB, 4 contexts | **308,625** |
| PostgreSQL (UNNEST multi-row insert) | 177,000 |

Note what this row does to section 1's "19×": through engine-level clients
PostgreSQL bulk ingest is 177k, not 4.6k — the TS adapter was the
bottleneck there. KronosDB still wins (1.14× single-context, 1.74×
multi-context), and its numbers include full tag/bloom indexing while both
include strict durability.

---

## 3. Multi-context vertical scaling — measured, both directions

Contexts are independent event stores inside one node (own segments, own
group-commit thread, own indexes). Same node, same disk, same client:

| Workload | 1 context | 4 contexts | Result |
|---|---|---|---|
| batch 1,000 × 16 workers | 201,500 ev/s, p50 65 ms | 308,625 ev/s, p50 35 ms | **+53%** |
| batch 1 × 256 workers | 34,376 ev/s | 24,719 ev/s | −28% |

Both rows are true and the mechanism explains them: contexts multiply the
**per-context serialization point** (writer lock, index maintenance,
encode), so CPU-bound bulk ingest scales. But tiny concurrent writes are
bounded by the shared disk's fsync — and one context already merges all 256
writers into a single fsync wave, so splitting into 4 contexts just issues
4× the fsyncs on the same device and fragments the batching. Multi-context
is a scaling lever for index/CPU-bound write load and workload isolation —
not a way to multiply fsync-bound tiny-write throughput on one volume.
PostgreSQL has no in-instance analog; its equivalent step is sharding
across databases.

---

## 4. When does more CPU help?

Server container capped at N CPUs, hammered by multiple Rust client
processes:

| Server CPUs | batch-1 × 256 workers | batch-1000 × 16 |
|---|---|---|
| 2 | 21,265 ev/s | 238,750 ev/s |
| 4 | 36,991 ev/s (+74%) | 238,500 ev/s |
| 8 | 39,635 ev/s (+7%) | 204,000 ev/s* |

For small concurrent appends, cores matter up to ~4, then the workload is
fsync-cadence-bound. For bulk ingest, **two cores already saturate the
disk pipeline at ~239k durable events/s** — the starred 8-CPU value is
lower only because on a 10-core laptop, giving the server more cores
starves the load generator (see limitations). Sizing takeaway: a 4-core
node covers ~37k tiny durable writes/s; bulk ingest capacity is bought
with disks, not cores.

---

## 5. Durability, crash recovery, restart

- **Acknowledgement semantics:** an append is acknowledged only after its
  bytes are `fdatasync`'d (Linux) / `F_FULLFSYNC`'d (macOS) and the quorum
  watermark passes it. A failed fsync **poisons the engine** — pending and
  future appends fail explicitly rather than being silently un-durable.
- **Crash suites** (run on every change): single-node kill-mid-commit, and
  a three-node suite that SIGKILLs the leader mid-write for 10 iterations —
  requiring failover, resumed writes, the killed node's catch-up, and
  **byte-identical segment convergence** across all three nodes. Zero
  acknowledged writes lost across all runs.
- **Hard-kill restart to ready: ≤ 0.37 s**, with 100,000/100,000 events
  readable immediately and writes resuming (first post-restart second:
  213 appends; 100k-event sequential read 70 ms). Segments are the
  database — recovery is a tail scan of the last segment, not a log replay.
- Post-ingest RSS ~86 MiB after a 100k-event run; on-disk footprint
  ~285 MB apparent (including preallocation) for the same sample.

---

## 6. KronosDB vs Axon Server SE 2026.0.4 (2026-07-17)

Measured on the pre-rewrite v0.6.0-dev engine (numbers for KronosDB are
*lower* than the current engine would produce). 4 CPUs / 8 GiB each.
Full report: `benchmarks/vs-axon/20260717-axon-vs-kronosdb.md`.

**The durability caveat dominates this comparison:** Axon Server 2026.0.4
acknowledges appends after a memory-mapped write, with `force()` on a
1,000 ms timer (verified in the shipped classes) — its sub-millisecond
acks do not mean data reached disk. KronosDB's acks do.

| Point | Axon Server | KronosDB (0.6.0-dev) |
|---|---|---|
| append b1/c1 | 1,113 ev/s, p50 0.72 ms *(weaker ack)* | 206 ev/s, p50 4.6 ms |
| append b100/c16 | 12,000 ev/s, p50 132 ms | 114,220 ev/s, p50 13.5 ms |
| append b1000/c16 | 0 completed in any 10 s window | 229,700 ev/s |
| tag read c16 | 173,020 ev/s | 201,112 ev/s |
| sequential 100k c16 | 1.11M ev/s | 3.44M ev/s |
| hard-kill restart to ready | 15.46 s | **≤ 0.37 s** |
| post-ingest RSS | ~1.40 GiB | ~86 MiB |

---

## 7. Replication cost (3-node cluster, 2 ms simulated inter-AZ RTT)

Measured 2026-07-17 on the 0.6.0 engine (2 ms group-commit timer era) with
netem-injected 2 ms RTT between nodes — quorum acks over the network:

| Point | 3-node cluster |
|---|---|
| append b1/c1 | ~104–109 ev/s, p50 9.1–9.3 ms |
| append b1/c16 | 1,414 ev/s, p50 10.0 ms |
| append b1000/c16 | 151,700 ev/s, p50 87 ms |
| append b1/c1, no commit timer | 248–257 ev/s, p50 3.5–3.7 ms |

The latency floor is physics — one quorum round trip plus two fsyncs — and
batching recovers throughput exactly as on a single node. These predate the
event-driven commit rewrite; current-cluster numbers will be refreshed.

---

## Honesty and limitations

1. **It's one laptop.** Clients and servers share 10 cores and one NVMe
   device. Absolute ceilings above ~40k tiny appends/s and ~240k bulk
   events/s are *client-limited* here, proven by the CPU-cap experiments.
   Shapes, inflection points, and A/B comparisons at fixed topology are
   trustworthy; press-release peak numbers are not the point of this doc.
2. **Client-inclusive vs engine-level numbers differ** — sometimes by 30×
   (PostgreSQL bulk: 4.6k through its TS adapter, 177k through Rust). We
   report both layers and label them. Demand the same of any benchmark.
3. **VM fsync state pollutes serial-latency measurements.** We caught our
   own harness reporting 2.5 ms for a 1.3 ms operation because prior
   disk-heavy scenarios left dirty-page backlog. All serial-latency numbers
   here come from idle-paired runs; mid-chain readings were discarded.
4. **The serial command-loop scenario drifts ±20%** on KronosDB across
   runs; we report the band and call it parity rather than cherry-picking
   the 331 cmd/s best run.
5. **Some tables mix engine versions** (labeled inline): Axon comparison
   and cluster numbers predate the event-driven-commit rewrite and
   understate the current engine.
6. **Axon Server's headline latency is not durability-equivalent** to
   either KronosDB or PostgreSQL in these tests; its rows are labeled.
7. Every run's raw output is preserved (`benchmarks/*/results/`,
   TS harness JSON documents) with image digests, git state, and
   environment metadata, and every measured run passed its correctness
   verification (313k+ assertions in the full TS suite).

## Reproducing

- TS comparison: `bun run benchmark:kronosdb-postgres -- --profile full`
  (kronos-ts repo; `--scenario`, `--backend`, `--cpu`,
  `--kronosdb-group-commit-ms`, `--kronosdb-image` to slice).
- Rust harness: `benchmarks/harness/run-docker.sh`, or the
  harness binary directly with `--store kronosdb|postgres|umadb|axon`,
  `--families append --append-batches 1 --concurrency 1,4,16,64,256`,
  `--contexts N` for multi-context, `--connect` for an external server.
