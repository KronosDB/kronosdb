# KronosDB Operations Guide

Running KronosDB in production, with a focus on Kubernetes.

## Ports

| Port | Purpose | Auth |
|---|---|---|
| 50051 | gRPC (event store, messaging, metadata Raft, segment Tail) | `kronosdb-token` metadata + optional TLS/mTLS |
| 9240 | Admin HTTP (console, API, probes, metrics) | `[admin.auth]` — none / token / OIDC |

## Probes

- **Liveness**: `GET /health` on the admin port — static `ok` while the process is up.
- **Readiness**: `GET /ready` — 200 only when a committed native leader claim can execute or forward writes. Returns 503 during elections, catch-up, epoch fencing, and startup recovery.
- **gRPC**: `grpc.health.v1.Health/Check` is served on the gRPC port (no auth), status mirrors `/ready`.

```yaml
livenessProbe:
  httpGet: { path: /health, port: 9240 }
readinessProbe:
  httpGet: { path: /ready, port: 9240 }
startupProbe:            # segment recovery on large volumes takes time
  httpGet: { path: /ready, port: 9240 }
  failureThreshold: 60
  periodSeconds: 5
```

## Graceful shutdown

On SIGTERM the server: asks connected clients to reconnect elsewhere → shuts the Raft core down (peers see a clean departure) → stops the storage engines (a final fsync pass releases in-flight writers; new appends are rejected) → waits up to `--drain-deadline-secs` (default 20) for connections to close, then exits.

Set `terminationGracePeriodSeconds` **above** the drain deadline (e.g. 30 for the default 20).

## Data directory & fencing

- Each context is a subdirectory of `--data-dir`; `.seg` files are the authoritative replicated event log. The small metadata Raft log lives under `default/raft/`. **Backups must include the whole data dir.**
- The server takes an exclusive `flock` on `<data-dir>/LOCK` at startup and refuses to start if another live process holds it. Two pods must never share a volume (`ReadWriteOnce` PVCs on a StatefulSet).
- Acked writes are fsynced (group commit batches the fsyncs). A failed fsync **poisons the engine**: pending and future appends fail with an explicit error until restart — writes are never silently acked as durable.

## Write durability

Every deployment uses the same native segment path. A single voter acknowledges after its local segment wave is durable. A cluster acknowledges when a majority of voter durable cursors reaches the append's next-exclusive position. Followers forward appends to the fenced leader; openraft carries only membership, election, context, and leader-claim metadata.

Loss of quorum stalls writes (CP) while local reads and subscriptions remain bounded by the last committed watermark.

## Helm chart

`charts/kronosdb` packages everything below: a voter StatefulSet with
stable-DNS peer lists and ordinal-derived node IDs, optional
passive-backup learners, headless + client + admin services, the context
manifest as a ConfigMap, auth secrets, PDB, anti-affinity, and probes.

```bash
helm install kronosdb charts/kronosdb \
  --set auth.accessToken=… --set admin.adminToken=… \
  --set contexts='{orders,billing}' \
  --set backup.url=s3://backups/kronos --set backup.credentialsSecret=aws-creds \
  --set persistence.storageClassName=local-nvme
```

The chart refuses to render with admin token auth enabled but no token
set, and warns at install when gRPC auth or backup is off. `replicas`
must match actual Raft membership — see the scaling note below.

## Clustering on Kubernetes

- Use a StatefulSet with a headless Service and **stable DNS names** in peer lists, never pod IPs (addresses are persisted in Raft membership):
  `KRONOSDB_CLUSTER_PEERS=1=kronosdb-0.kronosdb-hl:50051,2=kronosdb-1.kronosdb-hl:50051,3=kronosdb-2.kronosdb-hl:50051`
- The lowest voter ID bootstraps the cluster on first start.
- Context creation is replicated through the metadata control plane; event records replicate through per-context Tail streams.
- Known limitation: the boot-time peer list is not reconciled against persisted membership — scale-out is done via the admin API (`/api/cluster/add-voter`), and the static config should be updated to match afterwards.

## Declarative manifest

Declare contexts in a TOML manifest applied at startup (idempotent, never deletes):

```toml
[[contexts]]
name = "orders"
```

Mount the same manifest into every pod (ConfigMap) and point at it with `KRONOSDB_MANIFEST`.

## Admin authentication

Default is `none` with a loud startup warning — acceptable only when the port is unreachable from untrusted networks. Configure one of:

**Static token**

```toml
[admin.auth]
mode = "token"
token = "..."           # or KRONOSDB_ADMIN_TOKEN
```

APIs send `Authorization: Bearer <token>`; a browser can bootstrap a session once with `?access_token=<token>`.

**OIDC (admin realm — Keycloak, Auth0, Entra ID, ...)**

```toml
[admin.auth]
mode = "oidc"

[admin.oidc]
issuer = "https://keycloak.example.com/realms/platform"
client-id = "kronosdb-console"
client-secret = "..."               # omit for public clients; PKCE always used
redirect-url = "https://kronosdb.example.com/auth/callback"
role-claim = "realm_access.roles"   # Keycloak realm roles
required-role = "kronosdb-admin"
cookie-secret = "..."               # stable session key across restarts/replicas
```

The console runs the authorization-code + PKCE flow and stores an HMAC-signed session cookie; API calls send an IdP-issued JWT as `Authorization: Bearer`, validated against the realm's JWKS (issuer, expiry, optional `audience`, optional `required-role`). Secrets can come from env: `KRONOSDB_OIDC_CLIENT_SECRET`, `KRONOSDB_OIDC_COOKIE_SECRET`.

`/health`, `/ready`, `/metrics`, and `/auth/*` are always unauthenticated (probes and scrapers).

## Observability

- **Prometheus**: `GET /metrics` on the admin port — per-context engine counters (`kronosdb_appends_total`, cache hit/miss, DCB violations, positions) and Raft gauges (`kronosdb_raft_is_leader`, `kronosdb_raft_leader_known`, term, applied index, voter count). Alert on `kronosdb_raft_leader_known == 0`.
- **JSON logs**: set `KRONOSDB_LOG_FORMAT=json`. Log filtering via `RUST_LOG` (default `kronosdb=info,warn`).

## Production readiness

The alert set below is shipped as a PrometheusRule by the Helm chart
(`alerts.enabled=true`). If you run your own rules, replicate at least the
first three — they cover every failure mode that needs a human.

| Signal | Metric | Severity | Meaning |
|---|---|---|---|
| Engine poisoned | `kronosdb_engine_poisoned > 0` | critical | An fsync failure poisoned the engine: the process looks healthy while **rejecting all writes** until restart. |
| Writes stalled | `max(kronosdb_native_write_gate_open) == 0` | critical | No node holds a writable leader claim — quorum loss or stuck election. Reads keep serving locally. |
| Disk almost full | `kubelet_volume_stats_*` < 20% free | warning | The store never deletes. Disk exhaustion ends in fsync failure → poisoned engine. `kronosdb_data_dir_bytes` gives per-context growth for capacity planning. |
| Ack degradation | `rate(kronosdb_ack_degradations_total) > 0` | warning | Written-acks are falling back to durable pacing: the disk is behind. Latency climbs next. |
| Replication lag | `kronosdb_replication_lag_events` growing | warning | A follower (or the quorum) is not keeping up with the leader. |
| DCB rejection spike | `rate(kronosdb_dcb_violations_total)` sustained | info | Many writers competing on one tag. This is a **modeling smell**, not a database problem: a consistency boundary was drawn too coarse (the DCB equivalent of a table-level lock). Expect client retry amplification until the boundary is narrowed — e.g. per-entity tags instead of a global one. |

Environment checklist:

- **Clocks**: run NTP/chrony on every node. Correctness and ordering never
  depend on wall clocks, but scheduled-event punctuality follows the
  leader's clock across failover.
- **File descriptors**: sealed segments are mmap'd and every client stream
  holds a socket — raise `nofile` (65536 is a sane floor). On Kubernetes
  this is a node/kubelet setting, not a pod securityContext field.
- **Append size**: client gRPC messages are capped at tonic's default
  4 MB decode limit — one append batch (events + tags + payloads) must fit
  in a single message. Oversized batches are rejected at the transport
  with a clear error; there is no server-side re-chunking.
- **Memory limits**: leave headroom above the process for the page cache —
  reads are mmap-backed, so the kernel cache *is* the read cache and
  over-tight container limits convert reads into reclaim thrash.

## Backup & restore

Until a snapshot-export API exists, the supported procedure is volume-level:

1. Scale the pod down (or take a CSI VolumeSnapshot — crash-consistent is safe: recovery discards torn tails via CRC-checked markers).
2. Snapshot/copy the **entire** data dir — including each context's `raft/` subdirectory (`membership.bin`, log, snapshots). A backup without `raft/` loses the voter set and Raft state.
3. Restore = place the data dir on a fresh volume and start the server; contexts are auto-discovered.

Sealed segments are immutable, so incremental file-level backup (rsync, object storage) is also viable — but must still capture `raft/` and the active segment last.

## Known limitations

- No retention/truncation: volumes grow unboundedly (`tail` is fixed at 0). Size PVCs accordingly and monitor `kronosdb_head_position`.
- Boot-time peer config vs. persisted Raft membership is not reconciled automatically.
- Admin HTTP has no built-in TLS — terminate TLS at an ingress/mesh in front of it, especially with OIDC (cookies).
