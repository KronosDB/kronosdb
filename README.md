# KronosDB

An open-source DCB event store and distributed messaging bus built for reliable event sourcing at scale.

KronosDB provides append-only event storage with tag-based querying, and a distributed messaging layer (command bus, query bus, subscription queries), together with byte-exact segment replication and Raft-based leader election for fault tolerance, and a built-in admin console, all in a single binary with no external dependencies.

## Features

- **Event Storage** — Append-only log with consistency conditions for optimistic concurrency. Tag-based filtering and time-based replay.
- **Messaging** — Distributed command bus with routing, query bus with scatter-gather, and subscription queries with live updates.
- **Native replication** — Group-commit waves stream as byte-exact segment records; Raft handles only membership, election, and fencing epochs.
- **Snapshot store** — Key-sequence snapshot store for state caching.
- **Admin Console** — Built-in web UI for monitoring and controlling contexts, events, clients, processors, messaging handlers, cluster state, and server configuration. Protected by static-token or OIDC auth (connect it to a Keycloak/Auth0/Entra admin realm).
- **gRPC API** — Fast protocol buffer interface for all application operations. TLS and token-based authentication supported.
- **Production-ready operations** — Liveness/readiness probes (`/ready`, `grpc.health.v1`), Prometheus `/metrics`, JSON logs, graceful drain on SIGTERM, data-dir fencing, TLS/mTLS peer transport, and quorum-durable acknowledgements.
- **Declarative manifest** — Declare contexts in a TOML manifest ensured to exist at startup, instead of creating them through the admin API.

## Declarative manifest

Contexts can be declared in a manifest file that the server applies at startup — idempotent, never deletes:

```toml
# kronosdb-manifest.toml
[[contexts]]
name = "orders"

[[contexts]]
name = "payments"
```

Point the server at it with `--manifest`, `KRONOSDB_MANIFEST`, or a `manifest = "..."` key in `kronosdb.toml`; a `kronosdb-manifest.toml` in the working directory is picked up automatically. In a cluster, give every node the same manifest (on Kubernetes: mount one ConfigMap into each pod). Contexts can still be created at runtime through the admin console or API — those are replicated to all nodes through consensus.

## Performance

Measured against PostgreSQL 16 and Axon Server at strictly equal durability
(every append fsync'd and quorum-acknowledged before the response): faster
single-event appends, 2× at batch 10, 2.4–3.3× on wide DCB command
handling, 300k+ durable events/s bulk ingest on one node via multi-context
scaling, and hard-kill restart to ready in under 0.4 seconds.

## Ready to use

Start using it today with:

- **Kronos-ts** — A TypeScript framework inspired by Axon Framework 5 that can be used to build event-sourced applications on Node. [kronos-ts](https://github.com/KronosDB/kronos-ts)
- **Axon Framework** — First-class connector for Axon Framework 5 applications via [axon-kronosdb-connector](https://github.com/KronosDB/axon-kronosdb-connector).

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for details.
