# KronosDB

A open-source DCB eventstore and distributed messaging bus built for reliable event sourcing at scale.

KronosDB provides append-only event storage with tag-based querying, and a distributed messaging layer (command bus, query bus, subscription queries), together with Raft-based replication for fault tolerance, and a built-in admin console, all in a single binary with no external dependencies.

## Features

- **Event Storage** — Append-only log with consistency conditions for optimistic concurrency. Tag-based filtering and time-based replay.
- **Messaging** — Distributed command bus with routing, query bus with scatter-gather, and subscription queries with live updates.
- **Raft Consensus** — Multi-node replication with automatic leader election, learner nodes, and passive backups.
- **Snapshot store** — Key-sequence snapshot store for state caching.
- **Admin Console** — Built-in web UI for monitoring and controlling contexts, events, clients, processors, messaging handlers, cluster state, and server configuration. Protected by static-token or OIDC auth (connect it to a Keycloak/Auth0/Entra admin realm).
- **gRPC API** — Fast protocol buffer interface for all application operations. TLS and token-based authentication supported.
- **Production-ready operations** — Liveness/readiness probes (`/ready`, `grpc.health.v1`), Prometheus `/metrics`, JSON logs, graceful drain on SIGTERM, data-dir fencing, and a single-node fast path that skips the Raft round-trip on standalone deployments. See [docs/OPERATIONS.md](docs/OPERATIONS.md).
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

## Ready to use

Start using it today with:

- **Kronos-ts** — A typescript framework inspired by Axon Framework 5 that can be used to build event sourced applications on Node. [kronos-ts](https://github.com/KronosDB/kronos-ts)
- **Axon Framework** — First-class connector for Axon Framework 5 applications via [axon-kronosdb-connector](https://github.com/KronosDB/axon-kronosdb-connector).

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for details.
