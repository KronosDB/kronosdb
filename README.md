# KronosDB

A open-source DCB eventstore and distributed messaging bus built for reliable event sourcing at scale.

KronosDB provides append-only event storage with tag-based querying, and a distributed messaging layer (command bus, query bus, subscription queries), together with Raft-based replication for fault tolerance, and a built-in admin console, all in a single binary with no external dependencies.

## Features

- **Event Storage** — Append-only log with consistency conditions for optimistic concurrency. Tag-based filtering and time-based replay.
- **Messaging** — Distributed command bus with routing, query bus with scatter-gather, and subscription queries with live updates.
- **Raft Consensus** — Multi-node replication with automatic leader election, learner nodes, and passive backups.
- **Snapshot store** — Key-sequence snapshot store for state caching.
- **Admin Console** — Built-in web UI for monitoring and controlling contexts, events, clients, processors, messaging handlers, cluster state, and server configuration.
- **gRPC API** — Fast protocol buffer interface for all application operations. TLS and token-based authentication supported.

## Ready to use

Start using it today with:

- **Kronos-ts** — A typescript framework inspired by Axon Framework 5 that can be used to build event sourced applications on Node. [kronos-ts](https://github.com/KronosDB/kronos-ts)
- **Axon Framework** — First-class connector for Axon Framework 5 applications via [axon-kronosdb-connector](https://github.com/KronosDB/axon-kronosdb-connector).

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for details.
