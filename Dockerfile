FROM rust:1.88-bookworm AS builder

RUN apt-get update && apt-get install -y protobuf-compiler && rm -rf /var/lib/apt/lists/*
WORKDIR /build

# Cache dependencies: copy manifests and build scripts first
COPY Cargo.toml Cargo.lock ./
COPY crates/kronosdb-eventstore/Cargo.toml crates/kronosdb-eventstore/Cargo.toml
COPY crates/kronosdb-messaging/Cargo.toml crates/kronosdb-messaging/Cargo.toml
COPY crates/kronosdb-server/Cargo.toml crates/kronosdb-server/Cargo.toml
COPY crates/kronosdb-bench/Cargo.toml crates/kronosdb-bench/Cargo.toml

# Dummy sources so cargo can resolve the workspace and fetch deps
RUN mkdir -p crates/kronosdb-eventstore/src && echo "pub fn _d(){}" > crates/kronosdb-eventstore/src/lib.rs \
    && mkdir -p crates/kronosdb-messaging/src && echo "pub fn _d(){}" > crates/kronosdb-messaging/src/lib.rs \
    && mkdir -p crates/kronosdb-server/src && echo "fn main(){}" > crates/kronosdb-server/src/main.rs \
    && mkdir -p crates/kronosdb-bench/src && echo "pub fn _d(){}" > crates/kronosdb-bench/src/lib.rs

# Proto and build scripts needed for compilation
COPY proto/ proto/
COPY crates/kronosdb-eventstore/build.rs crates/kronosdb-eventstore/build.rs
COPY crates/kronosdb-server/build.rs crates/kronosdb-server/build.rs

# Build deps only (cached until Cargo.toml/Cargo.lock change)
RUN cargo build --release --package kronosdb-server 2>/dev/null || true

# Copy real source and rebuild
COPY crates/ crates/
RUN cargo build --release --package kronosdb-server \
    && strip target/release/kronosdb-server

# --- Runtime
FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates tini wget \
    && rm -rf /var/lib/apt/lists/*

RUN groupadd -r kronosdb && useradd -r -g kronosdb -m -d /data kronosdb

COPY --from=builder /build/target/release/kronosdb-server /usr/local/bin/kronosdb-server

ENV KRONOSDB_LISTEN=0.0.0.0:50051
ENV KRONOSDB_ADMIN_LISTEN=0.0.0.0:9240
ENV KRONOSDB_DATA_DIR=/data

USER kronosdb
VOLUME /data
EXPOSE 50051 9240

ENTRYPOINT ["tini", "--", "kronosdb-server"]

HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
    CMD wget -q --spider http://${KRONOSDB_ADMIN_LISTEN}/health || exit 1
