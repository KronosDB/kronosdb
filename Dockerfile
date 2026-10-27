# syntax=docker/dockerfile:1
FROM rust:1.90-bookworm AS builder

WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY crates/ crates/

# The registry and target directory are cache mounts: a source change
# rebuilds only the crates it touched. The binary is copied out of the
# mount, which is not part of the image.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/build/target \
    cargo build --release --package kronosdb-server \
    && strip target/release/kronosdb-server \
    && cp target/release/kronosdb-server /kronosdb-server

# --- Runtime
FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates tini wget \
    && rm -rf /var/lib/apt/lists/*

RUN groupadd -r kronosdb && useradd -r -g kronosdb -m -d /data kronosdb

COPY --from=builder /kronosdb-server /usr/local/bin/kronosdb-server

ENV KRONOSDB_LISTEN=0.0.0.0:50051
ENV KRONOSDB_ADMIN_LISTEN=0.0.0.0:9240
ENV KRONOSDB_DATA_DIR=/data

USER kronosdb
VOLUME /data
EXPOSE 50051 9240

ENTRYPOINT ["tini", "--", "kronosdb-server"]

HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
    CMD wget -q --spider http://${KRONOSDB_ADMIN_LISTEN}/health || exit 1
