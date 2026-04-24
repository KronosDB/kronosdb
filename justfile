# KronosDB development commands
# `just run` always recompiles before starting — no stale binaries.

# Kill any running kronosdb-server (SIGTERM, then SIGKILL after 2s)
stop:
    @pkill -f 'target.*kronosdb-server' 2>/dev/null && sleep 2 && pkill -9 -f 'target.*kronosdb-server' 2>/dev/null; echo "Stopped kronosdb-server" || echo "No kronosdb-server running"

# Run the server (kills existing instance, rebuilds, starts)
run *ARGS: stop
    cargo run --bin kronosdb-server -- {{ARGS}}

# Run in release mode
run-release *ARGS: stop
    cargo run --release --bin kronosdb-server -- {{ARGS}}

# Build (debug)
build:
    cargo build

# Build (release)
build-release:
    cargo build --release

# Run all tests
test:
    cargo test

# Run tests for a specific crate
test-crate CRATE:
    cargo test -p {{CRATE}}

# Wipe data and run fresh
fresh *ARGS: stop
    rm -rf data/
    cargo run --bin kronosdb-server -- {{ARGS}}

# Check compilation without producing a binary
check:
    cargo check

# Clippy lints
lint:
    cargo clippy --all-targets -- -D warnings

# Build the Axon connector (uses shared proto/ files)
connector-build:
    mvn -f connectors/axon/pom.xml compile

# Install the Axon connector to local Maven repo
connector-install:
    mvn -f connectors/axon/pom.xml install -DskipTests

# Build everything (server + connector)
build-all: build connector-install

# Inspect a .seg file (dumps events or summary)
inspect-segment *ARGS:
    cargo run -q -p kronosdb-eventstore --bin inspect_segment -- {{ARGS}}

# Phase 1 baseline: run the Raft-append baseline bench on 4dcffcd and regenerate
# .planning/phases/01-baseline/BASELINE.md + baseline-4dcffcd.json.
#
# Clears previous per-run JSONL records, runs the Criterion bench with the
# bench-instrumentation feature enabled (auto-enabled via kronosdb-bench's dep),
# then aggregates. Safe to re-run; overwrites both artifacts.
bench-baseline:
    @echo "-> clearing previous baseline records"
    @rm -rf target/baseline-records
    @echo "-> running raft_append_baseline (this can take several minutes)"
    cargo bench -p kronosdb-bench --bench raft_append_baseline
    @echo "-> aggregating into .planning/phases/01-baseline/"
    cargo run -q -p kronosdb-bench --bin aggregate_baseline -- \
        --records target/baseline-records \
        --out-dir .planning/phases/01-baseline \
        --commit 4dcffcd
    @echo "OK .planning/phases/01-baseline/BASELINE.md and baseline-4dcffcd.json updated"

# Phase 7 Linux gate bench: runs raft_append_baseline inside the same orbstack
# Docker container Phase 6 tied off on (kernel 6.19.13 aarch64, rust:1-bookworm).
# Uses named volume `kronosdb-linux-target` for /src/target to avoid virtiofs on
# the durability-sensitive path (see .planning/phases/06-crash-tests/06-LINUX-RUN.md).
# Writes aggregated output to
# .planning/phases/07-benchmarks-reassessment/phase-7-linux.json (renamed from
# the aggregator's default baseline-<commit>.json filename per Phase 7 D-14).
#
# Preconditions:
#   - orbstack (or Docker Desktop) is running
#   - docker CLI is on PATH
#
# Re-runnable; clears previous records in the named volume as needed.
# Phase 7 Wave 2 (Plan 07-04) extends this or invokes it alongside the
# log_store_only and raft_append_3node benches.
bench-linux:
    @echo "-> running raft_append_baseline inside orbstack Docker (rust:1-bookworm)"
    docker run --rm \
        -v "$PWD":/work:ro \
        -v kronosdb-linux-target:/src/target \
        -w /src \
        rust:1-bookworm bash -c '\
            set -euo pipefail; \
            apt-get update -qq && apt-get install -y -qq protobuf-compiler rsync >/dev/null; \
            rsync -a --delete --exclude=target --exclude=.git /work/ /src/; \
            rm -rf /src/target/baseline-records; \
            cargo bench -p kronosdb-bench --bench raft_append_baseline; \
            cargo run -q -p kronosdb-bench --bin aggregate_baseline -- \
                --records /src/target/baseline-records \
                --out-dir /src/.planning/phases/07-benchmarks-reassessment \
                --commit phase-7-linux; \
        '
    @echo "-> copying aggregator output out of the container volume"
    docker run --rm \
        -v "$PWD":/work \
        -v kronosdb-linux-target:/src/target \
        -w /src \
        rust:1-bookworm bash -c '\
            set -euo pipefail; \
            cp /src/.planning/phases/07-benchmarks-reassessment/baseline-phase-7-linux.json \
               /work/.planning/phases/07-benchmarks-reassessment/phase-7-linux.json; \
            rm -f /src/.planning/phases/07-benchmarks-reassessment/baseline-phase-7-linux.json; \
            rm -f /src/.planning/phases/07-benchmarks-reassessment/BASELINE.md; \
        '
    @echo "OK .planning/phases/07-benchmarks-reassessment/phase-7-linux.json written"
