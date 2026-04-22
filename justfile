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
