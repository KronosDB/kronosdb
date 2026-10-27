# KronosDB development commands
# `just run` always recompiles before starting — no stale binaries.

# Kill any running kronosdb-server (SIGTERM, then SIGKILL after 2s)
stop:
    @pkill -f 'target.*kronosdb-server' 2>/dev/null && sleep 2 && pkill -9 -f 'target.*kronosdb-server' 2>/dev/null; echo "Stopped kronosdb-server" || echo "No kronosdb-server running"

# Run the server (kills existing instance, rebuilds, starts). Dev mode logs
# hardware findings (macOS, slow fsync) at info; --dev=false to see them loud.
run *ARGS: stop
    KRONOSDB_DEV=1 cargo run --bin kronosdb-server -- {{ARGS}}

# The kronos CLI; bare `just kronos` opens the terminal UI
kronos *ARGS:
    cargo run -q --bin kronos -- {{ARGS}}

# Install the kronos CLI into ~/.cargo/bin
kronos-install:
    cargo install --path crates/kronosdb-cli --locked

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
    KRONOSDB_DEV=1 cargo run --bin kronosdb-server -- {{ARGS}}

# Check compilation without producing a binary
check:
    cargo check

# Clippy lints
lint:
    cargo clippy --all-targets -- -D warnings

# Inspect a .seg file (dumps events or summary)
inspect-segment *ARGS:
    cargo run -q -p kronosdb-eventstore --bin inspect_segment -- {{ARGS}}

# Rebuild the embedded admin console stylesheet (requires bun).
# Run after changing Tailwind classes in crates/kronosdb-server/src/admin.
css:
    cd crates/kronosdb-server/assets && bun install && bun x @tailwindcss/cli -i app.css -o ../static/css/app.min.css --minify

# Regenerate the native protocol codecs, spec tables and fixtures from protocol/kronosdb.schema
protocol:
    cargo run -q -p kronosdb-protocol-gen -- gen

# Fail if anything generated from the protocol schema is stale, then run the conformance suites (Rust, TypeScript, Java)
protocol-check:
    cargo run -q -p kronosdb-protocol-gen -- check
    cargo test -q -p kronosdb-protocol
    cd protocol/ts && node --experimental-strip-types --no-warnings --test conformance.test.ts
    mvn -q -f protocol/java/pom.xml verify
