use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Export the path where the `kronosdb-server` binary is expected to live so
    // integration tests can spawn it without an artifact dependency.
    // Stable Cargo does not expose `CARGO_BIN_EXE_*` for cross-package bins, and
    // kronosdb-server has no `[lib]` target (so a regular dev-dependency is ignored).
    // We do NOT invoke `cargo build` from this build script — doing so would be
    // re-entrant through the eventstore → server → eventstore dependency edge.
    // Instead the crash harness (tests/crash_harness/mod.rs) ensures the binary is
    // built once at the start of each test run via `cargo build -p kronosdb-server`.
    // `OUT_DIR` is `<target dir>/<profile>/build/<package>-<hash>/out`, so the
    // profile directory holding the workspace's binaries is three levels up.
    // Deriving it from there (rather than assuming `<workspace>/target`) keeps
    // the harness working under `CARGO_TARGET_DIR`, a configured `target-dir`,
    // or a shared target directory; assuming the default made every spawn fail
    // with `NotFound` in those setups.
    let out_dir = PathBuf::from(std::env::var("OUT_DIR")?);
    let profile_dir = out_dir
        .ancestors()
        .nth(3)
        .expect("OUT_DIR is <target>/<profile>/build/<package>/out");
    let bin_name = if cfg!(windows) {
        "kronosdb-server.exe"
    } else {
        "kronosdb-server"
    };
    let bin_path = profile_dir.join(bin_name);

    println!("cargo:rustc-env=KRONOSDB_SERVER_BIN={}", bin_path.display());
    println!("cargo:rerun-if-changed=build.rs");

    Ok(())
}
