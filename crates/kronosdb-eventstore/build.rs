use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_dir = "../../proto";

    // Existing: raft.proto — server+client for Raft transport.
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(&[format!("{proto_dir}/raft.proto")], &[proto_dir])?;

    // Phase 6 additive: compile eventstore.proto as CLIENT-ONLY stubs so the crash-test
    // harness can drive the server via gRPC without touching the server crate.
    // The server crate still compiles its own server+client stubs via its own build.rs —
    // this is a second, independent compilation into kronosdb-eventstore's OUT_DIR.
    tonic_build::configure()
        .build_server(false)
        .build_client(true)
        .compile_protos(
            &[
                format!("{proto_dir}/common.proto"),
                format!("{proto_dir}/eventstore.proto"),
            ],
            &[proto_dir],
        )?;

    // Phase 6 Task 1: export the path where the `kronosdb-server` binary is expected
    // to live so integration tests can spawn it without an artifact-dependency.
    // Stable Cargo does not expose `CARGO_BIN_EXE_*` for cross-package bins, and
    // kronosdb-server has no `[lib]` target (so a regular dev-dependency is ignored).
    // We do NOT invoke `cargo build` from this build script — doing so would be
    // re-entrant through the eventstore → server → eventstore dependency edge.
    // Instead the crash harness (tests/crash_harness/mod.rs) ensures the binary is
    // built once at the start of each test run via `cargo build -p kronosdb-server`.
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let workspace_root = manifest_dir
        .parent()
        .and_then(|p| p.parent())
        .expect("workspace root two levels above kronosdb-eventstore manifest");
    let profile = std::env::var("PROFILE").unwrap_or_else(|_| "debug".into());
    let bin_name = if cfg!(windows) {
        "kronosdb-server.exe"
    } else {
        "kronosdb-server"
    };
    let bin_path = workspace_root
        .join("target")
        .join(&profile)
        .join(bin_name);

    println!(
        "cargo:rustc-env=KRONOSDB_SERVER_BIN={}",
        bin_path.display()
    );
    println!("cargo:rerun-if-changed=build.rs");

    Ok(())
}
