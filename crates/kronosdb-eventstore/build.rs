use std::path::PathBuf;
use std::process::Command;

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

    // Phase 6 Task 1: export the path to the `kronosdb-server` binary so integration
    // tests can spawn it without an artifact-dependency (stable Cargo does not expose
    // CARGO_BIN_EXE_* for cross-package bins, and kronosdb-server has no `[lib]` target
    // so a regular dev-dependency is ignored). This derives the path from
    // `CARGO_MANIFEST_DIR` (…/crates/kronosdb-eventstore) → …/target/<profile>/kronosdb-server
    // and, if the binary is missing, runs `cargo build -p kronosdb-server` to produce it.
    // Idempotent: no-op when the binary is already built.
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

    if !bin_path.exists() {
        // Build the server binary with the same profile as the current compile unit.
        let mut cmd = Command::new(std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into()));
        cmd.current_dir(workspace_root)
            .arg("build")
            .arg("-p")
            .arg("kronosdb-server")
            .arg("--bin")
            .arg("kronosdb-server");
        if profile == "release" {
            cmd.arg("--release");
        }
        let status = cmd.status()?;
        if !status.success() {
            return Err(format!("cargo build -p kronosdb-server exited with {status}").into());
        }
    }

    println!(
        "cargo:rustc-env=KRONOSDB_SERVER_BIN={}",
        bin_path.display()
    );
    // Re-run build.rs if the binary is removed/rebuilt.
    println!("cargo:rerun-if-changed={}", bin_path.display());

    Ok(())
}
