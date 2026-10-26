//! Generates everything derived from `protocol/kronosdb.schema`:
//!
//! - `crates/kronosdb-protocol/src/generated.rs` — the Rust codec
//! - `protocol/ts/kronosdb-protocol.ts` and `kronosdb-protocol-json.ts` — the TypeScript codec
//! - `protocol/java/src/...` — the Java codec and its JSON bridge for the conformance suite
//! - the message reference in `protocol/README.md`
//! - `protocol/fixtures/*.json` + `*.bin` — golden frames, from `protocol/fixtures/source/*.json`
//!
//! `protocol-gen gen` writes them; `protocol-gen check` fails if any is stale.

mod dynamic;
mod java;
mod rust;
mod schema;
mod spec;
mod ts;

use std::collections::BTreeMap;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode, Stdio};

fn root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

/// Dotfiles (editor swap files, macOS `._` sidecars) are never fixtures.
fn is_hidden(path: &Path) -> bool {
    path.file_name()
        .is_some_and(|n| n.to_string_lossy().starts_with('.'))
}

fn rustfmt(src: &str) -> Result<String, String> {
    let mut child = Command::new("rustfmt")
        .args(["--edition", "2024", "--emit", "stdout"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| format!("running rustfmt: {e}"))?;
    child
        .stdin
        .take()
        .unwrap()
        .write_all(src.as_bytes())
        .map_err(|e| e.to_string())?;
    let out = child.wait_with_output().map_err(|e| e.to_string())?;
    if !out.status.success() {
        return Err(format!(
            "rustfmt rejected the generated code:\n{}",
            String::from_utf8_lossy(&out.stderr)
        ));
    }
    String::from_utf8(out.stdout).map_err(|e| e.to_string())
}

/// Every output file and its expected content, text or binary.
fn outputs(root: &Path) -> Result<BTreeMap<PathBuf, Vec<u8>>, String> {
    let schema_src = std::fs::read_to_string(root.join("protocol/kronosdb.schema"))
        .map_err(|e| format!("reading the schema: {e}"))?;
    let schema = schema::parse(&schema_src).map_err(|e| e.to_string())?;
    let hash = format!("{:08x}", crc32c::crc32c(schema_src.as_bytes()));

    let mut out = BTreeMap::new();
    out.insert(
        root.join("crates/kronosdb-protocol/src/generated.rs"),
        rustfmt(&rust::emit(&schema, &hash))?.into_bytes(),
    );
    out.insert(
        root.join("protocol/ts/kronosdb-protocol.ts"),
        ts::emit_codec(&schema, &hash).into_bytes(),
    );
    out.insert(
        root.join("protocol/ts/kronosdb-protocol-json.ts"),
        ts::emit_json(&schema).into_bytes(),
    );
    for file in java::emit_codec(&schema, &hash)
        .into_iter()
        .chain(java::emit_json(&schema))
    {
        out.insert(
            root.join("protocol/java").join(&file.path),
            file.content.into_bytes(),
        );
    }
    let readme_path = root.join("protocol/README.md");
    let readme =
        std::fs::read_to_string(&readme_path).map_err(|e| format!("reading README.md: {e}"))?;
    out.insert(
        readme_path,
        spec::splice(&readme, &spec::emit(&schema))?.into_bytes(),
    );

    let source_dir = root.join("protocol/fixtures/source");
    let mut sources: Vec<_> = std::fs::read_dir(&source_dir)
        .map_err(|e| format!("reading {}: {e}", source_dir.display()))?
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().is_some_and(|x| x == "json") && !is_hidden(p))
        .collect();
    sources.sort();
    for path in sources {
        let name = path.file_stem().unwrap().to_string_lossy().to_string();
        let text = std::fs::read_to_string(&path).map_err(|e| e.to_string())?;
        let fixture: serde_json::Value =
            serde_json::from_str(&text).map_err(|e| format!("{name}: {e}"))?;
        let bin =
            dynamic::encode_frame(&schema, &fixture).map_err(|e| format!("fixture {name}: {e}"))?;
        // The canonical form is what the reference decoder reads back: every
        // conformance suite compares against exactly this.
        let mut canonical =
            dynamic::decode_frame(&schema, &bin).map_err(|e| format!("fixture {name}: {e}"))?;
        canonical["description"] = fixture["description"].clone();
        let mut json = serde_json::to_string_pretty(&canonical).unwrap();
        json.push('\n');
        let dir = root.join("protocol/fixtures");
        out.insert(dir.join(format!("{name}.bin")), bin);
        out.insert(dir.join(format!("{name}.json")), json.into_bytes());
    }
    Ok(out)
}

fn main() -> ExitCode {
    let mode = std::env::args().nth(1).unwrap_or_else(|| "gen".into());
    let root = root();
    let outputs = match outputs(&root) {
        Ok(o) => o,
        Err(e) => {
            eprintln!("protocol-gen: {e}");
            return ExitCode::FAILURE;
        }
    };
    let display = |p: &Path| p.strip_prefix(&root).unwrap_or(p).display().to_string();
    match mode.as_str() {
        "gen" => {
            for (path, content) in &outputs {
                if std::fs::read(path).ok().as_deref() == Some(content.as_slice()) {
                    continue;
                }
                std::fs::create_dir_all(path.parent().unwrap()).unwrap();
                std::fs::write(path, content).unwrap();
                println!("wrote {}", display(path));
            }
            ExitCode::SUCCESS
        }
        "check" => {
            let stale: Vec<_> = outputs
                .iter()
                .filter(|(path, content)| {
                    std::fs::read(path).ok().as_deref() != Some(content.as_slice())
                })
                .map(|(path, _)| display(path))
                .collect();
            if stale.is_empty() {
                println!("protocol outputs are up to date");
                ExitCode::SUCCESS
            } else {
                eprintln!("stale protocol outputs (run `just protocol`):");
                for s in stale {
                    eprintln!("  {s}");
                }
                ExitCode::FAILURE
            }
        }
        other => {
            eprintln!("usage: protocol-gen [gen|check] (got `{other}`)");
            ExitCode::FAILURE
        }
    }
}
