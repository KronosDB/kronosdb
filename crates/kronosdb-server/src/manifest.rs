//! Declarative manifest — resources ensured to exist at startup, and kept
//! in sync while running (the file is watched; additions apply live).
//!
//! The manifest is a TOML file declaring resources (today: contexts) that the
//! server guarantees exist before it starts serving. Applying it is
//! idempotent: existing resources are left untouched, missing ones are
//! created, and nothing is ever deleted (removing an entry from the manifest
//! does NOT remove the resource — deletion stays an explicit operation).
//!
//! ```toml
//! # kronosdb-manifest.toml
//! [[contexts]]
//! name = "orders"
//!
//! [[contexts]]
//! name = "payments"
//! ```
//!
//! Contexts are created node-locally BEFORE Raft initialization, the same way
//! the built-in "default" context is bootstrapped. In a cluster, every node
//! must therefore be given the same manifest (on Kubernetes: mount one
//! ConfigMap into every pod). This is deterministic across nodes and safe to
//! combine with runtime creation through consensus: a replicated
//! `CreateContext` applying over a manifest-created context is an idempotent
//! no-op.

use std::path::Path;

use serde::Deserialize;

use kronosdb_eventstore::context::ContextManager;

/// Parsed manifest file.
#[derive(Deserialize, Debug, Default, PartialEq)]
pub struct Manifest {
    /// Event store contexts that must exist.
    #[serde(default)]
    pub contexts: Vec<ContextSpec>,
}

/// A declared context. Only `name` today; a table (not a bare string) so
/// per-context settings can be added without breaking existing manifests.
#[derive(Deserialize, Debug, PartialEq)]
pub struct ContextSpec {
    pub name: String,
}

/// Loads and parses a manifest file.
pub fn load(path: &Path) -> Result<Manifest, Box<dyn std::error::Error>> {
    let contents = std::fs::read_to_string(path)
        .map_err(|e| format!("failed to read manifest '{}': {e}", path.display()))?;
    let manifest = toml::from_str::<Manifest>(&contents)
        .map_err(|e| format!("failed to parse manifest '{}': {e}", path.display()))?;
    Ok(manifest)
}

/// Declared contexts that do not exist yet.
pub fn missing(manifest: &Manifest, existing: &[String]) -> Vec<String> {
    manifest
        .contexts
        .iter()
        .filter(|spec| !existing.iter().any(|name| name == &spec.name))
        .map(|spec| spec.name.clone())
        .collect()
}

/// Existing contexts the manifest no longer declares.
///
/// Deliberately informational only: removing an entry never deletes or
/// unloads anything — the context keeps serving, replicating, and backing
/// up. This exists so GitOps drift is *visible* (logged on manifest change)
/// instead of silent. Deletion stays an explicit admin operation.
pub fn undeclared(manifest: &Manifest, existing: &[String]) -> Vec<String> {
    existing
        .iter()
        .filter(|name| {
            *name != "default" && !manifest.contexts.iter().any(|spec| &spec.name == *name)
        })
        .cloned()
        .collect()
}

/// Ensures every declared context exists. Returns the names that were
/// actually created (already-existing ones are skipped silently).
pub fn apply(
    manifest: &Manifest,
    contexts: &ContextManager,
) -> Result<Vec<String>, kronosdb_eventstore::error::Error> {
    let mut created = Vec::new();
    for spec in &manifest.contexts {
        if contexts.context_exists(&spec.name) {
            continue;
        }
        contexts.create_context(&spec.name)?;
        created.push(spec.name.clone());
    }
    Ok(created)
}

#[cfg(test)]
mod tests {
    use super::*;
    use kronosdb_eventstore::segment::DEFAULT_SEGMENT_SIZE;

    #[test]
    fn parses_contexts() {
        let manifest: Manifest = toml::from_str(
            r#"
[[contexts]]
name = "orders"

[[contexts]]
name = "payments"
"#,
        )
        .unwrap();
        assert_eq!(
            manifest
                .contexts
                .iter()
                .map(|c| &c.name)
                .collect::<Vec<_>>(),
            ["orders", "payments"]
        );
    }

    #[test]
    fn empty_manifest_is_valid() {
        let manifest: Manifest = toml::from_str("").unwrap();
        assert!(manifest.contexts.is_empty());
    }

    #[test]
    fn missing_and_undeclared_diff_against_existing() {
        let manifest: Manifest = toml::from_str(
            r#"
[[contexts]]
name = "orders"

[[contexts]]
name = "billing"
"#,
        )
        .unwrap();
        let existing = vec![
            "default".to_string(),
            "orders".to_string(),
            "legacy".to_string(),
        ];

        assert_eq!(missing(&manifest, &existing), ["billing"]);
        // "default" is never reported as drift; "legacy" is informational
        // only — undeclared never implies deletion or unloading.
        assert_eq!(undeclared(&manifest, &existing), ["legacy"]);
    }

    #[test]
    fn apply_is_idempotent_and_reports_created() {
        let dir = tempfile::tempdir().unwrap();
        let contexts = ContextManager::new(dir.path(), DEFAULT_SEGMENT_SIZE).unwrap();
        contexts.create_context("default").unwrap();

        let manifest: Manifest = toml::from_str(
            r#"
[[contexts]]
name = "default"

[[contexts]]
name = "orders"
"#,
        )
        .unwrap();

        let created = apply(&manifest, &contexts).unwrap();
        assert_eq!(created, ["orders"]);
        assert!(contexts.context_exists("orders"));

        // Second apply creates nothing.
        let created = apply(&manifest, &contexts).unwrap();
        assert!(created.is_empty());
    }

    #[test]
    fn apply_rejects_invalid_names() {
        let dir = tempfile::tempdir().unwrap();
        let contexts = ContextManager::new(dir.path(), DEFAULT_SEGMENT_SIZE).unwrap();

        let manifest: Manifest = toml::from_str(
            r#"
[[contexts]]
name = "bad/name"
"#,
        )
        .unwrap();
        assert!(apply(&manifest, &contexts).is_err());
    }
}
