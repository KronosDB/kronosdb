use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use parking_lot::RwLock;

use crate::error::Error;
use crate::snapshot::SnapshotStore;
use crate::store::{EventStoreEngine, StoreOptions};

struct ContextEntry {
    engine: Arc<EventStoreEngine>,
    snapshots: Arc<SnapshotStore>,
}

/// Manages multiple isolated event store contexts.
///
/// Each context is a fully independent event store with its own segments,
/// tag indices, and writer. Analogous to a "database" in PostgreSQL.
///
/// Context names must be valid directory names (alphanumeric + hyphens + underscores).
pub struct ContextManager {
    /// Root data directory. Each context gets a subdirectory.
    data_dir: PathBuf,

    /// Fully initialized contexts, published atomically by name.
    contexts: RwLock<HashMap<String, ContextEntry>>,

    /// Store options for creating new contexts.
    store_options: StoreOptions,

    /// Exclusive advisory lock on `<data_dir>/LOCK`, held for the manager's
    /// lifetime (released by the OS when the process exits, even on crash).
    /// Fences the data dir: a second process — e.g. two pods mounting the
    /// same volume — fails fast instead of interleaving appends into the
    /// same active segment and corrupting it.
    _lock_file: std::fs::File,
}

impl ContextManager {
    /// Creates a new context manager rooted at the given directory.
    pub fn new(data_dir: &Path, default_segment_size: u64) -> Result<Self, Error> {
        Self::with_options(
            data_dir,
            StoreOptions {
                max_segment_size: default_segment_size,
                ..Default::default()
            },
        )
    }

    /// Creates a new context manager with full store options.
    ///
    /// Takes an exclusive advisory lock on `<data_dir>/LOCK` and fails fast
    /// if another live process already holds it.
    pub fn with_options(data_dir: &Path, store_options: StoreOptions) -> Result<Self, Error> {
        std::fs::create_dir_all(data_dir)?;

        let lock_path = data_dir.join("LOCK");
        let lock_file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(&lock_path)?;
        fs2::FileExt::try_lock_exclusive(&lock_file).map_err(|e| {
            Error::Io(std::io::Error::other(format!(
                "data dir '{}' is locked by another running process ({e}); \
                 refusing to open it twice — concurrent writers would corrupt segments",
                data_dir.display(),
            )))
        })?;

        let mut manager = Self {
            data_dir: data_dir.to_path_buf(),
            contexts: RwLock::new(HashMap::new()),
            store_options,
            _lock_file: lock_file,
        };

        // Auto-discover and open existing contexts.
        manager.discover_contexts()?;

        Ok(manager)
    }

    /// Creates a new context with the given name.
    /// Returns an error if the context already exists.
    pub fn create_context(&self, name: &str) -> Result<(), Error> {
        validate_context_name(name)?;

        let mut contexts = self.contexts.write();
        if contexts.contains_key(name) {
            return Err(Error::ContextAlreadyExists {
                name: name.to_string(),
            });
        }

        let context_dir = self.data_dir.join(name);
        let engine = Arc::new(EventStoreEngine::create_with_store_options(
            &context_dir,
            &self.store_options,
        )?);
        let snapshots = Arc::new(SnapshotStore::open(&context_dir.join("snapshots"))?);
        contexts.insert(name.to_string(), ContextEntry { engine, snapshots });

        Ok(())
    }

    /// Gets a reference to a context's event store.
    /// All operations (including append) take `&self`, so no mutable access needed.
    pub fn with_context<F, R>(&self, name: &str, f: F) -> Result<R, Error>
    where
        F: FnOnce(&EventStoreEngine) -> Result<R, Error>,
    {
        let contexts = self.contexts.read();
        let entry = contexts.get(name).ok_or_else(|| Error::ContextNotFound {
            name: name.to_string(),
        })?;
        f(&entry.engine)
    }

    /// Returns the root data directory.
    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    /// Gets a cloneable reference to a context's event store.
    /// Useful when you need to pass the store across thread boundaries
    /// (e.g., `spawn_blocking`).
    pub fn get_context(&self, name: &str) -> Result<Arc<EventStoreEngine>, Error> {
        let contexts = self.contexts.read();
        contexts
            .get(name)
            .map(|entry| Arc::clone(&entry.engine))
            .ok_or_else(|| Error::ContextNotFound {
                name: name.to_string(),
            })
    }

    /// Gets a cloneable reference to a context's snapshot store.
    pub fn get_snapshot_store(&self, name: &str) -> Result<Arc<SnapshotStore>, Error> {
        let contexts = self.contexts.read();
        contexts
            .get(name)
            .map(|entry| Arc::clone(&entry.snapshots))
            .ok_or_else(|| Error::ContextNotFound {
                name: name.to_string(),
            })
    }

    /// Lists all context names.
    pub fn list_contexts(&self) -> Vec<String> {
        let contexts = self.contexts.read();
        let mut names: Vec<String> = contexts.keys().cloned().collect();
        names.sort();
        names
    }

    /// Checks if a context exists.
    pub fn context_exists(&self, name: &str) -> bool {
        let contexts = self.contexts.read();
        contexts.contains_key(name)
    }

    /// Initiates shutdown on every context's engine: appends are rejected
    /// and each group-commit sync thread does a final fsync pass, releasing
    /// in-flight writers. Part of graceful termination — acked writes are
    /// already durable, this just prevents new work from hanging.
    pub fn shutdown_all(&self) {
        let contexts = self.contexts.read();
        for entry in contexts.values() {
            entry.engine.shutdown();
        }
    }

    /// Discovers and opens existing contexts from the data directory.
    fn discover_contexts(&mut self) -> Result<(), Error> {
        let mut contexts = self.contexts.write();

        for entry in std::fs::read_dir(&self.data_dir)? {
            let entry = entry?;
            let path = entry.path();

            if !path.is_dir() {
                continue;
            }

            let name = match path.file_name().and_then(|n| n.to_str()) {
                Some(n) => n.to_string(),
                None => continue,
            };

            // Skip hidden directories and invalid names.
            if name.starts_with('.') || validate_context_name(&name).is_err() {
                continue;
            }

            // Check if this directory looks like an event store (has .seg files).
            let has_segments = std::fs::read_dir(&path)?
                .filter_map(|e| e.ok())
                .any(|e| e.path().extension().is_some_and(|ext| ext == "seg"));

            if has_segments {
                let engine = Arc::new(EventStoreEngine::open_with_store_options(
                    &path,
                    &self.store_options,
                )?);
                let snapshots = Arc::new(SnapshotStore::open(&path.join("snapshots"))?);
                contexts.insert(name, ContextEntry { engine, snapshots });
            }
        }

        Ok(())
    }
}

/// Validates that a context name is safe to use as a directory name.
pub(crate) fn validate_context_name(name: &str) -> Result<(), Error> {
    if name.is_empty() {
        return Err(Error::InvalidContextName {
            name: name.to_string(),
            reason: "name cannot be empty".to_string(),
        });
    }
    if name.len() > 128 {
        return Err(Error::InvalidContextName {
            name: name.to_string(),
            reason: "name cannot exceed 128 characters".to_string(),
        });
    }
    if !name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
    {
        return Err(Error::InvalidContextName {
            name: name.to_string(),
            reason: "name must contain only alphanumeric characters, hyphens, and underscores"
                .to_string(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::append::AppendRequest;
    use crate::event::Tag;
    use crate::event::{AppendEvent, Position};
    use crate::segment::DEFAULT_SEGMENT_SIZE;

    fn tag(key: &str, value: &str) -> Tag {
        Tag::from_str(key, value)
    }

    /// Fencing: a second manager on the same data dir must fail fast, and
    /// dropping the first must release the lock.
    #[test]
    fn second_manager_on_same_data_dir_is_fenced() {
        let dir = tempfile::tempdir().unwrap();

        let first = ContextManager::new(dir.path(), DEFAULT_SEGMENT_SIZE).unwrap();
        let second = ContextManager::new(dir.path(), DEFAULT_SEGMENT_SIZE);
        let err = match second {
            Ok(_) => panic!("second open must be fenced"),
            Err(e) => e,
        };
        assert!(err.to_string().contains("locked by another"), "got: {err}");

        drop(first);
        ContextManager::new(dir.path(), DEFAULT_SEGMENT_SIZE)
            .expect("lock must be released on drop");
    }

    fn make_event(name: &str, tags: Vec<Tag>) -> AppendEvent {
        AppendEvent {
            identifier: format!("id-{name}"),
            name: name.into(),
            version: "1.0".into(),
            timestamp: 1712345678000,
            payload: b"data".to_vec(),
            metadata: vec![],
            tags,
        }
    }

    #[test]
    fn create_and_use_context() {
        let dir = tempfile::tempdir().unwrap();
        let manager = ContextManager::new(dir.path(), DEFAULT_SEGMENT_SIZE).unwrap();

        manager.create_context("orders").unwrap();
        assert!(manager.context_exists("orders"));

        // Append to the context.
        manager
            .with_context("orders", |store| {
                store.append(AppendRequest {
                    condition: None,
                    events: vec![make_event("OrderPlaced", vec![tag("orderId", "A")])],
                })?;
                Ok(())
            })
            .unwrap();

        // Read from the context.
        manager
            .with_context("orders", |store| {
                assert_eq!(store.head(), Position(1));
                Ok(())
            })
            .unwrap();
    }

    #[test]
    fn contexts_are_isolated() {
        let dir = tempfile::tempdir().unwrap();
        let manager = ContextManager::new(dir.path(), DEFAULT_SEGMENT_SIZE).unwrap();

        manager.create_context("orders").unwrap();
        manager.create_context("payments").unwrap();

        // Append to orders.
        manager
            .with_context("orders", |store| {
                store.append(AppendRequest {
                    condition: None,
                    events: vec![make_event("OrderPlaced", vec![tag("orderId", "A")])],
                })?;
                Ok(())
            })
            .unwrap();

        // Payments should be empty.
        manager
            .with_context("payments", |store| {
                assert_eq!(store.head(), Position(0));
                Ok(())
            })
            .unwrap();

        // Orders should have one event.
        manager
            .with_context("orders", |store| {
                assert_eq!(store.head(), Position(1));
                Ok(())
            })
            .unwrap();
    }

    #[test]
    fn context_not_found() {
        let dir = tempfile::tempdir().unwrap();
        let manager = ContextManager::new(dir.path(), DEFAULT_SEGMENT_SIZE).unwrap();

        let result = manager.with_context("nonexistent", |store| Ok(store.head()));
        assert!(matches!(result, Err(Error::ContextNotFound { .. })));
    }

    #[test]
    fn duplicate_context_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let manager = ContextManager::new(dir.path(), DEFAULT_SEGMENT_SIZE).unwrap();

        manager.create_context("orders").unwrap();
        let result = manager.create_context("orders");
        assert!(matches!(result, Err(Error::ContextAlreadyExists { .. })));
    }

    #[test]
    fn invalid_context_names() {
        let dir = tempfile::tempdir().unwrap();
        let manager = ContextManager::new(dir.path(), DEFAULT_SEGMENT_SIZE).unwrap();

        assert!(manager.create_context("").is_err());
        assert!(manager.create_context("has spaces").is_err());
        assert!(manager.create_context("has/slashes").is_err());
        assert!(manager.create_context("has.dots").is_err());

        // Valid names.
        assert!(manager.create_context("orders").is_ok());
        assert!(manager.create_context("my-context").is_ok());
        assert!(manager.create_context("context_v2").is_ok());
    }

    #[test]
    fn discover_contexts_on_open() {
        let dir = tempfile::tempdir().unwrap();

        // Create some contexts.
        {
            let manager = ContextManager::new(dir.path(), DEFAULT_SEGMENT_SIZE).unwrap();
            manager.create_context("orders").unwrap();
            manager.create_context("payments").unwrap();

            manager
                .with_context("orders", |store| {
                    store.append(AppendRequest {
                        condition: None,
                        events: vec![make_event("OrderPlaced", vec![tag("orderId", "A")])],
                    })?;
                    Ok(())
                })
                .unwrap();
        }

        // Reopen — contexts should be auto-discovered.
        {
            let manager = ContextManager::new(dir.path(), DEFAULT_SEGMENT_SIZE).unwrap();
            let contexts = manager.list_contexts();
            assert!(contexts.contains(&"orders".to_string()));
            // payments has no segments (empty store creates one), so it should be discovered too.

            manager
                .with_context("orders", |store| {
                    assert_eq!(store.head(), Position(1));
                    Ok(())
                })
                .unwrap();
        }
    }

    #[test]
    fn list_contexts() {
        let dir = tempfile::tempdir().unwrap();
        let manager = ContextManager::new(dir.path(), DEFAULT_SEGMENT_SIZE).unwrap();

        assert_eq!(manager.list_contexts(), Vec::<String>::new());

        manager.create_context("beta").unwrap();
        manager.create_context("alpha").unwrap();

        let list = manager.list_contexts();
        assert_eq!(list, vec!["alpha", "beta"]); // Sorted.
    }
}
