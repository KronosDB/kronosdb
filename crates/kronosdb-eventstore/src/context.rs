use std::collections::HashMap;
use std::path::{Path, PathBuf};

use parking_lot::RwLock;

use crate::error::Error;
use crate::store::EventStoreEngine;

/// Manages multiple isolated event store contexts.
///
/// Each context is a fully independent event store with its own segments,
/// tag indices, and writer. Analogous to a "database" in PostgreSQL.
///
/// Context names must be valid directory names (alphanumeric + hyphens + underscores).
pub struct ContextManager {
    /// Root data directory. Each context gets a subdirectory.
    data_dir: PathBuf,

    /// Active contexts, keyed by name.
    contexts: RwLock<HashMap<String, EventStoreEngine>>,

    /// Default segment size for new contexts.
    default_segment_size: u64,
}

impl ContextManager {
    /// Creates a new context manager rooted at the given directory.
    pub fn new(data_dir: &Path, default_segment_size: u64) -> Result<Self, Error> {
        std::fs::create_dir_all(data_dir)?;

        let mut manager = Self {
            data_dir: data_dir.to_path_buf(),
            contexts: RwLock::new(HashMap::new()),
            default_segment_size,
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
        let store = EventStoreEngine::create_with_options(&context_dir, self.default_segment_size)?;
        contexts.insert(name.to_string(), store);

        Ok(())
    }

    /// Gets a reference to a context's event store for read operations.
    /// The callback receives a reference to the EventStoreEngine.
    pub fn with_context<F, R>(&self, name: &str, f: F) -> Result<R, Error>
    where
        F: FnOnce(&EventStoreEngine) -> Result<R, Error>,
    {
        let contexts = self.contexts.read();
        let store = contexts.get(name).ok_or_else(|| Error::ContextNotFound {
            name: name.to_string(),
        })?;
        f(store)
    }

    /// Gets a mutable reference to a context's event store for write operations.
    /// The callback receives a mutable reference to the EventStoreEngine.
    pub fn with_context_mut<F, R>(&self, name: &str, f: F) -> Result<R, Error>
    where
        F: FnOnce(&mut EventStoreEngine) -> Result<R, Error>,
    {
        let mut contexts = self.contexts.write();
        let store = contexts
            .get_mut(name)
            .ok_or_else(|| Error::ContextNotFound {
                name: name.to_string(),
            })?;
        f(store)
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
                .any(|e| {
                    e.path()
                        .extension()
                        .is_some_and(|ext| ext == "seg")
                });

            if has_segments {
                let store =
                    EventStoreEngine::open_with_options(&path, self.default_segment_size)?;
                contexts.insert(name, store);
            }
        }

        Ok(())
    }
}

/// Validates that a context name is safe to use as a directory name.
fn validate_context_name(name: &str) -> Result<(), Error> {
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
    use crate::event::{AppendEvent, Position};
    use crate::append::AppendRequest;
    use crate::segment::DEFAULT_SEGMENT_SIZE;
    use crate::event::Tag;

    fn tag(key: &str, value: &str) -> Tag {
        Tag::from_str(key, value)
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
            .with_context_mut("orders", |store| {
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
                assert_eq!(store.head(), Position(2));
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
            .with_context_mut("orders", |store| {
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
                assert_eq!(store.head(), Position(1));
                Ok(())
            })
            .unwrap();

        // Orders should have one event.
        manager
            .with_context("orders", |store| {
                assert_eq!(store.head(), Position(2));
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
                .with_context_mut("orders", |store| {
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
                    assert_eq!(store.head(), Position(2));
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
