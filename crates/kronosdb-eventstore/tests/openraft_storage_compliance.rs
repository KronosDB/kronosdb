//! openraft's storage conformance suite, run against the production
//! `LogStore` + `EventStoreStateMachine` pair.
//!
//! openraft's correctness assumes its storage contract holds: vote durability,
//! truncate/purge semantics, log-state reporting, snapshot behavior, and
//! last-applied consistency. The unit tests in `raft/log_store.rs` are
//! self-authored; this suite checks the same implementations against the
//! contract as openraft itself defines it, deterministically.
//!
//! `Suite::test_all` is synchronous and builds its own tokio runtime per
//! case, so this must stay a plain `#[test]`.

use std::sync::Arc;

use openraft::StorageError;
use openraft::testing::{StoreBuilder, Suite};
use tempfile::TempDir;

use kronosdb_eventstore::context::ContextManager;
use kronosdb_eventstore::raft::log_store::{LogStore, LogStoreConfig};
use kronosdb_eventstore::raft::snapshot_store::SnapshotStore;
use kronosdb_eventstore::raft::state_machine::{AppliedControlState, EventStoreStateMachine};
use kronosdb_eventstore::raft::types::{NodeId, TypeConfig};

struct Builder;

impl StoreBuilder<TypeConfig, LogStore, EventStoreStateMachine, TempDir> for Builder {
    async fn build(
        &self,
    ) -> Result<(TempDir, LogStore, EventStoreStateMachine), StorageError<NodeId>> {
        let tmp = tempfile::tempdir().expect("tempdir");
        let raft_dir = tmp.path().join("raft");
        let log_store = LogStore::new(&raft_dir, LogStoreConfig::default()).expect("log store");
        let contexts = Arc::new(
            ContextManager::new(&tmp.path().join("data"), 16 * 1024 * 1024).expect("contexts"),
        );
        let snapshots =
            Arc::new(SnapshotStore::new(raft_dir.join("snapshots")).expect("snapshot store"));
        let (control_updates, _) = tokio::sync::watch::channel(AppliedControlState::default());
        let state_machine = EventStoreStateMachine::new(contexts, snapshots, control_updates)
            .expect("state machine");
        Ok((tmp, log_store, state_machine))
    }
}

#[test]
fn openraft_storage_compliance() {
    Suite::test_all(Builder).expect("openraft storage compliance suite");
}
