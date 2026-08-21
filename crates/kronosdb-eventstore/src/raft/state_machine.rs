// openraft's `StorageError` is the trait-fixed error type for this module.
#![allow(clippy::result_large_err)]

use std::io::{BufReader, BufWriter, Seek, SeekFrom};
use std::sync::Arc;

use openraft::storage::RaftStateMachine;
use openraft::{
    Entry, EntryPayload, LogId, OptionalSend, RaftLogId, RaftSnapshotBuilder, Snapshot,
    SnapshotMeta, StorageError, StoredMembership,
};

use crate::context::ContextManager;
use crate::error::Error;

use super::handler_registry::HandlerRoutingTable;
use super::snapshot_format::{MetadataSnapshot, read_snapshot, write_snapshot};
use super::snapshot_store::{PersistedControlState, SnapshotStore};
use super::types::{LeaderClaim, NodeId, RaftRequest, RaftResponse, TypeConfig};

#[derive(Debug, Clone, Default)]
pub struct AppliedControlState {
    pub last_applied: Option<LogId<NodeId>>,
    pub leader_claim: Option<LeaderClaim>,
}

/// Metadata-only Raft state machine.
///
/// Event appends are intentionally absent: native segment replication is the
/// data plane. This state machine owns only context creation, fencing leader
/// claims, and openraft membership/progress.
pub struct EventStoreStateMachine {
    contexts: Arc<ContextManager>,
    snapshot_store: Arc<SnapshotStore>,
    last_applied: Option<LogId<NodeId>>,
    last_membership: StoredMembership<NodeId, openraft::BasicNode>,
    leader_claim: Option<LeaderClaim>,
    /// Replicated messaging-handler routing table (ADR-0007). Shared with
    /// `ClusterManager` so dispatch paths read the same applied state.
    handler_routing: Arc<HandlerRoutingTable>,
    control_updates: tokio::sync::watch::Sender<AppliedControlState>,
}

impl EventStoreStateMachine {
    pub fn new(
        contexts: Arc<ContextManager>,
        snapshot_store: Arc<SnapshotStore>,
        handler_routing: Arc<HandlerRoutingTable>,
        control_updates: tokio::sync::watch::Sender<AppliedControlState>,
    ) -> Result<Self, Error> {
        let durable = snapshot_store.load_control_state().map_err(Error::Io)?;
        let latest_snapshot = snapshot_store.open_latest().map_err(Error::Io)?;
        let snapshot_state = match latest_snapshot.as_ref() {
            Some((_meta, path)) => {
                let file = std::fs::File::open(path).map_err(Error::Io)?;
                Some(read_snapshot(BufReader::new(file)).map_err(Error::Io)?)
            }
            None => None,
        };

        // Prefer whichever durable carrier is further ahead. SnapshotMeta has
        // the full LogId, while control-state.bin closes the between-snapshot
        // replay window. Comparing indices is sufficient because both values
        // describe this one linear applied history.
        let snapshot_log_id = latest_snapshot
            .as_ref()
            .and_then(|(meta, _)| meta.last_log_id);
        let durable_is_newer = match (&durable, snapshot_log_id) {
            (Some(state), Some(snapshot_id)) => state
                .last_applied
                .map(|id| id.index >= snapshot_id.index)
                .unwrap_or(false),
            (Some(_), None) => true,
            (None, _) => false,
        };

        let (last_applied, recovered_membership, metadata) = if durable_is_newer {
            let state = durable.expect("checked above");
            (state.last_applied, state.last_membership, state.metadata)
        } else if let Some((meta, _)) = latest_snapshot.as_ref() {
            (
                meta.last_log_id,
                meta.last_membership.clone(),
                snapshot_state.unwrap_or_default(),
            )
        } else {
            (
                None,
                StoredMembership::default(),
                MetadataSnapshot::default(),
            )
        };

        // Choose the newest membership that does not lie beyond last_applied.
        // This handles both pre-snapshot membership.bin recovery and the crash
        // window where a newer snapshot was committed just before its sidecar
        // membership was refreshed.
        let persisted_membership = snapshot_store.load_membership().map_err(Error::Io)?;
        let last_membership = match persisted_membership {
            Some(persisted)
                if persisted
                    .log_id()
                    .as_ref()
                    .map(|membership_id| {
                        last_applied
                            .as_ref()
                            .map(|applied_id| membership_id.index <= applied_id.index)
                            .unwrap_or(false)
                    })
                    .unwrap_or(true)
                    && persisted.log_id() >= recovered_membership.log_id() =>
            {
                persisted
            }
            _ => recovered_membership,
        };

        for name in &metadata.contexts {
            Self::create_context_idempotent(&contexts, name)?;
        }
        // Provisional restore: stale rows are removed by each node's startup
        // ClearNodeHandlers entry and by membership diffs.
        handler_routing.restore(metadata.handlers);

        tracing::info!(
            target: "raft.recovery",
            ?last_applied,
            leader_epoch = metadata.leader_claim.as_ref().map(|claim| claim.epoch),
            voter_count = last_membership.membership().voter_ids().count(),
            "metadata state machine recovered"
        );

        let state = Self {
            contexts,
            snapshot_store,
            last_applied,
            last_membership,
            leader_claim: metadata.leader_claim,
            handler_routing,
            control_updates,
        };
        state.publish_control_state();
        Ok(state)
    }

    fn create_context_idempotent(contexts: &ContextManager, name: &str) -> Result<(), Error> {
        match contexts.create_context(name) {
            Ok(()) | Err(Error::ContextAlreadyExists { .. }) => Ok(()),
            Err(error) => Err(error),
        }
    }

    fn metadata_snapshot(&self) -> MetadataSnapshot {
        MetadataSnapshot {
            contexts: self.contexts.list_contexts(),
            leader_claim: self.leader_claim.clone(),
            handlers: self.handler_routing.rows(),
        }
    }

    fn publish_control_state(&self) {
        self.control_updates.send_replace(AppliedControlState {
            last_applied: self.last_applied,
            leader_claim: self.leader_claim.clone(),
        });
    }

    fn persist_control_state(&self) -> Result<(), StorageError<NodeId>> {
        let state = PersistedControlState {
            last_applied: self.last_applied,
            last_membership: self.last_membership.clone(),
            metadata: self.metadata_snapshot(),
        };
        self.snapshot_store
            .save_control_state(&state)
            .map_err(|error| {
                StorageError::from_io_error(
                    openraft::ErrorSubject::StateMachine,
                    openraft::ErrorVerb::Write,
                    error,
                )
            })
    }

    fn apply_request(
        &mut self,
        request: RaftRequest,
        log_id: LogId<NodeId>,
    ) -> Result<RaftResponse, StorageError<NodeId>> {
        match request {
            RaftRequest::CreateContext { name } => {
                Self::create_context_idempotent(&self.contexts, &name).map_err(|error| {
                    StorageError::from_io_error(
                        openraft::ErrorSubject::StateMachine,
                        openraft::ErrorVerb::Write,
                        std::io::Error::other(format!(
                            "apply CreateContext({name}) failed: {error}"
                        )),
                    )
                })?;
                Ok(RaftResponse::ContextCreated)
            }
            RaftRequest::LeaderClaim {
                node_id,
                term,
                prior_epoch,
                voters,
                per_context_tails,
            } => {
                // The applied log index is the fencing token. It is allocated
                // by consensus and therefore cannot be supplied or reused by a
                // stale data-plane leader.
                let epoch = log_id.index;
                self.leader_claim = Some(LeaderClaim {
                    epoch,
                    node_id,
                    term,
                    prior_epoch,
                    voters,
                    per_context_tails,
                });
                Ok(RaftResponse::LeaderClaimed { epoch })
            }
            RaftRequest::RegisterHandler { registration } => {
                self.handler_routing.apply_register(registration);
                Ok(RaftResponse::Ok)
            }
            RaftRequest::DeregisterHandler {
                bus,
                kind,
                message_type,
                client_id,
                node_id,
            } => {
                self.handler_routing.apply_deregister(
                    &bus,
                    kind,
                    &message_type,
                    &client_id,
                    node_id,
                );
                Ok(RaftResponse::Ok)
            }
            RaftRequest::DeregisterClient { client_id, node_id } => {
                self.handler_routing
                    .apply_deregister_client(&client_id, node_id);
                Ok(RaftResponse::Ok)
            }
            RaftRequest::ClearNodeHandlers { node_id } => {
                self.handler_routing.apply_clear_node(node_id);
                Ok(RaftResponse::Ok)
            }
        }
    }
}

impl RaftStateMachine<TypeConfig> for EventStoreStateMachine {
    type SnapshotBuilder = EventStoreSnapshotBuilder;

    async fn applied_state(
        &mut self,
    ) -> Result<
        (
            Option<LogId<NodeId>>,
            StoredMembership<NodeId, openraft::BasicNode>,
        ),
        StorageError<NodeId>,
    > {
        Ok((self.last_applied, self.last_membership.clone()))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<RaftResponse>, StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let mut responses = Vec::new();
        let mut applied_any = false;

        for entry in entries {
            let log_id = *entry.get_log_id();
            let response = match entry.payload {
                EntryPayload::Normal(request) => self.apply_request(request, log_id)?,
                EntryPayload::Membership(membership) => {
                    self.last_membership = StoredMembership::new(Some(log_id), membership.clone());
                    self.snapshot_store
                        .save_membership(&self.last_membership)
                        .map_err(|error| {
                            StorageError::from_io_error(
                                openraft::ErrorSubject::StateMachine,
                                openraft::ErrorVerb::Write,
                                error,
                            )
                        })?;
                    // Handler registrations are leased to membership: rows
                    // owned by a node that left the cluster drop here, on
                    // every node, at the same log position.
                    let live: std::collections::BTreeSet<NodeId> =
                        membership.nodes().map(|(id, _)| *id).collect();
                    self.handler_routing.retain_nodes(&live);
                    RaftResponse::Ok
                }
                EntryPayload::Blank => RaftResponse::Ok,
            };
            self.last_applied = Some(log_id);
            responses.push(response);
            applied_any = true;
        }

        if applied_any {
            self.persist_control_state()?;
            self.publish_control_state();
        }
        Ok(responses)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        // Capture all metadata on the state-machine worker. The asynchronous
        // builder then serializes this immutable copy while applies continue.
        EventStoreSnapshotBuilder {
            last_applied: self.last_applied,
            last_membership: self.last_membership.clone(),
            metadata: self.metadata_snapshot(),
            snapshot_store: Arc::clone(&self.snapshot_store),
        }
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<tokio::fs::File>, StorageError<NodeId>> {
        let (_path, file) = self
            .snapshot_store
            .create_staging_data_file()
            .map_err(|error| {
                StorageError::from_io_error(
                    openraft::ErrorSubject::StateMachine,
                    openraft::ErrorVerb::Write,
                    error,
                )
            })?;
        Ok(Box::new(tokio::fs::File::from_std(file)))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId, openraft::BasicNode>,
        snapshot: Box<tokio::fs::File>,
    ) -> Result<(), StorageError<NodeId>> {
        let read_error = |error| {
            StorageError::from_io_error(
                openraft::ErrorSubject::StateMachine,
                openraft::ErrorVerb::Read,
                error,
            )
        };
        let write_error = |error| {
            StorageError::from_io_error(
                openraft::ErrorSubject::StateMachine,
                openraft::ErrorVerb::Write,
                error,
            )
        };

        let mut file = snapshot.into_std().await;
        file.seek(SeekFrom::Start(0)).map_err(read_error)?;
        let metadata = read_snapshot(BufReader::new(&mut file)).map_err(read_error)?;

        // Snapshot install is additive for event contexts. It may create
        // missing metadata-declared contexts, but must never reset, truncate,
        // delete, or append to any existing event context.
        for name in &metadata.contexts {
            Self::create_context_idempotent(&self.contexts, name).map_err(|error| {
                write_error(std::io::Error::other(format!(
                    "install snapshot CreateContext({name}) failed: {error}"
                )))
            })?;
        }

        // Persist the validated snapshot pair before publishing its progress.
        file.seek(SeekFrom::Start(0)).map_err(read_error)?;
        let install_tmp = self.snapshot_store.dir().join("install.data.tmp");
        {
            let mut output = std::fs::File::create(&install_tmp).map_err(write_error)?;
            std::io::copy(&mut file, &mut output).map_err(write_error)?;
        }
        drop(file);
        self.snapshot_store
            .commit_snapshot(meta, &install_tmp)
            .map_err(|error| {
                StorageError::from_io_error(
                    openraft::ErrorSubject::Snapshot(Some(meta.signature())),
                    openraft::ErrorVerb::Write,
                    error,
                )
            })?;

        self.last_applied = meta.last_log_id;
        self.last_membership = meta.last_membership.clone();
        self.leader_claim = metadata.leader_claim;
        self.handler_routing.restore(metadata.handlers);

        self.snapshot_store
            .save_membership(&self.last_membership)
            .map_err(write_error)?;
        self.persist_control_state()?;
        self.publish_control_state();
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<NodeId>> {
        match self.snapshot_store.open_latest().map_err(|error| {
            StorageError::from_io_error(
                openraft::ErrorSubject::Store,
                openraft::ErrorVerb::Read,
                error,
            )
        })? {
            Some((meta, data_path)) => {
                let file = std::fs::File::open(&data_path).map_err(|error| {
                    StorageError::from_io_error(
                        openraft::ErrorSubject::Snapshot(Some(meta.signature())),
                        openraft::ErrorVerb::Read,
                        error,
                    )
                })?;
                Ok(Some(Snapshot {
                    meta,
                    snapshot: Box::new(tokio::fs::File::from_std(file)),
                }))
            }
            None => Ok(None),
        }
    }
}

fn build_and_commit_snapshot(
    snapshot_store: &SnapshotStore,
    metadata: &MetadataSnapshot,
    meta: &SnapshotMeta<NodeId, openraft::BasicNode>,
) -> Result<std::path::PathBuf, Error> {
    let (staging_path, staging_file) = snapshot_store
        .create_staging_data_file()
        .map_err(Error::Io)?;
    let writer = write_snapshot(BufWriter::new(staging_file), metadata).map_err(Error::Io)?;
    drop(
        writer
            .into_inner()
            .map_err(|error| Error::Io(error.into_error()))?,
    );
    snapshot_store
        .commit_snapshot(meta, &staging_path)
        .map_err(Error::Io)
}

/// Builds a metadata-only Raft snapshot.
pub struct EventStoreSnapshotBuilder {
    last_applied: Option<LogId<NodeId>>,
    last_membership: StoredMembership<NodeId, openraft::BasicNode>,
    metadata: MetadataSnapshot,
    snapshot_store: Arc<SnapshotStore>,
}

impl RaftSnapshotBuilder<TypeConfig> for EventStoreSnapshotBuilder {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<NodeId>> {
        let snapshot_id = self
            .last_applied
            .map(|id| format!("{}-{}", id.leader_id, id.index))
            .unwrap_or_else(|| "empty".to_string());
        let meta = SnapshotMeta {
            last_log_id: self.last_applied,
            last_membership: self.last_membership.clone(),
            snapshot_id,
        };

        let data_path = build_and_commit_snapshot(&self.snapshot_store, &self.metadata, &meta)
            .map_err(|error| {
                StorageError::from_io_error(
                    openraft::ErrorSubject::Snapshot(Some(meta.signature())),
                    openraft::ErrorVerb::Write,
                    std::io::Error::other(format!("snapshot build: {error}")),
                )
            })?;
        let file = std::fs::File::open(&data_path).map_err(|error| {
            StorageError::from_io_error(
                openraft::ErrorSubject::Snapshot(Some(meta.signature())),
                openraft::ErrorVerb::Read,
                error,
            )
        })?;
        Ok(Snapshot {
            meta,
            snapshot: Box::new(tokio::fs::File::from_std(file)),
        })
    }
}
