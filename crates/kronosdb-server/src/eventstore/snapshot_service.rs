use std::sync::Arc;

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use kronosdb_eventstore::raft::cluster::ClusterManager;
use kronosdb_eventstore::snapshot::{Snapshot, SnapshotStore};

use crate::proto::kronosdb::snapshot as pb;
use crate::proto::kronosdb::snapshot::snapshot_store_server::SnapshotStoreServer as GrpcSnapshotStoreServer;

/// Default context when no `kronosdb-context` header is provided.
const DEFAULT_CONTEXT: &str = "default";

/// gRPC metadata header key for context routing.
const CONTEXT_HEADER: &str = "kronosdb-context";

/// gRPC service implementation for the snapshot store.
pub struct SnapshotServiceImpl {
    cluster: Arc<ClusterManager>,
}

impl SnapshotServiceImpl {
    pub fn new(cluster: Arc<ClusterManager>) -> Self {
        Self { cluster }
    }

    pub fn into_server(self) -> GrpcSnapshotStoreServer<Self> {
        GrpcSnapshotStoreServer::new(self)
    }

    fn extract_context<T>(request: &Request<T>) -> &str {
        request
            .metadata()
            .get(CONTEXT_HEADER)
            .and_then(|v| v.to_str().ok())
            .unwrap_or(DEFAULT_CONTEXT)
    }

    fn get_store(&self, context_name: &str) -> Result<Arc<SnapshotStore>, Status> {
        self.cluster
            .context_manager()
            .get_snapshot_store(context_name)
            .map_err(|e| Status::not_found(e.to_string()))
    }
}

#[tonic::async_trait]
impl pb::snapshot_store_server::SnapshotStore for SnapshotServiceImpl {
    type ListStream = ReceiverStream<Result<pb::ListSnapshotsResponse, Status>>;

    async fn add(
        &self,
        request: Request<pb::AddSnapshotRequest>,
    ) -> Result<Response<pb::AddSnapshotResponse>, Status> {
        let context_name = Self::extract_context(&request).to_string();
        let req = request.into_inner();

        let snapshot = req
            .snapshot
            .ok_or_else(|| Status::invalid_argument("snapshot is required"))?;

        let snap = from_proto_snapshot(snapshot);
        let key = req.key;
        let sequence = req.sequence;
        let prune = req.prune;

        let store = self.get_store(&context_name)?;
        tokio::task::spawn_blocking(move || store.add(&key, sequence, &snap, prune))
            .await
            .map_err(|e| Status::internal(format!("task join error: {e}")))?
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(pb::AddSnapshotResponse {}))
    }

    async fn delete(
        &self,
        request: Request<pb::DeleteSnapshotsRequest>,
    ) -> Result<Response<pb::DeleteSnapshotsResponse>, Status> {
        let context_name = Self::extract_context(&request).to_string();
        let req = request.into_inner();

        let store = self.get_store(&context_name)?;
        let key = req.key;
        let to_sequence = req.to_sequence;

        tokio::task::spawn_blocking(move || store.delete(&key, to_sequence))
            .await
            .map_err(|e| Status::internal(format!("task join error: {e}")))?
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(pb::DeleteSnapshotsResponse {}))
    }

    async fn list(
        &self,
        request: Request<pb::ListSnapshotsRequest>,
    ) -> Result<Response<Self::ListStream>, Status> {
        let context_name = Self::extract_context(&request).to_string();
        let req = request.into_inner();

        let store = self.get_store(&context_name)?;
        let key = req.key;
        let from_sequence = req.from_sequence;
        let to_sequence = req.to_sequence;

        let entries =
            tokio::task::spawn_blocking(move || store.list(&key, from_sequence, to_sequence))
                .await
                .map_err(|e| Status::internal(format!("task join error: {e}")))?
                .map_err(|e| Status::internal(e.to_string()))?;

        let (tx, rx) = mpsc::channel(64);

        tokio::spawn(async move {
            for entry in entries {
                let resp = pb::ListSnapshotsResponse {
                    key: entry.key,
                    sequence: entry.sequence,
                    snapshot: Some(to_proto_snapshot(&entry.snapshot)),
                };
                if tx.send(Ok(resp)).await.is_err() {
                    break;
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn get_last(
        &self,
        request: Request<pb::GetLastSnapshotRequest>,
    ) -> Result<Response<pb::GetLastSnapshotResponse>, Status> {
        let context_name = Self::extract_context(&request).to_string();
        let req = request.into_inner();

        let store = self.get_store(&context_name)?;
        let key = req.key;

        let entry = tokio::task::spawn_blocking(move || store.get_last(&key))
            .await
            .map_err(|e| Status::internal(format!("task join error: {e}")))?
            .map_err(|e| Status::internal(e.to_string()))?;

        match entry {
            Some(e) => Ok(Response::new(pb::GetLastSnapshotResponse {
                key: e.key,
                sequence: e.sequence,
                snapshot: Some(to_proto_snapshot(&e.snapshot)),
            })),
            None => Ok(Response::new(pb::GetLastSnapshotResponse {
                key: Vec::new(),
                sequence: 0,
                snapshot: None,
            })),
        }
    }
}

fn from_proto_snapshot(s: pb::Snapshot) -> Snapshot {
    Snapshot {
        name: s.name,
        version: s.version,
        payload: s.payload,
        timestamp: s.timestamp,
        metadata: s.metadata,
    }
}

fn to_proto_snapshot(s: &Snapshot) -> pb::Snapshot {
    pb::Snapshot {
        name: s.name.clone(),
        version: s.version.clone(),
        payload: s.payload.clone(),
        timestamp: s.timestamp,
        metadata: s.metadata.clone(),
    }
}
