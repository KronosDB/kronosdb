//! Inbound gRPC service for receiving Raft RPCs from peer nodes.
//!
//! One node-wide metadata Raft group carries context creation and fenced
//! leader claims. Event bytes never enter this transport.

use std::sync::Arc;

use openraft::Raft;
use tonic::{Request, Response, Status};

use super::proto;
use super::proto::raft_transport_server::RaftTransport;
use super::types::{RaftRequest, TypeConfig};

pub type RaftNode = Raft<TypeConfig>;

/// gRPC service that handles incoming Raft RPCs from peer nodes.
pub struct RaftTransportService {
    /// The node's single Raft node.
    default_raft: Arc<RaftNode>,
}

impl RaftTransportService {
    pub fn new(raft: Arc<RaftNode>) -> Self {
        Self { default_raft: raft }
    }

    pub fn into_server(self) -> proto::raft_transport_server::RaftTransportServer<Self> {
        // Metadata entries remain small, but use the same explicit peer limit.
        proto::raft_transport_server::RaftTransportServer::new(self)
            .max_decoding_message_size(super::network::RAFT_MAX_MESSAGE_BYTES)
            .max_encoding_message_size(super::network::RAFT_MAX_MESSAGE_BYTES)
    }
}

#[tonic::async_trait]
impl RaftTransport for RaftTransportService {
    async fn vote(
        &self,
        request: Request<proto::RaftVoteRequest>,
    ) -> Result<Response<proto::RaftVoteResponse>, Status> {
        let req: openraft::raft::VoteRequest<u64> =
            bincode::deserialize(&request.into_inner().data)
                .map_err(|e| Status::invalid_argument(format!("deserialize vote: {e}")))?;

        let resp = self
            .default_raft
            .vote(req)
            .await
            .map_err(|e| Status::internal(format!("raft vote: {e}")))?;

        let data = bincode::serialize(&resp)
            .map_err(|e| Status::internal(format!("serialize vote response: {e}")))?;

        Ok(Response::new(proto::RaftVoteResponse { data }))
    }

    async fn append_entries(
        &self,
        request: Request<proto::RaftAppendEntriesRequest>,
    ) -> Result<Response<proto::RaftAppendEntriesResponse>, Status> {
        let req: openraft::raft::AppendEntriesRequest<TypeConfig> =
            bincode::deserialize(&request.into_inner().data).map_err(|e| {
                Status::invalid_argument(format!("deserialize append_entries: {e}"))
            })?;

        let resp = self
            .default_raft
            .append_entries(req)
            .await
            .map_err(|e| Status::internal(format!("raft append_entries: {e}")))?;

        let data = bincode::serialize(&resp)
            .map_err(|e| Status::internal(format!("serialize append_entries response: {e}")))?;

        Ok(Response::new(proto::RaftAppendEntriesResponse { data }))
    }

    async fn install_snapshot(
        &self,
        request: Request<proto::RaftInstallSnapshotRequest>,
    ) -> Result<Response<proto::RaftInstallSnapshotResponse>, Status> {
        let req: openraft::raft::InstallSnapshotRequest<TypeConfig> =
            bincode::deserialize(&request.into_inner().data).map_err(|e| {
                Status::invalid_argument(format!("deserialize install_snapshot: {e}"))
            })?;

        let resp = self
            .default_raft
            .install_snapshot(req)
            .await
            .map_err(|e| Status::internal(format!("raft install_snapshot: {e}")))?;

        let data = bincode::serialize(&resp)
            .map_err(|e| Status::internal(format!("serialize install_snapshot response: {e}")))?;

        Ok(Response::new(proto::RaftInstallSnapshotResponse { data }))
    }

    async fn forward_write(
        &self,
        request: Request<proto::ForwardWriteRequest>,
    ) -> Result<Response<proto::ForwardWriteResponse>, Status> {
        let raft_req: RaftRequest = bincode::deserialize(&request.into_inner().data)
            .map_err(|e| Status::invalid_argument(format!("deserialize forward_write: {e}")))?;

        let resp = self
            .default_raft
            .client_write(raft_req)
            .await
            .map_err(|e| Status::unavailable(format!("raft client_write: {e}")))?;

        let data = bincode::serialize(&resp.data)
            .map_err(|e| Status::internal(format!("serialize forward_write response: {e}")))?;

        Ok(Response::new(proto::ForwardWriteResponse { data }))
    }
}
