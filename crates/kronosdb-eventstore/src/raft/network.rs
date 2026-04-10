use openraft::BasicNode;
use openraft::error::{InstallSnapshotError, RPCError, RaftError};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use tonic::transport::Channel;

use super::proto;
use super::proto::raft_transport_client::RaftTransportClient;
use super::types::{NodeId, TypeConfig};

type RaftRPCError<E = openraft::error::Infallible> =
    RPCError<NodeId, BasicNode, RaftError<NodeId, E>>;

/// Factory that creates gRPC network connections to peer nodes.
pub struct NetworkFactory;

impl RaftNetworkFactory<TypeConfig> for NetworkFactory {
    type Network = NetworkConnection;

    async fn new_client(&mut self, _target: NodeId, node: &BasicNode) -> Self::Network {
        NetworkConnection {
            addr: node.addr.clone(),
            client: None,
        }
    }
}

/// A single network connection to a peer node.
pub struct NetworkConnection {
    addr: String,
    client: Option<RaftTransportClient<Channel>>,
}

impl NetworkConnection {
    async fn get_client(&mut self) -> Result<&mut RaftTransportClient<Channel>, tonic::Status> {
        if self.client.is_none() {
            let endpoint = format!("http://{}", self.addr);
            let channel = Channel::from_shared(endpoint)
                .map_err(|e| tonic::Status::internal(format!("invalid endpoint: {e}")))?
                .connect()
                .await
                .map_err(|e| tonic::Status::unavailable(format!("connect failed: {e}")))?;
            self.client = Some(RaftTransportClient::new(channel));
        }
        Ok(self.client.as_mut().unwrap())
    }
}

impl RaftNetwork<TypeConfig> for NetworkConnection {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RaftRPCError> {
        let data = bincode::serialize(&rpc)
            .map_err(|e| RPCError::Network(openraft::error::NetworkError::new(&e)))?;

        let client = self
            .get_client()
            .await
            .map_err(|e| RPCError::Network(openraft::error::NetworkError::new(&e)))?;

        let resp = client
            .append_entries(proto::RaftAppendEntriesRequest { data })
            .await
            .map_err(|e| RPCError::Network(openraft::error::NetworkError::new(&e)))?;

        let response: AppendEntriesResponse<NodeId> = bincode::deserialize(&resp.into_inner().data)
            .map_err(|e| RPCError::Network(openraft::error::NetworkError::new(&e)))?;

        Ok(response)
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, BasicNode, RaftError<NodeId, InstallSnapshotError>>,
    > {
        let data = bincode::serialize(&rpc)
            .map_err(|e| RPCError::Network(openraft::error::NetworkError::new(&e)))?;

        let client = self
            .get_client()
            .await
            .map_err(|e| RPCError::Network(openraft::error::NetworkError::new(&e)))?;

        let resp = client
            .install_snapshot(proto::RaftInstallSnapshotRequest { data })
            .await
            .map_err(|e| RPCError::Network(openraft::error::NetworkError::new(&e)))?;

        let response: InstallSnapshotResponse<NodeId> =
            bincode::deserialize(&resp.into_inner().data)
                .map_err(|e| RPCError::Network(openraft::error::NetworkError::new(&e)))?;

        Ok(response)
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RaftRPCError> {
        let data = bincode::serialize(&rpc)
            .map_err(|e| RPCError::Network(openraft::error::NetworkError::new(&e)))?;

        let client = self
            .get_client()
            .await
            .map_err(|e| RPCError::Network(openraft::error::NetworkError::new(&e)))?;

        let resp = client
            .vote(proto::RaftVoteRequest { data })
            .await
            .map_err(|e| RPCError::Network(openraft::error::NetworkError::new(&e)))?;

        let response: VoteResponse<NodeId> = bincode::deserialize(&resp.into_inner().data)
            .map_err(|e| RPCError::Network(openraft::error::NetworkError::new(&e)))?;

        Ok(response)
    }
}
