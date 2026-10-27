use std::error::Error as StdError;
use std::sync::Arc;

use kronosdb_protocol as p;
use kronosdb_protocol::Frame;
use kronosdb_protocol::net::OwnedFrame;
use openraft::BasicNode;
use openraft::error::{InstallSnapshotError, NetworkError, RPCError, RaftError};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use serde::Serialize;
use serde::de::DeserializeOwned;

use super::types::{NodeId, TypeConfig};
use crate::replication::peer::{PeerPool, PeerTransportConfig};

type RaftRPCError<E = openraft::error::Infallible> =
    RPCError<NodeId, BasicNode, RaftError<NodeId, E>>;

/// Message-size ceiling for raft peer traffic (both directions). Must stay
/// well above the proposer's per-entry byte cap (`MAX_BATCH_BYTES` in
/// cluster.rs) plus framing, and above `snapshot_max_chunk_size`. Peer
/// connections carry frames of up to `PEER_MAX_FRAME`, which covers it.
pub const RAFT_MAX_MESSAGE_BYTES: usize = 64 * 1024 * 1024;

const _: () = assert!(RAFT_MAX_MESSAGE_BYTES <= crate::replication::PEER_MAX_MESSAGE_BYTES);

/// Factory that creates native network connections to peer nodes.
///
/// Raft keeps connections of its own, apart from forwarded appends and
/// cursor queries: a heartbeat must not wait behind a large frame.
#[derive(Clone)]
pub struct NetworkFactory {
    pool: Arc<PeerPool>,
}

impl NetworkFactory {
    pub fn new(transport: PeerTransportConfig) -> Self {
        Self {
            pool: Arc::new(PeerPool::new(transport)),
        }
    }
}

impl RaftNetworkFactory<TypeConfig> for NetworkFactory {
    type Network = NetworkConnection;

    async fn new_client(&mut self, _target: NodeId, node: &BasicNode) -> Self::Network {
        NetworkConnection {
            addr: node.addr.clone(),
            pool: Arc::clone(&self.pool),
        }
    }
}

/// A single network connection to a peer node.
pub struct NetworkConnection {
    addr: String,
    pool: Arc<PeerPool>,
}

fn network<R>(error: &(impl StdError + 'static)) -> RPCError<NodeId, BasicNode, R>
where
    R: StdError,
{
    RPCError::Network(NetworkError::new(error))
}

impl NetworkConnection {
    /// Serializes `rpc`, sends it in the frame `wrap` builds, and
    /// deserializes the answer `unwrap` extracts from the reply frame.
    // openraft's `RPCError` fixes the large error type.
    #[allow(clippy::result_large_err)]
    async fn call<Rq, Rs, R>(
        &self,
        rpc: &Rq,
        wrap: for<'a> fn(&'a [u8]) -> Frame<'a>,
        unwrap: for<'a> fn(&'a Frame<'a>) -> Option<&'a [u8]>,
    ) -> Result<Rs, RPCError<NodeId, BasicNode, R>>
    where
        Rq: Serialize,
        Rs: DeserializeOwned,
        R: StdError,
    {
        let data = bincode::serialize(rpc).map_err(|e| network(&e))?;
        let reply: OwnedFrame = self
            .pool
            .request(&self.addr, &wrap(&data))
            .await
            .map_err(|e| network(&e))?;
        let frame = reply.decode().map_err(|e| network(&e))?;
        let data = unwrap(&frame).ok_or_else(|| {
            network(&p::net::NetError::Unexpected {
                kind: reply.kind(),
                context: "instead of the Raft reply",
            })
        })?;
        bincode::deserialize(data).map_err(|e| network(&e))
    }
}

impl RaftNetwork<TypeConfig> for NetworkConnection {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RaftRPCError> {
        self.call(
            &rpc,
            |data| Frame::RaftAppendEntries(p::RaftAppendEntries { data }),
            |frame| match frame {
                Frame::RaftAppendEntriesReply(r) => Some(r.data),
                _ => None,
            },
        )
        .await
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, BasicNode, RaftError<NodeId, InstallSnapshotError>>,
    > {
        self.call(
            &rpc,
            |data| Frame::RaftInstallSnapshot(p::RaftInstallSnapshot { data }),
            |frame| match frame {
                Frame::RaftInstallSnapshotReply(r) => Some(r.data),
                _ => None,
            },
        )
        .await
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RaftRPCError> {
        self.call(
            &rpc,
            |data| Frame::RaftVote(p::RaftVote { data }),
            |frame| match frame {
                Frame::RaftVoteReply(r) => Some(r.data),
                _ => None,
            },
        )
        .await
    }
}
