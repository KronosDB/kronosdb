//! Inbound side of the Raft transport: peers' Raft RPCs and forwarded
//! control-plane writes, served from native protocol frames.
//!
//! One node-wide metadata Raft group carries context creation and fenced
//! leader claims. Event bytes never enter this transport.

use std::error::Error as StdError;

use kronosdb_protocol as p;
use kronosdb_protocol::Frame;
use kronosdb_protocol::net::server::{ServerStream, StreamSender};
use openraft::Raft;
use openraft::raft::{VoteRequest, VoteResponse};
use serde::Serialize;
use serde::de::DeserializeOwned;

use super::cluster::ClusterManager;
use super::types::{NodeId, RaftRequest, TypeConfig};
use crate::error::Error;

pub type RaftNode = Raft<TypeConfig>;

/// Serves one Raft request: the stream's first frame carries it, and one
/// frame answers it. The payloads are openraft's own serialized messages.
///
/// A vote request is put to `cluster` first: a node that started empty and
/// has not caught up answers "not granted" itself
/// ([`ClusterManager::admit_vote`]).
pub async fn serve(
    raft: &RaftNode,
    cluster: &ClusterManager,
    stream: ServerStream,
) -> Result<(), Error> {
    let out = &stream.out;
    match stream.request.decode().map_err(malformed)? {
        Frame::RaftVote(r) => {
            let request: VoteRequest<NodeId> = read(r.data, "vote")?;
            let response = if cluster.admit_vote(&request) {
                raft.vote(request)
                    .await
                    .map_err(|e| unavailable("vote", &e))?
            } else {
                not_granted(&request)
            };
            let data = encode(&response, "vote")?;
            answer(out, Frame::RaftVoteReply(p::RaftVoteReply { data: &data })).await
        }
        Frame::RaftAppendEntries(r) => {
            let response = raft
                .append_entries(read(r.data, "append_entries")?)
                .await
                .map_err(|e| unavailable("append_entries", &e))?;
            let data = encode(&response, "append_entries")?;
            let reply = p::RaftAppendEntriesReply { data: &data };
            answer(out, Frame::RaftAppendEntriesReply(reply)).await
        }
        Frame::RaftInstallSnapshot(r) => {
            let response = raft
                .install_snapshot(read(r.data, "install_snapshot")?)
                .await
                .map_err(|e| unavailable("install_snapshot", &e))?;
            let data = encode(&response, "install_snapshot")?;
            let reply = p::RaftInstallSnapshotReply { data: &data };
            answer(out, Frame::RaftInstallSnapshotReply(reply)).await
        }
        Frame::ForwardWrite(r) => {
            let request: RaftRequest = read(r.data, "forward_write")?;
            let response = raft
                .client_write(request)
                .await
                .map_err(|e| unavailable("client_write", &e))?;
            let data = encode(&response.data, "forward_write")?;
            let reply = p::ForwardWriteReply { data: &data };
            answer(out, Frame::ForwardWriteReply(reply)).await
        }
        _ => Err(Error::Rejected {
            message: format!(
                "frame kind {:#06x} is not a Raft request",
                stream.request.kind()
            ),
        }),
    }
}

/// The answer of a node that takes no part in elections. It echoes the
/// candidate's own vote, so the candidate learns nothing from it: no vote
/// granted, no higher term to step down for, no longer log to wait behind.
fn not_granted(request: &VoteRequest<NodeId>) -> VoteResponse<NodeId> {
    VoteResponse::new(request.vote, None, false)
}

/// A failed send means the peer went away; there is nobody to tell.
async fn answer(out: &StreamSender, reply: Frame<'_>) -> Result<(), Error> {
    let _ = out.finish(&reply).await;
    Ok(())
}

fn read<T: DeserializeOwned>(data: &[u8], what: &str) -> Result<T, Error> {
    bincode::deserialize(data).map_err(|e| Error::Rejected {
        message: format!("deserialize {what}: {e}"),
    })
}

fn encode<T: Serialize>(response: &T, what: &str) -> Result<Vec<u8>, Error> {
    bincode::serialize(response).map_err(|e| Error::Internal {
        message: format!("serialize {what} response: {e}"),
    })
}

fn malformed(error: impl StdError) -> Error {
    Error::Rejected {
        message: format!("malformed Raft request: {error}"),
    }
}

fn unavailable(what: &str, error: &impl StdError) -> Error {
    Error::Unavailable {
        message: format!("raft {what}: {error}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use openraft::{CommittedLeaderId, LogId, Vote};

    #[test]
    fn a_refusal_grants_nothing_and_tells_the_candidate_nothing() {
        let request = VoteRequest::new(
            Vote::new(7, 3),
            Some(LogId::new(CommittedLeaderId::new(6, 1), 40)),
        );
        let response = not_granted(&request);
        assert!(!response.vote_granted);
        assert_eq!(response.vote, request.vote);
        assert_eq!(response.last_log_id, None);
    }
}
