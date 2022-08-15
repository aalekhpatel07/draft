use crate::node::*;
use anyhow::Result;
use serde::{Serialize, Deserialize};
use tracing::instrument;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendEntriesRequest {

}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendEntriesResponse {

}


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VoteRequest {
    pub term: usize,
    pub candidate_id: usize,
    pub last_log_index: usize,
    pub last_log_term: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VoteResponse {
    pub term: usize,
    pub vote_granted: bool,
}

pub trait RaftRPC {
    fn handle_request_vote(&self, request: VoteRequest) -> Result<VoteResponse>;
    fn handle_append_entries(&self, request: AppendEntriesRequest) -> Result<AppendEntriesResponse>;
}

impl RaftRPC for RaftNode {

    #[instrument(skip(self), target="rpc::AppendEntries")]
    fn handle_append_entries(&self, request: AppendEntriesRequest) -> Result<AppendEntriesResponse> {
        unimplemented!()
    }
    #[instrument(skip(self), target="rpc::RequestVote")]
    fn handle_request_vote(&self, request: VoteRequest) -> Result<VoteResponse> {
        unimplemented!()
    }
}