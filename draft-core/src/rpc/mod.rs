mod request_vote;
mod append_entries;
mod utils;

pub use request_vote::*;
pub use append_entries::*;
use serde::{de::DeserializeOwned, Serialize};
pub use utils::*;

use tracing::{instrument, error};
use crate::{node::RaftNode, Storage};


/// Whoever implements this trait must propagate any errors that occur during the RPC call.
/// These methods don't mutate state.

pub trait TryRaftRPC {
    fn try_handle_request_vote(
        &mut self,
        request: VoteRequest,
    ) -> Result<VoteResponse, RequestVoteRPCError>;
    fn try_handle_append_entries(
        &mut self,
        request: AppendEntriesRequest,
    ) -> Result<AppendEntriesResponse, AppendEntriesRPCError>;
}

pub trait RaftRPC {
    fn handle_request_vote(
        &mut self,
        request: VoteRequest,
    ) -> color_eyre::Result<VoteResponse>;
    fn handle_append_entries(
        &mut self,
        request: AppendEntriesRequest,
    ) -> color_eyre::Result<AppendEntriesResponse>;
}


impl<S> TryRaftRPC for RaftNode<S> 
where
    S: Storage + Serialize + DeserializeOwned + Clone
{
    #[instrument(skip(self), target = "rpc::AppendEntries")]
    fn try_handle_append_entries(
        &mut self,
        request: AppendEntriesRequest,
    ) -> Result<AppendEntriesResponse, AppendEntriesRPCError> {
        let requested_term = request.term;

        let res = handle_append_entries(self, request);
        match res {
            Ok(result) => {
                self.persistent_state.current_term = result.term;
                
                if let Err(e) = self.save() {
                    error!("{}", e.to_string());
                }
                Ok(result)
            },
            Err(err) => {
                match err {
                    AppendEntriesRPCError::NodeOutOfDate { latest_term, .. } => {
                        self.persistent_state.current_term = latest_term;
                        if let Err(e) = self.save() {
                            error!("{}", e.to_string());
                        }
                        Err(err)
                    },
                    e => {
                        self.persistent_state.current_term = requested_term;
                        if let Err(e) = self.save() {
                            error!("{}", e.to_string());
                        }
                        Err(e)
                    }
                }
            }
        }
    }
    /// Given a vote request RPC, process the request without making any modifications to the state
    /// as described in Section 5.4.1 and Figure 3.
    #[instrument(skip(self), target = "rpc::RequestVote")]
    fn try_handle_request_vote(
        &mut self,
        request: VoteRequest,
    ) -> Result<VoteResponse, RequestVoteRPCError> {
        handle_request_vote(self, request)
    }
}

impl<S> RaftRPC for RaftNode<S> 
where
    S: Storage + Serialize + DeserializeOwned + Clone
{
    fn handle_append_entries(
        &mut self,
        request: AppendEntriesRequest,
    ) -> color_eyre::Result<AppendEntriesResponse> {
        let requested_term = request.term;
        match handle_append_entries(self, request) {
            Ok(response) => {
                Ok(response)
            },
            Err(err) => {
                match err {
                    AppendEntriesRPCError::NodeOutOfDate { latest_term, .. } => {
                        Ok(AppendEntriesResponse { term: latest_term, success: false })
                    },
                    _ => {
                        Ok(AppendEntriesResponse { term: requested_term, success: false })
                    }
                }
            }
        }
    }

    fn handle_request_vote(
        &mut self,
        request: VoteRequest,
    ) -> color_eyre::Result<VoteResponse> {
        
        let candidate_id = request.candidate_id;

        match handle_request_vote(self, request) {
            Ok(response) => {
                // The rpc was handled correctly as expected.
                // We must grant vote now.
                self.persistent_state.voted_for = Some(candidate_id);
                self.persistent_state.current_term = response.term;
                self.save()?;

                Ok(response)
            },
            Err(err) => {
                match err {
                    RequestVoteRPCError::AlreadyVoted { latest_term, .. } => Ok(VoteResponse { term: latest_term, vote_granted: false }),
                    RequestVoteRPCError::CandidateNodeHasEmptyLog { latest_term, .. } => Ok(VoteResponse { term: latest_term, vote_granted: false }),
                    RequestVoteRPCError::CandidateNodeHasStaleLog { latest_term, ..} => Ok(VoteResponse { term: latest_term, vote_granted: false }),
                    RequestVoteRPCError::NodeOutOfDate { latest_term, .. } => Ok(VoteResponse { term: latest_term, vote_granted: false }),
                }
            }
        }
    }
}

