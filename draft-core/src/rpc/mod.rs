mod request_vote;
mod append_entries;

pub use request_vote::*;
pub use append_entries::*;

use tracing::instrument;
use crate::node::RaftNode;


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


impl TryRaftRPC for RaftNode {
    #[instrument(skip(self), target = "rpc::AppendEntries")]
    fn try_handle_append_entries(
        &mut self,
        request: AppendEntriesRequest,
    ) -> Result<AppendEntriesResponse, AppendEntriesRPCError> {
        handle_append_entries(self, request)
    }
    /// Given a vote request RPC, process the request without making any modifications to the state
    /// as described in Section 5.4.1 and Figure 3.
    #[instrument(skip(self), target = "rpc::RequestVote")]
    fn try_handle_request_vote(
        &mut self,
        request: VoteRequest,
    ) -> Result<VoteResponse, RequestVoteRPCError> {
        handle_request_vote(&self, request)
    }
}

impl RaftRPC for RaftNode {
    fn handle_append_entries(
        &mut self,
        _request: AppendEntriesRequest,
    ) -> color_eyre::Result<AppendEntriesResponse> {
        todo!("Implement AppendEntries handler.")
    }

    fn handle_request_vote(
        &mut self,
        request: VoteRequest,
    ) -> color_eyre::Result<VoteResponse> {
        
        let candidate_id = request.candidate_id;

        match handle_request_vote(&self, request) {
            Ok(response) => {
                // The rpc was handled correctly as expected.
                // We must grant vote now.
                self.persistent_state.voted_for = Some(candidate_id);
                self.persistent_state.current_term = response.term;
                self.save_to_disk()?;

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