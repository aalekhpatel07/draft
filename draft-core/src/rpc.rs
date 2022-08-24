use std::cmp::Ordering;

use crate::node::*;
// use color_eyre::Result;
use serde::{Serialize, Deserialize};
use tracing::instrument;
use crate::errors::RequestVoteRPCError;


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendEntriesRequest {

}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendEntriesResponse {

}


#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct VoteRequest {
    pub term: usize,
    pub candidate_id: usize,
    pub last_log_index: usize,
    pub last_log_term: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct VoteResponse {
    pub term: usize,
    pub vote_granted: bool,
}

/// Whoever implements this trait must propagate any errors that occur during the RPC call.
/// These methods don't mutate state.
pub trait RaftRPC {
    fn handle_request_vote(&self, request: VoteRequest) -> Result<VoteResponse, RequestVoteRPCError>;
    fn handle_append_entries(&self, request: AppendEntriesRequest) -> color_eyre::Result<AppendEntriesResponse>;
}

impl RaftRPC for RaftNode {

    #[instrument(skip(self), target="rpc::AppendEntries")]
    fn handle_append_entries(&self, request: AppendEntriesRequest) -> color_eyre::Result<AppendEntriesResponse> {
        unimplemented!()
    }
    /// Given a vote request RPC, process the request without making any modifications to the state
    /// as described in Section 5.4.1 and Figure 3.
    #[instrument(skip(self), target="rpc::RequestVote")]
    fn handle_request_vote(&self, request: VoteRequest) -> Result<VoteResponse, RequestVoteRPCError> {

        if request.term < self.persistent_state.current_term {
            // The candidate is straight up out-of-date.
            // Inform it of the latest term that we know of.
 &           return Err(
                RequestVoteRPCError::NodeOutOfDate { 
                    self_id: self.metadata.id, 
                    handler_term: self.persistent_state.current_term, 
                    requested_term: request.term
                }
            );
        }

        if let Some(vote_recipient) = self.persistent_state.voted_for {
            if vote_recipient != request.candidate_id {
                // We've already voted for someone else.
                // Oops. Sorry, dear requester.
                return Err(
                    RequestVoteRPCError::AlreadyVoted {
                        self_id: self.metadata.id,
                        voted_for: vote_recipient,
                        requested_node_id: request.candidate_id
                    }
                );
            }
        }

        let self_log_len = self.persistent_state.log.len();
        match (self_log_len, request.last_log_index) {
            (1.., 0) => {
                // We have some entries but candidate comes bearing no entries.
                // Thus the candidate has a stale log and we must inform it.
                return Err(
                    RequestVoteRPCError::CandidateNodeHasEmptyLog { 
                        self_id: self.metadata.id, 
                        requested_node_id: request.candidate_id,
                        requested_num_log_entries: request.last_log_index, 
                        last_log_entry_term: self.persistent_state.log.last().expect("log should not be empty").0
                    }
                );
            },
            (1.., 1..) => {
                // We have entries as well as the candidate has some entries.
                // Determine which log is fresher by comparing the terms of the last entries.
                // If the terms are the same, the longer log is the fresher log.
                // Otherwise, the log with the higher last term is the fresher log.

                // Let's match the terms of the last entries.
                let self_last_log_entry_term = self.persistent_state.log.last().expect("log should not be empty").0;
                let request_last_log_term = request.last_log_term;

                match self_last_log_entry_term.cmp(&request_last_log_term) {
                    Ordering::Equal => {
                        // The terms of the last entries are the same. In this case,
                        // the longer log is more fresh. (Section 5.4.1)
                        let candidate_log_size = request.last_log_index;
                        if self_log_len > candidate_log_size {
                            return Err(
                                RequestVoteRPCError::CandidateNodeHasStaleLog { 
                                    self_id: self.metadata.id, 
                                    requested_node_id: request.candidate_id, 
                                    requested_last_log_entry_term: request.last_log_term, 
                                    self_last_log_entry_term, 
                                    requested_num_log_entries: request.last_log_index, 
                                    self_num_log_entries: self_log_len
                                }
                            );
                        }
                    }
                    Ordering::Greater => {
                        // We have a higher term (for the last log entry) than the candidate's.
                        // Thus our log is more fresh. Notify the candidate. (Section 5.4.1)
                        return Err(
                            RequestVoteRPCError::CandidateNodeHasStaleLog { 
                                self_id: self.metadata.id, 
                                requested_node_id: request.candidate_id, 
                                requested_last_log_entry_term: request.last_log_term, 
                                self_last_log_entry_term, 
                                requested_num_log_entries: request.last_log_index, 
                                self_num_log_entries: self.persistent_state.log.len()
                            }
                        )
                    }
                    Ordering::Less => {
                        // The candidate has a higher term. So the candidate has a fresher log.
                    }
                };
            },
            // In any other case, there can't ever be a rejection.
            _ => {}
        }
        return Ok(VoteResponse { vote_granted: true, term: self.persistent_state.current_term });
    }
}


#[cfg(test)]
mod tests {
    pub use super::*;
    pub use crate::*;

    pub mod request_vote {
        pub use super::*;

        #[test]
        fn request_vote_handler_basic() -> color_eyre::Result<()>{
            color_eyre::install()?;
            let mut receiver_raft = RaftNode::default();
            receiver_raft.persistent_state.current_term = 2;
            let request: VoteRequest = VoteRequest { term: 1, candidate_id: 2, last_log_index: 0, last_log_term: 0 };
            let res = receiver_raft.handle_request_vote(request);
            assert!(res.is_err());
            let err = res.unwrap_err();
            assert_eq!(err, RequestVoteRPCError::NodeOutOfDate { self_id: 0, handler_term: 2, requested_term: 1 });
            Ok(())
        }
    }
}
