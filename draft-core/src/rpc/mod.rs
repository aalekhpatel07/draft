mod append_entries;
mod request_vote;
pub mod utils;

pub use append_entries::*;
pub use request_vote::*;

use crate::{node::RaftNode, Storage};
use tracing::{error, instrument};

/// Whoever implements this trait must propagate any errors that occur during the RPC call.
/// These methods don't mutate state.

pub trait TryRaftRPC {
    fn try_handle_request_vote(
        &self,
        request: VoteRequest,
    ) -> Result<VoteResponse, RequestVoteRPCError>;
    fn try_handle_append_entries(
        &self,
        request: AppendEntriesRequest,
    ) -> Result<AppendEntriesResponse, AppendEntriesRPCError>;
}

pub trait RaftRPC {
    fn handle_request_vote(&self, request: VoteRequest) -> VoteResponse;
    fn handle_append_entries(
        &self,
        request: AppendEntriesRequest,
    ) -> AppendEntriesResponse;
}

impl<S> TryRaftRPC for RaftNode<S>
where
    S: Storage,
{
    /// Given a vote request RPC, process the request without making any modifications to the state
    /// as described in Section 5.4.1 and Figure 3.
    #[instrument(skip(self), target = "rpc::RequestVote")]
    fn try_handle_request_vote(
        &self,
        request: VoteRequest,
    ) -> Result<VoteResponse, RequestVoteRPCError> {
        let candidate_id = request.candidate_id;
        let requested_term = request.term;
        match handle_request_vote(self, request) {
            Ok(response) => {
                // The rpc was handled correctly as expected.
                // We must grant vote now.
                let mut persistent_state_guard = self
                    .persistent_state
                    .lock()
                    .expect("Failed to lock persistent state");
                persistent_state_guard.voted_for = Some(candidate_id);
                persistent_state_guard.current_term = response.term;
                drop(persistent_state_guard);

                if let Err(e) = self.save() {
                    error!("{}", e.to_string());
                }
                Ok(response)
            }
            Err(err) => match err {
                RequestVoteRPCError::NodeOutOfDate { latest_term, .. } => {
                    let mut persistent_state_guard = self
                        .persistent_state
                        .lock()
                        .expect("Failed to lock persistent state");
                    persistent_state_guard.current_term = latest_term;
                    drop(persistent_state_guard);

                    if let Err(e) = self.save() {
                        error!("{}", e.to_string());
                    }
                    Err(err)
                }
                e => {
                    let mut persistent_state_guard = self
                        .persistent_state
                        .lock()
                        .expect("Failed to lock persistent state");
                    persistent_state_guard.current_term = requested_term;
                    drop(persistent_state_guard);

                    if let Err(save_err) = self.save() {
                        error!("{}", save_err.to_string());
                    }

                    Err(e)
                }
            },
        }
    }
    #[instrument(skip(self), target = "rpc::AppendEntries")]
    fn try_handle_append_entries(
        &self,
        request: AppendEntriesRequest,
    ) -> Result<AppendEntriesResponse, AppendEntriesRPCError> {
        let requested_term = request.term;

        let res = handle_append_entries(self, request);
        let mut persistent_state_guard = self
            .persistent_state
            .lock()
            .expect("Failed to lock persistent state.");

        match res {
            Ok(result) => {
                persistent_state_guard.current_term = result.term;
                // NOTE:
                // Maybe serde holds the lock when serializing the PersistentState.
                // Not dropping it before self.save() causes a deadlock.
                drop(persistent_state_guard);

                if let Err(e) = self.save() {
                    error!("{}", e.to_string());
                }
                Ok(result)
            }
            Err(err) => match err {
                AppendEntriesRPCError::NodeOutOfDate { latest_term, .. } => {
                    persistent_state_guard.current_term = latest_term;
                    // NOTE:
                    // Maybe serde holds the lock when serializing the PersistentState.
                    // Not dropping it before self.save() causes a deadlock.
                    drop(persistent_state_guard);

                    if let Err(e) = self.save() {
                        error!("{}", e.to_string());
                    }
                    Err(err)
                }
                e => {
                    persistent_state_guard.current_term = requested_term;
                    // NOTE:
                    // Maybe serde holds the lock when serializing the PersistentState.
                    // Not dropping it before self.save() causes a deadlock.
                    drop(persistent_state_guard);

                    if let Err(e) = self.save() {
                        error!("{}", e.to_string());
                    }
                    Err(e)
                }
            },
        }
    }
}

impl<S> RaftRPC for RaftNode<S>
where
    S: Storage,
{
    fn handle_request_vote(&self, request: VoteRequest) -> VoteResponse {
        let requested_term = request.term;

        match self.try_handle_request_vote(request) {
            Ok(response) => response,
            Err(err) => match err {
                RequestVoteRPCError::NodeOutOfDate { latest_term, .. } => VoteResponse {
                    term: latest_term,
                    vote_granted: false,
                },
                _ => VoteResponse {
                    term: requested_term,
                    vote_granted: false,
                },
            },
        }
    }

    fn handle_append_entries(
        &self,
        request: AppendEntriesRequest,
    ) -> AppendEntriesResponse {
        let requested_term = request.term;
        match handle_append_entries(self, request) {
            Ok(response) => response,
            Err(err) => match err {
                AppendEntriesRPCError::NodeOutOfDate { latest_term, .. } => {
                    AppendEntriesResponse {
                        term: latest_term,
                        success: false,
                    }
                }
                _ => AppendEntriesResponse {
                    term: requested_term,
                    success: false,
                },
            },
        }
    }
}

#[cfg(test)]
mod tests {
    pub use super::*;
    pub use crate::*;
    use crate::rpc::utils::*;
    use std::sync::{Arc, Mutex};

    #[test]
    fn handle_request_vote_rpc_succeeds() {
        let raft: RaftNode<BufferBackend> = RaftNode::default();
        let response = raft.handle_request_vote(VoteRequest { term: 2, candidate_id: 2, last_log_index: 0, last_log_term: 0 });
        assert_eq!(response, VoteResponse { term: 2, vote_granted: true });
    }
    #[test]
    fn handle_request_vote_rpc_fails() -> color_eyre::Result<()> {
        let mut raft: RaftNode<BufferBackend> = RaftNode::default();
        raft.persistent_state = Arc::new(Mutex::new(persistent_state(2, Some(3), vec![])));

        let response = raft.handle_request_vote(
            VoteRequest { term: 2, candidate_id: 2, last_log_index: 0, last_log_term: 0 }
        );
        assert_eq!(response, VoteResponse { term: 2, vote_granted: false });

        Ok(())
    }

    #[test]
    fn handle_request_vote_rpc_fails_because_out_of_date() -> color_eyre::Result<()> {
        let mut raft: RaftNode<BufferBackend> = RaftNode::default();
        raft.persistent_state = Arc::new(Mutex::new(persistent_state(4, Some(2), vec![])));

        let response = raft.handle_request_vote(
            VoteRequest { term: 2, candidate_id: 2, last_log_index: 0, last_log_term: 0 }
        );
        assert_eq!(response, VoteResponse { term: 4, vote_granted: false });

        Ok(())
    }

    #[test]
    fn handle_append_entries_rpc_succeeds() {
        let raft: RaftNode<BufferBackend> = RaftNode::default();
        let request = append_entries_request(1, 2, 0, 0, vec![], 0);
        let response = raft.handle_append_entries(request);
        assert_eq!(response, AppendEntriesResponse { term: 1, success: true });
    }

    #[test]
    fn handle_append_entries_rpc_fails() {
        let mut raft: RaftNode<BufferBackend> = RaftNode::default();
        raft.persistent_state = Arc::new(Mutex::new(persistent_state(1, Some(1), vec![])));

        let request = append_entries_request(2, 1, 2, 1, vec![1, 1, 1, 2, 2], 2);
        let response = raft.handle_append_entries(request);
        
        assert_eq!(response, AppendEntriesResponse { term: 2, success: false });
    }
    #[test]
    fn handle_append_entries_rpc_fails_because_node_out_of_date() {
        let mut raft: RaftNode<BufferBackend> = RaftNode::default();
        raft.persistent_state = Arc::new(Mutex::new(persistent_state(2, None, vec![])));
        let request = append_entries_request(1, 1, 0, 0, vec![], 0);
        let response = raft.handle_append_entries(request);
        
        assert_eq!(response, AppendEntriesResponse { term: 2, success: false });
    }
}