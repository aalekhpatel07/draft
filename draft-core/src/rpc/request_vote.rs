use crate::RaftNode;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use thiserror::Error;

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct VoteRequest {
    pub term: usize,
    pub candidate_id: usize,
    pub last_log_index: usize,
    pub last_log_term: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct VoteResponse {
    pub term: usize,
    pub vote_granted: bool,
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum RequestVoteRPCError {
    #[error(
        "The receiver node ({self_id}) is in term ({handler_term}) which is higher than the candidate node's term ({requested_term})
         The latest term is ({latest_term}).
        "
    )]
    NodeOutOfDate {
        self_id: usize,
        handler_term: usize,
        requested_term: usize,
        latest_term: usize,
    },
    #[error(
        "The reciver node ({self_id}) has already voted for ({voted_for}) in this election term which is different from the candidate ({requested_node_id}). \n 
         The latest term is ({latest_term}).
        "
    )]
    AlreadyVoted {
        self_id: usize,
        voted_for: usize,
        requested_node_id: usize,
        latest_term: usize,
    },
    #[error(
        "The candidate node's ({requested_node_id}) log is not as fresh as the receiving node ({self_id})'s log.\n
         The candidate node's last log entry has term ({requested_last_log_entry_term}) and the receiving node's last log entry is ({self_last_log_entry_term:?}).\n
         The candidate node's has ({requested_num_log_entries}) log entries and the receiving node has ({self_num_log_entries}) log entries.
         The latest term is ({latest_term}).
        "
    )]
    CandidateNodeHasStaleLog {
        self_id: usize,
        requested_node_id: usize,
        requested_last_log_entry_term: usize,
        self_last_log_entry_term: usize,
        requested_num_log_entries: usize,
        self_num_log_entries: usize,
        latest_term: usize,
    },
    #[error(
        "The candidate node ({requested_node_id}) has a staler log than the receiving node ({self_id}).\n
         The candidate node's log is empty but the receiving node's log is not.\n
         The receiving node's log has ({requested_num_log_entries}) entries and the last entry has term ({last_log_entry_term}).
         The latest term is ({latest_term}).
        "
    )]
    CandidateNodeHasEmptyLog {
        self_id: usize,
        requested_node_id: usize,
        requested_num_log_entries: usize,
        last_log_entry_term: usize,
        latest_term: usize,
    },
}

/// Given a vote request RPC, process the request without making any modifications to the state
/// as described in Section 5.4.1 and Figure 3.
pub fn handle_request_vote<S>(
    receiver_node: &RaftNode<S>,
    request: VoteRequest,
) -> Result<VoteResponse, RequestVoteRPCError> {
    let persistent_state_guard = receiver_node
        .persistent_state
        .lock()
        .expect("Failed to lock persistent state");

    if request.term < persistent_state_guard.current_term {
        // The candidate is straight up out-of-date.
        // Inform it of the latest term that we know of.
        return Err(RequestVoteRPCError::NodeOutOfDate {
            self_id: receiver_node.metadata.id,
            handler_term: persistent_state_guard.current_term,
            requested_term: request.term,
            latest_term: persistent_state_guard.current_term,
        });
    }

    if let Some(vote_recipient) = persistent_state_guard.voted_for {
        // Check if we have already voted in the requested term.
        // If so, reject if a different candidate requests vote.
        if (vote_recipient != request.candidate_id)
            && (request.term == persistent_state_guard.current_term)
        {
            // We've already voted for someone else.
            // Oops. Sorry, dear requester.
            return Err(RequestVoteRPCError::AlreadyVoted {
                self_id: receiver_node.metadata.id,
                voted_for: vote_recipient,
                requested_node_id: request.candidate_id,
                latest_term: request.term,
            });
        }
    }

    let self_log_len = persistent_state_guard.log.len();
    match (self_log_len, request.last_log_index) {
        (1.., 0) => {
            // We have some entries but candidate comes bearing no entries.
            // Thus the candidate has a stale log and we must inform it.
            return Err(RequestVoteRPCError::CandidateNodeHasEmptyLog {
                self_id: receiver_node.metadata.id,
                requested_node_id: request.candidate_id,
                requested_num_log_entries: request.last_log_index,
                last_log_entry_term: persistent_state_guard.log.last().unwrap().0,
                latest_term: request.term,
            });
        }
        (1.., 1..) => {
            // We have entries as well as the candidate has some entries.
            // Determine which log is fresher by comparing the terms of the last entries.
            // If the terms are the same, the longer log is the fresher log.
            // Otherwise, the log with the higher last term is the fresher log.

            // Let's match the terms of the last entries.
            let self_last_log_entry_term = persistent_state_guard.log.last().unwrap().0;
            let request_last_log_term = request.last_log_term;

            match self_last_log_entry_term.cmp(&request_last_log_term) {
                Ordering::Equal => {
                    // The terms of the last entries are the same. In this case,
                    // the longer log is more fresh. (Section 5.4.1)
                    let candidate_log_size = request.last_log_index;
                    if self_log_len > candidate_log_size {
                        return Err(RequestVoteRPCError::CandidateNodeHasStaleLog {
                            self_id: receiver_node.metadata.id,
                            requested_node_id: request.candidate_id,
                            requested_last_log_entry_term: request.last_log_term,
                            self_last_log_entry_term,
                            requested_num_log_entries: request.last_log_index,
                            self_num_log_entries: self_log_len,
                            latest_term: request.term,
                        });
                    }
                }
                Ordering::Greater => {
                    // We have a higher term (for the last log entry) than the candidate's.
                    // Thus our log is more fresh. Notify the candidate. (Section 5.4.1)
                    return Err(RequestVoteRPCError::CandidateNodeHasStaleLog {
                        self_id: receiver_node.metadata.id,
                        requested_node_id: request.candidate_id,
                        requested_last_log_entry_term: request.last_log_term,
                        self_last_log_entry_term,
                        requested_num_log_entries: request.last_log_index,
                        self_num_log_entries: persistent_state_guard.log.len(),
                        latest_term: request.term,
                    });
                }
                Ordering::Less => {
                    // The candidate has a higher term. So the candidate has a fresher log.
                }
            };
        }
        // In any other case, there can't ever be a rejection.
        _ => {}
    }
    Ok(VoteResponse {
        vote_granted: true,
        term: request.term,
    })
}

#[cfg(test)]
mod tests {
    pub use super::*;
    pub use crate::*;
    pub use std::sync::{Arc, Mutex};

    #[allow(unused_imports)]
    pub use crate::rpc::utils::*;

    #[test]
    fn serialize_to_string_works() {
        let request = vote_request(0, 2, 0, 0);
        let serialized  = serde_json::to_string(&request);
        assert!(serialized.is_ok());
        assert_eq!(serialized.unwrap(), r#"{"term":0,"candidate_id":2,"last_log_index":0,"last_log_term":0}"#.to_owned());
    }

    #[test]
    fn deserialize_works() {
        let as_string = "{\"term\":0,\"candidate_id\":2,\"last_log_index\":0,\"last_log_term\":0}";
        let deserialized = serde_json::from_str::<VoteRequest>(&as_string);
        assert!(deserialized.is_ok());
        assert_eq!(deserialized.unwrap(), vote_request(0, 2, 0, 0));
    }

    macro_rules! request_vote_test {
        (
            $(#[$meta:meta])*
            $func_name:ident,
            $initial_persistent_state:expr,
            $final_persistent_state:expr,
            $request:expr,
            $response:pat
        ) => {
            $(#[$meta])*
            #[test]
            pub fn $func_name() {
                utils::set_up_logging();
                let mut receiver_raft: RaftNode<BufferBackend> = RaftNode::default();
                receiver_raft.persistent_state = Arc::new(Mutex::new($initial_persistent_state));
                assert!(receiver_raft.are_terms_non_decreasing());
                let request: VoteRequest = $request;
                let observed_response = receiver_raft.try_handle_request_vote(request);
                assert!(matches!(observed_response, $response));
                assert_eq!(*receiver_raft.persistent_state.lock().unwrap(), $final_persistent_state);
                assert!(receiver_raft.are_terms_non_decreasing());
            }
        };
    }

    request_vote_test!(
        /// Test that a receiver node grants the vote to any vote request initially.
        initial,
        PersistentState {
            current_term: 0,
            voted_for: None,
            log: vec![]
        },
        PersistentState {
            current_term: 0,
            voted_for: Some(2),
            log: vec![]
        },
        vote_request(0, 2, 0, 0),
        Ok(VoteResponse {
            term: 0,
            vote_granted: true
        })
    );

    request_vote_test!(
        /// Test that a receiver node grants
        /// the vote to a vote request when the receiver
        /// hasn't voted for anybody yet.
        grant_vote_if_not_voted_yet_and_log_at_least_up_to_date,
        PersistentState {
            current_term: 0,
            voted_for: None,
            log: vec![]
        },
        PersistentState {
            current_term: 1,
            voted_for: Some(2),
            log: vec![]
        },
        vote_request(1, 2, 0, 0),
        Ok(VoteResponse {
            term: 1,
            vote_granted: true
        })
    );

    request_vote_test!(
        /// Test that a receiver node rejects
        /// a vote request if the candidate
        /// is in a stale election term.
        reject_vote_if_candidate_in_stale_election_term,
        persistent_state(2, None, vec![]),
        persistent_state(2, None, vec![]),
        vote_request(1, 2, 0, 0),
        Err(RequestVoteRPCError::NodeOutOfDate {
            self_id: 1,
            handler_term: 2,
            requested_term: 1,
            latest_term: 2
        })
    );

    request_vote_test!(
        /// Test that a receiver node approves
        /// a vote request to the candidate
        /// if the receiver has already voted for that term
        /// but the vote was for the same candidate.
        grant_vote_if_already_voted_for_same_candidate,
        persistent_state(2, Some(2), vec![]),
        persistent_state(2, Some(2), vec![]),
        vote_request(2, 2, 0, 0),
        Ok(VoteResponse {
            term: 2,
            vote_granted: true
        })
    );

    request_vote_test!(
        /// Test that a receiver node denies
        /// a vote request to the candidate
        /// if the receiver has already voted for that term
        /// but the vote was for the other candidate.
        reject_vote_if_already_voted_for_other_candidate,
        persistent_state(0, Some(3), vec![]),
        persistent_state(0, Some(3), vec![]),
        vote_request(0, 2, 0, 0),
        Err(RequestVoteRPCError::AlreadyVoted {
            self_id: 1,
            voted_for: 3,
            requested_node_id: 2,
            latest_term: 0
        })
    );

    request_vote_test!(
        /// Test that a receiver node grants
        /// a vote request to the candidate
        /// if the receiver has already voted for the candidate
        /// but in a previous term.
        grant_vote_if_already_voted_for_same_candidate_but_in_a_previous_term,
        persistent_state(0, Some(2), vec![]),
        persistent_state(2, Some(2), vec![]),
        vote_request(2, 2, 0, 0),
        Ok(VoteResponse {
            term: 2,
            vote_granted: true
        })
    );

    request_vote_test!(
        /// Test that a receiver node grants
        /// a vote request of the candidate
        /// if the receiver has already voted for some other node
        /// but in a previous term.
        grant_vote_if_already_voted_for_other_candidate_but_in_a_previous_term,
        persistent_state(0, Some(3), vec![]),
        persistent_state(2, Some(2), vec![]),
        vote_request(2, 2, 0, 0),
        Ok(VoteResponse {
            term: 2,
            vote_granted: true
        })
    );

    request_vote_test!(
        /// Test that a receiver node rejects
        /// a vote request of the candidate
        /// if the receiver has not already voted
        /// and has a fresher log than the candidate.
        reject_vote_if_not_already_voted_but_candidate_log_is_stale,
        persistent_state(2, None, vec![1]),
        persistent_state(2, None, vec![1]),
        vote_request(2, 2, 0, 0),
        Err(RequestVoteRPCError::CandidateNodeHasEmptyLog {
            self_id: 1,
            requested_node_id: 2,
            requested_num_log_entries: 0,
            last_log_entry_term: 1,
            latest_term: 2
        })
    );

    request_vote_test!(
        /// Test that a receiver node grants
        /// a vote request to the candidate
        /// if the receiver has not already voted
        /// and has candidate has the same log as receiver's.
        grant_vote_if_not_already_voted_and_candidate_log_is_same_as_receiver_log,
        persistent_state(2, None, vec![1]),
        persistent_state(2, Some(2), vec![1]),
        vote_request(2, 2, 1, 1),
        Ok(VoteResponse {
            term: 2,
            vote_granted: true
        })
    );

    request_vote_test!(
        /// Test that a receiver node rejects
        /// a vote request of the candidate
        /// if the receiver has not already voted
        /// and has the candidate's last log term 
        /// is earlier than the receiver's last entry's.
        reject_vote_if_not_already_voted_and_candidate_last_log_term_is_earlier_than_receiver_log_last_entry_term,
        persistent_state(2, None, vec![2]),
        persistent_state(2, None, vec![2]),
        vote_request(2, 2, 1, 1),
        Err(RequestVoteRPCError::CandidateNodeHasStaleLog { 
            self_id: 1, 
            requested_node_id: 2, 
            requested_last_log_entry_term: 1, 
            self_last_log_entry_term: 2, 
            requested_num_log_entries: 1, 
            self_num_log_entries: 1,
            latest_term: 2
        })
    );

    request_vote_test!(
        /// Test that a receiver node rejects
        /// a vote request of the candidate
        /// if the receiver has not already voted
        /// and has the candidate has less entries
        /// than the recipient node.
        reject_vote_if_not_already_voted_and_candidate_has_less_entries_than_receiver,
        persistent_state(2, None, vec![2, 2]),
        persistent_state(2, None, vec![2, 2]),
        vote_request(2, 2, 1, 2),
        Err(RequestVoteRPCError::CandidateNodeHasStaleLog {
            self_id: 1,
            requested_node_id: 2,
            requested_last_log_entry_term: 2,
            self_last_log_entry_term: 2,
            requested_num_log_entries: 1,
            self_num_log_entries: 2,
            latest_term: 2
        })
    );

    request_vote_test!(
        /// Test that a receiver node grants
        /// a vote request to the candidate
        /// if the receiver has not already voted
        /// and has the candidate has more entries than receiver.
        grant_vote_if_not_already_voted_and_candidate_has_more_entries_than_receiver,
        persistent_state(1, None, vec![2, 2]),
        persistent_state(2, Some(2), vec![2, 2]),
        vote_request(2, 2, 4, 3),
        Ok(VoteResponse {
            term: 2,
            vote_granted: true
        })
    );

    request_vote_test!(
        /// Test that a receiver node rejects
        /// a vote request of the candidate
        /// despite itself having a stale log,
        /// because it was able to determine that
        /// the candidate has even more stale log.
        reject_vote_if_already_voted_in_some_previous_term_but_candidate_has_more_stale_log_than_receiver,
        persistent_state(2, Some(4), vec![2, 2]),
        persistent_state(6, Some(4), vec![2, 2]),
        vote_request(6, 2, 2, 1),
        Err(RequestVoteRPCError::CandidateNodeHasStaleLog {
            self_id: 1, 
            requested_node_id: 2, 
            requested_last_log_entry_term: 1, 
            self_last_log_entry_term: 2, 
            requested_num_log_entries: 2, 
            self_num_log_entries: 2,
            latest_term: 6
        })
    );

    request_vote_test!(
        /// Test that a receiver node rejects
        /// a vote request of the candidate
        /// if it has already voted for it in the same
        /// term but the candidate has more stale logs than the
        /// receiver.
        reject_vote_if_already_voted_for_the_same_candidate_in_this_term_but_candidate_has_more_stale_log_than_receiver,
        persistent_state(2, Some(2), vec![2, 2]),
        persistent_state(2, Some(2), vec![2, 2]),
        vote_request(2, 2, 2, 1),
        Err(RequestVoteRPCError::CandidateNodeHasStaleLog {
            self_id: 1, 
            requested_node_id: 2, 
            requested_last_log_entry_term: 1, 
            self_last_log_entry_term: 2, 
            requested_num_log_entries: 2, 
            self_num_log_entries: 2,
            latest_term: 2
        })
    );

    request_vote_test!(
        /// Test that a receiver node rejects
        /// a vote request of the candidate
        /// if it already voted for it in some previous term
        /// and the candidate has stale logs compared to the receiver.
        reject_vote_if_already_voted_for_the_same_candidate_in_some_previous_term_and_candidate_has_stale_log,
        persistent_state(1, Some(2), vec![2, 2]),
        persistent_state(2, Some(2), vec![2, 2]),
        vote_request(2, 2, 2, 1),
        Err(RequestVoteRPCError::CandidateNodeHasStaleLog { 
            self_id: 1, 
            requested_node_id: 2, 
            requested_last_log_entry_term: 1, 
            self_last_log_entry_term: 2, 
            requested_num_log_entries: 2, 
            self_num_log_entries: 2,
            latest_term: 2
        })
    );
}
