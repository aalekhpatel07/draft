use std::cmp::Ordering;

use serde::{Deserialize, Serialize};
use thiserror::Error;
use bytes::Bytes;
use crate::RaftNode;


#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct AppendEntriesRequest {
    pub term: usize,
    pub leader_id: usize,
    pub previous_log_index: usize,
    pub previous_log_term: usize,
    pub entries: Vec<Bytes>,
    pub leader_commit_index: usize
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct AppendEntriesResponse {
    pub term: usize,
    pub success: bool
}


#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum AppendEntriesRPCError {
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
        "The recipient node's ({self_id}) log does not contain a tail entry 
        at the leader's ({requested_node_id}) log's 1-based index ({requested_previous_log_index}) (Note: 0 means log is empty.).\n
        The recipient and the leader have inconsistent logs (at least) to the right of ({requested_previous_log_index}).\n
        The leader claims its tail entry is (term: {requested_previous_log_term}, index: {requested_previous_log_index}) but 
        the the recipient's tail entry is (term: {self_previous_log_term}, index: {self_previous_log_index}).\n
        The latest term is ({latest_term}).
        "
    )]
    RecipientHasNoMatchingLogEntry {
        self_id: usize,
        requested_node_id: usize,
        requested_previous_log_index: usize,
        requested_previous_log_term: usize,
        self_previous_log_index: usize,
        self_previous_log_term: usize,
        latest_term: usize,
    },

    #[error(
        "The recipient node's ({self_id}) log contains some ({self_log_len}) entries but the leader ({requested_node_id}) requested without a tail to look for.
         The latest term is ({latest_term}). The leader's commit_index is ({commit_index}).
        "
    )]
    LeaderHasRewindedExcessively {
        self_id: usize,
        requested_node_id: usize,
        requested_previous_log_index: usize,
        requested_previous_log_term: usize,
        self_log_len: usize,
        latest_term: usize,
        commit_index: usize
    }
}

// pub fn handle_heartbeat(
//     receiver_node: &RaftNode,
//     request: AppendEntriesRequest
// ) -> Result<AppendEntriesResponse, AppendEntriesRPCError>
// {
    
// }

pub fn handle_append_entries(
    receiver_node: &mut RaftNode,
    request: AppendEntriesRequest
) -> Result<AppendEntriesResponse, AppendEntriesRPCError> 
{

    if request.term < receiver_node.persistent_state.current_term {
        // The candidate is straight up out-of-date.
        // Inform it of the latest term that we know of.
        return Err(AppendEntriesRPCError::NodeOutOfDate {
            self_id: receiver_node.metadata.id,
            handler_term: receiver_node.persistent_state.current_term,
            requested_term: request.term,
            latest_term: receiver_node.persistent_state.current_term
        });
    }

    if request.entries.len() == 0 {
        // This is a hearbeat request.
        // No need to make any mutations to our states.

        // Just be a happy little node and respond to the question:
        // Does our log contain an entry at previous_log_index that has term previous_log_term?

        if receiver_node.persistent_state.log.is_empty() {
            // We have an empty log.

            if request.previous_log_index == 0 {
                // So does the leader.
                return Ok(AppendEntriesResponse { term: request.term, success: true })
            }

            // Leader has some entries in the log 
            // but no new entries for us to commit.

            // This may happen if we were in the minority
            // of nodes that failed to replicate committed logs,
            // at some point in the past. Now, the leader claims
            // it has some committed entries but we don't have anything in our local log.
            // This is a log inconsistency and we let the leader know
            // of it. The leader will retry later. (Section 5.3)
            
            // Eventually, the leader will realize we need the full log,
            // in which case, we'll get some new entries to append and
            // we'll live happily ever after. (ugh, probably?)

            return Err(AppendEntriesRPCError::RecipientHasNoMatchingLogEntry {
                self_id: receiver_node.metadata.id, 
                requested_node_id: request.leader_id, 
                requested_previous_log_index: request.previous_log_index, 
                requested_previous_log_term: request.previous_log_term, 
                self_previous_log_index: 0, // because our log is empty in this case. 
                self_previous_log_term: 0, // this is irrelevant if our log is empty.
                latest_term: request.term
            })
        }

        // We have some entries in our log. Must check what exists at that index.

        if request.previous_log_index == 0 {
            // Leader has been lagging behind. 
            // Claims to have no tail to append to.
            
            // Likely an inconsistent state. 
            // Leader has rewinded more than necessary.
            return Err(AppendEntriesRPCError::LeaderHasRewindedExcessively { 
                self_id: receiver_node.metadata.id, 
                requested_node_id: request.leader_id, 
                requested_previous_log_index: 0, 
                requested_previous_log_term: 0, 
                self_log_len: receiver_node.persistent_state.log.len(), 
                latest_term: request.term, 
                commit_index: request.leader_commit_index
            });
        }


        // First normalize the indices to 0-based.
        let requested_previous_log_index = request.previous_log_index - 1;

        // What's the term of the entry in our log at this index?
        match receiver_node.persistent_state.log.get(requested_previous_log_index) 
        {
            Some((self_previous_log_term, _)) => {
                // We do have some entry at that index.
                if *self_previous_log_term != request.previous_log_term {
                    // Different terms so a log inconsistency. (Section 5.3)
                    return Err(AppendEntriesRPCError::RecipientHasNoMatchingLogEntry { 
                        self_id: receiver_node.metadata.id, 
                        requested_node_id: request.leader_id,
                        requested_previous_log_index: request.previous_log_index,
                        requested_previous_log_term: request.previous_log_term, 
                        self_previous_log_index: request.previous_log_index,
                        self_previous_log_term: *self_previous_log_term, 
                        latest_term: request.term
                    });
                }
            },
            None => {
                // Leader points to a tail that we don't have.
                // This is a log inconsistency.
                return Err(AppendEntriesRPCError::RecipientHasNoMatchingLogEntry { 
                    self_id: receiver_node.metadata.id, 
                    requested_node_id: request.leader_id,
                    requested_previous_log_index: request.previous_log_index,
                    requested_previous_log_term: request.previous_log_term, 
                    self_previous_log_index: request.previous_log_index, // doesn't matter because we don't have that entry.
                    self_previous_log_term: 0, 
                    latest_term: request.term
                });
            }
        }

        // Heartbeat won't mutate any state so we can return here.
        return Ok(AppendEntriesResponse { term: request.term, success: true });
    }

    todo!("Add implementation for non-heartbeats.");

    // match request.previous_log_index.cmp(&0) {
    //     Ordering::Equal => {
    //         //
    //         // Since log indices are 1-based, we use an index of 0 to denote
    //         // the leader's log is empty.
    //         // Either we have an empty log,
    //         // or we must remove everything 
    //         // from our log anyways.
            
    //         // This is quite rare and may only happen
    //         // if something causes a leader's committed state 
    //         // to lag behind a follower severely. 
    //         // In practice, we won't need to clear
    //         // our potentially large log frequently because
    //         // a leader shouldn't lag very far behind in committing entries
    //         // to its log.
    //         receiver_node.persistent_state.log.clear();


    //     },
    //     Ordering::Greater => {

    //     },
    //     Ordering::Less => {
    //         panic!("previous log index is negative. This cannot happen.")
    //     }
    // }

    // Ok(AppendEntriesResponse { term: request.term, success: true })
}


#[cfg(test)]
pub mod tests {
    pub use crate::*;
    pub use super::*;
    pub use hashbrown::HashMap;


    #[allow(dead_code)]
    fn append_entries_request(
        term: usize,
        leader_id: usize,
        previous_log_index: usize,
        previous_log_term: usize,
        entries: Vec<Bytes>,
        leader_commit_index: usize
    ) -> AppendEntriesRequest {
        AppendEntriesRequest { term, leader_id, previous_log_index, previous_log_term, entries, leader_commit_index }
    }

    #[allow(dead_code)]
    fn persistent_state(current_term: usize, voted_for: Option<usize>, log: Vec<Log>) -> PersistentState
    {
        PersistentState { log, current_term, voted_for }
    }

    #[allow(dead_code)]
    fn volatile_state(
        commit_index: usize, 
        last_applied: usize, 
        next_index: Option<HashMap<usize, Option<usize>>>,
        match_index: Option<HashMap<usize, Option<usize>>>
    ) -> VolatileState 
    {
        VolatileState { commit_index, last_applied, next_index, match_index }
    }


    macro_rules! append_entries_test {
        (
            $(#[$meta:meta])*
            $func_name:ident,
            $initial_persistent_state:expr,
            $initial_volatile_state:expr,
            $request:expr,
            $response:pat,
            $final_persistent_state:expr,
            $final_volatile_state:expr
        ) => {
            $(#[$meta])*
            #[test]
            pub fn $func_name() {
                utils::set_up_logging();
                let mut receiver_raft = RaftNode::default();
                
                receiver_raft.persistent_state = $initial_persistent_state;
                receiver_raft.volatile_state = $initial_volatile_state;

                let request: AppendEntriesRequest = $request;
                let observed_response = receiver_raft.try_handle_append_entries(request);
                
                assert!(matches!(observed_response, $response));

                assert_eq!(receiver_raft.persistent_state, $final_persistent_state);
                assert_eq!(receiver_raft.volatile_state, $final_volatile_state);
            }
        };
    }

    append_entries_test!(
        /// Basic append entries.
        basic,
        persistent_state(0, Some(1), vec![]),
        volatile_state(0, 0, None, None),
        append_entries_request(0, 1, 0, 0, vec![], 0),
        Ok(AppendEntriesResponse { term: 0, success: true }),
        persistent_state(0, Some(1), vec![]),
        volatile_state(0, 0, None, None)
    );


}