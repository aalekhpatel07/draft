use serde::{Deserialize, Serialize};
use thiserror::Error;
use crate::{Log, RaftNode, Term};
use derivative::Derivative;
use tracing::trace;
use itertools::{
    Itertools,
    EitherOrBoth
};



#[derive(Debug, Clone, Serialize, Deserialize, Derivative)]
#[derivative(Default)]
pub struct AppendEntriesRequest {
    pub term: usize,
    pub leader_id: usize,
    pub previous_log_index: usize,
    pub previous_log_term: usize,
    #[derivative(Default(value="vec![]"))]
    pub entries: Vec<Log>,
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
         The latest term is ({latest_term}). The leader requested with ({requested_entries_len}) entries.
        "
    )]
    NodeOutOfDate {
        self_id: usize,
        handler_term: usize,
        requested_term: usize,
        latest_term: usize,
        requested_entries_len: usize
    },
    #[error(
        "The recipient node's ({self_id}) log does not contain a tail entry 
        at the leader's ({requested_node_id}) log's 1-based index ({requested_previous_log_index}) (Note: 0 means log is empty.).\n
        The recipient and the leader have inconsistent logs (at least) to the right of ({requested_previous_log_index}).\n
        The leader claims its tail entry is (term: {requested_previous_log_term}, index: {requested_previous_log_index}) but 
        the the recipient's tail entry is (term: {self_previous_log_term}, index: {self_previous_log_index}).\n
        The latest term is ({latest_term}). The leader requested with ({requested_entries_len}) entries.
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
        requested_entries_len: usize,
    },

    #[error(
        "The recipient node's ({self_id}) log contains some ({self_log_len}) entries but the leader ({requested_node_id}) requested without a tail to look for.
         The latest term is ({latest_term}). The leader's commit_index is ({commit_index}).
         The leader requested with ({requested_entries_len}) entries.
        "
    )]
    LeaderHasRewindedExcessively {
        self_id: usize,
        requested_node_id: usize,
        requested_previous_log_index: usize,
        requested_previous_log_term: usize,
        self_log_len: usize,
        latest_term: usize,
        commit_index: usize,
        requested_entries_len: usize
    }
}

// pub fn handle_heartbeat(
//     receiver_node: &RaftNode,
//     request: AppendEntriesRequest
// ) -> Result<AppendEntriesResponse, AppendEntriesRPCError>
// {
//     if receiver_node.persistent_state.log.is_empty() {
//         // We have an empty log.

//         if request.previous_log_index == 0 {
//             // So does the leader.
//             return Ok(AppendEntriesResponse { term: request.term, success: true })
//         }

//         // Leader has some entries in the log 
//         // but no new entries for us to commit.

//         // This may happen if we were in the minority
//         // of nodes that failed to replicate some of the logs
//         // at some point in the past. Now, the leader claims
//         // it has some entries but we don't have anything in our local log.
//         // This is a log inconsistency and we let the leader know
//         // of it. The leader will retry later. (Section 5.3)
        
//         // Eventually, the leader will realize we need the full log,
//         // in which case, we'll get some new entries to append and
//         // we'll live happily ever after. (umm, probably?)

//         return Err(AppendEntriesRPCError::RecipientHasNoMatchingLogEntry {
//             self_id: receiver_node.metadata.id, 
//             requested_node_id: request.leader_id, 
//             requested_previous_log_index: request.previous_log_index, 
//             requested_previous_log_term: request.previous_log_term, 
//             self_previous_log_index: 0, // because our log is empty in this case. 
//             self_previous_log_term: 0, // this is irrelevant if our log is empty.
//             latest_term: request.term,
//             requested_entries_len: request.entries.len()
//         })
//     }

//     // We have some entries in our log. Must check what exists at that index.

//     if request.previous_log_index == 0 {
//         // Leader has been lagging behind. 
//         // Claims to have no tail to append to.
        
//         // Likely an inconsistent state. 
//         // Leader has rewinded more than necessary.
//         return Err(AppendEntriesRPCError::LeaderHasRewindedExcessively { 
//             self_id: receiver_node.metadata.id, 
//             requested_node_id: request.leader_id, 
//             requested_previous_log_index: 0, 
//             requested_previous_log_term: 0, 
//             self_log_len: receiver_node.persistent_state.log.len(), 
//             latest_term: request.term, 
//             commit_index: request.leader_commit_index,
//             requested_entries_len: request.entries.len()
//         });
//     }


//     // First normalize the indices to 0-based.
//     let requested_previous_log_index = request.previous_log_index - 1;

//     // What's the term of the entry in our log at this index?
//     match receiver_node.persistent_state.log.get(requested_previous_log_index) 
//     {
//         Some((self_previous_log_term, _)) => {
//             // We do have some entry at that index.
//             if *self_previous_log_term != request.previous_log_term {
//                 // Different terms so a log inconsistency. (Section 5.3)
//                 return Err(AppendEntriesRPCError::RecipientHasNoMatchingLogEntry { 
//                     self_id: receiver_node.metadata.id, 
//                     requested_node_id: request.leader_id,
//                     requested_previous_log_index: request.previous_log_index,
//                     requested_previous_log_term: request.previous_log_term, 
//                     self_previous_log_index: request.previous_log_index,
//                     self_previous_log_term: *self_previous_log_term, 
//                     latest_term: request.term,
//                     requested_entries_len: request.entries.len()
//                 });
//             }
//         },
//         None => {
//             // Leader points to a tail that we don't have.
//             // This is a log inconsistency.
//             return Err(AppendEntriesRPCError::RecipientHasNoMatchingLogEntry { 
//                 self_id: receiver_node.metadata.id, 
//                 requested_node_id: request.leader_id,
//                 requested_previous_log_index: request.previous_log_index,
//                 requested_previous_log_term: request.previous_log_term, 
//                 self_previous_log_index: request.previous_log_index, // doesn't matter because we don't have that entry.
//                 self_previous_log_term: 0, 
//                 latest_term: request.term,
//                 requested_entries_len: request.entries.len()
//             });
//         }
//     }

//     // Heartbeat won't mutate any state so we can return here.
//     return Ok(AppendEntriesResponse { term: request.term, success: true });
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
            latest_term: receiver_node.persistent_state.current_term,
            requested_entries_len: request.entries.len()
        });
    }

    if receiver_node.persistent_state.log.is_empty() {
        // We have an empty log.
        if request.previous_log_index == 0 {
            // So does the leader.

            if !request.entries.is_empty() {
                // Just append the new entries to our log if available.
                let num_entries = request.entries.len();
                
                trace!("Clearing our log and replacing it with ({num_entries}) entries provided by the leader");
                receiver_node.persistent_state.log.clear();
                receiver_node.persistent_state.log.extend(request.entries.into_iter());
            }

            return Ok(AppendEntriesResponse { term: request.term, success: true });
        }

        // Leader has some entries in the log and has asked us to locate a tail, which
        // we don't have.

        // This may happen if we were in the minority
        // of nodes that failed to replicate some of the logs
        // at some point in the past. Now, the leader claims
        // it has some entries but we don't have anything in our local log.
        // This is a log inconsistency and we let the leader know
        // of it. The leader will retry later. (Section 5.3)
        
        // Eventually, the leader will realize we need the full log
        // and we'll get all the new entries to append and
        // we'll be up to speed.

        return Err(AppendEntriesRPCError::RecipientHasNoMatchingLogEntry {
            self_id: receiver_node.metadata.id, 
            requested_node_id: request.leader_id, 
            requested_previous_log_index: request.previous_log_index, 
            requested_previous_log_term: request.previous_log_term, 
            self_previous_log_index: 0, // because our log is empty in this case. 
            self_previous_log_term: 0, // this is irrelevant if our log is empty.
            latest_term: request.term,
            requested_entries_len: request.entries.len()
        });
    }

    // We have some entries in our local log.
    // Do we have one at the specified index though?

    // If we don't have enough entries, just stop early.
    if receiver_node.persistent_state.log.len() < request.previous_log_index {
        // Leader doesn't have a valid tail to point to.
        // Our stored log doesn't contain the tail entry that the leader wants us to
        // append new entries to.
        return Err(AppendEntriesRPCError::RecipientHasNoMatchingLogEntry { 
            self_id: receiver_node.metadata.id, 
            requested_node_id: request.leader_id, 
            requested_previous_log_index: request.previous_log_index, 
            requested_previous_log_term: request.previous_log_term, 
            self_previous_log_index: 0, 
            self_previous_log_term: 0, 
            latest_term: request.term, 
            requested_entries_len: request.entries.len()
        });
    }

        
        // Find the first entry from given entries that our log doesn't contain.
        
        // Get terms of all requested entries.
        let requested_entries_terms = 
            request
            .entries
            .iter()
            .map(|entry| entry.term());


        // Get terms of all stored entries starting from (and including)
        // the tail entry.
        // If request.previous_log_index is 0, it means no tail entry exists 
        // and we must start from the first entry.
        let stored_entries_terms = 
            receiver_node
            .persistent_state
            .log
            .iter()
            .skip(request.previous_log_index) // Start at the given tail entry.
            .map(|entry| entry.term());

        let mut should_extend_remaining_entries_from: Option<usize> = None;
        let mut should_remove_stored_entries_from: Option<usize> = None;
        
        let first_conflicting_index = 
        requested_entries_terms
        .zip_longest(stored_entries_terms)
        .enumerate()
        .position(|(index, pair)| {
            match pair {
                // The position iterator short-circuits either at the first non-matching term pair,
                // or when we have exhausted all stored entries.
                EitherOrBoth::Both(requested_entry, stored_entry) => {
                    if requested_entry != stored_entry {
                        // Found a conflicting index. Mark it so that we can continue adding requested entries from
                        // this index.
                        should_extend_remaining_entries_from = Some(index);
                        true
                    }
                    else {
                        false
                    }
                },
                // This may only get evaluated once, when the stored entries are exhausted.
                // If we reach here, it must mean there were no conflicting pairs.
                EitherOrBoth::Left(_) => {
                    // Our stored log is a prefix of the requested entries.
                    // Now we must append the remaining entries.
                    // First time this happens, we store the index.
                    if should_extend_remaining_entries_from.is_none() {
                        should_extend_remaining_entries_from = Some(index);
                    }
                    false
                },
                EitherOrBoth::Right(_) => {
                    // The requested entries are already a sub-array of our stored log.
                    // If our stored log as more entries (i.e. a suffix that doesn't exist in the requested entries),
                    // then mark its starting point.
                    if should_remove_stored_entries_from.is_none() {
                        should_remove_stored_entries_from = Some(request.previous_log_index + index);
                    }
                    false
                },
            }
        });

        match first_conflicting_index {
            Some(index_to_remove_from) => {
                // We need to remove all stored entries including and following (previous_log_index + index).
                let index_to_drain_local_log_from = request.previous_log_index + index_to_remove_from;

                let removed_local_entries = receiver_node.persistent_state.log.drain(index_to_drain_local_log_from..).collect::<Vec<Log>>();
                let removed_count = removed_local_entries.len();
                trace!("Found inconsistent log. Removed local entries (count: {removed_count}, range: ({index_to_drain_local_log_from}..))");
            },
            None => {
                // Logs are consistent so far.
            }
        }

        // We can guarantee that exactly one of should_remove_stored_entries_from
        // or should_extend_remaining_entries_from is Some, if any at all.
        assert!(!(should_extend_remaining_entries_from.is_some() && should_remove_stored_entries_from.is_some()));

        // Either we need to remove an extra suffix from our log,
        if let Some(index_of_suffix_to_remove) = should_remove_stored_entries_from {
            receiver_node.persistent_state.log.drain(index_of_suffix_to_remove..);
        }

        // Or we may append a suffix from the requested entries.
        if let Some(index_to_extend_from) = should_extend_remaining_entries_from {
            receiver_node.persistent_state.log.extend_from_slice(
                request.entries.get(index_to_extend_from..).unwrap()
            );
        }

        // Update our commit index to include our newly updated entries which the leader 
        // guarantees to have committed.
        let last_new_entry_index = receiver_node.persistent_state.log.len();
        if request.leader_commit_index > receiver_node.volatile_state.commit_index {
            receiver_node.volatile_state.commit_index = request.leader_commit_index.min(last_new_entry_index)
        }

        return Ok(AppendEntriesResponse{ term: request.term, success: true });
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
        entries: Vec<Log>,
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