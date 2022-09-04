use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::RaftNode;


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendEntriesRequest {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendEntriesResponse {}


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
        "The recipient node's ({self_id}) log does not contain the tail entry (specified by the requested 1-based index ({requested_last_log_index})). \n
         

        "
    )]
    RecipientHasNoMatchingLogEntry {
        self_id: usize,
        requested_node_id: usize,
        requested_last_log_index: usize,
        requested_last_log_entry_term: usize,
        self_last_log_entry_term: usize,
        requested_num_log_entries: usize,
        self_num_log_entries: usize,
        latest_term: usize,
    },
}

pub fn handle_append_entries(
    _receiver_node: &mut RaftNode,
    _request: AppendEntriesRequest
) -> Result<AppendEntriesResponse, AppendEntriesRPCError> {
    todo!("Implement handle append entries");
}