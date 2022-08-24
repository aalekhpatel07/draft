use thiserror::Error;


#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum RequestVoteRPCError {
    #[error("The receiver node ({self_id}) is in term ({handler_term}) which is higher than the candidate node's term ({requested_term})")]
    NodeOutOfDate { self_id: usize, handler_term: usize, requested_term: usize },
    #[error("The reciver node ({self_id}) has already voted for ({voted_for}) in this election term which is different from the candidate ({requested_node_id}).")]
    AlreadyVoted { self_id: usize, voted_for: usize, requested_node_id: usize },
    #[error(
        "The candidate node's ({requested_node_id}) log is not as fresh as the receiving node ({self_id})'s log.\n
         The candidate node's last log entry has term ({requested_last_log_entry_term}) and the receiving node's last log entry is ({self_last_log_entry_term:?}).\n
         The candidate node's has ({requested_num_log_entries}) log entries and the receiving node has ({self_num_log_entries}) log entries.
        "
    )]
    CandidateNodeHasStaleLog { 
        self_id: usize, 
        requested_node_id: usize, 
        requested_last_log_entry_term: usize, 
        self_last_log_entry_term: usize,
        requested_num_log_entries: usize,
        self_num_log_entries: usize
    },
    #[error(
        "The candidate node ({requested_node_id}) has a staler log than the receiving node ({self_id}).\n
         The candidate node's log is empty but the receiving node's log is not.\n
         The receiving node's log has ({requested_num_log_entries}) entries and the last entry has term ({last_log_entry_term}).
        "
    )]
    CandidateNodeHasEmptyLog { self_id: usize, requested_node_id: usize, requested_num_log_entries: usize, last_log_entry_term: usize },
}
