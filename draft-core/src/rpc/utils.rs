use crate::node::{PersistentState, VolatileState};
use crate::rpc::{AppendEntriesRequest, VoteRequest};
use bytes::Bytes;
use hashbrown::HashMap;

#[allow(dead_code)]
pub(crate) fn vote_request(
    term: usize,
    candidate_id: usize,
    last_log_index: usize,
    last_log_term: usize,
) -> VoteRequest {
    VoteRequest {
        term,
        candidate_id,
        last_log_index,
        last_log_term,
    }
}

#[allow(dead_code)]
pub(crate) fn append_entries_request(
    term: usize,
    leader_id: usize,
    previous_log_index: usize,
    previous_log_term: usize,
    terms: Vec<usize>,
    leader_commit_index: usize,
) -> AppendEntriesRequest {
    let mut log = Vec::new();
    log.extend(terms.iter().map(|term| (*term, Bytes::from(""))));
    AppendEntriesRequest {
        term,
        leader_id,
        previous_log_index,
        previous_log_term,
        entries: log,
        leader_commit_index,
    }
}

#[allow(dead_code)]
pub(crate) fn persistent_state(
    current_term: usize,
    voted_for: Option<usize>,
    terms: Vec<usize>,
) -> PersistentState {
    let mut log = Vec::new();
    log.extend(terms.iter().map(|term| (*term, Bytes::from(""))));
    PersistentState {
        log,
        current_term,
        voted_for,
    }
}

#[allow(dead_code)]
pub(crate) fn volatile_state(
    commit_index: usize,
    last_applied: usize,
    next_index: Option<HashMap<usize, Option<usize>>>,
    match_index: Option<HashMap<usize, Option<usize>>>,
) -> VolatileState {
    VolatileState {
        commit_index,
        last_applied,
        next_index,
        match_index,
    }
}
