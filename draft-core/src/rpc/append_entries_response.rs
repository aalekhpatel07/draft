use tokio::sync::mpsc::UnboundedSender;
use crate::AppendEntriesRequest;
use crate::AppendEntriesRequestWithoutLogs;
use crate::AppendEntriesResponse;
use crate::RaftNode;
use crate::Storage;

pub type Peer = usize;

/// Update our state because one of the followers responded to
/// our AppendEntries request. If the follower succeeded in replicating
/// the logs we sent it, we should update the cursor for the logs to send
/// in the next batch.
/// 
pub fn handle_append_entries_response<S>(
    receiver_node: &RaftNode<S>,
    sent_request: AppendEntriesRequestWithoutLogs,
    response: AppendEntriesResponse,
    peer_id: usize,
    append_entries_outbound_tx: UnboundedSender<(Peer, AppendEntriesRequest)>
) -> color_eyre::Result<()> 
where
    S: Storage + Default
{
    match response.success {
        true => {
            if !sent_request.is_heartbeat() {
                receiver_node.update_next_index_and_match_index_for_follower(
                    peer_id, 
                    sent_request.previous_log_index + sent_request.entries_length, 
                    sent_request.previous_log_index + sent_request.entries_length - 1
                );
            } else {
                receiver_node.update_next_index_and_match_index_for_follower(
                    peer_id, 
                    sent_request.previous_log_index,
                    sent_request.previous_log_index
                );
            }
        },
        false => {
            let mut persistent_state_guard = receiver_node.persistent_state.lock().unwrap();
            // Either we are in an older term.
            if response.term > persistent_state_guard.current_term {
                persistent_state_guard.current_term = response.term;
                drop(persistent_state_guard);
                receiver_node.save()?;
                receiver_node.become_follower();
            }
            // Or we need to decrement index and retry.
            else {
                let mut volatile_state_guard = receiver_node.volatile_state.lock().unwrap();
                // Decrement the log index for that peer.
                volatile_state_guard.next_index.insert(peer_id, sent_request.previous_log_index - 1);
                drop(volatile_state_guard);
                drop(persistent_state_guard);
                if let Ok(retry_request) = receiver_node.build_append_entries_request(sent_request.previous_log_index - 1, sent_request.is_heartbeat()) {
                    append_entries_outbound_tx.send((peer_id, retry_request))?;
                }
            }
        }
    }

    Ok(())
}

#[cfg(test)]
pub mod tests {
    // TODO: Write a macro and tests for handling an append entries response.
    use crate::*;
    use super::*;
    use std::sync::{Arc, Mutex};
    use tokio::runtime::Runtime;
    use tokio::sync::mpsc::unbounded_channel;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use crate::utils::macros::*;
    use hashbrown::HashMap;


    macro_rules! append_entries_response_test {
        (
            $(#[$meta:meta])*
            $func_name:ident,
            $initial_persistent_state:expr,
            $final_persistent_state:expr,
            $initial_volatile_state:expr,
            $final_volatile_state:expr,
            $initial_election:expr,
            $final_election:expr,
            $append_entries_request:expr,
            $append_entries_response:expr,
            $peer_id:literal,
            $should_retry:literal,
            $append_entries_request_retry:expr
        ) => {
            $(#[$meta])*
            #[test]
            pub fn $func_name() -> color_eyre::Result<()> {
                utils::set_up_logging();

                let mut receiver_raft: RaftNode<BufferBackend> = RaftNode::new();
                receiver_raft.persistent_state = Arc::new(Mutex::new($initial_persistent_state));
                receiver_raft.election = Arc::new(Mutex::new($initial_election));
                receiver_raft.volatile_state = Arc::new(Mutex::new($initial_volatile_state));

                let rt = Runtime::new()?;

                let (append_entries_outbound_tx, mut append_entries_outbound_rx) = unbounded_channel::<(Peer, AppendEntriesRequest)>();

                handle_append_entries_response(
                    &receiver_raft,
                    $append_entries_request.into(),
                    $append_entries_response,
                    $peer_id,
                    append_entries_outbound_tx
                )?;

                let counter = AtomicUsize::new(0);
                let expected_retry_request:Option<(Peer, AppendEntriesRequest)> = $append_entries_request_retry;

                rt.block_on(async move {
                    let mut observed_retry_request:Option<(Peer, AppendEntriesRequest)> = None;

                    while let Some(retry_request) = append_entries_outbound_rx.recv().await {
                        counter.fetch_add(1, Ordering::SeqCst);
                        observed_retry_request = Some(retry_request);
                    }

                    let value = counter.load(Ordering::SeqCst);

                    if $should_retry {
                        assert_eq!(value, 1);
                        assert_eq!(expected_retry_request.unwrap(), observed_retry_request.unwrap());

                    } else {
                        assert_eq!(value, 0);
                        assert!(expected_retry_request.is_none());
                        assert!(observed_retry_request.is_none());
                    }
                });

                assert_eq!(*receiver_raft.persistent_state.lock().unwrap(), $final_persistent_state);
                assert_eq!(*receiver_raft.volatile_state.lock().unwrap(), $final_volatile_state);
                assert_eq!(*receiver_raft.election.lock().unwrap(), $final_election);

                assert!(receiver_raft.are_terms_non_decreasing());

                Ok(())
            }
        };
    }


    append_entries_response_test!(
        /// If we receive a successful append entries response from the elected leader of the same term,
        /// we must acknowledge the response and update the match_index and next_index for this node.
        acknowledge_successful_heartbeat_response_from_follower,
        persistent_state(4, Some(2), vec![]),
        persistent_state(4, Some(2), vec![]),
        volatile_state(0, 0, None, None),
        volatile_state(0, 0, Some(hmap!{2: 0}), Some(hmap!{2: 0})),
        election(ElectionState::Leader, vec![2, 1]),
        election(ElectionState::Leader, vec![2, 1]),
        append_entries_request(4, 1, 0, 0, vec![], 0),
        append_entries_response(4, true),
        2,
        false,
        None
    );

    append_entries_response_test!(
        /// If we receive a successful append entries response from the elected leader of the same term,
        /// we must acknowledge the response and update the match_index and next_index for this node that already
        /// has some entries committed.
        acknowledge_successful_heartbeat_response_from_follower_with_entries,
        persistent_state(4, Some(2), vec![2, 3]),
        persistent_state(4, Some(2), vec![2, 3]),
        volatile_state(1, 0, Some(hmap!{2: 0, 1: 0}), Some(hmap!{2: 1, 1: 0})),
        volatile_state(1, 0, Some(hmap!{2: 2, 1: 0}), Some(hmap!{2: 1, 1: 0})),
        election(ElectionState::Leader, vec![2, 1]),
        election(ElectionState::Leader, vec![2, 1]),
        append_entries_request(4, 1, 1, 2, vec![3], 1),
        append_entries_response(4, true),
        2,
        false,
        None
    );
    append_entries_response_test!(
        /// We sent a Heartbeat to a follower assuming it had a log with terms [2].
        /// The follower didn't have that log and returned a failed response.
        /// In this case, we must decrement the next index for that follower and try
        /// again.
        acknowledge_heartbeat_failure_if_follower_lags_behind_and_we_decrement_to_the_start_of_our_log,
        persistent_state(4, Some(2), vec![2, 3]),
        persistent_state(4, Some(2), vec![2, 3]),
        volatile_state(1, 0, Some(hmap!{2: 2, 3: 0}), Some(hmap!{2: 1, 3: 0})),
        volatile_state(1, 0, Some(hmap!{2: 2, 3: 0}), Some(hmap!{2: 1, 3: 0})),
        election(ElectionState::Leader, vec![2, 1]),
        election(ElectionState::Leader, vec![2, 1]),
        append_entries_request(4, 1, 1, 2, vec![], 1),
        append_entries_response(4, false),
        3,
        true,
        Some((3, append_entries_request(4, 1, 0, 0, vec![], 1)))
    );
    append_entries_response_test!(
        /// We sent a Heartbeat to a follower assuming it had a log with terms [2, 3].
        /// The follower didn't have the tail entry of that log and returned a failed response.
        /// In this case, we must decrement the next index for that follower and try
        /// again.
        acknowledge_heartbeat_failure_if_follower_lags_behind,
        persistent_state(4, Some(2), vec![2, 3]),
        persistent_state(4, Some(2), vec![2, 3]),
        volatile_state(0, 0, Some(hmap!{2: 2, 3: 2}), Some(hmap!{2: 1, 3: 0})),
        volatile_state(0, 0, Some(hmap!{2: 2, 3: 1}), Some(hmap!{2: 1, 3: 0})),
        election(ElectionState::Leader, vec![2, 1]),
        election(ElectionState::Leader, vec![2, 1]),
        append_entries_request(4, 1, 2, 3, vec![], 0),
        append_entries_response(4, false),
        3,
        true,
        Some((3, append_entries_request(4, 1, 1, 2, vec![], 0)))
    );
    append_entries_response_test!(
        /// We sent a Heartbeat to a follower assuming it had a log with terms [2, 3].
        /// Turns out we were in a lower term than the follower. So we must update our current_term
        /// and should not retry. Instead become follower again.
        /// The follower didn't have the tail entry of that log and returned a failed response.
        acknowledge_heartbeat_failure_because_leader_lags_in_term,
        persistent_state(4, Some(2), vec![2, 3]),
        persistent_state(6, Some(2), vec![2, 3]),
        volatile_state(0, 0, Some(hmap!{2: 2, 3: 2}), Some(hmap!{2: 1, 3: 0})),
        volatile_state(0, 0, Some(hmap!{2: 2, 3: 2}), Some(hmap!{2: 1, 3: 0})),
        election(ElectionState::Leader, vec![2, 1]),
        election(ElectionState::Follower, vec![]),
        append_entries_request(4, 1, 2, 3, vec![], 0),
        append_entries_response(6, false),
        3,
        false,
        None
    );
}