use tokio::sync::mpsc::UnboundedSender;
use crate::VoteResponse;
use crate::RaftNode;
use crate::ElectionState;
use crate::Storage;



pub fn handle_request_vote_response<S>(
    receiver_node: &RaftNode<S>,
    response: VoteResponse,
    peer_id: usize,
    has_become_leader_tx: UnboundedSender<()>
) -> color_eyre::Result<()> 
where
    S: Storage + Default
{

    // We got a VoteResponse.

    let persistent_state_guard = receiver_node.persistent_state.lock().unwrap();
    let current_term = persistent_state_guard.current_term;
    drop(persistent_state_guard);

    if response.term < current_term {
        // Responder is out-of-date. Maybe too much of a lag?
        // Silently ignore it.
        return Ok(())
    }

    // Update our current_term, if we're out-of-date.
    if response.term > current_term {

        let mut persistent_state_guard = receiver_node.persistent_state.lock().unwrap();
        persistent_state_guard.current_term = response.term;

        // Respectfully become a follower, since we're out-of-date.
        let mut election_guard = receiver_node.election.lock().unwrap();
        
        election_guard.state = ElectionState::Follower;
        election_guard.voter_log = hashbrown::HashSet::default();
        // election_guard.current_term = response.term;

        drop(election_guard);
        drop(persistent_state_guard);

        if let Err(e) = receiver_node.save() {
            tracing::error!("{}", e.to_string());
        }
        return Ok(())
    }

    if response.vote_granted {
        // We got the vote from our peer.

        let mut election_guard = receiver_node.election.lock().unwrap();
        election_guard.voter_log.insert(peer_id);

        let vote_count = election_guard.voter_log.len();
        drop(election_guard);

        let peer_count = receiver_node.cluster.len();
        let all_node_count = peer_count + 1; // including self.
        
        // Essentially, we want ceil(all_node_count / 2) (float div).
        // which is the same as (all_node_count + 1) / 2 (integer div).

        let majority_vote_threshold = (all_node_count + 1) / 2;

        if vote_count >= majority_vote_threshold {
            tracing::trace!("Received a majority of the votes. Becoming the leader.");
            let mut election_guard = receiver_node.election.lock().unwrap();
            election_guard.state = ElectionState::Leader;
            drop(election_guard);

            // Notify ourselves of our election win.
            // And begin sending AppendEntriesRPCs.
            if let Err(e) = has_become_leader_tx.send(()) {
                tracing::error!("Error sending a notification of our election win: {}", e.to_string());
            }
        }
    }
    // if let Err(e) = self.save() {
    //     tracing::error!("{}", e.to_string());
    // }



    Ok(())
}


#[cfg(test)]
pub mod tests {
    use crate::*;
    use super::*;
    use std::sync::{Arc, Mutex};
    use tokio::runtime::Runtime;
    use tokio::sync::mpsc::unbounded_channel;
    use std::sync::atomic::{AtomicUsize, Ordering};


    macro_rules! request_vote_response_test {
        (
            $(#[$meta:meta])*
            $func_name:ident,
            $initial_persistent_state:expr,
            $final_persistent_state:expr,
            $initial_election:expr,
            $final_election:expr,
            $vote_response:expr,
            $peer_id:literal,
            $should_become_leader:literal
        ) => {
            $(#[$meta])*
            #[test]
            pub fn $func_name() -> color_eyre::Result<()> {
                utils::set_up_logging();

                let mut receiver_raft: RaftNode<BufferBackend> = RaftNode::new();
                receiver_raft.persistent_state = Arc::new(Mutex::new($initial_persistent_state));
                receiver_raft.election = Arc::new(Mutex::new($initial_election));

                let rt = Runtime::new()?;

                let (leader_tx, mut leader_rx) = unbounded_channel::<()>();

                handle_request_vote_response(
                    &receiver_raft,
                    $vote_response,
                    $peer_id,
                    leader_tx
                )?;

                let counter = AtomicUsize::new(0);

                rt.block_on(async move {
                    while let Some(_) = leader_rx.recv().await {
                        counter.fetch_add(1, Ordering::SeqCst);
                    }

                    let value = counter.load(Ordering::SeqCst);

                    if $should_become_leader {
                        assert_eq!(value, 1);
                    } else {
                        assert_eq!(value, 0);
                    }
                });

                assert_eq!(*receiver_raft.persistent_state.lock().unwrap(), $final_persistent_state);
                assert_eq!(*receiver_raft.election.lock().unwrap(), $final_election);

                assert!(receiver_raft.are_terms_non_decreasing());

                Ok(())
            }
        };
    }


    request_vote_response_test!(
        /// If we receive a successful vote response for an older term,
        /// we must silently ignore it because the follower is
        /// slow (i.e we may have asked for a vote in a previous term a long time ago, 
        /// but this is a new term.) Test that no mutations happen.
        ignore_granted_vote_response_from_an_older_term,
        persistent_state(4, Some(2), vec![]),
        persistent_state(4, Some(2), vec![]),
        election(ElectionState::Candidate, vec![]),
        election(ElectionState::Candidate, vec![]),
        vote_response(2, true),
        2,
        false
    );


    request_vote_response_test!(
        /// If we receive a rejected vote response for an older term,
        /// we must silently ignore it because the follower is
        /// slow (i.e we may have asked for a vote in a previous term a long time ago, 
        /// but this is a new term.) Test that no mutations happen.
        ignore_rejected_vote_response_from_an_older_term,
        persistent_state(4, Some(2), vec![]),
        persistent_state(4, Some(2), vec![]),
        election(ElectionState::Candidate, vec![]),
        election(ElectionState::Candidate, vec![]),
        vote_response(2, false),
        2,
        false
    );


    request_vote_response_test!(
        /// Suppose we receive a vote response from a follower
        /// that is in a higher term than us. We must acknowledge
        /// the new term, update our current_term, and reset out election
        /// states and politely back down to a Follower state.
        acknowledge_new_term_of_a_follower_from_the_future,
        persistent_state(4, Some(2), vec![]),
        persistent_state(6, Some(2), vec![]),
        election(ElectionState::Candidate, vec![1, 3]),
        election(ElectionState::Follower, vec![]),
        vote_response(6, false),
        2,
        false
    );

    request_vote_response_test!(
        /// Suppose we receive a vote response from a follower
        /// that is in the same term as us. We haven't yet received
        /// a vote from the node. Suppose we are a cluster of three
        /// and that we have already voted for ourselves. Acknowledge
        /// the granted vote, add it to our voter log, and since a majority
        /// has been achieved, become a leader.
        acknowledge_a_vote_response_and_become_leader_because_majority_achieved,
        persistent_state(6, Some(2), vec![]),
        persistent_state(6, Some(2), vec![]),
        election(ElectionState::Candidate, vec![1]),
        election(ElectionState::Leader, vec![1, 2]),
        vote_response(6, true),
        2,
        true
    );

    request_vote_response_test!(
        /// Suppose we receive a vote response from a follower
        /// that is in the same term as us. We haven't yet received
        /// a vote from the node. Suppose we are a cluster of three
        /// but we have not already voted for ourselves. Acknowledge
        /// the granted vote, add it to our voter log. Thus we have only
        /// 1 out of 3 votes. We should not become leader yet.
        acknowledge_a_vote_response_and_do_not_become_leader_because_majority_not_achieved_yet,
        persistent_state(6, Some(2), vec![]),
        persistent_state(6, Some(2), vec![]),
        election(ElectionState::Candidate, vec![]),
        election(ElectionState::Candidate, vec![2]),
        vote_response(6, true),
        2,
        false
    );
}

