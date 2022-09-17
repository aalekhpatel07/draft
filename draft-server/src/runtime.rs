use draft_core::{
    RaftNode, 
    config::RaftConfig, 
    Storage, 
    VoteResponse,
    AppendEntriesResponse, 
    RaftRPC
};
use tracing::{Instrument, info_span};
use crate::{VoteRequest, AppendEntriesRequest, Peer, PeerData, RaftTimer};
use rand::Rng;
use serde::{Serialize, de::DeserializeOwned};
use tokio::{time::sleep, sync::mpsc::{UnboundedReceiver, UnboundedSender}, select};
use crate::{Network, network::RaftServer};
use tokio::sync::mpsc;
use std::{sync::{Arc}, net::SocketAddr, fmt::Debug};
use std::time::Duration;



#[derive(Debug)]
pub struct RPCRx {
    pub request_vote_rx: UnboundedReceiver<VoteRequest>,
    pub append_entries_rx: UnboundedReceiver<AppendEntriesRequest>,
    pub request_vote_response_rx: UnboundedReceiver<(Peer, VoteResponse)>,
    pub append_entries_response_rx: UnboundedReceiver<(Peer, AppendEntriesResponse)>,
    pub request_vote_outbound_rx: UnboundedReceiver<(Peer, VoteRequest)>,
    pub append_entries_outbound_rx: UnboundedReceiver<(Peer, AppendEntriesRequest)>,
}

#[derive(Debug, Clone)]
pub struct RPCTx {
    pub request_vote_tx: UnboundedSender<VoteRequest>,
    pub append_entries_tx: UnboundedSender<AppendEntriesRequest>,
    pub request_vote_response_tx: UnboundedSender<(Peer, VoteResponse)>,
    pub append_entries_response_tx: UnboundedSender<(Peer, AppendEntriesResponse)>,
    pub request_vote_outbound_tx: UnboundedSender<(Peer, VoteRequest)>,
    pub append_entries_outbound_tx: UnboundedSender<(Peer, AppendEntriesRequest)>,
}

#[derive(Debug)]
pub struct ElectionRx {
    pub has_become_leader_rx: UnboundedReceiver<()>,
    pub has_become_candidate_rx: UnboundedReceiver<()>,
    pub election_timer_rx: UnboundedReceiver<()>,
    // pub reset_election_timer_rx: UnboundedReceiver<()>
}

#[derive(Debug, Clone)]
pub struct ElectionTx {
    pub has_become_leader_tx: UnboundedSender<()>,
    pub has_become_candidate_tx: UnboundedSender<()>,
    pub election_timer_tx: UnboundedSender<()>,
    pub reset_election_timer_tx: UnboundedSender<()>,
}


#[derive(Debug)]
pub struct RaftRuntime<S, N> {
    _core: Arc<RaftNode<S>>,
    _server: RaftServer<N>,
    _timer: RaftTimer,
    pub config: RaftConfig,
    rpc_rx: RPCRx,
    rpc_tx: RPCTx,
    election_rx: ElectionRx,
    election_tx: ElectionTx,
    socket_read_receiver: UnboundedReceiver<PeerData>,
    socket_write_sender: UnboundedSender<PeerData>,
}

pub async fn process_rpc<S: Storage + Default + core::fmt::Debug>(
    mut election_rx: ElectionRx,
    election_tx: ElectionTx,

    mut socket_read_receiver: UnboundedReceiver<PeerData>,
    socket_write_sender: UnboundedSender<PeerData>,

    raft: Arc<RaftNode<S>>,
    rpc_tx: RPCTx,
    mut rpc_rx: RPCRx
) -> color_eyre::Result<()> {

    loop {
        select! {

            // We received an AppendEntriesRequest from a leader.
            Some(request) = rpc_rx.append_entries_rx.recv() => {

                // Regardless of our status, make ourselves a follower
                // because we just received an RPC from the new leader
                // even if we end up rejecting this RPC.

                raft.reset_election();

                // Reset our timer.
                election_tx.reset_election_timer_tx.send(())?;

                let peer_id = request.leader_id;
                let rpc_kind_str: &str = {
                    if request.is_heartbeat() {
                        "Heartbeat"
                    } else {
                        "AppendEntries"
                    }
                };
                
                tracing::debug!("Received {}Request from leader ({:#?})", rpc_kind_str, peer_id);

                let response = raft.handle_append_entries(request).instrument(info_span!("append-entries")).into_inner();

                if let Ok(bytes_written) = send_rpc(response, peer_id, socket_write_sender.clone()).await {
                    tracing::trace!("Sending {}Response ({:#?} bytes) to peer ({:#?})", rpc_kind_str, bytes_written, peer_id);
                } else {
                    tracing::warn!("Couldn't send {}Response to peer {:#?}", rpc_kind_str, peer_id);
                }
            },

            // We received a VoteRequest from a candidate.
            Some(request) = rpc_rx.request_vote_rx.recv() => {

                let peer_id = request.candidate_id;
                tracing::debug!("Received VoteRequest from peer ({:#?})", peer_id);
                let response = raft.handle_request_vote(request).instrument(info_span!("vote-request")).into_inner();

                if response.vote_granted {
                    // Reset our election timer, by notifying the resetter.
                    election_tx.reset_election_timer_tx.send(())?;
                }

                if let Ok(bytes_written) = send_rpc(response, peer_id, socket_write_sender.clone()).await {
                    tracing::trace!("Sending VoteResponse ({:#?} bytes) to peer ({:#?})", bytes_written, peer_id);
                } else {
                    tracing::warn!("Couldn't send VoteResponse to peer {:#?}", peer_id);
                }
                tracing::trace!("Current raft:\n {:?}", raft);
            },

            // We received a VoteResponse from a peer.
            Some((peer_id, response)) = rpc_rx.request_vote_response_rx.recv() => {
                tracing::trace!("Received VoteRequestResponse from peer ({:#?})", peer_id);
                raft.handle_request_vote_response(peer_id, response, election_tx.has_become_leader_tx.clone());
            },

            // We received an AppendEntriesResponse from a follower.
            Some((_peer_id, _response)) = rpc_rx.append_entries_response_rx.recv() => {
                /*
                    TODO: Implement the retry-loop by decrementing indices
                          of the last log term, etc.
                */
                // todo!("Implement the retry-loop for when we receive a failed append entries response, or handle state change when it is successful");

            },
            // Election timer expired. Neither did we receive any AppendEntriesRPC nor
            // did we grant any votes. Trigger the election by stepping up as a candidate.
            Some(_) = election_rx.election_timer_rx.recv() => {
                tracing::debug!("Election timed out. Becoming candidate.");
                election_tx.has_become_candidate_tx.send(())?;
            },
            // Rules for Servers (Candidates) (Section 5.2-4)
            // We just became a candidate.
            Some(_) = election_rx.has_become_candidate_rx.recv() => {

                // Rules for Servers (Candidates) (Section 5.2)

                tracing::debug!("Yay! We are now a candidate.");
                raft.handle_becoming_candidate();
                
                // Reset the election timer.
                election_tx.reset_election_timer_tx.send(())?;

                // Send out VoteRequest to all nodes.
                let request = raft.build_vote_request();
                for node in raft.nodes() {
                    if let Err(send_err) = rpc_tx.request_vote_outbound_tx.send((node, request.clone())) {
                        tracing::error!("Failed to send to {} on the outbound request_vote channel.\n\nError: {}", node, send_err);
                    }
                }
            },

            // Rules for Servers (Leaders) (Section 5.2-4)
            // We just became a leader.
            Some(_) = election_rx.has_become_leader_rx.recv() => {

                tracing::debug!("Yay! We are now a leader.");
                raft.handle_becoming_leader();

                let request = raft.build_heartbeat();

                // Send out initial heartbeats to all nodes.
                for node in raft.nodes() {
                    if let Err(send_err) = rpc_tx.append_entries_outbound_tx.send((node, request.clone())) {
                        tracing::error!("Failed to send to {} on the outbound append_entries channel.\n\nError: {}", node, send_err);
                    }
                }
            },

            // The Output side of Network IO.

            // We wish to send a vote request to a peer.
            Some((peer_id, request)) = rpc_rx.request_vote_outbound_rx.recv() => {
                if let Ok(bytes_written) = send_rpc(request, peer_id, socket_write_sender.clone()).await {
                    tracing::debug!("Sending RequestVoteRPC ({:#?} bytes) to peer ({:#?})", bytes_written, peer_id);
                } else {
                    tracing::warn!("Couldn't send RequestVoteRPC to peer {:#?}", peer_id);
                }
            },
            // We wish to send an append entries request to a peer.
            Some((peer_id, request)) = rpc_rx.append_entries_outbound_rx.recv() => {

                if let Ok(bytes_written) = send_rpc(request, peer_id, socket_write_sender.clone()).await {
                    tracing::debug!("Sending AppendEntriesRPCRequest ({:#?} bytes) to follower ({:#?})", bytes_written, peer_id);
                } else {
                    tracing::warn!("Couldn't send AppendEntriesRPCRequest to peer {:#?}", peer_id);
                }
            },

            // The Input side of Network IO.

            // Just read the socket and classify each kind of rpc request/response
            // into its corresponding channels.
            Some((peer_id, data)) = socket_read_receiver.recv() => {

                let total_bytes = data.len();
                tracing::trace!("Received some data from peer ({:#?})", peer_id);
                // TODO: Is there a way to match on the deserialized object?
                if let Ok(rpc) = serde_json::from_slice::<VoteRequest>(&data) {
                    rpc_tx.request_vote_tx.send(rpc)?;
                }
                else if let Ok(rpc) = serde_json::from_slice::<VoteResponse>(&data) {
                    rpc_tx.request_vote_response_tx.send((peer_id, rpc))?;
                }
                else if let Ok(rpc) = serde_json::from_slice::<AppendEntriesRequest>(&data) {
                    rpc_tx.append_entries_tx.send(rpc)?;
                }
                else if let Ok(rpc) = serde_json::from_slice::<AppendEntriesResponse>(&data) {
                    rpc_tx.append_entries_response_tx.send((peer_id, rpc))?;
                }
                else {
                    tracing::warn!(
                        "Ignoring data (size: {:#?} bytes) received from peer ({}) because it could not be classified as a valid rpc.",
                        total_bytes,
                        peer_id
                    );
                }
            }
        }
    }
}

pub async fn send_rpc<Data: Serialize + DeserializeOwned + core::fmt::Debug>(
    data: Data, 
    peer_id: Peer,
    socket_write_sender: UnboundedSender<PeerData>
) -> color_eyre::Result<usize> 
{
    match serde_json::to_vec(&data) {
        Ok(serialized) => {
            let total_bytes = serialized.len();
            socket_write_sender.send((peer_id, serialized))?;
            return Ok(total_bytes);
        },
        Err(e) => {
            tracing::error!("Error: {:#?}\nFailed to serialize {:#?} to json.", e, data);
            return Err(e.into())
        }
    }
}

impl<S, N> RaftRuntime<S, N> 
where
    S: Storage + 'static + Send + Sync + Default + core::fmt::Debug,
    N: Network<SocketAddr> + Send + Sync + 'static
{
    pub fn new(config: RaftConfig) -> Self {

        let raft: Arc<RaftNode<S>> = Arc::new(config.clone().into());
        let (socket_write_sender, socket_write_receiver) = mpsc::unbounded_channel();
        let (socket_read_sender, socket_read_receiver ) = mpsc::unbounded_channel();

        let (request_vote_tx, request_vote_rx) = mpsc::unbounded_channel();
        let (request_vote_outbound_tx, request_vote_outbound_rx) = mpsc::unbounded_channel();
        let (append_entries_tx, append_entries_rx) = mpsc::unbounded_channel();
        let (append_entries_outbound_tx, append_entries_outbound_rx) = mpsc::unbounded_channel();
        let (request_vote_response_tx, request_vote_response_rx) = mpsc::unbounded_channel();
        let (append_entries_response_tx, append_entries_response_rx) = mpsc::unbounded_channel();

        let (has_become_candidate_tx, has_become_candidate_rx) = mpsc::unbounded_channel();
        let (has_become_leader_tx, has_become_leader_rx) = mpsc::unbounded_channel();
        let (election_timer_tx, election_timer_rx) = mpsc::unbounded_channel();
        let (reset_election_timer_tx, reset_election_timer_rx) = mpsc::unbounded_channel();

        let server: RaftServer<N> = RaftServer::new(
            config.clone(),
            socket_read_sender,
            socket_write_receiver,
        );

        let timer = RaftTimer::new(
            reset_election_timer_rx,
            election_timer_tx.clone(),
            150..300
        );

        Self {
            _core: raft,
            _server: server,
            _timer: timer,
            config,
            election_rx: ElectionRx { 
                has_become_leader_rx, 
                has_become_candidate_rx, 
                election_timer_rx, 
            },
            election_tx: ElectionTx {
                has_become_candidate_tx,
                has_become_leader_tx,
                election_timer_tx,
                reset_election_timer_tx
            },
            rpc_rx: RPCRx { request_vote_rx, append_entries_rx, request_vote_response_rx, append_entries_response_rx, request_vote_outbound_rx, append_entries_outbound_rx },
            rpc_tx: RPCTx { request_vote_tx, append_entries_tx, request_vote_response_tx, append_entries_response_tx, request_vote_outbound_tx, append_entries_outbound_tx },
            socket_read_receiver,
            socket_write_sender
        }
    }


    pub async fn run(self) -> color_eyre::Result<()> {

        let raft = self._core.clone();
        let timer = self._timer;

        let rpc_rx = self.rpc_rx;
        let rpc_tx = self.rpc_tx.clone();

        let socket_read_receiver = self.socket_read_receiver;
        let socket_write_sender = self.socket_write_sender.clone();

        let election_rx = self.election_rx;
        let election_tx = self.election_tx.clone();

        let t1 = tokio::spawn(async move {
            process_rpc(
                election_rx,
                election_tx,
                socket_read_receiver,
                socket_write_sender,
                raft,
                rpc_tx,
                rpc_rx
            ).await
        });

        let server = self._server;

        let mut rng = rand::thread_rng();
        let random_delay = rng.gen_range(3000..5000);

        let election_tx = self.election_tx.clone();

        tokio::spawn(async move {
            sleep(Duration::from_millis(random_delay)).await;
            election_tx.election_timer_tx.send(()).unwrap();
        });

        select! {
            _ = t1 => {
                println!("The process_rpc loop exited. But how?");
            },
            _ = server.run() => {
                println!("Server run completed.");
            },
            _ = timer.run() => {
                println!("Timer run completed.");
            }
        }

        Ok(())
    }
}