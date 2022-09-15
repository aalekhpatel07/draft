use draft_core::{
    RaftNode, 
    BufferBackend, 
    config::RaftConfig, 
    Storage, 
    NodeMetadata,
    VoteResponse,
    AppendEntriesResponse, RaftRPC
};
use crate::{set_up_logging, VoteRequest, AppendEntriesRequest, Peer, PeerData};
use rand::Rng;
use serde::{Serialize, de::DeserializeOwned};
use tokio::{time::{interval, sleep, timeout, Interval}, sync::mpsc::{UnboundedReceiver, UnboundedSender}};
use crate::{Network, network::RaftServer};
use tokio::net::UdpSocket;
use tokio::sync::mpsc;
use std::{sync::{Arc}, net::SocketAddr};
use std::time::Duration;



#[derive(Debug)]
pub struct RPCRxChannels {
    pub request_vote_rx: UnboundedReceiver<VoteRequest>,
    pub append_entries_rx: UnboundedReceiver<AppendEntriesRequest>,
    pub request_vote_response_rx: UnboundedReceiver<(Peer, VoteResponse)>,
    pub append_entries_response_rx: UnboundedReceiver<(Peer, AppendEntriesResponse)>
}

#[derive(Debug, Clone)]
pub struct RPCTxChannels {
    pub request_vote_tx: UnboundedSender<VoteRequest>,
    pub append_entries_tx: UnboundedSender<AppendEntriesRequest>,
    pub request_vote_response_tx: UnboundedSender<(Peer, VoteResponse)>,
    pub append_entries_response_tx: UnboundedSender<(Peer, AppendEntriesResponse)>
}

#[derive(Debug)]
pub struct RaftRuntime<S, N> {
    _core: Arc<RaftNode<S>>,
    _server: RaftServer<N>,
    pub config: RaftConfig,
    rpc_rx: RPCRxChannels,
    rpc_tx: RPCTxChannels,
    socket_read_receiver: UnboundedReceiver<PeerData>,
    socket_write_sender: UnboundedSender<PeerData>,
}

pub async fn classify_rpc_loop(
    mut socket_read_receiver: UnboundedReceiver<(usize, Vec<u8>)>,
    rpc_tx: RPCTxChannels,
) -> color_eyre::Result<()> {
    while let Some((peer_id, data)) = socket_read_receiver.recv().await {
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

    Ok(())
}


// pub async fn process_vote_response<S: Storage + Default>(
//     mut request_vote_response: UnboundedReceiver<(Peer, VoteResponse)>,
//     raft: Arc<RaftNode<S>>
// ) {

// }


pub async fn process_vote_request<S: Storage + Default>(
    mut vote_request_rx: UnboundedReceiver<VoteRequest>,
    socket_write_sender: UnboundedSender<PeerData>,
    raft: Arc<RaftNode<S>>
) -> color_eyre::Result<()> {
    
    while let Some(request) = vote_request_rx.recv().await {
    
        let peer_id = request.candidate_id;
        tracing::debug!("Received VoteRequest from peer ({:#?})", peer_id);
        let response = raft.handle_request_vote(request);

        if let Ok(bytes_written) = send_rpc(response, peer_id, socket_write_sender.clone()).await {
            tracing::debug!("Sending VoteResponse ({:#?} bytes) to peer ({:#?})", bytes_written, peer_id);
        } else {
            tracing::warn!("Couldn't send VoteResponse to peer {:#?}", peer_id);
        }
        tracing::debug!("Current state: {:#?}", raft.persistent_state.lock().unwrap());
    }

    Ok(())
}

pub async fn process_append_entries<S: Storage + Default>(
    mut append_entries_rx: UnboundedReceiver<AppendEntriesRequest>,
    socket_write_sender: UnboundedSender<PeerData>,
    raft: Arc<RaftNode<S>>
) -> color_eyre::Result<()> {

    while let Some(request) = append_entries_rx.recv().await {
        let peer_id = request.leader_id;
        tracing::debug!("Received AppendEntriesRequest from peer ({:#?})", peer_id);
        let response = raft.handle_append_entries(request);

        if let Ok(bytes_written) = send_rpc(response, peer_id, socket_write_sender.clone()).await {
            tracing::debug!("Sending AppendEntriesResponse ({:#?} bytes) to peer ({:#?})", bytes_written, peer_id);
        } else {
            tracing::warn!("Couldn't send AppendEntriesResponse to peer {:#?}", peer_id);
        }
        tracing::debug!("Current state: {:#?}", raft.persistent_state.lock().unwrap());
    }

    Ok(())
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
    S: Storage + 'static + Send + Sync + Default,
    N: Network<SocketAddr> + Send + Sync + 'static
{
    pub fn new(config: RaftConfig) -> Self {

        let raft: Arc<RaftNode<S>> = Arc::new(config.clone().into());
        let (socket_write_sender, socket_write_receiver) = mpsc::unbounded_channel();
        let (socket_read_sender, socket_read_receiver ) = mpsc::unbounded_channel();

        let (request_vote_tx, request_vote_rx) = mpsc::unbounded_channel();
        let (append_entries_tx, append_entries_rx) = mpsc::unbounded_channel();
        let (request_vote_response_tx, request_vote_response_rx) = mpsc::unbounded_channel();
        let (append_entries_response_tx, append_entries_response_rx) = mpsc::unbounded_channel();

        let server: RaftServer<N> = RaftServer::new(
            config.clone(),
            socket_read_sender,
            socket_write_receiver,
        );
        Self {
            _core: raft,
            _server: server,
            config,
            rpc_rx: RPCRxChannels { request_vote_rx, append_entries_rx, request_vote_response_rx, append_entries_response_rx },
            rpc_tx: RPCTxChannels { request_vote_tx, append_entries_tx, request_vote_response_tx, append_entries_response_tx },
            socket_read_receiver,
            socket_write_sender
        }
    }



    pub async fn run(self) -> color_eyre::Result<()> {
        let rpc_tx = self.rpc_tx.clone();
        let socket_read_receiver = self.socket_read_receiver;
        let append_entries_rx = self.rpc_rx.append_entries_rx;
        let vote_request_rx = self.rpc_rx.request_vote_rx;
        let raft = self._core.clone();

        let t1 = tokio::spawn(async move {
            classify_rpc_loop(socket_read_receiver, rpc_tx).await
        });

        let socket_write_sender = self.socket_write_sender.clone();

        let t2 = tokio::spawn(async move {
            process_append_entries(
                append_entries_rx, 
                socket_write_sender,
                raft
            ).await
        });


        let raft = self._core.clone();
        let socket_write_sender = self.socket_write_sender.clone();

        let t3 = tokio::spawn(async move {
            process_vote_request(
                vote_request_rx, 
                socket_write_sender,
                raft
            ).await
        });

        // let random_duration = rand::thread_rng().gen_range(0..1000u64);


        // tokio::select! {
            
        // }

        // match timeout(Duration::from_millis(random_duration), async {}).await {
        //     Ok(_) => {

        //     },
        //     Err(e) => {},
        // };

        let t4 = tokio::spawn(async move {
            self._server.run().await
        });

        let _ = tokio::join!(t1, t2, t3, t4);

        Ok(())
    }
}