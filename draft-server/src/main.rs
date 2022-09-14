use draft_core::{
    RaftNode, 
    BufferBackend, 
    config::RaftConfig, 
    Storage, 
    NodeMetadata,
    VoteResponse,
    AppendEntriesResponse, RaftRPC
};
use draft_server::{set_up_logging, VoteRequest, AppendEntriesRequest, Peer, PeerData};
use rand::Rng;
use serde::{Serialize, de::DeserializeOwned};
use tokio::{time::{interval, sleep, timeout}, sync::mpsc::{UnboundedReceiver, UnboundedSender}};
use draft_server::{Network, network::RaftServer};
use tokio::net::UdpSocket;
use tokio::sync::mpsc;
use std::{sync::{Arc}, net::SocketAddr};
use std::time::Duration;


#[derive(Debug)]
pub struct RPCRxChannels {
    pub request_vote_rx: UnboundedReceiver<VoteRequest>,
    pub append_entries_rx: UnboundedReceiver<AppendEntriesRequest>,
    pub request_vote_response_rx: UnboundedReceiver<VoteResponse>,
    pub append_entries_response_rx: UnboundedReceiver<AppendEntriesResponse>
}

#[derive(Debug, Clone)]
pub struct RPCTxChannels {
    pub request_vote_tx: UnboundedSender<VoteRequest>,
    pub append_entries_tx: UnboundedSender<AppendEntriesRequest>,
    pub request_vote_response_tx: UnboundedSender<VoteResponse>,
    pub append_entries_response_tx: UnboundedSender<AppendEntriesResponse>
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
            rpc_tx.request_vote_response_tx.send(rpc)?;
        }
        else if let Ok(rpc) = serde_json::from_slice::<AppendEntriesRequest>(&data) {
            rpc_tx.append_entries_tx.send(rpc)?;
        }
        else if let Ok(rpc) = serde_json::from_slice::<AppendEntriesResponse>(&data) {
            rpc_tx.append_entries_response_tx.send(rpc)?;
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




pub async fn process_vote_request<S: Storage + Default>(
    mut vote_request_rx: UnboundedReceiver<VoteRequest>,
    socket_write_sender: UnboundedSender<PeerData>,
    raft: Arc<RaftNode<S>>
) -> color_eyre::Result<()> {
    
    while let Some(request) = vote_request_rx.recv().await {
    
        let peer_id = request.candidate_id;
        tracing::debug!("Received VoteRequest from peer ({:#?})", peer_id);
        let response = raft.handle_request_vote(request);
        if let Ok(serialized) = serde_json::to_vec(&response) {
            socket_write_sender.send((peer_id, serialized))?;
            tracing::debug!("Sending VoteResponse to peer ({:#?})", peer_id);
        }else {
            tracing::warn!("Couldn't serialize VoteResponse {:#?}", response);
        }
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
        if let Ok(serialized) = serde_json::to_vec(&response) {
            socket_write_sender.send((peer_id, serialized))?;
            tracing::debug!("Sending AppendEntriesResponse to peer ({:#?})", peer_id);
        }else {
            tracing::warn!("Couldn't serialize AppendEntriesResponse {:#?}", response);
        }
    }

    Ok(())
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

    pub async fn send_rpc<Request: Serialize + DeserializeOwned + core::fmt::Debug>(
        request: Request, 
        peer_id: Peer,
        socket_write_sender: UnboundedSender<(usize, Vec<u8>)>
    ) -> color_eyre::Result<usize> 

    {

        match serde_json::to_vec(&request) {
            Ok(serialized) => {
                let total_bytes = serialized.len();
                socket_write_sender.send((peer_id, serialized))?;
                return Ok(total_bytes);
            },
            Err(e) => {
                tracing::error!("Error: {:#?}\nFailed to serialize {:#?} to json.", e, request);
                return Err(e.into())
            }
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

// pub async fn election_loop<S, N>(
//     server: Arc<RaftServer<N>>,
//     raft: Arc<RaftNode<S>>,
//     mut channels: RPCChannels
// ) {

//     tokio::select! {
//         append_entries_response = channels.append_entries_rx.recv() => {

//         },

//     }
//     // match timeout(Duration::from_secs(2), channels.append_entries_rx.recv().await).await {
//     //     Ok(val) => {},
//     //     Err(e) => {}
//     // }
// }

#[tokio::main]
pub async fn main() -> color_eyre::Result<()> {
    set_up_logging();

    let config1: RaftConfig = RaftConfig { 
        server: NodeMetadata { id: 1, addr: "127.0.0.1:9000".parse()? },
        peers: vec![
            NodeMetadata { id: 2, addr: "127.0.0.1:9001".parse()? },
            NodeMetadata { id: 3, addr: "127.0.0.1:9002".parse()? },
        ]
    };
    let raft1: RaftRuntime<BufferBackend, UdpSocket> = RaftRuntime::new(config1);


    // let config2: RaftConfig = RaftConfig { 
    //     server: NodeMetadata { id: 2, addr: "127.0.0.1:9001".parse()? },
    //     peers: vec![
    //         NodeMetadata { id: 1, addr: "127.0.0.1:9000".parse()? },
    //         NodeMetadata { id: 3, addr: "127.0.0.1:9002".parse()? },
    //     ]
    // };
    // let raft2: RaftRuntime<BufferBackend, UdpSocket> = RaftRuntime::new(config2);

    // let config3: RaftConfig = RaftConfig { 
    //     server: NodeMetadata { id: 3, addr: "127.0.0.1:9002".parse()? },
    //     peers: vec![
    //         NodeMetadata { id: 2, addr: "127.0.0.1:9001".parse()? },
    //         NodeMetadata { id: 1, addr: "127.0.0.1:9000".parse()? },
    //     ]
    // };
    // let raft3: RaftRuntime<BufferBackend, UdpSocket> = RaftRuntime::new(config3);
    // raft.run().await?;

    // tokio::spawn(async move {

    // });
    // let _results = tokio::join!(raft1.run(), raft2.run(), raft3.run());
    let _results = tokio::join!(raft1.run());
    // results.0.unwrap();
    // results.1.unwrap();
    // results.2.unwrap();

    Ok(())
}