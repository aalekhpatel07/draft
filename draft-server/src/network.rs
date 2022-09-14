use std::{net::SocketAddr};
use draft_core::{Cluster, NodeMetadata, config::RaftConfig};
use std::sync::Arc;
use tokio::{sync::mpsc, join};
use hashbrown::HashMap;
use tokio::net::UdpSocket;

pub type Peer = usize;
pub type PeerData = (Peer, Vec<u8>);



#[derive(Debug)]
pub struct RaftServer {
    pub server: NodeMetadata,

    pub peers: Arc<Cluster>,

    /// Stores a map of SocketAddr to Peer
    /// to identify the peer given a client addr.
    pub socket_addr_to_peer_map: Arc<HashMap<SocketAddr, Peer>>,

    // The underlying socket for network IO.
    pub socket: Arc<Option<UdpSocket>>,
    // RaftCore keeps the receiver end for the response.
    pub rpc_response_sender: Arc<mpsc::UnboundedSender<PeerData>>,

    // RaftCore keeps the sender end for the request.
    pub rpc_request_receiver: mpsc::UnboundedReceiver<PeerData>
}

pub fn get_socket_from_peer(peers: Arc<Cluster>, peer_id: Peer) -> Option<SocketAddr> {
    peers
    .get(&peer_id)
    .map(|v| v.addr)
}


impl RaftServer {

    pub fn with_peers(self, peers: &Cluster) -> Self {
        let mut socket_addr_to_peer_map = HashMap::default();
        for (&peer_id, peer_metadata) in peers {
            socket_addr_to_peer_map.insert(peer_metadata.addr, peer_id);
        }
        Self {
            peers: Arc::new(peers.clone()),
            socket_addr_to_peer_map: Arc::new(socket_addr_to_peer_map),
            ..self
        }
    }

    pub fn with_config(self, server: NodeMetadata) -> Self {
        Self {
            server,
            ..self
        }
    }

    pub fn new(
        server: NodeMetadata,
        peers: &Cluster,
        rpc_response_sender: mpsc::UnboundedSender<PeerData>,
        rpc_request_receiver: mpsc::UnboundedReceiver<PeerData>
    ) -> Self {

        let mut socket_addr_to_peer_map = HashMap::default();
        for (&peer_id, peer_metadata) in peers {
            socket_addr_to_peer_map.insert(peer_metadata.addr, peer_id);
        }

        Self {
            server,
            peers: Arc::new(peers.clone()),
            socket_addr_to_peer_map: Arc::new(socket_addr_to_peer_map),
            socket: Arc::new(None),
            rpc_request_receiver: rpc_request_receiver,
            rpc_response_sender: Arc::new(rpc_response_sender)
        }
    }

    pub async fn init(&mut self) -> std::io::Result<()> {
        if self.socket.is_none() {
            let socket = UdpSocket::bind(self.server.addr).await?;
            self.socket = Arc::new(Some(socket));
        }

        Ok(())
    }


    pub async fn run(mut self) -> color_eyre::Result<()> {
        self.init().await?;

        let rpc_response_sender = self.rpc_response_sender.clone();
        let socket_addr_to_peer_map = self.socket_addr_to_peer_map.clone();
        let socket = self.socket.clone();
        let peers = self.peers.clone();

        let server_addr = self.server.addr;
        let server_id = self.server.id;

        // So that anything the socket receives gets passed to the channel with the correct
        // peer identified.
        let task1 = tokio::spawn(async move {
            RaftServer::listen_for_rpc_response(socket_addr_to_peer_map, socket, rpc_response_sender).await
        });
        let socket = self.socket.clone();

        let task2 = tokio::spawn(async move {
            // Save to move self in here because we don't need it anymore.
            RaftServer::listen_for_rpc_requests(self.rpc_request_receiver, peers, socket).await
        });
        tracing::info!("Started Raft server for node (id: {}, ip: {})", server_id, server_addr);

        let (t1_result, t2_result) = join!(task1, task2);

        t1_result.unwrap().unwrap();
        t2_result.unwrap().unwrap();
        
        Ok(())
    }

    pub async fn listen_for_rpc_requests(
        mut rpc_request_receiver: mpsc::UnboundedReceiver<PeerData>,
        peers: Arc<Cluster>,
        socket: Arc<Option<UdpSocket>>,
    ) -> color_eyre::Result<()> {

        while let Some((peer_id, peer_data)) = rpc_request_receiver.recv().await {
            tracing::trace!("Handling rpc request for peer ({}) => into socket.", peer_id);
            match get_socket_from_peer(peers.clone(), peer_id) {

                Some(remote_socket_addr) => {
                    let maybe_bytes_sent = 
                    (*socket)
                    .as_ref()
                    .expect("Socket not initialized? Run .init() first.")
                    .send_to(&peer_data, remote_socket_addr)
                    .await;

                    if let Ok(bytes_sent) = maybe_bytes_sent {
                        tracing::trace!("Sent ({}/{}) bytes to peer {}.", bytes_sent, peer_data.len(), peer_id);
                    }else {
                        tracing::error!("Failed to send {} bytes to peer {}.", peer_data.len(), peer_id);
                    }

                },
                None => {
                    tracing::warn!("Asked to send an rpc to an unknown node (id: {:#?}) (i.e. not a part of our cluster), so ignoring.", peer_id);
                }
            }
        }
        Ok(())
    }

    pub async fn listen_for_rpc_response(
        socket_addr_to_peer_map: Arc<HashMap<SocketAddr, Peer>>, 
        socket: Arc<Option<UdpSocket>>,
        rpc_response_sender: Arc<mpsc::UnboundedSender<PeerData>>
    ) -> color_eyre::Result<()> {

        let socket = 
            (*socket)
            .as_ref()
            .expect("Socket not initialized? Run .init() first.");

        let mut buffer = [0; 2048];

        loop {
            let (len, addr) = socket.recv_from(&mut buffer).await?;
            let data = &buffer[0..len];
            match socket_addr_to_peer_map.get(&addr) {
                Some(&peer) => {
                    rpc_response_sender.send((peer, data.to_vec()))?;
                },
                None => {
                    tracing::warn!("Received data from an unknown client {:#?} (i.e. not part of our cluster), so ignoring.", addr);
                }
            }
        }
    }

}