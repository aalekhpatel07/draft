use std::{net::SocketAddr, time::Duration, io::Write, sync::atomic::AtomicUsize};
use draft_core::{Cluster, NodeMetadata, config::RaftConfig};
use tokio::{sync::mpsc::{self, UnboundedSender, UnboundedReceiver, unbounded_channel}, join, net::ToSocketAddrs};
use tokio::time::sleep;
use hashbrown::HashMap;
use tokio::net::UdpSocket;
use async_trait::async_trait;
use rand::{Rng, rngs::ThreadRng};
use std::sync::{Mutex, Arc};

pub type Peer = usize;
pub type PeerData = (Peer, Vec<u8>);


#[async_trait]
pub trait Network<A> 
where
    A: ToSocketAddrs,
    Self: Sized
{
    async fn bind(addr: A) -> std::io::Result<Self>;
    async fn send_to(&self, buf: &[u8], target: A) -> std::io::Result<usize>;
    async fn recv_from(&self, buf: &mut [u8]) -> std::io::Result<(usize, SocketAddr)>;
}

#[async_trait]
impl Network<SocketAddr> for UdpSocket
{
    async fn bind(addr: SocketAddr) -> std::io::Result<Self> {
        UdpSocket::bind(addr).await
    }

    async fn send_to(&self, buf: &[u8], target: SocketAddr) -> std::io::Result<usize> {
        self.send_to(buf, target).await
    }
    async fn recv_from(&self, buf: &mut [u8]) -> std::io::Result<(usize, SocketAddr)> {
        self.recv_from(buf).await
    }
}


#[derive(Debug)]
pub struct MockUdpSocket {
    pub addr: SocketAddr,
    pub peer_to_recv_from: SocketAddr,
    sent_log: Arc<Mutex<HashMap<SocketAddr, Vec<Vec<u8>>>>>,
    counter: AtomicUsize
}

impl MockUdpSocket {

    pub fn get_sent_log(&self) -> HashMap<SocketAddr, Vec<Vec<u8>>> {
        self.sent_log.lock().unwrap().clone()
    }
    // pub fn new(addr: SocketAddr, expected_response: &[u8], expected_request: &[u8]) -> Self {
    //     Self {
    //         addr,
    //         expected_request: Arc::new(expected_request.into()),
    //         expected_response: Arc::new(expected_response.into()),
    //     }
    // }

}

#[async_trait]
impl Network<SocketAddr> for MockUdpSocket {
    async fn bind(addr: SocketAddr) -> std::io::Result<Self> {
        // let (to_send_responses_into, to_send_responses_from) = unbounded_channel();
        // let (send_to_response_sender, send_to_response_receiver) = unbounded_channel();
        
        Ok(Self {
            addr,
            peer_to_recv_from: "127.0.0.1:9001".parse().unwrap(),
            sent_log: Arc::new(Mutex::new(HashMap::default())),
            counter: AtomicUsize::default()
        })

        // UdpSocket::bind(addr).await
    }

    async fn send_to(&self, buf: &[u8], target: SocketAddr) -> std::io::Result<usize> {
        // Assume we were able to send all the data into the wire.

        let mut sent_log = self.sent_log.lock().expect("Couldn't lock sent transaction log.");
        let target_log = sent_log.entry(target).or_insert(vec![]);
        target_log.push(buf.clone().to_vec());
        drop(sent_log);

        // sleep(Duration::from_millis(50)).await;
        Ok(buf.len())
    }
    async fn recv_from(&self, buf: &mut [u8]) -> std::io::Result<(usize, SocketAddr)> {
        // self.recv_from(buf).await
        // Let's not make sense of the actual data received.
        // We're only testing the send/receiving functionality.
        // Not *what* was sent.
        // Send an error 
        if self.counter.fetch_add(1, std::sync::atomic::Ordering::AcqRel) >= 3 {
            // Pretend that no more datagrams received on the socket for a long time.
            // sleep(Duration::from_secs(10_000)).await;
            return Err(std::io::Error::new(std::io::ErrorKind::Other, "Finished."))
        }

        buf[0] = 1;
        buf[0] = 2;
        buf[0] = 3;

        // Introduce some latency.
        sleep(Duration::from_millis(50)).await;
        // buf.write(&some_data);

        Ok((3, self.peer_to_recv_from))

    }
}

#[derive(Debug)]
pub struct RaftServer<N> {
    pub server: NodeMetadata,

    pub peers: Arc<Cluster>,

    /// Stores a map of SocketAddr to Peer
    /// to identify the peer given a client addr.
    pub socket_addr_to_peer_map: Arc<HashMap<SocketAddr, Peer>>,

    // The underlying socket for network IO.
    pub socket: Arc<Option<N>>,

    /// The receiver gets the data written to the socket
    /// by a peer.
    pub peer_data_sender: Arc<mpsc::UnboundedSender<PeerData>>,

    /// The sender sends some data to be written to the socket
    /// of a given peer.
    pub socket_write_receiver: mpsc::UnboundedReceiver<PeerData>
}

pub fn get_socket_from_peer(peers: Arc<Cluster>, peer_id: Peer) -> Option<SocketAddr> {
    peers
    .get(&peer_id)
    .map(|v| v.addr)
}


impl<N> RaftServer<N> 
where
    N: Network<SocketAddr> + Send + Sync + 'static
{

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

    pub fn with_config(self, config: RaftConfig) -> Self {

        let mut peers: Cluster = HashMap::new();
        let mut socket_addr_to_peer_map = HashMap::default();

        for peer in config.peers.iter() {
            peers.insert(peer.id, peer.clone());
            socket_addr_to_peer_map.insert(peer.addr, peer.id);
        }

        Self {
            server: config.server,
            socket_addr_to_peer_map: Arc::new(socket_addr_to_peer_map),
            peers: Arc::new(peers),
            ..self
        }
    }

    pub fn with_server(self, server: NodeMetadata) -> Self {
        Self {
            server,
            ..self
        }
    }

    pub fn new(
        config: RaftConfig,
        peer_data_sender: mpsc::UnboundedSender<PeerData>,
        socket_write_receiver: mpsc::UnboundedReceiver<PeerData>
    ) -> Self {

        Self {
            server: NodeMetadata::default(),
            peers: Arc::new(HashMap::default()),
            socket_addr_to_peer_map: Arc::new(HashMap::default()),
            socket: Arc::new(None),
            socket_write_receiver,
            peer_data_sender: Arc::new(peer_data_sender)
        }.with_config(config)
    }

    pub async fn init(&mut self) -> std::io::Result<()> {
        if self.socket.is_none() {
            let socket = N::bind(self.server.addr).await?;
            self.socket = Arc::new(Some(socket));
        }

        Ok(())
    }


    pub async fn run(mut self) -> color_eyre::Result<()> {
        self.init().await?;

        let rpc_response_sender = self.peer_data_sender.clone();
        let socket_addr_to_peer_map = self.socket_addr_to_peer_map.clone();
        let socket = self.socket.clone();
        let peers = self.peers.clone();

        let server_addr = self.server.addr;
        let server_id = self.server.id;

        // So that anything the socket receives gets passed to the channel with the correct
        // peer identified.
        let task1 = tokio::spawn(async move {
            RaftServer::listen_for_socket_read(socket_addr_to_peer_map, socket, rpc_response_sender).await
        });
        let socket = self.socket.clone();

        let task2 = tokio::spawn(async move {
            // Save to move self in here because we don't need it anymore.
            RaftServer::listen_for_socket_write(self.socket_write_receiver, peers, socket).await
        });
        tracing::info!("Started Raft server for node (id: {}, ip: {})", server_id, server_addr);

        let (t1_result, t2_result) = join!(task1, task2);

        t1_result.unwrap().unwrap();
        t2_result.unwrap().unwrap();
        
        Ok(())
    }

    pub async fn listen_for_socket_write(
        mut socket_write_receiver: mpsc::UnboundedReceiver<PeerData>,
        peers: Arc<Cluster>,
        socket: Arc<Option<N>>,
    ) -> color_eyre::Result<()> {

        while let Some((peer_id, peer_data)) = socket_write_receiver.recv().await {
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

    pub async fn listen_for_socket_read(
        socket_addr_to_peer_map: Arc<HashMap<SocketAddr, Peer>>, 
        socket: Arc<Option<N>>,
        peer_data_sender: Arc<mpsc::UnboundedSender<PeerData>>
    ) -> color_eyre::Result<()> {

        let socket = 
            (*socket)
            .as_ref()
            .expect("Socket not initialized? Run .init() first.");

        let mut buffer = [0; 2048];

        loop {

            match socket.recv_from(&mut buffer).await {
                Ok((len, addr)) => {
                    let data = &buffer[0..len];
                    
                    match socket_addr_to_peer_map.get(&addr) {
                        Some(&peer) => {
                            peer_data_sender.send((peer, data.to_vec()))?;
                        },
                        None => {
                            tracing::warn!("Received data from an unknown client {:#?} (i.e. not part of our cluster), so ignoring.", addr);
                        }
                    }
                },
                Err(e) => {
                    tracing::error!("Error occurred when tried to recv_from the socket. {:#?}", e);
                    return Ok(());
                }
            }
            // let (len, addr) = socket.recv_from(&mut buffer).await?;
        }
    }

}


#[cfg(test)]
mod tests {
    use tokio::*;
    use super::*;
    use crate::*;
    use crate::utils::set_up_logging;
    use tracing::{info, trace};


    #[tokio::test]
    async fn server_init_works() -> color_eyre::Result<()> {
        set_up_logging();
        let (_socket_write_sender, socket_write_receiver) = mpsc::unbounded_channel();
        let (peer_data_sender, _peer_data_receiver) = mpsc::unbounded_channel();
        let config = draft_core::config::RaftConfig::default();

        let mut server: RaftServer<MockUdpSocket> = RaftServer::new(
            config,
            peer_data_sender,
            socket_write_receiver
        );
        assert!(server.init().await.is_ok());
        let socket = (*server.socket).as_ref().unwrap();
        assert_eq!(socket.get_sent_log(), HashMap::default());
        Ok(())
    }

    #[tokio::test]
    async fn server_run_works() -> color_eyre::Result<()> {

        set_up_logging();
        let (socket_write_sender, socket_write_receiver) = mpsc::unbounded_channel();
        let (peer_data_sender, mut peer_data_receiver) = mpsc::unbounded_channel();
        let config = draft_core::config::RaftConfig::default();

        let mut server: RaftServer<MockUdpSocket> = RaftServer::new(
            config,
            peer_data_sender,
            socket_write_receiver
        );
        server.init().await?;

        let socket = server.socket.clone();

        let mut hmap = HashMap::default();
        let mut requests = Vec::new();

        for _ in 0..10 {
            socket_write_sender.send((2, vec![1, 2, 3, 4, 5]))?;
            requests.push(vec![1, 2, 3, 4, 5]);
        }

        hmap.insert("127.0.0.1:9001".parse()?, requests);

        tokio::spawn(async move {
            while let Some((node_id, rpc_request_data)) = peer_data_receiver.recv().await {
                trace!("Received some data (from {}) {:#?}", node_id, rpc_request_data);
                trace!("Socket: {:#?}", socket);
                    let socket = (*socket).as_ref().unwrap();
                    assert_eq!(socket.get_sent_log(), hmap);
            }
        });

        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(3)).await;
            drop(socket_write_sender);
        });

        server.run().await?;

        Ok(())
    }
}