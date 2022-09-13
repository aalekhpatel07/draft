// use std::net::TcpStream;

use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::sync::{mpsc, oneshot};
use tokio::{runtime};
use serde::{Serialize, Deserialize, de::DeserializeOwned};
use std::io::{BufWriter, BufReader, Write, Read};
use std::net::SocketAddr;
use crate::node::{NodeMetadata, Cluster, RaftNode};
use crate::config::RaftConfig;
use async_trait::async_trait;
use hashbrown::HashMap;
use color_eyre::eyre::eyre;
use std::sync::Arc;
use tokio::net::{UdpSocket};
use bytes::{Bytes, BytesMut};


impl From<RaftConfig> for UdpBackend {
    fn from(config: RaftConfig) -> Self {
        let mut cluster: Cluster = HashMap::new();
        for peer in config.peers.iter() {
            cluster.insert(peer.id, peer.clone());
        }
        let rt = runtime::Builder::new_current_thread().build().expect("Couldn't build a tokio runtime.");
        let result = rt.block_on(Self::new(&cluster, &config.server)).expect("Couldn't run UdpBackend::new");
        result
    }
}

impl From<RaftConfig> for DummyBackend {
    fn from(config: RaftConfig) -> Self {
        Self {}
        // let mut cluster: Cluster = HashMap::new();
        // for peer in config.peers.iter() {
        //     cluster.insert(peer.id, peer.clone());
        // }
        // Self::new(&cluster)
    }
}

#[async_trait]
pub trait Network: From<RaftConfig> + Default {
    async fn send_rpc<Request: Send + Serialize + DeserializeOwned>(
        &self, 
        node_id: usize, 
        rpc: Request
    ) -> color_eyre::Result<()>;
    async fn broadcast_rpc<Request: Send + Serialize + DeserializeOwned, Response: Send + Serialize + DeserializeOwned>(
        &self, 
        rpc_channel: mpsc::UnboundedReceiver<(usize, Request)>
    ) -> color_eyre::Result<()>;
    async fn run(&mut self) -> color_eyre::Result<()>;
}

#[derive(Debug)]
pub struct UdpBackend {
    cluster: Arc<Cluster>,
    server: Arc<NodeMetadata>,
    socket: Arc<Option<UdpSocket>>,
    peer_from_socket: Arc<HashMap<SocketAddr, usize>>,

    /// Represents an rpc for the given node with given data.
    rpc_request_sender: UnboundedSender<(usize, Bytes)>,
    rpc_request_receiver: UnboundedReceiver<(usize, Bytes)>,

    rpc_response_sender: UnboundedSender<(usize, Bytes)>,
    rpc_response_receiver: UnboundedReceiver<(usize, Bytes)>,
}

#[derive(Debug, Clone)]
pub struct DummyBackend {}

impl UdpBackend {

    pub async fn new(cluster: &Cluster, server: &NodeMetadata) -> color_eyre::Result<Self> {

        let socket = UdpSocket::bind(server.addr).await?;

        let (tx1, rx1) = mpsc::unbounded_channel();
        let (tx2, rx2) = mpsc::unbounded_channel();

        let mut peer_from_socket = HashMap::new();

        for (&peer_id, peer_metadata) in cluster.iter() {
            peer_from_socket.insert(peer_metadata.addr, peer_id);
        }

        Ok(Self {
            cluster: Arc::new(cluster.clone()),
            server: Arc::new(server.clone()),
            socket: Arc::new(Some(socket)),
            peer_from_socket: Arc::new(peer_from_socket),
            rpc_request_sender: tx1,
            rpc_request_receiver: rx1,
            rpc_response_sender: tx2,
            rpc_response_receiver: rx2,
        })
    }

    #[inline(always)]
    pub fn check_socket(&self) -> color_eyre::Result<()>{
        if (&*self.socket).is_none() {
            return Err(eyre!("No socket bound for self."));
        }
        Ok(())
    }

    pub async fn send_to(&self, node_id: usize, data: Bytes) -> Result<usize, std::io::Error> {
        let socket = (&*self.socket).as_ref().expect("Socket is None.");
        
        let peer_metadata = 
        self
        .cluster
        .get(&node_id)
        .expect(&format!("No node with given id: {}", node_id));

        socket.send_to(&data.to_vec(), peer_metadata.addr).await
    }

    pub async fn send_loop(&mut self) -> color_eyre::Result<()> {
        self.check_socket()?;

        for (node_id, rpc_request) in self.rpc_request_receiver.recv().await {
            match self.send_to(node_id, rpc_request).await {
                Ok(bytes_written) => {
                    tracing::trace!("Bytes written: {}", bytes_written);
                },
                Err(e) => {
                    tracing::error!(error=%e);
                }
            }
        }

        Ok(())
    }

    pub async fn run(&mut self) -> color_eyre::Result<()> {

        // TODO: Fix this.
        tokio::spawn(async move {
            for (node_id, rpc_request) in self.rpc_request_receiver.recv().await {
                match self.send_to(node_id, rpc_request).await {
                    Ok(bytes_written) => {
                        tracing::trace!("Bytes written: {}", bytes_written);
                    },
                    Err(e) => {
                        tracing::error!(error=%e);
                    }
                }
            }
        });

        // tokio::spawn(async {
        //     Self::send_loop(&mut self).await;
        // });

        // tokio::spawn(async {
        //     Self::recv_loop(&self).await
        // });

        // // tokio::spawn(async move {
        // //     self.send_loop().await;
        // // });

        // self.recv_loop().await;

        // tokio::spawn(async move {
        //         self.recv_loop() => {

        //         },
        //         _ = self.send_loop() => {

        //         }
        //     }
            // ;
            // self.send_loop();
        

        // tokio::spawn(async move {
        //     (&self).recv_loop().await.expect("Receive loop failed.");
        // });
        // tokio::spawn(async move {
        //     self.send_loop().await.expect("Send loop failed.");
        // });

        Ok(())
    }

    pub async fn recv_loop(&self) -> Result<(), std::io::Error> {

        let socket = UdpSocket::bind(self.server.addr).await?;
        let mut buffer = [0; 2048];

        loop {
            let (len, addr) = socket.recv_from(&mut buffer).await?;
            // Assume one message fills in the buffer completely.
            tracing::trace!("{:?} bytes sent from {:?}", len, addr);
            match self.peer_from_socket.get(&addr) {
                Some(peer_id) => {
                    let data = buffer[..len].to_vec();
                    if let Err(e) = 
                        self
                        .rpc_response_sender
                        .send((*peer_id, Bytes::from(data))) {
                        tracing::error!(receive_loop_error=%e);
                    }
                },
                None => {
                    // We received a packet from someone outside our cluster. Just ignore it.
                }
            }
        }

    }
}

#[async_trait]
impl Network for UdpBackend {
    async fn send_rpc<Request: Send + Serialize + DeserializeOwned>(
        &self, 
        node_id: usize, 
        rpc: Request
    ) -> color_eyre::Result<()>
    {
        if let Err(e) = self.rpc_request_sender.send((node_id, serde_json::to_vec(&rpc)?.into())) {
            tracing::error!(error_sending_rpc=%e);
        }

        Ok(())
    }

    async fn broadcast_rpc<Request: Send + Serialize + DeserializeOwned, Response: Send + Serialize + DeserializeOwned>(
        &self, 
        mut rpc_channel: mpsc::UnboundedReceiver<(usize, Request)>
    ) -> color_eyre::Result<()> {

        for (peer_id, rpc) in rpc_channel.recv().await {
            self.send_rpc(peer_id, rpc).await?;
        }

        Ok(())

    }

    async fn run(&mut self) -> color_eyre::Result<()> {
        UdpBackend::run(self).await
    }
}


#[async_trait]
impl Network for DummyBackend {
    async fn send_rpc<Request: Send>(&self, node_id: usize, rpc: Request) -> color_eyre::Result<()> {
        todo!("To implement")
    }
    async fn broadcast_rpc<Request: Send, Response: Send>(&self, rpc_channel: mpsc::UnboundedReceiver<(usize, Request)>) -> color_eyre::Result<()> {
        todo!("To implement")
    }
    async fn run(&mut self) -> color_eyre::Result<()> {
        todo!("To implement");
    }
}

impl Default for DummyBackend {
    fn default() -> Self {
        Self {}
    }
}

impl Default for UdpBackend {
    fn default() -> Self {
        let (tx1, rx1) = mpsc::unbounded_channel();
        let (tx2, rx2) = mpsc::unbounded_channel();

        Self {
            cluster: Arc::new(HashMap::new()),
            server: Arc::new(NodeMetadata::default()),
            peer_from_socket: Arc::new(HashMap::new()),
            socket: Arc::new(None),
            rpc_request_sender: tx1,
            rpc_request_receiver: rx1,
            rpc_response_sender: tx2,
            rpc_response_receiver: rx2
        }
    }
}