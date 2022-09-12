// use std::net::TcpStream;

use futures::{channel::{mpsc, oneshot}, io::{AsyncBufRead, AsyncBufReadExt, AsyncWrite, AsyncWriteExt}};
use serde::{Serialize, Deserialize, de::DeserializeOwned};
use std::io::{BufWriter, BufReader, Write, Read};
use crate::node::{NodeMetadata, Cluster, RaftNode};
use crate::config::RaftConfig;
use async_trait::async_trait;
use std::{net::{SocketAddr, TcpStream}, io::Error};
use hashbrown::HashMap;
use color_eyre::eyre::eyre;
use std::sync::{RwLock, Mutex};

// use std::net::{TcpStream};
// #[derive(Error, Debug)]
// pub enum NetworkError {
//     #[error(
//         ""
//     )]
//     UnderlyingConnectionError
// }

impl From<RaftConfig> for TcpBackend {
    fn from(config: RaftConfig) -> Self {
        let mut cluster: Cluster = HashMap::new();
        for peer in config.peers.iter() {
            cluster.insert(peer.id, peer.clone());
        }
        Self::new(&cluster)
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
pub trait Network: Default + From<RaftConfig> {
    async fn send_single_rpc<Request: Send + Serialize + DeserializeOwned, Response: Send + Serialize + DeserializeOwned>(&self, node_id: usize, rpc: Request) -> color_eyre::Result<oneshot::Receiver<Response>>;
    async fn broadcast_rpc<Request: Send + Serialize + DeserializeOwned, Response: Send + Serialize + DeserializeOwned>(&self, rpc_channel: mpsc::UnboundedSender<(usize, Request)>) -> color_eyre::Result<mpsc::UnboundedReceiver<Response>>;
}

pub type KindaConcurrentHashMap<K, V> = RwLock<hashbrown::HashMap<K, Mutex<V>>>;
pub type TcpStreamNodeMap = KindaConcurrentHashMap<usize, TcpStream>;

#[derive(Debug)]
/// For now, don't worry about optimizing connections.
/// We'll just shoot new TCPs for every RPC.
/// Eventually we'll try to cache the connection.
pub struct TcpBackend {
    pub cluster: Cluster,
    pub streams: TcpStreamNodeMap
}

#[derive(Debug, Clone)]
pub struct HttpBackend {

}

#[derive(Debug, Clone)]
pub struct DummyBackend {}

impl TcpBackend {
    pub fn new(cluster: &Cluster) -> Self {
        Self {
            cluster: cluster.clone(),
            streams: RwLock::new(HashMap::default())
        }
    }

    pub fn connect(&self, node_id: usize) -> color_eyre::Result<()> 
    {
        if self.streams.read().unwrap().contains_key(&node_id) {
            return Ok(());
        }
        if !self.cluster.contains_key(&node_id) {
            return Err(eyre!("No key found in cluster?"))
        }

        let node_metadata = 
        self
        .cluster
        .get(&node_id)
        .expect(&format!("No node with given id: {}", node_id));
        
        let value = TcpStream::connect(node_metadata.addr)?;
        
        self
        .streams
        .write()
        .unwrap()
        .entry(node_id)
        .or_insert(Mutex::new(value));
        Ok(())
    }

    pub fn read(&self, node_id: usize, buffer: &mut [u8]) -> Result<usize, std::io::Error> {

        let streams = self.streams.read().unwrap();

        let mut stream = 
        streams
        .get(&node_id)
        .unwrap()
        .lock()
        .unwrap();

        stream.read(buffer)
    }
    pub fn write(&self, node_id: usize, buffer: &[u8]) -> Result<usize, std::io::Error> {

        let streams = self.streams.read().unwrap();

        let mut stream = 
        streams
        .get(&node_id)
        .unwrap()
        .lock()
        .unwrap();
        stream.write(buffer)
    }
    // pub fn get(&self, node_id: usize) -> color_eyre::Result<(SocketAddr, u16)> {
    //     if let Some(value) = self.cluster.get(&node_id) {
    //         Ok((value.addr, value.port))
    //     } else {
    //         Err(eyre!("Given node_id not found in cluster."))
    //         .with_suggestion(|| "Try specifying a node_id that exists in the cluster. Note that we won't have our own id in the cluster.")
    //     }
    // }

    // pub async fn read(&self, node_id: usize, buf: &mut [u8]) -> core::result::Result<usize, std::io::Error> {
    //     self
    //     .streams
    //     .get(&node_id)
    //     .map(|conn| {
    //         conn.read_buf(buf)
    //     }).expect("No such connection.")
    // }

    // pub fn write(&self, node_id: usize)


    // pub fn send_message(&self, node_id: usize, buffer: &[u8]) -> color_eyre::Result<Vec<u8>> {
    //     let (mut addr, port) = self.get(node_id)?;
    //     addr.set_port(port);

    //     let mut connection = TcpStream::connect(addr)
    //         .map(|stream| TcpConnection::new(&stream))?;
        
    //     connection.writer.write_all(buffer)?;
    //     let mut response_buffer = Vec::new();
    //     let response = connection.reader.read_to_end(&mut response_buffer);
    //     let bytes_written = response.unwrap();
    //     Ok(response_buffer)
    // }

}

#[async_trait]
impl Network for TcpBackend {
    async fn send_single_rpc<Request: Send + Serialize + DeserializeOwned, Response: Send + Serialize + DeserializeOwned>(
        &self, 
        node_id: usize, 
        rpc: Request
    ) -> color_eyre::Result<oneshot::Receiver<Response>> 
    {
        todo!("To implement")
    }
    async fn broadcast_rpc<Request: Send + Serialize + DeserializeOwned, Response: Send + Serialize + DeserializeOwned>(&self, rpc_channel: mpsc::UnboundedSender<(usize, Request)>) -> color_eyre::Result<mpsc::UnboundedReceiver<Response>> {
        todo!("To implement")
    }
}


#[async_trait]
impl Network for DummyBackend {
    async fn send_single_rpc<Request: Send, Response: Send>(&self, node_id: usize, rpc: Request) -> color_eyre::Result<oneshot::Receiver<Response>> {
        todo!("To implement")
    }
    async fn broadcast_rpc<Request: Send, Response: Send>(&self, rpc_channel: mpsc::UnboundedSender<(usize, Request)>) -> color_eyre::Result<mpsc::UnboundedReceiver<Response>> {
        todo!("To implement")
    }
}

impl Default for DummyBackend {
    fn default() -> Self {
        Self {}
    }
}

impl Default for TcpBackend {
    fn default() -> Self {
        Self {
            cluster: hashbrown::HashMap::default(),
            streams: RwLock::new(HashMap::default())
        }
    }
}