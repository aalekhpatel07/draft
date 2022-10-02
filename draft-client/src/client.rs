// use std::net::{SocketAddr, TcpStream};

// use tokio::{sync::mpsc::{self, UnboundedReceiver, UnboundedSender}, io::AsyncReadExt, spawn};
// use thiserror::Error;
// use color_eyre::{eyre::eyre, Result};
// use bytes::{Bytes, BytesMut};
// use async_trait::async_trait;
// use draft_state_machine::{
//     RaftStateMachine,
//     StateMachine,
//     redis::{
//         Redis,
//         RedisError,
//         RedisResponse,
//         Command
//     }
// };
// use tokio::net::{TcpListener, ToSocketAddrs};


// #[async_trait]
// pub trait RaftClient {
//     type Request: Into<Bytes>;

//     async fn send(&self, message: Self::Request);
//     // async fn recv(&mut self) -> Option<>;
// }


// #[derive(Debug, Clone)]
// pub struct ClientConfig {
//     pub tcp_ports: Vec<u16>,
//     pub unix_ports: Vec<u16>,
// }

// impl Default for ClientConfig {
//     fn default() -> Self {
//         Self {
//             tcp_ports: vec![14800, 14801],
//             unix_ports: vec![]
//         }
//     }
// }


// #[derive(Debug)]
// pub struct Client {
//     pub config: ClientConfig,
//     pub command_sender: mpsc::UnboundedSender<Bytes>,
//     pub command_receiver: mpsc::UnboundedReceiver<Bytes>,
// }

// impl Client {
//     pub fn new(
//         config: ClientConfig,
//         sender: mpsc::UnboundedSender<Bytes>,
//         receiver: mpsc::UnboundedReceiver<Bytes>
//     ) -> Self 
//     {
//         Self {
//             config,
//             command_sender: sender,
//             command_receiver: receiver
//         }
//     }
    
//     pub async fn run(mut self) -> Result<()> {
//         let sender = self.command_sender;
//         let mut receiver = self.command_receiver;

//         for port in &self.config.tcp_ports {
            
//             let mut receiver = create_tcp_listener(*port).await?;
//             let sender_cp = sender.clone();

//             spawn(async move {
//                 while let Some((socket_addr, stream)) = receiver.recv().await {
//                     let _ = Self::handle_stream(sender_cp.clone(), socket_addr, stream).await;
//                 }
//             });
//         }
//         Ok(())
//     }

//     pub async fn handle_stream(
//         to_client_tx: UnboundedSender<Bytes>,
//         // from_raft_rx: Un
//         // from_raft_rx: UnboundedReceiver<Bytes>,
//         socket: SocketAddr,
//         mut stream: tokio::net::TcpStream,
//     ) -> Result<()> {
//         let (mut read_half, mut write_half) = stream.into_split();

//         // let (to_raft, mut from_raft) = mpsc::unbounded_channel();

//         let handle_read = spawn(async move {
//             let mut buf = BytesMut::with_capacity(1024);
//             while let Ok(num_bytes) = read_half.read_buf(&mut buf).await {
//                 if num_bytes == 0 { return; }

//             }
//         });

//         // println!("socket: {socket}, stream {stream:#?}");
//         to_client_tx.send(Bytes::from(format!("socket: {socket}")))?;
//         Ok(())
//     }
// }

// #[async_trait]
// impl RaftClient for Client {
//     type Request = Command;
//     async fn send(&self, message: Self::Request) {
        
//     }
// }


// pub async fn create_tcp_listener(
//     port: u16
// ) -> Result<UnboundedReceiver<(SocketAddr, tokio::net::TcpStream)>>
// {
//     // let (stream_writer_tx, mut stream_writer_rx) = mpsc::unbounded_channel();
//     let (tx, rx) = mpsc::unbounded_channel::<(SocketAddr, tokio::net::TcpStream)>();

//     let listener = TcpListener::bind(format!("0.0.0.0:{}", port).parse::<SocketAddr>()?).await?;
//     spawn(async move {
//         while let Ok((stream, socket)) = listener.accept().await {
//             tx.send((socket, stream)).unwrap();
//         }
//     });
//     Ok(rx)
// }

// #[cfg(test)]
// pub mod tests {
//     use super::*;

//     #[tokio::test]
//     pub async fn new_client() {

//         let (command_sender_tx, mut command_sender_rx) = mpsc::unbounded_channel();
//         let (command_receiver_tx, mut command_receiver_rx) = mpsc::unbounded_channel();


//         let client = Client::new(
//             ClientConfig::default(),
//             command_sender_tx,
//             command_receiver_rx
//         );

//         let t1 = spawn(async move {
//             while let Some(msg) = command_sender_rx.recv().await {
//                 println!("received: {:#?}", msg);
//             }
//         });
//         let _ = tokio::join!(t1, client.run());
//         // client.run().await;
//     }
// }