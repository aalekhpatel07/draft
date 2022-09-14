// use tokio::{sync::mpsc::{self, UnboundedSender}, net::TcpListener, io::AsyncReadExt};
// use draft_core::*;
// use tracing::{instrument, error, info};
// use std::net::SocketAddr;
// use anyhow::Result;
// use draft_server::{RPCSenderChannels, setup_logging};


// async fn create_tcp_server(addr: SocketAddr, rpc_filter_channel_sender: UnboundedSender<Vec<u8>>) {
//     let server = TcpListener::bind(addr).await.expect("Failed to bind to address");

//     loop {
//         let (mut socket, _remote_addr) = server.accept().await.expect("Failed to accept connection");
//         let mut buf: Vec<u8> = vec![];

//         while let Ok(bytes_read) = socket.read_buf(&mut buf).await {
//             if bytes_read == 0 {
//                 break;
//             }
//         }
//         rpc_filter_channel_sender.send(buf.clone()).expect("Failed to send message");
//         buf.clear();
//     }
// }

// async fn classify_rpc(
//     mut rpc_filter_channel_receiver: mpsc::UnboundedReceiver<Vec<u8>>,
//     rpc_sender_channels: RPCSenderChannels
// ) {
//     loop {
//         let rpc = rpc_filter_channel_receiver.recv().await.expect("Failed to receive message");
//         if let Ok(request) = rmp_serde::from_slice::<AppendEntriesRequest>(&rpc) {
//             rpc_sender_channels.append_entries.send(request).expect("Failed to send AppendEntries");
//         } else if let Ok(request) = rmp_serde::from_slice::<VoteRequest>(&rpc) {
//             rpc_sender_channels.request_vote.send(request).expect("Failed to send VoteRequest");
//         } else {
//             error!("Unknown RPC type");
//         }
//     }
// }

// #[tokio::main]
// async fn main() -> Result<()>{
//     setup_logging();

//     let (rpc_filter_channel_tx, rpc_filter_channel_rx) = mpsc::unbounded_channel();

//     let (append_entries_tx, mut append_entries_rx) = mpsc::unbounded_channel();
//     let (vote_tx, mut vote_rx) = mpsc::unbounded_channel();

//     let rpc_sender_channels = RPCSenderChannels {
//         request_vote: vote_tx,
//         append_entries: append_entries_tx,
//     };


//     let server_addresses = vec![
//         "127.0.0.1:8080".parse::<SocketAddr>().unwrap(),
//         "127.0.0.1:8081".parse::<SocketAddr>().unwrap(),
//     ];

//     for server_address in server_addresses {
//         tokio::spawn(create_tcp_server(server_address, rpc_filter_channel_tx.clone()));
//     }

//     tokio::spawn(classify_rpc(rpc_filter_channel_rx, rpc_sender_channels));


//     let f1 = tokio::spawn(async move {
//         while let Some(request) = append_entries_rx.recv().await {
//             // tracing::info!("Received AppendEntriesRequest: {:#?}", request);
//         }
//     });
//     let f2 = tokio::spawn(async move {
//         while let Some(request) = vote_rx.recv().await {
//             // tracing::info!("Received VoteRequest: {:#?}", request);
//         }
//     });

//     tokio::select! {

//         _ = f1 => {

//         },
//         _ = f2 => {

//         },
//         _ = tokio::time::sleep(std::time::Duration::from_secs(10)) => {
//             info!("About to sleep for 10 seconds.");
//         }
//     }


//     Ok(())

// }

use draft_server::network::RaftServer;
use draft_core::*;
use tokio::sync::mpsc;
use hashbrown::HashMap;
use tracing::{info, Level};
use tracing_subscriber;


#[tokio::main]
pub async fn main() -> color_eyre::Result<()> {

    // Setup tracing.
    tracing_subscriber::fmt()
    .with_max_level(Level::TRACE)
    .init();

    // let node: RaftNode<BufferBackend> = RaftNode::default();

    let (rpc_request_sender, rpc_request_receiver) = mpsc::unbounded_channel();
    let (rpc_response_sender, mut rpc_response_receiver) = mpsc::unbounded_channel();

    let config = draft_core::config::RaftConfig::default();

    let mut peers: Cluster = HashMap::new();
    for peer in config.peers.iter() {
        peers.insert(peer.id, peer.clone());
    }

    let server = RaftServer::new(
        config.server, 
        &peers,
        rpc_response_sender,
        rpc_request_receiver,
    )
    .with_peers(&peers);


    // tokio::spawn(async move {
    //     while let Some((node_id, rpc_request_data)) = rpc_response_receiver.recv().await {
    //         info!("Received some data (from {}) {:#?}", node_id, rpc_request_data);
    //     }
    // });

    // for _ in 0..100 {
    //     rpc_request_sender.send((2, vec![1, 2, 3]))?;
    // }

    server.run().await?;

    Ok(())
}