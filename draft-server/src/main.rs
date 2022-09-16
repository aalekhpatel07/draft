use draft_core::{
    BufferBackend, 
    config::RaftConfig, 
    NodeMetadata,
};
use draft_server::{set_up_logging, RaftRuntime};
use tokio::net::UdpSocket;
use tracing::Level;

#[tokio::main]
pub async fn main() -> color_eyre::Result<()> {
    set_up_logging(Level::TRACE);

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