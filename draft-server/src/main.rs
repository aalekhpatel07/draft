use std::path::PathBuf;

use draft_core::{
    BufferBackend, 
    config::{RaftConfig, ServerConfig, load_from_file}, 
    NodeMetadata,
};
use draft_server::{set_up_logging, RaftRuntime};
use tokio::net::UdpSocket;
use tracing::Level;
use structopt::StructOpt;


#[derive(StructOpt)]
#[structopt(name="Raft Server.", about = "The runtime for Raft. (without the state-machine backend for now).")]
pub struct Opt {
    #[structopt(short, long, parse(from_os_str))]
    config: PathBuf,

    #[structopt(short, long, parse(from_occurrences), default_value = "0")]
    verbose: u8,
}


#[tokio::main]
pub async fn main() -> color_eyre::Result<()> {

    let opt = Opt::from_args();

    let log_level = {
        match opt.verbose {
            0 => Level::INFO,
            1 => Level::DEBUG,
            _ => Level::TRACE
        }
    };

    set_up_logging(log_level);
    
    let config_path = opt.config;

    let config = load_from_file(config_path)?;

    let raft: RaftRuntime<BufferBackend, UdpSocket> = RaftRuntime::new(config);

    raft.run().await?;

    // let config1: RaftConfig = RaftConfig { 
    //     server: ServerConfig { id: 1, addr: "127.0.0.1:9000".parse()?, storage: None },
    //     peers: vec![
    //         NodeMetadata { id: 2, addr: "127.0.0.1:9001".parse()? },
    //         NodeMetadata { id: 3, addr: "127.0.0.1:9002".parse()? },
    //     ]
    // };
    // let raft1: RaftRuntime<BufferBackend, UdpSocket> = RaftRuntime::new(config1);


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
    // let _results = tokio::join!(raft.run());
    // results.0.unwrap();
    // results.1.unwrap();
    // results.2.unwrap();

    Ok(())
}