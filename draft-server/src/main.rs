use std::path::PathBuf;

use draft_core::{
    BufferBackend, 
    config::load_from_file, 
};
use draft_server::{RaftRuntime, set_up_logging};
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
    
    let config = load_from_file(opt.config)?;
    let raft: RaftRuntime<BufferBackend, UdpSocket> = RaftRuntime::new(config);
    raft.run().await?;
    Ok(())
}