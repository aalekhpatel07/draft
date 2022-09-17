use structopt::StructOpt;
use std::{net::SocketAddr, path::PathBuf, ops::Add, fs::File, io::Write};
use draft_core::{config::RaftConfig, config::ServerConfig, utils::set_up_logging, NodeMetadata};
use std::fs::{create_dir_all};
use tracing::{Level, info, warn, debug, trace, error};
use color_eyre::eyre::eyre;


#[derive(Debug, Clone)]
pub struct Address(SocketAddr);

impl From<&str> for Address {
    fn from(s: &str) -> Self {
        if let Ok(socket_addr) = s.parse::<SocketAddr>() {
            Address(socket_addr)
        }else {
            let full_path: SocketAddr = format!("127.0.0.1:{s}").parse().unwrap();
            Address(full_path)
        }
    }
}

#[derive(StructOpt, Debug)]
#[structopt(
    name="Raft Config Splitter", 
    about="Generate raft cluster config files given a set of network addresses."
)]
pub struct Args {
    #[structopt(short="-o", long="--out-dir", parse(from_os_str))]
    out_directory: PathBuf,
    #[structopt(short="-p", long="--peers", parse(from_str))]
    peers: Vec<Address>,
    // #[structopt(parse(from_occurrences), default_value="0")]
    // verbose: u8
}

pub fn build_config(peers: &[Address]) -> Vec<(Address, RaftConfig)> {
    let mut configs = Vec::new();

    let all_metadata = peers
    .iter()
    .enumerate()
    .map(|(idx, address)| NodeMetadata { id: idx, addr: address.0 });

    for (idx, peer) in peers.iter().enumerate() {

        let own_address: SocketAddr = format!("127.0.0.1:{}", peer.0.port()).parse().unwrap();
        let server_config = ServerConfig { id: idx, addr: own_address, storage: None };

        let peer_metadata = all_metadata.clone().filter(|metadata| metadata.id != idx).collect::<Vec<NodeMetadata>>();
        configs.push((peer.clone(), RaftConfig { server: server_config, peers: peer_metadata }));
    }
    configs
}

pub fn main() -> color_eyre::Result<()> {
    let args = Args::from_args();
    // let level = match args.verbose {
    //     0 => Level::INFO,
    //     1 => Level::DEBUG,
    //     _ => Level::TRACE
    // };
    set_up_logging();

    match args.out_directory.exists() {
        true => {
            if !args.out_directory.clone().is_dir() {
                return Err(eyre!("Specified directory is not a valid directory. Please specify a valid directory to generate files into."));
            }
        },
        false => {
            debug!("Directory {:#?} does not exist. Creating a new directory at the given path.", args.out_directory.clone());
            create_dir_all(args.out_directory.clone())?;
        },
    }

    let out_dir = args.out_directory.clone();

    for (peer, config) in build_config(&args.peers) {
        let out_file_name = format!("config-{:02}.toml", config.server.id);
        let out_file_path = out_dir.join(PathBuf::from(out_file_name));

        let first_line = format!("# This config is generated for a raft node (id: {}) that runs on the server identified by {} .\n", config.server.id, peer.0);
        
        let mut file = File::create(out_file_path)?;
        file.write_all(&first_line.as_bytes())?;
        let bytes = toml::to_vec(&config)?;
        file.write_all(&bytes)?;
        debug!("Created config for node (id: {}) that runs on the server identified by {}", config.server.id, peer.0);

    }

    Ok(())
}