use structopt::StructOpt;
use std::{net::{SocketAddr, ToSocketAddrs}, path::PathBuf, fs::File, io::Write, convert::TryFrom};
use draft_core::{config::RaftConfig, config::ServerConfig, utils::set_up_logging, NodeMetadata};
use std::fs::create_dir_all;
use tracing::debug;
use color_eyre::eyre::eyre;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Address(SocketAddr);

impl From<&str> for Address
{
    fn from(s: &str) -> Self {
        if let Ok(mut maybe_socket) = ToSocketAddrs::to_socket_addrs(s) {
            if let Some(socket_addr) = maybe_socket.next() {
                return Address(socket_addr)
            }
        }
        let full_path: SocketAddr = format!("127.0.0.1:{s}").parse().unwrap();
        Address(full_path)
    }
}

#[derive(Debug, Clone)]
pub struct ExistingDirectory(PathBuf);

fn parse_and_check_directory_exists(s: &str) -> color_eyre::Result<ExistingDirectory> {

    let dir = PathBuf::from(s);
    match dir.exists() {
            true => {
                if !dir.clone().is_dir() {
                    return Err(eyre!("Specified directory is not a valid directory. Please specify a valid directory to generate files into."));
                }
            },
            false => {
                debug!("Directory {:#?} does not exist. Creating a new directory at the given path.", dir.clone());
                create_dir_all(dir.clone())?;
            },
        }
    
    return Ok(ExistingDirectory(dir))
}

#[derive(StructOpt, Debug)]
#[structopt(
    name="Raft Config Splitter", 
    about="Generate raft cluster config files given a set of network addresses."
)]
pub struct Args {
    #[structopt(short="-o", long="--out-dir", parse(try_from_str = parse_and_check_directory_exists))]
    out_directory: ExistingDirectory,
    #[structopt(short="-p", long="--peers", parse(from_str))]
    peers: Vec<Address>,
}

pub fn build_config(peers: &[Address]) -> Vec<(Address, RaftConfig)> {
    let mut configs = Vec::new();

    let all_metadata = peers
    .iter()
    .enumerate()
    .map(|(idx, address)| NodeMetadata { id: idx + 1, addr: address.0 });

    for (idx, peer) in peers.iter().enumerate() {

        let own_address: SocketAddr = format!("127.0.0.1:{}", peer.0.port()).parse().unwrap();
        let server_config = ServerConfig { id: idx + 1, addr: own_address, storage: None };

        let peer_metadata = all_metadata.clone().filter(|metadata| metadata.id != (idx + 1)).collect::<Vec<NodeMetadata>>();
        configs.push((peer.clone(), RaftConfig { server: server_config, peers: peer_metadata }));
    }
    configs
}

#[cfg(not(tarpaulin_include))]
pub fn main() -> color_eyre::Result<()> {
    let args = Args::from_args();
    set_up_logging();
    let out_dir = args.out_directory.0.clone();

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


#[cfg(test)]
pub mod tests {
    use super::Address;
    use super::*;

    #[test]
    fn from_str_for_address() {
        assert_eq!(Address::from("5000"), Address("127.0.0.1:5000".parse().unwrap()));
        assert_eq!(Address::from("192.168.0.1:3000"), Address("192.168.0.1:3000".parse().unwrap()));
        assert_eq!(Address::from("5000"), Address("127.0.0.1:5000".parse().unwrap()));

        // Just check that google.com get resolved.
        assert_eq!(Address::from("google.com:80").0.port(), 80);
    }

    #[test]
    fn args_struct() {
        let args_str = vec![
            "raft-split-config",
            "--out-dir",
            "/tmp/raft-split-config-test",
            "-p",
            "5000", 
            "6000",
            "192.168.1.1:1337"
        ];
        
        let opts = Args::from_iter_safe(args_str.iter());
        assert!(opts.is_ok());
        let opts = opts.unwrap();

        assert_eq!(opts.out_directory.0, PathBuf::from("/tmp/raft-split-config-test"));
        assert_eq!(opts.peers, vec![
            Address::from("127.0.0.1:5000"),
            Address::from("127.0.0.1:6000"),
            Address::from("192.168.1.1:1337")
        ])
    }

    #[test]
    fn args_struct_errs_when_out_dir_is_not_a_dir() {
        let args_str = vec![
            "raft-split-config",
            "--out-dir",
            "/etc/os-release",
            "-p",
            "5000", 
            "6000",
            "192.168.1.1:1337"
        ];
        
        let opts = Args::from_iter_safe(args_str.iter());
        assert!(opts.is_err());
    }
    #[test]
    fn build_config_three_node_cluster() {

        let args_str = vec![
            "raft-split-config",
            "--out-dir",
            "/tmp/raft-split-config-test",
            "-p",
            "5000", 
            "6000",
            "192.168.1.1:1337",
        ];
        
        let opts = Args::from_iter_safe(args_str.iter());
        assert!(opts.is_ok());
        let opts = opts.unwrap();

        let configs = build_config(&opts.peers);

        assert_eq!(configs.len(), 3);

        assert_eq!(configs.get(0).unwrap().0, Address::from("127.0.0.1:5000"));
        assert_eq!(
            configs.get(0).unwrap().1, 
            RaftConfig { 
                peers: vec![
                    NodeMetadata {
                        id: 2,
                        addr: "127.0.0.1:6000".parse().unwrap()
                    },
                    NodeMetadata {
                        id: 3,
                        addr: "192.168.1.1:1337".parse().unwrap()
                    },
                ],
                server: ServerConfig { id: 1, addr: "127.0.0.1:5000".parse().unwrap(), storage: None }
            }
        );

        assert_eq!(configs.get(1).unwrap().0, Address::from("127.0.0.1:6000"));
        assert_eq!(
            configs.get(1).unwrap().1, 
            RaftConfig { 
                peers: vec![
                    NodeMetadata {
                        id: 1,
                        addr: "127.0.0.1:5000".parse().unwrap()
                    },
                    NodeMetadata {
                        id: 3,
                        addr: "192.168.1.1:1337".parse().unwrap()
                    },
                ],
                server: ServerConfig { id: 2, addr: "127.0.0.1:6000".parse().unwrap(), storage: None }
            }
        );

        assert_eq!(configs.get(2).unwrap().0, Address::from("192.168.1.1:1337"));
        assert_eq!(
            configs.get(2).unwrap().1, 
            RaftConfig { 
                peers: vec![
                    NodeMetadata {
                        id: 1,
                        addr: "127.0.0.1:5000".parse().unwrap()
                    },
                    NodeMetadata {
                        id: 2,
                        addr: "127.0.0.1:6000".parse().unwrap()
                    },
                ],
                server: ServerConfig { id: 3, addr: "127.0.0.1:1337".parse().unwrap(), storage: None }
            }
        );

    }
}