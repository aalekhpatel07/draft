use std::net::SocketAddr;
use std::path::Path;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use derive_builder::Builder;
use hashbrown::HashMap;

use serde::{Deserialize, Serialize};

use crate::storage::Storage;
use crate::config::{RaftConfig, load_from_file};

pub type Log = (usize, Bytes);
pub type Port = u16;
pub type Cluster = HashMap<usize, NodeMetadata>;
pub type Shared<T> = Arc<Mutex<T>>;

pub trait Term {
    fn term(&self) -> usize;
}

impl Term for Log {
    fn term(&self) -> usize {
        self.0
    }
}

#[derive(Debug, Clone, PartialEq, Builder, Eq, Serialize, Deserialize)]
pub struct NodeMetadata {
    pub id: usize,
    pub addr: SocketAddr,
}

impl Default for NodeMetadata {
    fn default() -> Self {
        Self { id: 0, addr: "192.168.0.1:8000".parse().unwrap() }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Builder, Default, PartialEq, Eq)]
pub struct PersistentState {
    #[builder(default = "vec![]")]
    pub log: Vec<Log>,
    #[builder(default = "0")]
    pub current_term: usize,
    #[builder(default = "None")]
    pub voted_for: Option<usize>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Builder, Default, PartialEq, Eq)]
#[builder(default)]
pub struct VolatileState {
    pub commit_index: usize,
    pub last_applied: usize,
    pub next_index: Option<HashMap<usize, Option<usize>>>,
    pub match_index: Option<HashMap<usize, Option<usize>>>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum ElectionState {
    Follower,
    Candidate,
    Leader,
}

impl Default for ElectionState {
    fn default() -> Self {
        ElectionState::Follower
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RaftNode<S> {
    pub metadata: NodeMetadata,
    pub cluster: HashMap<usize, NodeMetadata>,
    pub persistent_state: Shared<PersistentState>,
    pub volatile_state: Shared<VolatileState>,
    pub election_state: Shared<ElectionState>,
    #[serde(skip)]
    pub storage: S,
}

impl<S> PartialEq for RaftNode<S>
where
    S: Storage
{
    /// Exclude self.storage from equality check.
    fn eq(&self, other: &Self) -> bool {
        self.metadata.eq(&other.metadata)
            && self.election_state.lock().expect("Couldn't lock own election state.").eq(&other.election_state.lock().expect("Couldn't lock other's election state."))
            && self.persistent_state.lock().expect("Couldn't lock own persistent state.").eq(&other.persistent_state.lock().expect("Couldn't lock other's persistent state."))
            && self.volatile_state.lock().expect("Couldn't lock own volatile state.").eq(&other.volatile_state.lock().expect("Couldn't lock other's volatile state."))
            && self.cluster.eq(&other.cluster)
    }
}

impl<S> Eq for RaftNode<S> where S: Storage {}

impl<S> RaftNode<S>
where
    S: Storage,
{
    pub fn save(&self) -> color_eyre::Result<usize> {
        match serde_json::to_vec(self) {
            Ok(serialized_data) => Ok(self.storage.save(&serialized_data)?),
            Err(e) => Err(e.into()),
        }
    }
    pub fn load(&mut self) -> color_eyre::Result<Self> {
        match self.storage.load() {
            Ok(serialized_data) => {
                let self_: Self = serde_json::from_slice(&serialized_data)?;
                Ok(self_)
            }
            Err(e) => Err(e),
        }
    }

    /// Determine whether all entries in our log have non-decreasing terms.
    #[cfg(test)]
    pub fn are_terms_non_decreasing(&self) -> bool {
        let guard = self.persistent_state.lock().expect("Failed to lock persistent state");
            guard
            .log
            .iter()
            .zip(guard.log.iter().skip(1))
            .all(|(predecessor_entry, successor_entry)| {
                successor_entry.term() >= predecessor_entry.term()
            })
    }

    pub fn with_config<P>(mut self, path_to_config: P) -> Self 
    where
        P: AsRef<Path>
    {
        let config = load_from_file(path_to_config).expect("Failed to load from config file.");

        self.metadata.id = config.server.id;
        self.metadata.addr = config.server.addr;

        self.cluster = HashMap::new();
        
        for node_metadata in config.peers.iter() {
            self.cluster.insert(node_metadata.id, node_metadata.clone());
        }

        self
    } 

    pub fn new() -> Self {
        Self::default().with_config("/etc/raftd/raftd.toml")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;
    use std::net::TcpListener;
    use std::{fmt::Debug, path::PathBuf};

    // use crate::UdpBackend;
    use crate::storage::{BufferBackend, FileStorageBackend};
    // use crate::network::{DummyBackend as DummyNetworkBackend};
    use std::sync::Once;

    static INIT: Once = Once::new();

    fn setup_server() {
        INIT.call_once(|| {
            start_tcp_server_on_port("127.0.0.1:9001".parse().unwrap())
        });
    }

    fn start_tcp_server_on_port(addr: SocketAddr) {
        let listener = TcpListener::bind(addr).unwrap();

        std::thread::spawn(move || {
            for stream in listener.incoming() {
                match stream {
                    Ok(mut stream) => {
                        std::thread::spawn(move || {
                            let mut s: String = String::new();
                            stream.read_to_string(&mut s).unwrap();
                            println!("{:#?}", s);
                            s.clear();
                        });
                    },
                    Err(_) => {}
                }
            }
        });

        // while match listener.accept() {
        //     Ok((mut socket, _addr)) => {
        //         std::thread::spawn(move || {
        //             let mut buffer = Vec::new();
        //             socket.read_to_end(&mut buffer).unwrap();
        //             print!("{:#?}", buffer);
        //         });
        //     },
        //     Err(_err) => {}
        // }
        // {}
    }


    #[test]
    #[cfg(not(tarpaulin))]
    fn new_works() {
        setup_server();
        let node: RaftNode<BufferBackend> = RaftNode::new();
        
        assert_eq!(node.metadata.addr, "127.0.0.1:9000".parse().unwrap());
        assert_eq!(node.metadata.id, 1);

        let mut hmap = HashMap::new();

        hmap.insert(2, NodeMetadata { id: 2, addr: "127.0.0.1:9001".parse().unwrap()});
        hmap.insert(3, NodeMetadata { id: 3, addr: "127.0.0.1:9002".parse().unwrap()});

        assert_eq!(node.cluster, hmap);
    }

    // #[test]
    // fn connect_works() {
    //     setup_server();
    //     let node: RaftNode<BufferBackend, UdpBackend> = RaftNode::new();
    //     assert!(node.cluster.contains_key(&2));
    //     node.network.connect(2).expect("Couldn't connect to node with id 2.");
    //     let data = "Hello raft!\n".as_bytes();
    //     node.network.write(2, &data).expect("Failed to write.");
    // }
    #[test]
    fn default_node_has_log_path_configured() {
        let node: RaftNode<FileStorageBackend> = RaftNode::default();
        assert_eq!(node.storage.log_file_path, PathBuf::from("/tmp/raft.d"))
    }
    #[test]
    fn it_works() {
        let state = PersistentStateBuilder::default()
            .current_term(10)
            .log(vec![(10, Bytes::from("hello"))])
            .build()
            .expect("Couldn't build persistent state with builder.");

        assert_eq!(state.current_term, 10);
        assert_eq!(state.log.len(), 1);
        assert_eq!(state.log[0].0, 10);
        assert_eq!(state.log[0].1, Bytes::from("hello"));
        assert_eq!(state.voted_for, None);
    }

    fn save_works<S>()
    where
        S: Storage + Debug,
    {
        let state = PersistentStateBuilder::default()
            .current_term(10)
            .log(vec![(10, Bytes::from("hello"))])
            .build()
            .expect("Couldn't build persistent state with builder.");

        let mut raft: RaftNode<S> = RaftNode::<S> {
            persistent_state: Arc::new(Mutex::new(state)),
            ..Default::default()
        };

        raft.save().unwrap();
        let reloaded = raft.load().unwrap();
        assert_eq!(reloaded, raft);
    }

    #[test]
    fn save_to_disk_works() {
        save_works::<FileStorageBackend>();
    }
    #[test]
    fn save_to_buffer_works() {
        save_works::<BufferBackend>();
    }
}
