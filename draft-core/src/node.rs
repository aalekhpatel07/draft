use std::fmt::{Debug, write};
use std::net::SocketAddr;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use derive_builder::Builder;
use hashbrown::HashMap;
use itertools::Itertools;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

use serde::{Deserialize, Serialize};

use crate::{VoteResponse, VoteRequest, AppendEntriesRequest};
use crate::config::{load_from_file, RaftConfig};
use crate::storage::Storage;

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

#[derive(Debug, Copy, Clone, PartialEq, Builder, Eq, Serialize, Deserialize)]
pub struct NodeMetadata {
    pub id: usize,
    pub addr: SocketAddr,
}

impl Default for NodeMetadata {
    fn default() -> Self {
        Self {
            id: 1,
            addr: "127.0.0.1:9000".parse().unwrap(),
        }
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
    pub next_index: HashMap<usize, usize>,
    pub match_index: HashMap<usize, usize>,
}

#[derive(Debug, Default, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub struct Election {
    pub state: ElectionState,
    pub voter_log: hashbrown::HashSet<usize>,
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

#[derive(Clone, Serialize, Deserialize, Default)]
pub struct RaftNode<S> {
    pub metadata: NodeMetadata,
    pub cluster: HashMap<usize, NodeMetadata>,
    #[serde(with = "arc_mutex_serde")]
    pub persistent_state: Shared<PersistentState>,
    #[serde(with = "arc_mutex_serde")]
    pub volatile_state: Shared<VolatileState>,
    #[serde(with = "arc_mutex_serde")]
    pub election: Shared<Election>,
    #[serde(skip)]
    pub storage: S,
}

impl<S> core::fmt::Debug for RaftNode<S>
where
    S: Debug 
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RaftNode")
            .field("persistent_state", &self.persistent_state)
            .field("volatile_state", &self.volatile_state)
            .field("election", &self.election)
            .field("cluster", &self.cluster)
            .field("metadata", &self.metadata)
            .finish()
    }
}

/// Delegate ser/de to the data inside ArcMutex.
///
/// Inspired from:
/// https://users.rust-lang.org/t/how-to-serialize-deserialize-an-async-std-rwlock-t-where-t-serialize-deserialize/37407/2
mod arc_mutex_serde {
    use serde::de::Deserializer;
    use serde::ser::Serializer;
    use serde::{Deserialize, Serialize};
    use std::sync::{Arc, Mutex};

    pub fn serialize<S, T>(val: &Arc<Mutex<T>>, s: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
        T: Serialize,
    {
        T::serialize(&*val.lock().unwrap(), s)
    }

    pub fn deserialize<'de, D, T>(d: D) -> Result<Arc<Mutex<T>>, D::Error>
    where
        D: Deserializer<'de>,
        T: Deserialize<'de>,
    {
        Ok(Arc::new(Mutex::new(T::deserialize(d)?)))
    }
}

impl<S> PartialEq for RaftNode<S>
where
    S: Storage,
{
    /// Exclude self.storage from equality check.
    fn eq(&self, other: &Self) -> bool {
        self.metadata.eq(&other.metadata)
            && self
                .election
                .lock()
                .expect("Couldn't lock own election state.")
                .eq(&other
                    .election
                    .lock()
                    .expect("Couldn't lock other's election state."))
            && self
                .persistent_state
                .lock()
                .expect("Couldn't lock own persistent state.")
                .eq(&other
                    .persistent_state
                    .lock()
                    .expect("Couldn't lock other's persistent state."))
            && self
                .volatile_state
                .lock()
                .expect("Couldn't lock own volatile state.")
                .eq(&other
                    .volatile_state
                    .lock()
                    .expect("Couldn't lock other's volatile state."))
            && self.cluster.eq(&other.cluster)
    }
}

impl<S> Eq for RaftNode<S> where S: Storage {}

impl<S> RaftNode<S>
where
    S: Storage + Default,
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
        let guard = self
            .persistent_state
            .lock()
            .expect("Failed to lock persistent state");
        guard.log.iter().zip(guard.log.iter().skip(1)).all(
            |(predecessor_entry, successor_entry)| {
                successor_entry.term() >= predecessor_entry.term()
            },
        )
    }

    pub fn with_config<P>(mut self, path_to_config: P) -> Self
    where
        P: AsRef<Path>,
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

    pub fn reset_election(&self) {
        let mut election_guard = self.election.lock().unwrap();
        election_guard.state = ElectionState::Follower;
        election_guard.voter_log = hashbrown::HashSet::default();
        drop(election_guard);
    }
    pub fn intialize_leader_volatile_state(&self) {

        let persistent_state_guard = self.persistent_state.lock().unwrap();
        let last_log_index = persistent_state_guard.log.len();

        drop(persistent_state_guard);
        
        let mut match_index_map = HashMap::default();
        let mut next_index_map = HashMap::default();

        self
        .nodes()
        .iter()
        .for_each(|&peer_id| {
            next_index_map.insert(peer_id, last_log_index + 1);
            match_index_map.insert(peer_id, 0);
        });

        let mut volatile_state_guard = self.volatile_state.lock().unwrap();
        
        volatile_state_guard.match_index = match_index_map;
        volatile_state_guard.next_index = next_index_map;

        drop(volatile_state_guard);
    }

    pub fn handle_request_vote_response(
        &self, 
        peer_id: usize, 
        response: VoteResponse,
        has_become_leader_tx: UnboundedSender<()>
    ) {
        // We got a VoteResponse.

        let persistent_state_guard = self.persistent_state.lock().unwrap();
        let current_term = persistent_state_guard.current_term;
        drop(persistent_state_guard);

        // Update our current_term, if we're out-of-date.
        if response.term > current_term {

            let mut persistent_state_guard = self.persistent_state.lock().unwrap();
            persistent_state_guard.current_term = response.term;

            // Respectfully become a follower, since we're out-of-date.
            let mut election_guard = self.election.lock().unwrap();
            
            election_guard.state = ElectionState::Follower;
            election_guard.voter_log = hashbrown::HashSet::default();
            // election_guard.current_term = response.term;

            drop(election_guard);
            drop(persistent_state_guard);

            if let Err(e) = self.save() {
                tracing::error!("{}", e.to_string());
            }
            return
        }

        if response.vote_granted {
            // We got the vote from our peer.

            let mut election_guard = self.election.lock().unwrap();
            election_guard.voter_log.insert(peer_id);

            let vote_count = election_guard.voter_log.len();
            drop(election_guard);

            let majority_vote_threshold = (self.cluster.len() + 1) / 2;
            if vote_count >= majority_vote_threshold {
                tracing::trace!("Received a majority of the votes. Becoming the leader.");
                let mut election_guard = self.election.lock().unwrap();
                election_guard.state = ElectionState::Leader;
                drop(election_guard);

                // Notify ourselves of our election win.
                // And begin sending AppendEntriesRPCs.
                if let Err(e) = has_become_leader_tx.send(()) {
                    tracing::error!("Error sending a notification of our election win: {}", e.to_string());
                }
            }
        }
        // if let Err(e) = self.save() {
        //     tracing::error!("{}", e.to_string());
        // }
    }

    pub fn handle_becoming_candidate(&self) {
        let mut persistent_state_guard = self.persistent_state.lock().unwrap();

        persistent_state_guard.current_term += 1;
        persistent_state_guard.voted_for = Some(self.metadata.id);

        let mut election_guard = self.election.lock().unwrap();
        
        election_guard.state = ElectionState::Candidate;
        election_guard.voter_log = hashbrown::HashSet::default();

        drop(election_guard);

    }

    pub fn handle_becoming_leader(&self) {
        let mut election_guard = self.election.lock().unwrap();
        election_guard.state = ElectionState::Leader;
        drop(election_guard);

        self.intialize_leader_volatile_state();

    }

    pub fn build_vote_request(&self) -> VoteRequest {
        let persistent_state_guard = self.persistent_state.lock().unwrap();
        let term = persistent_state_guard.current_term;
        let last_log_index = persistent_state_guard.log.len();
        let last_log_term = {
            if persistent_state_guard.log.is_empty() {
                0
            }
            else {
                persistent_state_guard.log.last().unwrap().term()
            }
        };
        drop(persistent_state_guard);
        
        VoteRequest { term, candidate_id: self.metadata.id, last_log_index, last_log_term}
    }
    
    pub fn build_heartbeat(&self) -> AppendEntriesRequest {
            // pub term: usize,
            // pub leader_id: usize,
            // pub previous_log_index: usize,
            // pub previous_log_term: usize,
            // #[derivative(Default(value = "vec![]"))]
            // pub entries: Vec<Log>,
            // pub leader_commit_index: usize,
        let persistent_state_guard = self.persistent_state.lock().unwrap();
        let term = persistent_state_guard.current_term;
        let last_log_index = persistent_state_guard.log.len();
        let last_log_term = {
            if persistent_state_guard.log.is_empty() {
                0
            }
            else {
                persistent_state_guard.log.last().unwrap().term()
            }
        };
        drop(persistent_state_guard);

        let volatile_state_guard = self.volatile_state.lock().unwrap();
        let leader_commit_index = volatile_state_guard.commit_index;
        drop(volatile_state_guard);

        AppendEntriesRequest { 
            term, 
            leader_id: self.metadata.id, 
            previous_log_index: last_log_index, 
            previous_log_term: last_log_term, 
            entries: vec![], 
            leader_commit_index
        }
    }

    pub fn nodes(&self) -> Vec<usize> {
        self.cluster.keys().map(|&k| k).collect_vec()
    }

    pub fn update_next_index_and_match_index_for_follower(
        &self, 
        peer_id: usize,
        next_index: usize,
        match_index: usize,
    ) {
        let mut volatile_state_guard = self.volatile_state.lock().unwrap();

        volatile_state_guard
        .match_index
        .insert(peer_id, match_index);

        volatile_state_guard
        .next_index
        .insert(peer_id, next_index);
        
        drop(volatile_state_guard)
    }

}

impl<S> From<RaftConfig> for RaftNode<S>
where
    S: Storage + Default
{
    fn from(config: RaftConfig) -> Self {

        let mut cluster = HashMap::new();

        for node_metadata in config.peers.iter() {
            cluster.insert(node_metadata.id, node_metadata.clone());
        }

        Self {
            metadata: NodeMetadata { id: config.server.id, addr: config.server.addr },
            cluster,
            persistent_state: Arc::new(Mutex::new(PersistentState::default())),
            volatile_state: Arc::new(Mutex::new(VolatileState::default())),
            election: Arc::new(Mutex::new(Election::default())),
            storage: S::default()
        }
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
        INIT.call_once(|| start_tcp_server_on_port("127.0.0.1:9001".parse().unwrap()));
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
                    }
                    Err(_) => {}
                }
            }
        });

    }

    #[ignore]
    #[test]
    #[cfg(not(tarpaulin))]
    fn new_works() {
        setup_server();
        let node: RaftNode<BufferBackend> = RaftNode::new();

        assert_eq!(node.metadata.addr, "127.0.0.1:9000".parse().unwrap());
        assert_eq!(node.metadata.id, 1);

        let mut hmap = HashMap::new();

        hmap.insert(
            2,
            NodeMetadata {
                id: 2,
                addr: "127.0.0.1:9001".parse().unwrap(),
            },
        );
        hmap.insert(
            3,
            NodeMetadata {
                id: 3,
                addr: "127.0.0.1:9002".parse().unwrap(),
            },
        );

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
        S: Storage + Debug + Default,
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
        println!("{reloaded:#?}");
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
