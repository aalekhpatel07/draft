use bytes::Bytes;
use derive_builder::Builder;
use hashbrown::HashMap;

use serde::{Deserialize, Serialize};

use crate::{Storage, FileStorageBackend, BufferBackend};

pub type Log = (usize, Bytes);
pub type Port = u16;

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
    pub port: Port,
}

impl Default for NodeMetadata 
{
    fn default() -> Self {

        Self {
            id: 0,
            port: 8000,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaftNode<S>
{
    pub metadata: NodeMetadata,
    pub cluster: HashMap<usize, NodeMetadata>,
    pub persistent_state: PersistentState,
    pub volatile_state: VolatileState,
    pub election_state: ElectionState,
    #[serde(skip)]
    pub storage: S
}

impl Default for RaftNode<FileStorageBackend> 
{
    fn default() -> Self {
        Self {
            metadata: NodeMetadata::default(),
            cluster: HashMap::new(),
            persistent_state: PersistentState::default(),
            volatile_state: VolatileState::default(),
            election_state: ElectionState::Follower,
            storage: FileStorageBackend::new("/tmp/raft.d")
        }
    }
}

impl Default for RaftNode<BufferBackend>
{
    fn default() -> Self {
        Self {
            metadata: NodeMetadata::default(),
            cluster: HashMap::new(),
            persistent_state: PersistentState::default(),
            volatile_state: VolatileState::default(),
            election_state: ElectionState::Follower,
            storage: BufferBackend::new()
        }
    }
}

impl<S> PartialEq for RaftNode<S>
where
    S: Storage
{
    /// Exclude self.storage from equality check.
    fn eq(&self, other: &Self) -> bool {
        self.metadata.eq(&other.metadata) &&
        self.election_state.eq(&other.election_state) &&
        self.persistent_state.eq(&other.persistent_state) &&
        self.volatile_state.eq(&other.volatile_state) &&
        self.cluster.eq(&other.cluster)
    }
}

impl<S> Eq for RaftNode<S>
where
    S: Storage {}

impl<S> RaftNode<S>
where
    S: Storage
{
    pub fn save(&mut self) -> color_eyre::Result<usize> {
        match serde_json::to_vec(self) {
            Ok(serialized_data) => {
                Ok(self.storage.save(&serialized_data)?)
            },
            Err(e) => {
                Err(e.into())
            }
        }
    }
    pub fn load(&mut self) -> color_eyre::Result<Self> {
        match self.storage.load() {
            Ok(serialized_data) => {
                let self_: Self = serde_json::from_slice(&serialized_data)?;
                Ok(self_)
            },
            Err(e) => {
                Err(e)
            }
        }
    }

    /// Determine whether all entries in our log have non-decreasing terms.
    #[cfg(test)]
    pub fn are_terms_non_decreasing(&self) -> bool {
        self
        .persistent_state
        .log
        .iter()
        .zip(
            self
            .persistent_state
            .log
            .iter()
            .skip(1)
        )
        .all(|(predecessor_entry, successor_entry)| {
            successor_entry.term() >= predecessor_entry.term()
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

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

    #[test]
    fn save_to_disk_works() {
        let state = PersistentStateBuilder::default()
            .current_term(10)
            .log(vec![(10, Bytes::from("hello"))])
            .build()
            .expect("Couldn't build persistent state with builder.");

        let mut raft: RaftNode<FileStorageBackend> = RaftNode::default();
        raft.persistent_state = state;

        raft.save().unwrap();
        let reloaded = raft.load().unwrap();
        assert_eq!(reloaded, raft);
    }

    #[test]
    fn save_to_buffer_works() {
        let state = PersistentStateBuilder::default()
            .current_term(10)
            .log(vec![(10, Bytes::from("hello"))])
            .build()
            .expect("Couldn't build persistent state with builder.");

        let mut raft: RaftNode<BufferBackend> = RaftNode::default();
        raft.persistent_state = state;

        raft.save().unwrap();
        let reloaded = raft.load().unwrap();
        assert_eq!(reloaded, raft);
    }
}
