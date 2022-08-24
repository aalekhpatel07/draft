use derive_builder::Builder;
use hashbrown::HashMap;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

pub type Log<T> = (usize, T);
pub type Port = u16;

#[derive(Debug, Clone, Serialize, Deserialize, Default, Builder, PartialEq, Eq)]
#[builder(default)]
pub struct NodeMetadata {
    pub id: usize,
    #[builder(default = "8000")]
    pub port: Port,
    #[builder(default = "self.default_log_file_path()?")]
    pub log_file_path: PathBuf,
}

impl NodeMetadataBuilder {
    fn default_log_file_path(&self) -> Result<PathBuf, String> {
        match self.log_file_path {
            Some(ref log_file_path) => {
                let path = PathBuf::from(log_file_path);
                if !path.exists() || path.is_file() {
                    Ok(path
                        .canonicalize()
                        .expect("Failed to canonicalize log file path"))
                } else {
                    Err(format!("Log file path {:#?} is not a file", log_file_path))
                }
            }
            None => Err("log_file_path is required".to_string()),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Builder, Default, PartialEq, Eq)]
pub struct PersistentState<T> {
    #[builder(default = "vec![]")]
    pub log: Vec<Log<T>>,
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

#[derive(Debug, Clone, Serialize, Deserialize, Builder, Default, PartialEq, Eq)]
pub struct RaftNode {
    pub metadata: NodeMetadata,
    #[builder(default = "HashMap::new()")]
    pub cluster: HashMap<usize, NodeMetadata>,
    #[builder(default)]
    pub persistent_state: PersistentState<String>,
    #[builder(default)]
    pub volatile_state: VolatileState,
    #[builder(default)]
    pub election_state: ElectionState,
}

impl RaftNode {
    pub fn save_to_disk(&self) -> color_eyre::Result<usize> {
        let file_path = self.metadata.log_file_path.clone();
        let as_bytes = rmp_serde::to_vec(self)?;
        let total_bytes = as_bytes.len();
        std::fs::write(file_path, as_bytes)?;
        Ok(total_bytes)
    }
    pub fn reload_from_disk(&self) -> color_eyre::Result<Self> {
        let file_path = self.metadata.log_file_path.clone();
        let as_bytes = std::fs::read(file_path)?;
        let res = rmp_serde::decode::from_slice(&as_bytes)?;
        Ok(res)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let state = PersistentStateBuilder::default()
            .current_term(10)
            .log(vec![(10, "hello".to_string())])
            .build()
            .expect("Couldn't build persistent state with builder.");

        assert_eq!(state.current_term, 10);
        assert_eq!(state.log.len(), 1);
        assert_eq!(state.log[0].0, 10);
        assert_eq!(state.log[0].1, "hello".to_string());
        assert_eq!(state.voted_for, None);
    }

    #[test]
    fn save_to_disk_works() {
        let state = PersistentStateBuilder::default()
            .current_term(10)
            .log(vec![(10, "hello".to_string())])
            .build()
            .expect("Couldn't build persistent state with builder.");
        let raft = RaftNodeBuilder::default()
            .persistent_state(state)
            .metadata(
                NodeMetadataBuilder::default()
                    .log_file_path(PathBuf::from("/tmp/raftd.conf"))
                    .build()
                    .expect("Couldn't build metadata with builder."),
            )
            .build()
            .expect("Couldn't build raft node with builder.");

        raft.save_to_disk().unwrap();
        let reloaded = raft.reload_from_disk().unwrap();
        assert_eq!(reloaded, raft);
    }
}
