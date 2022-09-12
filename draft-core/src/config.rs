use crate::node::{NodeMetadata};
use serde::{Serialize, Deserialize};
use toml::from_slice;
use std::{path::Path, fs::read_to_string};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RaftConfig {
    pub server: NodeMetadata,
    pub peers: Vec<NodeMetadata>
}

pub fn load_from_file<P: AsRef<Path>>(path: P) -> Result<RaftConfig, std::io::Error> {
    // let config: Result<RaftConfig, std::io::Error> = 
    read_to_string(path)
    .map(|contents| {
        toml::from_str(&contents).expect("Couldn't parse toml.")
    })
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::NodeMetadata;

    #[test]
    pub fn config_basic() {
        let toml_str = r#"
            [server]
            id = 1
            addr = "127.0.0.1:9000"

            [[peers]]
            id = 2
            addr = "127.0.0.1:9001"

            [[peers]]
            id = 3
            addr = "127.0.0.1:9002"
        "#;
        let config: RaftConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(config.server.addr, "127.0.0.1:9000".parse().unwrap());
        assert_eq!(config.server.id, 1);
        assert_eq!(config.peers, vec![
            NodeMetadata {
                id: 2,
                addr: "127.0.0.1:9001".parse().unwrap()
            },
            NodeMetadata {
                id: 3,
                addr: "127.0.0.1:9002".parse().unwrap()
            },
        ]);
    }

    #[test]
    pub fn config_from_file() {
        let path = "/etc/raftd/raftd.toml";
        let config = load_from_file(path).unwrap();
        assert_eq!(config.server.addr, "127.0.0.1:9000".parse().unwrap());
        assert_eq!(config.server.id, 1);
        assert_eq!(config.peers, vec![
            NodeMetadata {
                id: 2,
                addr: "127.0.0.1:9001".parse().unwrap()
            },
            NodeMetadata {
                id: 3,
                addr: "127.0.0.1:9002".parse().unwrap()
            },
        ]);
    }
}