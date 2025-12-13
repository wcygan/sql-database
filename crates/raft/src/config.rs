//! Raft node configuration.

use crate::NodeId;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Configuration for a Raft node in the cluster.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NodeConfig {
    /// Unique identifier for this node.
    pub node_id: NodeId,

    /// Directory for storing Raft log and state files.
    pub data_dir: PathBuf,

    /// Address this node listens on (e.g., "127.0.0.1:5000").
    pub listen_addr: String,

    /// Addresses of peer nodes for initial cluster formation.
    /// Format: Vec of (node_id, address) pairs.
    pub peers: Vec<(NodeId, String)>,

    /// Election timeout range in milliseconds.
    /// A random value within this range is used for each election.
    pub election_timeout_min_ms: u64,
    pub election_timeout_max_ms: u64,

    /// Heartbeat interval in milliseconds.
    pub heartbeat_interval_ms: u64,

    /// Whether to use persistent storage (survives restarts).
    /// When false, uses in-memory storage (useful for testing).
    pub persistent_storage: bool,
}

impl Default for NodeConfig {
    fn default() -> Self {
        Self {
            node_id: 1,
            data_dir: PathBuf::from("./raft_data"),
            listen_addr: "127.0.0.1:5000".to_string(),
            peers: Vec::new(),
            election_timeout_min_ms: 150,
            election_timeout_max_ms: 300,
            heartbeat_interval_ms: 50,
            persistent_storage: false, // Default to in-memory for tests
        }
    }
}

impl NodeConfig {
    /// Create a new node configuration.
    pub fn new(node_id: NodeId, data_dir: PathBuf) -> Self {
        Self {
            node_id,
            data_dir,
            ..Default::default()
        }
    }

    /// Set the listen address.
    pub fn with_listen_addr(mut self, addr: impl Into<String>) -> Self {
        self.listen_addr = addr.into();
        self
    }

    /// Add a peer node.
    pub fn with_peer(mut self, node_id: NodeId, addr: impl Into<String>) -> Self {
        self.peers.push((node_id, addr.into()));
        self
    }

    /// Set election timeout range.
    pub fn with_election_timeout(mut self, min_ms: u64, max_ms: u64) -> Self {
        self.election_timeout_min_ms = min_ms;
        self.election_timeout_max_ms = max_ms;
        self
    }

    /// Set heartbeat interval.
    pub fn with_heartbeat_interval(mut self, ms: u64) -> Self {
        self.heartbeat_interval_ms = ms;
        self
    }

    /// Enable or disable persistent storage.
    ///
    /// When enabled, Raft log and state are persisted to disk and survive restarts.
    /// When disabled, uses in-memory storage (useful for testing).
    pub fn with_persistent_storage(mut self, enabled: bool) -> Self {
        self.persistent_storage = enabled;
        self
    }

    /// Path to the Raft log file.
    pub fn log_path(&self) -> PathBuf {
        self.data_dir.join("raft.log")
    }

    /// Path to the Raft state file (vote, term).
    pub fn state_path(&self) -> PathBuf {
        self.data_dir.join("raft_state.json")
    }

    /// Path to the snapshot directory.
    pub fn snapshot_dir(&self) -> PathBuf {
        self.data_dir.join("snapshots")
    }

    /// Build OpenRaft config from this node config.
    pub fn to_openraft_config(&self) -> openraft::Config {
        openraft::Config {
            election_timeout_min: self.election_timeout_min_ms,
            election_timeout_max: self.election_timeout_max_ms,
            heartbeat_interval: self.heartbeat_interval_ms,
            ..Default::default()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config() {
        let config = NodeConfig::default();
        assert_eq!(config.node_id, 1);
        assert_eq!(config.listen_addr, "127.0.0.1:5000");
        assert!(config.peers.is_empty());
    }

    #[test]
    fn config_builder_pattern() {
        let config = NodeConfig::new(42, PathBuf::from("/data"))
            .with_listen_addr("0.0.0.0:8080")
            .with_peer(2, "192.168.1.2:8080")
            .with_peer(3, "192.168.1.3:8080")
            .with_election_timeout(200, 400)
            .with_heartbeat_interval(100);

        assert_eq!(config.node_id, 42);
        assert_eq!(config.listen_addr, "0.0.0.0:8080");
        assert_eq!(config.peers.len(), 2);
        assert_eq!(config.election_timeout_min_ms, 200);
        assert_eq!(config.heartbeat_interval_ms, 100);
    }

    #[test]
    fn paths() {
        let config = NodeConfig::new(1, PathBuf::from("/var/raft"));
        assert_eq!(config.log_path(), PathBuf::from("/var/raft/raft.log"));
        assert_eq!(
            config.state_path(),
            PathBuf::from("/var/raft/raft_state.json")
        );
        assert_eq!(config.snapshot_dir(), PathBuf::from("/var/raft/snapshots"));
    }
}
