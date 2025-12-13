//! Raft network implementation.
//!
//! This module provides the network layer for Raft communication between nodes.
//! - For single-node mode, we use a stub implementation (Network).
//! - For multi-node mode, we use HTTP-based communication (HttpNetwork).

use crate::type_config::TypeConfig;
use crate::NodeId;
use openraft::error::{InstallSnapshotError, RPCError, RaftError, Unreachable};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::BasicNode;
use std::collections::HashMap;
use std::io;
use std::sync::Arc;
use std::time::Duration;

/// Network factory for creating Raft network connections.
#[derive(Clone, Debug)]
pub struct NetworkFactory {
    /// Node ID of this node (for logging).
    #[allow(dead_code)]
    node_id: NodeId,
}

impl NetworkFactory {
    /// Create a new network factory for the given node.
    pub fn new(node_id: NodeId) -> Self {
        Self { node_id }
    }
}

impl RaftNetworkFactory<TypeConfig> for NetworkFactory {
    type Network = Network;

    async fn new_client(&mut self, target: NodeId, _node: &BasicNode) -> Self::Network {
        Network::new(target)
    }
}

/// Network client for communicating with a single Raft peer.
///
/// In single-node mode, this is a stub that never actually sends messages
/// since there are no other nodes in the cluster.
#[derive(Clone, Debug)]
pub struct Network {
    /// Target node ID.
    target: NodeId,
}

impl Network {
    /// Create a new network client for the given target node.
    pub fn new(target: NodeId) -> Self {
        Self { target }
    }
}

impl RaftNetwork<TypeConfig> for Network {
    async fn append_entries(
        &mut self,
        _req: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        // In single-node mode, there are no other nodes to send to.
        // This should never be called for a single-node cluster.
        Err(RPCError::Unreachable(Unreachable::new(&io::Error::new(
            io::ErrorKind::NotConnected,
            format!("No network connection to node {}", self.target),
        ))))
    }

    async fn install_snapshot(
        &mut self,
        _req: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, BasicNode, RaftError<NodeId, InstallSnapshotError>>,
    > {
        // In single-node mode, no snapshot installation needed.
        Err(RPCError::Unreachable(Unreachable::new(&io::Error::new(
            io::ErrorKind::NotConnected,
            format!("No network connection to node {}", self.target),
        ))))
    }

    async fn vote(
        &mut self,
        _req: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        // In single-node mode, no voting needed.
        Err(RPCError::Unreachable(Unreachable::new(&io::Error::new(
            io::ErrorKind::NotConnected,
            format!("No network connection to node {}", self.target),
        ))))
    }
}

/// Cluster configuration mapping node IDs to addresses.
#[derive(Clone, Debug, Default)]
pub struct ClusterConfig {
    /// Map of node_id -> address (e.g., "http://localhost:5001")
    nodes: HashMap<NodeId, String>,
}

impl ClusterConfig {
    /// Create a new empty cluster configuration.
    pub fn new() -> Self {
        Self {
            nodes: HashMap::new(),
        }
    }

    /// Add a node to the cluster configuration.
    pub fn add_node(&mut self, node_id: NodeId, addr: impl Into<String>) {
        self.nodes.insert(node_id, addr.into());
    }

    /// Get the address for a node.
    pub fn get_address(&self, node_id: NodeId) -> Option<&str> {
        self.nodes.get(&node_id).map(|s| s.as_str())
    }

    /// Create from a list of (node_id, address) pairs.
    pub fn from_peers(peers: Vec<(NodeId, String)>) -> Self {
        let mut config = Self::new();
        for (id, addr) in peers {
            config.add_node(id, addr);
        }
        config
    }
}

/// HTTP-based network factory for multi-node clusters.
#[derive(Clone)]
pub struct HttpNetworkFactory {
    /// This node's ID.
    node_id: NodeId,
    /// Cluster configuration with node addresses.
    cluster_config: Arc<ClusterConfig>,
    /// Shared HTTP client for connection pooling.
    client: reqwest::Client,
}

impl HttpNetworkFactory {
    /// Create a new HTTP network factory.
    pub fn new(node_id: NodeId, cluster_config: ClusterConfig) -> Self {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .connect_timeout(Duration::from_secs(5))
            .pool_max_idle_per_host(10)
            .build()
            .expect("Failed to create HTTP client");

        Self {
            node_id,
            cluster_config: Arc::new(cluster_config),
            client,
        }
    }
}

impl std::fmt::Debug for HttpNetworkFactory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpNetworkFactory")
            .field("node_id", &self.node_id)
            .field("cluster_config", &self.cluster_config)
            .finish()
    }
}

impl RaftNetworkFactory<TypeConfig> for HttpNetworkFactory {
    type Network = HttpNetwork;

    async fn new_client(&mut self, target: NodeId, _node: &BasicNode) -> Self::Network {
        let base_url = self
            .cluster_config
            .get_address(target)
            .map(|s| s.to_string())
            .unwrap_or_else(|| format!("http://unknown-node-{}", target));

        HttpNetwork::new(target, base_url, self.client.clone())
    }
}

/// HTTP-based network client for communicating with a single Raft peer.
pub struct HttpNetwork {
    /// Target node ID.
    target: NodeId,
    /// Base URL for the target node (e.g., "http://localhost:5001").
    base_url: String,
    /// HTTP client.
    client: reqwest::Client,
}

impl HttpNetwork {
    /// Create a new HTTP network client for the given target node.
    pub fn new(target: NodeId, base_url: String, client: reqwest::Client) -> Self {
        Self {
            target,
            base_url,
            client,
        }
    }

    /// Send a POST request to the target node.
    async fn post<Req, Resp>(&self, endpoint: &str, request: &Req) -> Result<Resp, io::Error>
    where
        Req: serde::Serialize,
        Resp: serde::de::DeserializeOwned,
    {
        let url = format!("{}{}", self.base_url, endpoint);

        let response = self
            .client
            .post(&url)
            .json(request)
            .send()
            .await
            .map_err(|e| io::Error::new(io::ErrorKind::ConnectionRefused, e.to_string()))?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(io::Error::other(format!(
                "HTTP {} from {}: {}",
                status, url, body
            )));
        }

        response
            .json()
            .await
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))
    }
}

impl Clone for HttpNetwork {
    fn clone(&self) -> Self {
        Self {
            target: self.target,
            base_url: self.base_url.clone(),
            client: self.client.clone(),
        }
    }
}

impl std::fmt::Debug for HttpNetwork {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpNetwork")
            .field("target", &self.target)
            .field("base_url", &self.base_url)
            .finish()
    }
}

impl RaftNetwork<TypeConfig> for HttpNetwork {
    async fn append_entries(
        &mut self,
        req: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        self.post("/raft/append_entries", &req)
            .await
            .map_err(|e| RPCError::Unreachable(Unreachable::new(&e)))
    }

    async fn install_snapshot(
        &mut self,
        req: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, BasicNode, RaftError<NodeId, InstallSnapshotError>>,
    > {
        self.post("/raft/install_snapshot", &req)
            .await
            .map_err(|e| RPCError::Unreachable(Unreachable::new(&e)))
    }

    async fn vote(
        &mut self,
        req: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        self.post("/raft/vote", &req)
            .await
            .map_err(|e| RPCError::Unreachable(Unreachable::new(&e)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn network_factory_creates_network() {
        let factory = NetworkFactory::new(1);
        assert_eq!(factory.node_id, 1);
    }

    #[test]
    fn network_stores_target() {
        let network = Network::new(42);
        assert_eq!(network.target, 42);
    }
}
