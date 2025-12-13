//! OpenRaft consensus integration for sql-database.
//!
//! This crate provides distributed consensus for the database using OpenRaft.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────────┐
//! │                           Database Node                                  │
//! │  ┌───────────────┐     ┌──────────────┐     ┌───────────────────────┐   │
//! │  │   Database    │────▶│  RaftNode    │────▶│   MemRaftStore        │   │
//! │  │  (lib.rs)     │     │              │     │ (log + state machine) │   │
//! │  └───────────────┘     └──────────────┘     └───────────────────────┘   │
//! │         │                     │                        │                │
//! │         ▼                     ▼                        ▼                │
//! │  ┌───────────────┐     ┌──────────────┐     ┌───────────────────────┐   │
//! │  │ HeapFile      │     │ HTTP Server  │     │   ApplyHandler        │   │
//! │  │ (storage)     │◀────│ (axum)       │     │ (callback to storage) │   │
//! │  └───────────────┘     └──────────────┘     └───────────────────────┘   │
//! └─────────────────────────────────────────────────────────────────────────┘
//!                                │
//!                    ┌───────────┴───────────┐
//!                    ▼                       ▼
//!          ┌──────────────────┐    ┌──────────────────┐
//!          │   Node 2         │    │   Node 3         │
//!          │ (HTTP Client)    │    │ (HTTP Client)    │
//!          └──────────────────┘    └──────────────────┘
//! ```
//!
//! # Single-Node Mode
//!
//! For development and testing, a single-node cluster can be started:
//!
//! ```rust,ignore
//! use database::{Database, RaftConfig};
//!
//! let db = Database::with_raft_config(
//!     path,
//!     "catalog.json",
//!     "wal.log",
//!     32,
//!     Some(RaftConfig::single_node(1)),
//! ).await?;
//! ```
//!
//! # Multi-Node Cluster
//!
//! For a replicated cluster, configure each node with its peers:
//!
//! ```rust,ignore
//! use database::{Database, RaftConfig};
//!
//! // Node 1 (leader bootstrap node)
//! let config = RaftConfig::cluster(
//!     1,                                    // node_id
//!     "127.0.0.1:5001",                    // listen address
//!     vec![                                 // peer nodes
//!         (2, "127.0.0.1:5002".to_string()),
//!         (3, "127.0.0.1:5003".to_string()),
//!     ],
//! );
//!
//! let db = Database::with_raft_config(
//!     path,
//!     "catalog.json",
//!     "wal.log",
//!     32,
//!     Some(config),
//! ).await?;
//! ```
//!
//! # HTTP Endpoints
//!
//! Each node exposes the following Raft RPC endpoints:
//! - `POST /raft/append_entries` - Log replication from leader
//! - `POST /raft/vote` - Leader election votes
//! - `POST /raft/install_snapshot` - State transfer for new nodes
//! - `GET /health` - Node health and Raft status
//!
//! # Modules
//!
//! - [`command`]: Raft command types for DML/DDL operations
//! - [`config`]: Node configuration (data directory, ports)
//! - [`http_server`]: Axum HTTP endpoints for Raft RPCs
//! - [`log_storage`]: In-memory Raft log and state machine storage
//! - [`persistent_storage`]: Durable Raft log and state machine storage
//! - [`network`]: HTTP transport for inter-node communication
//! - [`type_config`]: OpenRaft type configuration
//!
//! # Future Milestones
//!
//! - Snapshots and log compaction
//! - Dynamic membership changes
//! - Read scaling via follower reads

pub mod command;
pub mod config;
pub mod http_server;
pub mod log_storage;
pub mod network;
pub mod persistent_storage;
pub mod state_machine;
pub mod type_config;

pub use command::{Command, CommandResponse};
pub use config::NodeConfig;
pub use http_server::{create_router, start_server, RaftHttpState, ServerHandle};
pub use log_storage::{
    new_log_store, new_state_machine_store, ApplyHandler, LogStore, MemRaftStore, StateMachineStore,
};
pub use network::{ClusterConfig, HttpNetwork, HttpNetworkFactory, Network, NetworkFactory};
pub use persistent_storage::{PersistentLogStore, PersistentRaftStore};
pub use type_config::TypeConfig;

use openraft::{Raft, StorageError, StorageIOError};
use std::sync::Arc;

/// The Raft consensus node type for this database.
pub type RaftNode = Raft<TypeConfig>;

/// Node identifier type.
pub type NodeId = u64;

/// Create an in-memory Raft storage instance.
///
/// Used for testing and development where persistence is not required.
pub fn create_mem_storage(apply_handler: Option<ApplyHandler>) -> LogStore {
    let store = match apply_handler {
        Some(handler) => MemRaftStore::with_apply_handler(handler),
        None => MemRaftStore::new(),
    };
    Arc::new(store)
}

/// Create a persistent Raft storage instance.
///
/// Used for production deployments where durability is required.
/// Storage survives restarts and includes checksums for data integrity.
///
/// # Arguments
/// * `config` - Node configuration specifying data directory
/// * `apply_handler` - Optional callback for applying commands to database storage
///
/// # Errors
/// Returns an error if the storage directory cannot be created or accessed.
#[allow(clippy::result_large_err)] // StorageError from OpenRaft is inherently large
pub fn create_persistent_storage(
    config: &NodeConfig,
    apply_handler: Option<ApplyHandler>,
) -> Result<PersistentLogStore, StorageError<NodeId>> {
    let store =
        PersistentRaftStore::open_with_handler(&config.data_dir, apply_handler).map_err(|e| {
            StorageError::IO {
                source: StorageIOError::write_logs(&e),
            }
        })?;
    Ok(Arc::new(store))
}
