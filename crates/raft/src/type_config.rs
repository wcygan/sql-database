//! OpenRaft type configuration for the database.
//!
//! This module defines the `TypeConfig` that configures all the generic types
//! used throughout the Raft implementation.

use crate::{Command, CommandResponse, NodeId};
use openraft::BasicNode;
use std::io::Cursor;

// Declare raft types using the macro for openraft 0.9 compatibility
openraft::declare_raft_types!(
    /// Raft type configuration for the sql-database.
    pub TypeConfig:
        D = Command,
        R = CommandResponse,
        NodeId = NodeId,
        Node = BasicNode,
        Entry = openraft::Entry<TypeConfig>,
        SnapshotData = Cursor<Vec<u8>>,
);

/// Type alias for log entries in this database.
pub type Entry = openraft::Entry<TypeConfig>;

/// Type alias for log ID.
pub type LogId = openraft::LogId<NodeId>;

/// Type alias for vote.
pub type Vote = openraft::Vote<NodeId>;

/// Type alias for snapshot metadata.
pub type SnapshotMeta = openraft::SnapshotMeta<NodeId, BasicNode>;

/// Type alias for membership config.
pub type Membership = openraft::Membership<NodeId, BasicNode>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn type_config_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<TypeConfig>();
    }
}
