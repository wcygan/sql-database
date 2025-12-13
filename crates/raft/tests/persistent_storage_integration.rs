//! Integration tests for persistent Raft storage.
//!
//! These tests verify that the persistent storage correctly survives restarts
//! and can be used with actual Raft nodes.

use common::{RecordId, TableId};
use openraft::storage::Adaptor;
use openraft::{BasicNode, Raft};
use raft::{
    ApplyHandler, Command, CommandResponse, NetworkFactory, NodeConfig, PersistentRaftStore,
    TypeConfig,
};
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use tempfile::TempDir;
use types::Value;

/// Helper to create a simple apply handler for testing.
fn test_apply_handler() -> ApplyHandler {
    Arc::new(|cmd| match cmd {
        Command::Insert { .. } => CommandResponse::Insert {
            rid: RecordId {
                page_id: common::PageId(0),
                slot: 0,
            },
        },
        Command::Update { .. } => CommandResponse::Update { rows_affected: 1 },
        Command::Delete { .. } => CommandResponse::Delete { rows_affected: 1 },
        _ => CommandResponse::Ddl,
    })
}

/// Test that persistent storage survives a restart with a real Raft node.
#[tokio::test]
async fn persistent_storage_survives_restart() {
    let dir = TempDir::new().unwrap();

    // Phase 1: Create store, write data through Raft
    {
        let store = Arc::new(
            PersistentRaftStore::open_with_handler(dir.path(), Some(test_apply_handler())).unwrap(),
        );

        // Create Raft node with persistent storage
        let (log_store, state_machine) =
            Adaptor::<TypeConfig, Arc<PersistentRaftStore>>::new(store.clone());

        let raft_config = Arc::new(openraft::Config {
            cluster_name: "test-cluster".to_string(),
            election_timeout_min: 150,
            election_timeout_max: 300,
            heartbeat_interval: 50,
            ..Default::default()
        });

        let network = NetworkFactory::new(1);
        let raft = Raft::<TypeConfig>::new(1, raft_config, network, log_store, state_machine)
            .await
            .unwrap();

        // Initialize single-node cluster
        let mut members = BTreeMap::new();
        members.insert(1u64, BasicNode::default());
        raft.initialize(members).await.unwrap();

        // Wait for leader election
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

        // Write some commands through Raft
        let cmd1 = Command::Insert {
            table_id: TableId(1),
            row: vec![Value::Int(42), Value::Text("alice".to_string())],
        };
        raft.client_write(cmd1).await.unwrap();

        let cmd2 = Command::Insert {
            table_id: TableId(1),
            row: vec![Value::Int(100), Value::Text("bob".to_string())],
        };
        raft.client_write(cmd2).await.unwrap();

        // Shutdown raft
        let _ = raft.shutdown().await;
    }

    // Small delay to ensure file handles are closed
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Phase 2: Reopen store and verify data persisted
    {
        let store = Arc::new(
            PersistentRaftStore::open_with_handler(dir.path(), Some(test_apply_handler())).unwrap(),
        );

        // Verify log entries were persisted
        let (mut log_store, _state_machine) =
            Adaptor::<TypeConfig, Arc<PersistentRaftStore>>::new(store.clone());

        use openraft::storage::RaftLogStorage;
        let log_state = log_store.get_log_state().await.unwrap();

        // Should have entries (exact count depends on Raft internals - membership + commands)
        assert!(
            log_state.last_log_id.is_some(),
            "Expected log entries to be persisted"
        );
        let last_index = log_state.last_log_id.unwrap().index;
        assert!(
            last_index >= 2,
            "Expected at least 2 log entries, got {}",
            last_index
        );

        println!("Recovered log state: {:?}", log_state);
    }
}

/// Test that vote persists across restarts.
#[tokio::test]
async fn vote_persists_across_restart() {
    let dir = TempDir::new().unwrap();
    let vote = openraft::Vote::new(5, 1);

    // Phase 1: Save vote
    {
        let store = Arc::new(PersistentRaftStore::open(dir.path()).unwrap());
        let (mut log_store, _) =
            Adaptor::<TypeConfig, Arc<PersistentRaftStore>>::new(store.clone());

        use openraft::storage::RaftLogStorage;
        log_store.save_vote(&vote).await.unwrap();
    }

    // Phase 2: Verify vote persisted
    {
        let store = Arc::new(PersistentRaftStore::open(dir.path()).unwrap());
        let (mut log_store, _) =
            Adaptor::<TypeConfig, Arc<PersistentRaftStore>>::new(store.clone());

        use openraft::storage::RaftLogStorage;
        let read_vote = log_store.read_vote().await.unwrap();
        assert_eq!(read_vote, Some(vote));
    }
}

/// Test that committed index persists.
#[tokio::test]
async fn committed_persists_across_restart() {
    let dir = TempDir::new().unwrap();
    let committed = openraft::LogId::new(openraft::CommittedLeaderId::new(1, 1), 5);

    // Phase 1: Save committed
    {
        let store = Arc::new(PersistentRaftStore::open(dir.path()).unwrap());
        let (mut log_store, _) =
            Adaptor::<TypeConfig, Arc<PersistentRaftStore>>::new(store.clone());

        use openraft::storage::RaftLogStorage;
        log_store.save_committed(Some(committed)).await.unwrap();
    }

    // Phase 2: Verify committed persisted
    {
        let store = Arc::new(PersistentRaftStore::open(dir.path()).unwrap());
        let (mut log_store, _) =
            Adaptor::<TypeConfig, Arc<PersistentRaftStore>>::new(store.clone());

        use openraft::storage::RaftLogStorage;
        let read_committed = log_store.read_committed().await.unwrap();
        assert_eq!(read_committed, Some(committed));
    }
}

/// Test the config builder pattern for persistent storage.
#[test]
fn config_persistent_storage_flag() {
    let config = NodeConfig::new(1, PathBuf::from("/data"))
        .with_persistent_storage(true)
        .with_listen_addr("0.0.0.0:5000");

    assert!(config.persistent_storage);
    assert_eq!(config.node_id, 1);
    assert_eq!(config.listen_addr, "0.0.0.0:5000");
}

/// Test factory functions for creating storage.
#[test]
fn storage_factory_creates_mem_store() {
    let store = raft::create_mem_storage(None);
    // Just verify it compiles and returns something
    assert!(Arc::strong_count(&store) >= 1);
}

/// Test factory function for persistent storage.
#[test]
fn storage_factory_creates_persistent_store() {
    let dir = TempDir::new().unwrap();
    let config = NodeConfig::new(1, dir.path().to_path_buf()).with_persistent_storage(true);

    let store = raft::create_persistent_storage(&config, None).unwrap();
    assert!(Arc::strong_count(&store) >= 1);

    // Verify directories were created
    assert!(dir.path().join("snapshots").exists());
}

/// Test that log file is created and has correct magic bytes on first write.
#[tokio::test]
async fn log_file_structure() {
    use openraft::storage::RaftStorage;
    use std::fs::File;
    use std::io::Read;

    let dir = TempDir::new().unwrap();

    let mut store = Arc::new(PersistentRaftStore::open(dir.path()).unwrap());

    // Initially no log file
    let log_path = dir.path().join("raft.log");
    assert!(!log_path.exists());

    // Append an entry using RaftStorage trait
    use openraft::{Entry, EntryPayload};
    let entry = Entry {
        log_id: openraft::LogId::new(openraft::CommittedLeaderId::new(1, 1), 1),
        payload: EntryPayload::Normal(Command::Insert {
            table_id: TableId(1),
            row: vec![Value::Int(42)],
        }),
    };
    store.append_to_log(vec![entry]).await.unwrap();

    // Now log file should exist
    assert!(log_path.exists());

    // Read log file and verify magic bytes
    let mut file = File::open(&log_path).unwrap();
    let mut magic = [0u8; 4];
    file.read_exact(&mut magic).unwrap();

    // "RAFT" = 0x52414654 in little-endian
    assert_eq!(magic, [0x54, 0x46, 0x41, 0x52]);
}

/// Test that state file is created with proper JSON structure.
#[tokio::test]
async fn state_file_structure() {
    use openraft::storage::RaftLogStorage;
    use std::fs;

    let dir = TempDir::new().unwrap();

    let store = Arc::new(PersistentRaftStore::open(dir.path()).unwrap());
    let (mut log_store, _) = Adaptor::<TypeConfig, Arc<PersistentRaftStore>>::new(store.clone());

    // Initially no state file
    let state_path = dir.path().join("raft_state.json");
    assert!(!state_path.exists());

    // Save a vote
    let vote = openraft::Vote::new(1, 1);
    log_store.save_vote(&vote).await.unwrap();

    // State file should now exist
    assert!(state_path.exists());

    // Parse as JSON to verify structure
    let contents = fs::read_to_string(&state_path).unwrap();
    let json: serde_json::Value = serde_json::from_str(&contents).unwrap();

    // Should have expected fields
    assert!(json.get("vote").is_some());
    assert!(json.get("committed").is_some());
    assert!(json.get("last_purged_log_id").is_some());
    assert!(json.get("snapshot_idx").is_some());
}
