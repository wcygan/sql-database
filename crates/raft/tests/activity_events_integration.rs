//! Integration tests for Raft activity events.
//!
//! These tests verify that activity events are correctly generated and sent
//! when commands are applied to the state machine.

use common::{PageId, RecordId, TableId};
use openraft::storage::Adaptor;
use openraft::{BasicNode, Raft};
use raft::{
    activity_channel, ApplyHandler, Command, CommandResponse, NetworkFactory, NodeConfig,
    PersistentRaftStore, TypeConfig,
};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use tokio::time::timeout;
use types::Value;

/// Helper to create a simple apply handler for testing.
fn test_apply_handler() -> ApplyHandler {
    Arc::new(|cmd| match cmd {
        Command::Insert { .. } => CommandResponse::Insert {
            rid: RecordId {
                page_id: PageId(0),
                slot: 0,
            },
        },
        Command::Update { .. } => CommandResponse::Update { rows_affected: 1 },
        Command::Delete { .. } => CommandResponse::Delete { rows_affected: 1 },
        _ => CommandResponse::Ddl,
    })
}

/// Test that activity events are sent when commands are applied via in-memory store.
#[tokio::test]
async fn mem_store_sends_activity_events() {
    let (tx, mut rx) = activity_channel();

    // Create store with activity channel
    let store = raft::create_mem_storage_with_activity(Some(test_apply_handler()), Some(tx));

    // Create Raft node
    let (log_store, state_machine) =
        Adaptor::<TypeConfig, Arc<raft::MemRaftStore>>::new(store.clone());

    let raft_config = Arc::new(openraft::Config {
        cluster_name: "test-activity".to_string(),
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

    // Wait for leader election and receive membership/blank events
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Drain initial events (membership change, possible blanks)
    let mut initial_events = vec![];
    while let Ok(Some(event)) = timeout(Duration::from_millis(50), rx.recv()).await {
        initial_events.push(event);
    }
    assert!(
        !initial_events.is_empty(),
        "Should have received initial events"
    );

    // Write an INSERT command
    let cmd = Command::Insert {
        table_id: TableId(42),
        row: vec![Value::Int(1), Value::Text("test".to_string())],
    };
    raft.client_write(cmd).await.unwrap();

    // Receive the INSERT event
    let event = timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("Should receive event")
        .expect("Channel should not be closed");

    assert!(
        event.description.contains("INSERT"),
        "Event should be INSERT: {}",
        event.description
    );
    assert!(
        event.description.contains("table=42"),
        "Event should reference table 42"
    );
    assert!(
        event.description.contains("cols=2"),
        "Event should have 2 columns"
    );
    assert!(event.log_index > 0, "Log index should be positive");
    assert!(event.term > 0, "Term should be positive");

    let _ = raft.shutdown().await;
}

/// Test that activity events are sent for DDL commands.
#[tokio::test]
async fn mem_store_sends_ddl_activity_events() {
    let (tx, mut rx) = activity_channel();

    let store = raft::create_mem_storage_with_activity(Some(test_apply_handler()), Some(tx));

    let (log_store, state_machine) =
        Adaptor::<TypeConfig, Arc<raft::MemRaftStore>>::new(store.clone());

    let raft_config = Arc::new(openraft::Config {
        cluster_name: "test-ddl".to_string(),
        election_timeout_min: 150,
        election_timeout_max: 300,
        heartbeat_interval: 50,
        ..Default::default()
    });

    let network = NetworkFactory::new(1);
    let raft = Raft::<TypeConfig>::new(1, raft_config, network, log_store, state_machine)
        .await
        .unwrap();

    let mut members = BTreeMap::new();
    members.insert(1u64, BasicNode::default());
    raft.initialize(members).await.unwrap();

    tokio::time::sleep(Duration::from_millis(200)).await;

    // Drain initial events
    while timeout(Duration::from_millis(50), rx.recv()).await.is_ok() {}

    // Test CREATE TABLE
    let cmd = Command::CreateTable {
        name: "users".to_string(),
        table_id: TableId(1),
        columns: vec![],
        primary_key: None,
    };
    raft.client_write(cmd).await.unwrap();

    let event = timeout(Duration::from_secs(1), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(event.description.contains("CREATE TABLE users"));

    // Test CREATE INDEX
    let cmd = Command::CreateIndex {
        table_id: TableId(1),
        index_name: "idx_test".to_string(),
        columns: vec!["col1".to_string()],
    };
    raft.client_write(cmd).await.unwrap();

    let event = timeout(Duration::from_secs(1), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(event.description.contains("CREATE INDEX idx_test"));

    // Test DROP INDEX
    let cmd = Command::DropIndex {
        table_id: TableId(1),
        index_name: "idx_test".to_string(),
    };
    raft.client_write(cmd).await.unwrap();

    let event = timeout(Duration::from_secs(1), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(event.description.contains("DROP INDEX idx_test"));

    // Test DROP TABLE
    let cmd = Command::DropTable {
        table_id: TableId(1),
    };
    raft.client_write(cmd).await.unwrap();

    let event = timeout(Duration::from_secs(1), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(event.description.contains("DROP TABLE id=1"));

    let _ = raft.shutdown().await;
}

/// Test that activity events are sent for UPDATE and DELETE commands.
#[tokio::test]
async fn mem_store_sends_dml_activity_events() {
    let (tx, mut rx) = activity_channel();

    let store = raft::create_mem_storage_with_activity(Some(test_apply_handler()), Some(tx));

    let (log_store, state_machine) =
        Adaptor::<TypeConfig, Arc<raft::MemRaftStore>>::new(store.clone());

    let raft_config = Arc::new(openraft::Config {
        cluster_name: "test-dml".to_string(),
        election_timeout_min: 150,
        election_timeout_max: 300,
        heartbeat_interval: 50,
        ..Default::default()
    });

    let network = NetworkFactory::new(1);
    let raft = Raft::<TypeConfig>::new(1, raft_config, network, log_store, state_machine)
        .await
        .unwrap();

    let mut members = BTreeMap::new();
    members.insert(1u64, BasicNode::default());
    raft.initialize(members).await.unwrap();

    tokio::time::sleep(Duration::from_millis(200)).await;

    // Drain initial events
    while timeout(Duration::from_millis(50), rx.recv()).await.is_ok() {}

    // Test UPDATE
    let cmd = Command::Update {
        table_id: TableId(5),
        rid: RecordId {
            page_id: PageId(1),
            slot: 7,
        },
        new_row: vec![Value::Int(999)],
    };
    raft.client_write(cmd).await.unwrap();

    let event = timeout(Duration::from_secs(1), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(event.description.contains("UPDATE table=5 rid=1:7"));

    // Test DELETE
    let cmd = Command::Delete {
        table_id: TableId(5),
        rid: RecordId {
            page_id: PageId(2),
            slot: 3,
        },
    };
    raft.client_write(cmd).await.unwrap();

    let event = timeout(Duration::from_secs(1), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(event.description.contains("DELETE table=5 rid=2:3"));

    let _ = raft.shutdown().await;
}

/// Test that persistent storage also sends activity events.
#[tokio::test]
async fn persistent_store_sends_activity_events() {
    let dir = TempDir::new().unwrap();
    let (tx, mut rx) = activity_channel();

    let config = NodeConfig::new(1, dir.path().to_path_buf()).with_persistent_storage(true);

    let store = raft::create_persistent_storage_with_activity(
        &config,
        Some(test_apply_handler()),
        Some(tx),
    )
    .unwrap();

    let (log_store, state_machine) =
        Adaptor::<TypeConfig, Arc<PersistentRaftStore>>::new(store.clone());

    let raft_config = Arc::new(openraft::Config {
        cluster_name: "test-persistent-activity".to_string(),
        election_timeout_min: 150,
        election_timeout_max: 300,
        heartbeat_interval: 50,
        ..Default::default()
    });

    let network = NetworkFactory::new(1);
    let raft = Raft::<TypeConfig>::new(1, raft_config, network, log_store, state_machine)
        .await
        .unwrap();

    let mut members = BTreeMap::new();
    members.insert(1u64, BasicNode::default());
    raft.initialize(members).await.unwrap();

    tokio::time::sleep(Duration::from_millis(200)).await;

    // Drain initial events
    while timeout(Duration::from_millis(50), rx.recv()).await.is_ok() {}

    // Write a command
    let cmd = Command::Insert {
        table_id: TableId(99),
        row: vec![Value::Bool(true)],
    };
    raft.client_write(cmd).await.unwrap();

    // Should receive activity event
    let event = timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("Should receive event")
        .expect("Channel should not be closed");

    assert!(event.description.contains("INSERT table=99"));
    assert!(event.description.contains("cols=1"));

    let _ = raft.shutdown().await;
}

/// Test that log indices and terms are correctly propagated in events.
#[tokio::test]
async fn activity_events_have_correct_indices() {
    let (tx, mut rx) = activity_channel();

    let store = raft::create_mem_storage_with_activity(Some(test_apply_handler()), Some(tx));

    let (log_store, state_machine) =
        Adaptor::<TypeConfig, Arc<raft::MemRaftStore>>::new(store.clone());

    let raft_config = Arc::new(openraft::Config {
        cluster_name: "test-indices".to_string(),
        election_timeout_min: 150,
        election_timeout_max: 300,
        heartbeat_interval: 50,
        ..Default::default()
    });

    let network = NetworkFactory::new(1);
    let raft = Raft::<TypeConfig>::new(1, raft_config, network, log_store, state_machine)
        .await
        .unwrap();

    let mut members = BTreeMap::new();
    members.insert(1u64, BasicNode::default());
    raft.initialize(members).await.unwrap();

    tokio::time::sleep(Duration::from_millis(200)).await;

    // Drain initial events
    while timeout(Duration::from_millis(50), rx.recv()).await.is_ok() {}

    // Write multiple commands and verify increasing indices
    let mut last_index = 0u64;
    for i in 0..5 {
        let cmd = Command::Insert {
            table_id: TableId(i),
            row: vec![Value::Int(i as i64)],
        };
        raft.client_write(cmd).await.unwrap();

        let event = timeout(Duration::from_secs(1), rx.recv())
            .await
            .unwrap()
            .unwrap();

        assert!(
            event.log_index > last_index,
            "Log index should increase: {} > {}",
            event.log_index,
            last_index
        );
        last_index = event.log_index;

        // All events should be in the same term (single node, no elections)
        assert!(event.term > 0);
    }

    let _ = raft.shutdown().await;
}

/// Test that no events are sent when activity channel is not configured.
#[tokio::test]
async fn no_activity_events_without_channel() {
    // Create store without activity channel
    let store = raft::create_mem_storage(Some(test_apply_handler()));

    let (log_store, state_machine) =
        Adaptor::<TypeConfig, Arc<raft::MemRaftStore>>::new(store.clone());

    let raft_config = Arc::new(openraft::Config {
        cluster_name: "test-no-channel".to_string(),
        election_timeout_min: 150,
        election_timeout_max: 300,
        heartbeat_interval: 50,
        ..Default::default()
    });

    let network = NetworkFactory::new(1);
    let raft = Raft::<TypeConfig>::new(1, raft_config, network, log_store, state_machine)
        .await
        .unwrap();

    let mut members = BTreeMap::new();
    members.insert(1u64, BasicNode::default());
    raft.initialize(members).await.unwrap();

    tokio::time::sleep(Duration::from_millis(200)).await;

    // Write a command - should not panic even without channel
    let cmd = Command::Insert {
        table_id: TableId(1),
        row: vec![Value::Int(42)],
    };
    let result = raft.client_write(cmd).await;
    assert!(result.is_ok(), "Command should succeed without channel");

    let _ = raft.shutdown().await;
}
