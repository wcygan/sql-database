//! Integration tests for Raft consensus mode.

use database::{activity_channel, Database, QueryResult, RaftConfig};
use std::time::Duration;
use tempfile::TempDir;
use tokio::time::timeout;

/// Test that database can be created with Raft disabled (default).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn database_without_raft() {
    let tmp = TempDir::new().unwrap();
    let db = Database::new(tmp.path(), "catalog.json", "wal.log", 32)
        .await
        .unwrap();

    assert!(!db.is_raft_enabled());
    assert!(db.is_leader()); // Non-Raft mode is always "leader"
    assert_eq!(db.node_id(), 1);

    // Basic operations should work
    db.execute("CREATE TABLE test (id INT)").await.unwrap();

    let result = db.execute("INSERT INTO test VALUES (1)").await.unwrap();
    if let QueryResult::Count { affected } = result {
        assert_eq!(affected, 1);
    } else {
        panic!("Expected count result");
    }
}

/// Test that database can be created with Raft enabled.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn database_with_raft_enabled() {
    let tmp = TempDir::new().unwrap();
    let raft_config = RaftConfig::single_node(1);

    let db =
        Database::with_raft_config(tmp.path(), "catalog.json", "wal.log", 32, Some(raft_config))
            .await
            .unwrap();

    assert!(db.is_raft_enabled());
    assert!(db.is_leader()); // Single-node cluster is always leader
    assert_eq!(db.node_id(), 1);

    // Basic operations should still work
    db.execute("CREATE TABLE test (id INT)").await.unwrap();

    let result = db.execute("INSERT INTO test VALUES (42)").await.unwrap();
    if let QueryResult::Count { affected } = result {
        assert_eq!(affected, 1);
    } else {
        panic!("Expected count result");
    }

    // Verify data was written
    let result = db.execute("SELECT * FROM test").await.unwrap();
    if let QueryResult::Rows { rows, .. } = result {
        assert_eq!(rows.len(), 1);
    } else {
        panic!("Expected rows result");
    }
}

/// Test that Raft config with enabled=false is the same as no Raft.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn raft_config_disabled() {
    let tmp = TempDir::new().unwrap();
    let raft_config = RaftConfig::default(); // disabled by default

    let db =
        Database::with_raft_config(tmp.path(), "catalog.json", "wal.log", 32, Some(raft_config))
            .await
            .unwrap();

    assert!(!db.is_raft_enabled());
    // Node ID defaults to 1 when Raft is disabled
    assert_eq!(db.node_id(), 1);
}

/// Test current_leader returns correct value.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn current_leader_single_node() {
    let tmp = TempDir::new().unwrap();
    let raft_config = RaftConfig::single_node(1);

    let db =
        Database::with_raft_config(tmp.path(), "catalog.json", "wal.log", 32, Some(raft_config))
            .await
            .unwrap();

    // In single-node mode, this node should be the leader
    let leader = db.current_leader().await;
    assert_eq!(leader, Some(1));
}

/// Test multiple INSERTs through Raft preserve order and data integrity.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn raft_multiple_inserts() {
    let tmp = TempDir::new().unwrap();
    let raft_config = RaftConfig::single_node(1);

    let db =
        Database::with_raft_config(tmp.path(), "catalog.json", "wal.log", 32, Some(raft_config))
            .await
            .unwrap();

    // Create a table with multiple columns
    db.execute("CREATE TABLE users (id INT, name TEXT, active BOOL)")
        .await
        .unwrap();

    // Insert multiple rows through Raft
    db.execute("INSERT INTO users VALUES (1, 'alice', true)")
        .await
        .unwrap();
    db.execute("INSERT INTO users VALUES (2, 'bob', false)")
        .await
        .unwrap();
    db.execute("INSERT INTO users VALUES (3, 'charlie', true)")
        .await
        .unwrap();

    // Verify all rows are present
    let result = db.execute("SELECT * FROM users").await.unwrap();
    if let QueryResult::Rows { rows, schema } = result {
        assert_eq!(schema, vec!["id", "name", "active"]);
        assert_eq!(rows.len(), 3);
    } else {
        panic!("Expected rows result");
    }
}

/// Test INSERT with different value types through Raft.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn raft_insert_different_types() {
    let tmp = TempDir::new().unwrap();
    let raft_config = RaftConfig::single_node(1);

    let db =
        Database::with_raft_config(tmp.path(), "catalog.json", "wal.log", 32, Some(raft_config))
            .await
            .unwrap();

    // Test INT
    db.execute("CREATE TABLE int_test (val INT)").await.unwrap();
    db.execute("INSERT INTO int_test VALUES (42)")
        .await
        .unwrap();
    db.execute("INSERT INTO int_test VALUES (100)")
        .await
        .unwrap();
    db.execute("INSERT INTO int_test VALUES (0)").await.unwrap();

    let result = db.execute("SELECT * FROM int_test").await.unwrap();
    if let QueryResult::Rows { rows, .. } = result {
        assert_eq!(rows.len(), 3);
    } else {
        panic!("Expected rows result");
    }

    // Test TEXT
    db.execute("CREATE TABLE text_test (val TEXT)")
        .await
        .unwrap();
    db.execute("INSERT INTO text_test VALUES ('hello')")
        .await
        .unwrap();
    db.execute("INSERT INTO text_test VALUES ('world')")
        .await
        .unwrap();

    let result = db.execute("SELECT * FROM text_test").await.unwrap();
    if let QueryResult::Rows { rows, .. } = result {
        assert_eq!(rows.len(), 2);
    } else {
        panic!("Expected rows result");
    }

    // Test BOOL
    db.execute("CREATE TABLE bool_test (val BOOL)")
        .await
        .unwrap();
    db.execute("INSERT INTO bool_test VALUES (true)")
        .await
        .unwrap();
    db.execute("INSERT INTO bool_test VALUES (false)")
        .await
        .unwrap();

    let result = db.execute("SELECT * FROM bool_test").await.unwrap();
    if let QueryResult::Rows { rows, .. } = result {
        assert_eq!(rows.len(), 2);
    } else {
        panic!("Expected rows result");
    }
}

/// Test that Raft-mode INSERT and non-Raft SELECT work together.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn raft_insert_then_select_with_filter() {
    let tmp = TempDir::new().unwrap();
    let raft_config = RaftConfig::single_node(1);

    let db =
        Database::with_raft_config(tmp.path(), "catalog.json", "wal.log", 32, Some(raft_config))
            .await
            .unwrap();

    db.execute("CREATE TABLE products (id INT, price INT, in_stock BOOL)")
        .await
        .unwrap();

    // Insert through Raft
    db.execute("INSERT INTO products VALUES (1, 100, true)")
        .await
        .unwrap();
    db.execute("INSERT INTO products VALUES (2, 200, false)")
        .await
        .unwrap();
    db.execute("INSERT INTO products VALUES (3, 150, true)")
        .await
        .unwrap();

    // SELECT with filter (goes through standard executor path)
    let result = db
        .execute("SELECT * FROM products WHERE in_stock = true")
        .await
        .unwrap();
    if let QueryResult::Rows { rows, .. } = result {
        assert_eq!(rows.len(), 2); // id=1 and id=3 have in_stock=true
    } else {
        panic!("Expected rows result");
    }
}

/// Test UPDATE through Raft consensus.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn raft_update_rows() {
    let tmp = TempDir::new().unwrap();
    let raft_config = RaftConfig::single_node(1);

    let db =
        Database::with_raft_config(tmp.path(), "catalog.json", "wal.log", 32, Some(raft_config))
            .await
            .unwrap();

    db.execute("CREATE TABLE employees (id INT, name TEXT, salary INT)")
        .await
        .unwrap();

    // Insert some rows through Raft
    db.execute("INSERT INTO employees VALUES (1, 'alice', 50000)")
        .await
        .unwrap();
    db.execute("INSERT INTO employees VALUES (2, 'bob', 60000)")
        .await
        .unwrap();
    db.execute("INSERT INTO employees VALUES (3, 'charlie', 55000)")
        .await
        .unwrap();

    // Update all rows through Raft
    let result = db
        .execute("UPDATE employees SET salary = 70000")
        .await
        .unwrap();
    if let QueryResult::Count { affected } = result {
        assert_eq!(affected, 3);
    } else {
        panic!("Expected count result");
    }

    // Verify all salaries were updated
    let result = db.execute("SELECT * FROM employees").await.unwrap();
    if let QueryResult::Rows { rows, .. } = result {
        assert_eq!(rows.len(), 3);
        for row in rows {
            assert_eq!(row.values[2], types::Value::Int(70000));
        }
    } else {
        panic!("Expected rows result");
    }
}

/// Test UPDATE with WHERE clause through Raft.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn raft_update_with_filter() {
    let tmp = TempDir::new().unwrap();
    let raft_config = RaftConfig::single_node(1);

    let db =
        Database::with_raft_config(tmp.path(), "catalog.json", "wal.log", 32, Some(raft_config))
            .await
            .unwrap();

    db.execute("CREATE TABLE items (id INT, status TEXT)")
        .await
        .unwrap();

    db.execute("INSERT INTO items VALUES (1, 'pending')")
        .await
        .unwrap();
    db.execute("INSERT INTO items VALUES (2, 'active')")
        .await
        .unwrap();
    db.execute("INSERT INTO items VALUES (3, 'pending')")
        .await
        .unwrap();

    // Update only pending items
    let result = db
        .execute("UPDATE items SET status = 'processed' WHERE status = 'pending'")
        .await
        .unwrap();
    if let QueryResult::Count { affected } = result {
        assert_eq!(affected, 2);
    } else {
        panic!("Expected count result");
    }

    // Verify: 2 processed, 1 active
    let result = db
        .execute("SELECT * FROM items WHERE status = 'processed'")
        .await
        .unwrap();
    if let QueryResult::Rows { rows, .. } = result {
        assert_eq!(rows.len(), 2);
    } else {
        panic!("Expected rows result");
    }

    let result = db
        .execute("SELECT * FROM items WHERE status = 'active'")
        .await
        .unwrap();
    if let QueryResult::Rows { rows, .. } = result {
        assert_eq!(rows.len(), 1);
    } else {
        panic!("Expected rows result");
    }
}

/// Test DELETE through Raft consensus.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn raft_delete_rows() {
    let tmp = TempDir::new().unwrap();
    let raft_config = RaftConfig::single_node(1);

    let db =
        Database::with_raft_config(tmp.path(), "catalog.json", "wal.log", 32, Some(raft_config))
            .await
            .unwrap();

    db.execute("CREATE TABLE logs (id INT, level TEXT)")
        .await
        .unwrap();

    db.execute("INSERT INTO logs VALUES (1, 'info')")
        .await
        .unwrap();
    db.execute("INSERT INTO logs VALUES (2, 'error')")
        .await
        .unwrap();
    db.execute("INSERT INTO logs VALUES (3, 'info')")
        .await
        .unwrap();
    db.execute("INSERT INTO logs VALUES (4, 'debug')")
        .await
        .unwrap();

    // Delete all info logs
    let result = db
        .execute("DELETE FROM logs WHERE level = 'info'")
        .await
        .unwrap();
    if let QueryResult::Count { affected } = result {
        assert_eq!(affected, 2);
    } else {
        panic!("Expected count result");
    }

    // Verify only 2 rows remain
    let result = db.execute("SELECT * FROM logs").await.unwrap();
    if let QueryResult::Rows { rows, .. } = result {
        assert_eq!(rows.len(), 2);
    } else {
        panic!("Expected rows result");
    }
}

/// Test DELETE all rows through Raft.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn raft_delete_all_rows() {
    let tmp = TempDir::new().unwrap();
    let raft_config = RaftConfig::single_node(1);

    let db =
        Database::with_raft_config(tmp.path(), "catalog.json", "wal.log", 32, Some(raft_config))
            .await
            .unwrap();

    db.execute("CREATE TABLE temp (id INT)").await.unwrap();

    db.execute("INSERT INTO temp VALUES (1)").await.unwrap();
    db.execute("INSERT INTO temp VALUES (2)").await.unwrap();
    db.execute("INSERT INTO temp VALUES (3)").await.unwrap();

    // Delete all rows (no WHERE clause)
    let result = db.execute("DELETE FROM temp").await.unwrap();
    if let QueryResult::Count { affected } = result {
        assert_eq!(affected, 3);
    } else {
        panic!("Expected count result");
    }

    // Verify table is empty
    let result = db.execute("SELECT * FROM temp").await.unwrap();
    if let QueryResult::Rows { rows, .. } = result {
        assert_eq!(rows.len(), 0);
    } else {
        panic!("Expected rows result");
    }
}

/// Test combined INSERT, UPDATE, DELETE through Raft.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn raft_full_dml_workflow() {
    let tmp = TempDir::new().unwrap();
    let raft_config = RaftConfig::single_node(1);

    let db =
        Database::with_raft_config(tmp.path(), "catalog.json", "wal.log", 32, Some(raft_config))
            .await
            .unwrap();

    db.execute("CREATE TABLE orders (id INT, customer TEXT, total INT)")
        .await
        .unwrap();

    // INSERT
    db.execute("INSERT INTO orders VALUES (1, 'alice', 100)")
        .await
        .unwrap();
    db.execute("INSERT INTO orders VALUES (2, 'bob', 200)")
        .await
        .unwrap();
    db.execute("INSERT INTO orders VALUES (3, 'alice', 150)")
        .await
        .unwrap();

    // UPDATE: Apply discount to alice's orders
    let result = db
        .execute("UPDATE orders SET total = 50 WHERE customer = 'alice'")
        .await
        .unwrap();
    if let QueryResult::Count { affected } = result {
        assert_eq!(affected, 2);
    } else {
        panic!("Expected count result");
    }

    // DELETE: Remove bob's order
    let result = db
        .execute("DELETE FROM orders WHERE customer = 'bob'")
        .await
        .unwrap();
    if let QueryResult::Count { affected } = result {
        assert_eq!(affected, 1);
    } else {
        panic!("Expected count result");
    }

    // Verify final state: 2 rows, both with total=50
    let result = db.execute("SELECT * FROM orders").await.unwrap();
    if let QueryResult::Rows { rows, .. } = result {
        assert_eq!(rows.len(), 2);
        for row in rows {
            assert_eq!(row.values[1], types::Value::Text("alice".to_string()));
            assert_eq!(row.values[2], types::Value::Int(50));
        }
    } else {
        panic!("Expected rows result");
    }
}

/// Test creating a multi-node cluster configuration.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn raft_cluster_config_creation() {
    // Test the cluster() constructor for multi-node configuration
    let config = RaftConfig::cluster(
        1,
        "127.0.0.1:5001",
        vec![
            (2, "127.0.0.1:5002".to_string()),
            (3, "127.0.0.1:5003".to_string()),
        ],
    );

    assert!(config.enabled);
    assert_eq!(config.node_id, 1);
    assert!(config.is_multi_node());
    assert_eq!(config.listen_addr, Some("127.0.0.1:5001".to_string()));
    assert_eq!(config.peers.len(), 2);
}

/// Test that single-node config is not considered multi-node.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn raft_single_node_not_multi_node() {
    let config = RaftConfig::single_node(1);
    assert!(!config.is_multi_node());
    assert!(config.listen_addr.is_none());
    assert!(config.peers.is_empty());
}

/// Test that a node can start with multi-node configuration as a single node.
/// This simulates the first node in a cluster starting up.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn raft_first_node_starts_as_cluster() {
    let tmp = TempDir::new().unwrap();

    // Start node 1 as a cluster with no peers yet (it will be the initial leader)
    let raft_config = RaftConfig::cluster(1, "127.0.0.1:15001", vec![]);

    let db =
        Database::with_raft_config(tmp.path(), "catalog.json", "wal.log", 32, Some(raft_config))
            .await
            .unwrap();

    assert!(db.is_raft_enabled());

    // Give it a moment for leader election
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Should become leader as single node
    assert!(db.is_leader());
    assert_eq!(db.current_leader().await, Some(1));

    // Should be able to execute DDL and DML
    db.execute("CREATE TABLE test (id INT, name TEXT)")
        .await
        .unwrap();

    let result = db
        .execute("INSERT INTO test VALUES (1, 'hello')")
        .await
        .unwrap();
    if let QueryResult::Count { affected } = result {
        assert_eq!(affected, 1);
    } else {
        panic!("Expected count result");
    }

    // Verify data
    let result = db.execute("SELECT * FROM test").await.unwrap();
    if let QueryResult::Rows { rows, schema } = result {
        assert_eq!(schema, vec!["id", "name"]);
        assert_eq!(rows.len(), 1);
    } else {
        panic!("Expected rows result");
    }
}

/// Test that persistent storage configuration works.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn raft_persistent_storage_config() {
    let raft_config = RaftConfig::single_node_persistent(1);
    assert!(raft_config.persistent_storage);
    assert!(raft_config.enabled);

    // Builder pattern
    let raft_config2 = RaftConfig::single_node(1).with_persistent_storage(true);
    assert!(raft_config2.persistent_storage);

    // Cluster persistent
    let cluster_config = RaftConfig::cluster_persistent(1, "127.0.0.1:5001", vec![]);
    assert!(cluster_config.persistent_storage);
}

/// Test that database with persistent Raft storage can be created and used.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn raft_with_persistent_storage() {
    let tmp = TempDir::new().unwrap();
    let raft_config = RaftConfig::single_node_persistent(1);

    let db =
        Database::with_raft_config(tmp.path(), "catalog.json", "wal.log", 32, Some(raft_config))
            .await
            .unwrap();

    assert!(db.is_raft_enabled());
    assert!(db.is_leader());

    // Basic operations should work
    db.execute("CREATE TABLE test (id INT, name TEXT)")
        .await
        .unwrap();

    let result = db
        .execute("INSERT INTO test VALUES (1, 'alice')")
        .await
        .unwrap();
    if let QueryResult::Count { affected } = result {
        assert_eq!(affected, 1);
    } else {
        panic!("Expected count result");
    }

    // Verify data
    let result = db.execute("SELECT * FROM test").await.unwrap();
    if let QueryResult::Rows { rows, .. } = result {
        assert_eq!(rows.len(), 1);
    } else {
        panic!("Expected rows result");
    }

    // Verify raft directory was created
    assert!(tmp.path().join("raft").exists());
    assert!(tmp.path().join("raft/snapshots").exists());
}

/// Test that data persists across database restarts with persistent Raft storage.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn raft_data_survives_restart() {
    let tmp = TempDir::new().unwrap();

    // Phase 1: Create database, insert data
    {
        let raft_config = RaftConfig::single_node_persistent(1);
        let db = Database::with_raft_config(
            tmp.path(),
            "catalog.json",
            "wal.log",
            32,
            Some(raft_config),
        )
        .await
        .unwrap();

        db.execute("CREATE TABLE users (id INT, name TEXT)")
            .await
            .unwrap();

        db.execute("INSERT INTO users VALUES (1, 'alice')")
            .await
            .unwrap();
        db.execute("INSERT INTO users VALUES (2, 'bob')")
            .await
            .unwrap();

        // Verify data was written
        let result = db.execute("SELECT * FROM users").await.unwrap();
        if let QueryResult::Rows { rows, .. } = result {
            assert_eq!(rows.len(), 2);
        } else {
            panic!("Expected rows result");
        }

        // Drop the database (simulates restart)
        drop(db);
    }

    // Small delay to ensure file handles are closed
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Phase 2: Reopen database and verify data persisted
    {
        let raft_config = RaftConfig::single_node_persistent(1);
        let db = Database::with_raft_config(
            tmp.path(),
            "catalog.json",
            "wal.log",
            32,
            Some(raft_config),
        )
        .await
        .unwrap();

        assert!(db.is_raft_enabled());

        // Table should exist and have data
        let result = db.execute("SELECT * FROM users").await.unwrap();
        if let QueryResult::Rows { rows, schema } = result {
            assert_eq!(schema, vec!["id", "name"]);
            assert_eq!(rows.len(), 2, "Expected 2 rows to survive restart");
        } else {
            panic!("Expected rows result");
        }
    }
}

// =============================================================================
// Activity Events Tests
// =============================================================================

/// Test that activity events are received for INSERT operations.
/// Note: DDL (CREATE TABLE) doesn't go through Raft, only DML does.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn activity_events_insert() {
    let tmp = TempDir::new().unwrap();
    let (tx, mut rx) = activity_channel();

    let raft_config = RaftConfig::single_node(1).with_activity_sender(tx);

    let db =
        Database::with_raft_config(tmp.path(), "catalog.json", "wal.log", 32, Some(raft_config))
            .await
            .unwrap();

    // Wait for initial Raft setup events
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Drain initial events (membership, blanks, etc.)
    while timeout(Duration::from_millis(50), rx.recv()).await.is_ok() {}

    // Create table (DDL doesn't go through Raft)
    db.execute("CREATE TABLE test (id INT, name TEXT)")
        .await
        .unwrap();

    // Insert a row (DML goes through Raft)
    db.execute("INSERT INTO test VALUES (1, 'alice')")
        .await
        .unwrap();

    // Receive INSERT event
    let event = timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("Should receive event")
        .expect("Channel not closed");
    assert!(
        event.description.contains("INSERT"),
        "Got: {}",
        event.description
    );
    assert!(event.log_index > 0);
    assert!(event.term > 0);
}

/// Test that activity events are received for UPDATE operations.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn activity_events_update() {
    let tmp = TempDir::new().unwrap();
    let (tx, mut rx) = activity_channel();

    let raft_config = RaftConfig::single_node(1).with_activity_sender(tx);

    let db =
        Database::with_raft_config(tmp.path(), "catalog.json", "wal.log", 32, Some(raft_config))
            .await
            .unwrap();

    tokio::time::sleep(Duration::from_millis(200)).await;
    while timeout(Duration::from_millis(50), rx.recv()).await.is_ok() {}

    // DDL doesn't go through Raft
    db.execute("CREATE TABLE items (id INT, price INT)")
        .await
        .unwrap();

    db.execute("INSERT INTO items VALUES (1, 100)")
        .await
        .unwrap();
    db.execute("INSERT INTO items VALUES (2, 200)")
        .await
        .unwrap();
    // Drain INSERT events
    let _ = timeout(Duration::from_secs(1), rx.recv()).await;
    let _ = timeout(Duration::from_secs(1), rx.recv()).await;

    // Update rows
    let result = db
        .execute("UPDATE items SET price = 50 WHERE id = 1")
        .await
        .unwrap();
    if let QueryResult::Count { affected } = result {
        assert_eq!(affected, 1);
    }

    // Should receive UPDATE event
    let event = timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("Should receive event")
        .expect("Channel not closed");
    assert!(
        event.description.contains("UPDATE"),
        "Got: {}",
        event.description
    );
}

/// Test that activity events are received for DELETE operations.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn activity_events_delete() {
    let tmp = TempDir::new().unwrap();
    let (tx, mut rx) = activity_channel();

    let raft_config = RaftConfig::single_node(1).with_activity_sender(tx);

    let db =
        Database::with_raft_config(tmp.path(), "catalog.json", "wal.log", 32, Some(raft_config))
            .await
            .unwrap();

    tokio::time::sleep(Duration::from_millis(200)).await;
    while timeout(Duration::from_millis(50), rx.recv()).await.is_ok() {}

    // DDL doesn't go through Raft
    db.execute("CREATE TABLE items (id INT)").await.unwrap();

    db.execute("INSERT INTO items VALUES (1)").await.unwrap();
    db.execute("INSERT INTO items VALUES (2)").await.unwrap();
    // Drain INSERT events
    let _ = timeout(Duration::from_secs(1), rx.recv()).await;
    let _ = timeout(Duration::from_secs(1), rx.recv()).await;

    // Delete a row
    let result = db.execute("DELETE FROM items WHERE id = 1").await.unwrap();
    if let QueryResult::Count { affected } = result {
        assert_eq!(affected, 1);
    }

    // Should receive DELETE event
    let event = timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("Should receive event")
        .expect("Channel not closed");
    assert!(
        event.description.contains("DELETE"),
        "Got: {}",
        event.description
    );
}

/// Test that multiple operations generate events with increasing log indices.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn activity_events_increasing_indices() {
    let tmp = TempDir::new().unwrap();
    let (tx, mut rx) = activity_channel();

    let raft_config = RaftConfig::single_node(1).with_activity_sender(tx);

    let db =
        Database::with_raft_config(tmp.path(), "catalog.json", "wal.log", 32, Some(raft_config))
            .await
            .unwrap();

    tokio::time::sleep(Duration::from_millis(200)).await;
    while timeout(Duration::from_millis(50), rx.recv()).await.is_ok() {}

    // DDL doesn't go through Raft
    db.execute("CREATE TABLE seq (id INT)").await.unwrap();

    // Insert multiple rows and verify indices increase
    let mut last_index = 0u64;
    for i in 1..=5 {
        db.execute(&format!("INSERT INTO seq VALUES ({})", i))
            .await
            .unwrap();

        let event = timeout(Duration::from_secs(1), rx.recv())
            .await
            .expect("Should receive event")
            .expect("Channel not closed");

        assert!(
            event.log_index > last_index,
            "Log index should increase: {} > {}",
            event.log_index,
            last_index
        );
        last_index = event.log_index;
    }
}

/// Test activity events with persistent storage.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn activity_events_persistent_storage() {
    let tmp = TempDir::new().unwrap();
    let (tx, mut rx) = activity_channel();

    let raft_config = RaftConfig::single_node_persistent(1).with_activity_sender(tx);

    let db =
        Database::with_raft_config(tmp.path(), "catalog.json", "wal.log", 32, Some(raft_config))
            .await
            .unwrap();

    tokio::time::sleep(Duration::from_millis(200)).await;
    while timeout(Duration::from_millis(50), rx.recv()).await.is_ok() {}

    // DDL doesn't go through Raft
    db.execute("CREATE TABLE persist_test (id INT)")
        .await
        .unwrap();

    // DML goes through Raft
    db.execute("INSERT INTO persist_test VALUES (42)")
        .await
        .unwrap();

    let event = timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("Should receive event")
        .expect("Channel not closed");

    assert!(event.description.contains("INSERT"));
}

/// Test that no events are sent when activity channel is not configured.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn no_activity_events_without_channel() {
    let tmp = TempDir::new().unwrap();

    // Create without activity channel
    let raft_config = RaftConfig::single_node(1);

    let db =
        Database::with_raft_config(tmp.path(), "catalog.json", "wal.log", 32, Some(raft_config))
            .await
            .unwrap();

    // Operations should succeed without panicking
    db.execute("CREATE TABLE test (id INT)").await.unwrap();
    db.execute("INSERT INTO test VALUES (1)").await.unwrap();
    db.execute("UPDATE test SET id = 2 WHERE id = 1")
        .await
        .unwrap();
    db.execute("DELETE FROM test WHERE id = 2").await.unwrap();

    // Verify operations worked
    let result = db.execute("SELECT * FROM test").await.unwrap();
    if let QueryResult::Rows { rows, .. } = result {
        assert_eq!(rows.len(), 0);
    } else {
        panic!("Expected rows result");
    }
}

/// Test activity events for a full DML workflow (INSERT, UPDATE, DELETE).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn activity_events_full_workflow() {
    let tmp = TempDir::new().unwrap();
    let (tx, mut rx) = activity_channel();

    let raft_config = RaftConfig::single_node(1).with_activity_sender(tx);

    let db =
        Database::with_raft_config(tmp.path(), "catalog.json", "wal.log", 32, Some(raft_config))
            .await
            .unwrap();

    tokio::time::sleep(Duration::from_millis(200)).await;
    while timeout(Duration::from_millis(50), rx.recv()).await.is_ok() {}

    // DDL doesn't go through Raft
    db.execute("CREATE TABLE workflow (id INT, val INT)")
        .await
        .unwrap();

    // INSERT
    db.execute("INSERT INTO workflow VALUES (1, 100)")
        .await
        .unwrap();
    let event = timeout(Duration::from_secs(1), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(event.description.contains("INSERT"));

    // UPDATE
    db.execute("UPDATE workflow SET val = 200 WHERE id = 1")
        .await
        .unwrap();
    let event = timeout(Duration::from_secs(1), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(event.description.contains("UPDATE"));

    // DELETE
    db.execute("DELETE FROM workflow WHERE id = 1")
        .await
        .unwrap();
    let event = timeout(Duration::from_secs(1), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(event.description.contains("DELETE"));
}
