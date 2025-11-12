//! Integration tests for the client library.
//!
//! These tests start a server in the background and connect with the client.

use anyhow::Result;
use client::Client;
use database::Database;
use protocol::{ClientRequest, ServerResponse, frame};
use std::future::Future;
use std::sync::Arc;
use tempfile::TempDir;
use tokio::net::{TcpListener, TcpStream};
use types::Value;

/// Helper to run a test with a temporary server.
async fn with_test_server<F, Fut>(f: F) -> Result<()>
where
    F: FnOnce(String) -> Fut,
    Fut: Future<Output = Result<()>>,
{
    let temp_dir = TempDir::new()?;
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?.to_string();

    let db = Arc::new(Database::new(temp_dir.path(), "catalog.json", "test.wal", 10).await?);

    // Spawn server in background
    let server_task = tokio::spawn(run_server(listener, db));

    // Run test function
    let result = f(addr).await;

    // Cleanup
    server_task.abort();

    result
}

/// Run the server loop.
async fn run_server(listener: TcpListener, db: Arc<Database>) -> Result<()> {
    loop {
        let (socket, _addr) = listener.accept().await?;
        let db_clone = db.clone();
        tokio::spawn(async move {
            let _ = handle_client(socket, db_clone).await;
        });
    }
}

/// Handle a single client connection.
async fn handle_client(mut socket: TcpStream, db: Arc<Database>) -> Result<()> {
    loop {
        let request: ClientRequest = match frame::read_message_async(&mut socket).await {
            Ok(req) => req,
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e.into()),
        };

        match request {
            ClientRequest::Execute { sql } => {
                let result = db.execute(&sql).await;
                let response = match result {
                    Ok(database::QueryResult::Rows { schema, rows }) => {
                        ServerResponse::Rows { schema, rows }
                    }
                    Ok(database::QueryResult::Count { affected }) => {
                        ServerResponse::Count { affected }
                    }
                    Ok(database::QueryResult::Empty) => ServerResponse::Empty,
                    Err(e) => ServerResponse::Error {
                        code: protocol::ErrorCode::ExecutionError,
                        message: e.to_string(),
                    },
                };
                frame::write_message_async(&mut socket, &response).await?;
            }
            ClientRequest::Close => break,
        }
    }
    Ok(())
}

#[tokio::test]
async fn test_connect_and_close() {
    with_test_server(|addr| async move {
        let mut client = Client::connect(&addr).await?;
        client.close().await?;
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn test_create_table() {
    with_test_server(|addr| async move {
        let mut client = Client::connect(&addr).await?;

        let result = client
            .execute("CREATE TABLE users (id INT, name TEXT)")
            .await?;
        assert!(result.is_empty());

        client.close().await?;
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn test_insert_and_select() {
    with_test_server(|addr| async move {
        let mut client = Client::connect(&addr).await?;

        // Create table
        client
            .execute("CREATE TABLE users (id INT, name TEXT)")
            .await?;

        // Insert data
        let result = client
            .execute("INSERT INTO users VALUES (1, 'Alice')")
            .await?;
        assert_eq!(result.affected_count(), 1);

        // Query data
        let result = client.execute("SELECT * FROM users").await?;
        let (schema, rows) = result.rows().expect("Expected rows");
        assert_eq!(schema, &vec!["id".to_string(), "name".to_string()]);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].values[0], Value::Int(1));
        assert_eq!(rows[0].values[1], Value::Text("Alice".to_string()));

        client.close().await?;
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn test_multiple_operations() {
    with_test_server(|addr| async move {
        let mut client = Client::connect(&addr).await?;

        // Create table
        client
            .execute("CREATE TABLE products (id INT, name TEXT, price INT)")
            .await?;

        // Insert multiple rows
        client
            .execute("INSERT INTO products VALUES (1, 'Laptop', 1000)")
            .await?;
        client
            .execute("INSERT INTO products VALUES (2, 'Mouse', 25)")
            .await?;
        client
            .execute("INSERT INTO products VALUES (3, 'Keyboard', 75)")
            .await?;

        // Query with filter
        let result = client
            .execute("SELECT * FROM products WHERE price > 50")
            .await?;
        let (_, rows) = result.rows().expect("Expected rows");
        assert_eq!(rows.len(), 2); // Laptop and Keyboard

        client.close().await?;
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn test_error_handling() {
    with_test_server(|addr| async move {
        let mut client = Client::connect(&addr).await?;

        // Query non-existent table
        let result = client.execute("SELECT * FROM nonexistent").await;
        assert!(result.is_err());

        let err = result.unwrap_err();
        assert!(err.is_database_error());
        assert!(err.to_string().contains("nonexistent") || err.to_string().contains("not found"));

        client.close().await?;
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn test_connection_reuse() {
    with_test_server(|addr| async move {
        let mut client = Client::connect(&addr).await?;

        // Create table
        client.execute("CREATE TABLE numbers (value INT)").await?;

        // Insert multiple rows using the same connection
        for i in 0..10 {
            let result = client
                .execute(&format!("INSERT INTO numbers VALUES ({})", i))
                .await?;
            assert_eq!(result.affected_count(), 1);
        }

        // Query all rows
        let result = client.execute("SELECT * FROM numbers").await?;
        let (_, rows) = result.rows().expect("Expected rows");
        assert_eq!(rows.len(), 10);

        client.close().await?;
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn test_drop_table() {
    with_test_server(|addr| async move {
        let mut client = Client::connect(&addr).await?;

        // Create and drop table
        client.execute("CREATE TABLE temp (id INT)").await?;
        let result = client.execute("DROP TABLE temp").await?;
        assert!(result.is_empty());

        client.close().await?;
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn test_create_index() {
    with_test_server(|addr| async move {
        let mut client = Client::connect(&addr).await?;

        // Create table and index
        client
            .execute("CREATE TABLE indexed (id INT, value TEXT)")
            .await?;
        let result = client
            .execute("CREATE INDEX idx_value ON indexed(value)")
            .await?;
        assert!(result.is_empty());

        // Insert and query using index
        client
            .execute("INSERT INTO indexed VALUES (1, 'apple')")
            .await?;
        let result = client
            .execute("SELECT * FROM indexed WHERE value = 'apple'")
            .await?;
        let (_, rows) = result.rows().expect("Expected rows");
        assert_eq!(rows.len(), 1);

        client.close().await?;
        Ok(())
    })
    .await
    .unwrap();
}
