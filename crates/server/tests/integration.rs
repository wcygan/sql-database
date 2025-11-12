//! End-to-end tests for the client/server pipeline.
//!
//! These tests spin up a temporary TCP server using the `testsupport` crate,
//! connect with the public `client` library, and execute real SQL against an
//! isolated database directory.

use anyhow::{Result, anyhow, bail};
use client::{Client, ClientError, QueryResult as ClientQueryResult};
use protocol::ErrorCode;
use std::future::Future;
use testsupport::prelude::TestServer;
use tokio::task;
use types::Value;

async fn run_with_server<F, Fut>(test: F) -> Result<()>
where
    F: FnOnce(String) -> Fut,
    Fut: Future<Output = Result<()>>,
{
    let server = TestServer::start().await?;
    let addr = server.address().to_string();
    let result = test(addr).await;
    drop(server);
    result
}

#[tokio::test]
async fn client_can_execute_basic_queries() {
    run_with_server(|addr| async move {
        let mut client = Client::connect(&addr).await?;

        client
            .execute("CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")
            .await?;

        expect_count(
            client
                .execute("INSERT INTO users VALUES (1, 'Ada')")
                .await?,
            1,
        )?;
        expect_count(
            client
                .execute("INSERT INTO users VALUES (2, 'Bob')")
                .await?,
            1,
        )?;

        match client
            .execute("SELECT name FROM users WHERE id = 2")
            .await?
        {
            ClientQueryResult::Rows { mut rows, .. } => {
                assert_eq!(rows.len(), 1);
                let row = rows.pop().unwrap();
                assert_eq!(row.values[0], Value::Text("Bob".to_string()));
            }
            other => bail!("expected rows, got {:?}", other),
        }

        client.close().await?;
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn ddl_lifecycle_is_remote_accessible() {
    run_with_server(|addr| async move {
        let mut client = Client::connect(&addr).await?;

        client
            .execute("CREATE TABLE products (id INT, name TEXT)")
            .await?;
        match client.execute("DROP TABLE products").await? {
            ClientQueryResult::Empty => {}
            other => bail!("expected empty response for drop, got {:?}", other),
        }

        // Table can be recreated after drop to ensure catalog state is reset.
        client
            .execute("CREATE TABLE products (id INT, name TEXT)")
            .await?;

        client.close().await?;
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn concurrent_clients_see_each_others_writes() {
    run_with_server(|addr| async move {
        let mut setup = Client::connect(&addr).await?;
        setup
            .execute("CREATE TABLE events (id INT PRIMARY KEY, label TEXT)")
            .await?;
        setup.close().await?;

        let mut tasks = Vec::new();
        for i in 0..8 {
            let addr = addr.clone();
            tasks.push(task::spawn(async move {
                let mut client = Client::connect(&addr).await?;
                let sql = format!("INSERT INTO events VALUES ({i}, 'evt{i}')");
                expect_count(client.execute(&sql).await?, 1)?;
                client.close().await?;
                Result::<()>::Ok(())
            }));
        }

        for handle in tasks {
            handle.await??;
        }

        let mut verifier = Client::connect(&addr).await?;
        match verifier.execute("SELECT * FROM events").await? {
            ClientQueryResult::Rows { rows, .. } => assert_eq!(rows.len(), 8),
            other => bail!("expected rows, got {:?}", other),
        }
        verifier.close().await?;

        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn reports_missing_table_errors() {
    run_with_server(|addr| async move {
        let mut client = Client::connect(&addr).await?;

        match client.execute("SELECT * FROM nope").await {
            Err(ClientError::Database { code, message }) => {
                assert!(matches!(
                    code,
                    ErrorCode::PlanError | ErrorCode::CatalogError
                ));
                assert!(message.to_lowercase().contains("nope"));
            }
            other => bail!("expected database error, got {:?}", other),
        }

        client.close().await?;
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn propagates_parse_errors() {
    run_with_server(|addr| async move {
        let mut client = Client::connect(&addr).await?;

        match client.execute("CREAT TABLE broken").await {
            Err(ClientError::Database { code, .. }) => assert_eq!(code, ErrorCode::ParseError),
            other => bail!("expected parse error, got {:?}", other),
        }

        client.close().await?;
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn enforces_primary_keys_over_the_wire() {
    run_with_server(|addr| async move {
        let mut client = Client::connect(&addr).await?;

        client
            .execute("CREATE TABLE accounts (id INT PRIMARY KEY, name TEXT)")
            .await?;
        expect_count(
            client
                .execute("INSERT INTO accounts VALUES (1, 'alice')")
                .await?,
            1,
        )?;

        match client
            .execute("INSERT INTO accounts VALUES (1, 'duplicate')")
            .await
        {
            Err(ClientError::Database { code, .. }) => {
                assert_eq!(code, ErrorCode::ConstraintViolation);
            }
            other => bail!("expected constraint violation, got {:?}", other),
        }

        client.close().await?;
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn survives_abrupt_connection_drop() {
    run_with_server(|addr| async move {
        {
            let mut client = Client::connect(&addr).await?;
            client
                .execute("CREATE TABLE metrics (id INT PRIMARY KEY, value INT)")
                .await?;
            expect_count(
                client.execute("INSERT INTO metrics VALUES (1, 10)").await?,
                1,
            )?;
            // Intentionally drop without sending Close to mimic abrupt client exit.
        }

        let mut client = Client::connect(&addr).await?;
        expect_count(
            client.execute("INSERT INTO metrics VALUES (2, 20)").await?,
            1,
        )?;

        match client.execute("SELECT * FROM metrics").await? {
            ClientQueryResult::Rows { rows, .. } => assert_eq!(rows.len(), 2),
            other => bail!("expected rows, got {:?}", other),
        }

        client.close().await?;
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn handles_large_result_sets() {
    run_with_server(|addr| async move {
        let mut client = Client::connect(&addr).await?;
        client
            .execute("CREATE TABLE nums (id INT PRIMARY KEY, label TEXT)")
            .await?;

        for i in 0..200 {
            let sql = format!("INSERT INTO nums VALUES ({i}, 'n{i}')");
            expect_count(client.execute(&sql).await?, 1)?;
        }

        match client.execute("SELECT * FROM nums").await? {
            ClientQueryResult::Rows { rows, .. } => assert_eq!(rows.len(), 200),
            other => bail!("expected rows, got {:?}", other),
        }

        client.close().await?;
        Ok(())
    })
    .await
    .unwrap();
}

fn expect_count(result: ClientQueryResult, expected: u64) -> Result<()> {
    match result {
        ClientQueryResult::Count { affected } => {
            assert_eq!(affected, expected);
            Ok(())
        }
        other => Err(anyhow!("expected Count result, got {:?}", other)),
    }
}
