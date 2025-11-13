//! Concurrent clients example demonstrating multiple simultaneous connections.
//!
//! This example shows how to:
//! - Connect multiple clients concurrently
//! - Execute operations from different tasks
//! - Handle concurrent inserts
//! - Verify isolation and consistency
//!
//! Run the server first:
//!   cargo run --bin toydb-server -- --data-dir /tmp/example-db
//!
//! Then run this example:
//!   cargo run --example concurrent_clients

use anyhow::Result;
use client::Client;
use tokio::task;

#[tokio::main]
async fn main() -> Result<()> {
    println!("=== Concurrent Clients Example ===\n");

    // Set up the table with a single client
    println!("Setting up table...");
    let mut setup_client = Client::connect("localhost:5432").await?;
    setup_client
        .execute("CREATE TABLE events (id INT PRIMARY KEY, name TEXT, worker INT)")
        .await?;
    setup_client.close().await?;
    println!("Table created.\n");

    // Spawn 10 concurrent workers
    const NUM_WORKERS: i32 = 10;
    const EVENTS_PER_WORKER: i32 = 5;

    println!(
        "Spawning {} workers, each inserting {} events...",
        NUM_WORKERS, EVENTS_PER_WORKER
    );

    let mut handles = Vec::new();
    for worker_id in 0..NUM_WORKERS {
        handles.push(task::spawn(async move {
            worker_task(worker_id, EVENTS_PER_WORKER).await
        }));
    }

    // Wait for all workers to complete
    for (i, handle) in handles.into_iter().enumerate() {
        handle.await??;
        println!("Worker {} completed", i);
    }

    println!("\nAll workers completed!\n");

    // Verify the results
    println!("Verifying results...");
    let mut verify_client = Client::connect("localhost:5432").await?;

    let result = verify_client.execute("SELECT * FROM events").await?;

    if let Some((_, rows)) = result.rows() {
        let total_expected = NUM_WORKERS * EVENTS_PER_WORKER;
        println!("Expected {} rows, found {} rows", total_expected, rows.len());
        assert_eq!(
            rows.len() as i32,
            total_expected,
            "Row count mismatch!"
        );
    }

    // Query events grouped by worker (demonstrates filtering)
    println!("\nEvents per worker:");
    for worker_id in 0..NUM_WORKERS {
        let sql = format!("SELECT * FROM events WHERE worker = {}", worker_id);
        let result = verify_client.execute(&sql).await?;
        if let Some((_, rows)) = result.rows() {
            println!("  Worker {}: {} events", worker_id, rows.len());
        }
    }

    // Clean up
    println!("\nCleaning up...");
    verify_client.execute("DROP TABLE events").await?;
    verify_client.close().await?;

    println!("\n=== Example Complete ===");
    Ok(())
}

/// Individual worker task that connects and inserts events.
async fn worker_task(worker_id: i32, num_events: i32) -> Result<()> {
    let mut client = Client::connect("localhost:5432").await?;

    for event_id in 0..num_events {
        // Each worker gets a unique range of IDs to avoid conflicts
        let global_id = worker_id * 1000 + event_id;
        let sql = format!(
            "INSERT INTO events VALUES ({}, 'event_{}', {})",
            global_id, global_id, worker_id
        );

        let result = client.execute(&sql).await?;
        assert_eq!(
            result.affected_count(),
            1,
            "Insert should affect 1 row"
        );
    }

    client.close().await?;
    Ok(())
}
