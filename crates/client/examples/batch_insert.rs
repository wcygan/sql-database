//! Batch insert example demonstrating efficient bulk data loading.
//!
//! This example shows how to:
//! - Efficiently insert large numbers of rows
//! - Measure insertion performance
//! - Use a single connection for bulk operations
//! - Track progress during long-running operations
//!
//! Run the server first:
//!   cargo run --bin toydb-server -- --data-dir /tmp/example-db
//!
//! Then run this example:
//!   cargo run --example batch_insert

use anyhow::Result;
use client::Client;
use std::time::Instant;

#[tokio::main]
async fn main() -> Result<()> {
    println!("=== Batch Insert Example ===\n");

    const BATCH_SIZE: usize = 1000;
    const REPORT_INTERVAL: usize = 100;

    println!("Connecting to localhost:5432...");
    let mut client = Client::connect("localhost:5432").await?;
    println!("Connected!\n");

    // Create table for bulk data
    println!("Creating table 'measurements'...");
    client
        .execute("CREATE TABLE measurements (id INT PRIMARY KEY, sensor_id INT, value INT, timestamp INT)")
        .await?;
    println!("Table created.\n");

    // Perform batch insert
    println!("Inserting {} rows...", BATCH_SIZE);
    println!("(Reporting progress every {} rows)\n", REPORT_INTERVAL);

    let start = Instant::now();
    let mut inserted = 0;

    for i in 0..BATCH_SIZE {
        let sensor_id = (i % 10) as i32; // 10 different sensors
        let value = (i * 7 % 100) as i32; // Some pseudo-random values
        let timestamp = 1000000 + i as i32; // Sequential timestamps

        let sql = format!(
            "INSERT INTO measurements VALUES ({}, {}, {}, {})",
            i, sensor_id, value, timestamp
        );

        let result = client.execute(&sql).await?;
        assert_eq!(
            result.affected_count(),
            1,
            "Each insert should affect 1 row"
        );

        inserted += 1;

        // Report progress
        if (i + 1) % REPORT_INTERVAL == 0 {
            let elapsed = start.elapsed();
            let rate = inserted as f64 / elapsed.as_secs_f64();
            println!(
                "  Progress: {}/{} rows ({:.1} rows/sec)",
                i + 1,
                BATCH_SIZE,
                rate
            );
        }
    }

    let elapsed = start.elapsed();
    let rate = BATCH_SIZE as f64 / elapsed.as_secs_f64();

    println!("\nBatch insert completed!");
    println!("  Total rows: {}", BATCH_SIZE);
    println!("  Total time: {:.2?}", elapsed);
    println!("  Average rate: {:.1} rows/sec", rate);
    println!();

    // Verify the data
    println!("Verifying data...");
    let result = client.execute("SELECT * FROM measurements").await?;

    if let Some((schema, rows)) = result.rows() {
        println!("  Schema: {:?}", schema);
        println!("  Row count: {}", rows.len());
        assert_eq!(rows.len(), BATCH_SIZE, "Row count mismatch!");
    }

    // Query aggregations (if supported, otherwise just count)
    println!("\nQuerying data by sensor...");
    for sensor_id in 0..3 {
        let sql = format!("SELECT * FROM measurements WHERE sensor_id = {}", sensor_id);
        let result = client.execute(&sql).await?;
        if let Some((_, rows)) = result.rows() {
            println!("  Sensor {}: {} measurements", sensor_id, rows.len());
        }
    }
    println!();

    // Clean up
    println!("Cleaning up...");
    client.execute("DROP TABLE measurements").await?;
    client.close().await?;

    println!("\n=== Example Complete ===");
    Ok(())
}
