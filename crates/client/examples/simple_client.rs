//! Simple client example demonstrating basic database operations.
//!
//! This example shows how to:
//! - Connect to the database server
//! - Execute DDL (CREATE TABLE)
//! - Execute DML (INSERT)
//! - Execute queries (SELECT)
//! - Handle results
//! - Close connections gracefully
//!
//! Run the server first:
//!   cargo run --bin toydb-server -- --data-dir /tmp/example-db
//!
//! Then run this example:
//!   cargo run --example simple_client

use anyhow::Result;
use client::Client;

#[tokio::main]
async fn main() -> Result<()> {
    println!("=== Simple Client Example ===\n");

    // Connect to the database server
    println!("Connecting to localhost:5432...");
    let mut client = Client::connect("localhost:5432").await?;
    println!("Connected!\n");

    // Create a table
    println!("Creating table 'users'...");
    client
        .execute("CREATE TABLE users (id INT PRIMARY KEY, name TEXT, age INT)")
        .await?;
    println!("Table created.\n");

    // Insert some data
    println!("Inserting data...");
    let result = client
        .execute("INSERT INTO users VALUES (1, 'Alice', 30)")
        .await?;
    println!("Inserted {} row(s)", result.affected_count());

    let result = client
        .execute("INSERT INTO users VALUES (2, 'Bob', 25)")
        .await?;
    println!("Inserted {} row(s)\n", result.affected_count());

    // Query the data
    println!("Querying all users...");
    let result = client.execute("SELECT * FROM users").await?;

    if let Some((schema, rows)) = result.rows() {
        println!("Schema: {:?}", schema);
        println!("Found {} rows:", rows.len());
        for row in rows {
            println!("  {:?}", row.values);
        }
    }
    println!();

    // Query with a filter
    println!("Querying users where age > 26...");
    let result = client
        .execute("SELECT name, age FROM users WHERE age > 26")
        .await?;

    if let Some((schema, rows)) = result.rows() {
        println!("Schema: {:?}", schema);
        println!("Found {} rows:", rows.len());
        for row in rows {
            println!("  {:?}", row.values);
        }
    }
    println!();

    // Clean up
    println!("Dropping table...");
    client.execute("DROP TABLE users").await?;
    println!("Table dropped.\n");

    // Close the connection
    println!("Closing connection...");
    client.close().await?;
    println!("Connection closed.");

    println!("\n=== Example Complete ===");
    Ok(())
}
