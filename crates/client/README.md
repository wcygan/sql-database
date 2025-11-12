# Client

Async client library for connecting to the toy SQL database server.

## Features

- **Simple API**: Connect, execute, close
- **Async-first**: Built on tokio for non-blocking I/O
- **Type-safe**: Structured query results with schema and rows
- **Error handling**: Detailed error types for connection, protocol, and database errors

## Usage

```rust
use client::Client;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Connect to server
    let mut client = Client::connect("localhost:5432").await?;

    // Execute DDL
    client.execute("CREATE TABLE users (id INT, name TEXT)").await?;

    // Execute DML
    let result = client.execute("INSERT INTO users VALUES (1, 'Alice')").await?;
    println!("Inserted {} row(s)", result.affected_count());

    // Execute queries
    let result = client.execute("SELECT * FROM users").await?;
    if let Some((schema, rows)) = result.rows() {
        println!("Columns: {:?}", schema);
        println!("Rows: {}", rows.len());
    }

    // Close connection
    client.close().await?;
    Ok(())
}
```

## API

### `Client::connect(addr: &str)`

Connect to the database server at the given address.

### `Client::execute(&mut self, sql: &str)`

Execute a SQL statement and return the result. Returns:
- `QueryResult::Rows { schema, rows }` for SELECT queries
- `QueryResult::Count { affected }` for INSERT/UPDATE/DELETE
- `QueryResult::Empty` for DDL statements

### `Client::close(&mut self)`

Close the connection gracefully.

## Error Handling

The client uses a structured error type:

```rust
pub enum ClientError {
    Connection(io::Error),      // Network connection failed
    Protocol(io::Error),         // Framing or serialization error
    Database { code, message },  // Server-side error
}
```

Use error helper methods:
- `err.is_connection_error()` - Check if connection failed
- `err.is_protocol_error()` - Check if protocol error
- `err.is_database_error()` - Check if server error
- `err.error_code()` - Get ErrorCode for database errors

## Testing

Run tests (requires server crate):

```bash
cargo test --package client
```
