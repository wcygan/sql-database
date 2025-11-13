# Client Examples

Example programs demonstrating the client library API for connecting to the toy SQL database.

## Prerequisites

Start the database server before running any examples:

```bash
cargo run --bin toydb-server -- --data-dir /tmp/example-db
```

The server will listen on `localhost:5432` by default.

## Examples

### simple_client.rs

Basic client usage: connect, create table, insert, query, clean up.

```bash
cargo run --package client --example simple_client
```

**Demonstrates:**
- Connecting to the server
- Creating tables with primary keys
- Inserting rows
- Querying data with filters
- Handling query results
- Closing connections gracefully

**Duration:** ~1 second

---

### concurrent_clients.rs

Multiple clients executing operations simultaneously.

```bash
cargo run --package client --example concurrent_clients
```

**Demonstrates:**
- Spawning multiple concurrent clients
- Independent task execution
- Concurrent inserts without conflicts
- Data consistency verification
- Shared database state across connections

**Duration:** ~2-3 seconds

---

### batch_insert.rs

Efficient bulk data loading with performance metrics.

```bash
cargo run --package client --example batch_insert
```

**Demonstrates:**
- Inserting large numbers of rows (1000+)
- Measuring insertion performance
- Progress reporting during long operations
- Using a single connection for bulk work
- Verifying data integrity after bulk load

**Duration:** ~5-30 seconds (depending on system)

---

### error_handling.rs

Graceful recovery from various error conditions.

```bash
cargo run --package client --example error_handling
```

**Demonstrates:**
- Connection errors (server unreachable)
- SQL syntax errors (invalid statements)
- Constraint violations (duplicate primary keys)
- Table not found errors (missing catalog entries)
- Error type classification
- Recovering and continuing after errors

**Duration:** ~2 seconds

## Automatic Cleanup

All examples clean up their tables before exiting. The server automatically manages database files in the `--data-dir` specified when starting the server.

To completely reset the database between runs:

```bash
rm -rf /tmp/example-db
cargo run --bin toydb-server -- --data-dir /tmp/example-db
```

## Running All Examples

Run all examples in sequence:

```bash
cargo run --package client --example simple_client && \
cargo run --package client --example concurrent_clients && \
cargo run --package client --example batch_insert && \
cargo run --package client --example error_handling
```

## Customizing Examples

All examples connect to `localhost:5432` by default. To connect to a different server, modify the connection string:

```rust
let mut client = Client::connect("hostname:port").await?;
```

## Common Issues

**"Connection refused"**
- Ensure the server is running: `cargo run --bin toydb-server`
- Check that the server is listening on the expected port

**"Address already in use"**
- Another server instance is running
- Kill the existing process or use a different port

**"Primary key constraint violation"**
- Table data persists between runs
- Clean the data directory: `rm -rf /tmp/example-db`
