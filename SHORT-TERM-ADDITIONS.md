# Short-Term Additions

This document tracks immediate next steps to complete the client-server architecture. These items build directly on the async database conversion completed in the previous session.

## Client-Server Architecture

### TCP Server Implementation
**Status**: Not Started
**Complexity**: Medium
**Impact**: High
**Priority**: 1 (Core functionality)

Implement a TCP server that accepts client connections and executes SQL statements remotely.

**Implementation approach:**
- Create `crates/server` binary crate
- Use `tokio::net::TcpListener` to accept connections
- Spawn a task per connection to handle concurrent clients
- Read framed `ClientRequest` messages using `protocol::frame::read_message()`
- Execute SQL via `Database::execute().await`
- Convert `QueryResult` to `ServerResponse` and send back
- Handle errors gracefully (convert to `ServerResponse::Error`)
- Add graceful shutdown on SIGINT/SIGTERM
- Support command-line args: `--host`, `--port`, `--data-dir`, `--catalog-file`, `--wal-file`

**Key features:**
- Concurrent connection handling (one task per client)
- Shared `Arc<Database>` across all connection handlers
- Proper error handling and client disconnection
- Logging of connections and queries (optional)

**Related files:**
- Create `crates/server/src/main.rs` - TCP server entry point
- Create `crates/server/Cargo.toml` - dependencies: database, protocol, tokio, anyhow, clap
- `crates/protocol/src/lib.rs` - existing wire protocol
- `crates/database/src/lib.rs` - async Database API

**Example server structure:**
```rust
#[tokio::main]
async fn main() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:5432").await?;
    let db = Arc::new(Database::new(...).await?);

    loop {
        let (socket, addr) = listener.accept().await?;
        let db = db.clone();
        tokio::spawn(async move {
            handle_client(socket, db).await
        });
    }
}

async fn handle_client(mut socket: TcpStream, db: Arc<Database>) -> Result<()> {
    loop {
        let request = frame::read_message(&mut socket).await?;
        match request {
            ClientRequest::Execute { sql } => {
                let result = db.execute(&sql).await;
                let response = match result {
                    Ok(QueryResult::Rows { schema, rows }) =>
                        ServerResponse::Rows { schema, rows },
                    Ok(QueryResult::Count { affected }) =>
                        ServerResponse::Count { affected },
                    Ok(QueryResult::Empty) =>
                        ServerResponse::Empty,
                    Err(e) => ServerResponse::Error {
                        code: ErrorCode::ExecutionError,
                        message: e.to_string()
                    },
                };
                frame::write_message(&mut socket, &response).await?;
            }
            ClientRequest::Close => break,
        }
    }
    Ok(())
}
```

**Testing approach:**
- Unit tests: Protocol serialization roundtrips
- Integration tests: Connect, execute queries, verify results
- Concurrent client tests: Multiple connections simultaneously
- Error handling tests: Invalid SQL, connection drops

**Educational value**: Network programming, concurrent connection handling, request/response protocols

---

### Client Library Implementation
**Status**: Not Started
**Complexity**: Medium
**Impact**: High
**Priority**: 2 (Required for CLI)

Create a reusable client library that other programs can use to connect to the database server.

**Implementation approach:**
- Create `crates/client` library crate (not binary)
- Implement `Client` struct with connection state
- Provide `connect()` function returning `Client`
- Provide `execute()` method on `Client` returning results
- Handle connection pooling (optional, future enhancement)
- Support connection timeout configuration
- Implement automatic reconnection on transient failures (optional)

**Key features:**
- Simple, ergonomic API: `client.execute("SELECT * FROM users")`
- Automatic framing using `protocol::frame`
- Connection management (open, close, reuse)
- Error handling with helpful messages
- Async API matching server expectations

**Related files:**
- Create `crates/client/src/lib.rs` - Client struct and methods
- Create `crates/client/Cargo.toml` - dependencies: protocol, common, types, tokio, anyhow
- `crates/protocol/src/lib.rs` - wire protocol for requests/responses

**API design:**
```rust
pub struct Client {
    socket: TcpStream,
}

impl Client {
    pub async fn connect(addr: &str) -> Result<Self> {
        let socket = TcpStream::connect(addr).await?;
        Ok(Self { socket })
    }

    pub async fn execute(&mut self, sql: &str) -> Result<QueryResult> {
        let request = ClientRequest::Execute { sql: sql.to_string() };
        frame::write_message(&mut self.socket, &request).await?;

        let response: ServerResponse = frame::read_message(&mut self.socket).await?;
        match response {
            ServerResponse::Rows { schema, rows } =>
                Ok(QueryResult::Rows { schema, rows }),
            ServerResponse::Count { affected } =>
                Ok(QueryResult::Count { affected }),
            ServerResponse::Empty =>
                Ok(QueryResult::Empty),
            ServerResponse::Error { code, message } =>
                Err(anyhow!("{:?}: {}", code, message)),
        }
    }

    pub async fn close(&mut self) -> Result<()> {
        frame::write_message(&mut self.socket, &ClientRequest::Close).await?;
        Ok(())
    }
}
```

**Testing approach:**
- Unit tests: Client creation, request formatting
- Integration tests: Connect to real server, execute queries
- Error tests: Server unavailable, malformed responses
- Concurrent tests: Multiple clients to same server

**Educational value**: Client-server communication, API design, error propagation

---

### Client CLI Binary
**Status**: Not Started
**Complexity**: Low
**Impact**: Medium
**Priority**: 3 (User-facing tool)

Create a command-line client binary that uses the client library to interact with the database server.

**Implementation approach:**
- Create `crates/client-cli` binary crate
- Use `clap` for argument parsing
- Support `-e/--execute` flag for one-shot queries (like REPL)
- Support interactive mode (read-eval-print loop)
- Use `client` crate for all server communication
- Pretty-print query results using `common::pretty`
- Handle connection errors gracefully with helpful messages

**Key features:**
- Execute mode: `client-cli -e "SELECT * FROM users" --host localhost --port 5432`
- Interactive mode: Read queries from stdin, send to server, display results
- Connection configuration via CLI args
- Pretty output formatting matching REPL style
- Graceful error messages for connection failures

**Related files:**
- Create `crates/client-cli/src/main.rs` - CLI entry point
- Create `crates/client-cli/Cargo.toml` - dependencies: client, common, types, tokio, anyhow, clap
- `crates/client/src/lib.rs` - client library API

**Example usage:**
```bash
# One-shot query
cargo run --bin client-cli -- -e "SELECT * FROM users" --host localhost --port 5432

# Interactive mode
cargo run --bin client-cli -- --host localhost --port 5432
> SELECT * FROM users;
┌────┬───────┬─────┐
│ id │ name  │ age │
├────┼───────┼─────┤
│ 1  │ Alice │ 30  │
│ 2  │ Bob   │ 25  │
└────┴───────┴─────┘
> .quit
```

**CLI structure:**
```rust
#[derive(Parser)]
struct Args {
    #[arg(long, default_value = "localhost")]
    host: String,

    #[arg(long, default_value = "5432")]
    port: u16,

    #[arg(short = 'e', long)]
    execute: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let addr = format!("{}:{}", args.host, args.port);
    let mut client = Client::connect(&addr).await?;

    if let Some(sql) = args.execute {
        // Execute mode
        execute_and_print(&mut client, &sql).await?;
    } else {
        // Interactive mode
        interactive_loop(&mut client).await?;
    }

    client.close().await?;
    Ok(())
}
```

**Testing approach:**
- Manual testing: Start server, run client, verify results
- Integration tests: Automated client-server roundtrip tests
- Error handling: Server down, network issues

**Educational value**: Building user-facing CLI tools, client library usage

---

### Client-Server Integration Tests
**Status**: Not Started
**Complexity**: Medium
**Impact**: High
**Priority**: 4 (Quality assurance)

Add comprehensive integration tests for the entire client-server system.

**Implementation approach:**
- Create `crates/server/tests/integration.rs`
- Start server in background for each test (use `tokio::spawn`)
- Connect multiple clients concurrently
- Execute various SQL statements (DDL, DML, queries)
- Verify results match expected output
- Test error conditions (invalid SQL, connection drops)
- Test concurrent operations (multiple clients simultaneously)
- Add cleanup logic to stop server after tests

**Test scenarios:**
1. **Basic connectivity**: Client connects, executes simple query, disconnects
2. **DDL operations**: CREATE TABLE, DROP TABLE from remote client
3. **DML operations**: INSERT, UPDATE, DELETE via client
4. **Concurrent queries**: Multiple clients reading simultaneously
5. **Concurrent writes**: Multiple clients inserting/updating (test serialization)
6. **Error handling**: Invalid SQL returns proper error response
7. **Connection drop**: Client disconnects mid-query
8. **Large result sets**: Query returning thousands of rows
9. **Connection reuse**: Single client executes multiple queries
10. **Primary key enforcement**: Remote INSERT violating PK constraint

**Related files:**
- Create `crates/server/tests/integration.rs` - integration test suite
- Use `crates/client` for test client connections
- Use `crates/testsupport` helpers where applicable

**Test helper pattern:**
```rust
async fn with_test_server<F, Fut>(f: F) -> Result<()>
where
    F: FnOnce(String) -> Fut,
    Fut: Future<Output = Result<()>>,
{
    let temp_dir = tempfile::tempdir()?;
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;

    // Spawn server in background
    let db = Arc::new(Database::new(temp_dir.path(), "catalog.json", "test.wal", 10).await?);
    let server_task = tokio::spawn(async move {
        run_server(listener, db).await
    });

    // Run test function
    let result = f(addr.to_string()).await;

    // Cleanup
    server_task.abort();
    result
}

#[tokio::test]
async fn test_basic_query() {
    with_test_server(|addr| async move {
        let mut client = Client::connect(&addr).await?;

        // Create table
        client.execute("CREATE TABLE users (id INT, name TEXT)").await?;

        // Insert data
        let result = client.execute("INSERT INTO users VALUES (1, 'Alice')").await?;
        assert!(matches!(result, QueryResult::Count { affected: 1 }));

        // Query data
        let result = client.execute("SELECT * FROM users").await?;
        if let QueryResult::Rows { rows, .. } = result {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].values[1], Value::Text("Alice".to_string()));
        } else {
            panic!("Expected rows");
        }

        Ok(())
    }).await.unwrap();
}
```

**Testing approach:**
- Automated integration tests in CI
- Test all supported SQL operations remotely
- Stress test with many concurrent connections
- Verify behavior matches local REPL

**Educational value**: Integration testing, test infrastructure, concurrent testing

---

### Documentation and Examples
**Status**: Not Started
**Complexity**: Low
**Impact**: Medium
**Priority**: 5 (User experience)

Document the client-server architecture and provide usage examples.

**Implementation approach:**
- Add `README.md` to each new crate explaining its purpose
- Create `docs/client-server.md` with architecture overview
- Add examples directory with sample client programs
- Document wire protocol format
- Add troubleshooting guide for common issues

**Documentation sections:**
1. **Architecture overview**: Diagram showing client → protocol → server → database
2. **Getting started**: How to start server and connect with client
3. **Wire protocol**: Frame format, message types, error codes
4. **Client library API**: All public methods with examples
5. **Server configuration**: Command-line args, environment variables
6. **Security considerations**: No authentication (yet), network exposure
7. **Performance tuning**: Connection pooling, query batching (future)

**Example programs to include:**
- `examples/simple_client.rs` - Basic connect and query
- `examples/concurrent_clients.rs` - Multiple clients simultaneously
- `examples/batch_insert.rs` - Efficient bulk loading
- `examples/error_handling.rs` - Graceful error recovery

**Related files:**
- Create `docs/client-server.md`
- Create `crates/server/README.md`
- Create `crates/client/README.md`
- Create `crates/client-cli/README.md`
- Create `examples/` directory in workspace root

**Educational value**: Technical writing, API documentation, system design communication

---

## Implementation Order

Recommended sequence (dependencies noted):

1. **Server** (depends on: protocol ✅, database ✅)
   - Implement TCP server accepting connections
   - Add basic request/response handling
   - Test with manual telnet/netcat

2. **Client library** (depends on: protocol ✅)
   - Implement Client struct and connect/execute methods
   - Test against running server

3. **Client CLI** (depends on: client library)
   - Build command-line interface
   - Add pretty printing and error handling

4. **Integration tests** (depends on: server, client)
   - Add comprehensive test suite
   - Test all SQL operations remotely
   - Test concurrent access

5. **Documentation** (depends on: all above)
   - Write guides and examples
   - Add diagrams and troubleshooting

## Success Criteria

The client-server architecture is complete when:

- ✅ Server accepts multiple concurrent TCP connections
- ✅ Clients can execute DDL, DML, and queries remotely
- ✅ Results are correctly serialized and returned
- ✅ Errors are properly handled and reported
- ✅ All integration tests pass
- ✅ Client CLI provides good UX (like `psql`)
- ✅ Documentation explains architecture clearly

## Future Enhancements (Beyond Short-Term)

These items are not in critical path but would improve the system:

- **Connection pooling**: Reuse connections instead of creating new ones
- **Authentication**: Username/password or token-based auth
- **TLS encryption**: Secure communication over network
- **Query cancellation**: Allow clients to cancel long-running queries
- **Prepared statements**: Parse once, execute many times with parameters
- **Streaming results**: Send large result sets incrementally
- **Connection limits**: Max concurrent connections configuration
- **Rate limiting**: Prevent abuse from single client

## Notes

- All new crates should follow workspace dependency patterns (use `workspace = true`)
- Server and client use existing `protocol` crate (no changes needed)
- Database async API is ready for multi-threaded server use
- Focus on correctness first, performance second
- Keep educational mission in mind: code should be readable and well-commented

This roadmap builds directly on the async conversion work and completes the client-server transformation.
