# Client-Server Architecture

This document describes the client-server architecture of the toy SQL database, including quickstart instructions, design decisions, and architectural details.

## Quick Start

### Running the Server

Start the database server on the default port (5432):

```bash
cargo run --bin toydb-server
```

With custom configuration:

```bash
cargo run --bin toydb-server -- \
  --host 0.0.0.0 \
  --port 5432 \
  --data-dir ./db_data \
  --buffer-pages 512
```

You should see output like:
```
Server listening on 127.0.0.1:5432
Data directory: "./db_data"
Buffer pool: 256 pages

Press Ctrl+C to shut down
```

### Connecting with the Client

**Execute Mode** (one-shot query):

```bash
cargo run --bin toydb-client -- \
  -e "CREATE TABLE users (id INT, name TEXT)" \
  --host localhost \
  --port 5432
```

**Interactive Mode** (REPL):

```bash
cargo run --bin toydb-client
```

Example session:
```
Connected to localhost:5432

Type SQL statements or .quit to exit

> CREATE TABLE users (id INT, name TEXT);
Success

> INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob');
2 row(s) affected

> SELECT * FROM users;
┌────┬───────┐
│ id │ name  │
├────┼───────┤
│ 1  │ Alice │
│ 2  │ Bob   │
└────┴───────┘

> .quit
```

### Testing Locally

1. **Start the server in one terminal:**
   ```bash
   cargo run --bin toydb-server
   ```

2. **Connect with the client in another terminal:**
   ```bash
   cargo run --bin toydb-client
   ```

3. **Run integration tests:**
   ```bash
   # Test client library
   cargo test --package client

   # Test server
   cargo test --package server
   ```

4. **Run multiple concurrent clients:**
   ```bash
   # Terminal 1
   cargo run --bin toydb-client

   # Terminal 2
   cargo run --bin toydb-client

   # Terminal 3
   cargo run --bin toydb-client
   ```

All clients share the same database state through the server.

---

## Architecture Overview

### High-Level Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Client Applications                       │
│  ┌───────────────┐  ┌───────────────┐  ┌──────────────┐   │
│  │  client-cli   │  │  Custom Apps  │  │  Integration │   │
│  │    (REPL)     │  │  using client │  │    Tests     │   │
│  └───────┬───────┘  └───────┬───────┘  └──────┬───────┘   │
└──────────┼──────────────────┼──────────────────┼───────────┘
           │                  │                  │
           v                  v                  v
┌─────────────────────────────────────────────────────────────┐
│                    Client Library (crates/client)            │
│  • Async API: connect(), execute(), close()                 │
│  • Error handling: ClientError enum                         │
│  • Result mapping: ServerResponse → QueryResult             │
└──────────────────────────┬──────────────────────────────────┘
                           │
                           │ TCP Connection
                           │
┌──────────────────────────┴──────────────────────────────────┐
│                 Wire Protocol (crates/protocol)              │
│  • Framing: [u32 length][bincode payload]                   │
│  • Messages: ClientRequest, ServerResponse                  │
│  • Error codes: ParseError, ExecutionError, etc.            │
└──────────────────────────┬──────────────────────────────────┘
                           │
                           │ Async I/O (tokio)
                           │
┌──────────────────────────┴──────────────────────────────────┐
│                    Server (crates/server)                    │
│  • TCP listener: Accept connections on 127.0.0.1:5432       │
│  • Connection handler: One task per client                  │
│  • Request logging: [addr] SQL: <query>                     │
│  • Response logging: [addr] Completed in Xms: <result>      │
│  • Shared database: Arc<Database>                           │
└──────────────────────────┬──────────────────────────────────┘
                           │
                           v
┌─────────────────────────────────────────────────────────────┐
│                  Database (crates/database)                  │
│  • Async API: execute(sql) → QueryResult                    │
│  • Shared state: Arc<RwLock<Catalog>>, Arc<Mutex<Pager>>    │
│  • Components: Parser, Planner, Executor, Storage, WAL      │
└─────────────────────────────────────────────────────────────┘
```

### Component Responsibilities

#### Client Library (`crates/client`)

**Purpose**: Reusable async library for connecting to the database server.

**Key Types:**
- `Client`: Main struct with connection state
- `QueryResult`: Typed results (Rows, Count, Empty)
- `ClientError`: Structured errors (Connection, Protocol, Database)

**API:**
```rust
pub struct Client {
    socket: TcpStream,
}

impl Client {
    pub async fn connect(addr: &str) -> Result<Self>;
    pub async fn execute(&mut self, sql: &str) -> Result<QueryResult>;
    pub async fn close(&mut self) -> Result<()>;
}
```

**Responsibilities:**
- Establish TCP connections to server
- Frame requests using wire protocol
- Convert server responses to typed results
- Map server errors to client error types
- Handle connection lifecycle

#### Client CLI (`crates/client-cli`)

**Purpose**: User-facing command-line tool for interactive and scripted database access.

**Features:**
- Execute mode: `toydb-client -e "SELECT * FROM users"`
- Interactive mode: REPL with rustyline for history/editing
- Pretty-printed results using `common::pretty`
- Meta commands: `.help`, `.quit`

**Implementation:**
- Uses `client` library for all server communication
- No local database logic (pure remote client)
- Simple, focused on UX

#### Server (`crates/server`)

**Purpose**: TCP server that accepts client connections and executes SQL remotely.

**Key Features:**
- **Concurrent connections**: One `tokio::spawn` task per client
- **Shared database**: Single `Arc<Database>` across all handlers
- **Request/response logging**: Client address, SQL, timing, results
- **Graceful shutdown**: Responds to SIGINT/SIGTERM

**Connection Handling:**
```rust
async fn handle_client(socket: TcpStream, db: Arc<Database>) -> Result<()> {
    let client_addr = socket.peer_addr()?.to_string();

    loop {
        let request = read_message_async(&mut socket).await?;

        match request {
            ClientRequest::Execute { sql } => {
                log_request(&client_addr, &sql);
                let start = Instant::now();

                let result = db.execute(&sql).await;
                let response = convert_result(result);

                log_response(&client_addr, start.elapsed(), &response);
                write_message_async(&mut socket, &response).await?;
            }
            ClientRequest::Close => break,
        }
    }
}
```

**Logging Format:**
```
[127.0.0.1:54321] SQL: CREATE TABLE users (id INT, name TEXT)
[127.0.0.1:54321] Completed in 1.234ms: DDL success

[127.0.0.1:54321] SQL: INSERT INTO users VALUES (1, 'Alice')
[127.0.0.1:54321] Completed in 567μs: 1 affected

[127.0.0.1:54321] SQL: SELECT * FROM users
[127.0.0.1:54321] Completed in 234μs: 1 rows
```

#### Wire Protocol (`crates/protocol`)

**Purpose**: Define the binary protocol for client-server communication.

**Message Types:**
```rust
pub enum ClientRequest {
    Execute { sql: String },
    Close,
}

pub enum ServerResponse {
    Rows { schema: Vec<String>, rows: Vec<Row> },
    Count { affected: u64 },
    Empty,
    Error { code: ErrorCode, message: String },
}

pub enum ErrorCode {
    ParseError,
    PlanError,
    ExecutionError,
    CatalogError,
    StorageError,
    WalError,
    ConstraintViolation,
    IoError,
    Unknown,
}
```

**Framing Format:**
```
┌────────────────┬────────────────────────────────┐
│ Length (4B LE) │ Bincode-encoded payload        │
│ u32            │ Serialized message             │
└────────────────┴────────────────────────────────┘

Max frame size: 64 MB
Encoding: bincode (deterministic binary serialization)
```

**Async API:**
```rust
pub async fn write_message_async<W, T>(writer: &mut W, message: &T) -> io::Result<()>
where
    W: AsyncWriteExt + Unpin,
    T: Serialize;

pub async fn read_message_async<R, T>(reader: &mut R) -> io::Result<T>
where
    R: AsyncReadExt + Unpin,
    T: for<'de> Deserialize<'de>;
```

---

## Key Design Decisions

### 1. TCP with Custom Protocol (Not PostgreSQL Wire Protocol)

**Decision**: Use direct TCP with a simple custom protocol instead of PostgreSQL wire protocol.

**Rationale:**
- **Educational clarity**: Easier to understand than PostgreSQL's complex protocol
- **Simplicity**: Custom protocol is ~150 lines vs. thousands for PostgreSQL
- **Full control**: Can evolve protocol to match database features exactly
- **No authentication overhead**: Simplified for learning purposes

**Trade-offs:**
- Not compatible with `psql` or existing PostgreSQL tools
- Custom client required
- No connection pooling protocol (e.g., PgBouncer)

### 2. Bincode Serialization

**Decision**: Use bincode for message serialization.

**Rationale:**
- **Zero-copy deserialization**: Efficient binary format
- **Type safety**: Serde ensures correct serialization/deserialization
- **Small messages**: Binary encoding is compact
- **Rust-native**: First-class serde support

**Trade-offs:**
- Not human-readable (vs. JSON or text protocols)
- Rust-specific (clients in other languages would need bincode libraries)
- Breaking changes require protocol version management

### 3. Async-First with Tokio

**Decision**: All networking code is async using tokio.

**Rationale:**
- **Concurrency**: Handle many clients with few threads
- **Shared database**: `Arc<Database>` works naturally with async
- **Scalability**: Non-blocking I/O allows efficient connection handling
- **Ecosystem**: Tokio is the standard async runtime in Rust

**Implementation:**
```rust
// Server: One task per connection
loop {
    let (socket, addr) = listener.accept().await?;
    let db = db.clone(); // Arc clone is cheap

    tokio::spawn(async move {
        handle_client(socket, db).await
    });
}

// Client: Async API matches server
let mut client = Client::connect("localhost:5432").await?;
let result = client.execute("SELECT * FROM users").await?;
```

### 4. Shared Database with Arc

**Decision**: Single `Arc<Database>` shared across all connection handlers.

**Rationale:**
- **Consistency**: All clients see the same database state
- **Simplicity**: No database replication or sharding complexity
- **Correctness**: Database already has proper locking (RwLock for catalog, Mutex for WAL/pager)

**Lock Hierarchy:**
```rust
pub struct Database {
    catalog: Arc<RwLock<Catalog>>,  // Read-heavy, many readers
    pager: Arc<Mutex<FilePager>>,   // Write-heavy, exclusive access
    wal: Arc<Mutex<Wal>>,           // Write-heavy, exclusive access
}
```

### 5. Request/Response Pattern (No Streaming)

**Decision**: Simple request/response model (one SQL → one result).

**Rationale:**
- **Simplicity**: Easy to understand and implement
- **Atomic operations**: Each request is independent
- **Error handling**: Clear error boundaries per request

**Trade-offs:**
- No result streaming for large queries (all rows buffered)
- No query cancellation mid-execution
- No prepared statements with bind parameters

**Future Enhancement**: Could add streaming with chunked responses.

### 6. Logging for Observability

**Decision**: Log all requests and responses with timing.

**Rationale:**
- **Debugging**: Track query execution and performance
- **Performance analysis**: Identify slow queries
- **Client tracking**: Know which clients are active
- **Educational**: See what's happening in the system

**Format:**
```
[client_addr] SQL: <query>
[client_addr] Completed in <duration>: <result>
```

---

## Wire Protocol Details

### Connection Lifecycle

```
Client                           Server
  │                                │
  ├─── TCP Connect ───────────────>│
  │                                │
  ├─── ClientRequest::Execute ───>│
  │                                │
  │<─── ServerResponse::Rows ──────┤
  │                                │
  ├─── ClientRequest::Execute ───>│
  │                                │
  │<─── ServerResponse::Error ─────┤
  │                                │
  ├─── ClientRequest::Close ──────>│
  │                                │
  │<─── Connection closed ─────────┤
```

### Request Format

```rust
// ClientRequest::Execute
{
    "Execute": {
        "sql": "SELECT * FROM users"
    }
}

// Serialized as:
[0x12, 0x00, 0x00, 0x00]  // Length: 18 bytes
[bincode payload...]       // Execute variant + sql string
```

### Response Format

```rust
// ServerResponse::Rows
{
    "Rows": {
        "schema": ["id", "name"],
        "rows": [
            { "values": [Int(1), Text("Alice")] },
            { "values": [Int(2), Text("Bob")] }
        ]
    }
}

// ServerResponse::Error
{
    "Error": {
        "code": "ExecutionError",
        "message": "table not found: users"
    }
}
```

### Error Handling

**Network Errors**: Connection drops, timeouts
- Client: Returns `ClientError::Connection`
- Server: Logs error, closes connection

**Protocol Errors**: Invalid frames, deserialization failures
- Client: Returns `ClientError::Protocol`
- Server: Sends `ServerResponse::Error` with `IoError` code

**Database Errors**: SQL errors, constraint violations
- Client: Returns `ClientError::Database { code, message }`
- Server: Maps `DbError` → `ErrorCode`, sends `ServerResponse::Error`

---

## Testing Strategy

### Unit Tests

**Client Library** (`crates/client/src/lib.rs`):
- `QueryResult` helper methods
- Error type conversions

**Client Error** (`crates/client/src/error.rs`):
- Error variant detection
- Error code extraction
- Display formatting

**Server Error Mapping** (`crates/server/src/error.rs`):
- `DbError` → `ErrorCode` conversion for all variants

### Integration Tests

**Client Integration** (`crates/client/tests/integration.rs`):
- Test server helper: `with_test_server()`
- Connection lifecycle: connect, execute, close
- DDL operations: CREATE/DROP TABLE/INDEX
- DML operations: INSERT, SELECT, UPDATE, DELETE
- Error handling: Invalid SQL, missing tables
- Connection reuse: Multiple queries on same connection

**Server Integration** (`crates/server/tests/integration.rs`):
- Basic connectivity
- Concurrent connections (5 clients simultaneously)
- Primary key enforcement (documents expected behavior)
- Large result sets
- Connection drops

### Manual Testing

1. **Start server with logging:**
   ```bash
   cargo run --bin toydb-server
   ```

2. **Connect with client:**
   ```bash
   cargo run --bin toydb-client
   ```

3. **Execute various SQL statements:**
   - DDL: `CREATE TABLE`, `CREATE INDEX`
   - DML: `INSERT`, `UPDATE`, `DELETE`
   - Queries: `SELECT` with filters, joins

4. **Observe server logs:**
   - Request logging: SQL statements
   - Response logging: Timing and results
   - Client connection/disconnection

---

## Performance Characteristics

### Concurrency Model

- **Server**: Task-per-connection (tokio green threads)
- **Database**: Single shared instance with internal locking
- **Bottlenecks**:
  - Catalog writes (exclusive lock)
  - WAL appends (exclusive lock)
  - Pager access (exclusive lock for writes)

### Latency

Typical request latencies (local development):
- **DDL** (CREATE TABLE): 1-2ms
- **INSERT**: 0.5-1ms
- **SELECT** (small result): 0.2-0.5ms
- **SELECT** (100 rows): 2-5ms

Factors:
- Network overhead: Minimal for localhost (~0.1ms)
- Serialization: Bincode is very fast (~0.05ms)
- Database execution: Depends on query complexity

### Scalability Limits

**Current Limits:**
- Max frame size: 64 MB (prevents memory exhaustion)
- Concurrent connections: Limited by OS file descriptors (~1024 default)
- Query result size: All rows buffered in memory

**Production Considerations:**
- Would need connection pooling
- Would need result streaming for large queries
- Would need query timeout and cancellation
- Would need authentication and authorization

---

## Future Enhancements

### Near-Term

1. **Connection pooling**: Reuse connections instead of creating new ones
2. **Prepared statements**: Parse once, execute many times with parameters
3. **Query cancellation**: Allow clients to cancel long-running queries
4. **Batch operations**: Execute multiple statements in one request

### Long-Term

1. **Authentication**: Username/password or token-based auth
2. **TLS encryption**: Secure communication over network
3. **Streaming results**: Send large result sets incrementally
4. **Asynchronous replication**: Read replicas for scalability
5. **Connection limits**: Max concurrent connections configuration
6. **Rate limiting**: Prevent abuse from single client
7. **Query timeout**: Configurable per-query timeout
8. **Connection state**: Track session variables, transactions

---

## Comparison with PostgreSQL

| Feature | Toy Database | PostgreSQL |
|---------|--------------|------------|
| **Protocol** | Custom bincode | PGSQL wire protocol |
| **Serialization** | Binary (bincode) | Binary + text |
| **Authentication** | None | MD5, SCRAM, etc. |
| **Encryption** | None | TLS support |
| **Prepared statements** | No | Yes |
| **Transactions** | No | Full ACID |
| **Cursors** | No | Yes (server-side) |
| **Async/Pipelining** | Request/response | Pipeline mode |
| **Result streaming** | No (buffered) | Yes (cursor-based) |
| **Connection pooling** | No | PgBouncer, etc. |
| **Authentication** | None | Many methods |

---

## Summary

The client-server architecture implements a minimal but functional remote database system using:

- **Simple wire protocol**: Length-prefixed bincode messages
- **Async I/O**: Tokio for concurrent connection handling
- **Shared database**: Arc-wrapped Database with internal locking
- **Request/response pattern**: One SQL statement → one result
- **Comprehensive logging**: Track all requests, responses, and timing

The design prioritizes **educational clarity** and **correctness** over production features like authentication, encryption, and streaming results. It demonstrates core client-server patterns while remaining simple enough to understand and extend.

For production use, this architecture would need:
- Authentication and authorization
- TLS encryption
- Connection pooling
- Query cancellation
- Result streaming
- Rate limiting
- Monitoring and metrics
