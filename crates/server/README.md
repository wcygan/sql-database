# Server

TCP server for the toy SQL database. Accepts client connections and executes SQL statements remotely.

## Features

- **Concurrent connections**: Handles multiple clients simultaneously using tokio
- **Wire protocol**: Length-prefixed bincode serialization for requests/responses
- **Shared database**: Single `Arc<Database>` shared across all connection handlers
- **Graceful shutdown**: Responds to SIGINT/SIGTERM signals
- **Error handling**: Maps database errors to protocol error codes

## Usage

Start the server with default settings:

```bash
cargo run --bin toydb-server
```

Configure host, port, and storage:

```bash
cargo run --bin toydb-server -- \
  --host 0.0.0.0 \
  --port 5432 \
  --data-dir ./my_data \
  --buffer-pages 512
```

## Command-line Options

- `--host <HOST>`: Host address to bind to (default: 127.0.0.1)
- `--port <PORT>`: Port to listen on (default: 5432)
- `--data-dir <PATH>`: Directory for catalog, WAL, and table files (default: ./db_data)
- `--catalog-file <NAME>`: Catalog filename (default: catalog.json)
- `--wal-file <NAME>`: WAL filename (default: toydb.wal)
- `--buffer-pages <N>`: Buffer pool size in pages (default: 256)

## Architecture

```
Client → TcpStream → Protocol → Server → Database → Storage/WAL
```

Each client connection is handled in a separate tokio task, allowing concurrent execution.

## Testing

Run integration tests:

```bash
cargo test --package server
```

Tests include:
- Basic connectivity
- DDL operations (CREATE/DROP TABLE)
- DML operations (INSERT/SELECT)
- Error handling
- Concurrent connections
- Connection reuse
- Primary key enforcement
