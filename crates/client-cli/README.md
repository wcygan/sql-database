# Client CLI

Command-line interface for connecting to the toy SQL database server.

## Features

- **Execute mode**: Run a single query and exit
- **Interactive mode**: REPL interface with line editing and history
- **Pretty printing**: Formatted table output for query results
- **Connection configuration**: Specify host and port

## Usage

### Execute Mode

Run a single SQL statement and exit:

```bash
cargo run --bin toydb-client -- -e "SELECT * FROM users" --host localhost --port 5432
```

### Interactive Mode

Start an interactive REPL session:

```bash
cargo run --bin toydb-client -- --host localhost --port 5432
```

Example session:

```
Connected to localhost:5432

Type SQL statements or .quit to exit

> CREATE TABLE users (id INT, name TEXT);
Success
> INSERT INTO users VALUES (1, 'Alice');
1 row(s) affected
> SELECT * FROM users;
┌────┬───────┐
│ id │ name  │
├────┼───────┤
│ 1  │ Alice │
└────┴───────┘
> .quit
```

## Commands

### SQL Statements

Enter any valid SQL statement:
- DDL: `CREATE TABLE`, `DROP TABLE`, `CREATE INDEX`, `DROP INDEX`
- DML: `INSERT`, `SELECT`, `UPDATE`, `DELETE`
- Query: `EXPLAIN`, `EXPLAIN ANALYZE`

### Meta Commands

- `.help` - Show help message
- `.quit` or `.exit` - Exit the client (or press Ctrl+C or Ctrl+D)

## Command-line Options

- `--host <HOST>` - Host address to connect to (default: localhost)
- `--port <PORT>` - Port to connect to (default: 5432)
- `-e, --execute <SQL>` - Execute SQL and exit

## Requirements

The server must be running before starting the client:

```bash
# In one terminal
cargo run --bin toydb-server

# In another terminal
cargo run --bin toydb-client
```
