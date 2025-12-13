# Justfile for sql-database project
# https://github.com/casey/just

# Default data directory for development
data_dir := "./db_data"

# List available commands
default:
    @just --list

# Build all crates
build:
    cargo build

# Build release binaries
build-release:
    cargo build --release

# Run all tests
test:
    cargo test

# Run tests with coverage report
coverage:
    ./scripts/coverage.sh

# Format code
fmt:
    cargo fmt

# Check formatting without modifying files
fmt-check:
    cargo fmt -- --check

# Run clippy lints
clippy:
    cargo clippy --all-targets --all-features

# Run clippy with warnings as errors
clippy-strict:
    cargo clippy --all-targets --all-features -- -D warnings

# Check code without building
check:
    cargo check

# Start the interactive REPL
repl *args:
    cargo run --package repl -- {{args}}

# Start REPL with custom data directory
repl-dev dir=data_dir:
    cargo run --package repl -- --data-dir {{dir}}

# Execute SQL and exit
repl-exec sql:
    cargo run --package repl -- -e "{{sql}}"

# View WAL contents
wal-viewer path *args:
    cargo run --package wal-viewer -- {{path}} {{args}}

# View WAL contents in JSON format
wal-viewer-json path:
    cargo run --package wal-viewer -- {{path}} --format json

# View WAL for default data directory
wal-view:
    cargo run --package wal-viewer -- {{data_dir}}/toydb.wal

# Clean build artifacts
clean:
    cargo clean

# Clean and remove development database
clean-all: clean
    rm -rf {{data_dir}}

# Reset development database (remove data directory)
reset-db:
    rm -rf {{data_dir}}

# Full validation: format, clippy, test
validate: fmt clippy test

# CI-style validation: format check, clippy strict, test
ci: fmt-check clippy-strict test

# Development workflow: reset DB and start REPL
dev: reset-db repl-dev

# ============================================================================
# Server Commands
# ============================================================================

# Start the TCP server (standalone mode, no Raft)
server *args:
    cargo run --release --package server -- {{args}}

# Start server with custom data directory
server-dev dir=data_dir:
    cargo run --release --package server -- --data-dir {{dir}}

# Connect to server with client CLI
client port="5432":
    cargo run --release --package client-cli -- --port {{port}}

# ============================================================================
# Raft Cluster Commands
# ============================================================================

# Directories for cluster nodes
node1_dir := "./node1"
node2_dir := "./node2"
node3_dir := "./node3"

# Start single-node Raft server (simplest Raft mode)
raft-single:
    cargo run --release --package server -- \
        --node-id 1 \
        --data-dir {{node1_dir}} \
        --port 5001

# Start single-node Raft server with persistent storage
raft-single-persistent:
    cargo run --release --package server -- \
        --node-id 1 \
        --persistent \
        --data-dir {{node1_dir}} \
        --port 5001

# Start Node 1 of 3-node cluster (leader bootstrap)
raft-node1:
    cargo run --release --package server -- \
        --node-id 1 \
        --raft-addr 127.0.0.1:6001 \
        --peer 2,127.0.0.1:6002 \
        --peer 3,127.0.0.1:6003 \
        --data-dir {{node1_dir}} \
        --port 5001

# Start Node 2 of 3-node cluster
raft-node2:
    cargo run --release --package server -- \
        --node-id 2 \
        --raft-addr 127.0.0.1:6002 \
        --peer 1,127.0.0.1:6001 \
        --peer 3,127.0.0.1:6003 \
        --data-dir {{node2_dir}} \
        --port 5002

# Start Node 3 of 3-node cluster
raft-node3:
    cargo run --release --package server -- \
        --node-id 3 \
        --raft-addr 127.0.0.1:6003 \
        --peer 1,127.0.0.1:6001 \
        --peer 2,127.0.0.1:6002 \
        --data-dir {{node3_dir}} \
        --port 5003

# Start Node 1 with persistent storage
raft-node1-persistent:
    cargo run --release --package server -- \
        --node-id 1 \
        --raft-addr 127.0.0.1:6001 \
        --peer 2,127.0.0.1:6002 \
        --peer 3,127.0.0.1:6003 \
        --data-dir {{node1_dir}} \
        --port 5001 \
        --persistent

# Start Node 2 with persistent storage
raft-node2-persistent:
    cargo run --release --package server -- \
        --node-id 2 \
        --raft-addr 127.0.0.1:6002 \
        --peer 1,127.0.0.1:6001 \
        --peer 3,127.0.0.1:6003 \
        --data-dir {{node2_dir}} \
        --port 5002 \
        --persistent

# Start Node 3 with persistent storage
raft-node3-persistent:
    cargo run --release --package server -- \
        --node-id 3 \
        --raft-addr 127.0.0.1:6003 \
        --peer 1,127.0.0.1:6001 \
        --peer 2,127.0.0.1:6002 \
        --data-dir {{node3_dir}} \
        --port 5003 \
        --persistent

# Connect client to Node 1 (leader)
client-node1:
    cargo run --release --package client-cli -- --port 5001

# Connect client to Node 2
client-node2:
    cargo run --release --package client-cli -- --port 5002

# Connect client to Node 3
client-node3:
    cargo run --release --package client-cli -- --port 5003

# Clean up cluster data directories
raft-clean:
    rm -rf {{node1_dir}} {{node2_dir}} {{node3_dir}}

# Reset cluster: clean data and rebuild
raft-reset: raft-clean build-release
