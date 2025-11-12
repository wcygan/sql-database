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
