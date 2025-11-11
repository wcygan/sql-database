# Executor Crate

Volcano-style iterator-based query executor for the SQL database.

## Purpose

Executes physical plans from the planner using a pull-based iterator model. Coordinates with storage, buffer pool, and WAL for data access and durability.

## Quick Start

```rust
use executor::{execute_query, ExecutionContext};
use planner::PhysicalPlan;

let mut ctx = ExecutionContext::new(&catalog, &mut pager, &mut wal, data_dir);
let results = execute_query(plan, &mut ctx)?;
```

## Operators

- **SeqScan** - Sequential table scan
- **IndexScan** - Index-based scan (stub)
- **Filter** - Predicate evaluation
- **Project** - Column selection
- **Insert** - Row insertion with WAL
- **Update** - Row updates (partial)
- **Delete** - Row deletion (partial)

## Commands

```bash
cargo check -p executor      # Validate
cargo test -p executor        # Run tests
cargo clippy -p executor      # Lints
```
