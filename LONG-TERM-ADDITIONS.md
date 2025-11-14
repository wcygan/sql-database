# Long-Term Additions

This document tracks substantial improvements that would enhance the educational SQL database beyond its current feature set. These are organized by complexity and impact.

## Index & Constraint Improvements

### Persist Primary Key Indexes
**Status**: Not Started
**Complexity**: Medium
**Impact**: High

Currently, PK indexes are rebuilt by scanning heap files on first table access. Persisting them would improve startup time for large tables.

**Implementation approach:**
- Design on-disk B+Tree structure for PK → RecordId mapping
- Store index files alongside heap files (e.g., `users.pk_idx`)
- Load index into memory on first access instead of scanning
- Update index file on INSERT/DELETE operations
- Validate index consistency during table open (or add REINDEX command)

**Related files:**
- `crates/executor/src/pk_index.rs` - extend for persistence
- `crates/storage/` - add B+Tree implementation
- `crates/executor/src/lib.rs:857-901` - replace scan with load

**Educational value**: Teaches index persistence, B+Tree structures, WAL-based crash recovery for indexes

---

### Secondary Indexes (Non-Unique)
**Status**: Not Started
**Complexity**: High
**Impact**: High

Support `CREATE INDEX idx_name ON table (column)` for non-unique columns to accelerate WHERE clause evaluation.

**Implementation approach:**
- Extend parser to recognize CREATE/DROP INDEX statements
- Store index metadata in catalog alongside table metadata
- Implement B+Tree index structure mapping column value → list of RecordIds
- Modify planner to choose IndexScan over SeqScan when beneficial
- Update indexes on INSERT/UPDATE/DELETE operations

**Related files:**
- `crates/parser/src/ast.rs` - add CreateIndex/DropIndex statements
- `crates/catalog/src/lib.rs` - add IndexMeta persistence
- `crates/executor/src/scan.rs:65-160` - complete IndexScanExec stub
- `crates/planner/src/lib.rs` - add cost-based index selection

**Educational value**: Cost-based query optimization, index selection heuristics, covering indexes

---

### UNIQUE Constraints (Non-Primary)
**Status**: Not Started
**Complexity**: Medium
**Impact**: Medium

Support `UNIQUE (column)` constraints separate from PRIMARY KEY, allowing multiple unique constraints per table.

**Implementation approach:**
- Extend parser to recognize UNIQUE table constraints
- Store unique constraint metadata in catalog (similar to primary_key)
- Create separate UniqueIndex instances in ExecutionContext
- Validate uniqueness on INSERT/UPDATE for all unique constraints
- Handle NULL values (NULLs are always unique in SQL standard)

**Related files:**
- `crates/parser/src/lib.rs` - extend constraint parsing
- `crates/catalog/src/lib.rs` - add `unique_constraints: Vec<Vec<ColumnId>>`
- `crates/executor/src/dml.rs` - check all unique indexes

**Educational value**: Multiple constraint handling, NULL semantics in uniqueness

---

### Foreign Key Constraints
**Status**: Not Started
**Complexity**: Very High
**Impact**: High

Support `FOREIGN KEY (col) REFERENCES other_table(col)` to enforce referential integrity between tables.

**Implementation approach:**
- Extend parser for FOREIGN KEY constraint syntax
- Store FK metadata in catalog (referencing table, columns, on delete/update actions)
- Validate referenced table/columns exist during CREATE TABLE
- On INSERT/UPDATE: verify referenced key exists in parent table
- On DELETE/UPDATE parent: enforce CASCADE/RESTRICT/SET NULL actions
- Detect circular FK references and reject

**Related files:**
- `crates/parser/src/ast.rs` - add ForeignKey constraint variant
- `crates/catalog/src/lib.rs` - add FK metadata to TableMeta
- `crates/executor/src/dml.rs` - add FK validation to all DML ops
- `crates/planner/` - handle multi-table FK validation

**Educational value**: Referential integrity, cascade actions, constraint dependency graphs

---

## Query Features

### Aggregate Functions (COUNT, SUM, AVG, MIN, MAX)
**Status**: Not Started
**Complexity**: Medium
**Impact**: High

Support `SELECT COUNT(*), SUM(price) FROM orders GROUP BY customer_id`.

**Implementation approach:**
- Extend expr crate with AggregateExpr enum (Count/Sum/Avg/Min/Max)
- Add AggregateExec operator implementing hash-based aggregation
- Extend parser to recognize aggregate function calls
- Implement GROUP BY clause parsing and planning
- Handle HAVING clause for post-aggregation filtering

**Related files:**
- `crates/expr/src/lib.rs` - add AggregateExpr variant
- `crates/executor/src/` - create `aggregate.rs` with AggregateExec
- `crates/parser/src/ast.rs` - add GroupBy/Having to SelectStatement
- `crates/planner/src/lib.rs` - build aggregation pipeline

**Educational value**: Hash aggregation algorithms, aggregate state management, NULL handling

---

### JOIN Operations (INNER, LEFT, RIGHT, FULL)
**Status**: Not Started
**Complexity**: Very High
**Impact**: High

Support `SELECT * FROM users u INNER JOIN orders o ON u.id = o.user_id`.

**Implementation approach:**
- Extend parser to recognize JOIN clauses with ON conditions
- Implement HashJoinExec (build hash table on smaller relation, probe with larger)
- Implement NestedLoopJoinExec for non-equi joins
- Add join type support (Inner/Left/Right/Full outer)
- Extend planner to choose join algorithm based on table sizes

**Related files:**
- `crates/parser/src/ast.rs` - add JOIN clauses to SelectStatement
- `crates/executor/src/` - create `join.rs` with HashJoinExec/NestedLoopJoinExec
- `crates/planner/src/lib.rs` - add join planning and algorithm selection
- `crates/expr/src/lib.rs` - extend for multi-table column resolution

**Educational value**: Join algorithms, hash join vs nested loop tradeoffs, outer join semantics

---

### ORDER BY and LIMIT/OFFSET
**Status**: ✅ Completed
**Complexity**: Medium
**Impact**: Medium

Support `SELECT * FROM users ORDER BY name ASC LIMIT 10 OFFSET 20`.

**Completed (commits af74bca, 510cb60, a28c238):**
- ✅ Parser support for ORDER BY/LIMIT/OFFSET clauses (SortDirection enum, OrderByExpr)
- ✅ SortExec operator with stable sort (materializing, in-memory sorting)
- ✅ LimitExec operator with early termination optimization
- ✅ Planner integration (Sort/Limit physical plan nodes)
- ✅ Builder integration (construct Sort/Limit operators from plans)
- ✅ Comprehensive unit tests (11 tests for SortExec, 13 tests for LimitExec)
- ✅ Comprehensive integration tests (15 tests including pagination scenarios)
- ✅ ExecutionStats tracking for both operators

**Not yet implemented:**
- External merge sort for large result sets exceeding memory
- LIMIT pushdown optimization through complex query plans

**Related files:**
- `crates/parser/src/ast.rs:37-43` - OrderBy/Limit/Offset in SelectStatement
- `crates/executor/src/sort.rs` - SortExec implementation + 11 unit tests
- `crates/executor/src/limit.rs` - LimitExec implementation + 13 unit tests
- `crates/planner/src/lib.rs` - Sort/Limit planning logic
- `crates/database/tests/order_limit_offset.rs` - 15 integration tests

**Educational value**: Materialization vs pipelining, stable sorting, pagination patterns, early termination

---

### Subqueries (Correlated and Non-Correlated)
**Status**: Not Started
**Complexity**: Very High
**Impact**: Medium

Support `SELECT * FROM users WHERE id IN (SELECT user_id FROM orders WHERE total > 100)`.

**Implementation approach:**
- Extend parser to recognize scalar/list subqueries in WHERE/SELECT clauses
- Implement SubqueryExec operator
- Handle correlated subqueries (re-execute for each outer row)
- Optimize non-correlated subqueries (execute once, cache results)
- Implement EXISTS/NOT EXISTS/IN/NOT IN operators

**Related files:**
- `crates/parser/src/ast.rs` - add Subquery to ResolvedExpr
- `crates/executor/src/` - create `subquery.rs`
- `crates/planner/src/lib.rs` - detect correlation, plan execution strategy

**Educational value**: Query decorrelation, subquery optimization, caching strategies

---

## Transaction & Concurrency

### Multi-Statement Transactions
**Status**: Not Started
**Complexity**: High
**Impact**: High

Support `BEGIN; INSERT ...; UPDATE ...; COMMIT;` with rollback capability.

**Implementation approach:**
- Add transaction context to ExecutionContext (active_txn_id, isolation level)
- Implement transaction log (separate from WAL) tracking uncommitted operations
- Support BEGIN/COMMIT/ROLLBACK statements
- Implement rollback by replaying inverse operations from transaction log
- Ensure atomicity across multiple DML statements

**Related files:**
- `crates/executor/src/lib.rs` - add transaction state to ExecutionContext
- `crates/wal/` - extend for transaction boundaries
- `crates/repl/src/main.rs` - add transaction commands

**Educational value**: ACID properties, atomicity across statements, rollback implementation

---

### MVCC (Multi-Version Concurrency Control)
**Status**: Not Started
**Complexity**: Very High
**Impact**: High

Support concurrent transactions with snapshot isolation to avoid read locks.

**Implementation approach:**
- Add version numbers to every row (xmin, xmax transaction IDs)
- Modify HeapTable to store multiple row versions
- Implement visibility rules (which version is visible to which transaction)
- Add vacuum process to garbage collect old versions
- Implement snapshot isolation level

**Related files:**
- `crates/storage/src/lib.rs` - extend Row with version metadata
- `crates/executor/src/lib.rs` - add transaction ID generation
- Create `crates/mvcc/` for visibility and vacuum logic

**Educational value**: Snapshot isolation, version visibility, garbage collection

---

### Row-Level Locking
**Status**: Not Started
**Complexity**: High
**Impact**: Medium

Support `SELECT ... FOR UPDATE` and automatic row locks during UPDATE/DELETE.

**Implementation approach:**
- Add lock table mapping RecordId → (txn_id, lock_type)
- Implement lock acquisition with timeout/deadlock detection
- Support shared (read) and exclusive (write) locks
- Release locks on COMMIT/ROLLBACK
- Detect and break deadlocks using wait-for graph

**Related files:**
- Create `crates/locking/` for lock manager
- `crates/executor/src/dml.rs` - acquire locks before mutations
- `crates/executor/src/scan.rs` - support FOR UPDATE clause

**Educational value**: Two-phase locking, deadlock detection, lock granularity

---

## Performance Optimizations

### Query Plan Caching
**Status**: Not Started
**Complexity**: Medium
**Impact**: Medium

Cache compiled query plans for frequently executed queries to reduce planning overhead.

**Implementation approach:**
- Hash normalized SQL query text as cache key
- Store PhysicalPlan in LRU cache (bounded size)
- Invalidate cache entries on schema changes (CREATE/DROP TABLE)
- Add EXPLAIN ANALYZE to show plan reuse statistics

**Related files:**
- `crates/planner/src/lib.rs` - add plan cache HashMap
- `crates/repl/src/main.rs` - invalidate on DDL operations

**Educational value**: Plan caching strategies, cache invalidation, query normalization

---

### Vectorized Execution
**Status**: Not Started
**Complexity**: Very High
**Impact**: High

Process rows in batches (e.g., 1024 at a time) instead of Volcano one-row-at-a-time model.

**Implementation approach:**
- Change Executor trait to return `Option<RecordBatch>` (batch of rows)
- Implement vectorized expression evaluation (SIMD where possible)
- Modify all operators to process batches (FilterExec, ProjectExec, etc.)
- Add RecordBatch type to common crate

**Related files:**
- `crates/executor/src/lib.rs` - change Executor::next() signature
- `crates/common/src/lib.rs` - enhance RecordBatch for columnar layout
- `crates/expr/src/lib.rs` - add batch expression evaluation

**Educational value**: Vectorization benefits, CPU cache efficiency, columnar data layout

---

### Parallel Query Execution
**Status**: Not Started
**Complexity**: Very High
**Impact**: High

Execute independent sub-plans concurrently using thread pool.

**Implementation approach:**
- Identify parallelizable operators (independent scans, hash join build/probe)
- Add thread pool to ExecutionContext
- Implement parallel SeqScan (partition pages across threads)
- Add exchange operators (gather results from parallel workers)
- Handle synchronization and result ordering

**Related files:**
- `crates/executor/src/lib.rs` - add thread pool to ExecutionContext
- `crates/executor/src/scan.rs` - implement parallel scan
- Create `crates/executor/src/exchange.rs` for data shuffling

**Educational value**: Parallel query processing, work partitioning, result merging

---

## Storage & Durability

### Table Partitioning
**Status**: Not Started
**Complexity**: Very High
**Impact**: Medium

Support `CREATE TABLE orders PARTITION BY RANGE (order_date)` to split large tables.

**Implementation approach:**
- Extend parser for PARTITION BY clause
- Store partition metadata in catalog (partition key, ranges/lists)
- Create separate heap files per partition (e.g., orders_2024q1.heap)
- Modify planner to prune partitions based on WHERE clause
- Implement partition-aware INSERT routing

**Related files:**
- `crates/catalog/src/lib.rs` - add partition metadata
- `crates/storage/src/lib.rs` - support multiple heap files per table
- `crates/planner/src/lib.rs` - add partition pruning

**Educational value**: Table partitioning strategies, partition pruning optimization

---

### Write-Ahead Log Checkpointing
**Status**: Not Started
**Complexity**: High
**Impact**: High

Implement periodic checkpoints to truncate WAL and speed up crash recovery.

**Implementation approach:**
- Add checkpoint process that flushes dirty pages to disk
- Record checkpoint LSN (log sequence number) in control file
- Truncate WAL before checkpoint LSN after successful checkpoint
- During recovery, start replay from last checkpoint instead of beginning
- Implement incremental checkpointing (limit I/O impact)

**Related files:**
- `crates/wal/src/lib.rs` - add checkpoint() method
- `crates/buffer/src/lib.rs` - track dirty pages
- Create checkpoint control file in data directory

**Educational value**: Checkpoint strategies, recovery optimization, LSN tracking

---

### Compression (Page-Level and Column-Level)
**Status**: Not Started
**Complexity**: High
**Impact**: Medium

Compress heap pages and/or columns to reduce storage footprint and I/O.

**Implementation approach:**
- Add compression flag to page headers
- Implement compression codecs (LZ4, Snappy for page-level)
- Decompress on read, compress on write (transparent to operators)
- Add column-level compression for columnar storage (dictionary encoding, RLE)
- Measure compression ratios in EXPLAIN output

**Related files:**
- `crates/storage/src/lib.rs` - add compression/decompression layer
- `crates/common/src/lib.rs` - add compression config to Config

**Educational value**: Compression algorithms, I/O vs CPU tradeoffs, dictionary encoding

---

## SQL Feature Completeness

### Data Types (DATE, TIMESTAMP, DECIMAL, BLOB)
**Status**: Not Started
**Complexity**: Medium
**Impact**: Medium

Expand beyond Int/Text/Bool to support richer data types.

**Implementation approach:**
- Add new SqlType variants and corresponding Value variants
- Implement type-specific operators (date arithmetic, string concatenation)
- Add type casting functions (CAST(x AS INT))
- Extend parser to recognize new type literals
- Add type coercion rules for mixed-type expressions

**Related files:**
- `crates/types/src/lib.rs` - add SqlType/Value variants
- `crates/expr/src/lib.rs` - add type-specific operators
- `crates/parser/src/lib.rs` - parse new literals

**Educational value**: Type systems, coercion rules, domain-specific operators

---

### Window Functions (ROW_NUMBER, RANK, LAG, LEAD)
**Status**: Not Started
**Complexity**: Very High
**Impact**: Medium

Support `SELECT name, ROW_NUMBER() OVER (ORDER BY score DESC) FROM users`.

**Implementation approach:**
- Extend parser for OVER clauses with PARTITION BY and ORDER BY
- Implement WindowExec operator (buffer partition, compute window function)
- Support frame specifications (ROWS BETWEEN, RANGE BETWEEN)
- Add built-in window functions (ROW_NUMBER, RANK, DENSE_RANK, LAG, LEAD)

**Related files:**
- `crates/parser/src/ast.rs` - add WindowFunction and OverClause
- `crates/executor/src/` - create `window.rs` with WindowExec
- `crates/expr/src/lib.rs` - add window function evaluation

**Educational value**: Window function semantics, frame processing, ranking algorithms

---

### Common Table Expressions (WITH)
**Status**: Not Started
**Complexity**: High
**Impact**: Medium

Support `WITH tmp AS (SELECT ...) SELECT * FROM tmp JOIN other_table`.

**Implementation approach:**
- Extend parser to recognize WITH clauses
- Store CTE definitions in planner context during planning
- Support recursive CTEs (WITH RECURSIVE)
- Optimize CTE materialization (inline vs materialize once)
- Handle CTE scoping rules

**Related files:**
- `crates/parser/src/ast.rs` - add WithClause to SelectStatement
- `crates/planner/src/lib.rs` - maintain CTE context during planning
- `crates/executor/src/` - add CTE materialization operator

**Educational value**: CTE scoping, recursive query processing, materialization strategies

---

### Views (CREATE VIEW, DROP VIEW)
**Status**: Not Started
**Complexity**: Medium
**Impact**: Medium

Support `CREATE VIEW active_users AS SELECT * FROM users WHERE active = true`.

**Implementation approach:**
- Add ViewMeta to catalog storing view name and query text
- Parse CREATE/DROP VIEW statements
- During SELECT, expand view references to their underlying queries
- Support materialized views (cache query results, refresh on demand)
- Handle view dependency tracking (can't drop table if view depends on it)

**Related files:**
- `crates/catalog/src/lib.rs` - add view storage
- `crates/parser/src/ast.rs` - add CreateView/DropView statements
- `crates/planner/src/lib.rs` - expand views during planning

**Educational value**: View expansion, query rewriting, dependency management

---

## Observability & Debugging

### EXPLAIN and EXPLAIN ANALYZE
**Status**: Foundation Implemented ✅
**Complexity**: Medium
**Impact**: High

**Completed (commit 85a9a62):**
- ✅ Parser support for `EXPLAIN` and `EXPLAIN ANALYZE` syntax
- ✅ `ExecutionStats` struct in common crate (timing, row counts, pages scanned)
- ✅ `Executor::stats()` trait method for statistics collection
- ✅ Full instrumentation in `SeqScanExec` with `Instant` timing
- ✅ Duration formatting helper (`format_duration()`)
- ✅ Planner handles `Statement::Explain` by planning inner query

**Remaining work:**
- Add REPL handling to distinguish EXPLAIN vs EXPLAIN ANALYZE execution
- Instrument remaining operators: FilterExec, ProjectExec, InsertExec, UpdateExec, DeleteExec
- Create `explain_analyze()` formatting function to display plan with statistics
- Add parser tests for EXPLAIN syntax variants
- Add integration tests for end-to-end EXPLAIN ANALYZE queries
- Track operator-specific metrics (e.g., FilterExec selectivity, hash table size for joins)

**Implementation approach:**
- Each operator tracks timing with `Instant::now()` and `elapsed()`
- Statistics accumulated in operator fields, returned via `stats()` method
- REPL executes query and displays results + timing for EXPLAIN ANALYZE
- REPL displays plan without execution for plain EXPLAIN

**Related files:**
- `crates/common/src/lib.rs:175-225` - ExecutionStats struct
- `crates/parser/src/ast.rs:39-43` - Explain statement variant
- `crates/parser/src/lib.rs:127-132` - EXPLAIN parsing
- `crates/planner/src/lib.rs:203-207` - EXPLAIN planning
- `crates/executor/src/lib.rs:1155-1160` - Executor::stats() trait method
- `crates/executor/src/scan.rs:95-140` - SeqScanExec instrumentation

**Educational value**: Query profiling, performance analysis, timing measurement, operator statistics

---

### Query Performance Statistics
**Status**: Not Started
**Complexity**: Medium
**Impact**: Medium

Track query execution metrics for performance analysis.

**Implementation approach:**
- Add statistics collector recording query execution times
- Track cache hit rates (plan cache, buffer pool)
- Expose statistics via `SHOW STATS` command
- Log slow queries (configurable threshold)
- Add histogram of query durations

**Related files:**
- Create `crates/stats/` for statistics collection
- `crates/repl/src/main.rs` - add SHOW STATS command

**Educational value**: Performance monitoring, observability, profiling

---

### Debug Logging and Trace Events
**Status**: Not Started
**Complexity**: Low
**Impact**: Low

Add structured logging to trace query execution flow.

**Implementation approach:**
- Integrate `tracing` crate for structured logging
- Add log statements at key execution points (operator open/next/close)
- Support log level configuration (trace/debug/info/warn/error)
- Output execution traces for debugging

**Related files:**
- Add `tracing` dependency to workspace Cargo.toml
- Instrument all crates with trace! and debug! macros

**Educational value**: Observability best practices, structured logging

---

## Testing & Quality

### Property-Based Testing for DML Operations
**Status**: Not Started
**Complexity**: Medium
**Impact**: Medium

Use `proptest` to generate random DML sequences and verify invariants.

**Implementation approach:**
- Generate random INSERT/UPDATE/DELETE sequences
- Verify PK uniqueness maintained after all operations
- Verify row counts match expected values
- Test crash recovery by killing executor mid-operation
- Verify WAL replay produces same final state

**Related files:**
- `crates/executor/src/lib.rs` - add proptest suite
- `crates/wal/src/lib.rs` - add crash recovery proptests

**Educational value**: Property-based testing, invariant checking, fuzzing

---

### Fuzzing for Parser and Executor
**Status**: Not Started
**Complexity**: Medium
**Impact**: Medium

Use `cargo-fuzz` to find crashes and panics in parser/executor.

**Implementation approach:**
- Create fuzz targets for parser (feed random SQL strings)
- Create fuzz targets for executor (random query plans)
- Run AFL or libFuzzer to generate test cases
- Add regression tests for discovered crashes

**Related files:**
- Create `fuzz/` directory with fuzz targets
- Add fuzzing infrastructure to CI pipeline

**Educational value**: Fuzzing techniques, crash discovery, security testing

---

### Benchmark Suite
**Status**: Not Started
**Complexity**: Medium
**Impact**: Low

Create standard benchmark suite (e.g., TPC-H subset) for performance tracking.

**Implementation approach:**
- Implement subset of TPC-H queries (select 5-10 representative queries)
- Add benchmark runner using `criterion` crate
- Track query execution time across commits
- Generate performance regression reports

**Related files:**
- Create `benches/` directory with TPC-H queries
- Add benchmark runner script

**Educational value**: Performance benchmarking, regression detection, query optimization validation

---

## Documentation & Education

### Tutorial Chapters
**Status**: Not Started
**Complexity**: Low
**Impact**: High (Educational)

Write step-by-step tutorials for key concepts.

**Chapters to add:**
1. "Building a Query: From SQL to Execution" (parser → planner → executor)
2. "Primary Keys Under the Hood" (index structure, uniqueness enforcement)
3. "Write-Ahead Logging for Durability" (WAL design, crash recovery)
4. "Volcano Iterator Model" (pull-based execution, operator composition)
5. "Buffer Pool Management" (page replacement, dirty page tracking)

**Related files:**
- Create `docs/tutorials/` directory
- Add Mermaid diagrams for architecture visualization

**Educational value**: Core pedagogical content for database course

---

### Interactive REPL Improvements
**Status**: Not Started
**Complexity**: Low
**Impact**: Medium

Enhance REPL with history, tab completion, syntax highlighting.

**Implementation approach:**
- Integrate `rustyline` for readline-style editing
- Add command history persistence
- Implement tab completion for table/column names
- Add syntax highlighting for SQL keywords
- Support multi-line query input

**Related files:**
- `crates/repl/src/main.rs` - integrate rustyline

**Educational value**: Improved developer experience, ergonomics

---

## Notes

- Items marked **Very High** complexity likely require multiple weeks of focused work
- **Impact** ratings reflect educational value + system capability improvement
- Many features depend on others (e.g., JOINs benefit from indexes, transactions need MVCC)
- Prioritize based on learning objectives (e.g., focus on query features before advanced concurrency)

This list will evolve as the database matures. Contributions should align with the educational mission: each feature should clearly demonstrate a database systems concept.
