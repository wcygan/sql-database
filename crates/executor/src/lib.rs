//! Query executor: executes physical plans using a Volcano-style iterator model.
//!
//! The executor bridges the planner's physical operators with the storage layer,
//! buffer pool, and WAL to perform actual query execution. It implements a pull-based
//! iterator model where each operator pulls rows from its children.
//!
//! # Architecture
//!
//! ```text
//! Physical Plan
//!     ↓
//! Build Executor Tree
//!     ↓
//! open() → Initialize resources
//!     ↓
//! next() → Pull rows iteratively
//!     ↓
//! close() → Clean up resources
//! ```
//!
//! # Example
//!
//! ```no_run
//! use executor::{execute_query, ExecutionContext};
//! use planner::PhysicalPlan;
//! use catalog::Catalog;
//! use buffer::FilePager;
//! use wal::Wal;
//! use common::TableId;
//! use std::path::PathBuf;
//!
//! let catalog = Catalog::new();
//! let mut pager = FilePager::new(PathBuf::from("/tmp/db"), 100);
//! let mut wal = Wal::open("/tmp/db/wal.log").unwrap();
//! let mut ctx = ExecutionContext::new(&catalog, &mut pager, &mut wal, PathBuf::from("/tmp/db"));
//!
//! let plan = PhysicalPlan::SeqScan {
//!     table_id: TableId(1),
//!     schema: vec!["id".into(), "name".into()],
//! };
//! let results = execute_query(plan, &mut ctx).unwrap();
//! ```

#[cfg(test)]
mod tests;

mod builder;
mod dml;
mod filter;
mod project;
mod scan;

use catalog::Catalog;
use common::{DbError, DbResult, Row, TableId};
use planner::PhysicalPlan;
use std::path::PathBuf;
use storage::HeapTable;
use wal::{Wal, WalRecord};

/// Volcano-style iterator interface for query execution.
///
/// Each operator implements this trait to provide a pull-based execution model.
/// Operators initialize resources in `open()`, produce rows via `next()`, and
/// clean up in `close()`.
pub trait Executor {
    /// Initialize the operator (open files, allocate buffers, etc.).
    fn open(&mut self, ctx: &mut ExecutionContext) -> DbResult<()>;

    /// Fetch the next row, or None if exhausted.
    fn next(&mut self, ctx: &mut ExecutionContext) -> DbResult<Option<Row>>;

    /// Release resources (close files, flush buffers, etc.).
    fn close(&mut self, ctx: &mut ExecutionContext) -> DbResult<()>;

    /// Return the schema (column names) of rows produced by this operator.
    fn schema(&self) -> &[String];
}

/// Shared execution context passed to all operators.
///
/// Contains references to the catalog, buffer pool (pager), and WAL for
/// coordinating data access and durability.
pub struct ExecutionContext<'a> {
    pub catalog: &'a Catalog,
    pub pager: &'a mut dyn buffer::Pager,
    pub wal: &'a mut Wal,
    pub data_dir: PathBuf,
}

impl<'a> ExecutionContext<'a> {
    /// Create a new execution context.
    pub fn new(
        catalog: &'a Catalog,
        pager: &'a mut dyn buffer::Pager,
        wal: &'a mut Wal,
        data_dir: PathBuf,
    ) -> Self {
        Self {
            catalog,
            pager,
            wal,
            data_dir,
        }
    }

    /// Open a heap table for the given table ID.
    pub fn heap_table(&mut self, table_id: TableId) -> DbResult<impl HeapTable + '_> {
        let table_meta = self.catalog.table_by_id(table_id)?;

        let file_path = self.data_dir.join(format!("{}.heap", table_meta.name));
        storage::HeapFile::open(&file_path, table_id.0)
    }

    /// Log a DML operation to the WAL.
    pub fn log_dml(&mut self, record: WalRecord) -> DbResult<()> {
        self.wal.append(&record)?;
        self.wal.sync()
    }
}

/// Execute a query plan and return all result rows.
///
/// This is the main entry point for executing SELECT queries that return data.
///
/// # Errors
///
/// Returns `DbError::Executor` if execution fails at any stage.
pub fn execute_query(plan: PhysicalPlan, ctx: &mut ExecutionContext) -> DbResult<Vec<Row>> {
    let mut executor = builder::build_executor(plan)?;

    executor.open(ctx)?;

    let mut results = Vec::new();
    while let Some(row) = executor.next(ctx)? {
        results.push(row);
    }

    executor.close(ctx)?;

    Ok(results)
}

/// Execute a DML statement (INSERT/UPDATE/DELETE) and return affected row count.
///
/// DML statements return a single row containing the number of affected rows.
///
/// # Errors
///
/// Returns `DbError::Executor` if execution fails or no result is produced.
pub fn execute_dml(plan: PhysicalPlan, ctx: &mut ExecutionContext) -> DbResult<u64> {
    let mut executor = builder::build_executor(plan)?;

    executor.open(ctx)?;

    let result = executor
        .next(ctx)?
        .ok_or_else(|| DbError::Executor("DML operation returned no result".into()))?;

    executor.close(ctx)?;

    // DML operators return single row with affected count
    match result.0.get(0) {
        Some(types::Value::Int(count)) => Ok(*count as u64),
        Some(other) => Err(DbError::Executor(format!(
            "DML result count must be integer, got {:?}",
            other
        ))),
        None => Err(DbError::Executor("DML result has no columns".into())),
    }
}
