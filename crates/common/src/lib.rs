#[cfg(test)]
mod tests;

pub mod pretty;

use serde::{Deserialize, Serialize};
use std::{collections::HashMap, io, path::PathBuf, time::Duration};
use thiserror::Error;
use types::Value;

/// Identifier for a column within a table schema.
/// Examples:
/// - `let id_col: ColumnId = 1; // maps to "id"`
/// - `let name_col: ColumnId = 2; // maps to "name"`
/// - `let price_col: ColumnId = 5; // maps to "price"`
pub type ColumnId = u16;

/// Logical identifier for a page in the storage layer.
/// Examples:
/// - `let freelist_page = PageId(0);`
/// - `let user_data_page = PageId(42);`
/// - `let index_page = PageId(9001);`
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PageId(pub u64);

/// Logical identifier for a table registered in the catalog.
/// Examples:
/// - `let users = TableId(7);`
/// - `let orders = TableId(11);`
/// - `let system_tables = TableId(0);`
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TableId(pub u64);

/// Fully-qualified identifier for a record within a page.
/// Examples:
/// - `let rid = RecordId { page_id: PageId(42), slot: 3 };`
/// - `let rid = RecordId { page_id: PageId(1024), slot: 0 };`
/// - `let rid = RecordId { page_id: PageId(1), slot: 255 };`
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RecordId {
    pub page_id: PageId,
    pub slot: u16,
}

/// Positional row representation backed by `types::Value`.
/// Examples:
/// - `let row = Row::new(vec![Value::Int(1)]);`
/// - `let row = Row::new(vec![Value::Text("alice".into()), Value::Bool(true)]);`
/// - `let row = Row::new(vec![Value::Int(10), Value::Null]);`
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Row {
    pub values: Vec<Value>,
    #[serde(skip)]
    #[serde(default)]
    rid: Option<RecordId>,
}

impl Row {
    pub fn new(values: Vec<Value>) -> Self {
        Self { values, rid: None }
    }

    pub fn from_values(values: Vec<Value>) -> Self {
        Self::new(values)
    }

    pub fn with_rid(mut self, rid: RecordId) -> Self {
        self.rid = Some(rid);
        self
    }

    pub fn set_rid(&mut self, rid: Option<RecordId>) {
        self.rid = rid;
    }

    pub fn rid(&self) -> Option<RecordId> {
        self.rid
    }

    pub fn into_values(self) -> Vec<Value> {
        self.values
    }
}

impl From<Vec<Value>> for Row {
    fn from(values: Vec<Value>) -> Self {
        Row::new(values)
    }
}

/// Named projection of a row keyed by column name.
/// Examples:
/// - `let mut map = RowMap::new(); map.insert("id".into(), Value::Int(1));`
/// - `let map = RowMap::from([("name".into(), Value::Text("alice".into()))]);`
/// - `let map = RowMap::from([("active".into(), Value::Bool(true)), ("age".into(), Value::Int(30))]);`
/// - `let map = RowMap::from([("deleted_at".into(), Value::Null)]);`
pub type RowMap = HashMap<String, Value>;

/// Rectangular result set carrying column labels and rows.
/// Examples:
/// - `let rb = RecordBatch { columns: vec!["id".into()], rows: vec![Row::new(vec![Value::Int(1)])] };`
/// - `let rb = RecordBatch { columns: vec!["id".into(), "name".into()], rows: vec![Row::new(vec![Value::Int(1), Value::Text("alice".into())])] };`
/// - `let rb = RecordBatch { columns: vec!["count".into()], rows: vec![Row::new(vec![Value::Int(42)]), Row::new(vec![Value::Int(84)])] };`
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RecordBatch {
    pub columns: Vec<String>,
    pub rows: Vec<Row>,
}

/// Canonical error type shared across database subsystems.
#[derive(Error, Debug)]
pub enum DbError {
    #[error("parse: {0}")]
    Parser(String),
    #[error("plan: {0}")]
    Planner(String),
    #[error("exec: {0}")]
    Executor(String),
    #[error("catalog: {0}")]
    Catalog(String),
    #[error("storage: {0}")]
    Storage(String),
    #[error("wal: {0}")]
    Wal(String),
    #[error("constraint violation: {0}")]
    Constraint(String),
    #[error(transparent)]
    Io(#[from] io::Error),
}

/// Result alias that carries a `DbError`.
pub type DbResult<T> = Result<T, DbError>;

/// Runtime configuration for the database components.
///
/// # Example
/// ```
/// use common::Config;
/// use std::path::PathBuf;
///
/// let config = Config::builder()
///     .data_dir(PathBuf::from("./my_db"))
///     .page_size(8192)
///     .buffer_pool_pages(512)
///     .wal_enabled(true)
///     .build();
/// ```
#[derive(Clone, Debug, Serialize, Deserialize, bon::Builder)]
pub struct Config {
    /// Directory where table data, catalog metadata, and WAL files live.
    #[builder(default = PathBuf::from("./db_data"))]
    pub data_dir: PathBuf,
    /// Fixed-size page allocation in bytes.
    #[builder(default = 4096)]
    pub page_size: usize,
    /// Number of pages the buffer pool keeps resident.
    #[builder(default = 256)]
    pub buffer_pool_pages: usize,
    /// Controls whether the write-ahead log is enabled.
    #[builder(default = true)]
    pub wal_enabled: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("./db_data"),
            page_size: 4096,
            buffer_pool_pages: 256,
            wal_enabled: true,
        }
    }
}

/// Execution statistics collected during query execution for EXPLAIN ANALYZE.
///
/// # Examples
/// ```
/// use common::ExecutionStats;
/// use std::time::Duration;
///
/// let stats = ExecutionStats {
///     open_time: Duration::from_millis(5),
///     total_next_time: Duration::from_millis(150),
///     close_time: Duration::from_millis(2),
///     rows_produced: 1000,
///     rows_filtered: 500,
///     pages_scanned: 10,
/// };
/// assert_eq!(stats.total_time().as_millis(), 157);
/// ```
#[derive(Clone, Debug, Default)]
pub struct ExecutionStats {
    /// Time spent in open() method
    pub open_time: Duration,
    /// Cumulative time spent across all next() calls
    pub total_next_time: Duration,
    /// Time spent in close() method
    pub close_time: Duration,
    /// Number of rows returned by this operator
    pub rows_produced: u64,
    /// Number of rows filtered out (FilterExec only)
    pub rows_filtered: u64,
    /// Number of pages scanned (SeqScan only)
    pub pages_scanned: u64,
}

impl ExecutionStats {
    /// Returns total execution time (open + next + close)
    pub fn total_time(&self) -> Duration {
        self.open_time + self.total_next_time + self.close_time
    }

    /// Formats duration in human-readable form (e.g., "123.45ms", "1.234s")
    pub fn format_duration(d: Duration) -> String {
        let micros = d.as_micros();
        if micros < 1000 {
            format!("{micros}µs")
        } else if micros < 1_000_000 {
            format!("{:.2}ms", micros as f64 / 1000.0)
        } else {
            format!("{:.3}s", micros as f64 / 1_000_000.0)
        }
    }
}

/// Convenient re-exports for downstream crates.
pub mod prelude {
    pub use crate::{Config, DbError, DbResult, ExecutionStats, RecordBatch, Row, RowMap};
    pub use types::{SqlType, Value};
}
