use serde::{Deserialize, Serialize};
use std::{collections::HashMap, io, path::PathBuf};
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
/// - `let row = Row(vec![Value::Int(1)]);`
/// - `let row = Row(vec![Value::Text("alice".into()), Value::Bool(true)]);`
/// - `let row = Row(vec![Value::Int(10), Value::Null]);`
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Row(pub Vec<Value>);

/// Named projection of a row keyed by column name.
/// Examples:
/// - `let mut map = RowMap::new(); map.insert("id".into(), Value::Int(1));`
/// - `let map = RowMap::from([("name".into(), Value::Text("alice".into()))]);`
/// - `let map = RowMap::from([("active".into(), Value::Bool(true)), ("age".into(), Value::Int(30))]);`
/// - `let map = RowMap::from([("deleted_at".into(), Value::Null)]);`
pub type RowMap = HashMap<String, Value>;

/// Rectangular result set carrying column labels and rows.
/// Examples:
/// - `let rb = RecordBatch { columns: vec!["id".into()], rows: vec![Row(vec![Value::Int(1)])] };`
/// - `let rb = RecordBatch { columns: vec!["id".into(), "name".into()], rows: vec![Row(vec![Value::Int(1), Value::Text("alice".into())])] };`
/// - `let rb = RecordBatch { columns: vec!["count".into()], rows: vec![Row(vec![Value::Int(42)]), Row(vec![Value::Int(84)])] };`
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
    #[error(transparent)]
    Io(#[from] io::Error),
}

/// Result alias that carries a `DbError`.
pub type DbResult<T> = Result<T, DbError>;

/// Runtime configuration for the database components.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Config {
    /// Directory where table data, catalog metadata, and WAL files live.
    pub data_dir: PathBuf,
    /// Fixed-size page allocation in bytes.
    pub page_size: usize,
    /// Number of pages the buffer pool keeps resident.
    pub buffer_pool_pages: usize,
    /// Controls whether the write-ahead log is enabled.
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

/// Convenient re-exports for downstream crates.
pub mod prelude {
    pub use crate::{Config, DbError, DbResult, RecordBatch, Row, RowMap};
    pub use types::{SqlType, Value};
}
