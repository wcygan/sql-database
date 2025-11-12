//! Test execution context and database state management.
//!
//! Provides helpers for creating isolated test environments with temporary
//! storage, catalog, buffer pool, and WAL. Each test gets its own clean state
//! that is automatically cleaned up on drop.

use catalog::{Catalog, Column};
use common::{DbResult, Row, TableId};
use executor::ExecutionContext;
use std::path::{Path, PathBuf};
use storage::HeapTable;
use tempfile::TempDir;

/// A test execution context with isolated temporary storage.
///
/// This struct manages the lifecycle of all database components needed for testing:
/// - Temporary directory for storage files
/// - Catalog for schema metadata
/// - Buffer pool (FilePager) for page caching
/// - Write-ahead log for durability
///
/// All components are automatically cleaned up when the context is dropped.
///
/// # Example
///
/// ```
/// use testsupport::prelude::*;
///
/// let mut ctx = TestContext::new().unwrap();
/// let mut exec_ctx = ctx.execution_context();
///
/// // Use exec_ctx for query execution
/// // All temporary files are cleaned up when ctx is dropped
/// ```
pub struct TestContext {
    _temp_dir: TempDir,
    catalog: &'static mut Catalog,
    pager: &'static mut buffer::FilePager,
    wal: &'static mut wal::Wal,
    data_dir: PathBuf,
}

impl TestContext {
    /// Create a new test context with default configuration.
    ///
    /// Creates a temporary directory and initializes all database components
    /// with default settings suitable for testing.
    pub fn new() -> DbResult<Self> {
        let temp_dir = tempfile::tempdir()?;
        Self::with_dir(temp_dir)
    }

    /// Create a test context using an existing temporary directory.
    ///
    /// This is useful when you need control over the temp directory lifecycle
    /// or want to inspect files after the test completes.
    pub fn with_dir(temp_dir: TempDir) -> DbResult<Self> {
        let catalog = Box::leak(Box::new(Catalog::new()));
        let pager = Box::leak(Box::new(buffer::FilePager::new(temp_dir.path(), 10)));
        let wal_path = temp_dir.path().join("test.wal");
        let wal = Box::leak(Box::new(wal::Wal::open(&wal_path)?));

        Ok(Self {
            data_dir: temp_dir.path().to_path_buf(),
            _temp_dir: temp_dir,
            catalog: &mut *catalog,
            pager,
            wal,
        })
    }

    /// Create a test context with a pre-populated catalog.
    ///
    /// This is useful when tests need specific tables or indexes to already exist.
    ///
    /// # Example
    ///
    /// ```
    /// use catalog::{Catalog, Column};
    /// use types::SqlType;
    /// use testsupport::prelude::*;
    ///
    /// let mut catalog = Catalog::new();
    /// catalog.create_table("users", vec![
    ///     Column::new("id", SqlType::Int),
    ///     Column::new("name", SqlType::Text),
    /// ], None).unwrap();
    ///
    /// let ctx = TestContext::with_catalog(catalog).unwrap();
    /// ```
    pub fn with_catalog(catalog: Catalog) -> DbResult<Self> {
        let temp_dir = tempfile::tempdir()?;
        let catalog = Box::leak(Box::new(catalog));
        let pager = Box::leak(Box::new(buffer::FilePager::new(temp_dir.path(), 10)));
        let wal_path = temp_dir.path().join("test.wal");
        let wal = Box::leak(Box::new(wal::Wal::open(&wal_path)?));

        Ok(Self {
            data_dir: temp_dir.path().to_path_buf(),
            _temp_dir: temp_dir,
            catalog: &mut *catalog,
            pager,
            wal,
        })
    }

    /// Get an execution context for running queries.
    ///
    /// The returned context borrows from this TestContext and can be used
    /// to execute queries via the executor.
    pub fn execution_context(&mut self) -> ExecutionContext<'_> {
        ExecutionContext::new(self.catalog, self.pager, self.wal, self.data_dir.clone())
    }

    /// Get the path to the data directory.
    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    /// Get a reference to the catalog.
    pub fn catalog(&self) -> &Catalog {
        self.catalog
    }

    /// Get a mutable reference to the catalog.
    pub fn catalog_mut(&mut self) -> &mut Catalog {
        self.catalog
    }
}

/// Helper function to create a basic test catalog with a simple table.
///
/// Creates a catalog with a "users" table containing:
/// - id: INT (column 0)
/// - name: TEXT (column 1)
/// - age: INT (column 2)
///
/// This is useful for basic tests that don't need complex schemas.
pub fn create_simple_catalog() -> Catalog {
    let mut catalog = Catalog::new();
    catalog
        .create_table(
            "users",
            vec![
                Column::new("id".to_string(), types::SqlType::Int),
                Column::new("name".to_string(), types::SqlType::Text),
                Column::new("age".to_string(), types::SqlType::Int),
            ],
            None,
        )
        .expect("Failed to create users table");
    catalog
}

/// Helper function to insert test rows into a table.
///
/// This is a convenience function for tests that need to populate tables
/// with data before running queries.
///
/// # Example
///
/// ```no_run
/// use common::{Row, TableId};
/// use types::Value;
/// use testsupport::prelude::*;
///
/// let mut ctx = TestContext::new().unwrap();
/// let mut exec_ctx = ctx.execution_context();
///
/// let rows = vec![
///     Row::new(vec![Value::Int(1), Value::Text("Alice".into()), Value::Int(30)]),
///     Row::new(vec![Value::Int(2), Value::Text("Bob".into()), Value::Int(25)]),
/// ];
///
/// insert_test_rows(&mut exec_ctx, TableId(1), rows).unwrap();
/// ```
pub fn insert_test_rows(
    ctx: &mut ExecutionContext,
    table_id: TableId,
    rows: Vec<Row>,
) -> DbResult<()> {
    let table_meta = ctx.catalog.table_by_id(table_id)?;
    let file_path = ctx.data_dir.join(format!("{}.heap", table_meta.name));
    let mut heap_table = storage::HeapFile::open(&file_path, table_id.0)?;

    for row in rows {
        heap_table.insert(&row)?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_context_creation() {
        let ctx = TestContext::new();
        assert!(ctx.is_ok());
    }

    #[test]
    fn test_context_with_catalog() {
        let catalog = create_simple_catalog();
        let ctx = TestContext::with_catalog(catalog);
        assert!(ctx.is_ok());

        let ctx = ctx.unwrap();
        assert!(ctx.catalog().table("users").is_ok());
    }

    #[test]
    fn test_execution_context() {
        let mut ctx = TestContext::new().unwrap();
        let _exec_ctx = ctx.execution_context();
        // Just verify we can create an execution context
    }

    #[test]
    fn test_simple_catalog() {
        let catalog = create_simple_catalog();
        let table = catalog.table("users").unwrap();
        assert_eq!(table.name, "users");
        assert_eq!(table.schema.columns().len(), 3);
    }
}
