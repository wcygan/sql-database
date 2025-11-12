//! Test setup macros for reducing boilerplate across the SQL database test suite.
//!
//! This module provides declarative macros that simplify common test patterns:
//! - Database context setup with tables and schemas
//! - Page and storage setup for low-level tests
//! - WAL setup for durability tests
//! - Row and value construction

/// Creates a test database context with a table and schema.
///
/// This macro simplifies the common pattern of creating a TestContext with
/// a catalog, pager, and WAL, then adding a table with specified columns.
///
/// # Syntax
///
/// ```text
/// test_db!(context_var, table: "table_name", cols: ["col1" => Type1, "col2" => Type2, ...])
/// test_db!(context_var, table: "table_name", cols: ["col1" => Type1, ...], pk: [0, 1])
/// ```
///
/// # Examples
///
/// ```
/// use testsupport::test_db;
/// use types::SqlType;
///
/// // Simple table setup
/// test_db!(mut ctx, table: "users", cols: ["id" => SqlType::Int, "name" => SqlType::Text]);
/// let mut exec_ctx = ctx.execution_context();
/// // Use exec_ctx for query execution
/// ```
///
/// ```
/// use testsupport::test_db;
/// use types::SqlType;
///
/// // With primary key
/// test_db!(mut ctx, table: "users",
///          cols: ["id" => SqlType::Int, "name" => SqlType::Text],
///          pk: [0]);
/// let mut exec_ctx = ctx.execution_context();
/// // Table has primary key on column 0 (id)
/// ```
#[macro_export]
macro_rules! test_db {
    // Variant with primary key and mut
    (mut $ctx:ident, table: $name:expr, cols: [$($col:expr => $typ:expr),+ $(,)?], pk: [$($pk_col:expr),+ $(,)?]) => {
        let mut _catalog = ::catalog::Catalog::new();
        _catalog.create_table(
            $name,
            vec![$(::catalog::Column::new($col, $typ)),+],
            Some(vec![$($pk_col),+])
        ).unwrap();
        let mut $ctx = $crate::context::TestContext::with_catalog(_catalog).unwrap();
    };

    // Variant with primary key
    ($ctx:ident, table: $name:expr, cols: [$($col:expr => $typ:expr),+ $(,)?], pk: [$($pk_col:expr),+ $(,)?]) => {
        let mut _catalog = ::catalog::Catalog::new();
        _catalog.create_table(
            $name,
            vec![$(::catalog::Column::new($col, $typ)),+],
            Some(vec![$($pk_col),+])
        ).unwrap();
        let $ctx = $crate::context::TestContext::with_catalog(_catalog).unwrap();
    };

    // Variant without primary key with mut
    (mut $ctx:ident, table: $name:expr, cols: [$($col:expr => $typ:expr),+ $(,)?]) => {
        let mut _catalog = ::catalog::Catalog::new();
        _catalog.create_table(
            $name,
            vec![$(::catalog::Column::new($col, $typ)),+],
            None
        ).unwrap();
        let mut $ctx = $crate::context::TestContext::with_catalog(_catalog).unwrap();
    };

    // Variant without primary key
    ($ctx:ident, table: $name:expr, cols: [$($col:expr => $typ:expr),+ $(,)?]) => {
        let mut _catalog = ::catalog::Catalog::new();
        _catalog.create_table(
            $name,
            vec![$(::catalog::Column::new($col, $typ)),+],
            None
        ).unwrap();
        let $ctx = $crate::context::TestContext::with_catalog(_catalog).unwrap();
    };

    // Variant for multiple tables with mut
    (mut $ctx:ident, tables: [$(($name:expr, [$($col:expr => $typ:expr),+ $(,)?])),+ $(,)?]) => {
        let mut _catalog = ::catalog::Catalog::new();
        $(
            _catalog.create_table(
                $name,
                vec![$(::catalog::Column::new($col, $typ)),+],
                None
            ).unwrap();
        )+
        let mut $ctx = $crate::context::TestContext::with_catalog(_catalog).unwrap();
    };

    // Variant for multiple tables
    ($ctx:ident, tables: [$(($name:expr, [$($col:expr => $typ:expr),+ $(,)?])),+ $(,)?]) => {
        let mut _catalog = ::catalog::Catalog::new();
        $(
            _catalog.create_table(
                $name,
                vec![$(::catalog::Column::new($col, $typ)),+],
                None
            ).unwrap();
        )+
        let $ctx = $crate::context::TestContext::with_catalog(_catalog).unwrap();
    };
}

/// Creates a simple pager setup for buffer pool tests.
///
/// This macro simplifies buffer pool test setup by creating a temporary directory,
/// FilePager, and TableId in one line.
///
/// # Syntax
///
/// ```text
/// test_pager!(pager_var, table_var)
/// test_pager!(pager_var, table_var, capacity: 5)
/// ```
///
/// # Examples
///
/// ```
/// use testsupport::test_pager;
/// use buffer::Pager;
///
/// test_pager!(pager, table);
///
/// let pid = pager.allocate_page(table).unwrap();
/// let page = pager.fetch_page(table, pid).unwrap();
/// // Use page...
/// ```
///
/// ```
/// use testsupport::test_pager;
///
/// test_pager!(pager, table, capacity: 2);
/// // Pager with capacity of 2 pages for testing eviction
/// ```
#[macro_export]
macro_rules! test_pager {
    ($pager:ident, $table:ident) => {
        let _dir = ::tempfile::tempdir().unwrap();
        let mut $pager = ::buffer::FilePager::new(_dir.path(), 10);
        let $table = ::common::TableId(1);
    };

    ($pager:ident, $table:ident, capacity: $cap:expr) => {
        let _dir = ::tempfile::tempdir().unwrap();
        let mut $pager = ::buffer::FilePager::new(_dir.path(), $cap);
        let $table = ::common::TableId(1);
    };
}

/// Creates a WAL setup for durability tests.
///
/// This macro simplifies WAL test setup by creating a temporary directory
/// and opening a WAL file in one line.
///
/// # Syntax
///
/// ```text
/// test_wal!(wal_var)
/// ```
///
/// # Examples
///
/// ```
/// use testsupport::test_wal;
/// use wal::WalRecord;
/// use common::{TableId, PageId, RecordId};
/// use types::Value;
///
/// test_wal!(wal);
///
/// let record = WalRecord::Insert {
///     table: TableId(1),
///     row: vec![Value::Int(1), Value::Text("Alice".into())],
///     rid: RecordId { page_id: PageId(0), slot: 0 },
/// };
///
/// wal.append(&record).unwrap();
/// wal.sync().unwrap();
/// ```
#[macro_export]
macro_rules! test_wal {
    ($wal:ident) => {
        let _dir = ::tempfile::tempdir().unwrap();
        let _wal_path = _dir.path().join("test.wal");
        #[allow(unused_mut)]
        let mut $wal = ::wal::Wal::open(&_wal_path).unwrap();
    };
}

/// Creates a Row with typed values.
///
/// This macro simplifies row construction for tests by providing a concise
/// syntax for creating rows with different value types.
///
/// # Syntax
///
/// ```text
/// row![Value1, Value2, ...]           // Mixed values
/// row![int: 1, 2, 3]                  // All integers
/// row![text: "alice", "bob"]          // All text
/// row![bool: true, false, true]       // All booleans
/// ```
///
/// # Examples
///
/// ```
/// use testsupport::row;
/// use types::Value;
///
/// // Mixed types
/// let r = row![Value::Int(1), Value::Text("Alice".into()), Value::Bool(true)];
/// assert_eq!(r.values.len(), 3);
/// ```
///
/// ```
/// use testsupport::row;
///
/// // All integers
/// let r = row![int: 1, 2, 3];
/// assert_eq!(r.values.len(), 3);
/// ```
///
/// ```
/// use testsupport::row;
///
/// // All text
/// let r = row![text: "alice", "bob", "charlie"];
/// assert_eq!(r.values.len(), 3);
/// ```
///
/// ```
/// use testsupport::row;
///
/// // All booleans
/// let r = row![bool: true, false, true];
/// assert_eq!(r.values.len(), 3);
/// ```
#[macro_export]
macro_rules! row {
    // Mixed values
    [$($val:expr),+ $(,)?] => {
        ::common::Row::new(vec![$($val),+])
    };

    // All integers
    [int: $($val:expr),+ $(,)?] => {
        ::common::Row::new(vec![$(::types::Value::Int($val)),+])
    };

    // All text
    [text: $($val:expr),+ $(,)?] => {
        ::common::Row::new(vec![$(::types::Value::Text($val.to_string())),+])
    };

    // All booleans
    [bool: $($val:expr),+ $(,)?] => {
        ::common::Row::new(vec![$(::types::Value::Bool($val)),+])
    };
}

#[cfg(test)]
mod tests {
    use buffer::Pager;
    use types::{SqlType, Value};

    #[test]
    fn test_db_macro_single_table() {
        test_db!(ctx, table: "users", cols: ["id" => SqlType::Int, "name" => SqlType::Text]);

        let catalog = ctx.catalog();
        let table = catalog.table("users").unwrap();
        assert_eq!(table.name, "users");
        assert_eq!(table.schema.columns().len(), 2);
    }

    #[test]
    fn test_db_macro_with_primary_key() {
        test_db!(ctx, table: "users",
                 cols: ["id" => SqlType::Int, "name" => SqlType::Text],
                 pk: [0]);

        let catalog = ctx.catalog();
        let table = catalog.table("users").unwrap();
        assert_eq!(table.primary_key, Some(vec![0]));
    }

    #[test]
    fn test_db_macro_multiple_tables() {
        test_db!(ctx, tables: [
            ("users", ["id" => SqlType::Int, "name" => SqlType::Text]),
            ("posts", ["id" => SqlType::Int, "title" => SqlType::Text])
        ]);

        let catalog = ctx.catalog();
        assert!(catalog.table("users").is_ok());
        assert!(catalog.table("posts").is_ok());
    }

    #[test]
    fn test_pager_macro() {
        test_pager!(pager, table);

        let pid = pager.allocate_page(table).unwrap();
        assert_eq!(pid.0, 0);
    }

    #[test]
    fn test_pager_macro_with_capacity() {
        test_pager!(pager, table, capacity: 5);

        let pid = pager.allocate_page(table).unwrap();
        assert_eq!(pid.0, 0);
    }

    #[test]
    fn test_wal_macro() {
        test_wal!(wal);

        // Verify WAL is created and writable
        let _ = wal;
    }

    #[test]
    fn test_row_macro_mixed() {
        let r = row![
            Value::Int(1),
            Value::Text("Alice".into()),
            Value::Bool(true)
        ];
        assert_eq!(r.values.len(), 3);
        assert_eq!(r.values[0], Value::Int(1));
    }

    #[test]
    fn test_row_macro_int() {
        let r = row![int: 1, 2, 3];
        assert_eq!(r.values.len(), 3);
        assert_eq!(r.values[0], Value::Int(1));
        assert_eq!(r.values[1], Value::Int(2));
        assert_eq!(r.values[2], Value::Int(3));
    }

    #[test]
    fn test_row_macro_text() {
        let r = row![text: "alice", "bob"];
        assert_eq!(r.values.len(), 2);
        assert_eq!(r.values[0], Value::Text("alice".to_string()));
        assert_eq!(r.values[1], Value::Text("bob".to_string()));
    }

    #[test]
    fn test_row_macro_bool() {
        let r = row![bool: true, false, true];
        assert_eq!(r.values.len(), 3);
        assert_eq!(r.values[0], Value::Bool(true));
        assert_eq!(r.values[1], Value::Bool(false));
        assert_eq!(r.values[2], Value::Bool(true));
    }
}
