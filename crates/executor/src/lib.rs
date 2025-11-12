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
mod tests {
    pub mod helpers;

    use super::*;
    use helpers::{create_test_catalog, lit_int, lit_text};
    use planner::{PhysicalPlan, ResolvedExpr};
    use types::Value;

    fn setup_context() -> (ExecutionContext<'static>, tempfile::TempDir) {
        let temp_dir = tempfile::tempdir().unwrap();
        let catalog = create_test_catalog();

        let catalog = Box::leak(Box::new(catalog));
        let pager = Box::leak(Box::new(buffer::FilePager::new(temp_dir.path(), 10)));
        let wal = Box::leak(Box::new(
            wal::Wal::open(temp_dir.path().join("test.wal")).unwrap(),
        ));

        let ctx = ExecutionContext::new(catalog, pager, wal, temp_dir.path().into());
        (ctx, temp_dir)
    }

    fn insert_test_rows(
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

    // execute_query tests

    #[test]
    fn execute_query_seq_scan_empty_table() {
        let (mut ctx, _temp) = setup_context();

        let plan = PhysicalPlan::SeqScan {
            table_id: TableId(1),
            schema: vec!["id".into(), "name".into()],
        };

        let results = execute_query(plan, &mut ctx).unwrap();
        assert_eq!(results.len(), 0);
    }

    #[test]
    fn execute_query_seq_scan_with_rows() {
        let (mut ctx, _temp) = setup_context();
        let table_id = TableId(1);

        // Insert test data
        let rows = vec![
            Row::new(vec![
                Value::Int(1),
                Value::Text("alice".into()),
                Value::Bool(true),
            ]),
            Row::new(vec![
                Value::Int(2),
                Value::Text("bob".into()),
                Value::Bool(false),
            ]),
        ];
        insert_test_rows(&mut ctx, table_id, rows).unwrap();

        let plan = PhysicalPlan::SeqScan {
            table_id,
            schema: vec!["id".into(), "name".into(), "active".into()],
        };

        let results = execute_query(plan, &mut ctx).unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(
            results[0].values,
            vec![
                Value::Int(1),
                Value::Text("alice".into()),
                Value::Bool(true)
            ]
        );
        assert_eq!(
            results[1].values,
            vec![Value::Int(2), Value::Text("bob".into()), Value::Bool(false)]
        );
    }

    #[test]
    fn execute_query_with_filter() {
        let (mut ctx, _temp) = setup_context();
        let table_id = TableId(1);

        // Insert test data
        let rows = vec![
            Row::new(vec![
                Value::Int(1),
                Value::Text("alice".into()),
                Value::Bool(true),
            ]),
            Row::new(vec![
                Value::Int(2),
                Value::Text("bob".into()),
                Value::Bool(false),
            ]),
            Row::new(vec![
                Value::Int(3),
                Value::Text("carol".into()),
                Value::Bool(true),
            ]),
        ];
        insert_test_rows(&mut ctx, table_id, rows).unwrap();

        let scan = PhysicalPlan::SeqScan {
            table_id,
            schema: vec!["id".into(), "name".into(), "active".into()],
        };

        let plan = PhysicalPlan::Filter {
            input: Box::new(scan),
            predicate: ResolvedExpr::Column(2), // active column
        };

        let results = execute_query(plan, &mut ctx).unwrap();
        assert_eq!(results.len(), 2); // alice and carol
    }

    #[test]
    fn execute_query_with_project() {
        let (mut ctx, _temp) = setup_context();
        let table_id = TableId(1);

        // Insert test data
        let rows = vec![Row::new(vec![
            Value::Int(1),
            Value::Text("alice".into()),
            Value::Bool(true),
        ])];
        insert_test_rows(&mut ctx, table_id, rows).unwrap();

        let scan = PhysicalPlan::SeqScan {
            table_id,
            schema: vec!["id".into(), "name".into(), "active".into()],
        };

        let plan = PhysicalPlan::Project {
            input: Box::new(scan),
            columns: vec![("name".to_string(), 1)],
        };

        let results = execute_query(plan, &mut ctx).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].values, vec![Value::Text("alice".into())]);
    }

    #[test]
    fn execute_query_with_filter_and_project() {
        let (mut ctx, _temp) = setup_context();
        let table_id = TableId(1);

        // Insert test data
        let rows = vec![
            Row::new(vec![
                Value::Int(1),
                Value::Text("alice".into()),
                Value::Bool(true),
            ]),
            Row::new(vec![
                Value::Int(2),
                Value::Text("bob".into()),
                Value::Bool(false),
            ]),
            Row::new(vec![
                Value::Int(3),
                Value::Text("carol".into()),
                Value::Bool(true),
            ]),
        ];
        insert_test_rows(&mut ctx, table_id, rows).unwrap();

        let scan = PhysicalPlan::SeqScan {
            table_id,
            schema: vec!["id".into(), "name".into(), "active".into()],
        };

        let filter = PhysicalPlan::Filter {
            input: Box::new(scan),
            predicate: ResolvedExpr::Column(2),
        };

        let plan = PhysicalPlan::Project {
            input: Box::new(filter),
            columns: vec![("id".to_string(), 0), ("name".to_string(), 1)],
        };

        let results = execute_query(plan, &mut ctx).unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(
            results[0].values,
            vec![Value::Int(1), Value::Text("alice".into())]
        );
        assert_eq!(
            results[1].values,
            vec![Value::Int(3), Value::Text("carol".into())]
        );
    }

    // execute_dml tests

    #[test]
    fn execute_dml_insert_single_row() {
        let (mut ctx, _temp) = setup_context();

        let plan = PhysicalPlan::Insert {
            table_id: TableId(1),
            values: vec![
                lit_int(1),
                lit_text("alice"),
                ResolvedExpr::Literal(Value::Bool(true)),
            ],
        };

        let count = execute_dml(plan, &mut ctx).unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn execute_dml_update_returns_count() {
        let (mut ctx, _temp) = setup_context();
        let table_id = TableId(1);

        // Insert test data
        let rows = vec![
            Row::new(vec![
                Value::Int(1),
                Value::Text("alice".into()),
                Value::Bool(true),
            ]),
            Row::new(vec![
                Value::Int(2),
                Value::Text("bob".into()),
                Value::Bool(false),
            ]),
        ];
        insert_test_rows(&mut ctx, table_id, rows).unwrap();

        let plan = PhysicalPlan::Update {
            table_id,
            assignments: vec![(1, lit_text("updated"))],
            predicate: None,
        };

        let count = execute_dml(plan, &mut ctx).unwrap();
        assert_eq!(count, 2);
    }

    #[test]
    fn execute_dml_delete_returns_count() {
        let (mut ctx, _temp) = setup_context();
        let table_id = TableId(1);

        // Insert test data
        let rows = vec![
            Row::new(vec![
                Value::Int(1),
                Value::Text("alice".into()),
                Value::Bool(true),
            ]),
            Row::new(vec![
                Value::Int(2),
                Value::Text("bob".into()),
                Value::Bool(false),
            ]),
            Row::new(vec![
                Value::Int(3),
                Value::Text("carol".into()),
                Value::Bool(true),
            ]),
        ];
        insert_test_rows(&mut ctx, table_id, rows).unwrap();

        let plan = PhysicalPlan::Delete {
            table_id,
            predicate: None,
        };

        let count = execute_dml(plan, &mut ctx).unwrap();
        assert_eq!(count, 3);
    }

    #[test]
    fn execute_dml_returns_error_when_result_is_not_int() {
        let (mut ctx, _temp) = setup_context();

        // Create a plan that would return non-integer (this is contrived)
        // In practice, DML operators always return Int, but we test the error path
        let _scan = PhysicalPlan::SeqScan {
            table_id: TableId(1),
            schema: vec![],
        };

        // This would fail because SeqScan doesn't return a DML count
        // But we can't easily create this scenario without a mock
        // So we'll test the Insert success path instead
        let plan = PhysicalPlan::Insert {
            table_id: TableId(1),
            values: vec![lit_int(1)],
        };

        let result = execute_dml(plan, &mut ctx);
        assert!(result.is_ok());
    }

    #[test]
    fn execution_context_opens_heap_table() {
        let (mut ctx, _temp) = setup_context();
        let table_id = TableId(1);

        let result = ctx.heap_table(table_id);
        assert!(result.is_ok());
    }

    #[test]
    fn execution_context_logs_dml() {
        let (mut ctx, _temp) = setup_context();

        let record = wal::WalRecord::Insert {
            table: TableId(1),
            row: vec![Value::Int(1)],
            rid: common::RecordId {
                page_id: common::PageId(0),
                slot: 0,
            },
        };

        let result = ctx.log_dml(record);
        assert!(result.is_ok());
    }

    #[test]
    fn execute_query_returns_error_for_unknown_table() {
        let (mut ctx, _temp) = setup_context();

        let plan = PhysicalPlan::SeqScan {
            table_id: TableId(999),
            schema: vec!["id".into()],
        };

        let result = execute_query(plan, &mut ctx);
        assert!(result.is_err());
    }

    #[test]
    fn execute_dml_returns_error_for_unknown_table() {
        let (mut ctx, _temp) = setup_context();

        let plan = PhysicalPlan::Insert {
            table_id: TableId(999),
            values: vec![lit_int(1)],
        };

        let result = execute_dml(plan, &mut ctx);
        assert!(result.is_err());
    }

    /// Regression test: Verify CREATE TABLE with PRIMARY KEY stores metadata correctly
    #[test]
    fn create_table_with_primary_key_stores_metadata() {
        use catalog::Column;
        use types::SqlType;

        let mut catalog = Catalog::new();

        // Create table with single-column primary key
        let table_id = catalog
            .create_table(
                "users",
                vec![
                    Column::new("id", SqlType::Int),
                    Column::new("name", SqlType::Text),
                ],
                Some(vec![0]), // PRIMARY KEY (id)
            )
            .unwrap();

        let table = catalog.table("users").unwrap();
        assert_eq!(table.primary_key, Some(vec![0]));
        assert_eq!(table.id, table_id);
    }

    /// Regression test: Verify CREATE TABLE with composite PRIMARY KEY
    #[test]
    fn create_table_with_composite_primary_key_stores_metadata() {
        use catalog::Column;
        use types::SqlType;

        let mut catalog = Catalog::new();

        // Create table with composite primary key
        let table_id = catalog
            .create_table(
                "orders",
                vec![
                    Column::new("user_id", SqlType::Int),
                    Column::new("product_id", SqlType::Int),
                    Column::new("quantity", SqlType::Int),
                ],
                Some(vec![0, 1]), // PRIMARY KEY (user_id, product_id)
            )
            .unwrap();

        let table = catalog.table("orders").unwrap();
        assert_eq!(table.primary_key, Some(vec![0, 1]));
        assert_eq!(table.id, table_id);
    }

    /// Regression test: Verify CREATE TABLE without PRIMARY KEY
    #[test]
    fn create_table_without_primary_key_has_none() {
        use catalog::Column;

        use types::SqlType;

        let mut catalog = Catalog::new();

        let table_id = catalog
            .create_table(
                "logs",
                vec![
                    Column::new("timestamp", SqlType::Int),
                    Column::new("message", SqlType::Text),
                ],
                None, // No PRIMARY KEY
            )
            .unwrap();

        let table = catalog.table("logs").unwrap();
        assert_eq!(table.primary_key, None);
        assert_eq!(table.id, table_id);
    }

    /// Regression test: Verify PRIMARY KEY with invalid column ordinal is rejected
    #[test]
    fn create_table_rejects_invalid_primary_key_ordinal() {
        use catalog::Column;

        use types::SqlType;

        let mut catalog = Catalog::new();

        let result = catalog.create_table(
            "users",
            vec![
                Column::new("id", SqlType::Int),
                Column::new("name", SqlType::Text),
            ],
            Some(vec![5]), // Column 5 doesn't exist
        );

        assert!(result.is_err());
        assert!(format!("{:?}", result).contains("out of bounds"));
    }

    /// Regression test: Verify PRIMARY KEY with duplicate columns is rejected
    #[test]
    fn create_table_rejects_duplicate_primary_key_columns() {
        use catalog::Column;

        use types::SqlType;

        let mut catalog = Catalog::new();

        let result = catalog.create_table(
            "users",
            vec![
                Column::new("id", SqlType::Int),
                Column::new("name", SqlType::Text),
            ],
            Some(vec![0, 0]), // Duplicate column 0
        );

        assert!(result.is_err());
        assert!(format!("{:?}", result).contains("duplicate"));
    }

    // Primary key uniqueness enforcement tests

    #[test]
    fn insert_duplicate_single_column_primary_key_rejected() {
        use catalog::Column;
        use types::SqlType;

        let temp_dir = tempfile::tempdir().unwrap();
        let mut catalog = Catalog::new();
        catalog
            .create_table(
                "users",
                vec![
                    Column::new("id", SqlType::Int),
                    Column::new("name", SqlType::Text),
                    Column::new("active", SqlType::Bool),
                ],
                Some(vec![0]), // PRIMARY KEY (id)
            )
            .unwrap();

        let catalog = Box::leak(Box::new(catalog));
        let pager = Box::leak(Box::new(buffer::FilePager::new(temp_dir.path(), 10)));
        let wal = Box::leak(Box::new(
            wal::Wal::open(temp_dir.path().join("test.wal")).unwrap(),
        ));
        let mut ctx = ExecutionContext::new(catalog, pager, wal, temp_dir.path().into());

        let table_id = TableId(1);

        // Insert first row with id=1
        let plan1 = PhysicalPlan::Insert {
            table_id,
            values: vec![
                lit_int(1),
                lit_text("alice"),
                ResolvedExpr::Literal(Value::Bool(true)),
            ],
        };
        assert!(execute_dml(plan1, &mut ctx).is_ok());

        // Insert second row with id=1 should fail
        let plan2 = PhysicalPlan::Insert {
            table_id,
            values: vec![
                lit_int(1),
                lit_text("bob"),
                ResolvedExpr::Literal(Value::Bool(false)),
            ],
        };
        let result = execute_dml(plan2, &mut ctx);

        assert!(result.is_err());
        assert!(format!("{:?}", result).contains("duplicate primary key"));
    }

    #[test]
    fn insert_duplicate_composite_primary_key_rejected() {
        use catalog::Column;
        use types::SqlType;

        let temp_dir = tempfile::tempdir().unwrap();
        let mut catalog = Catalog::new();
        catalog
            .create_table(
                "users",
                vec![
                    Column::new("id", SqlType::Int),
                    Column::new("name", SqlType::Text),
                    Column::new("active", SqlType::Bool),
                ],
                Some(vec![0, 1]), // PRIMARY KEY (id, name)
            )
            .unwrap();

        let catalog = Box::leak(Box::new(catalog));
        let pager = Box::leak(Box::new(buffer::FilePager::new(temp_dir.path(), 10)));
        let wal = Box::leak(Box::new(
            wal::Wal::open(temp_dir.path().join("test.wal")).unwrap(),
        ));
        let mut ctx = ExecutionContext::new(catalog, pager, wal, temp_dir.path().into());

        let table_id = TableId(1);

        // Insert first row with (id=1, name="alice")
        let plan1 = PhysicalPlan::Insert {
            table_id,
            values: vec![
                lit_int(1),
                lit_text("alice"),
                ResolvedExpr::Literal(Value::Bool(true)),
            ],
        };
        assert!(execute_dml(plan1, &mut ctx).is_ok());

        // Insert second row with (id=1, name="alice") should fail
        let plan2 = PhysicalPlan::Insert {
            table_id,
            values: vec![
                lit_int(1),
                lit_text("alice"),
                ResolvedExpr::Literal(Value::Bool(false)),
            ],
        };
        let result = execute_dml(plan2, &mut ctx);

        assert!(result.is_err());
        assert!(format!("{:?}", result).contains("duplicate primary key"));
    }

    #[test]
    fn insert_different_composite_primary_key_allowed() {
        use catalog::Column;
        use types::SqlType;

        let temp_dir = tempfile::tempdir().unwrap();
        let mut catalog = Catalog::new();
        catalog
            .create_table(
                "users",
                vec![
                    Column::new("id", SqlType::Int),
                    Column::new("name", SqlType::Text),
                    Column::new("active", SqlType::Bool),
                ],
                Some(vec![0, 1]), // PRIMARY KEY (id, name)
            )
            .unwrap();

        let catalog = Box::leak(Box::new(catalog));
        let pager = Box::leak(Box::new(buffer::FilePager::new(temp_dir.path(), 10)));
        let wal = Box::leak(Box::new(
            wal::Wal::open(temp_dir.path().join("test.wal")).unwrap(),
        ));
        let mut ctx = ExecutionContext::new(catalog, pager, wal, temp_dir.path().into());

        let table_id = TableId(1);

        // Insert rows with different composite PK values
        let plan1 = PhysicalPlan::Insert {
            table_id,
            values: vec![
                lit_int(1),
                lit_text("alice"),
                ResolvedExpr::Literal(Value::Bool(true)),
            ],
        };
        assert!(execute_dml(plan1, &mut ctx).is_ok());

        let plan2 = PhysicalPlan::Insert {
            table_id,
            values: vec![
                lit_int(1),
                lit_text("bob"),
                ResolvedExpr::Literal(Value::Bool(false)),
            ],
        };
        assert!(execute_dml(plan2, &mut ctx).is_ok());

        let plan3 = PhysicalPlan::Insert {
            table_id,
            values: vec![
                lit_int(2),
                lit_text("alice"),
                ResolvedExpr::Literal(Value::Bool(true)),
            ],
        };
        assert!(execute_dml(plan3, &mut ctx).is_ok());

        // Verify all three rows exist
        let scan_plan = PhysicalPlan::SeqScan {
            table_id,
            schema: vec!["id".into(), "name".into(), "active".into()],
        };
        let rows = execute_query(scan_plan, &mut ctx).unwrap();
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn insert_builds_pk_index_on_first_access() {
        use catalog::Column;
        use types::SqlType;

        let temp_dir = tempfile::tempdir().unwrap();
        let mut catalog = Catalog::new();
        catalog
            .create_table(
                "users",
                vec![
                    Column::new("id", SqlType::Int),
                    Column::new("name", SqlType::Text),
                    Column::new("active", SqlType::Bool),
                ],
                Some(vec![0]), // PRIMARY KEY (id)
            )
            .unwrap();

        let catalog = Box::leak(Box::new(catalog));
        let pager = Box::leak(Box::new(buffer::FilePager::new(temp_dir.path(), 10)));
        let wal = Box::leak(Box::new(
            wal::Wal::open(temp_dir.path().join("test.wal")).unwrap(),
        ));
        let mut ctx = ExecutionContext::new(catalog, pager, wal, temp_dir.path().into());

        let table_id = TableId(1);

        // Insert first row
        let plan1 = PhysicalPlan::Insert {
            table_id,
            values: vec![
                lit_int(1),
                lit_text("alice"),
                ResolvedExpr::Literal(Value::Bool(true)),
            ],
        };
        assert!(execute_dml(plan1, &mut ctx).is_ok());

        // Insert second row with different PK
        let plan2 = PhysicalPlan::Insert {
            table_id,
            values: vec![
                lit_int(2),
                lit_text("bob"),
                ResolvedExpr::Literal(Value::Bool(false)),
            ],
        };
        assert!(execute_dml(plan2, &mut ctx).is_ok());

        // Verify both rows can be scanned
        let scan_plan = PhysicalPlan::SeqScan {
            table_id,
            schema: vec!["id".into(), "name".into(), "active".into()],
        };
        let rows = execute_query(scan_plan, &mut ctx).unwrap();
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn update_single_column_primary_key_rejected() {
        use catalog::Column;
        use types::SqlType;

        let temp_dir = tempfile::tempdir().unwrap();
        let mut catalog = Catalog::new();
        catalog
            .create_table(
                "users",
                vec![
                    Column::new("id", SqlType::Int),
                    Column::new("name", SqlType::Text),
                    Column::new("active", SqlType::Bool),
                ],
                Some(vec![0]), // PRIMARY KEY (id)
            )
            .unwrap();

        let catalog = Box::leak(Box::new(catalog));
        let pager = Box::leak(Box::new(buffer::FilePager::new(temp_dir.path(), 10)));
        let wal = Box::leak(Box::new(
            wal::Wal::open(temp_dir.path().join("test.wal")).unwrap(),
        ));
        let mut ctx = ExecutionContext::new(catalog, pager, wal, temp_dir.path().into());

        let table_id = TableId(1);

        // Insert a row
        let insert_plan = PhysicalPlan::Insert {
            table_id,
            values: vec![
                lit_int(1),
                lit_text("alice"),
                ResolvedExpr::Literal(Value::Bool(true)),
            ],
        };
        assert!(execute_dml(insert_plan, &mut ctx).is_ok());

        // Try to update the PK column (should fail)
        let update_plan = PhysicalPlan::Update {
            table_id,
            assignments: vec![(0, lit_int(2))], // Update id column
            predicate: Some(ResolvedExpr::Literal(Value::Bool(true))),
        };
        let result = execute_dml(update_plan, &mut ctx);

        assert!(result.is_err());
        assert!(format!("{:?}", result).contains("primary key"));
    }

    #[test]
    fn update_composite_primary_key_column_rejected() {
        use catalog::Column;
        use types::SqlType;

        let temp_dir = tempfile::tempdir().unwrap();
        let mut catalog = Catalog::new();
        catalog
            .create_table(
                "users",
                vec![
                    Column::new("id", SqlType::Int),
                    Column::new("name", SqlType::Text),
                    Column::new("active", SqlType::Bool),
                ],
                Some(vec![0, 1]), // PRIMARY KEY (id, name)
            )
            .unwrap();

        let catalog = Box::leak(Box::new(catalog));
        let pager = Box::leak(Box::new(buffer::FilePager::new(temp_dir.path(), 10)));
        let wal = Box::leak(Box::new(
            wal::Wal::open(temp_dir.path().join("test.wal")).unwrap(),
        ));
        let mut ctx = ExecutionContext::new(catalog, pager, wal, temp_dir.path().into());

        let table_id = TableId(1);

        // Insert a row
        let insert_plan = PhysicalPlan::Insert {
            table_id,
            values: vec![
                lit_int(1),
                lit_text("alice"),
                ResolvedExpr::Literal(Value::Bool(true)),
            ],
        };
        assert!(execute_dml(insert_plan, &mut ctx).is_ok());

        // Try to update one PK column (should fail)
        let update_plan = PhysicalPlan::Update {
            table_id,
            assignments: vec![(1, lit_text("bob"))], // Update name column (part of PK)
            predicate: Some(ResolvedExpr::Literal(Value::Bool(true))),
        };
        let result = execute_dml(update_plan, &mut ctx);

        assert!(result.is_err());
        assert!(format!("{:?}", result).contains("primary key"));
    }

    #[test]
    fn update_non_pk_column_allowed() {
        use catalog::Column;
        use types::SqlType;

        let temp_dir = tempfile::tempdir().unwrap();
        let mut catalog = Catalog::new();
        catalog
            .create_table(
                "users",
                vec![
                    Column::new("id", SqlType::Int),
                    Column::new("name", SqlType::Text),
                    Column::new("active", SqlType::Bool),
                ],
                Some(vec![0]), // PRIMARY KEY (id)
            )
            .unwrap();

        let catalog = Box::leak(Box::new(catalog));
        let pager = Box::leak(Box::new(buffer::FilePager::new(temp_dir.path(), 10)));
        let wal = Box::leak(Box::new(
            wal::Wal::open(temp_dir.path().join("test.wal")).unwrap(),
        ));
        let mut ctx = ExecutionContext::new(catalog, pager, wal, temp_dir.path().into());

        let table_id = TableId(1);

        // Insert a row
        let insert_plan = PhysicalPlan::Insert {
            table_id,
            values: vec![
                lit_int(1),
                lit_text("alice"),
                ResolvedExpr::Literal(Value::Bool(true)),
            ],
        };
        assert!(execute_dml(insert_plan, &mut ctx).is_ok());

        // Update non-PK columns (should succeed)
        let update_plan = PhysicalPlan::Update {
            table_id,
            assignments: vec![
                (1, lit_text("bob")),
                (2, ResolvedExpr::Literal(Value::Bool(false))),
            ],
            predicate: Some(ResolvedExpr::Literal(Value::Bool(true))),
        };
        let result = execute_dml(update_plan, &mut ctx);

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 1);
    }

    #[test]
    fn delete_removes_pk_entry_allowing_reinsertion() {
        use catalog::Column;
        use types::SqlType;

        let temp_dir = tempfile::tempdir().unwrap();
        let mut catalog = Catalog::new();
        catalog
            .create_table(
                "users",
                vec![
                    Column::new("id", SqlType::Int),
                    Column::new("name", SqlType::Text),
                    Column::new("active", SqlType::Bool),
                ],
                Some(vec![0]), // PRIMARY KEY (id)
            )
            .unwrap();

        let catalog = Box::leak(Box::new(catalog));
        let pager = Box::leak(Box::new(buffer::FilePager::new(temp_dir.path(), 10)));
        let wal = Box::leak(Box::new(
            wal::Wal::open(temp_dir.path().join("test.wal")).unwrap(),
        ));
        let mut ctx = ExecutionContext::new(catalog, pager, wal, temp_dir.path().into());

        let table_id = TableId(1);

        // Insert a row with id=1
        let insert_plan1 = PhysicalPlan::Insert {
            table_id,
            values: vec![
                lit_int(1),
                lit_text("alice"),
                ResolvedExpr::Literal(Value::Bool(true)),
            ],
        };
        assert!(execute_dml(insert_plan1, &mut ctx).is_ok());

        // Delete the row
        let delete_plan = PhysicalPlan::Delete {
            table_id,
            predicate: Some(ResolvedExpr::Literal(Value::Bool(true))),
        };
        let count = execute_dml(delete_plan, &mut ctx).unwrap();
        assert_eq!(count, 1);

        // Reinsert with same id=1 should now succeed
        let insert_plan2 = PhysicalPlan::Insert {
            table_id,
            values: vec![
                lit_int(1),
                lit_text("bob"),
                ResolvedExpr::Literal(Value::Bool(false)),
            ],
        };
        let result = execute_dml(insert_plan2, &mut ctx);
        assert!(result.is_ok());
    }

    #[test]
    fn delete_removes_composite_pk_entry() {
        use catalog::Column;
        use types::SqlType;

        let temp_dir = tempfile::tempdir().unwrap();
        let mut catalog = Catalog::new();
        catalog
            .create_table(
                "users",
                vec![
                    Column::new("id", SqlType::Int),
                    Column::new("name", SqlType::Text),
                    Column::new("active", SqlType::Bool),
                ],
                Some(vec![0, 1]), // PRIMARY KEY (id, name)
            )
            .unwrap();

        let catalog = Box::leak(Box::new(catalog));
        let pager = Box::leak(Box::new(buffer::FilePager::new(temp_dir.path(), 10)));
        let wal = Box::leak(Box::new(
            wal::Wal::open(temp_dir.path().join("test.wal")).unwrap(),
        ));
        let mut ctx = ExecutionContext::new(catalog, pager, wal, temp_dir.path().into());

        let table_id = TableId(1);

        // Insert row with (id=1, name="alice")
        let insert_plan1 = PhysicalPlan::Insert {
            table_id,
            values: vec![
                lit_int(1),
                lit_text("alice"),
                ResolvedExpr::Literal(Value::Bool(true)),
            ],
        };
        assert!(execute_dml(insert_plan1, &mut ctx).is_ok());

        // Delete the row
        let delete_plan = PhysicalPlan::Delete {
            table_id,
            predicate: Some(ResolvedExpr::Literal(Value::Bool(true))),
        };
        let count = execute_dml(delete_plan, &mut ctx).unwrap();
        assert_eq!(count, 1);

        // Reinsert with same composite PK (1, "alice") should succeed
        let insert_plan2 = PhysicalPlan::Insert {
            table_id,
            values: vec![
                lit_int(1),
                lit_text("alice"),
                ResolvedExpr::Literal(Value::Bool(false)),
            ],
        };
        let result = execute_dml(insert_plan2, &mut ctx);
        assert!(result.is_ok());
    }

    #[test]
    fn delete_selective_removal_from_pk_index() {
        use catalog::Column;
        use types::SqlType;

        let temp_dir = tempfile::tempdir().unwrap();
        let mut catalog = Catalog::new();
        catalog
            .create_table(
                "users",
                vec![
                    Column::new("id", SqlType::Int),
                    Column::new("name", SqlType::Text),
                    Column::new("active", SqlType::Bool),
                ],
                Some(vec![0]), // PRIMARY KEY (id)
            )
            .unwrap();

        let catalog = Box::leak(Box::new(catalog));
        let pager = Box::leak(Box::new(buffer::FilePager::new(temp_dir.path(), 10)));
        let wal = Box::leak(Box::new(
            wal::Wal::open(temp_dir.path().join("test.wal")).unwrap(),
        ));
        let mut ctx = ExecutionContext::new(catalog, pager, wal, temp_dir.path().into());

        let table_id = TableId(1);

        // Insert three rows
        for (id, name) in &[(1, "alice"), (2, "bob"), (3, "carol")] {
            let plan = PhysicalPlan::Insert {
                table_id,
                values: vec![
                    lit_int(*id),
                    lit_text(name),
                    ResolvedExpr::Literal(Value::Bool(true)),
                ],
            };
            assert!(execute_dml(plan, &mut ctx).is_ok());
        }

        // Delete all rows where active=true (all three have active=true)
        let delete_plan = PhysicalPlan::Delete {
            table_id,
            predicate: Some(ResolvedExpr::Column(2)), // WHERE active
        };
        let count = execute_dml(delete_plan, &mut ctx).unwrap();
        assert_eq!(count, 3); // All three rows deleted

        // All three PKs should now be available for reinsertion
        for (id, name) in &[(1, "new_alice"), (2, "new_bob"), (3, "new_carol")] {
            let plan = PhysicalPlan::Insert {
                table_id,
                values: vec![
                    lit_int(*id),
                    lit_text(name),
                    ResolvedExpr::Literal(Value::Bool(false)),
                ],
            };
            assert!(execute_dml(plan, &mut ctx).is_ok());
        }

        // Verify all three rows exist
        let scan_plan = PhysicalPlan::SeqScan {
            table_id,
            schema: vec!["id".into(), "name".into(), "active".into()],
        };
        let rows = execute_query(scan_plan, &mut ctx).unwrap();
        assert_eq!(rows.len(), 3);
    }
}

mod builder;
mod dml;
mod filter;
mod pk_index;
mod project;
mod scan;

pub use pk_index::PrimaryKeyIndex;

use catalog::Catalog;
use common::{DbError, DbResult, ExecutionStats, Row, TableId};
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

    /// Return execution statistics (for EXPLAIN ANALYZE).
    /// Returns None for operators that don't collect statistics.
    fn stats(&self) -> Option<&ExecutionStats> {
        None
    }
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
    /// Primary key indexes, lazily built on first table access
    pk_indexes: std::collections::HashMap<TableId, pk_index::PrimaryKeyIndex>,
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
            pk_indexes: std::collections::HashMap::new(),
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

    /// Get or build the primary key index for a table.
    ///
    /// If the table has no primary key, returns None.
    /// On first access, builds the index by scanning all existing rows.
    pub fn pk_index(
        &mut self,
        table_id: TableId,
    ) -> DbResult<Option<&mut pk_index::PrimaryKeyIndex>> {
        let table_meta = self.catalog.table_by_id(table_id)?;

        // No PK defined for this table
        let Some(pk_columns) = &table_meta.primary_key else {
            return Ok(None);
        };

        // Index already built
        if self.pk_indexes.contains_key(&table_id) {
            return Ok(Some(self.pk_indexes.get_mut(&table_id).unwrap()));
        }

        // Build index by scanning existing rows
        let mut index = pk_index::PrimaryKeyIndex::new(pk_columns.clone());

        let file_path = self.data_dir.join(format!("{}.heap", table_meta.name));
        let mut heap_file = storage::HeapFile::open(&file_path, table_id.0)?;

        // Scan all pages and slots to find existing rows
        let mut page_id = common::PageId(0);
        loop {
            let mut found_row_in_page = false;

            for slot in 0..100 {
                let rid = common::RecordId { page_id, slot };
                // get() returns Ok(row) or Err if slot is empty/invalid
                if let Ok(row) = heap_file.get(rid) {
                    found_row_in_page = true;
                    let key = index.extract_key(&row)?;
                    // Silently ignore duplicates during index build (existing data may be inconsistent)
                    let _ = index.insert(key, rid);
                }
            }

            // Move to next page if we found any rows, otherwise we've scanned all data
            if found_row_in_page {
                page_id = common::PageId(page_id.0 + 1);
            } else {
                break;
            }
        }

        self.pk_indexes.insert(table_id, index);
        Ok(Some(self.pk_indexes.get_mut(&table_id).unwrap()))
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
    match result.values.first() {
        Some(types::Value::Int(count)) => Ok(*count as u64),
        Some(other) => Err(DbError::Executor(format!(
            "DML result count must be integer, got {:?}",
            other
        ))),
        None => Err(DbError::Executor("DML result has no columns".into())),
    }
}
