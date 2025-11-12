//! Demonstration of test macro usage to replace boilerplate.
//!
//! This file shows how the new test macros from `testsupport` can dramatically
//! reduce test setup code. Compare the "before" and "after" versions.

#[cfg(test)]
mod tests {
    use crate::*;
    use catalog::Column;
    use common::TableId;
    use planner::ResolvedExpr;
    use testsupport::prelude::*;
    use types::{SqlType, Value};

    /// BEFORE: Traditional test setup (17 lines of boilerplate)
    #[test]
    fn test_primary_key_enforcement_old_style() {
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
                lit!(int: 1),
                lit!(text: "alice"),
                ResolvedExpr::Literal(Value::Bool(true)),
            ],
        };
        assert!(execute_dml(plan1, &mut ctx).is_ok());

        // Insert second row with id=1 should fail
        let plan2 = PhysicalPlan::Insert {
            table_id,
            values: vec![
                lit!(int: 1),
                lit!(text: "bob"),
                ResolvedExpr::Literal(Value::Bool(false)),
            ],
        };
        let result = execute_dml(plan2, &mut ctx);

        assert!(result.is_err());
        assert!(format!("{:?}", result).contains("duplicate primary key"));
    }

    /// AFTER: Using test_db macro (reduced to 3 lines of setup!)
    #[test]
    fn test_primary_key_enforcement_new_style() {
        // Just 3 lines to set up the entire test database!
        test_db!(test_ctx, table: "users",
                 cols: ["id" => SqlType::Int, "name" => SqlType::Text, "active" => SqlType::Bool],
                 pk: [0]);
        let mut ctx = test_ctx.execution_context();
        let table_id = TableId(1);

        // Insert first row with id=1
        let plan1 = PhysicalPlan::Insert {
            table_id,
            values: vec![
                lit!(int: 1),
                lit!(text: "alice"),
                ResolvedExpr::Literal(Value::Bool(true)),
            ],
        };
        assert!(execute_dml(plan1, &mut ctx).is_ok());

        // Insert second row with id=1 should fail
        let plan2 = PhysicalPlan::Insert {
            table_id,
            values: vec![
                lit!(int: 1),
                lit!(text: "bob"),
                ResolvedExpr::Literal(Value::Bool(false)),
            ],
        };
        let result = execute_dml(plan2, &mut ctx);

        assert!(result.is_err());
        assert!(format!("{:?}", result).contains("duplicate primary key"));
    }

    /// BEFORE: Composite primary key test (17 lines of boilerplate)
    #[test]
    fn test_composite_pk_old_style() {
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

        // Test logic here...
        assert!(ctx.catalog.table("users").is_ok());
    }

    /// AFTER: Using test_db macro with composite primary key
    #[test]
    fn test_composite_pk_new_style() {
        // Just one line for setup with composite PK!
        test_db!(test_ctx, table: "users",
                 cols: ["id" => SqlType::Int, "name" => SqlType::Text, "active" => SqlType::Bool],
                 pk: [0, 1]);
        let mut ctx = test_ctx.execution_context();
        let table_id = TableId(1);

        // Test logic here...
        assert!(ctx.catalog.table("users").is_ok());
    }

    /// Demonstrating the row! macro for cleaner row construction
    #[test]
    fn test_row_macro_usage() {
        test_db!(test_ctx, table: "users",
                 cols: ["id" => SqlType::Int, "name" => SqlType::Text, "active" => SqlType::Bool]);
        let mut ctx = test_ctx.execution_context();

        // BEFORE: Verbose row construction
        let row_old = common::Row::new(vec![
            Value::Int(1),
            Value::Text("Alice".to_string()),
            Value::Bool(true),
        ]);

        // AFTER: Using row! macro
        let row_new = row![
            Value::Int(1),
            Value::Text("Alice".to_string()),
            Value::Bool(true)
        ];

        // Even better: type-specific variants
        let row_ints = row![int: 1, 2, 3];
        let row_text = row![text: "alice", "bob", "charlie"];
        let row_bool = row![bool: true, false, true];

        assert_eq!(row_old.values.len(), 3);
        assert_eq!(row_new.values.len(), 3);
        assert_eq!(row_ints.values.len(), 3);
        assert_eq!(row_text.values.len(), 3);
        assert_eq!(row_bool.values.len(), 3);
    }

    /// Demonstrating multiple tables setup
    #[test]
    fn test_multiple_tables_setup() {
        // Set up multiple tables in one macro call
        test_db!(test_ctx, tables: [
            ("users", ["id" => SqlType::Int, "name" => SqlType::Text]),
            ("posts", ["id" => SqlType::Int, "title" => SqlType::Text, "user_id" => SqlType::Int]),
            ("comments", ["id" => SqlType::Int, "post_id" => SqlType::Int, "text" => SqlType::Text])
        ]);

        let ctx = test_ctx.execution_context();
        assert!(ctx.catalog.table("users").is_ok());
        assert!(ctx.catalog.table("posts").is_ok());
        assert!(ctx.catalog.table("comments").is_ok());
    }
}
