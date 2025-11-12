//! Integration tests demonstrating testsupport usage.

use testsupport::prelude::*;
use types::Value;

#[test]
fn test_run_sql_script_basic() {
    let output = run_sql_script(
        r#"
        CREATE TABLE users (id INT, name TEXT, age INT);
        INSERT INTO users VALUES (1, 'Alice', 30);
        INSERT INTO users VALUES (2, 'Bob', 25);
        SELECT * FROM users;
    "#,
    )
    .unwrap();

    assert!(output.contains("Created table 'users'"));
    assert!(output.contains("1 row(s) affected"));
    assert!(output.contains("Alice"));
    assert!(output.contains("Bob"));
}

#[test]
fn test_run_sql_script_with_filter() {
    let output = run_sql_script(
        r#"
        CREATE TABLE users (id INT, name TEXT, age INT);
        INSERT INTO users VALUES (1, 'Alice', 30);
        INSERT INTO users VALUES (2, 'Bob', 25);
        INSERT INTO users VALUES (3, 'Charlie', 35);
        SELECT * FROM users WHERE age > 25;
    "#,
    )
    .unwrap();

    assert!(output.contains("Alice"));
    assert!(!output.contains("Bob")); // Bob's age is 25, not > 25
    assert!(output.contains("Charlie"));
}

#[test]
fn test_test_context_isolation() {
    use catalog::Column;
    use types::SqlType;

    // Create two separate contexts
    let mut ctx1 = TestContext::new().unwrap();
    let ctx2 = TestContext::new().unwrap();

    // Create table in ctx1
    ctx1.catalog_mut()
        .create_table(
            "users",
            vec![
                Column::new("id".to_string(), SqlType::Int),
                Column::new("name".to_string(), SqlType::Text),
            ],
            None,
        )
        .unwrap();

    // ctx1 should have the table
    assert!(ctx1.catalog().table("users").is_ok());

    // ctx2 should not have the table (isolated)
    assert!(ctx2.catalog().table("users").is_err());
}

#[test]
fn test_fixtures_and_assertions() {
    use common::TableId;

    // Create a table using simple catalog helper
    let catalog = create_simple_catalog();
    let mut ctx = TestContext::with_catalog(catalog).unwrap();

    // Insert test rows using helpers
    let rows = vec![
        mixed_row(vec![
            Value::Int(1),
            Value::Text("Alice".into()),
            Value::Int(30),
        ]),
        mixed_row(vec![
            Value::Int(2),
            Value::Text("Bob".into()),
            Value::Int(25),
        ]),
    ];

    let mut exec_ctx = ctx.execution_context();
    insert_test_rows(&mut exec_ctx, TableId(1), rows.clone()).unwrap();

    // Verify rows using assertions
    assert_rows_equal(&rows[0], &rows[0]);
    assert_row_sets_equal(&rows, &rows);
}

#[test]
fn test_row_builders() {
    let int_r = int_row(&[1, 2, 3]);
    assert_eq!(int_r.values.len(), 3);
    assert_eq!(int_r.values[0], Value::Int(1));

    let text_r = text_row(&["a", "b", "c"]);
    assert_eq!(text_r.values.len(), 3);
    assert_eq!(text_r.values[0], Value::Text("a".into()));

    let bool_r = bool_row(&[true, false]);
    assert_eq!(bool_r.values.len(), 2);
    assert_eq!(bool_r.values[0], Value::Bool(true));

    let null_r = null_row(3);
    assert_eq!(null_r.values.len(), 3);
    assert_eq!(null_r.values[0], Value::Null);
}

#[test]
fn test_expression_builders() {
    use expr::BinaryOp;

    let lit = lit_int(42);
    match lit {
        planner::ResolvedExpr::Literal(Value::Int(42)) => {}
        _ => panic!("Expected literal int"),
    }

    let column = col(0);
    match column {
        planner::ResolvedExpr::Column(0) => {}
        _ => panic!("Expected column reference"),
    }

    let expr = binary(col(0), BinaryOp::Eq, lit_int(42));
    match expr {
        planner::ResolvedExpr::Binary { .. } => {}
        _ => panic!("Expected binary expression"),
    }
}

#[test]
fn test_sample_data() {
    use testsupport::fixtures::data::*;

    let users = sample_users();
    assert_eq!(users.len(), 3);
    assert_eq!(users[0].values[0], Value::Int(1));

    let products = sample_products();
    assert_eq!(products.len(), 3);

    let orders = sample_orders();
    assert_eq!(orders.len(), 3);
}

#[test]
fn test_schemas() {
    use testsupport::fixtures::schemas::*;

    let users = users_schema();
    assert_eq!(users.columns().len(), 3);

    let products = products_schema();
    assert_eq!(products.columns().len(), 3);

    let orders = orders_schema();
    assert_eq!(orders.columns().len(), 4);
}

#[test]
fn test_error_assertions() {
    use common::DbError;

    let result: Result<(), DbError> = Err(DbError::Executor("test error".into()));
    assert_error_contains(result, "test error");

    let result2: Result<(), DbError> = Err(DbError::Executor("table not found".into()));
    assert_executor_error(result2, "table not found");
}

#[test]
fn test_snapshot_testing_pattern() {
    // This demonstrates the intended usage pattern with insta
    let output = run_sql_script(
        r#"
        CREATE TABLE products (id INT, name TEXT, price INT);
        INSERT INTO products VALUES (1, 'Laptop', 1000);
        INSERT INTO products VALUES (2, 'Mouse', 25);
        SELECT * FROM products WHERE price > 100;
    "#,
    )
    .unwrap();

    // In real tests, you would use:
    // insta::assert_snapshot!(output);
    // For this test, we just verify the output is reasonable
    assert!(output.contains("Created table 'products'"));
    assert!(output.contains("Laptop"));
    assert!(!output.contains("Mouse")); // Mouse price is 25, not > 100
}

#[test]
fn test_multiple_statements_same_context() {
    let mut ctx = TestContext::new().unwrap();

    // First statement creates table
    let output1 = run_sql_script_with_context(
        "CREATE TABLE users (id INT, name TEXT);",
        &mut ctx,
    )
    .unwrap();
    assert!(output1.contains("Created table 'users'"));

    // Second statement inserts data
    let output2 = run_sql_script_with_context(
        "INSERT INTO users VALUES (1, 'Alice');",
        &mut ctx,
    )
    .unwrap();
    assert!(output2.contains("1 row(s) affected"));

    // Third statement queries data
    let output3 = run_sql_script_with_context("SELECT * FROM users;", &mut ctx).unwrap();
    assert!(output3.contains("Alice"));
}

// Property-based tests demonstrating proptest usage
#[cfg(test)]
mod proptest_tests {
    use proptest::prelude::*;
    use testsupport::proptest_generators::*;

    // Configure proptest to run fewer cases for faster tests
    // Default is 256 cases, we use 50 for quick feedback
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(50))]

        #[test]
        fn prop_row_clone_equals(row in arb_row()) {
            let cloned = row.clone();
            assert_eq!(row.values, cloned.values);
        }

        #[test]
        fn prop_value_equals_self(value in arb_value()) {
            assert_eq!(value, value);
        }

        #[test]
        fn prop_row_with_len_has_correct_length(len in 1usize..20) {
            let strategy = arb_row_with_len(len);
            proptest!(|(row in strategy)| {
                prop_assert_eq!(row.values.len(), len);
            });
        }
    }
}
