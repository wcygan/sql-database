//! Integration tests demonstrating testsupport usage.

use testsupport::prelude::*;
use types::Value;

#[tokio::test]
async fn test_run_sql_script_basic() {
    let output = run_sql_script(
        r#"
        CREATE TABLE users (id INT, name TEXT, age INT);
        INSERT INTO users VALUES (1, 'Alice', 30);
        INSERT INTO users VALUES (2, 'Bob', 25);
        SELECT * FROM users;
    "#,
    )
    .await
    .unwrap();

    assert!(output.contains("Created table 'users'"));
    assert!(output.contains("1 row(s) affected"));
    assert!(output.contains("Alice"));
    assert!(output.contains("Bob"));
}

#[tokio::test]
async fn test_run_sql_script_with_filter() {
    let output = run_sql_script(
        r#"
        CREATE TABLE users (id INT, name TEXT, age INT);
        INSERT INTO users VALUES (1, 'Alice', 30);
        INSERT INTO users VALUES (2, 'Bob', 25);
        INSERT INTO users VALUES (3, 'Charlie', 35);
        SELECT * FROM users WHERE age > 25;
    "#,
    )
    .await
    .unwrap();

    assert!(output.contains("Alice"));
    assert!(!output.contains("Bob")); // Bob's age is 25, not > 25
    assert!(output.contains("Charlie"));
}

#[tokio::test]
async fn test_test_context_isolation() {
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

#[tokio::test]
async fn test_fixtures_and_assertions() {
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

#[tokio::test]
async fn test_row_builders() {
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

#[tokio::test]
async fn test_expression_builders() {
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

#[tokio::test]
async fn test_sample_data() {
    use testsupport::fixtures::data::*;

    let users = sample_users();
    assert_eq!(users.len(), 3);
    assert_eq!(users[0].values[0], Value::Int(1));

    let products = sample_products();
    assert_eq!(products.len(), 3);

    let orders = sample_orders();
    assert_eq!(orders.len(), 3);
}

#[tokio::test]
async fn test_schemas() {
    use testsupport::fixtures::schemas::*;

    let users = users_schema();
    assert_eq!(users.columns().len(), 3);

    let products = products_schema();
    assert_eq!(products.columns().len(), 3);

    let orders = orders_schema();
    assert_eq!(orders.columns().len(), 4);
}

#[tokio::test]
async fn test_error_assertions() {
    use common::DbError;

    let result: Result<(), DbError> = Err(DbError::Executor("test error".into()));
    assert_error_contains(result, "test error");

    let result2: Result<(), DbError> = Err(DbError::Executor("table not found".into()));
    assert_executor_error(result2, "table not found");
}

#[tokio::test]
async fn test_snapshot_testing_pattern() {
    // This demonstrates the intended usage pattern with insta
    let output = run_sql_script(
        r#"
        CREATE TABLE products (id INT, name TEXT, price INT);
        INSERT INTO products VALUES (1, 'Laptop', 1000);
        INSERT INTO products VALUES (2, 'Mouse', 25);
        SELECT * FROM products WHERE price > 100;
    "#,
    )
    .await
    .unwrap();

    // In real tests, you would use:
    // insta::assert_snapshot!(output);
    // For this test, we just verify the output is reasonable
    assert!(output.contains("Created table 'products'"));
    assert!(output.contains("Laptop"));
    assert!(!output.contains("Mouse")); // Mouse price is 25, not > 100
}

#[tokio::test]
async fn test_sql_script_create_and_drop_table() {
    let output = run_sql_script(
        r#"
        CREATE TABLE temp_table (id INT, data TEXT);
        INSERT INTO temp_table VALUES (1, 'test');
        SELECT * FROM temp_table;
        DROP TABLE temp_table;
    "#,
    )
    .await
    .unwrap();

    assert!(output.contains("Created table 'temp_table'"));
    assert!(output.contains("test"));
    assert!(output.contains("Dropped table 'temp_table'"));
}

#[tokio::test]
async fn test_sql_script_multiple_inserts() {
    let output = run_sql_script(
        r#"
        CREATE TABLE numbers (id INT, value INT);
        INSERT INTO numbers VALUES (1, 10);
        INSERT INTO numbers VALUES (2, 20);
        INSERT INTO numbers VALUES (3, 30);
        INSERT INTO numbers VALUES (4, 40);
        INSERT INTO numbers VALUES (5, 50);
        SELECT * FROM numbers;
    "#,
    )
    .await
    .unwrap();

    assert!(output.contains("Created table 'numbers'"));
    for i in 1..=5 {
        assert!(output.contains(&format!("{}", i * 10)));
    }
}

#[tokio::test]
async fn test_sql_script_with_filters_and_projections() {
    let output = run_sql_script(
        r#"
        CREATE TABLE employees (id INT, name TEXT, salary INT, department TEXT);
        INSERT INTO employees VALUES (1, 'Alice', 75000, 'Engineering');
        INSERT INTO employees VALUES (2, 'Bob', 65000, 'Marketing');
        INSERT INTO employees VALUES (3, 'Charlie', 80000, 'Engineering');
        INSERT INTO employees VALUES (4, 'Diana', 70000, 'Sales');
        SELECT name FROM employees WHERE salary > 70000;
    "#,
    )
    .await
    .unwrap();

    assert!(output.contains("Alice"));
    assert!(output.contains("Charlie"));
    assert!(!output.contains("Bob"));
    assert!(!output.contains("Diana"));
}

#[tokio::test]
async fn test_sql_script_create_index() {
    let output = run_sql_script(
        r#"
        CREATE TABLE indexed_table (id INT, value TEXT);
        CREATE INDEX idx_value ON indexed_table(value);
        INSERT INTO indexed_table VALUES (1, 'apple');
        INSERT INTO indexed_table VALUES (2, 'banana');
        INSERT INTO indexed_table VALUES (3, 'cherry');
        SELECT * FROM indexed_table WHERE value = 'banana';
    "#,
    )
    .await
    .unwrap();

    assert!(output.contains("Created table 'indexed_table'"));
    assert!(output.contains("Created index 'idx_value'"));
    assert!(output.contains("banana"));
    assert!(!output.contains("apple"));
    assert!(!output.contains("cherry"));
}

#[tokio::test]
async fn test_sql_script_drop_index() {
    let output = run_sql_script(
        r#"
        CREATE TABLE test_table (id INT, name TEXT);
        CREATE INDEX test_idx ON test_table(name);
        INSERT INTO test_table VALUES (1, 'test');
        DROP INDEX test_idx;
    "#,
    )
    .await
    .unwrap();

    assert!(output.contains("Created table 'test_table'"));
    assert!(output.contains("Created index 'test_idx'"));
    assert!(output.contains("Dropped index 'test_idx'"));
}

#[tokio::test]
async fn test_sql_script_complex_where_clauses() {
    let output = run_sql_script(
        r#"
        CREATE TABLE inventory (id INT, product TEXT, quantity INT, price INT);
        INSERT INTO inventory VALUES (1, 'Widget', 100, 5);
        INSERT INTO inventory VALUES (2, 'Gadget', 50, 15);
        INSERT INTO inventory VALUES (3, 'Doohickey', 75, 10);
        INSERT INTO inventory VALUES (4, 'Thingamajig', 25, 20);
        SELECT product FROM inventory WHERE quantity > 30;
    "#,
    )
    .await
    .unwrap();

    assert!(output.contains("Widget"));
    assert!(output.contains("Gadget"));
    assert!(output.contains("Doohickey"));
    assert!(!output.contains("Thingamajig"));
}

#[tokio::test]
async fn test_sql_script_with_nulls() {
    let output = run_sql_script(
        r#"
        CREATE TABLE nullable_data (id INT, value TEXT, optional INT);
        INSERT INTO nullable_data VALUES (1, 'has_value', 42);
        INSERT INTO nullable_data VALUES (2, 'no_value', NULL);
        SELECT * FROM nullable_data;
    "#,
    )
    .await
    .unwrap();

    assert!(output.contains("has_value"));
    assert!(output.contains("no_value"));
    assert!(output.contains("42"));
}

#[tokio::test]
async fn test_sql_script_primary_key_constraint() {
    let output = run_sql_script(
        r#"
        CREATE TABLE users_pk (id INT PRIMARY KEY, username TEXT, email TEXT);
        INSERT INTO users_pk VALUES (1, 'alice', 'alice@example.com');
        INSERT INTO users_pk VALUES (2, 'bob', 'bob@example.com');
        SELECT * FROM users_pk;
    "#,
    )
    .await
    .unwrap();

    assert!(output.contains("Created table 'users_pk'"));
    assert!(output.contains("alice"));
    assert!(output.contains("bob"));
}

#[tokio::test]
async fn test_sql_script_primary_key_declared() {
    // Test that tables with PRIMARY KEY declarations can be created
    // Note: PK constraint enforcement is not yet implemented, so duplicate inserts will succeed
    let output = run_sql_script(
        r#"
        CREATE TABLE pk_test (id INT PRIMARY KEY, data TEXT);
        INSERT INTO pk_test VALUES (1, 'first');
        INSERT INTO pk_test VALUES (2, 'second');
        SELECT * FROM pk_test;
    "#,
    )
    .await
    .unwrap();

    assert!(output.contains("Created table 'pk_test'"));
    assert!(output.contains("first"));
    assert!(output.contains("second"));
}

#[tokio::test]
async fn test_sql_script_multiple_tables() {
    let output = run_sql_script(
        r#"
        CREATE TABLE customers (id INT, name TEXT);
        CREATE TABLE orders (id INT, customer_id INT, amount INT);
        INSERT INTO customers VALUES (1, 'Alice');
        INSERT INTO customers VALUES (2, 'Bob');
        INSERT INTO orders VALUES (101, 1, 50);
        INSERT INTO orders VALUES (102, 2, 75);
        SELECT * FROM customers;
        SELECT * FROM orders;
    "#,
    )
    .await
    .unwrap();

    assert!(output.contains("Created table 'customers'"));
    assert!(output.contains("Created table 'orders'"));
    assert!(output.contains("Alice"));
    assert!(output.contains("Bob"));
    assert!(output.contains("101"));
    assert!(output.contains("102"));
}

#[tokio::test]
async fn test_sql_script_empty_table_query() {
    let output = run_sql_script(
        r#"
        CREATE TABLE empty_table (id INT, value TEXT);
        SELECT * FROM empty_table;
    "#,
    )
    .await
    .unwrap();

    assert!(output.contains("Created table 'empty_table'"));
    // Empty result set - just verify no errors occurred
}

#[tokio::test]
async fn test_sql_script_boolean_columns() {
    let output = run_sql_script(
        r#"
        CREATE TABLE flags (id INT, name TEXT, active BOOL);
        INSERT INTO flags VALUES (1, 'feature_a', TRUE);
        INSERT INTO flags VALUES (2, 'feature_b', FALSE);
        INSERT INTO flags VALUES (3, 'feature_c', TRUE);
        SELECT * FROM flags WHERE active = TRUE;
    "#,
    )
    .await
    .unwrap();

    assert!(output.contains("feature_a"));
    assert!(output.contains("feature_c"));
    assert!(!output.contains("feature_b"));
}

#[tokio::test]
async fn test_sql_script_case_sensitivity() {
    let output = run_sql_script(
        r#"
        CREATE TABLE MixedCase (Id INT, Name TEXT);
        INSERT INTO MixedCase VALUES (1, 'Test');
        SELECT * FROM mixedcase;
    "#,
    )
    .await
    .unwrap();

    assert!(output.contains("Created table 'mixedcase'"));
    assert!(output.contains("Test"));
}

#[tokio::test]
async fn test_sql_script_wildcard_projection() {
    let output = run_sql_script(
        r#"
        CREATE TABLE all_columns (col1 INT, col2 TEXT, col3 INT, col4 BOOL);
        INSERT INTO all_columns VALUES (1, 'test', 42, TRUE);
        SELECT * FROM all_columns;
    "#,
    )
    .await
    .unwrap();

    assert!(output.contains("test"));
    assert!(output.contains("42"));
}

#[tokio::test]
async fn test_sql_script_sequential_operations() {
    let output = run_sql_script(
        r#"
        CREATE TABLE counter (id INT, count INT);
        INSERT INTO counter VALUES (1, 0);
        SELECT * FROM counter;
        INSERT INTO counter VALUES (2, 1);
        SELECT * FROM counter;
        INSERT INTO counter VALUES (3, 2);
        SELECT * FROM counter;
    "#,
    )
    .await
    .unwrap();

    // Verify all inserts and selects occurred
    assert!(output.contains("Created table 'counter'"));
    for i in 0..=2 {
        assert!(output.contains(&format!("{}", i)));
    }
}

#[tokio::test]
async fn test_multiple_statements_same_context() {
    use database::Database;

    let temp_dir = tempfile::tempdir().unwrap();
    let db = Database::new(temp_dir.path(), "catalog.json", "test.wal", 10)
        .await
        .unwrap();

    // First statement creates table
    let output1 = run_sql_script_with_db("CREATE TABLE users (id INT, name TEXT);", &db)
        .await
        .unwrap();
    assert!(output1.contains("Created table 'users'"));

    // Second statement inserts data
    let output2 = run_sql_script_with_db("INSERT INTO users VALUES (1, 'Alice');", &db)
        .await
        .unwrap();
    assert!(output2.contains("1 row(s) affected"));

    // Third statement queries data
    let output3 = run_sql_script_with_db("SELECT * FROM users;", &db)
        .await
        .unwrap();
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
