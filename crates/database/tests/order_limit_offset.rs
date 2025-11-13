//! Integration tests for ORDER BY, LIMIT, and OFFSET functionality.

use anyhow::Result;
use database::{Database, QueryResult};
use num_integer::Integer;
use types::Value;

#[tokio::test]
async fn order_by_single_column_ascending() -> Result<()> {
    let temp_dir = tempfile::tempdir()?;
    let db = Database::new(temp_dir.path(), "catalog.json", "test.wal", 10).await?;

    // Create table and insert test data
    db.execute("CREATE TABLE users (id INT PRIMARY KEY, name TEXT, age INT)")
        .await?;
    db.execute("INSERT INTO users VALUES (3, 'Charlie', 35)")
        .await?;
    db.execute("INSERT INTO users VALUES (1, 'Alice', 25)").await?;
    db.execute("INSERT INTO users VALUES (2, 'Bob', 30)").await?;

    // Query with ORDER BY
    let result = db.execute("SELECT * FROM users ORDER BY age ASC").await?;

    match result {
        QueryResult::Rows { rows, .. } => {
            assert_eq!(rows.len(), 3);
            // Should be ordered by age: Alice (25), Bob (30), Charlie (35)
            assert_eq!(rows[0].values[0], Value::Int(1)); // Alice's id
            assert_eq!(rows[1].values[0], Value::Int(2)); // Bob's id
            assert_eq!(rows[2].values[0], Value::Int(3)); // Charlie's id
        }
        _ => panic!("Expected Rows result"),
    }

    Ok(())
}

#[tokio::test]
async fn order_by_single_column_descending() -> Result<()> {
    let temp_dir = tempfile::tempdir()?;
    let db = Database::new(temp_dir.path(), "catalog.json", "test.wal", 10).await?;

    db.execute("CREATE TABLE products (id INT PRIMARY KEY, name TEXT, price INT)")
        .await?;
    db.execute("INSERT INTO products VALUES (1, 'Widget', 100)")
        .await?;
    db.execute("INSERT INTO products VALUES (2, 'Gadget', 150)")
        .await?;
    db.execute("INSERT INTO products VALUES (3, 'Doohickey', 75)")
        .await?;

    let result = db
        .execute("SELECT * FROM products ORDER BY price DESC")
        .await?;

    match result {
        QueryResult::Rows { rows, .. } => {
            assert_eq!(rows.len(), 3);
            // Should be ordered by price descending: 150, 100, 75
            assert_eq!(rows[0].values[0], Value::Int(2)); // Gadget
            assert_eq!(rows[1].values[0], Value::Int(1)); // Widget
            assert_eq!(rows[2].values[0], Value::Int(3)); // Doohickey
        }
        _ => panic!("Expected Rows result"),
    }

    Ok(())
}

#[tokio::test]
async fn order_by_multiple_columns() -> Result<()> {
    let temp_dir = tempfile::tempdir()?;
    let db = Database::new(temp_dir.path(), "catalog.json", "test.wal", 10).await?;

    db.execute("CREATE TABLE employees (id INT PRIMARY KEY, department TEXT, salary INT)")
        .await?;
    db.execute("INSERT INTO employees VALUES (1, 'Engineering', 100000)")
        .await?;
    db.execute("INSERT INTO employees VALUES (2, 'Sales', 80000)")
        .await?;
    db.execute("INSERT INTO employees VALUES (3, 'Engineering', 120000)")
        .await?;
    db.execute("INSERT INTO employees VALUES (4, 'Sales', 90000)")
        .await?;

    // Order by department ASC, then salary DESC within each department
    let result = db
        .execute("SELECT * FROM employees ORDER BY department ASC, salary DESC")
        .await?;

    match result {
        QueryResult::Rows { rows, .. } => {
            assert_eq!(rows.len(), 4);
            // Engineering first (alphabetically), highest salary first
            assert_eq!(rows[0].values[0], Value::Int(3)); // Engineering, 120000
            assert_eq!(rows[1].values[0], Value::Int(1)); // Engineering, 100000
            // Then Sales, highest salary first
            assert_eq!(rows[2].values[0], Value::Int(4)); // Sales, 90000
            assert_eq!(rows[3].values[0], Value::Int(2)); // Sales, 80000
        }
        _ => panic!("Expected Rows result"),
    }

    Ok(())
}

#[tokio::test]
async fn limit_restricts_result_count() -> Result<()> {
    let temp_dir = tempfile::tempdir()?;
    let db = Database::new(temp_dir.path(), "catalog.json", "test.wal", 10).await?;

    db.execute("CREATE TABLE numbers (id INT PRIMARY KEY, value INT)")
        .await?;

    for i in 1..=10 {
        db.execute(&format!("INSERT INTO numbers VALUES ({}, {})", i, i))
            .await?;
    }

    let result = db.execute("SELECT * FROM numbers LIMIT 5").await?;

    match result {
        QueryResult::Rows { rows, .. } => {
            assert_eq!(rows.len(), 5, "LIMIT should restrict to 5 rows");
        }
        _ => panic!("Expected Rows result"),
    }

    Ok(())
}

#[tokio::test]
async fn offset_skips_rows() -> Result<()> {
    let temp_dir = tempfile::tempdir()?;
    let db = Database::new(temp_dir.path(), "catalog.json", "test.wal", 10).await?;

    db.execute("CREATE TABLE items (id INT PRIMARY KEY, name TEXT)")
        .await?;
    db.execute("INSERT INTO items VALUES (1, 'First')").await?;
    db.execute("INSERT INTO items VALUES (2, 'Second')").await?;
    db.execute("INSERT INTO items VALUES (3, 'Third')").await?;
    db.execute("INSERT INTO items VALUES (4, 'Fourth')").await?;

    let result = db.execute("SELECT * FROM items OFFSET 2").await?;

    match result {
        QueryResult::Rows { rows, .. } => {
            assert_eq!(rows.len(), 2, "Should skip first 2 rows");
            // Should return id 3 and 4
            assert!(rows.iter().any(|r| r.values[0] == Value::Int(3)));
            assert!(rows.iter().any(|r| r.values[0] == Value::Int(4)));
        }
        _ => panic!("Expected Rows result"),
    }

    Ok(())
}

#[tokio::test]
async fn pagination_through_ordered_records() -> Result<()> {
    let temp_dir = tempfile::tempdir()?;
    let db = Database::new(temp_dir.path(), "catalog.json", "test.wal", 10).await?;

    // Setup: 10 records, page size of 2
    db.execute("CREATE TABLE records (id INT PRIMARY KEY, value INT)")
        .await?;

    for i in 1..=10 {
        db.execute(&format!("INSERT INTO records VALUES ({}, {})", i, i * 10))
            .await?;
    }

    // Page through the results with page size 2
    let page_size: usize = 2;
    let total_records: usize = 10;
    let total_pages = Integer::div_ceil(&total_records, &page_size); // 5 pages

    let mut all_ids = Vec::new();

    for page in 0..total_pages {
        let offset = page * page_size;
        let query = format!(
            "SELECT * FROM records ORDER BY value ASC LIMIT {} OFFSET {}",
            page_size, offset
        );
        let result = db.execute(&query).await?;

        match result {
            QueryResult::Rows { rows, .. } => {
                assert!(
                    rows.len() <= page_size,
                    "Page should have at most {} rows",
                    page_size
                );

                for row in &rows {
                    let id = match &row.values[0] {
                        Value::Int(v) => *v as i32,
                        _ => panic!("Expected Int value for id"),
                    };
                    all_ids.push(id);
                }
            }
            _ => panic!("Expected Rows result"),
        }
    }

    // Verify we got all 10 records in order
    assert_eq!(all_ids.len(), 10);
    assert_eq!(all_ids, vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);

    Ok(())
}

#[tokio::test]
async fn limit_and_offset_combined() -> Result<()> {
    let temp_dir = tempfile::tempdir()?;
    let db = Database::new(temp_dir.path(), "catalog.json", "test.wal", 10).await?;

    db.execute("CREATE TABLE data (id INT PRIMARY KEY, value TEXT)")
        .await?;

    for i in 1..=10 {
        db.execute(&format!("INSERT INTO data VALUES ({}, 'value{}')", i, i))
            .await?;
    }

    // Skip first 3, take next 4
    let result = db.execute("SELECT * FROM data LIMIT 4 OFFSET 3").await?;

    match result {
        QueryResult::Rows { rows, .. } => {
            assert_eq!(rows.len(), 4);
            // Should get ids 4, 5, 6, 7 (assuming insertion order)
            assert!(rows.iter().any(|r| r.values[0] == Value::Int(4)));
            assert!(rows.iter().any(|r| r.values[0] == Value::Int(5)));
            assert!(rows.iter().any(|r| r.values[0] == Value::Int(6)));
            assert!(rows.iter().any(|r| r.values[0] == Value::Int(7)));
        }
        _ => panic!("Expected Rows result"),
    }

    Ok(())
}

#[tokio::test]
async fn order_by_with_limit() -> Result<()> {
    let temp_dir = tempfile::tempdir()?;
    let db = Database::new(temp_dir.path(), "catalog.json", "test.wal", 10).await?;

    db.execute("CREATE TABLE scores (id INT PRIMARY KEY, player TEXT, score INT)")
        .await?;
    db.execute("INSERT INTO scores VALUES (1, 'Alice', 100)")
        .await?;
    db.execute("INSERT INTO scores VALUES (2, 'Bob', 150)")
        .await?;
    db.execute("INSERT INTO scores VALUES (3, 'Charlie', 120)")
        .await?;
    db.execute("INSERT INTO scores VALUES (4, 'Diana', 180)")
        .await?;

    // Get top 2 scores
    let result = db
        .execute("SELECT * FROM scores ORDER BY score DESC LIMIT 2")
        .await?;

    match result {
        QueryResult::Rows { rows, .. } => {
            assert_eq!(rows.len(), 2);
            // Should get Diana (180) and Bob (150)
            assert_eq!(rows[0].values[0], Value::Int(4)); // Diana
            assert_eq!(rows[1].values[0], Value::Int(2)); // Bob
        }
        _ => panic!("Expected Rows result"),
    }

    Ok(())
}

#[tokio::test]
async fn order_by_with_limit_and_offset() -> Result<()> {
    let temp_dir = tempfile::tempdir()?;
    let db = Database::new(temp_dir.path(), "catalog.json", "test.wal", 10).await?;

    db.execute("CREATE TABLE rankings (id INT PRIMARY KEY, score INT)")
        .await?;

    for i in 1..=10 {
        db.execute(&format!("INSERT INTO rankings VALUES ({}, {})", i, i * 10))
            .await?;
    }

    // Get ranks 4-6 (skip top 3, take next 3)
    let result = db
        .execute("SELECT * FROM rankings ORDER BY score DESC LIMIT 3 OFFSET 3")
        .await?;

    match result {
        QueryResult::Rows { rows, .. } => {
            assert_eq!(rows.len(), 3);
            // Descending order: 100, 90, 80, [70, 60, 50], 40, 30, 20, 10
            // Should get scores 70, 60, 50 (ids 7, 6, 5)
            assert_eq!(rows[0].values[0], Value::Int(7));
            assert_eq!(rows[1].values[0], Value::Int(6));
            assert_eq!(rows[2].values[0], Value::Int(5));
        }
        _ => panic!("Expected Rows result"),
    }

    Ok(())
}

#[tokio::test]
async fn offset_beyond_total_rows_returns_empty() -> Result<()> {
    let temp_dir = tempfile::tempdir()?;
    let db = Database::new(temp_dir.path(), "catalog.json", "test.wal", 10).await?;

    db.execute("CREATE TABLE small (id INT PRIMARY KEY)").await?;
    db.execute("INSERT INTO small VALUES (1)").await?;
    db.execute("INSERT INTO small VALUES (2)").await?;

    let result = db.execute("SELECT * FROM small OFFSET 10").await?;

    match result {
        QueryResult::Rows { rows, .. } => {
            assert_eq!(rows.len(), 0, "Offset beyond rows should return empty");
        }
        _ => panic!("Expected Rows result"),
    }

    Ok(())
}

#[tokio::test]
async fn limit_larger_than_total_rows_returns_all() -> Result<()> {
    let temp_dir = tempfile::tempdir()?;
    let db = Database::new(temp_dir.path(), "catalog.json", "test.wal", 10).await?;

    db.execute("CREATE TABLE tiny (id INT PRIMARY KEY)").await?;
    db.execute("INSERT INTO tiny VALUES (1)").await?;
    db.execute("INSERT INTO tiny VALUES (2)").await?;

    let result = db.execute("SELECT * FROM tiny LIMIT 100").await?;

    match result {
        QueryResult::Rows { rows, .. } => {
            assert_eq!(rows.len(), 2, "Should return all available rows");
        }
        _ => panic!("Expected Rows result"),
    }

    Ok(())
}

#[tokio::test]
async fn order_by_text_column_lexicographic() -> Result<()> {
    let temp_dir = tempfile::tempdir()?;
    let db = Database::new(temp_dir.path(), "catalog.json", "test.wal", 10).await?;

    db.execute("CREATE TABLE words (id INT PRIMARY KEY, word TEXT)")
        .await?;
    db.execute("INSERT INTO words VALUES (1, 'zebra')").await?;
    db.execute("INSERT INTO words VALUES (2, 'apple')").await?;
    db.execute("INSERT INTO words VALUES (3, 'mango')").await?;

    let result = db.execute("SELECT * FROM words ORDER BY word ASC").await?;

    match result {
        QueryResult::Rows { rows, .. } => {
            assert_eq!(rows.len(), 3);
            // Lexicographic order: apple, mango, zebra
            assert_eq!(rows[0].values[0], Value::Int(2)); // apple
            assert_eq!(rows[1].values[0], Value::Int(3)); // mango
            assert_eq!(rows[2].values[0], Value::Int(1)); // zebra
        }
        _ => panic!("Expected Rows result"),
    }

    Ok(())
}

#[tokio::test]
async fn order_by_with_where_clause() -> Result<()> {
    let temp_dir = tempfile::tempdir()?;
    let db = Database::new(temp_dir.path(), "catalog.json", "test.wal", 10).await?;

    db.execute("CREATE TABLE filtered (id INT PRIMARY KEY, category TEXT, value INT)")
        .await?;
    db.execute("INSERT INTO filtered VALUES (1, 'A', 100)")
        .await?;
    db.execute("INSERT INTO filtered VALUES (2, 'B', 200)")
        .await?;
    db.execute("INSERT INTO filtered VALUES (3, 'A', 150)")
        .await?;
    db.execute("INSERT INTO filtered VALUES (4, 'B', 50)")
        .await?;

    let result = db
        .execute("SELECT * FROM filtered WHERE category = 'A' ORDER BY value DESC")
        .await?;

    match result {
        QueryResult::Rows { rows, .. } => {
            assert_eq!(rows.len(), 2);
            // Category A, ordered by value DESC: 150, 100
            assert_eq!(rows[0].values[0], Value::Int(3));
            assert_eq!(rows[1].values[0], Value::Int(1));
        }
        _ => panic!("Expected Rows result"),
    }

    Ok(())
}

#[tokio::test]
async fn pagination_with_filtering() -> Result<()> {
    let temp_dir = tempfile::tempdir()?;
    let db = Database::new(temp_dir.path(), "catalog.json", "test.wal", 10).await?;

    db.execute("CREATE TABLE events (id INT PRIMARY KEY, type TEXT, timestamp INT)")
        .await?;

    for i in 1..=20 {
        let event_type = if i % 2 == 0 { "even" } else { "odd" };
        db.execute(&format!(
            "INSERT INTO events VALUES ({}, '{}', {})",
            i, event_type, i
        ))
        .await?;
    }

    // Page through even events only
    let page1 = db
        .execute("SELECT * FROM events WHERE type = 'even' ORDER BY timestamp ASC LIMIT 3 OFFSET 0")
        .await?;

    let page2 = db
        .execute("SELECT * FROM events WHERE type = 'even' ORDER BY timestamp ASC LIMIT 3 OFFSET 3")
        .await?;

    match (page1, page2) {
        (QueryResult::Rows { rows: rows1, .. }, QueryResult::Rows { rows: rows2, .. }) => {
            assert_eq!(rows1.len(), 3, "First page should have 3 rows");
            assert_eq!(rows2.len(), 3, "Second page should have 3 rows");

            // Verify timestamps are in order
            let ts1_0 = match &rows1[0].values[2] {
                Value::Int(v) => *v,
                _ => panic!("Expected Int value"),
            };
            let ts1_2 = match &rows1[2].values[2] {
                Value::Int(v) => *v,
                _ => panic!("Expected Int value"),
            };
            let ts2_0 = match &rows2[0].values[2] {
                Value::Int(v) => *v,
                _ => panic!("Expected Int value"),
            };

            assert!(ts1_0 < ts1_2, "Page 1 should be ordered");
            assert!(ts1_2 < ts2_0, "Page 2 should come after page 1");
        }
        _ => panic!("Expected Rows results"),
    }

    Ok(())
}

#[tokio::test]
async fn empty_table_with_order_and_limit() -> Result<()> {
    let temp_dir = tempfile::tempdir()?;
    let db = Database::new(temp_dir.path(), "catalog.json", "test.wal", 10).await?;

    db.execute("CREATE TABLE empty (id INT PRIMARY KEY, value INT)")
        .await?;

    let result = db
        .execute("SELECT * FROM empty ORDER BY value DESC LIMIT 10")
        .await?;

    match result {
        QueryResult::Rows { rows, .. } => {
            assert_eq!(rows.len(), 0, "Empty table should return no rows");
        }
        _ => panic!("Expected Rows result"),
    }

    Ok(())
}
