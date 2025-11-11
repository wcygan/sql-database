use super::*;

#[test]
fn parse_basic_statements() {
    let sql = r#"
        CREATE TABLE users (id INT, name TEXT, age INT);
        INSERT INTO users VALUES (1, 'Will', 27);
        SELECT id, name FROM users WHERE age > 20;
    "#;

    let stmts = parse_sql(sql).expect("parser should succeed");
    assert_eq!(stmts.len(), 3);

    match &stmts[0] {
        Statement::CreateTable { name, columns } => {
            assert_eq!(name, "users");
            assert_eq!(columns.len(), 3);
            assert_eq!(columns[1].name, "name");
            assert_eq!(columns[1].ty, "TEXT");
        }
        other => panic!("expected CreateTable, got {other:?}"),
    }

    match &stmts[1] {
        Statement::Insert { table, values } => {
            assert_eq!(table, "users");
            assert_eq!(values.len(), 3);
        }
        other => panic!("expected Insert, got {other:?}"),
    }

    match &stmts[2] {
        Statement::Select {
            table,
            selection,
            columns,
        } => {
            assert_eq!(table, "users");
            assert_eq!(columns.len(), 2);
            let selection = selection.as_ref().expect("WHERE clause required");
            let display = format!("{selection:?}");
            assert!(display.contains("age"));
        }
        other => panic!("expected Select, got {other:?}"),
    }
}
