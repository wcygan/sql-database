use super::*;
use types::Value;

fn stmt(sql: &str) -> Statement {
    let sql = sql.trim();
    let mut stmts = parse_sql(sql).expect("parser should succeed");
    assert_eq!(stmts.len(), 1, "expected exactly one statement from {sql}");
    stmts.remove(0)
}

fn stmts(sql: &str) -> Vec<Statement> {
    parse_sql(sql).expect("parser should succeed")
}

#[test]
fn parse_create_and_drop_statements() {
    let sql = r#"
        CREATE TABLE USERS (ID INT, NAME TEXT, active BOOL);
        CREATE INDEX idx_users_name ON USERS(name);
        DROP TABLE users;
        DROP INDEX idx_users_name;
    "#;

    let stmts = stmts(sql);
    assert_eq!(stmts.len(), 4);

    match &stmts[0] {
        Statement::CreateTable { name, columns, .. } => {
            assert_eq!(name, "users");
            assert_eq!(columns.len(), 3);
            assert_eq!(columns[0].name, "id");
            assert_eq!(columns[0].ty, "INT");
            assert_eq!(columns[2].name, "active");
            assert_eq!(columns[2].ty, "BOOL");
        }
        other => panic!("expected CreateTable, got {other:?}"),
    }

    match &stmts[1] {
        Statement::CreateIndex {
            name,
            table,
            column,
        } => {
            assert_eq!(name, "idx_users_name");
            assert_eq!(table, "users");
            assert_eq!(column, "name");
        }
        other => panic!("expected CreateIndex, got {other:?}"),
    }

    assert!(matches!(
        stmts[2],
        Statement::DropTable { ref name } if name == "users"
    ));
    assert!(matches!(
        stmts[3],
        Statement::DropIndex { ref name } if name == "idx_users_name"
    ));
}

#[test]
fn parse_dml_statements() {
    let insert = stmt("INSERT INTO posts VALUES (42, 'Hello', true)");
    match insert {
        Statement::Insert { table, values } => {
            assert_eq!(table, "posts");
            assert_eq!(values.len(), 3);
            assert!(matches!(values[0], Expr::Literal(Value::Int(42))));
            assert!(matches!(values[1], Expr::Literal(Value::Text(_))));
            assert!(matches!(values[2], Expr::Literal(Value::Bool(true))));
        }
        other => panic!("expected Insert, got {other:?}"),
    }

    let select = stmt("SELECT id, name FROM posts WHERE id > 10");
    match select {
        Statement::Select {
            table,
            columns,
            selection,
        } => {
            assert_eq!(table, "posts");
            assert_eq!(columns.len(), 2);
            assert!(matches!(columns[0], SelectItem::Column(ref c) if c == "id"));
            let predicate = selection.expect("expected WHERE clause");
            let formatted = format!("{predicate:?}");
            assert!(formatted.contains("Binary"));
        }
        other => panic!("expected Select, got {other:?}"),
    }

    let update = stmt("UPDATE posts SET title = 'new' WHERE id = 1");
    match update {
        Statement::Update {
            table,
            assignments,
            selection,
        } => {
            assert_eq!(table, "posts");
            assert_eq!(assignments.len(), 1);
            assert_eq!(assignments[0].0, "title");
            assert!(matches!(assignments[0].1, Expr::Literal(Value::Text(_))));
            assert!(selection.is_some());
        }
        other => panic!("expected Update, got {other:?}"),
    }

    let delete = stmt("DELETE FROM posts WHERE title = 'old'");
    match delete {
        Statement::Delete { table, selection } => {
            assert_eq!(table, "posts");
            assert!(selection.is_some());
        }
        other => panic!("expected Delete, got {other:?}"),
    }
}

#[test]
fn reject_multi_row_insert() {
    let result = parse_sql("INSERT INTO users VALUES (1), (2)");
    let err = result.expect_err("multi-row insert should fail");
    assert!(format!("{err:?}").contains("multi-row"));
}

#[test]
fn reject_aliases_and_joins() {
    // SELECT aliases not supported.
    let err = parse_sql("SELECT name AS n FROM users").expect_err("aliases should be rejected");
    assert!(
        format!("{err:?}").contains("select aliases not supported"),
        "{err:?}"
    );

    // JOINs not supported.
    let result = parse_sql("SELECT * FROM users u JOIN posts p ON u.id = p.user_id");
    let err = result.expect_err("joins should fail");
    assert!(format!("{err:?}").contains("joins"));
}

#[test]
fn reject_multi_table_delete() {
    let err = parse_sql("DELETE FROM users, posts WHERE users.id = posts.user_id")
        .expect_err("multi-table delete should fail");
    assert!(format!("{err:?}").contains("multi-table DELETE"));
}

#[test]
fn drop_rejects_non_table_objects() {
    let err = parse_sql("DROP VIEW users").expect_err("DROP VIEW should fail");
    assert!(format!("{err:?}").contains("unsupported DROP type"));
}

#[test]
fn create_index_validates_inputs() {
    let err = parse_sql("CREATE INDEX ON users(name)").expect_err("name required");
    assert!(format!("{err:?}").contains("index name required"));

    let err = parse_sql("CREATE INDEX idx_bad ON users((name || 'x'))")
        .expect_err("complex index expressions not supported");
    assert!(format!("{err:?}").contains("unsupported index column"));
}

#[test]
fn select_requires_from_clause_and_single_table() {
    let err = parse_sql("SELECT 1").expect_err("FROM clause should be required");
    assert!(format!("{err:?}").contains("SELECT requires FROM clause"));

    let err = parse_sql("SELECT * FROM users, posts").expect_err("multi-table select should fail");
    assert!(format!("{err:?}").contains("joins not supported"));
}

#[test]
fn insert_requires_values_clause() {
    let err = parse_sql("INSERT INTO users SELECT 1").expect_err("VALUES clause required");
    assert!(format!("{err:?}").contains("INSERT expects VALUES list"));
}

#[test]
fn literal_parsing_requires_ints() {
    let err =
        parse_sql("INSERT INTO users VALUES (1.5)").expect_err("non-integer literal should fail");
    assert!(format!("{err:?}").contains("invalid int literal"));
}

#[test]
fn unsupported_binary_and_unary_ops_report_errors() {
    let err = parse_sql("SELECT * FROM users WHERE (id + 1) > 2")
        .expect_err("arithmetic ops are not supported");
    let msg = format!("{err:?}");
    assert!(msg.contains("unsupported operator"), "{msg}");

    let err =
        parse_sql("SELECT * FROM users WHERE -id = 1").expect_err("unary minus should be rejected");
    let msg = format!("{err:?}");
    assert!(msg.contains("unsupported unary operator"), "{msg}");
}

#[test]
fn wildcard_options_not_supported() {
    let err =
        parse_sql("SELECT * EXCEPT (name) FROM users").expect_err("wildcard options should fail");
    assert!(format!("{err:?}").contains("wildcard options not supported"));
}

#[test]
fn sql_subset_v1_select_variants() {
    let select_no_filter = stmt("SELECT id, name FROM users");
    match select_no_filter {
        Statement::Select {
            table,
            columns,
            selection,
        } => {
            assert_eq!(table, "users");
            assert_eq!(
                columns,
                vec![
                    SelectItem::Column("id".into()),
                    SelectItem::Column("name".into()),
                ]
            );
            assert!(selection.is_none());
        }
        other => panic!("expected Select, got {other:?}"),
    }

    let select_wildcard = stmt("SELECT * FROM users");
    match select_wildcard {
        Statement::Select { columns, .. } => {
            assert_eq!(columns, vec![SelectItem::Wildcard]);
        }
        other => panic!("expected Select, got {other:?}"),
    }

    let select_complex = stmt("SELECT * FROM users WHERE name = 'Will' AND age < 30");
    match select_complex {
        Statement::Select { selection, .. } => {
            let selection = selection.expect("WHERE clause required");
            match selection {
                Expr::Binary { op, left, right } => {
                    assert_eq!(op, expr::BinaryOp::And);
                    assert!(matches!(*left, Expr::Binary { .. }));
                    assert!(matches!(*right, Expr::Binary { .. }));
                }
                other => panic!("expected AND binary, got {other:?}"),
            }
        }
        other => panic!("expected Select, got {other:?}"),
    }
}

#[test]
fn sql_subset_v1_example_script() {
    let sql = r#"
        CREATE TABLE users (id INT, name TEXT, age INT);
        CREATE INDEX idx_users_id ON users(id);
        CREATE INDEX idx_users_age ON users(age);
        INSERT INTO users VALUES (1, 'Will', 27);
        INSERT INTO users VALUES (2, 'Ada', 31);
        SELECT * FROM users WHERE age > 25;
        UPDATE users SET name = 'William' WHERE id = 1;
        DELETE FROM users WHERE age > 30;
        SELECT id, name FROM users;
        DROP INDEX idx_users_id;
        DROP TABLE users;
    "#;

    let stmts = stmts(sql);
    assert_eq!(stmts.len(), 11);

    match &stmts[0] {
        Statement::CreateTable { name, columns, .. } => {
            assert_eq!(name, "users");
            assert_eq!(columns.len(), 3);
        }
        other => panic!("expected CreateTable, got {other:?}"),
    }

    match &stmts[1] {
        Statement::CreateIndex {
            name,
            table,
            column,
        } => {
            assert_eq!(name, "idx_users_id");
            assert_eq!(table, "users");
            assert_eq!(column, "id");
        }
        other => panic!("expected first CreateIndex, got {other:?}"),
    }

    match &stmts[2] {
        Statement::CreateIndex { name, column, .. } => {
            assert_eq!(name, "idx_users_age");
            assert_eq!(column, "age");
        }
        other => panic!("expected second CreateIndex, got {other:?}"),
    }

    match &stmts[3] {
        Statement::Insert { table, values } => {
            assert_eq!(table, "users");
            assert!(matches!(
                values.as_slice(),
                [
                    Expr::Literal(Value::Int(1)),
                    Expr::Literal(Value::Text(_)),
                    Expr::Literal(Value::Int(27))
                ]
            ));
        }
        other => panic!("expected first Insert, got {other:?}"),
    }

    match &stmts[4] {
        Statement::Insert { values, .. } => {
            assert!(matches!(
                values.as_slice(),
                [
                    Expr::Literal(Value::Int(2)),
                    Expr::Literal(Value::Text(_)),
                    Expr::Literal(Value::Int(31))
                ]
            ));
        }
        other => panic!("expected second Insert, got {other:?}"),
    }

    match &stmts[5] {
        Statement::Select { selection, .. } => {
            let selection = selection.as_ref().expect("WHERE clause required");
            let formatted = format!("{selection:?}");
            assert!(formatted.contains("age"));
        }
        other => panic!("expected Select with WHERE, got {other:?}"),
    }

    match &stmts[6] {
        Statement::Update {
            assignments,
            selection,
            ..
        } => {
            assert_eq!(assignments.len(), 1);
            assert_eq!(assignments[0].0, "name");
            assert!(selection.is_some());
        }
        other => panic!("expected Update, got {other:?}"),
    }

    match &stmts[7] {
        Statement::Delete { selection, .. } => {
            assert!(selection.is_some());
        }
        other => panic!("expected Delete, got {other:?}"),
    }

    match &stmts[8] {
        Statement::Select {
            columns, selection, ..
        } => {
            assert!(selection.is_none());
            let expected = vec![
                SelectItem::Column("id".into()),
                SelectItem::Column("name".into()),
            ];
            assert_eq!(columns, &expected);
        }
        other => panic!("expected final Select, got {other:?}"),
    }

    assert!(matches!(
        stmts[9],
        Statement::DropIndex { ref name } if name == "idx_users_id"
    ));

    assert!(matches!(
        stmts[10],
        Statement::DropTable { ref name } if name == "users"
    ));
}

#[test]
fn reject_create_view_statement() {
    let err = parse_sql("CREATE VIEW v AS SELECT * FROM users")
        .expect_err("CREATE VIEW should be rejected");
    assert!(format!("{err:?}").contains("unsupported statement"));
}

#[test]
fn select_rejects_values_and_set_operations() {
    let err = parse_sql("VALUES (1)").expect_err("standalone VALUES should fail");
    assert!(
        format!("{err:?}").contains("standalone VALUES not supported"),
        "{err:?}"
    );

    let err = parse_sql("SELECT 1 UNION SELECT 2").expect_err("SET ops should fail");
    assert!(
        format!("{err:?}").contains("SET operations not supported"),
        "{err:?}"
    );
}

#[test]
fn select_item_limitations() {
    let err = parse_sql("SELECT users.* FROM users").expect_err("qualified wildcards should fail");
    assert!(
        format!("{err:?}").contains("qualified wildcard not supported"),
        "{err:?}"
    );

    let err = parse_sql("SELECT id + 1 FROM users")
        .expect_err("complex projection expressions should fail");
    assert!(
        format!("{err:?}").contains("unsupported select item"),
        "{err:?}"
    );
}

#[test]
fn derived_table_sources_rejected() {
    let err =
        parse_sql("SELECT * FROM (SELECT 1) sub").expect_err("derived tables in FROM should fail");
    assert!(
        format!("{err:?}").contains("unsupported table factor"),
        "{err:?}"
    );

    let err = parse_sql("DELETE FROM (SELECT 1) sub")
        .expect_err("DELETE should also reject derived tables");
    assert!(
        format!("{err:?}").contains("unsupported table factor"),
        "{err:?}"
    );
}

#[test]
fn compound_identifiers_and_nested_exprs_parse() {
    let stmt = stmt("SELECT * FROM users WHERE (users.id) = (1)");
    let selection = match stmt {
        Statement::Select { selection, .. } => selection.expect("WHERE clause required"),
        other => panic!("expected Select, got {other:?}"),
    };

    match selection {
        Expr::Binary { left, right, .. } => {
            assert!(
                matches!(*left, Expr::Column(ref c) if c == "id"),
                "compound identifier should become column: {left:?}"
            );
            assert!(
                matches!(*right, Expr::Literal(Value::Int(1))),
                "nested literal should parse: {right:?}"
            );
        }
        other => panic!("expected binary comparison, got {other:?}"),
    }
}

#[test]
fn unsupported_exists_expressions_report_errors() {
    let err = parse_sql("SELECT * FROM users WHERE EXISTS (SELECT 1 FROM users)")
        .expect_err("EXISTS expressions should fail");
    assert!(format!("{err:?}").contains("unsupported expr"), "{err:?}");
}

#[test]
fn boolean_and_null_literals_are_supported() {
    let stmt = stmt("INSERT INTO flags VALUES (TRUE, NULL)");
    match stmt {
        Statement::Insert { values, .. } => {
            assert!(matches!(values[0], Expr::Literal(Value::Bool(true))));
            assert!(matches!(values[1], Expr::Literal(Value::Null)));
        }
        other => panic!("expected Insert, got {other:?}"),
    }
}

#[test]
fn comparison_and_logical_operators_parse_correctly() {
    let stmt = stmt(
        "SELECT * FROM users \
         WHERE (age < 18 OR age <= 21) AND (age > 65 OR age >= 80)",
    );
    let selection = match stmt {
        Statement::Select { selection, .. } => selection.expect("WHERE clause required"),
        other => panic!("expected Select, got {other:?}"),
    };

    match selection {
        Expr::Binary { op, left, right } => {
            assert_eq!(op, expr::BinaryOp::And);
            let check_or = |expr: &Expr, first: expr::BinaryOp, second: expr::BinaryOp| match expr {
                Expr::Binary { op, left, right } => {
                    assert_eq!(*op, expr::BinaryOp::Or);
                    assert!(matches!(**left, Expr::Binary { op, .. } if op == first));
                    assert!(matches!(**right, Expr::Binary { op, .. } if op == second));
                }
                other => panic!("expected OR branch, got {other:?}"),
            };
            check_or(&left, expr::BinaryOp::Lt, expr::BinaryOp::Le);
            check_or(&right, expr::BinaryOp::Gt, expr::BinaryOp::Ge);
        }
        other => panic!("expected binary AND, got {other:?}"),
    }
}

#[test]
fn unary_plus_is_rejected() {
    let err = parse_sql("SELECT * FROM users WHERE +id = 1").expect_err("unary plus should fail");
    assert!(
        format!("{err:?}").contains("unsupported unary operator"),
        "{err:?}"
    );
}

#[test]
fn create_index_supports_qualified_columns() {
    let stmt = stmt("CREATE INDEX idx_users_name ON users (users.name)");
    match stmt {
        Statement::CreateIndex { column, .. } => assert_eq!(column, "name"),
        other => panic!("expected CreateIndex, got {other:?}"),
    }
}

#[test]
fn not_equal_operator_is_supported() {
    let sql = "SELECT * FROM users WHERE id != 5";
    let stmt = stmt(sql);
    match stmt {
        Statement::Select { selection, .. } => {
            let selection = selection.expect("WHERE clause required");
            match selection {
                Expr::Binary { op, .. } => {
                    assert_eq!(op, expr::BinaryOp::Ne, "expected NotEq operator");
                }
                other => panic!("expected binary comparison, got {other:?}"),
            }
        }
        other => panic!("expected Select, got {other:?}"),
    }
}

#[test]
fn not_operator_is_supported() {
    let sql = "SELECT * FROM users WHERE NOT active";
    let stmt = stmt(sql);
    match stmt {
        Statement::Select { selection, .. } => {
            let selection = selection.expect("WHERE clause required");
            match selection {
                Expr::Unary { op, .. } => {
                    assert_eq!(op, expr::UnaryOp::Not, "expected NOT operator");
                }
                other => panic!("expected unary NOT, got {other:?}"),
            }
        }
        other => panic!("expected Select, got {other:?}"),
    }
}

#[test]
fn or_operator_is_supported() {
    let sql = "SELECT * FROM users WHERE id = 1 OR id = 2";
    let stmt = stmt(sql);
    match stmt {
        Statement::Select { selection, .. } => {
            let selection = selection.expect("WHERE clause required");
            match selection {
                Expr::Binary { op, .. } => {
                    assert_eq!(op, expr::BinaryOp::Or, "expected OR operator");
                }
                other => panic!("expected binary OR, got {other:?}"),
            }
        }
        other => panic!("expected Select, got {other:?}"),
    }
}

#[test]
fn update_assignment_requires_valid_expressions() {
    // Test UPDATE with complex unsupported expression in assignment
    let err = parse_sql("UPDATE users SET score = score + 10 WHERE id = 1")
        .expect_err("arithmetic in assignment should fail");
    assert!(
        format!("{err:?}").contains("unsupported operator"),
        "expected unsupported operator error, got: {err:?}"
    );
}

#[test]
fn invalid_sql_syntax_produces_parse_error() {
    // Test completely invalid SQL to trigger sqlparser error
    let err = parse_sql("SELECTTTT FROMM users WHEREE").expect_err("invalid SQL should fail");
    assert!(
        format!("{err:?}").contains("SQL parse error"),
        "expected SQL parse error, got: {err:?}"
    );
}

#[test]
fn create_table_with_single_column_primary_key() {
    let stmts = parse_sql("CREATE TABLE users (id INT, name TEXT, PRIMARY KEY (id))").unwrap();
    assert_eq!(stmts.len(), 1);

    match &stmts[0] {
        Statement::CreateTable {
            name,
            columns,
            primary_key,
        } => {
            assert_eq!(name, "users");
            assert_eq!(columns.len(), 2);
            assert_eq!(columns[0].name, "id");
            assert_eq!(columns[1].name, "name");
            assert_eq!(primary_key, &Some(vec!["id".to_string()]));
        }
        _ => panic!("expected CreateTable statement"),
    }
}

#[test]
fn create_table_with_composite_primary_key() {
    let stmts = parse_sql(
        "CREATE TABLE orders (user_id INT, product_id INT, PRIMARY KEY (user_id, product_id))",
    )
    .unwrap();
    assert_eq!(stmts.len(), 1);

    match &stmts[0] {
        Statement::CreateTable {
            name, primary_key, ..
        } => {
            assert_eq!(name, "orders");
            assert_eq!(
                primary_key,
                &Some(vec!["user_id".to_string(), "product_id".to_string()])
            );
        }
        _ => panic!("expected CreateTable statement"),
    }
}

#[test]
fn create_table_without_primary_key() {
    let stmts = parse_sql("CREATE TABLE users (id INT, name TEXT)").unwrap();
    assert_eq!(stmts.len(), 1);

    match &stmts[0] {
        Statement::CreateTable { primary_key, .. } => {
            assert_eq!(primary_key, &None);
        }
        _ => panic!("expected CreateTable statement"),
    }
}

#[test]
fn create_table_primary_key_case_insensitive() {
    let stmts = parse_sql("CREATE TABLE users (ID INT, NAME TEXT, PRIMARY KEY (ID))").unwrap();
    assert_eq!(stmts.len(), 1);

    match &stmts[0] {
        Statement::CreateTable { primary_key, .. } => {
            // normalize_ident lowercases identifiers
            assert_eq!(primary_key, &Some(vec!["id".to_string()]));
        }
        _ => panic!("expected CreateTable statement"),
    }
}
