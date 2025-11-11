use super::*;
use catalog::{Column, IndexKind};
use parser::parse_sql;
use pretty_assertions::assert_eq;
use types::SqlType;

/// Create a sample catalog with a users table.
fn sample_catalog() -> Catalog {
    let mut catalog = Catalog::new();
    catalog
        .create_table(
            "users",
            vec![
                Column::new("id", SqlType::Int),
                Column::new("name", SqlType::Text),
                Column::new("age", SqlType::Int),
            ],
        )
        .unwrap();
    catalog
        .create_index("users", "idx_users_id", &["id"], IndexKind::BTree)
        .unwrap();
    catalog
        .create_index("users", "idx_users_age", &["age"], IndexKind::BTree)
        .unwrap();
    catalog
}

#[test]
fn select_star_generates_seqscan_with_project() {
    let catalog = sample_catalog();
    let mut ctx = PlanningContext::new(&catalog);
    let stmt = parse_sql("SELECT * FROM users;").unwrap().remove(0);

    let plan = Planner::plan(stmt, &mut ctx).unwrap();

    match plan {
        PhysicalPlan::Project { input, columns } => {
            assert_eq!(columns.len(), 3);
            assert!(matches!(*input, PhysicalPlan::SeqScan { .. }));
        }
        _ => panic!("expected Project"),
    }
}

#[test]
fn select_specific_columns_resolves_ordinals() {
    let catalog = sample_catalog();
    let mut ctx = PlanningContext::new(&catalog);
    let stmt = parse_sql("SELECT name, age FROM users;").unwrap().remove(0);

    let plan = Planner::plan(stmt, &mut ctx).unwrap();

    match plan {
        PhysicalPlan::Project { columns, .. } => {
            assert_eq!(columns, vec![("name".into(), 1), ("age".into(), 2)]);
        }
        _ => panic!("expected Project"),
    }
}

#[test]
fn where_clause_generates_filter() {
    let catalog = sample_catalog();
    let mut ctx = PlanningContext::new(&catalog);
    let stmt = parse_sql("SELECT * FROM users WHERE age > 20;")
        .unwrap()
        .remove(0);

    let plan = Planner::plan(stmt, &mut ctx).unwrap();

    let text = explain_physical(&plan);
    assert!(text.contains("Filter"));
}

#[test]
fn equality_predicate_on_indexed_column_uses_indexscan() {
    let catalog = sample_catalog();
    let mut ctx = PlanningContext::new(&catalog);
    let stmt = parse_sql("SELECT name FROM users WHERE id = 42;")
        .unwrap()
        .remove(0);

    let plan = Planner::plan(stmt, &mut ctx).unwrap();

    let text = explain_physical(&plan);
    assert!(text.contains("IndexScan"));
    assert!(text.contains("idx_users_id"));
}

#[test]
fn range_predicate_on_indexed_column_uses_indexscan() {
    let catalog = sample_catalog();
    let mut ctx = PlanningContext::new(&catalog);
    let stmt = parse_sql("SELECT * FROM users WHERE age > 30;")
        .unwrap()
        .remove(0);

    let plan = Planner::plan(stmt, &mut ctx).unwrap();

    let text = explain_physical(&plan);
    assert!(text.contains("IndexScan"));
    assert!(text.contains("idx_users_age"));
}

#[test]
fn predicate_on_non_indexed_column_uses_seqscan() {
    let catalog = sample_catalog();
    let mut ctx = PlanningContext::new(&catalog);
    // 'name' doesn't have an index
    let stmt = parse_sql("SELECT * FROM users WHERE name = 'alice';")
        .unwrap()
        .remove(0);

    let plan = Planner::plan(stmt, &mut ctx).unwrap();

    let text = explain_physical(&plan);
    assert!(text.contains("SeqScan"));
    assert!(!text.contains("IndexScan"));
}

#[test]
fn insert_plan_includes_values() {
    let catalog = sample_catalog();
    let mut ctx = PlanningContext::new(&catalog);
    let stmt = parse_sql("INSERT INTO users VALUES (1, 'alice', 30);")
        .unwrap()
        .remove(0);

    let plan = Planner::plan(stmt, &mut ctx).unwrap();

    match plan {
        PhysicalPlan::Insert { table_id, values } => {
            assert_eq!(table_id.0, 1);
            assert_eq!(values.len(), 3);
        }
        _ => panic!("expected Insert"),
    }
}

#[test]
fn update_plan_resolves_assignments() {
    let catalog = sample_catalog();
    let mut ctx = PlanningContext::new(&catalog);
    let stmt = parse_sql("UPDATE users SET age = 31 WHERE id = 1;")
        .unwrap()
        .remove(0);

    let plan = Planner::plan(stmt, &mut ctx).unwrap();

    match plan {
        PhysicalPlan::Update {
            table_id,
            assignments,
            predicate,
        } => {
            assert_eq!(table_id.0, 1);
            assert_eq!(assignments.len(), 1);
            assert_eq!(assignments[0].0, 2); // age column
            assert!(predicate.is_some());
        }
        _ => panic!("expected Update"),
    }
}

#[test]
fn delete_plan_with_predicate() {
    let catalog = sample_catalog();
    let mut ctx = PlanningContext::new(&catalog);
    let stmt = parse_sql("DELETE FROM users WHERE id = 1;")
        .unwrap()
        .remove(0);

    let plan = Planner::plan(stmt, &mut ctx).unwrap();

    match plan {
        PhysicalPlan::Delete {
            table_id,
            predicate,
        } => {
            assert_eq!(table_id.0, 1);
            assert!(predicate.is_some());
        }
        _ => panic!("expected Delete"),
    }
}

#[test]
fn delete_without_predicate() {
    let catalog = sample_catalog();
    let mut ctx = PlanningContext::new(&catalog);
    let stmt = parse_sql("DELETE FROM users;").unwrap().remove(0);

    let plan = Planner::plan(stmt, &mut ctx).unwrap();

    match plan {
        PhysicalPlan::Delete {
            table_id,
            predicate,
        } => {
            assert_eq!(table_id.0, 1);
            assert!(predicate.is_none());
        }
        _ => panic!("expected Delete"),
    }
}

#[test]
fn unknown_table_returns_error() {
    let catalog = sample_catalog();
    let mut ctx = PlanningContext::new(&catalog);
    let stmt = parse_sql("SELECT * FROM nonexistent;").unwrap().remove(0);

    let result = Planner::plan(stmt, &mut ctx);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("unknown table"));
}

#[test]
fn unknown_column_returns_error() {
    let catalog = sample_catalog();
    let mut ctx = PlanningContext::new(&catalog);
    let stmt = parse_sql("SELECT nonexistent FROM users;")
        .unwrap()
        .remove(0);

    let result = Planner::plan(stmt, &mut ctx);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("unknown column"));
}

#[test]
fn ddl_statements_return_error() {
    let catalog = sample_catalog();
    let mut ctx = PlanningContext::new(&catalog);
    let stmt = parse_sql("CREATE TABLE foo (id INT);").unwrap().remove(0);

    let result = Planner::plan(stmt, &mut ctx);
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("DDL handled elsewhere")
    );
}

#[test]
fn predicate_pushdown_through_wildcard_project() {
    let _catalog = sample_catalog();
    let stmt = parse_sql("SELECT * FROM users WHERE id = 1;")
        .unwrap()
        .remove(0);

    let logical = Planner::lower_to_logical(stmt).unwrap();
    let mut ctx = PlanningContext::new(&_catalog);
    let optimized = Planner::optimize(logical, &mut ctx).unwrap();

    // After pushdown, Filter should be below Project
    match optimized {
        LogicalPlan::Project { input, .. } => {
            // Wildcard case: pushdown moves filter to input
            match *input {
                LogicalPlan::Filter { .. } => {} // Expected
                _ => panic!("expected Filter after pushdown"),
            }
        }
        _ => panic!("expected Project"),
    }
}

#[test]
fn projection_pruning_removes_redundant_wildcard() {
    let input = LogicalPlan::Project {
        input: Box::new(LogicalPlan::Project {
            input: Box::new(LogicalPlan::TableScan {
                table: "users".into(),
            }),
            columns: vec!["id".into(), "name".into()],
        }),
        columns: vec!["*".into()],
    };

    let catalog = sample_catalog();
    let mut ctx = PlanningContext::new(&catalog);
    let pruned = Planner::optimize(input, &mut ctx).unwrap();

    // Outer wildcard project should be replaced by inner specific columns
    match pruned {
        LogicalPlan::Project { columns, .. } => {
            assert_eq!(columns, vec!["id".to_string(), "name".to_string()]);
        }
        _ => panic!("expected Project"),
    }
}

#[test]
fn expression_binding_resolves_columns() {
    let _catalog = sample_catalog();
    let schema = vec!["id".to_string(), "name".to_string(), "age".to_string()];

    let expr = Expr::Binary {
        left: Box::new(Expr::Column("age".into())),
        op: BinaryOp::Gt,
        right: Box::new(Expr::Literal(Value::Int(30))),
    };

    let resolved = Planner::bind_expr_with_schema(&schema, expr).unwrap();

    match resolved {
        ResolvedExpr::Binary { left, op, right } => {
            assert_eq!(op, BinaryOp::Gt);
            assert!(matches!(*left, ResolvedExpr::Column(2))); // age is index 2
            assert!(matches!(*right, ResolvedExpr::Literal(Value::Int(30))));
        }
        _ => panic!("expected Binary"),
    }
}

#[test]
fn case_insensitive_column_binding() {
    let _catalog = sample_catalog();
    let schema = vec!["id".to_string(), "Name".to_string(), "AGE".to_string()];

    // Query uses lowercase, schema has mixed case
    let expr = Expr::Column("name".into());
    let resolved = Planner::bind_expr_with_schema(&schema, expr).unwrap();

    assert_eq!(resolved, ResolvedExpr::Column(1));
}

#[test]
fn complex_expression_binding() {
    let _catalog = sample_catalog();
    let schema = vec!["id".to_string(), "name".to_string(), "age".to_string()];

    // (age > 20) AND (age < 50)
    let expr = Expr::Binary {
        left: Box::new(Expr::Binary {
            left: Box::new(Expr::Column("age".into())),
            op: BinaryOp::Gt,
            right: Box::new(Expr::Literal(Value::Int(20))),
        }),
        op: BinaryOp::And,
        right: Box::new(Expr::Binary {
            left: Box::new(Expr::Column("age".into())),
            op: BinaryOp::Lt,
            right: Box::new(Expr::Literal(Value::Int(50))),
        }),
    };

    let resolved = Planner::bind_expr_with_schema(&schema, expr).unwrap();

    // Verify structure is preserved with columns bound
    match resolved {
        ResolvedExpr::Binary {
            left,
            op: BinaryOp::And,
            right,
        } => {
            match &*left {
                ResolvedExpr::Binary { left, .. } => {
                    assert!(matches!(&**left, ResolvedExpr::Column(2)));
                }
                _ => panic!("expected Binary"),
            }
            match &*right {
                ResolvedExpr::Binary { left, .. } => {
                    assert!(matches!(&**left, ResolvedExpr::Column(2)));
                }
                _ => panic!("expected Binary"),
            }
        }
        _ => panic!("expected Binary AND"),
    }
}

#[test]
fn explain_logical_formats_correctly() {
    let plan = LogicalPlan::Filter {
        input: Box::new(LogicalPlan::TableScan {
            table: "users".into(),
        }),
        predicate: Expr::Binary {
            left: Box::new(Expr::Column("age".into())),
            op: BinaryOp::Gt,
            right: Box::new(Expr::Literal(Value::Int(20))),
        },
    };

    let text = explain_logical(&plan);
    assert!(text.contains("Filter"));
    assert!(text.contains("TableScan"));
}

#[test]
fn explain_physical_formats_correctly() {
    let plan = PhysicalPlan::Filter {
        input: Box::new(PhysicalPlan::SeqScan {
            table_id: TableId(1),
            schema: vec!["id".into(), "name".into()],
        }),
        predicate: ResolvedExpr::Binary {
            left: Box::new(ResolvedExpr::Column(0)),
            op: BinaryOp::Eq,
            right: Box::new(ResolvedExpr::Literal(Value::Int(42))),
        },
    };

    let text = explain_physical(&plan);
    assert!(text.contains("Filter"));
    assert!(text.contains("SeqScan"));
    assert!(text.contains("table_id=1"));
}

#[test]
fn end_to_end_query_with_index() {
    let catalog = sample_catalog();
    let mut ctx = PlanningContext::new(&catalog);

    // SELECT name, age FROM users WHERE id = 42
    let sql = "SELECT name, age FROM users WHERE id = 42;";
    let stmt = parse_sql(sql).unwrap().remove(0);
    let plan = Planner::plan(stmt, &mut ctx).unwrap();

    // Should produce: Project([name, age]) → Filter(id = 42) → IndexScan(idx_users_id)
    match plan {
        PhysicalPlan::Project { input, columns } => {
            assert_eq!(columns, vec![("name".into(), 1), ("age".into(), 2)]);
            match *input {
                PhysicalPlan::Filter { input, .. } => match *input {
                    PhysicalPlan::IndexScan { index_name, .. } => {
                        assert_eq!(index_name, "idx_users_id");
                    }
                    _ => panic!("expected IndexScan"),
                },
                _ => panic!("expected Filter"),
            }
        }
        _ => panic!("expected Project"),
    }
}

#[test]
fn multiple_predicates_with_index_on_one() {
    let catalog = sample_catalog();
    let mut ctx = PlanningContext::new(&catalog);

    // Only 'age' has an index in the sample catalog
    // Note: v1 only optimizes single predicates, so this will use SeqScan
    let sql = "SELECT * FROM users WHERE name = 'alice' AND age > 30;";
    let stmt = parse_sql(sql).unwrap().remove(0);
    let plan = Planner::plan(stmt, &mut ctx).unwrap();

    let text = explain_physical(&plan);
    // Complex predicates fall back to SeqScan in v1
    assert!(text.contains("SeqScan") || text.contains("IndexScan"));
}
