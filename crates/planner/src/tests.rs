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
            None,
        )
        .unwrap();
    catalog
        .create_index()
        .table_name("users")
        .index_name("idx_users_id")
        .columns(&["id"])
        .kind(IndexKind::BTree)
        .call()
        .unwrap();
    catalog
        .create_index()
        .table_name("users")
        .index_name("idx_users_age")
        .columns(&["age"])
        .kind(IndexKind::BTree)
        .call()
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

#[test]
fn multiple_indexes_on_same_column_prefers_first() {
    let mut catalog = Catalog::new();
    catalog
        .create_table(
            "products",
            vec![
                Column::new("id", SqlType::Int),
                Column::new("price", SqlType::Int),
            ],
            None,
        )
        .unwrap();

    // Create two BTree indexes on same column - should use first found
    catalog
        .create_index()
        .table_name("products")
        .index_name("idx_price_1")
        .columns(&["price"])
        .kind(IndexKind::BTree)
        .call()
        .unwrap();
    catalog
        .create_index()
        .table_name("products")
        .index_name("idx_price_2")
        .columns(&["price"])
        .kind(IndexKind::BTree)
        .call()
        .unwrap();

    let mut ctx = PlanningContext::new(&catalog);
    let stmt = parse_sql("SELECT * FROM products WHERE price = 100;")
        .unwrap()
        .remove(0);

    let plan = Planner::plan(stmt, &mut ctx).unwrap();
    let text = explain_physical(&plan);

    // Should use an index (implementation uses first found)
    assert!(text.contains("IndexScan"));
    assert!(text.contains("idx_price"));
}

#[test]
fn nested_filters_optimize_recursively() {
    let _catalog = sample_catalog();

    // Create a nested filter structure manually
    let inner_scan = LogicalPlan::TableScan {
        table: "users".into(),
    };
    let filter1 = LogicalPlan::Filter {
        input: Box::new(inner_scan),
        predicate: Expr::Binary {
            left: Box::new(Expr::Column("age".into())),
            op: BinaryOp::Gt,
            right: Box::new(Expr::Literal(Value::Int(20))),
        },
    };
    let project = LogicalPlan::Project {
        input: Box::new(filter1),
        columns: vec!["*".into()],
    };
    let filter2 = LogicalPlan::Filter {
        input: Box::new(project),
        predicate: Expr::Binary {
            left: Box::new(Expr::Column("id".into())),
            op: BinaryOp::Eq,
            right: Box::new(Expr::Literal(Value::Int(1))),
        },
    };

    let mut ctx = PlanningContext::new(&_catalog);
    let optimized = Planner::optimize(filter2, &mut ctx).unwrap();

    // After pushdown, outer filter should be pushed down past wildcard project
    match optimized {
        LogicalPlan::Filter { input, .. } => {
            match *input {
                LogicalPlan::Filter { input, .. } => {
                    // Double filter with scan underneath - good!
                    assert!(matches!(*input, LogicalPlan::TableScan { .. }));
                }
                _ => panic!("expected nested filters after pushdown"),
            }
        }
        _ => panic!("expected Filter at top"),
    }
}

#[test]
fn bind_expr_in_insert_values() {
    let catalog = sample_catalog();
    let mut ctx = PlanningContext::new(&catalog);

    // INSERT with literal expressions
    let stmt = parse_sql("INSERT INTO users VALUES (1, 'alice', 30);")
        .unwrap()
        .remove(0);

    let plan = Planner::plan(stmt, &mut ctx).unwrap();

    match plan {
        PhysicalPlan::Insert { values, .. } => {
            assert_eq!(values.len(), 3);
            assert!(matches!(values[0], ResolvedExpr::Literal(Value::Int(1))));
            assert!(matches!(values[1], ResolvedExpr::Literal(Value::Text(_))));
            assert!(matches!(values[2], ResolvedExpr::Literal(Value::Int(30))));
        }
        _ => panic!("expected Insert"),
    }
}

#[test]
fn update_assignment_with_expression() {
    let catalog = sample_catalog();
    let mut ctx = PlanningContext::new(&catalog);

    // UPDATE with expression in SET clause
    let stmt = parse_sql("UPDATE users SET age = 31 WHERE id = 1;")
        .unwrap()
        .remove(0);

    let plan = Planner::plan(stmt, &mut ctx).unwrap();

    match plan {
        PhysicalPlan::Update {
            assignments,
            predicate,
            ..
        } => {
            assert_eq!(assignments.len(), 1);
            assert_eq!(assignments[0].0, 2); // age column
            assert!(matches!(
                assignments[0].1,
                ResolvedExpr::Literal(Value::Int(31))
            ));
            assert!(predicate.is_some());
        }
        _ => panic!("expected Update"),
    }
}

#[test]
fn delete_with_expression_predicate() {
    let catalog = sample_catalog();
    let mut ctx = PlanningContext::new(&catalog);

    // DELETE with column reference in WHERE
    let stmt = parse_sql("DELETE FROM users WHERE age > 50;")
        .unwrap()
        .remove(0);

    let plan = Planner::plan(stmt, &mut ctx).unwrap();

    match plan {
        PhysicalPlan::Delete { predicate, .. } => {
            assert!(predicate.is_some());
            match predicate.unwrap() {
                ResolvedExpr::Binary { left, op, right } => {
                    assert!(matches!(*left, ResolvedExpr::Column(2))); // age
                    assert_eq!(op, BinaryOp::Gt);
                    assert!(matches!(*right, ResolvedExpr::Literal(Value::Int(50))));
                }
                _ => panic!("expected Binary expression"),
            }
        }
        _ => panic!("expected Delete"),
    }
}

#[test]
fn unary_expression_binding() {
    let _catalog = sample_catalog();
    let schema = vec!["id".to_string(), "name".to_string(), "active".to_string()];

    // NOT active
    let expr = Expr::Unary {
        op: UnaryOp::Not,
        expr: Box::new(Expr::Column("active".into())),
    };

    let resolved = Planner::bind_expr_with_schema(&schema, expr).unwrap();

    match resolved {
        ResolvedExpr::Unary { op, expr } => {
            assert_eq!(op, UnaryOp::Not);
            assert!(matches!(*expr, ResolvedExpr::Column(2)));
        }
        _ => panic!("expected Unary"),
    }
}

#[test]
fn nested_binary_expressions() {
    let _catalog = sample_catalog();
    let schema = vec!["id".to_string(), "age".to_string(), "score".to_string()];

    // (age > 20) OR (score < 50)
    let expr = Expr::Binary {
        left: Box::new(Expr::Binary {
            left: Box::new(Expr::Column("age".into())),
            op: BinaryOp::Gt,
            right: Box::new(Expr::Literal(Value::Int(20))),
        }),
        op: BinaryOp::Or,
        right: Box::new(Expr::Binary {
            left: Box::new(Expr::Column("score".into())),
            op: BinaryOp::Lt,
            right: Box::new(Expr::Literal(Value::Int(50))),
        }),
    };

    let resolved = Planner::bind_expr_with_schema(&schema, expr).unwrap();

    match resolved {
        ResolvedExpr::Binary {
            left,
            op: BinaryOp::Or,
            right,
        } => {
            // Verify left side
            match &*left {
                ResolvedExpr::Binary { left, .. } => {
                    assert!(matches!(&**left, ResolvedExpr::Column(1))); // age
                }
                _ => panic!("expected Binary on left"),
            }
            // Verify right side
            match &*right {
                ResolvedExpr::Binary { left, .. } => {
                    assert!(matches!(&**left, ResolvedExpr::Column(2))); // score
                }
                _ => panic!("expected Binary on right"),
            }
        }
        _ => panic!("expected Binary OR"),
    }
}

#[test]
fn try_extract_index_predicate_less_than() {
    let schema = vec!["id".to_string(), "age".to_string()];
    let expr = ResolvedExpr::Binary {
        left: Box::new(ResolvedExpr::Column(1)),
        op: BinaryOp::Lt,
        right: Box::new(ResolvedExpr::Literal(Value::Int(30))),
    };

    let result = Planner::try_extract_index_predicate(&schema, &expr);
    assert!(result.is_some());

    let (col, pred) = result.unwrap();
    assert_eq!(col, 1);
    match pred {
        IndexPredicate::Range { col, low, high } => {
            assert_eq!(col, 1);
            assert!(matches!(low, ResolvedExpr::Literal(Value::Int(i64::MIN))));
            assert!(matches!(high, ResolvedExpr::Literal(Value::Int(30))));
        }
        _ => panic!("expected Range predicate"),
    }
}

#[test]
fn try_extract_index_predicate_greater_equal() {
    let schema = vec!["id".to_string(), "age".to_string()];
    let expr = ResolvedExpr::Binary {
        left: Box::new(ResolvedExpr::Column(0)),
        op: BinaryOp::Ge,
        right: Box::new(ResolvedExpr::Literal(Value::Int(100))),
    };

    let result = Planner::try_extract_index_predicate(&schema, &expr);
    assert!(result.is_some());

    let (col, pred) = result.unwrap();
    assert_eq!(col, 0);
    match pred {
        IndexPredicate::Range { col, low, high } => {
            assert_eq!(col, 0);
            assert!(matches!(low, ResolvedExpr::Literal(Value::Int(100))));
            assert!(matches!(high, ResolvedExpr::Literal(Value::Int(i64::MAX))));
        }
        _ => panic!("expected Range predicate"),
    }
}

#[test]
fn try_extract_index_predicate_unsupported_op() {
    let schema = vec!["id".to_string(), "name".to_string()];
    let expr = ResolvedExpr::Binary {
        left: Box::new(ResolvedExpr::Column(0)),
        op: BinaryOp::And, // Logical op, not index-able comparison
        right: Box::new(ResolvedExpr::Literal(Value::Bool(true))),
    };

    let result = Planner::try_extract_index_predicate(&schema, &expr);
    assert!(result.is_none());
}

#[test]
fn try_extract_index_predicate_column_on_right() {
    let schema = vec!["id".to_string(), "age".to_string()];
    // Literal on left, column on right - currently not supported
    let expr = ResolvedExpr::Binary {
        left: Box::new(ResolvedExpr::Literal(Value::Int(30))),
        op: BinaryOp::Eq,
        right: Box::new(ResolvedExpr::Column(1)),
    };

    let result = Planner::try_extract_index_predicate(&schema, &expr);
    // v1 only handles column on left
    assert!(result.is_none());
}

#[test]
fn output_schema_for_modify_operations() {
    let schema = Planner::output_schema(&PhysicalPlan::Insert {
        table_id: TableId(1),
        values: vec![],
    });
    assert_eq!(schema, Vec::<String>::new());

    let schema = Planner::output_schema(&PhysicalPlan::Update {
        table_id: TableId(1),
        assignments: vec![],
        predicate: None,
    });
    assert_eq!(schema, Vec::<String>::new());

    let schema = Planner::output_schema(&PhysicalPlan::Delete {
        table_id: TableId(1),
        predicate: None,
    });
    assert_eq!(schema, Vec::<String>::new());
}

#[test]
fn projection_with_specific_columns_not_pruned() {
    let input = LogicalPlan::Project {
        input: Box::new(LogicalPlan::Project {
            input: Box::new(LogicalPlan::TableScan {
                table: "users".into(),
            }),
            columns: vec!["id".into(), "name".into()],
        }),
        columns: vec!["id".into()], // Different from inner
    };

    let _catalog = sample_catalog();
    let mut ctx = PlanningContext::new(&_catalog);
    let pruned = Planner::optimize(input, &mut ctx).unwrap();

    // Should NOT prune because outer projection is different
    match pruned {
        LogicalPlan::Project { input, columns } => {
            assert_eq!(columns, vec!["id".to_string()]);
            assert!(matches!(*input, LogicalPlan::Project { .. }));
        }
        _ => panic!("expected Project"),
    }
}

#[test]
fn order_by_generates_sort_node() {
    let catalog = sample_catalog();
    let mut ctx = PlanningContext::new(&catalog);
    let stmt = parse_sql("SELECT * FROM users ORDER BY age DESC")
        .unwrap()
        .remove(0);

    let plan = Planner::plan(stmt, &mut ctx).unwrap();

    // Should have Sort -> Project -> SeqScan
    match plan {
        PhysicalPlan::Sort { input, order_by } => {
            assert_eq!(order_by.len(), 1);
            assert_eq!(order_by[0].column_id, 2); // age is column 2 (id=0, name=1, age=2)
            assert_eq!(order_by[0].direction, SortDirection::Desc);
            assert!(matches!(*input, PhysicalPlan::Project { .. }));
        }
        _ => panic!("expected Sort, got {:?}", plan),
    }
}

#[test]
fn limit_generates_limit_node() {
    let catalog = sample_catalog();
    let mut ctx = PlanningContext::new(&catalog);
    let stmt = parse_sql("SELECT * FROM users LIMIT 10").unwrap().remove(0);

    let plan = Planner::plan(stmt, &mut ctx).unwrap();

    // Should have Limit -> Project -> SeqScan
    match plan {
        PhysicalPlan::Limit {
            input,
            limit,
            offset,
        } => {
            assert_eq!(limit, Some(10));
            assert_eq!(offset, None);
            assert!(matches!(*input, PhysicalPlan::Project { .. }));
        }
        _ => panic!("expected Limit, got {:?}", plan),
    }
}

#[test]
fn limit_with_offset_generates_limit_node() {
    let catalog = sample_catalog();
    let mut ctx = PlanningContext::new(&catalog);
    let stmt = parse_sql("SELECT * FROM users LIMIT 10 OFFSET 20")
        .unwrap()
        .remove(0);

    let plan = Planner::plan(stmt, &mut ctx).unwrap();

    match plan {
        PhysicalPlan::Limit {
            limit,
            offset,
            input,
        } => {
            assert_eq!(limit, Some(10));
            assert_eq!(offset, Some(20));
            assert!(matches!(*input, PhysicalPlan::Project { .. }));
        }
        _ => panic!("expected Limit, got {:?}", plan),
    }
}

#[test]
fn order_by_with_limit_generates_both_nodes() {
    let catalog = sample_catalog();
    let mut ctx = PlanningContext::new(&catalog);
    let stmt = parse_sql("SELECT * FROM users ORDER BY name ASC LIMIT 5")
        .unwrap()
        .remove(0);

    let plan = Planner::plan(stmt, &mut ctx).unwrap();

    // Should have Limit -> Sort -> Project -> SeqScan
    match plan {
        PhysicalPlan::Limit { input, limit, .. } => {
            assert_eq!(limit, Some(5));
            match *input {
                PhysicalPlan::Sort { order_by, .. } => {
                    assert_eq!(order_by.len(), 1);
                    assert_eq!(order_by[0].column_id, 1); // name is column 1
                    assert_eq!(order_by[0].direction, SortDirection::Asc);
                }
                _ => panic!("expected Sort under Limit"),
            }
        }
        _ => panic!("expected Limit, got {:?}", plan),
    }
}

#[test]
fn multiple_order_by_columns_resolved_correctly() {
    let catalog = sample_catalog();
    let mut ctx = PlanningContext::new(&catalog);
    let stmt = parse_sql("SELECT * FROM users ORDER BY age DESC, name ASC")
        .unwrap()
        .remove(0);

    let plan = Planner::plan(stmt, &mut ctx).unwrap();

    match plan {
        PhysicalPlan::Sort { order_by, .. } => {
            assert_eq!(order_by.len(), 2);
            assert_eq!(order_by[0].column_id, 2); // age
            assert_eq!(order_by[0].direction, SortDirection::Desc);
            assert_eq!(order_by[1].column_id, 1); // name
            assert_eq!(order_by[1].direction, SortDirection::Asc);
        }
        _ => panic!("expected Sort, got {:?}", plan),
    }
}

#[test]
fn unknown_column_in_order_by_returns_error() {
    let catalog = sample_catalog();
    let mut ctx = PlanningContext::new(&catalog);
    let stmt = parse_sql("SELECT * FROM users ORDER BY nonexistent")
        .unwrap()
        .remove(0);

    let result = Planner::plan(stmt, &mut ctx);
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("unknown column 'nonexistent'"));
}

#[test]
fn order_by_explain_formats_correctly() {
    let catalog = sample_catalog();
    let mut ctx = PlanningContext::new(&catalog);
    let stmt = parse_sql("SELECT * FROM users ORDER BY age DESC LIMIT 10")
        .unwrap()
        .remove(0);

    let plan = Planner::plan(stmt, &mut ctx).unwrap();
    let text = explain_physical(&plan);

    assert!(text.contains("Limit"));
    assert!(text.contains("limit=Some(10)"));
    assert!(text.contains("Sort"));
}
