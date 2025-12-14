use super::*;
use types::Value::*;

fn schema(cols: &[&str]) -> Vec<String> {
    cols.iter().map(|s| s.to_string()).collect()
}

/// Helper to create an unqualified column reference.
fn col(name: &str) -> Expr {
    Expr::Column {
        table: None,
        name: name.to_string(),
    }
}

/// Helper to create a qualified column reference (table.column).
fn qual_col(table: &str, name: &str) -> Expr {
    Expr::Column {
        table: Some(table.to_string()),
        name: name.to_string(),
    }
}

#[test]
fn eval_literals_and_columns() {
    let row = Row::new(vec![Int(1), Text("Will".into()), Bool(true)]);
    let schema = schema(&["id", "name", "active"]);
    let ctx = EvalContext { schema: &schema };

    assert_eq!(ctx.eval(&Expr::Literal(Int(42)), &row).unwrap(), Int(42));
    assert_eq!(ctx.eval(&col("name"), &row).unwrap(), Text("Will".into()));
}

#[test]
fn eval_comparisons() {
    let row = Row::new(vec![Int(10), Int(20)]);
    let schema = schema(&["a", "b"]);
    let ctx = EvalContext { schema: &schema };

    let lt = Expr::Binary {
        left: Box::new(col("a")),
        op: BinaryOp::Lt,
        right: Box::new(col("b")),
    };
    assert_eq!(ctx.eval(&lt, &row).unwrap(), Bool(true));
}

#[test]
fn eval_logical_ops() {
    let row = Row::new(vec![Bool(true), Bool(false)]);
    let schema = schema(&["x", "y"]);
    let ctx = EvalContext { schema: &schema };

    let expr = Expr::Binary {
        left: Box::new(col("x")),
        op: BinaryOp::And,
        right: Box::new(col("y")),
    };
    assert_eq!(ctx.eval(&expr, &row).unwrap(), Bool(false));
}

#[test]
fn or_operator_truth_table() {
    let schema = schema(&["a", "b"]);
    let ctx = EvalContext { schema: &schema };
    let expr = Expr::Binary {
        left: Box::new(col("a")),
        op: BinaryOp::Or,
        right: Box::new(col("b")),
    };

    for (lhs, rhs, expected) in [
        (false, false, false),
        (false, true, true),
        (true, false, true),
        (true, true, true),
    ] {
        let result = ctx
            .eval(&expr, &Row::new(vec![Bool(lhs), Bool(rhs)]))
            .unwrap_or_else(|e| panic!("unexpected error for ({lhs}, {rhs}): {e:?}"));
        match result {
            Bool(actual) => assert_eq!(
                actual, expected,
                "a={lhs}, b={rhs} should yield {expected}, got {actual}"
            ),
            other => panic!("expected Value::Bool, got {other:?}"),
        }
    }
}

#[test]
fn not_operator() {
    let row = Row::new(vec![Bool(false)]);
    let schema = schema(&["f"]);
    let ctx = EvalContext { schema: &schema };

    let expr = Expr::Unary {
        op: UnaryOp::Not,
        expr: Box::new(col("f")),
    };
    assert_eq!(ctx.eval(&expr, &row).unwrap(), Bool(true));
}

#[test]
fn mismatched_types_fail() {
    let row = Row::new(vec![Int(1), Text("hi".into())]);
    let schema = schema(&["a", "b"]);
    let ctx = EvalContext { schema: &schema };

    let expr = Expr::Binary {
        left: Box::new(col("a")),
        op: BinaryOp::Eq,
        right: Box::new(col("b")),
    };

    let err = ctx.eval(&expr, &row).unwrap_err();
    assert!(format!("{err:?}").contains("incompatible types"));
}

#[test]
fn not_operator_requires_bool_input() {
    let row = Row::new(vec![Int(0)]);
    let schema = schema(&["flag"]);
    let ctx = EvalContext { schema: &schema };

    let expr = Expr::Unary {
        op: UnaryOp::Not,
        expr: Box::new(col("flag")),
    };

    let err = ctx.eval(&expr, &row).unwrap_err();
    assert!(format!("{err:?}").contains("NOT expects bool"));
}

#[test]
fn logical_ops_require_boolean_operands() {
    let row = Row::new(vec![Bool(true), Int(1)]);
    let schema = schema(&["a", "b"]);
    let ctx = EvalContext { schema: &schema };

    let expr = Expr::Binary {
        left: Box::new(col("a")),
        op: BinaryOp::Or,
        right: Box::new(col("b")),
    };

    let err = ctx.eval(&expr, &row).unwrap_err();
    assert!(format!("{err:?}").contains("AND/OR expects bools"));
}

#[test]
fn comparison_variants_cover_all_orderings() {
    let empty_schema: Vec<String> = Vec::new();
    let ctx = EvalContext {
        schema: &empty_schema,
    };
    let cmp = |op, left, right| {
        let expr = Expr::Binary {
            left: Box::new(Expr::Literal(Int(left))),
            op,
            right: Box::new(Expr::Literal(Int(right))),
        };
        match ctx.eval(&expr, &Row::new(vec![])).unwrap() {
            Bool(b) => b,
            other => panic!("expected bool, got {other:?}"),
        }
    };

    assert!(cmp(BinaryOp::Eq, 5, 5));
    assert!(!cmp(BinaryOp::Eq, 5, 6));
    assert!(cmp(BinaryOp::Ne, 5, 6));
    assert!(cmp(BinaryOp::Lt, 1, 2));
    assert!(cmp(BinaryOp::Le, 2, 2));
    assert!(cmp(BinaryOp::Le, 1, 2));
    assert!(cmp(BinaryOp::Gt, 3, 2));
    assert!(cmp(BinaryOp::Ge, 3, 3));
    assert!(cmp(BinaryOp::Ge, 4, 3));
}

// --- Tests for qualified column references (JOIN support) ---

#[test]
fn eval_qualified_column_exact_match() {
    // Schema with qualified column names (as produced by join)
    let row = Row::new(vec![Int(1), Text("alice".into()), Int(100)]);
    let schema = schema(&["users.id", "users.name", "orders.amount"]);
    let ctx = EvalContext { schema: &schema };

    // Qualified reference: users.id -> ordinal 0
    assert_eq!(ctx.eval(&qual_col("users", "id"), &row).unwrap(), Int(1));

    // Qualified reference: orders.amount -> ordinal 2
    assert_eq!(
        ctx.eval(&qual_col("orders", "amount"), &row).unwrap(),
        Int(100)
    );
}

#[test]
fn eval_unqualified_column_suffix_match() {
    // Schema with qualified column names
    let row = Row::new(vec![Int(1), Text("alice".into()), Int(100)]);
    let schema = schema(&["users.id", "users.name", "orders.amount"]);
    let ctx = EvalContext { schema: &schema };

    // Unqualified "name" should match "users.name" (suffix match)
    assert_eq!(ctx.eval(&col("name"), &row).unwrap(), Text("alice".into()));

    // Unqualified "amount" should match "orders.amount"
    assert_eq!(ctx.eval(&col("amount"), &row).unwrap(), Int(100));
}

#[test]
fn eval_unqualified_column_simple_schema() {
    // Simple schema without table prefixes
    let row = Row::new(vec![Int(42), Text("test".into())]);
    let schema = schema(&["id", "value"]);
    let ctx = EvalContext { schema: &schema };

    // Unqualified columns should match simple names
    assert_eq!(ctx.eval(&col("id"), &row).unwrap(), Int(42));
    assert_eq!(ctx.eval(&col("value"), &row).unwrap(), Text("test".into()));
}

#[test]
fn eval_qualified_column_not_found() {
    let row = Row::new(vec![Int(1)]);
    let schema = schema(&["users.id"]);
    let ctx = EvalContext { schema: &schema };

    // Wrong table qualifier
    let err = ctx.eval(&qual_col("orders", "id"), &row).unwrap_err();
    assert!(format!("{err:?}").contains("unknown column 'orders.id'"));
}

#[test]
fn eval_unqualified_column_not_found() {
    let row = Row::new(vec![Int(1)]);
    let schema = schema(&["users.id"]);
    let ctx = EvalContext { schema: &schema };

    // Column doesn't exist
    let err = ctx.eval(&col("name"), &row).unwrap_err();
    assert!(format!("{err:?}").contains("unknown column 'name'"));
}

#[test]
fn eval_join_condition_expression() {
    // Simulating: SELECT * FROM users u JOIN orders o ON u.id = o.user_id
    let row = Row::new(vec![Int(1), Text("alice".into()), Int(101), Int(1)]);
    let schema = schema(&["u.id", "u.name", "o.id", "o.user_id"]);
    let ctx = EvalContext { schema: &schema };

    // u.id = o.user_id
    let condition = Expr::Binary {
        left: Box::new(qual_col("u", "id")),
        op: BinaryOp::Eq,
        right: Box::new(qual_col("o", "user_id")),
    };

    assert_eq!(ctx.eval(&condition, &row).unwrap(), Bool(true));
}

#[test]
fn eval_join_condition_no_match() {
    // Row where u.id != o.user_id
    let row = Row::new(vec![Int(1), Text("alice".into()), Int(101), Int(2)]);
    let schema = schema(&["u.id", "u.name", "o.id", "o.user_id"]);
    let ctx = EvalContext { schema: &schema };

    // u.id = o.user_id
    let condition = Expr::Binary {
        left: Box::new(qual_col("u", "id")),
        op: BinaryOp::Eq,
        right: Box::new(qual_col("o", "user_id")),
    };

    assert_eq!(ctx.eval(&condition, &row).unwrap(), Bool(false));
}
