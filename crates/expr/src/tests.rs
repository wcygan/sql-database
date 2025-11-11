use super::*;
use types::Value::*;

fn schema(cols: &[&str]) -> Vec<String> {
    cols.iter().map(|s| s.to_string()).collect()
}

#[test]
fn eval_literals_and_columns() {
    let row = Row(vec![Int(1), Text("Will".into()), Bool(true)]);
    let schema = schema(&["id", "name", "active"]);
    let ctx = EvalContext { schema: &schema };

    assert_eq!(ctx.eval(&Expr::Literal(Int(42)), &row).unwrap(), Int(42));
    assert_eq!(
        ctx.eval(&Expr::Column("name".into()), &row).unwrap(),
        Text("Will".into())
    );
}

#[test]
fn eval_comparisons() {
    let row = Row(vec![Int(10), Int(20)]);
    let schema = schema(&["a", "b"]);
    let ctx = EvalContext { schema: &schema };

    let lt = Expr::Binary {
        left: Box::new(Expr::Column("a".into())),
        op: BinaryOp::Lt,
        right: Box::new(Expr::Column("b".into())),
    };
    assert_eq!(ctx.eval(&lt, &row).unwrap(), Bool(true));
}

#[test]
fn eval_logical_ops() {
    let row = Row(vec![Bool(true), Bool(false)]);
    let schema = schema(&["x", "y"]);
    let ctx = EvalContext { schema: &schema };

    let expr = Expr::Binary {
        left: Box::new(Expr::Column("x".into())),
        op: BinaryOp::And,
        right: Box::new(Expr::Column("y".into())),
    };
    assert_eq!(ctx.eval(&expr, &row).unwrap(), Bool(false));
}

#[test]
fn or_operator_truth_table() {
    let schema = schema(&["a", "b"]);
    let ctx = EvalContext { schema: &schema };
    let expr = Expr::Binary {
        left: Box::new(Expr::Column("a".into())),
        op: BinaryOp::Or,
        right: Box::new(Expr::Column("b".into())),
    };

    for (lhs, rhs, expected) in [
        (false, false, false),
        (false, true, true),
        (true, false, true),
        (true, true, true),
    ] {
        let result = ctx
            .eval(&expr, &Row(vec![Bool(lhs), Bool(rhs)]))
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
    let row = Row(vec![Bool(false)]);
    let schema = schema(&["f"]);
    let ctx = EvalContext { schema: &schema };

    let expr = Expr::Unary {
        op: UnaryOp::Not,
        expr: Box::new(Expr::Column("f".into())),
    };
    assert_eq!(ctx.eval(&expr, &row).unwrap(), Bool(true));
}

#[test]
fn mismatched_types_fail() {
    let row = Row(vec![Int(1), Text("hi".into())]);
    let schema = schema(&["a", "b"]);
    let ctx = EvalContext { schema: &schema };

    let expr = Expr::Binary {
        left: Box::new(Expr::Column("a".into())),
        op: BinaryOp::Eq,
        right: Box::new(Expr::Column("b".into())),
    };

    let err = ctx.eval(&expr, &row).unwrap_err();
    assert!(format!("{err:?}").contains("incompatible types"));
}

#[test]
fn not_operator_requires_bool_input() {
    let row = Row(vec![Int(0)]);
    let schema = schema(&["flag"]);
    let ctx = EvalContext { schema: &schema };

    let expr = Expr::Unary {
        op: UnaryOp::Not,
        expr: Box::new(Expr::Column("flag".into())),
    };

    let err = ctx.eval(&expr, &row).unwrap_err();
    assert!(format!("{err:?}").contains("NOT expects bool"));
}

#[test]
fn logical_ops_require_boolean_operands() {
    let row = Row(vec![Bool(true), Int(1)]);
    let schema = schema(&["a", "b"]);
    let ctx = EvalContext { schema: &schema };

    let expr = Expr::Binary {
        left: Box::new(Expr::Column("a".into())),
        op: BinaryOp::Or,
        right: Box::new(Expr::Column("b".into())),
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
        match ctx.eval(&expr, &Row(vec![])).unwrap() {
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
