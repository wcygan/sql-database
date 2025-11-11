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
