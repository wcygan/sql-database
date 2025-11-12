//! Common test fixtures and data generators.
//!
//! Provides reusable test data, row builders, and expression builders
//! following patterns from existing executor test helpers.

use common::Row;
use expr::{BinaryOp, UnaryOp};
use planner::ResolvedExpr;
use types::Value;

/// Build a row with integer values.
///
/// # Example
///
/// ```
/// use testsupport::prelude::*;
///
/// let row = int_row(&[1, 2, 3]);
/// assert_eq!(row.values.len(), 3);
/// ```
pub fn int_row(values: &[i64]) -> Row {
    Row::new(values.iter().map(|&v| Value::Int(v)).collect())
}

/// Build a row with text values.
///
/// # Example
///
/// ```
/// use testsupport::prelude::*;
///
/// let row = text_row(&["Alice", "Bob", "Charlie"]);
/// assert_eq!(row.values.len(), 3);
/// ```
pub fn text_row(values: &[&str]) -> Row {
    Row::new(values
        .iter()
        .map(|&v| Value::Text(v.to_string()))
        .collect())
}

/// Build a row with boolean values.
///
/// # Example
///
/// ```
/// use testsupport::prelude::*;
///
/// let row = bool_row(&[true, false, true]);
/// assert_eq!(row.values.len(), 3);
/// ```
pub fn bool_row(values: &[bool]) -> Row {
    Row::new(values.iter().map(|&v| Value::Bool(v)).collect())
}

/// Build a row with mixed value types.
///
/// # Example
///
/// ```
/// use testsupport::prelude::*;
/// use types::Value;
///
/// let row = mixed_row(vec![
///     Value::Int(42),
///     Value::Text("hello".into()),
///     Value::Bool(true),
/// ]);
/// assert_eq!(row.values.len(), 3);
/// ```
pub fn mixed_row(values: Vec<Value>) -> Row {
    Row::new(values)
}

/// Build a row with NULL values.
///
/// # Example
///
/// ```
/// use testsupport::prelude::*;
///
/// let row = null_row(3);
/// assert_eq!(row.values.len(), 3);
/// ```
pub fn null_row(count: usize) -> Row {
    Row::new(vec![Value::Null; count])
}

// Expression builders for testing

/// Create a literal integer expression.
///
/// # Example
///
/// ```
/// use testsupport::prelude::*;
///
/// let expr = lit_int(42);
/// ```
pub fn lit_int(value: i64) -> ResolvedExpr {
    ResolvedExpr::Literal(Value::Int(value))
}

/// Create a literal text expression.
///
/// # Example
///
/// ```
/// use testsupport::prelude::*;
///
/// let expr = lit_text("hello");
/// ```
pub fn lit_text(value: &str) -> ResolvedExpr {
    ResolvedExpr::Literal(Value::Text(value.to_string()))
}

/// Create a literal boolean expression.
///
/// # Example
///
/// ```
/// use testsupport::prelude::*;
///
/// let expr = lit_bool(true);
/// ```
pub fn lit_bool(value: bool) -> ResolvedExpr {
    ResolvedExpr::Literal(Value::Bool(value))
}

/// Create a literal NULL expression.
///
/// # Example
///
/// ```
/// use testsupport::prelude::*;
///
/// let expr = lit_null();
/// ```
pub fn lit_null() -> ResolvedExpr {
    ResolvedExpr::Literal(Value::Null)
}

/// Create a literal value expression.
///
/// # Example
///
/// ```
/// use testsupport::prelude::*;
/// use types::Value;
///
/// let expr = lit(Value::Int(42));
/// ```
pub fn lit(value: Value) -> ResolvedExpr {
    ResolvedExpr::Literal(value)
}

/// Create a column reference expression.
///
/// # Example
///
/// ```
/// use testsupport::prelude::*;
///
/// let expr = col(0); // References first column
/// ```
pub fn col(id: u16) -> ResolvedExpr {
    ResolvedExpr::Column(id)
}

/// Create a binary operation expression.
///
/// # Example
///
/// ```
/// use testsupport::prelude::*;
/// use expr::BinaryOp;
///
/// let expr = binary(col(0), BinaryOp::Eq, lit_int(42));
/// ```
pub fn binary(left: ResolvedExpr, op: BinaryOp, right: ResolvedExpr) -> ResolvedExpr {
    ResolvedExpr::Binary {
        left: Box::new(left),
        op,
        right: Box::new(right),
    }
}

/// Create a unary operation expression.
///
/// # Example
///
/// ```
/// use testsupport::prelude::*;
/// use expr::UnaryOp;
///
/// let expr = unary(UnaryOp::Not, lit_bool(true));
/// ```
pub fn unary(op: UnaryOp, operand: ResolvedExpr) -> ResolvedExpr {
    ResolvedExpr::Unary {
        op,
        expr: Box::new(operand),
    }
}

/// Common table schemas for testing.
pub mod schemas {
    use catalog::{Column, TableSchema};
    use types::SqlType;

    /// Create a simple users table schema with id, name, age columns.
    ///
    /// Schema:
    /// - id: INT (column 0)
    /// - name: TEXT (column 1)
    /// - age: INT (column 2)
    pub fn users_schema() -> TableSchema {
        TableSchema::try_new(vec![
            Column::new("id".to_string(), SqlType::Int),
            Column::new("name".to_string(), SqlType::Text),
            Column::new("age".to_string(), SqlType::Int),
        ])
        .expect("valid schema")
    }

    /// Create a products table schema with id, name, price columns.
    ///
    /// Schema:
    /// - id: INT (column 0)
    /// - name: TEXT (column 1)
    /// - price: INT (column 2)
    pub fn products_schema() -> TableSchema {
        TableSchema::try_new(vec![
            Column::new("id".to_string(), SqlType::Int),
            Column::new("name".to_string(), SqlType::Text),
            Column::new("price".to_string(), SqlType::Int),
        ])
        .expect("valid schema")
    }

    /// Create an orders table schema with id, user_id, product_id, quantity columns.
    ///
    /// Schema:
    /// - id: INT (column 0)
    /// - user_id: INT (column 1)
    /// - product_id: INT (column 2)
    /// - quantity: INT (column 3)
    pub fn orders_schema() -> TableSchema {
        TableSchema::try_new(vec![
            Column::new("id".to_string(), SqlType::Int),
            Column::new("user_id".to_string(), SqlType::Int),
            Column::new("product_id".to_string(), SqlType::Int),
            Column::new("quantity".to_string(), SqlType::Int),
        ])
        .expect("valid schema")
    }
}

/// Sample test data generators.
pub mod data {
    use super::*;

    /// Generate sample user rows for testing.
    ///
    /// Returns rows for:
    /// - (1, "Alice", 30)
    /// - (2, "Bob", 25)
    /// - (3, "Charlie", 35)
    pub fn sample_users() -> Vec<Row> {
        vec![
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
            mixed_row(vec![
                Value::Int(3),
                Value::Text("Charlie".into()),
                Value::Int(35),
            ]),
        ]
    }

    /// Generate sample product rows for testing.
    ///
    /// Returns rows for:
    /// - (1, "Laptop", 1000)
    /// - (2, "Mouse", 25)
    /// - (3, "Keyboard", 75)
    pub fn sample_products() -> Vec<Row> {
        vec![
            mixed_row(vec![
                Value::Int(1),
                Value::Text("Laptop".into()),
                Value::Int(1000),
            ]),
            mixed_row(vec![
                Value::Int(2),
                Value::Text("Mouse".into()),
                Value::Int(25),
            ]),
            mixed_row(vec![
                Value::Int(3),
                Value::Text("Keyboard".into()),
                Value::Int(75),
            ]),
        ]
    }

    /// Generate sample order rows for testing.
    ///
    /// Returns rows for:
    /// - (1, 1, 1, 1) - Alice ordered 1 Laptop
    /// - (2, 1, 2, 2) - Alice ordered 2 Mice
    /// - (3, 2, 3, 1) - Bob ordered 1 Keyboard
    pub fn sample_orders() -> Vec<Row> {
        vec![
            int_row(&[1, 1, 1, 1]),
            int_row(&[2, 1, 2, 2]),
            int_row(&[3, 2, 3, 1]),
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_int_row() {
        let row = int_row(&[1, 2, 3]);
        assert_eq!(row.values.len(), 3);
        assert_eq!(row.values[0], Value::Int(1));
        assert_eq!(row.values[1], Value::Int(2));
        assert_eq!(row.values[2], Value::Int(3));
    }

    #[test]
    fn test_text_row() {
        let row = text_row(&["a", "b", "c"]);
        assert_eq!(row.values.len(), 3);
        assert_eq!(row.values[0], Value::Text("a".into()));
        assert_eq!(row.values[1], Value::Text("b".into()));
        assert_eq!(row.values[2], Value::Text("c".into()));
    }

    #[test]
    fn test_bool_row() {
        let row = bool_row(&[true, false, true]);
        assert_eq!(row.values.len(), 3);
        assert_eq!(row.values[0], Value::Bool(true));
        assert_eq!(row.values[1], Value::Bool(false));
        assert_eq!(row.values[2], Value::Bool(true));
    }

    #[test]
    fn test_null_row() {
        let row = null_row(3);
        assert_eq!(row.values.len(), 3);
        assert_eq!(row.values[0], Value::Null);
        assert_eq!(row.values[1], Value::Null);
        assert_eq!(row.values[2], Value::Null);
    }

    #[test]
    fn test_lit_builders() {
        match lit_int(42) {
            ResolvedExpr::Literal(Value::Int(42)) => {}
            _ => panic!("Expected literal int"),
        }

        match lit_text("hello") {
            ResolvedExpr::Literal(Value::Text(s)) if s == "hello" => {}
            _ => panic!("Expected literal text"),
        }

        match lit_bool(true) {
            ResolvedExpr::Literal(Value::Bool(true)) => {}
            _ => panic!("Expected literal bool"),
        }

        match lit_null() {
            ResolvedExpr::Literal(Value::Null) => {}
            _ => panic!("Expected literal null"),
        }
    }

    #[test]
    fn test_col_builder() {
        match col(5) {
            ResolvedExpr::Column(5) => {}
            _ => panic!("Expected column reference"),
        }
    }

    #[test]
    fn test_sample_data() {
        let users = data::sample_users();
        assert_eq!(users.len(), 3);

        let products = data::sample_products();
        assert_eq!(products.len(), 3);

        let orders = data::sample_orders();
        assert_eq!(orders.len(), 3);
    }

    #[test]
    fn test_schemas() {
        let users = schemas::users_schema();
        assert_eq!(users.columns().len(), 3);

        let products = schemas::products_schema();
        assert_eq!(products.columns().len(), 3);

        let orders = schemas::orders_schema();
        assert_eq!(orders.columns().len(), 4);
    }
}
