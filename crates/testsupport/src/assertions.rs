//! Custom assertion helpers for testing.
//!
//! Provides specialized assertion functions and macros for common
//! database testing patterns.

use common::{DbError, DbResult, Row};
use executor::Executor;
use types::Value;

/// Assert that an executor returns a specific row next.
///
/// This is useful for testing query execution and verifying results
/// in a specific order.
///
/// # Example
///
/// ```no_run
/// use testsupport::prelude::*;
/// use executor::Executor;
/// use types::Value;
///
/// # fn example(mut exec: impl Executor, mut ctx: executor::ExecutionContext) {
/// assert_next_row(&mut exec, &mut ctx, &[Value::Int(1), Value::Text("Alice".into())]);
/// # }
/// ```
pub fn assert_next_row<E: Executor + ?Sized>(
    executor: &mut E,
    ctx: &mut executor::ExecutionContext,
    expected: &[Value],
) {
    let row = executor
        .next(ctx)
        .expect("executor next failed")
        .expect("expected row but got None");
    assert_eq!(
        &row.values, expected,
        "Row mismatch: expected {:?}, got {:?}",
        expected, row.values
    );
}

/// Assert that an executor is exhausted (returns None).
///
/// This is useful for verifying that a query has returned all expected rows.
///
/// # Example
///
/// ```no_run
/// use testsupport::prelude::*;
/// use executor::Executor;
///
/// # fn example(mut exec: impl Executor, mut ctx: executor::ExecutionContext) {
/// assert_exhausted(&mut exec, &mut ctx);
/// # }
/// ```
pub fn assert_exhausted<E: Executor + ?Sized>(
    executor: &mut E,
    ctx: &mut executor::ExecutionContext,
) {
    let result = executor.next(ctx).expect("executor next failed");
    assert!(
        result.is_none(),
        "Expected executor to be exhausted, but got row: {:?}",
        result
    );
}

/// Assert that an operation returns an error containing a specific substring.
///
/// This is useful for testing error handling and validation logic.
///
/// # Example
///
/// ```no_run
/// use testsupport::prelude::*;
///
/// let result: Result<(), common::DbError> = Err(common::DbError::Executor("table not found".into()));
/// assert_error_contains(result, "table not found");
/// ```
pub fn assert_error_contains<T>(result: DbResult<T>, expected_msg: &str) {
    match result {
        Ok(_) => panic!("Expected error containing '{}', but got Ok", expected_msg),
        Err(e) => {
            let error_string = e.to_string();
            assert!(
                error_string.contains(expected_msg),
                "Expected error to contain '{}', but got: {}",
                expected_msg,
                error_string
            );
        }
    }
}

/// Assert that an operation returns a specific executor error.
///
/// # Example
///
/// ```no_run
/// use testsupport::prelude::*;
/// use common::DbError;
///
/// let result: Result<(), DbError> = Err(DbError::Executor("invalid operation".into()));
/// assert_executor_error(result, "invalid operation");
/// ```
pub fn assert_executor_error<T>(result: DbResult<T>, expected_msg: &str) {
    match result {
        Ok(_) => panic!(
            "Expected executor error '{}', but got Ok",
            expected_msg
        ),
        Err(DbError::Executor(msg)) => {
            assert!(
                msg.contains(expected_msg),
                "Expected executor error to contain '{}', but got: {}",
                expected_msg,
                msg
            );
        }
        Err(other) => panic!(
            "Expected executor error '{}', but got different error: {}",
            expected_msg, other
        ),
    }
}

/// Assert that two rows are equal.
///
/// # Example
///
/// ```no_run
/// use testsupport::prelude::*;
/// use types::Value;
///
/// let row1 = int_row(&[1, 2, 3]);
/// let row2 = int_row(&[1, 2, 3]);
/// assert_rows_equal(&row1, &row2);
/// ```
pub fn assert_rows_equal(actual: &Row, expected: &Row) {
    assert_eq!(
        actual.values, expected.values,
        "Rows not equal:\nExpected: {:?}\nActual:   {:?}",
        expected.values, actual.values
    );
}

/// Assert that two vectors of rows are equal.
///
/// This compares both length and contents of the row vectors.
///
/// # Example
///
/// ```no_run
/// use testsupport::prelude::*;
///
/// let rows1 = vec![int_row(&[1, 2]), int_row(&[3, 4])];
/// let rows2 = vec![int_row(&[1, 2]), int_row(&[3, 4])];
/// assert_row_sets_equal(&rows1, &rows2);
/// ```
pub fn assert_row_sets_equal(actual: &[Row], expected: &[Row]) {
    assert_eq!(
        actual.len(),
        expected.len(),
        "Row count mismatch: expected {} rows, got {}",
        expected.len(),
        actual.len()
    );

    for (i, (actual_row, expected_row)) in actual.iter().zip(expected.iter()).enumerate() {
        assert_eq!(
            actual_row.values, expected_row.values,
            "Row {} mismatch:\nExpected: {:?}\nActual:   {:?}",
            i, expected_row.values, actual_row.values
        );
    }
}

/// Assert that a value matches an expected value with a custom error message.
///
/// # Example
///
/// ```no_run
/// use testsupport::prelude::*;
/// use types::Value;
///
/// let value = Value::Int(42);
/// assert_value_eq(&value, &Value::Int(42), "answer to life");
/// ```
pub fn assert_value_eq(actual: &Value, expected: &Value, context: &str) {
    assert_eq!(
        actual, expected,
        "{}: expected {:?}, got {:?}",
        context, expected, actual
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fixtures::*;

    #[test]
    fn test_assert_rows_equal() {
        let row1 = int_row(&[1, 2, 3]);
        let row2 = int_row(&[1, 2, 3]);
        assert_rows_equal(&row1, &row2);
    }

    #[test]
    #[should_panic(expected = "Rows not equal")]
    fn test_assert_rows_equal_fails() {
        let row1 = int_row(&[1, 2, 3]);
        let row2 = int_row(&[1, 2, 4]);
        assert_rows_equal(&row1, &row2);
    }

    #[test]
    fn test_assert_row_sets_equal() {
        let rows1 = vec![int_row(&[1, 2]), int_row(&[3, 4])];
        let rows2 = vec![int_row(&[1, 2]), int_row(&[3, 4])];
        assert_row_sets_equal(&rows1, &rows2);
    }

    #[test]
    #[should_panic(expected = "Row count mismatch")]
    fn test_assert_row_sets_equal_different_length() {
        let rows1 = vec![int_row(&[1, 2])];
        let rows2 = vec![int_row(&[1, 2]), int_row(&[3, 4])];
        assert_row_sets_equal(&rows1, &rows2);
    }

    #[test]
    fn test_assert_error_contains() {
        let result: DbResult<()> = Err(DbError::Executor("table not found".into()));
        assert_error_contains(result, "table not found");
    }

    #[test]
    #[should_panic(expected = "but got Ok")]
    fn test_assert_error_contains_no_error() {
        let result: DbResult<()> = Ok(());
        assert_error_contains(result, "some error");
    }

    #[test]
    fn test_assert_executor_error() {
        let result: DbResult<()> = Err(DbError::Executor("invalid operation".into()));
        assert_executor_error(result, "invalid operation");
    }

    #[test]
    fn test_assert_value_eq() {
        let value = Value::Int(42);
        assert_value_eq(&value, &Value::Int(42), "test context");
    }

    #[test]
    #[should_panic(expected = "test context")]
    fn test_assert_value_eq_fails() {
        let value = Value::Int(42);
        assert_value_eq(&value, &Value::Int(43), "test context");
    }
}
