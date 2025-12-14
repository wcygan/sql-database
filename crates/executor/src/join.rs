//! Join operators: combines rows from multiple tables.

use crate::filter::eval_resolved_expr;
use crate::{ExecutionContext, Executor};
use common::{DbResult, ExecutionStats, Row};
use planner::ResolvedExpr;
use std::time::Instant;
use types::Value;

/// Nested loop join operator - simple O(n*m) join algorithm.
///
/// For each row from the left table, iterates all rows from the right table,
/// evaluating the join condition on combined rows.
///
/// # Algorithm
///
/// 1. `open()`: Materialize all right-side rows into memory, fetch first left row.
/// 2. `next()`: For each left row, iterate through all right rows, evaluate condition.
/// 3. When all right rows exhausted for current left, advance to next left row.
/// 4. `close()`: Release materialized rows and close children.
///
/// # Performance
///
/// - Time: O(n * m) where n = left rows, m = right rows
/// - Space: O(m) to materialize right side
///
/// This is the simplest join algorithm, suitable for small tables or when no
/// better access method is available. More sophisticated algorithms (HashJoin,
/// MergeJoin) would be used for larger datasets.
pub struct NestedLoopJoinExec {
    left_input: Box<dyn Executor>,
    right_input: Box<dyn Executor>,
    condition: ResolvedExpr,
    schema: Vec<String>,

    // State
    current_left_row: Option<Row>,
    right_materialized: Vec<Row>,
    right_cursor: usize,
    stats: ExecutionStats,
}

impl NestedLoopJoinExec {
    /// Create a new nested loop join operator.
    ///
    /// # Arguments
    ///
    /// * `left` - Left (outer) input executor
    /// * `right` - Right (inner) input executor, will be materialized
    /// * `condition` - Join condition (ON clause) with resolved column ordinals
    /// * `schema` - Combined output schema (left columns followed by right columns)
    pub fn new(
        left: Box<dyn Executor>,
        right: Box<dyn Executor>,
        condition: ResolvedExpr,
        schema: Vec<String>,
    ) -> Self {
        Self {
            left_input: left,
            right_input: right,
            condition,
            schema,
            current_left_row: None,
            right_materialized: Vec::new(),
            right_cursor: 0,
            stats: ExecutionStats::default(),
        }
    }

    /// Combine a left and right row into a single row.
    ///
    /// The combined row has all columns from the left row first,
    /// followed by all columns from the right row.
    fn combine_rows(&self, left: &Row, right: &Row) -> Row {
        let mut combined_values = left.values.clone();
        combined_values.extend(right.values.clone());
        Row::new(combined_values)
    }

    /// Evaluate the join condition against a combined row.
    ///
    /// Returns true if the rows should be joined, false otherwise.
    /// NULL condition results are treated as false (SQL semantics).
    fn eval_condition(&self, row: &Row) -> DbResult<bool> {
        let result = eval_resolved_expr(&self.condition, row)?;
        match result {
            Value::Bool(b) => Ok(b),
            Value::Null => Ok(false),
            other => Err(common::DbError::Executor(format!(
                "join condition must evaluate to boolean, got {:?}",
                other
            ))),
        }
    }
}

impl Executor for NestedLoopJoinExec {
    fn open(&mut self, ctx: &mut ExecutionContext) -> DbResult<()> {
        let start = Instant::now();
        self.stats = ExecutionStats::default();

        // Open both children
        self.left_input.open(ctx)?;
        self.right_input.open(ctx)?;

        // Materialize right side for repeated iteration
        self.right_materialized.clear();
        while let Some(row) = self.right_input.next(ctx)? {
            self.right_materialized.push(row);
        }

        // Get first left row
        self.current_left_row = self.left_input.next(ctx)?;
        self.right_cursor = 0;

        self.stats.open_time = start.elapsed();
        Ok(())
    }

    fn next(&mut self, ctx: &mut ExecutionContext) -> DbResult<Option<Row>> {
        let start = Instant::now();

        loop {
            // Check if we have a current left row
            let left_row = match &self.current_left_row {
                Some(r) => r.clone(),
                None => {
                    self.stats.total_next_time += start.elapsed();
                    return Ok(None);
                }
            };

            // Try to find matching right row
            while self.right_cursor < self.right_materialized.len() {
                let right_row = &self.right_materialized[self.right_cursor];
                self.right_cursor += 1;

                // Combine rows and evaluate join condition
                let combined = self.combine_rows(&left_row, right_row);

                if self.eval_condition(&combined)? {
                    self.stats.rows_produced += 1;
                    self.stats.total_next_time += start.elapsed();
                    return Ok(Some(combined));
                }
            }

            // Exhausted right side for current left row, advance left
            self.current_left_row = self.left_input.next(ctx)?;
            self.right_cursor = 0;
        }
    }

    fn close(&mut self, ctx: &mut ExecutionContext) -> DbResult<()> {
        let start = Instant::now();

        self.right_materialized.clear();
        self.current_left_row = None;
        self.left_input.close(ctx)?;
        self.right_input.close(ctx)?;

        self.stats.close_time = start.elapsed();
        Ok(())
    }

    fn schema(&self) -> &[String] {
        &self.schema
    }

    fn stats(&self) -> Option<&ExecutionStats> {
        Some(&self.stats)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::helpers::{
        assert_exhausted, assert_next_row, create_test_catalog, MockExecutor,
    };
    use expr::BinaryOp;
    use testsupport::prelude::*;

    #[test]
    fn join_empty_left_returns_none() {
        let left = Box::new(MockExecutor::new(vec![], vec!["id".into()]));
        let right = Box::new(MockExecutor::new(vec![int_row(&[1])], vec!["value".into()]));
        let condition = lit!(bool: true); // Always true
        let schema = vec!["left.id".into(), "right.value".into()];

        let mut join = NestedLoopJoinExec::new(left, right, condition, schema);

        let temp_dir = tempfile::tempdir().unwrap();
        let catalog = create_test_catalog();
        let mut pager = buffer::FilePager::new(temp_dir.path(), 10);
        let mut wal = wal::Wal::open(temp_dir.path().join("test.wal")).unwrap();
        let mut ctx = ExecutionContext::new(&catalog, &mut pager, &mut wal, temp_dir.path().into());

        join.open(&mut ctx).unwrap();
        assert_exhausted(&mut join, &mut ctx);
        join.close(&mut ctx).unwrap();
    }

    #[test]
    fn join_empty_right_returns_none() {
        let left = Box::new(MockExecutor::new(vec![int_row(&[1])], vec!["id".into()]));
        let right = Box::new(MockExecutor::new(vec![], vec!["value".into()]));
        let condition = lit!(bool: true);
        let schema = vec!["left.id".into(), "right.value".into()];

        let mut join = NestedLoopJoinExec::new(left, right, condition, schema);

        let temp_dir = tempfile::tempdir().unwrap();
        let catalog = create_test_catalog();
        let mut pager = buffer::FilePager::new(temp_dir.path(), 10);
        let mut wal = wal::Wal::open(temp_dir.path().join("test.wal")).unwrap();
        let mut ctx = ExecutionContext::new(&catalog, &mut pager, &mut wal, temp_dir.path().into());

        join.open(&mut ctx).unwrap();
        assert_exhausted(&mut join, &mut ctx);
        join.close(&mut ctx).unwrap();
    }

    #[test]
    fn join_cross_product_with_always_true() {
        // Cross product: all combinations
        let left = Box::new(MockExecutor::new(
            vec![int_row(&[1]), int_row(&[2])],
            vec!["a".into()],
        ));
        let right = Box::new(MockExecutor::new(
            vec![int_row(&[10]), int_row(&[20])],
            vec!["b".into()],
        ));
        let condition = lit!(bool: true);
        let schema = vec!["left.a".into(), "right.b".into()];

        let mut join = NestedLoopJoinExec::new(left, right, condition, schema);

        let temp_dir = tempfile::tempdir().unwrap();
        let catalog = create_test_catalog();
        let mut pager = buffer::FilePager::new(temp_dir.path(), 10);
        let mut wal = wal::Wal::open(temp_dir.path().join("test.wal")).unwrap();
        let mut ctx = ExecutionContext::new(&catalog, &mut pager, &mut wal, temp_dir.path().into());

        join.open(&mut ctx).unwrap();

        // 2 left rows x 2 right rows = 4 combinations
        assert_next_row(&mut join, &mut ctx, int_row(&[1, 10]));
        assert_next_row(&mut join, &mut ctx, int_row(&[1, 20]));
        assert_next_row(&mut join, &mut ctx, int_row(&[2, 10]));
        assert_next_row(&mut join, &mut ctx, int_row(&[2, 20]));
        assert_exhausted(&mut join, &mut ctx);

        join.close(&mut ctx).unwrap();
    }

    #[test]
    fn join_with_equality_condition() {
        // SELECT * FROM users u JOIN orders o ON u.id = o.user_id
        let left = Box::new(MockExecutor::new(
            vec![int_row(&[1, 100]), int_row(&[2, 200])],
            vec!["id".into(), "value".into()],
        ));
        let right = Box::new(MockExecutor::new(
            vec![
                int_row(&[101, 1]), // matches user 1
                int_row(&[102, 2]), // matches user 2
                int_row(&[103, 1]), // matches user 1
            ],
            vec!["order_id".into(), "user_id".into()],
        ));

        // ON left.id = right.user_id (column 0 = column 3)
        let condition = binary(col(0), BinaryOp::Eq, col(3));
        let schema = vec![
            "u.id".into(),
            "u.value".into(),
            "o.order_id".into(),
            "o.user_id".into(),
        ];

        let mut join = NestedLoopJoinExec::new(left, right, condition, schema);

        let temp_dir = tempfile::tempdir().unwrap();
        let catalog = create_test_catalog();
        let mut pager = buffer::FilePager::new(temp_dir.path(), 10);
        let mut wal = wal::Wal::open(temp_dir.path().join("test.wal")).unwrap();
        let mut ctx = ExecutionContext::new(&catalog, &mut pager, &mut wal, temp_dir.path().into());

        join.open(&mut ctx).unwrap();

        // User 1 matches orders 101 and 103, user 2 matches order 102
        assert_next_row(&mut join, &mut ctx, int_row(&[1, 100, 101, 1]));
        assert_next_row(&mut join, &mut ctx, int_row(&[1, 100, 103, 1]));
        assert_next_row(&mut join, &mut ctx, int_row(&[2, 200, 102, 2]));
        assert_exhausted(&mut join, &mut ctx);

        join.close(&mut ctx).unwrap();
    }

    #[test]
    fn join_no_matches_returns_none() {
        let left = Box::new(MockExecutor::new(vec![int_row(&[1])], vec!["a".into()]));
        let right = Box::new(MockExecutor::new(vec![int_row(&[2])], vec!["b".into()]));

        // ON left.a = right.b (will never match)
        let condition = binary(col(0), BinaryOp::Eq, col(1));
        let schema = vec!["left.a".into(), "right.b".into()];

        let mut join = NestedLoopJoinExec::new(left, right, condition, schema);

        let temp_dir = tempfile::tempdir().unwrap();
        let catalog = create_test_catalog();
        let mut pager = buffer::FilePager::new(temp_dir.path(), 10);
        let mut wal = wal::Wal::open(temp_dir.path().join("test.wal")).unwrap();
        let mut ctx = ExecutionContext::new(&catalog, &mut pager, &mut wal, temp_dir.path().into());

        join.open(&mut ctx).unwrap();
        assert_exhausted(&mut join, &mut ctx);
        join.close(&mut ctx).unwrap();
    }

    #[test]
    fn join_schema_is_combined() {
        let left = Box::new(MockExecutor::new(vec![], vec!["a".into(), "b".into()]));
        let right = Box::new(MockExecutor::new(vec![], vec!["c".into(), "d".into()]));
        let condition = lit!(bool: true);
        let schema = vec!["l.a".into(), "l.b".into(), "r.c".into(), "r.d".into()];

        let join = NestedLoopJoinExec::new(left, right, condition, schema);

        assert_eq!(
            join.schema(),
            &["l.a".to_string(), "l.b".to_string(), "r.c".to_string(), "r.d".to_string()]
        );
    }
}
