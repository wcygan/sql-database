//! Project operator: selects and reorders columns.

use crate::{ExecutionContext, Executor};
use common::{ColumnId, DbResult, ExecutionStats, Row};
use std::time::Instant;

/// Project operator - selects/reorders columns from input rows.
///
/// Produces rows with a subset of columns in a specified order.
/// Each projection is a (output_name, column_id) pair.
pub struct ProjectExec {
    input: Box<dyn Executor>,
    projections: Vec<(String, ColumnId)>,
    stats: ExecutionStats,
}

impl ProjectExec {
    /// Create a new project operator.
    pub fn new(input: Box<dyn Executor>, projections: Vec<(String, ColumnId)>) -> Self {
        Self {
            input,
            projections,
            stats: ExecutionStats::default(),
        }
    }
}

impl Executor for ProjectExec {
    fn open(&mut self, ctx: &mut ExecutionContext) -> DbResult<()> {
        let start = Instant::now();
        self.stats = ExecutionStats::default();
        self.input.open(ctx)?;
        self.stats.open_time = start.elapsed();
        Ok(())
    }

    fn next(&mut self, ctx: &mut ExecutionContext) -> DbResult<Option<Row>> {
        let start = Instant::now();

        let row = match self.input.next(ctx)? {
            Some(r) => r,
            None => {
                self.stats.total_next_time += start.elapsed();
                return Ok(None);
            }
        };

        let rid = row.rid();

        // Project columns by evaluating each column reference
        let mut projected_values = Vec::with_capacity(self.projections.len());

        for (_name, col_id) in &self.projections {
            let idx = *col_id as usize;
            let value = row
                .values
                .get(idx)
                .ok_or_else(|| {
                    common::DbError::Executor(format!(
                        "column index {} out of bounds (row has {} columns)",
                        idx,
                        row.values.len()
                    ))
                })?
                .clone();
            projected_values.push(value);
        }

        let mut projected = Row::new(projected_values);
        projected.set_rid(rid);

        self.stats.rows_produced += 1;
        self.stats.total_next_time += start.elapsed();
        Ok(Some(projected))
    }

    fn close(&mut self, ctx: &mut ExecutionContext) -> DbResult<()> {
        let start = Instant::now();
        self.input.close(ctx)?;
        self.stats.close_time = start.elapsed();
        Ok(())
    }

    fn schema(&self) -> &[String] {
        // Return just the output names
        static EMPTY: Vec<String> = Vec::new();
        &EMPTY
    }

    fn stats(&self) -> Option<&ExecutionStats> {
        Some(&self.stats)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::helpers::{
        assert_error_contains, assert_exhausted, assert_next_row, setup_test_context, MockExecutor,
    };
    use types::Value;

    #[test]
    fn project_single_column() {
        let rows = vec![
            Row::new(vec![
                Value::Int(1),
                Value::Text("alice".into()),
                Value::Bool(true),
            ]),
            Row::new(vec![
                Value::Int(2),
                Value::Text("bob".into()),
                Value::Bool(false),
            ]),
        ];
        let input = Box::new(MockExecutor::new(
            rows,
            vec!["id".into(), "name".into(), "active".into()],
        ));

        // Project just the name column (index 1)
        let projections = vec![("name".to_string(), 1)];
        let mut project = ProjectExec::new(input, projections);

        let (mut ctx, _temp) = setup_test_context();

        project.open(&mut ctx).unwrap();

        // Should return only name column
        assert_next_row(
            &mut project,
            &mut ctx,
            Row::new(vec![Value::Text("alice".into())]),
        );
        assert_next_row(
            &mut project,
            &mut ctx,
            Row::new(vec![Value::Text("bob".into())]),
        );
        assert_exhausted(&mut project, &mut ctx);

        project.close(&mut ctx).unwrap();
    }

    #[test]
    fn project_multiple_columns() {
        let rows = vec![Row::new(vec![
            Value::Int(1),
            Value::Text("alice".into()),
            Value::Bool(true),
        ])];
        let input = Box::new(MockExecutor::new(
            rows,
            vec!["id".into(), "name".into(), "active".into()],
        ));

        // Project id and active (columns 0 and 2)
        let projections = vec![("id".to_string(), 0), ("active".to_string(), 2)];
        let mut project = ProjectExec::new(input, projections);

        let (mut ctx, _temp) = setup_test_context();

        project.open(&mut ctx).unwrap();
        assert_next_row(
            &mut project,
            &mut ctx,
            Row::new(vec![Value::Int(1), Value::Bool(true)]),
        );
        assert_exhausted(&mut project, &mut ctx);

        project.close(&mut ctx).unwrap();
    }

    #[test]
    fn project_reorder_columns() {
        let rows = vec![Row::new(vec![
            Value::Int(1),
            Value::Text("alice".into()),
            Value::Bool(true),
        ])];
        let input = Box::new(MockExecutor::new(
            rows,
            vec!["id".into(), "name".into(), "active".into()],
        ));

        // Project in reverse order: active, name, id
        let projections = vec![
            ("active".to_string(), 2),
            ("name".to_string(), 1),
            ("id".to_string(), 0),
        ];
        let mut project = ProjectExec::new(input, projections);

        let (mut ctx, _temp) = setup_test_context();

        project.open(&mut ctx).unwrap();
        assert_next_row(
            &mut project,
            &mut ctx,
            Row::new(vec![
                Value::Bool(true),
                Value::Text("alice".into()),
                Value::Int(1),
            ]),
        );
        assert_exhausted(&mut project, &mut ctx);

        project.close(&mut ctx).unwrap();
    }

    #[test]
    fn project_duplicate_column() {
        let rows = vec![Row::new(vec![Value::Int(42), Value::Text("test".into())])];
        let input = Box::new(MockExecutor::new(rows, vec!["id".into(), "name".into()]));

        // Project same column twice
        let projections = vec![("id1".to_string(), 0), ("id2".to_string(), 0)];
        let mut project = ProjectExec::new(input, projections);

        let (mut ctx, _temp) = setup_test_context();

        project.open(&mut ctx).unwrap();
        assert_next_row(
            &mut project,
            &mut ctx,
            Row::new(vec![Value::Int(42), Value::Int(42)]),
        );
        assert_exhausted(&mut project, &mut ctx);

        project.close(&mut ctx).unwrap();
    }

    #[test]
    fn project_empty_input_returns_none() {
        let input = Box::new(MockExecutor::new(vec![], vec![]));
        let projections = vec![("id".to_string(), 0)];
        let mut project = ProjectExec::new(input, projections);

        let (mut ctx, _temp) = setup_test_context();

        project.open(&mut ctx).unwrap();
        assert_exhausted(&mut project, &mut ctx);
        project.close(&mut ctx).unwrap();
    }

    #[test]
    fn project_column_out_of_bounds_returns_error() {
        let rows = vec![Row::new(vec![Value::Int(1), Value::Text("alice".into())])];
        let input = Box::new(MockExecutor::new(rows, vec!["id".into(), "name".into()]));

        // Try to project column 5 which doesn't exist
        let projections = vec![("nonexistent".to_string(), 5)];
        let mut project = ProjectExec::new(input, projections);

        let (mut ctx, _temp) = setup_test_context();

        project.open(&mut ctx).unwrap();
        assert_error_contains(project.next(&mut ctx), "out of bounds");
    }

    #[test]
    fn project_first_column() {
        let rows = vec![Row::new(vec![Value::Int(100), Value::Text("data".into())])];
        let input = Box::new(MockExecutor::new(rows, vec!["id".into(), "name".into()]));

        let projections = vec![("id".to_string(), 0)];
        let mut project = ProjectExec::new(input, projections);

        let (mut ctx, _temp) = setup_test_context();

        project.open(&mut ctx).unwrap();
        assert_next_row(&mut project, &mut ctx, Row::new(vec![Value::Int(100)]));
        assert_exhausted(&mut project, &mut ctx);

        project.close(&mut ctx).unwrap();
    }

    #[test]
    fn project_last_column() {
        let rows = vec![Row::new(vec![
            Value::Int(1),
            Value::Text("alice".into()),
            Value::Bool(true),
        ])];
        let input = Box::new(MockExecutor::new(
            rows,
            vec!["id".into(), "name".into(), "active".into()],
        ));

        let projections = vec![("active".to_string(), 2)];
        let mut project = ProjectExec::new(input, projections);

        let (mut ctx, _temp) = setup_test_context();

        project.open(&mut ctx).unwrap();
        assert_next_row(&mut project, &mut ctx, Row::new(vec![Value::Bool(true)]));
        assert_exhausted(&mut project, &mut ctx);

        project.close(&mut ctx).unwrap();
    }

    #[test]
    fn project_open_delegates_to_input() {
        let rows = vec![Row::new(vec![Value::Int(1)])];
        let input = Box::new(MockExecutor::new(rows, vec!["id".into()]));

        let projections = vec![("id".to_string(), 0)];
        let mut project = ProjectExec::new(input, projections);

        let (mut ctx, _temp) = setup_test_context();

        // Open should succeed
        assert!(project.open(&mut ctx).is_ok());
    }

    #[test]
    fn project_close_delegates_to_input() {
        let rows = vec![Row::new(vec![Value::Int(1)])];
        let input = Box::new(MockExecutor::new(rows, vec!["id".into()]));

        let projections = vec![("id".to_string(), 0)];
        let mut project = ProjectExec::new(input, projections);

        let (mut ctx, _temp) = setup_test_context();

        project.open(&mut ctx).unwrap();
        assert!(project.close(&mut ctx).is_ok());
    }

    #[test]
    fn project_propagates_input_error() {
        let input = Box::new(MockExecutor::with_next_error(common::DbError::Executor(
            "test error".into(),
        )));

        let projections = vec![("id".to_string(), 0)];
        let mut project = ProjectExec::new(input, projections);

        let (mut ctx, _temp) = setup_test_context();

        project.open(&mut ctx).unwrap();
        assert_error_contains(project.next(&mut ctx), "test error");
    }
}
