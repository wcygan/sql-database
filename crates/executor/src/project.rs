//! Project operator: selects and reorders columns.

use crate::{ExecutionContext, Executor};
use common::{ColumnId, DbResult, Row};

/// Project operator - selects/reorders columns from input rows.
///
/// Produces rows with a subset of columns in a specified order.
/// Each projection is a (output_name, column_id) pair.
pub struct ProjectExec {
    input: Box<dyn Executor>,
    projections: Vec<(String, ColumnId)>,
}

impl ProjectExec {
    /// Create a new project operator.
    pub fn new(input: Box<dyn Executor>, projections: Vec<(String, ColumnId)>) -> Self {
        Self { input, projections }
    }
}

impl Executor for ProjectExec {
    fn open(&mut self, ctx: &mut ExecutionContext) -> DbResult<()> {
        self.input.open(ctx)
    }

    fn next(&mut self, ctx: &mut ExecutionContext) -> DbResult<Option<Row>> {
        let row = match self.input.next(ctx)? {
            Some(r) => r,
            None => return Ok(None),
        };

        // Project columns by evaluating each column reference
        let mut projected_values = Vec::with_capacity(self.projections.len());

        for (_name, col_id) in &self.projections {
            let idx = *col_id as usize;
            let value = row
                .0
                .get(idx)
                .ok_or_else(|| {
                    common::DbError::Executor(format!(
                        "column index {} out of bounds (row has {} columns)",
                        idx,
                        row.0.len()
                    ))
                })?
                .clone();
            projected_values.push(value);
        }

        Ok(Some(Row(projected_values)))
    }

    fn close(&mut self, ctx: &mut ExecutionContext) -> DbResult<()> {
        self.input.close(ctx)
    }

    fn schema(&self) -> &[String] {
        // Return just the output names
        static EMPTY: Vec<String> = Vec::new();
        &EMPTY
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::helpers::*;
    use types::Value;

    fn setup_context() -> (ExecutionContext<'static>, tempfile::TempDir) {
        let temp_dir = tempfile::tempdir().unwrap();
        let catalog = create_test_catalog();

        // Leak resources for 'static lifetime (test-only pattern)
        let catalog = Box::leak(Box::new(catalog));
        let mut pager = Box::leak(Box::new(buffer::FilePager::new(temp_dir.path(), 10)));
        let mut wal = Box::leak(Box::new(wal::Wal::open(temp_dir.path().join("test.wal")).unwrap()));

        let ctx = ExecutionContext::new(catalog, pager, wal, temp_dir.path().into());
        (ctx, temp_dir)
    }

    #[test]
    fn project_single_column() {
        let rows = vec![
            Row(vec![Value::Int(1), Value::Text("alice".into()), Value::Bool(true)]),
            Row(vec![Value::Int(2), Value::Text("bob".into()), Value::Bool(false)]),
        ];
        let input = Box::new(MockExecutor::new(rows, vec!["id".into(), "name".into(), "active".into()]));

        // Project just the name column (index 1)
        let projections = vec![("name".to_string(), 1)];
        let mut project = ProjectExec::new(input, projections);

        let (mut ctx, _temp) = setup_context();

        project.open(&mut ctx).unwrap();

        // Should return only name column
        assert_next_row(&mut project, &mut ctx, Row(vec![Value::Text("alice".into())]));
        assert_next_row(&mut project, &mut ctx, Row(vec![Value::Text("bob".into())]));
        assert_exhausted(&mut project, &mut ctx);

        project.close(&mut ctx).unwrap();
    }

    #[test]
    fn project_multiple_columns() {
        let rows = vec![
            Row(vec![Value::Int(1), Value::Text("alice".into()), Value::Bool(true)]),
        ];
        let input = Box::new(MockExecutor::new(rows, vec!["id".into(), "name".into(), "active".into()]));

        // Project id and active (columns 0 and 2)
        let projections = vec![
            ("id".to_string(), 0),
            ("active".to_string(), 2),
        ];
        let mut project = ProjectExec::new(input, projections);

        let (mut ctx, _temp) = setup_context();

        project.open(&mut ctx).unwrap();
        assert_next_row(&mut project, &mut ctx, Row(vec![Value::Int(1), Value::Bool(true)]));
        assert_exhausted(&mut project, &mut ctx);

        project.close(&mut ctx).unwrap();
    }

    #[test]
    fn project_reorder_columns() {
        let rows = vec![
            Row(vec![Value::Int(1), Value::Text("alice".into()), Value::Bool(true)]),
        ];
        let input = Box::new(MockExecutor::new(rows, vec!["id".into(), "name".into(), "active".into()]));

        // Project in reverse order: active, name, id
        let projections = vec![
            ("active".to_string(), 2),
            ("name".to_string(), 1),
            ("id".to_string(), 0),
        ];
        let mut project = ProjectExec::new(input, projections);

        let (mut ctx, _temp) = setup_context();

        project.open(&mut ctx).unwrap();
        assert_next_row(
            &mut project,
            &mut ctx,
            Row(vec![Value::Bool(true), Value::Text("alice".into()), Value::Int(1)]),
        );
        assert_exhausted(&mut project, &mut ctx);

        project.close(&mut ctx).unwrap();
    }

    #[test]
    fn project_duplicate_column() {
        let rows = vec![
            Row(vec![Value::Int(42), Value::Text("test".into())]),
        ];
        let input = Box::new(MockExecutor::new(rows, vec!["id".into(), "name".into()]));

        // Project same column twice
        let projections = vec![
            ("id1".to_string(), 0),
            ("id2".to_string(), 0),
        ];
        let mut project = ProjectExec::new(input, projections);

        let (mut ctx, _temp) = setup_context();

        project.open(&mut ctx).unwrap();
        assert_next_row(&mut project, &mut ctx, Row(vec![Value::Int(42), Value::Int(42)]));
        assert_exhausted(&mut project, &mut ctx);

        project.close(&mut ctx).unwrap();
    }

    #[test]
    fn project_empty_input_returns_none() {
        let input = Box::new(MockExecutor::new(vec![], vec![]));
        let projections = vec![("id".to_string(), 0)];
        let mut project = ProjectExec::new(input, projections);

        let (mut ctx, _temp) = setup_context();

        project.open(&mut ctx).unwrap();
        assert_exhausted(&mut project, &mut ctx);
        project.close(&mut ctx).unwrap();
    }

    #[test]
    fn project_column_out_of_bounds_returns_error() {
        let rows = vec![
            Row(vec![Value::Int(1), Value::Text("alice".into())]),
        ];
        let input = Box::new(MockExecutor::new(rows, vec!["id".into(), "name".into()]));

        // Try to project column 5 which doesn't exist
        let projections = vec![("nonexistent".to_string(), 5)];
        let mut project = ProjectExec::new(input, projections);

        let (mut ctx, _temp) = setup_context();

        project.open(&mut ctx).unwrap();
        assert_error_contains(project.next(&mut ctx), "out of bounds");
    }

    #[test]
    fn project_first_column() {
        let rows = vec![
            Row(vec![Value::Int(100), Value::Text("data".into())]),
        ];
        let input = Box::new(MockExecutor::new(rows, vec!["id".into(), "name".into()]));

        let projections = vec![("id".to_string(), 0)];
        let mut project = ProjectExec::new(input, projections);

        let (mut ctx, _temp) = setup_context();

        project.open(&mut ctx).unwrap();
        assert_next_row(&mut project, &mut ctx, Row(vec![Value::Int(100)]));
        assert_exhausted(&mut project, &mut ctx);

        project.close(&mut ctx).unwrap();
    }

    #[test]
    fn project_last_column() {
        let rows = vec![
            Row(vec![Value::Int(1), Value::Text("alice".into()), Value::Bool(true)]),
        ];
        let input = Box::new(MockExecutor::new(rows, vec!["id".into(), "name".into(), "active".into()]));

        let projections = vec![("active".to_string(), 2)];
        let mut project = ProjectExec::new(input, projections);

        let (mut ctx, _temp) = setup_context();

        project.open(&mut ctx).unwrap();
        assert_next_row(&mut project, &mut ctx, Row(vec![Value::Bool(true)]));
        assert_exhausted(&mut project, &mut ctx);

        project.close(&mut ctx).unwrap();
    }

    #[test]
    fn project_open_delegates_to_input() {
        let rows = vec![Row(vec![Value::Int(1)])];
        let input = Box::new(MockExecutor::new(rows, vec!["id".into()]));

        let projections = vec![("id".to_string(), 0)];
        let mut project = ProjectExec::new(input, projections);

        let (mut ctx, _temp) = setup_context();

        // Open should succeed
        assert!(project.open(&mut ctx).is_ok());
    }

    #[test]
    fn project_close_delegates_to_input() {
        let rows = vec![Row(vec![Value::Int(1)])];
        let input = Box::new(MockExecutor::new(rows, vec!["id".into()]));

        let projections = vec![("id".to_string(), 0)];
        let mut project = ProjectExec::new(input, projections);

        let (mut ctx, _temp) = setup_context();

        project.open(&mut ctx).unwrap();
        assert!(project.close(&mut ctx).is_ok());
    }

    #[test]
    fn project_propagates_input_error() {
        let input = Box::new(MockExecutor::with_next_error(
            common::DbError::Executor("test error".into())
        ));

        let projections = vec![("id".to_string(), 0)];
        let mut project = ProjectExec::new(input, projections);

        let (mut ctx, _temp) = setup_context();

        project.open(&mut ctx).unwrap();
        assert_error_contains(project.next(&mut ctx), "test error");
    }
}
