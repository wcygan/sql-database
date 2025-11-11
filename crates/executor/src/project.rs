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
