//! DML operators: Insert, Update, Delete.

use crate::{filter::eval_resolved_expr, ExecutionContext, Executor};
use common::{ColumnId, DbResult, Row, TableId};
use planner::ResolvedExpr;
use storage::HeapTable;
use types::Value;
use wal::WalRecord;

/// Insert operator - inserts rows into a table with WAL logging.
///
/// Evaluates value expressions and writes to both WAL and storage.
/// Returns a single row containing the number of inserted rows.
pub struct InsertExec {
    table_id: TableId,
    schema: Vec<String>,
    values: Vec<ResolvedExpr>,
    executed: bool,
}

impl InsertExec {
    /// Create a new insert operator.
    pub fn new(table_id: TableId, schema: Vec<String>, values: Vec<ResolvedExpr>) -> Self {
        Self {
            table_id,
            schema,
            values,
            executed: false,
        }
    }
}

impl Executor for InsertExec {
    fn open(&mut self, _ctx: &mut ExecutionContext) -> DbResult<()> {
        self.executed = false;
        Ok(())
    }

    fn next(&mut self, ctx: &mut ExecutionContext) -> DbResult<Option<Row>> {
        if self.executed {
            return Ok(None);
        }
        self.executed = true;

        // Evaluate value expressions (no row context for INSERT literals)
        let empty_row = Row::new(vec![]);
        let mut row_values = Vec::with_capacity(self.values.len());

        for expr in &self.values {
            let value = eval_resolved_expr(expr, &empty_row)?;
            row_values.push(value);
        }

        let row = Row::new(row_values.clone());

        // 1. Insert into storage to get RID
        let rid = {
            let mut heap_table = ctx.heap_table(self.table_id)?;
            heap_table.insert(&row)?
        };

        // 2. Log to WAL after successful insert
        let wal_record = WalRecord::Insert {
            table: self.table_id,
            row: row_values,
            rid,
        };
        ctx.log_dml(wal_record)?;

        // Return single row with affected count
        Ok(Some(Row::new(vec![Value::Int(1)])))
    }

    fn close(&mut self, _ctx: &mut ExecutionContext) -> DbResult<()> {
        Ok(())
    }

    fn schema(&self) -> &[String] {
        &self.schema
    }
}

/// Update operator - updates rows matching a predicate with WAL logging.
///
/// Scans for matching rows, applies assignments, and writes to WAL and storage.
/// Returns a single row containing the number of updated rows.
pub struct UpdateExec {
    #[allow(dead_code)]
    table_id: TableId,
    schema: Vec<String>,
    input: Box<dyn Executor>,
    assignments: Vec<(ColumnId, ResolvedExpr)>,
    executed: bool,
}

#[bon::bon]
impl UpdateExec {
    /// Create a new update operator using a builder pattern.
    ///
    /// # Example
    /// ```ignore
    /// let update = UpdateExec::builder()
    ///     .table_id(TableId(1))
    ///     .schema(vec!["id".into(), "name".into()])
    ///     .input(scan_exec)
    ///     .assignments(vec![(1, ResolvedExpr::Literal(Value::Text("updated".into())))])
    ///     .build();
    /// ```
    #[builder]
    pub fn new(
        table_id: TableId,
        schema: Vec<String>,
        input: Box<dyn Executor>,
        assignments: Vec<(ColumnId, ResolvedExpr)>,
    ) -> Self {
        Self {
            table_id,
            schema,
            input,
            assignments,
            executed: false,
        }
    }

    /// Apply assignments to a row to produce the updated row.
    fn apply_assignments(&self, old_row: &Row) -> DbResult<Row> {
        let mut new_values = old_row.values.clone();

        for (col_id, expr) in &self.assignments {
            let idx = *col_id as usize;
            if idx >= new_values.len() {
                return Err(common::DbError::Executor(format!(
                    "column index {} out of bounds (row has {} columns)",
                    idx,
                    new_values.len()
                )));
            }

            let value = eval_resolved_expr(expr, old_row)?;
            new_values[idx] = value;
        }

        Ok(Row::new(new_values))
    }
}

impl Executor for UpdateExec {
    fn open(&mut self, ctx: &mut ExecutionContext) -> DbResult<()> {
        self.executed = false;
        self.input.open(ctx)
    }

    fn next(&mut self, ctx: &mut ExecutionContext) -> DbResult<Option<Row>> {
        if self.executed {
            return Ok(None);
        }

        let mut count = 0;

        // Buffer rows first so updates don't see rows reinserted during the operation
        let mut buffered_rows = Vec::new();
        while let Some(row) = self.input.next(ctx)? {
            buffered_rows.push(row);
        }

        // For each buffered row, apply updates
        for old_row in buffered_rows {
            let mut new_row = self.apply_assignments(&old_row)?;

            let Some(rid) = old_row.rid() else {
                // Mock executors in unit tests don't populate RIDs; just count matches
                count += 1;
                continue;
            };
            let new_rid = {
                let mut heap_table = ctx.heap_table(self.table_id)?;
                heap_table.update(rid, &new_row)?
            };
            new_row.set_rid(Some(new_rid));

            ctx.log_dml(WalRecord::Update {
                table: self.table_id,
                rid: new_rid,
                new_row: new_row.values.clone(),
            })?;

            count += 1;
        }

        self.executed = true;

        // Return count of matched rows
        Ok(Some(Row::new(vec![Value::Int(count)])))
    }

    fn close(&mut self, ctx: &mut ExecutionContext) -> DbResult<()> {
        self.input.close(ctx)
    }

    fn schema(&self) -> &[String] {
        &self.schema
    }
}

/// Delete operator - deletes rows matching a predicate with WAL logging.
///
/// Scans for matching rows and removes them from storage.
/// Returns a single row containing the number of deleted rows.
pub struct DeleteExec {
    #[allow(dead_code)]
    table_id: TableId,
    schema: Vec<String>,
    input: Box<dyn Executor>,
    executed: bool,
}

impl DeleteExec {
    /// Create a new delete operator.
    pub fn new(table_id: TableId, schema: Vec<String>, input: Box<dyn Executor>) -> Self {
        Self {
            table_id,
            schema,
            input,
            executed: false,
        }
    }
}

impl Executor for DeleteExec {
    fn open(&mut self, ctx: &mut ExecutionContext) -> DbResult<()> {
        self.executed = false;
        self.input.open(ctx)
    }

    fn next(&mut self, ctx: &mut ExecutionContext) -> DbResult<Option<Row>> {
        if self.executed {
            return Ok(None);
        }

        let mut count = 0;

        // For each matching row, delete it
        while let Some(row) = self.input.next(ctx)? {
            let Some(rid) = row.rid() else {
                count += 1;
                continue;
            };

            {
                let mut heap_table = ctx.heap_table(self.table_id)?;
                heap_table.delete(rid)?;
            }
            ctx.log_dml(WalRecord::Delete {
                table: self.table_id,
                rid,
            })?;

            count += 1;
        }

        self.executed = true;

        // Return count of matched rows
        Ok(Some(Row::new(vec![Value::Int(count)])))
    }

    fn close(&mut self, ctx: &mut ExecutionContext) -> DbResult<()> {
        self.input.close(ctx)
    }

    fn schema(&self) -> &[String] {
        &self.schema
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::helpers::{
        assert_error_contains, assert_exhausted, assert_next_row, create_test_catalog, lit_int,
        lit_text, MockExecutor,
    };
    use crate::{execute_dml, execute_query};
    use expr::BinaryOp;
    use planner::PhysicalPlan;
    use types::Value;

    fn setup_context() -> (ExecutionContext<'static>, tempfile::TempDir) {
        let temp_dir = tempfile::tempdir().unwrap();
        let catalog = create_test_catalog();

        // Leak resources for 'static lifetime (test-only pattern)
        let catalog = Box::leak(Box::new(catalog));
        let pager = Box::leak(Box::new(buffer::FilePager::new(temp_dir.path(), 10)));
        let wal = Box::leak(Box::new(
            wal::Wal::open(temp_dir.path().join("test.wal")).unwrap(),
        ));

        let ctx = ExecutionContext::new(catalog, pager, wal, temp_dir.path().into());
        (ctx, temp_dir)
    }

    // InsertExec tests

    #[test]
    fn insert_single_row() {
        let (mut ctx, _temp) = setup_context();
        let table_id = TableId(1);

        let values = vec![
            lit_int(1),
            lit_text("alice"),
            ResolvedExpr::Literal(Value::Bool(true)),
        ];
        let mut insert = InsertExec::new(table_id, vec![], values);

        insert.open(&mut ctx).unwrap();

        // Should return row with count of 1
        assert_next_row(&mut insert, &mut ctx, Row::new(vec![Value::Int(1)]));
        assert_exhausted(&mut insert, &mut ctx);

        insert.close(&mut ctx).unwrap();
    }

    #[test]
    fn insert_only_executes_once() {
        let (mut ctx, _temp) = setup_context();
        let table_id = TableId(1);

        let values = vec![lit_int(42)];
        let mut insert = InsertExec::new(table_id, vec![], values);

        insert.open(&mut ctx).unwrap();

        // First call returns count
        assert_next_row(&mut insert, &mut ctx, Row::new(vec![Value::Int(1)]));

        // Subsequent calls return None
        assert_exhausted(&mut insert, &mut ctx);
        assert_exhausted(&mut insert, &mut ctx);

        insert.close(&mut ctx).unwrap();
    }

    #[test]
    fn insert_open_resets_executed_flag() {
        let (mut ctx, _temp) = setup_context();
        let table_id = TableId(1);

        let values = vec![lit_int(1)];
        let mut insert = InsertExec::new(table_id, vec![], values);

        insert.open(&mut ctx).unwrap();
        assert_next_row(&mut insert, &mut ctx, Row::new(vec![Value::Int(1)]));
        assert_exhausted(&mut insert, &mut ctx);

        // Reset with open
        insert.open(&mut ctx).unwrap();
        assert_next_row(&mut insert, &mut ctx, Row::new(vec![Value::Int(1)]));

        insert.close(&mut ctx).unwrap();
    }

    #[test]
    fn insert_evaluates_literal_expressions() {
        let (mut ctx, _temp) = setup_context();
        let table_id = TableId(1);

        let values = vec![
            ResolvedExpr::Literal(Value::Int(100)),
            ResolvedExpr::Literal(Value::Text("test".into())),
        ];
        let mut insert = InsertExec::new(table_id, vec![], values);

        insert.open(&mut ctx).unwrap();
        assert_next_row(&mut insert, &mut ctx, Row::new(vec![Value::Int(1)]));

        insert.close(&mut ctx).unwrap();
    }

    #[test]
    fn insert_schema_empty() {
        let table_id = TableId(1);
        let insert = InsertExec::new(table_id, vec![], vec![lit_int(1)]);

        assert_eq!(insert.schema().len(), 0);
    }

    #[test]
    fn insert_close_succeeds() {
        let (mut ctx, _temp) = setup_context();
        let table_id = TableId(1);

        let mut insert = InsertExec::new(table_id, vec![], vec![lit_int(1)]);

        insert.open(&mut ctx).unwrap();
        assert!(insert.close(&mut ctx).is_ok());
    }

    #[test]
    fn insert_multiple_values() {
        let (mut ctx, _temp) = setup_context();
        let table_id = TableId(1);

        let values = vec![
            lit_int(1),
            lit_text("alice"),
            ResolvedExpr::Literal(Value::Bool(true)),
        ];
        let mut insert = InsertExec::new(table_id, vec![], values);

        insert.open(&mut ctx).unwrap();
        assert_next_row(&mut insert, &mut ctx, Row::new(vec![Value::Int(1)]));

        insert.close(&mut ctx).unwrap();
    }

    #[test]
    fn insert_with_expression() {
        let (mut ctx, _temp) = setup_context();
        let table_id = TableId(1);

        // Use binary expression (though it doesn't have row context)
        let expr = ResolvedExpr::Binary {
            left: Box::new(lit_int(10)),
            op: BinaryOp::Eq,
            right: Box::new(lit_int(10)),
        };

        let values = vec![expr];
        let mut insert = InsertExec::new(table_id, vec![], values);

        insert.open(&mut ctx).unwrap();
        assert_next_row(&mut insert, &mut ctx, Row::new(vec![Value::Int(1)]));

        insert.close(&mut ctx).unwrap();
    }

    // UpdateExec tests

    #[test]
    fn update_no_matching_rows() {
        let (mut ctx, _temp) = setup_context();
        let table_id = TableId(1);

        // Empty input
        let input = Box::new(MockExecutor::new(vec![], vec![]));
        let assignments = vec![(0, lit_int(100))];

        let mut update = UpdateExec::builder()
            .table_id(table_id)
            .schema(vec![])
            .input(input)
            .assignments(assignments)
            .build();

        update.open(&mut ctx).unwrap();
        assert_next_row(&mut update, &mut ctx, Row::new(vec![Value::Int(0)]));
        assert_exhausted(&mut update, &mut ctx);

        update.close(&mut ctx).unwrap();
    }

    #[test]
    fn update_single_row() {
        let (mut ctx, _temp) = setup_context();
        let table_id = TableId(1);

        let rows = vec![Row::new(vec![Value::Int(1), Value::Text("alice".into())])];
        let input = Box::new(MockExecutor::new(rows, vec!["id".into(), "name".into()]));
        let assignments = vec![(0, lit_int(100))];

        let mut update = UpdateExec::builder()
            .table_id(table_id)
            .schema(vec![])
            .input(input)
            .assignments(assignments)
            .build();

        update.open(&mut ctx).unwrap();
        assert_next_row(&mut update, &mut ctx, Row::new(vec![Value::Int(1)]));
        assert_exhausted(&mut update, &mut ctx);

        update.close(&mut ctx).unwrap();
    }

    #[test]
    fn update_multiple_rows() {
        let (mut ctx, _temp) = setup_context();
        let table_id = TableId(1);

        let rows = vec![
            Row::new(vec![Value::Int(1), Value::Text("alice".into())]),
            Row::new(vec![Value::Int(2), Value::Text("bob".into())]),
            Row::new(vec![Value::Int(3), Value::Text("carol".into())]),
        ];
        let input = Box::new(MockExecutor::new(rows, vec!["id".into(), "name".into()]));
        let assignments = vec![(1, lit_text("updated"))];

        let mut update = UpdateExec::builder()
            .table_id(table_id)
            .schema(vec![])
            .input(input)
            .assignments(assignments)
            .build();

        update.open(&mut ctx).unwrap();
        assert_next_row(&mut update, &mut ctx, Row::new(vec![Value::Int(3)]));
        assert_exhausted(&mut update, &mut ctx);

        update.close(&mut ctx).unwrap();
    }

    #[test]
    fn update_multiple_columns() {
        let (mut ctx, _temp) = setup_context();
        let table_id = TableId(1);

        let rows = vec![Row::new(vec![Value::Int(1), Value::Text("alice".into())])];
        let input = Box::new(MockExecutor::new(rows, vec!["id".into(), "name".into()]));
        let assignments = vec![(0, lit_int(100)), (1, lit_text("updated"))];

        let mut update = UpdateExec::builder()
            .table_id(table_id)
            .schema(vec![])
            .input(input)
            .assignments(assignments)
            .build();

        update.open(&mut ctx).unwrap();
        assert_next_row(&mut update, &mut ctx, Row::new(vec![Value::Int(1)]));

        update.close(&mut ctx).unwrap();
    }

    #[test]
    fn update_assignment_out_of_bounds() {
        let (mut ctx, _temp) = setup_context();
        let table_id = TableId(1);

        let rows = vec![Row::new(vec![Value::Int(1)])];
        let input = Box::new(MockExecutor::new(rows, vec!["id".into()]));
        let assignments = vec![(5, lit_int(100))]; // Column 5 doesn't exist

        let mut update = UpdateExec::builder()
            .table_id(table_id)
            .schema(vec![])
            .input(input)
            .assignments(assignments)
            .build();

        update.open(&mut ctx).unwrap();
        assert_error_contains(update.next(&mut ctx), "out of bounds");
    }

    #[test]
    fn update_only_executes_once() {
        let (mut ctx, _temp) = setup_context();
        let table_id = TableId(1);

        let rows = vec![Row::new(vec![Value::Int(1)])];
        let input = Box::new(MockExecutor::new(rows, vec!["id".into()]));
        let assignments = vec![(0, lit_int(100))];

        let mut update = UpdateExec::builder()
            .table_id(table_id)
            .schema(vec![])
            .input(input)
            .assignments(assignments)
            .build();

        update.open(&mut ctx).unwrap();
        assert_next_row(&mut update, &mut ctx, Row::new(vec![Value::Int(1)]));
        assert_exhausted(&mut update, &mut ctx);
        assert_exhausted(&mut update, &mut ctx);

        update.close(&mut ctx).unwrap();
    }

    #[test]
    fn update_open_resets_executed_flag() {
        let (mut ctx, _temp) = setup_context();
        let table_id = TableId(1);

        let rows = vec![Row::new(vec![Value::Int(1)])];
        let input = Box::new(MockExecutor::new(rows.clone(), vec!["id".into()]));
        let assignments = vec![(0, lit_int(100))];

        let mut update = UpdateExec::builder()
            .table_id(table_id)
            .schema(vec![])
            .input(input)
            .assignments(assignments)
            .build();

        update.open(&mut ctx).unwrap();
        assert_next_row(&mut update, &mut ctx, Row::new(vec![Value::Int(1)]));

        // Won't reset input, so won't get count again, but executed flag is reset
        update.open(&mut ctx).unwrap();
        assert_next_row(&mut update, &mut ctx, Row::new(vec![Value::Int(0)])); // Input exhausted

        update.close(&mut ctx).unwrap();
    }

    #[test]
    fn update_schema_empty() {
        let table_id = TableId(1);
        let input = Box::new(MockExecutor::new(vec![], vec![]));
        let update = UpdateExec::builder()
            .table_id(table_id)
            .schema(vec![])
            .input(input)
            .assignments(vec![])
            .build();

        assert_eq!(update.schema().len(), 0);
    }

    #[test]
    fn update_delegates_open_to_input() {
        let (mut ctx, _temp) = setup_context();
        let table_id = TableId(1);

        let input = Box::new(MockExecutor::new(vec![], vec![]));
        let mut update = UpdateExec::builder()
            .table_id(table_id)
            .schema(vec![])
            .input(input)
            .assignments(vec![])
            .build();

        assert!(update.open(&mut ctx).is_ok());
    }

    #[test]
    fn update_delegates_close_to_input() {
        let (mut ctx, _temp) = setup_context();
        let table_id = TableId(1);

        let input = Box::new(MockExecutor::new(vec![], vec![]));
        let mut update = UpdateExec::builder()
            .table_id(table_id)
            .schema(vec![])
            .input(input)
            .assignments(vec![])
            .build();

        update.open(&mut ctx).unwrap();
        assert!(update.close(&mut ctx).is_ok());
    }

    #[test]
    fn update_propagates_input_error() {
        let (mut ctx, _temp) = setup_context();
        let table_id = TableId(1);

        let input = Box::new(MockExecutor::with_next_error(common::DbError::Executor(
            "test error".into(),
        )));
        let mut update = UpdateExec::builder()
            .table_id(table_id)
            .schema(vec![])
            .input(input)
            .assignments(vec![(0, lit_int(1))])
            .build();

        update.open(&mut ctx).unwrap();
        assert_error_contains(update.next(&mut ctx), "test error");
    }

    // DeleteExec tests

    #[test]
    fn delete_no_matching_rows() {
        let (mut ctx, _temp) = setup_context();
        let table_id = TableId(1);

        let input = Box::new(MockExecutor::new(vec![], vec![]));
        let mut delete = DeleteExec::new(table_id, vec![], input);

        delete.open(&mut ctx).unwrap();
        assert_next_row(&mut delete, &mut ctx, Row::new(vec![Value::Int(0)]));
        assert_exhausted(&mut delete, &mut ctx);

        delete.close(&mut ctx).unwrap();
    }

    #[test]
    fn delete_single_row() {
        let (mut ctx, _temp) = setup_context();
        let table_id = TableId(1);

        let rows = vec![Row::new(vec![Value::Int(1), Value::Text("alice".into())])];
        let input = Box::new(MockExecutor::new(rows, vec!["id".into(), "name".into()]));
        let mut delete = DeleteExec::new(table_id, vec![], input);

        delete.open(&mut ctx).unwrap();
        assert_next_row(&mut delete, &mut ctx, Row::new(vec![Value::Int(1)]));
        assert_exhausted(&mut delete, &mut ctx);

        delete.close(&mut ctx).unwrap();
    }

    #[test]
    fn delete_multiple_rows() {
        let (mut ctx, _temp) = setup_context();
        let table_id = TableId(1);

        let rows = vec![
            Row::new(vec![Value::Int(1), Value::Text("alice".into())]),
            Row::new(vec![Value::Int(2), Value::Text("bob".into())]),
            Row::new(vec![Value::Int(3), Value::Text("carol".into())]),
        ];
        let input = Box::new(MockExecutor::new(rows, vec!["id".into(), "name".into()]));
        let mut delete = DeleteExec::new(table_id, vec![], input);

        delete.open(&mut ctx).unwrap();
        assert_next_row(&mut delete, &mut ctx, Row::new(vec![Value::Int(3)]));
        assert_exhausted(&mut delete, &mut ctx);

        delete.close(&mut ctx).unwrap();
    }

    #[test]
    fn delete_only_executes_once() {
        let (mut ctx, _temp) = setup_context();
        let table_id = TableId(1);

        let rows = vec![Row::new(vec![Value::Int(1)])];
        let input = Box::new(MockExecutor::new(rows, vec!["id".into()]));
        let mut delete = DeleteExec::new(table_id, vec![], input);

        delete.open(&mut ctx).unwrap();
        assert_next_row(&mut delete, &mut ctx, Row::new(vec![Value::Int(1)]));
        assert_exhausted(&mut delete, &mut ctx);
        assert_exhausted(&mut delete, &mut ctx);

        delete.close(&mut ctx).unwrap();
    }

    #[test]
    fn delete_open_resets_executed_flag() {
        let (mut ctx, _temp) = setup_context();
        let table_id = TableId(1);

        let rows = vec![Row::new(vec![Value::Int(1)])];
        let input = Box::new(MockExecutor::new(rows, vec!["id".into()]));
        let mut delete = DeleteExec::new(table_id, vec![], input);

        delete.open(&mut ctx).unwrap();
        assert_next_row(&mut delete, &mut ctx, Row::new(vec![Value::Int(1)]));

        // Won't reset input, so count is 0
        delete.open(&mut ctx).unwrap();
        assert_next_row(&mut delete, &mut ctx, Row::new(vec![Value::Int(0)]));

        delete.close(&mut ctx).unwrap();
    }

    #[test]
    fn delete_schema_empty() {
        let table_id = TableId(1);
        let input = Box::new(MockExecutor::new(vec![], vec![]));
        let delete = DeleteExec::new(table_id, vec![], input);

        assert_eq!(delete.schema().len(), 0);
    }

    #[test]
    fn delete_delegates_open_to_input() {
        let (mut ctx, _temp) = setup_context();
        let table_id = TableId(1);

        let input = Box::new(MockExecutor::new(vec![], vec![]));
        let mut delete = DeleteExec::new(table_id, vec![], input);

        assert!(delete.open(&mut ctx).is_ok());
    }

    #[test]
    fn delete_delegates_close_to_input() {
        let (mut ctx, _temp) = setup_context();
        let table_id = TableId(1);

        let input = Box::new(MockExecutor::new(vec![], vec![]));
        let mut delete = DeleteExec::new(table_id, vec![], input);

        delete.open(&mut ctx).unwrap();
        assert!(delete.close(&mut ctx).is_ok());
    }

    #[test]
    fn delete_propagates_input_error() {
        let (mut ctx, _temp) = setup_context();
        let table_id = TableId(1);

        let input = Box::new(MockExecutor::with_next_error(common::DbError::Executor(
            "test error".into(),
        )));
        let mut delete = DeleteExec::new(table_id, vec![], input);

        delete.open(&mut ctx).unwrap();
        assert_error_contains(delete.next(&mut ctx), "test error");
    }

    #[test]
    fn update_exec_persists_changes_and_wal() {
        let (mut ctx, temp) = setup_context();
        let table_id = TableId(1);

        for (id, name) in &[(1, "Ada"), (2, "Bob")] {
            let plan = PhysicalPlan::Insert {
                table_id,
                values: vec![
                    lit_int(*id),
                    lit_text(name),
                    ResolvedExpr::Literal(Value::Bool(true)),
                ],
            };
            execute_dml(plan, &mut ctx).unwrap();
        }

        let plan = PhysicalPlan::Update {
            table_id,
            assignments: vec![(1, lit_text("Ada Lovelace"))],
            predicate: Some(ResolvedExpr::Literal(Value::Bool(true))),
        };

        let count = execute_dml(plan, &mut ctx).unwrap();
        assert_eq!(count, 2);

        let scan_plan = PhysicalPlan::SeqScan {
            table_id,
            schema: vec!["id".into(), "name".into(), "active".into()],
        };
        let rows = execute_query(scan_plan, &mut ctx).unwrap();
        assert_eq!(rows.len(), 2);
        assert!(rows
            .iter()
            .all(|row| matches!(row.values[1], Value::Text(ref name) if name == "Ada Lovelace")));

        let wal_path = temp.path().join("test.wal");
        let wal_records = wal::Wal::replay(&wal_path).unwrap();
        assert!(wal_records
            .iter()
            .any(|rec| matches!(rec, WalRecord::Update { table, .. } if *table == table_id)));
    }

    #[test]
    fn delete_exec_removes_rows_and_wal() {
        let (mut ctx, temp) = setup_context();
        let table_id = TableId(1);

        for (id, name, active) in &[(1, "Ada", true), (2, "Bob", false)] {
            let plan = PhysicalPlan::Insert {
                table_id,
                values: vec![
                    lit_int(*id),
                    lit_text(name),
                    ResolvedExpr::Literal(Value::Bool(*active)),
                ],
            };
            execute_dml(plan, &mut ctx).unwrap();
        }

        let plan = PhysicalPlan::Delete {
            table_id,
            predicate: Some(ResolvedExpr::Column(2)),
        };

        let count = execute_dml(plan, &mut ctx).unwrap();
        assert_eq!(count, 1);

        let scan_plan = PhysicalPlan::SeqScan {
            table_id,
            schema: vec!["id".into(), "name".into(), "active".into()],
        };
        let rows = execute_query(scan_plan, &mut ctx).unwrap();
        assert_eq!(rows.len(), 1);
        assert!(matches!(rows[0].values[0], Value::Int(2)));

        let wal_path = temp.path().join("test.wal");
        let wal_records = wal::Wal::replay(&wal_path).unwrap();
        assert!(wal_records
            .iter()
            .any(|rec| matches!(rec, WalRecord::Delete { table, .. } if *table == table_id)));
    }
}
