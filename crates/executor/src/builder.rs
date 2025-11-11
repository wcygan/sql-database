//! Builder: constructs executor trees from physical plans.

use crate::{
    dml::{DeleteExec, InsertExec, UpdateExec},
    filter::FilterExec,
    project::ProjectExec,
    scan::{IndexScanExec, SeqScanExec},
    Executor,
};
use common::DbResult;
use planner::PhysicalPlan;

/// Build an executor tree from a physical plan.
///
/// Recursively constructs operator instances, wiring up child inputs.
///
/// # Errors
///
/// Returns `DbError::Executor` if the plan contains unsupported operators.
pub fn build_executor(plan: PhysicalPlan) -> DbResult<Box<dyn Executor>> {
    match plan {
        PhysicalPlan::SeqScan { table_id, schema } => {
            Ok(Box::new(SeqScanExec::new(table_id, schema)))
        }

        PhysicalPlan::IndexScan {
            table_id,
            index_name,
            predicate,
            schema,
        } => Ok(Box::new(IndexScanExec::new(
            table_id, index_name, predicate, schema,
        ))),

        PhysicalPlan::Filter { input, predicate } => {
            let child = build_executor(*input)?;
            Ok(Box::new(FilterExec::new(child, predicate)))
        }

        PhysicalPlan::Project { input, columns } => {
            let child = build_executor(*input)?;
            Ok(Box::new(ProjectExec::new(child, columns)))
        }

        PhysicalPlan::Insert { table_id, values } => {
            // No input operator for INSERT
            let schema = vec![]; // INSERT doesn't produce a schema
            Ok(Box::new(InsertExec::new(table_id, schema, values)))
        }

        PhysicalPlan::Update {
            table_id,
            assignments,
            predicate,
        } => {
            // Build scan + optional filter as input
            let table_meta = get_table_schema_stub(table_id);
            let mut input: Box<dyn Executor> = Box::new(SeqScanExec::new(table_id, table_meta));

            if let Some(pred) = predicate {
                input = Box::new(FilterExec::new(input, pred));
            }

            let schema = vec![];
            Ok(Box::new(UpdateExec::new(
                table_id,
                schema,
                input,
                assignments,
            )))
        }

        PhysicalPlan::Delete {
            table_id,
            predicate,
        } => {
            // Build scan + optional filter as input
            let table_meta = get_table_schema_stub(table_id);
            let mut input: Box<dyn Executor> = Box::new(SeqScanExec::new(table_id, table_meta));

            if let Some(pred) = predicate {
                input = Box::new(FilterExec::new(input, pred));
            }

            let schema = vec![];
            Ok(Box::new(DeleteExec::new(table_id, schema, input)))
        }
    }
}

/// Stub: get table schema for a table ID.
///
/// TODO: This should query the catalog properly. For now, returns empty schema.
fn get_table_schema_stub(_table_id: common::TableId) -> Vec<String> {
    vec![]
}
