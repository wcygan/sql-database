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
        } => Ok(Box::new(
            IndexScanExec::builder()
                .table_id(table_id)
                .index_name(index_name)
                .predicate(predicate)
                .schema(schema)
                .build(),
        )),

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
            Ok(Box::new(
                UpdateExec::builder()
                    .table_id(table_id)
                    .schema(schema)
                    .input(input)
                    .assignments(assignments)
                    .build(),
            ))
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

#[cfg(test)]
mod tests {
    use super::*;
    use common::TableId;
    use expr::BinaryOp;
    use planner::ResolvedExpr;
    use types::Value;

    #[test]
    fn build_seq_scan() {
        let plan = PhysicalPlan::SeqScan {
            table_id: TableId(1),
            schema: vec!["id".into(), "name".into()],
        };

        let executor = build_executor(plan);
        assert!(executor.is_ok());

        let executor = executor.unwrap();
        assert_eq!(executor.schema(), &["id", "name"]);
    }

    #[test]
    fn build_seq_scan_empty_schema() {
        let plan = PhysicalPlan::SeqScan {
            table_id: TableId(1),
            schema: vec![],
        };

        let executor = build_executor(plan);
        assert!(executor.is_ok());
        assert_eq!(executor.unwrap().schema().len(), 0);
    }

    #[test]
    fn build_index_scan() {
        use planner::IndexPredicate;

        let plan = PhysicalPlan::IndexScan {
            table_id: TableId(1),
            index_name: "idx_users_id".into(),
            predicate: IndexPredicate::Eq {
                col: 0,
                value: ResolvedExpr::Literal(Value::Int(42)),
            },
            schema: vec!["id".into()],
        };

        let executor = build_executor(plan);
        assert!(executor.is_ok());
    }

    #[test]
    fn build_filter() {
        let input = PhysicalPlan::SeqScan {
            table_id: TableId(1),
            schema: vec!["id".into()],
        };

        let plan = PhysicalPlan::Filter {
            input: Box::new(input),
            predicate: ResolvedExpr::Literal(Value::Bool(true)),
        };

        let executor = build_executor(plan);
        assert!(executor.is_ok());
    }

    #[test]
    fn build_filter_with_complex_predicate() {
        let input = PhysicalPlan::SeqScan {
            table_id: TableId(1),
            schema: vec!["id".into(), "age".into()],
        };

        let predicate = ResolvedExpr::Binary {
            left: Box::new(ResolvedExpr::Column(0)),
            op: BinaryOp::Eq,
            right: Box::new(ResolvedExpr::Literal(Value::Int(42))),
        };

        let plan = PhysicalPlan::Filter {
            input: Box::new(input),
            predicate,
        };

        let executor = build_executor(plan);
        assert!(executor.is_ok());
    }

    #[test]
    fn build_project() {
        let input = PhysicalPlan::SeqScan {
            table_id: TableId(1),
            schema: vec!["id".into(), "name".into()],
        };

        let plan = PhysicalPlan::Project {
            input: Box::new(input),
            columns: vec![("id".to_string(), 0)],
        };

        let executor = build_executor(plan);
        assert!(executor.is_ok());
    }

    #[test]
    fn build_project_multiple_columns() {
        let input = PhysicalPlan::SeqScan {
            table_id: TableId(1),
            schema: vec!["id".into(), "name".into(), "age".into()],
        };

        let plan = PhysicalPlan::Project {
            input: Box::new(input),
            columns: vec![("name".to_string(), 1), ("id".to_string(), 0)],
        };

        let executor = build_executor(plan);
        assert!(executor.is_ok());
    }

    #[test]
    fn build_insert() {
        let plan = PhysicalPlan::Insert {
            table_id: TableId(1),
            values: vec![
                ResolvedExpr::Literal(Value::Int(1)),
                ResolvedExpr::Literal(Value::Text("alice".into())),
            ],
        };

        let executor = build_executor(plan);
        assert!(executor.is_ok());
        assert_eq!(executor.unwrap().schema().len(), 0);
    }

    #[test]
    fn build_update_without_predicate() {
        let plan = PhysicalPlan::Update {
            table_id: TableId(1),
            assignments: vec![(0, ResolvedExpr::Literal(Value::Int(100)))],
            predicate: None,
        };

        let executor = build_executor(plan);
        assert!(executor.is_ok());
    }

    #[test]
    fn build_update_with_predicate() {
        let predicate = ResolvedExpr::Binary {
            left: Box::new(ResolvedExpr::Column(0)),
            op: BinaryOp::Gt,
            right: Box::new(ResolvedExpr::Literal(Value::Int(10))),
        };

        let plan = PhysicalPlan::Update {
            table_id: TableId(1),
            assignments: vec![(1, ResolvedExpr::Literal(Value::Text("updated".into())))],
            predicate: Some(predicate),
        };

        let executor = build_executor(plan);
        assert!(executor.is_ok());
    }

    #[test]
    fn build_delete_without_predicate() {
        let plan = PhysicalPlan::Delete {
            table_id: TableId(1),
            predicate: None,
        };

        let executor = build_executor(plan);
        assert!(executor.is_ok());
    }

    #[test]
    fn build_delete_with_predicate() {
        let predicate = ResolvedExpr::Binary {
            left: Box::new(ResolvedExpr::Column(2)),
            op: BinaryOp::Eq,
            right: Box::new(ResolvedExpr::Literal(Value::Bool(false))),
        };

        let plan = PhysicalPlan::Delete {
            table_id: TableId(1),
            predicate: Some(predicate),
        };

        let executor = build_executor(plan);
        assert!(executor.is_ok());
    }

    #[test]
    fn build_nested_filter_over_scan() {
        let scan = PhysicalPlan::SeqScan {
            table_id: TableId(1),
            schema: vec!["id".into(), "active".into()],
        };

        let filter = PhysicalPlan::Filter {
            input: Box::new(scan),
            predicate: ResolvedExpr::Column(1),
        };

        let executor = build_executor(filter);
        assert!(executor.is_ok());
    }

    #[test]
    fn build_nested_project_over_filter_over_scan() {
        let scan = PhysicalPlan::SeqScan {
            table_id: TableId(1),
            schema: vec!["id".into(), "name".into(), "active".into()],
        };

        let filter = PhysicalPlan::Filter {
            input: Box::new(scan),
            predicate: ResolvedExpr::Column(2),
        };

        let project = PhysicalPlan::Project {
            input: Box::new(filter),
            columns: vec![("name".to_string(), 1)],
        };

        let executor = build_executor(project);
        assert!(executor.is_ok());
    }
}
