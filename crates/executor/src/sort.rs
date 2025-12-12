//! Sort operator: orders rows based on specified columns.

use crate::{ExecutionContext, Executor};
use common::{ColumnId, DbResult, ExecutionStats, Row};
use planner::SortDirection;
use std::cmp::Ordering;
use std::time::Instant;
use types::Value;

/// Resolved ORDER BY clause with column ID and direction.
#[derive(Clone, Debug)]
pub struct SortKey {
    pub column_id: ColumnId,
    pub direction: SortDirection,
}

/// Sort operator - materializes input and returns rows in sorted order.
///
/// This is a blocking operator that must consume all input rows before
/// returning the first sorted row. Uses stable sort to preserve insertion
/// order for equal keys.
pub struct SortExec {
    input: Box<dyn Executor>,
    sort_keys: Vec<SortKey>,
    sorted_rows: Option<Vec<Row>>,
    current_index: usize,
    stats: ExecutionStats,
}

impl SortExec {
    /// Create a new sort operator.
    pub fn new(input: Box<dyn Executor>, sort_keys: Vec<SortKey>) -> Self {
        Self {
            input,
            sort_keys,
            sorted_rows: None,
            current_index: 0,
            stats: ExecutionStats::default(),
        }
    }

    /// Materialize and sort all rows from input.
    fn materialize_and_sort(&mut self, ctx: &mut ExecutionContext) -> DbResult<()> {
        let mut rows = Vec::new();

        // Collect all rows from input
        while let Some(row) = self.input.next(ctx)? {
            rows.push(row);
        }

        // Sort rows using stable sort
        let sort_keys = &self.sort_keys;
        rows.sort_by(|a, b| compare_rows(a, b, sort_keys));

        self.sorted_rows = Some(rows);
        self.current_index = 0;
        Ok(())
    }
}

impl Executor for SortExec {
    fn open(&mut self, ctx: &mut ExecutionContext) -> DbResult<()> {
        let start = Instant::now();
        self.stats = ExecutionStats::default();
        self.sorted_rows = None;
        self.current_index = 0;
        self.input.open(ctx)?;
        self.stats.open_time = start.elapsed();
        Ok(())
    }

    fn next(&mut self, ctx: &mut ExecutionContext) -> DbResult<Option<Row>> {
        let start = Instant::now();

        // Materialize and sort on first call to next()
        if self.sorted_rows.is_none() {
            self.materialize_and_sort(ctx)?;
        }

        // Return next sorted row
        let result = match &self.sorted_rows {
            Some(rows) => {
                if self.current_index < rows.len() {
                    let row = rows[self.current_index].clone();
                    self.current_index += 1;
                    self.stats.rows_produced += 1;
                    Ok(Some(row))
                } else {
                    Ok(None)
                }
            }
            None => Ok(None),
        };

        self.stats.total_next_time += start.elapsed();
        result
    }

    fn close(&mut self, ctx: &mut ExecutionContext) -> DbResult<()> {
        let start = Instant::now();
        self.sorted_rows = None;
        self.input.close(ctx)?;
        self.stats.close_time = start.elapsed();
        Ok(())
    }

    fn schema(&self) -> &[String] {
        self.input.schema()
    }

    fn stats(&self) -> Option<&ExecutionStats> {
        Some(&self.stats)
    }
}

/// Compare two rows based on sort keys.
fn compare_rows(a: &Row, b: &Row, sort_keys: &[SortKey]) -> Ordering {
    for key in sort_keys {
        let col_idx = key.column_id as usize;

        // Get values, treating out-of-bounds as NULL
        let val_a = a.values.get(col_idx);
        let val_b = b.values.get(col_idx);

        let ordering = match (val_a, val_b) {
            (None, None) => Ordering::Equal,
            (None, Some(_)) => Ordering::Less, // NULL sorts before non-NULL
            (Some(_), None) => Ordering::Greater, // non-NULL sorts after NULL
            (Some(a_val), Some(b_val)) => compare_values(a_val, b_val),
        };

        // Apply sort direction
        let directed_ordering = match key.direction {
            SortDirection::Asc => ordering,
            SortDirection::Desc => ordering.reverse(),
        };

        // If not equal, we have our answer
        if directed_ordering != Ordering::Equal {
            return directed_ordering;
        }
        // Otherwise, continue to next sort key
    }

    Ordering::Equal
}

/// Compare two values for sorting.
fn compare_values(a: &Value, b: &Value) -> Ordering {
    match (a, b) {
        // NULL handling
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::Null, _) => Ordering::Less,
        (_, Value::Null) => Ordering::Greater,

        // Int comparison
        (Value::Int(a), Value::Int(b)) => a.cmp(b),

        // Text comparison (lexicographic)
        (Value::Text(a), Value::Text(b)) => a.cmp(b),

        // Bool comparison (false < true)
        (Value::Bool(a), Value::Bool(b)) => a.cmp(b),

        // Cross-type comparisons: order by type (Bool < Int < Text)
        (Value::Bool(_), Value::Int(_)) => Ordering::Less,
        (Value::Bool(_), Value::Text(_)) => Ordering::Less,
        (Value::Int(_), Value::Bool(_)) => Ordering::Greater,
        (Value::Int(_), Value::Text(_)) => Ordering::Less,
        (Value::Text(_), Value::Bool(_)) => Ordering::Greater,
        (Value::Text(_), Value::Int(_)) => Ordering::Greater,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::helpers::{
        assert_exhausted, assert_next_row, setup_test_context, MockExecutor,
    };
    use planner::SortDirection;

    #[test]
    fn sort_single_column_ascending() {
        let (mut ctx, _temp) = setup_test_context();

        // Input: rows with Int values in random order
        let rows = vec![
            Row::new(vec![Value::Int(3)]),
            Row::new(vec![Value::Int(1)]),
            Row::new(vec![Value::Int(2)]),
        ];
        let input = Box::new(MockExecutor::new(rows, vec!["value".to_string()]));

        let sort_keys = vec![SortKey {
            column_id: 0,
            direction: SortDirection::Asc,
        }];
        let mut sort_exec = SortExec::new(input, sort_keys);

        sort_exec.open(&mut ctx).unwrap();

        // Should return rows in ascending order
        assert_next_row(&mut sort_exec, &mut ctx, Row::new(vec![Value::Int(1)]));
        assert_next_row(&mut sort_exec, &mut ctx, Row::new(vec![Value::Int(2)]));
        assert_next_row(&mut sort_exec, &mut ctx, Row::new(vec![Value::Int(3)]));
        assert_exhausted(&mut sort_exec, &mut ctx);

        sort_exec.close(&mut ctx).unwrap();
    }

    #[test]
    fn sort_single_column_descending() {
        let (mut ctx, _temp) = setup_test_context();

        let rows = vec![
            Row::new(vec![Value::Int(1)]),
            Row::new(vec![Value::Int(3)]),
            Row::new(vec![Value::Int(2)]),
        ];
        let input = Box::new(MockExecutor::new(rows, vec!["value".to_string()]));

        let sort_keys = vec![SortKey {
            column_id: 0,
            direction: SortDirection::Desc,
        }];
        let mut sort_exec = SortExec::new(input, sort_keys);

        sort_exec.open(&mut ctx).unwrap();

        assert_next_row(&mut sort_exec, &mut ctx, Row::new(vec![Value::Int(3)]));
        assert_next_row(&mut sort_exec, &mut ctx, Row::new(vec![Value::Int(2)]));
        assert_next_row(&mut sort_exec, &mut ctx, Row::new(vec![Value::Int(1)]));
        assert_exhausted(&mut sort_exec, &mut ctx);

        sort_exec.close(&mut ctx).unwrap();
    }

    #[test]
    fn sort_multiple_columns() {
        let (mut ctx, _temp) = setup_test_context();

        // Sort by first column ASC, then second column DESC
        let rows = vec![
            Row::new(vec![Value::Int(1), Value::Int(10)]),
            Row::new(vec![Value::Int(2), Value::Int(30)]),
            Row::new(vec![Value::Int(1), Value::Int(20)]),
            Row::new(vec![Value::Int(2), Value::Int(10)]),
        ];
        let input = Box::new(MockExecutor::new(
            rows,
            vec!["a".to_string(), "b".to_string()],
        ));

        let sort_keys = vec![
            SortKey {
                column_id: 0,
                direction: SortDirection::Asc,
            },
            SortKey {
                column_id: 1,
                direction: SortDirection::Desc,
            },
        ];
        let mut sort_exec = SortExec::new(input, sort_keys);

        sort_exec.open(&mut ctx).unwrap();

        // First column 1 rows, sorted by second column DESC (20, 10)
        assert_next_row(
            &mut sort_exec,
            &mut ctx,
            Row::new(vec![Value::Int(1), Value::Int(20)]),
        );
        assert_next_row(
            &mut sort_exec,
            &mut ctx,
            Row::new(vec![Value::Int(1), Value::Int(10)]),
        );
        // Then column 2 rows, sorted by second column DESC (30, 10)
        assert_next_row(
            &mut sort_exec,
            &mut ctx,
            Row::new(vec![Value::Int(2), Value::Int(30)]),
        );
        assert_next_row(
            &mut sort_exec,
            &mut ctx,
            Row::new(vec![Value::Int(2), Value::Int(10)]),
        );
        assert_exhausted(&mut sort_exec, &mut ctx);

        sort_exec.close(&mut ctx).unwrap();
    }

    #[test]
    fn sort_with_null_values() {
        let (mut ctx, _temp) = setup_test_context();

        // NULLs should sort before non-NULL values
        let rows = vec![
            Row::new(vec![Value::Int(3)]),
            Row::new(vec![Value::Null]),
            Row::new(vec![Value::Int(1)]),
            Row::new(vec![Value::Null]),
        ];
        let input = Box::new(MockExecutor::new(rows, vec!["value".to_string()]));

        let sort_keys = vec![SortKey {
            column_id: 0,
            direction: SortDirection::Asc,
        }];
        let mut sort_exec = SortExec::new(input, sort_keys);

        sort_exec.open(&mut ctx).unwrap();

        // NULLs first, then sorted values
        assert_next_row(&mut sort_exec, &mut ctx, Row::new(vec![Value::Null]));
        assert_next_row(&mut sort_exec, &mut ctx, Row::new(vec![Value::Null]));
        assert_next_row(&mut sort_exec, &mut ctx, Row::new(vec![Value::Int(1)]));
        assert_next_row(&mut sort_exec, &mut ctx, Row::new(vec![Value::Int(3)]));
        assert_exhausted(&mut sort_exec, &mut ctx);

        sort_exec.close(&mut ctx).unwrap();
    }

    #[test]
    fn sort_text_lexicographic() {
        let (mut ctx, _temp) = setup_test_context();

        let rows = vec![
            Row::new(vec![Value::Text("zebra".to_string())]),
            Row::new(vec![Value::Text("apple".to_string())]),
            Row::new(vec![Value::Text("mango".to_string())]),
        ];
        let input = Box::new(MockExecutor::new(rows, vec!["word".to_string()]));

        let sort_keys = vec![SortKey {
            column_id: 0,
            direction: SortDirection::Asc,
        }];
        let mut sort_exec = SortExec::new(input, sort_keys);

        sort_exec.open(&mut ctx).unwrap();

        assert_next_row(
            &mut sort_exec,
            &mut ctx,
            Row::new(vec![Value::Text("apple".to_string())]),
        );
        assert_next_row(
            &mut sort_exec,
            &mut ctx,
            Row::new(vec![Value::Text("mango".to_string())]),
        );
        assert_next_row(
            &mut sort_exec,
            &mut ctx,
            Row::new(vec![Value::Text("zebra".to_string())]),
        );
        assert_exhausted(&mut sort_exec, &mut ctx);

        sort_exec.close(&mut ctx).unwrap();
    }

    #[test]
    fn sort_empty_input() {
        let (mut ctx, _temp) = setup_test_context();

        let input = Box::new(MockExecutor::new(vec![], vec!["value".to_string()]));
        let sort_keys = vec![SortKey {
            column_id: 0,
            direction: SortDirection::Asc,
        }];
        let mut sort_exec = SortExec::new(input, sort_keys);

        sort_exec.open(&mut ctx).unwrap();
        assert_exhausted(&mut sort_exec, &mut ctx);
        sort_exec.close(&mut ctx).unwrap();
    }

    #[test]
    fn sort_single_row() {
        let (mut ctx, _temp) = setup_test_context();

        let rows = vec![Row::new(vec![Value::Int(42)])];
        let input = Box::new(MockExecutor::new(rows, vec!["value".to_string()]));
        let sort_keys = vec![SortKey {
            column_id: 0,
            direction: SortDirection::Asc,
        }];
        let mut sort_exec = SortExec::new(input, sort_keys);

        sort_exec.open(&mut ctx).unwrap();
        assert_next_row(&mut sort_exec, &mut ctx, Row::new(vec![Value::Int(42)]));
        assert_exhausted(&mut sort_exec, &mut ctx);
        sort_exec.close(&mut ctx).unwrap();
    }

    #[test]
    fn sort_schema_delegation() {
        let (ctx, _temp) = setup_test_context();

        let input = Box::new(MockExecutor::new(
            vec![],
            vec!["id".to_string(), "name".to_string()],
        ));
        let sort_keys = vec![SortKey {
            column_id: 0,
            direction: SortDirection::Asc,
        }];
        let sort_exec = SortExec::new(input, sort_keys);

        assert_eq!(sort_exec.schema(), &["id", "name"]);
        drop(ctx);
    }

    #[test]
    fn sort_stable_sort_preserves_order() {
        let (mut ctx, _temp) = setup_test_context();

        // Multiple rows with same sort key value should maintain insertion order
        let rows = vec![
            Row::new(vec![Value::Int(1), Value::Text("first".to_string())]),
            Row::new(vec![Value::Int(1), Value::Text("second".to_string())]),
            Row::new(vec![Value::Int(1), Value::Text("third".to_string())]),
        ];
        let input = Box::new(MockExecutor::new(
            rows,
            vec!["value".to_string(), "label".to_string()],
        ));

        let sort_keys = vec![SortKey {
            column_id: 0,
            direction: SortDirection::Asc,
        }];
        let mut sort_exec = SortExec::new(input, sort_keys);

        sort_exec.open(&mut ctx).unwrap();

        // Should maintain insertion order for equal keys
        assert_next_row(
            &mut sort_exec,
            &mut ctx,
            Row::new(vec![Value::Int(1), Value::Text("first".to_string())]),
        );
        assert_next_row(
            &mut sort_exec,
            &mut ctx,
            Row::new(vec![Value::Int(1), Value::Text("second".to_string())]),
        );
        assert_next_row(
            &mut sort_exec,
            &mut ctx,
            Row::new(vec![Value::Int(1), Value::Text("third".to_string())]),
        );
        assert_exhausted(&mut sort_exec, &mut ctx);

        sort_exec.close(&mut ctx).unwrap();
    }

    #[test]
    fn sort_stats_tracking() {
        let (mut ctx, _temp) = setup_test_context();

        let rows = vec![
            Row::new(vec![Value::Int(3)]),
            Row::new(vec![Value::Int(1)]),
            Row::new(vec![Value::Int(2)]),
        ];
        let input = Box::new(MockExecutor::new(rows, vec!["value".to_string()]));
        let sort_keys = vec![SortKey {
            column_id: 0,
            direction: SortDirection::Asc,
        }];
        let mut sort_exec = SortExec::new(input, sort_keys);

        sort_exec.open(&mut ctx).unwrap();

        // Consume all rows
        while sort_exec.next(&mut ctx).unwrap().is_some() {}

        sort_exec.close(&mut ctx).unwrap();

        let stats = sort_exec.stats().unwrap();
        assert_eq!(stats.rows_produced, 3);
        assert!(stats.open_time.as_nanos() > 0);
        assert!(stats.total_next_time.as_nanos() > 0);
        assert!(stats.close_time.as_nanos() > 0);
    }

    #[test]
    fn sort_open_resets_state() {
        let (mut ctx, _temp) = setup_test_context();

        let rows = vec![Row::new(vec![Value::Int(1)])];
        let input = Box::new(MockExecutor::new(rows, vec!["value".to_string()]));
        let sort_keys = vec![SortKey {
            column_id: 0,
            direction: SortDirection::Asc,
        }];
        let mut sort_exec = SortExec::new(input, sort_keys);

        // First execution
        sort_exec.open(&mut ctx).unwrap();
        assert_next_row(&mut sort_exec, &mut ctx, Row::new(vec![Value::Int(1)]));
        sort_exec.close(&mut ctx).unwrap();

        // Second open should reset state
        // Note: MockExecutor is exhausted, so this tests that sorted_rows is cleared
        sort_exec.open(&mut ctx).unwrap();
        assert_exhausted(&mut sort_exec, &mut ctx);
        sort_exec.close(&mut ctx).unwrap();
    }
}
