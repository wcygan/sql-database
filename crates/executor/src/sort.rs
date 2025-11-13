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
            (None, Some(_)) => Ordering::Less,    // NULL sorts before non-NULL
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
