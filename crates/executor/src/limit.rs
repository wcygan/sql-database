//! Limit operator: restricts output to a subset of rows.

use crate::{ExecutionContext, Executor};
use common::{DbResult, ExecutionStats, Row};
use std::time::Instant;

/// Limit operator - applies LIMIT and OFFSET to input rows.
///
/// Skips `offset` rows, then returns up to `limit` rows.
/// If offset is None, starts from the beginning.
/// If limit is None, returns all rows after offset.
pub struct LimitExec {
    input: Box<dyn Executor>,
    limit: Option<u64>,
    offset: Option<u64>,
    rows_skipped: u64,
    rows_returned: u64,
    stats: ExecutionStats,
}

impl LimitExec {
    /// Create a new limit operator.
    pub fn new(input: Box<dyn Executor>, limit: Option<u64>, offset: Option<u64>) -> Self {
        Self {
            input,
            limit,
            offset,
            rows_skipped: 0,
            rows_returned: 0,
            stats: ExecutionStats::default(),
        }
    }
}

impl Executor for LimitExec {
    fn open(&mut self, ctx: &mut ExecutionContext) -> DbResult<()> {
        let start = Instant::now();
        self.stats = ExecutionStats::default();
        self.rows_skipped = 0;
        self.rows_returned = 0;
        self.input.open(ctx)?;
        self.stats.open_time = start.elapsed();
        Ok(())
    }

    fn next(&mut self, ctx: &mut ExecutionContext) -> DbResult<Option<Row>> {
        let start = Instant::now();

        // Check if we've hit the limit
        if let Some(limit) = self.limit {
            if self.rows_returned >= limit {
                self.stats.total_next_time += start.elapsed();
                return Ok(None);
            }
        }

        // Skip rows until we reach offset
        let offset = self.offset.unwrap_or(0);
        while self.rows_skipped < offset {
            match self.input.next(ctx)? {
                Some(_) => {
                    self.rows_skipped += 1;
                }
                None => {
                    self.stats.total_next_time += start.elapsed();
                    return Ok(None);
                }
            }
        }

        // Return the next row
        match self.input.next(ctx)? {
            Some(row) => {
                self.rows_returned += 1;
                self.stats.rows_produced += 1;
                self.stats.total_next_time += start.elapsed();
                Ok(Some(row))
            }
            None => {
                self.stats.total_next_time += start.elapsed();
                Ok(None)
            }
        }
    }

    fn close(&mut self, ctx: &mut ExecutionContext) -> DbResult<()> {
        let start = Instant::now();
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::helpers::{
        assert_exhausted, assert_next_row, create_test_catalog, MockExecutor,
    };
    use buffer::FilePager;
    use types::Value;
    use wal::Wal;

    fn create_test_context() -> ExecutionContext<'static> {
        let catalog = Box::leak(Box::new(create_test_catalog()));
        let temp_dir = Box::leak(Box::new(tempfile::tempdir().unwrap()));
        let pager = Box::leak(Box::new(FilePager::new(temp_dir.path(), 10)));
        let wal = Box::leak(Box::new(Wal::open(temp_dir.path().join("test.wal")).unwrap()));

        ExecutionContext::new(catalog, pager, wal, temp_dir.path().into())
    }

    #[test]
    fn limit_only() {
        let mut ctx = create_test_context();

        let rows = vec![
            Row::new(vec![Value::Int(1)]),
            Row::new(vec![Value::Int(2)]),
            Row::new(vec![Value::Int(3)]),
            Row::new(vec![Value::Int(4)]),
            Row::new(vec![Value::Int(5)]),
        ];
        let input = Box::new(MockExecutor::new(rows, vec!["value".to_string()]));
        let mut limit_exec = LimitExec::new(input, Some(3), None);

        limit_exec.open(&mut ctx).unwrap();

        assert_next_row(&mut limit_exec, &mut ctx, Row::new(vec![Value::Int(1)]));
        assert_next_row(&mut limit_exec, &mut ctx, Row::new(vec![Value::Int(2)]));
        assert_next_row(&mut limit_exec, &mut ctx, Row::new(vec![Value::Int(3)]));
        assert_exhausted(&mut limit_exec, &mut ctx);

        limit_exec.close(&mut ctx).unwrap();
    }

    #[test]
    fn offset_only() {
        let mut ctx = create_test_context();

        let rows = vec![
            Row::new(vec![Value::Int(1)]),
            Row::new(vec![Value::Int(2)]),
            Row::new(vec![Value::Int(3)]),
            Row::new(vec![Value::Int(4)]),
        ];
        let input = Box::new(MockExecutor::new(rows, vec!["value".to_string()]));
        let mut limit_exec = LimitExec::new(input, None, Some(2));

        limit_exec.open(&mut ctx).unwrap();

        // Should skip first 2 rows
        assert_next_row(&mut limit_exec, &mut ctx, Row::new(vec![Value::Int(3)]));
        assert_next_row(&mut limit_exec, &mut ctx, Row::new(vec![Value::Int(4)]));
        assert_exhausted(&mut limit_exec, &mut ctx);

        limit_exec.close(&mut ctx).unwrap();
    }

    #[test]
    fn limit_and_offset() {
        let mut ctx = create_test_context();

        let rows = vec![
            Row::new(vec![Value::Int(1)]),
            Row::new(vec![Value::Int(2)]),
            Row::new(vec![Value::Int(3)]),
            Row::new(vec![Value::Int(4)]),
            Row::new(vec![Value::Int(5)]),
        ];
        let input = Box::new(MockExecutor::new(rows, vec!["value".to_string()]));
        let mut limit_exec = LimitExec::new(input, Some(2), Some(2));

        limit_exec.open(&mut ctx).unwrap();

        // Skip first 2, take next 2
        assert_next_row(&mut limit_exec, &mut ctx, Row::new(vec![Value::Int(3)]));
        assert_next_row(&mut limit_exec, &mut ctx, Row::new(vec![Value::Int(4)]));
        assert_exhausted(&mut limit_exec, &mut ctx);

        limit_exec.close(&mut ctx).unwrap();
    }

    #[test]
    fn offset_beyond_total_rows() {
        let mut ctx = create_test_context();

        let rows = vec![
            Row::new(vec![Value::Int(1)]),
            Row::new(vec![Value::Int(2)]),
        ];
        let input = Box::new(MockExecutor::new(rows, vec!["value".to_string()]));
        let mut limit_exec = LimitExec::new(input, None, Some(10));

        limit_exec.open(&mut ctx).unwrap();
        assert_exhausted(&mut limit_exec, &mut ctx);
        limit_exec.close(&mut ctx).unwrap();
    }

    #[test]
    fn limit_larger_than_total_rows() {
        let mut ctx = create_test_context();

        let rows = vec![
            Row::new(vec![Value::Int(1)]),
            Row::new(vec![Value::Int(2)]),
        ];
        let input = Box::new(MockExecutor::new(rows, vec!["value".to_string()]));
        let mut limit_exec = LimitExec::new(input, Some(100), None);

        limit_exec.open(&mut ctx).unwrap();

        // Should return all available rows
        assert_next_row(&mut limit_exec, &mut ctx, Row::new(vec![Value::Int(1)]));
        assert_next_row(&mut limit_exec, &mut ctx, Row::new(vec![Value::Int(2)]));
        assert_exhausted(&mut limit_exec, &mut ctx);

        limit_exec.close(&mut ctx).unwrap();
    }

    #[test]
    fn limit_zero() {
        let mut ctx = create_test_context();

        let rows = vec![
            Row::new(vec![Value::Int(1)]),
            Row::new(vec![Value::Int(2)]),
        ];
        let input = Box::new(MockExecutor::new(rows, vec!["value".to_string()]));
        let mut limit_exec = LimitExec::new(input, Some(0), None);

        limit_exec.open(&mut ctx).unwrap();
        assert_exhausted(&mut limit_exec, &mut ctx);
        limit_exec.close(&mut ctx).unwrap();
    }

    #[test]
    fn empty_input() {
        let mut ctx = create_test_context();

        let input = Box::new(MockExecutor::new(vec![], vec!["value".to_string()]));
        let mut limit_exec = LimitExec::new(input, Some(5), Some(2));

        limit_exec.open(&mut ctx).unwrap();
        assert_exhausted(&mut limit_exec, &mut ctx);
        limit_exec.close(&mut ctx).unwrap();
    }

    #[test]
    fn schema_delegation() {
        let ctx = create_test_context();

        let input = Box::new(MockExecutor::new(
            vec![],
            vec!["id".to_string(), "name".to_string()],
        ));
        let limit_exec = LimitExec::new(input, Some(10), None);

        assert_eq!(limit_exec.schema(), &["id", "name"]);
        drop(ctx);
    }

    #[test]
    fn limit_one() {
        let mut ctx = create_test_context();

        let rows = vec![
            Row::new(vec![Value::Int(1)]),
            Row::new(vec![Value::Int(2)]),
            Row::new(vec![Value::Int(3)]),
        ];
        let input = Box::new(MockExecutor::new(rows, vec!["value".to_string()]));
        let mut limit_exec = LimitExec::new(input, Some(1), None);

        limit_exec.open(&mut ctx).unwrap();
        assert_next_row(&mut limit_exec, &mut ctx, Row::new(vec![Value::Int(1)]));
        assert_exhausted(&mut limit_exec, &mut ctx);
        limit_exec.close(&mut ctx).unwrap();
    }

    #[test]
    fn offset_one() {
        let mut ctx = create_test_context();

        let rows = vec![
            Row::new(vec![Value::Int(1)]),
            Row::new(vec![Value::Int(2)]),
        ];
        let input = Box::new(MockExecutor::new(rows, vec!["value".to_string()]));
        let mut limit_exec = LimitExec::new(input, None, Some(1));

        limit_exec.open(&mut ctx).unwrap();
        assert_next_row(&mut limit_exec, &mut ctx, Row::new(vec![Value::Int(2)]));
        assert_exhausted(&mut limit_exec, &mut ctx);
        limit_exec.close(&mut ctx).unwrap();
    }

    #[test]
    fn stats_tracking() {
        let mut ctx = create_test_context();

        let rows = vec![
            Row::new(vec![Value::Int(1)]),
            Row::new(vec![Value::Int(2)]),
            Row::new(vec![Value::Int(3)]),
        ];
        let input = Box::new(MockExecutor::new(rows, vec!["value".to_string()]));
        let mut limit_exec = LimitExec::new(input, Some(2), None);

        limit_exec.open(&mut ctx).unwrap();

        // Consume all rows
        while limit_exec.next(&mut ctx).unwrap().is_some() {}

        limit_exec.close(&mut ctx).unwrap();

        let stats = limit_exec.stats().unwrap();
        assert_eq!(stats.rows_produced, 2);
        assert!(stats.open_time.as_nanos() > 0);
        assert!(stats.total_next_time.as_nanos() > 0);
        assert!(stats.close_time.as_nanos() > 0);
    }

    #[test]
    fn open_resets_state() {
        let mut ctx = create_test_context();

        let rows = vec![
            Row::new(vec![Value::Int(1)]),
            Row::new(vec![Value::Int(2)]),
            Row::new(vec![Value::Int(3)]),
        ];
        let input = Box::new(MockExecutor::new(rows, vec!["value".to_string()]));
        let mut limit_exec = LimitExec::new(input, Some(1), None);

        // First execution
        limit_exec.open(&mut ctx).unwrap();
        assert_next_row(&mut limit_exec, &mut ctx, Row::new(vec![Value::Int(1)]));

        // Verify state before close
        assert_eq!(limit_exec.rows_returned, 1);
        limit_exec.close(&mut ctx).unwrap();

        // Second open should reset counters
        limit_exec.open(&mut ctx).unwrap();
        assert_eq!(limit_exec.rows_returned, 0);
        assert_eq!(limit_exec.rows_skipped, 0);

        // Should be able to return one more row (row 2 from input)
        assert_next_row(&mut limit_exec, &mut ctx, Row::new(vec![Value::Int(2)]));
        assert_exhausted(&mut limit_exec, &mut ctx);
        limit_exec.close(&mut ctx).unwrap();
    }

    #[test]
    fn pagination_scenario() {
        let mut ctx = create_test_context();

        // Simulate pagination: page size 2, get page 2 (skip 2, take 2)
        let rows = vec![
            Row::new(vec![Value::Int(1)]),
            Row::new(vec![Value::Int(2)]),
            Row::new(vec![Value::Int(3)]),
            Row::new(vec![Value::Int(4)]),
            Row::new(vec![Value::Int(5)]),
        ];
        let input = Box::new(MockExecutor::new(rows, vec!["value".to_string()]));
        let mut limit_exec = LimitExec::new(input, Some(2), Some(2));

        limit_exec.open(&mut ctx).unwrap();

        // Should get rows 3 and 4
        assert_next_row(&mut limit_exec, &mut ctx, Row::new(vec![Value::Int(3)]));
        assert_next_row(&mut limit_exec, &mut ctx, Row::new(vec![Value::Int(4)]));
        assert_exhausted(&mut limit_exec, &mut ctx);

        limit_exec.close(&mut ctx).unwrap();
    }
}
