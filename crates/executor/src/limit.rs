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
