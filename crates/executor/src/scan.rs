//! Scan operators: SeqScan and IndexScan.

use crate::{ExecutionContext, Executor};
use common::{DbResult, PageId, RecordId, Row, TableId};
use planner::IndexPredicate;
use storage::HeapTable;

/// Sequential scan operator - iterates all rows in a table.
///
/// Scans pages sequentially from beginning to end, fetching each page
/// via the buffer pool and deserializing rows.
pub struct SeqScanExec {
    table_id: TableId,
    schema: Vec<String>,
    current_page: PageId,
    current_slot: u16,
    num_pages: Option<u64>,
}

impl SeqScanExec {
    /// Create a new sequential scan operator.
    pub fn new(table_id: TableId, schema: Vec<String>) -> Self {
        Self {
            table_id,
            schema,
            current_page: PageId(0),
            current_slot: 0,
            num_pages: None,
        }
    }

    /// Try to fetch the next row from storage.
    fn fetch_next_row(&mut self, ctx: &mut ExecutionContext) -> DbResult<Option<Row>> {
        let mut heap_table = ctx.heap_table(self.table_id)?;

        // Check if we've exhausted all pages
        let num_pages = match self.num_pages {
            Some(n) => n,
            None => {
                // Compute number of pages on first call
                let n = compute_num_pages(&mut heap_table)?;
                self.num_pages = Some(n);
                n
            }
        };

        if self.current_page.0 >= num_pages {
            return Ok(None);
        }

        // Try to fetch current slot
        loop {
            let rid = RecordId {
                page_id: self.current_page,
                slot: self.current_slot,
            };

            match heap_table.get(rid) {
                Ok(row) => {
                    // Found a row, advance slot and return
                    self.current_slot += 1;
                    return Ok(Some(row));
                }
                Err(e) => {
                    // Check if this is just an empty/deleted slot
                    let is_empty_slot = matches!(e, common::DbError::Storage(ref msg) if msg.contains("slot") || msg.contains("empty"));

                    if is_empty_slot {
                        // Try next slot in same page
                        self.current_slot += 1;

                        // If we've tried enough slots, move to next page
                        // (heuristic: assume max ~100 slots per page)
                        if self.current_slot > 100 {
                            self.current_page = PageId(self.current_page.0 + 1);
                            self.current_slot = 0;

                            if self.current_page.0 >= num_pages {
                                return Ok(None);
                            }
                        }
                    } else {
                        // Real error, propagate
                        return Err(e);
                    }
                }
            }
        }
    }
}

impl Executor for SeqScanExec {
    fn open(&mut self, _ctx: &mut ExecutionContext) -> DbResult<()> {
        // Reset state
        self.current_page = PageId(0);
        self.current_slot = 0;
        self.num_pages = None;
        Ok(())
    }

    fn next(&mut self, ctx: &mut ExecutionContext) -> DbResult<Option<Row>> {
        self.fetch_next_row(ctx)
    }

    fn close(&mut self, _ctx: &mut ExecutionContext) -> DbResult<()> {
        // Nothing to clean up for seq scan
        Ok(())
    }

    fn schema(&self) -> &[String] {
        &self.schema
    }
}

/// Index scan operator - uses B+Tree index to find rows (stub for future).
///
/// Currently falls back to sequential scan. Will be implemented when
/// the index crate is added.
pub struct IndexScanExec {
    #[allow(dead_code)]
    table_id: TableId,
    #[allow(dead_code)]
    index_name: String,
    #[allow(dead_code)]
    predicate: IndexPredicate,
    schema: Vec<String>,
    seq_scan: SeqScanExec,
}

impl IndexScanExec {
    /// Create a new index scan operator.
    pub fn new(
        table_id: TableId,
        index_name: String,
        predicate: IndexPredicate,
        schema: Vec<String>,
    ) -> Self {
        // TODO: When index crate is implemented, use B+Tree here
        let seq_scan = SeqScanExec::new(table_id, schema.clone());

        Self {
            table_id,
            index_name,
            predicate,
            schema,
            seq_scan,
        }
    }
}

impl Executor for IndexScanExec {
    fn open(&mut self, ctx: &mut ExecutionContext) -> DbResult<()> {
        // TODO: Open B+Tree index
        self.seq_scan.open(ctx)
    }

    fn next(&mut self, ctx: &mut ExecutionContext) -> DbResult<Option<Row>> {
        // TODO: Use index to find matching RIDs, then fetch from heap
        self.seq_scan.next(ctx)
    }

    fn close(&mut self, ctx: &mut ExecutionContext) -> DbResult<()> {
        // TODO: Close B+Tree index
        self.seq_scan.close(ctx)
    }

    fn schema(&self) -> &[String] {
        &self.schema
    }
}

/// Helper: compute number of pages in a heap file.
fn compute_num_pages(heap_table: &mut impl HeapTable) -> DbResult<u64> {
    // Try to probe increasing page IDs until we get an error
    // This is a simple heuristic; ideally HeapTable would expose num_pages()
    let mut page_id = 0;
    loop {
        let rid = RecordId {
            page_id: PageId(page_id),
            slot: 0,
        };

        match heap_table.get(rid) {
            Ok(_) => page_id += 1,
            Err(e) => {
                // Check if this is a "page not found" error
                if matches!(e, common::DbError::Storage(ref msg) if msg.contains("page") || msg.contains("beyond"))
                {
                    return Ok(page_id);
                }
                // For empty slots, the page exists
                if matches!(e, common::DbError::Storage(ref msg) if msg.contains("slot") || msg.contains("empty"))
                {
                    page_id += 1;
                } else {
                    return Err(e);
                }
            }
        }

        // Safety limit
        if page_id > 100_000 {
            return Ok(page_id);
        }
    }
}
