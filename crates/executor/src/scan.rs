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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::helpers::{assert_exhausted, assert_next_row, create_test_catalog};
    use catalog::Column;
    use planner::ResolvedExpr;
    use types::{SqlType, Value};

    fn setup_context() -> (ExecutionContext<'static>, tempfile::TempDir) {
        let temp_dir = tempfile::tempdir().unwrap();
        let catalog = create_test_catalog();

        // Leak resources for 'static lifetime (test-only pattern)
        let catalog = Box::leak(Box::new(catalog));
        let pager = Box::leak(Box::new(buffer::FilePager::new(temp_dir.path(), 10)));
        let wal = Box::leak(Box::new(wal::Wal::open(temp_dir.path().join("test.wal")).unwrap()));

        let ctx = ExecutionContext::new(catalog, pager, wal, temp_dir.path().into());
        (ctx, temp_dir)
    }

    fn insert_test_rows(ctx: &mut ExecutionContext, table_id: TableId, rows: Vec<Row>) -> DbResult<()> {
        let table_meta = ctx.catalog.table_by_id(table_id)?;
        let file_path = ctx.data_dir.join(format!("{}.heap", table_meta.name));

        let mut heap_table = storage::HeapFile::open(&file_path, table_id.0)?;

        for row in rows {
            heap_table.insert(&row)?;
        }

        Ok(())
    }

    #[test]
    fn seq_scan_empty_table() {
        let (mut ctx, _temp) = setup_context();
        let table_id = TableId(1);

        let mut scan = SeqScanExec::new(table_id, vec!["id".into(), "name".into()]);

        scan.open(&mut ctx).unwrap();
        assert_exhausted(&mut scan, &mut ctx);
        scan.close(&mut ctx).unwrap();
    }

    #[test]
    fn seq_scan_single_row() {
        let (mut ctx, _temp) = setup_context();
        let table_id = TableId(1);

        // Insert a row
        let rows = vec![Row(vec![Value::Int(1), Value::Text("alice".into()), Value::Bool(true)])];
        insert_test_rows(&mut ctx, table_id, rows).unwrap();

        let mut scan = SeqScanExec::new(table_id, vec!["id".into(), "name".into(), "active".into()]);

        scan.open(&mut ctx).unwrap();
        assert_next_row(&mut scan, &mut ctx, Row(vec![Value::Int(1), Value::Text("alice".into()), Value::Bool(true)]));
        assert_exhausted(&mut scan, &mut ctx);
        scan.close(&mut ctx).unwrap();
    }

    #[test]
    fn seq_scan_multiple_rows() {
        let (mut ctx, _temp) = setup_context();
        let table_id = TableId(1);

        // Insert multiple rows
        let rows = vec![
            Row(vec![Value::Int(1), Value::Text("alice".into()), Value::Bool(true)]),
            Row(vec![Value::Int(2), Value::Text("bob".into()), Value::Bool(false)]),
            Row(vec![Value::Int(3), Value::Text("carol".into()), Value::Bool(true)]),
        ];
        insert_test_rows(&mut ctx, table_id, rows).unwrap();

        let mut scan = SeqScanExec::new(table_id, vec!["id".into(), "name".into(), "active".into()]);

        scan.open(&mut ctx).unwrap();
        assert_next_row(&mut scan, &mut ctx, Row(vec![Value::Int(1), Value::Text("alice".into()), Value::Bool(true)]));
        assert_next_row(&mut scan, &mut ctx, Row(vec![Value::Int(2), Value::Text("bob".into()), Value::Bool(false)]));
        assert_next_row(&mut scan, &mut ctx, Row(vec![Value::Int(3), Value::Text("carol".into()), Value::Bool(true)]));
        assert_exhausted(&mut scan, &mut ctx);
        scan.close(&mut ctx).unwrap();
    }

    #[test]
    fn seq_scan_schema_matches() {
        let table_id = TableId(1);

        let scan = SeqScanExec::new(table_id, vec!["id".into(), "name".into()]);

        assert_eq!(scan.schema(), &["id", "name"]);
    }

    #[test]
    fn seq_scan_open_resets_state() {
        let (mut ctx, _temp) = setup_context();
        let table_id = TableId(1);

        // Insert rows
        let rows = vec![
            Row(vec![Value::Int(1), Value::Text("alice".into()), Value::Bool(true)]),
            Row(vec![Value::Int(2), Value::Text("bob".into()), Value::Bool(false)]),
        ];
        insert_test_rows(&mut ctx, table_id, rows).unwrap();

        let mut scan = SeqScanExec::new(table_id, vec!["id".into(), "name".into(), "active".into()]);

        // First scan
        scan.open(&mut ctx).unwrap();
        assert_next_row(&mut scan, &mut ctx, Row(vec![Value::Int(1), Value::Text("alice".into()), Value::Bool(true)]));

        // Reset with open
        scan.open(&mut ctx).unwrap();
        assert_next_row(&mut scan, &mut ctx, Row(vec![Value::Int(1), Value::Text("alice".into()), Value::Bool(true)]));
        assert_next_row(&mut scan, &mut ctx, Row(vec![Value::Int(2), Value::Text("bob".into()), Value::Bool(false)]));
        assert_exhausted(&mut scan, &mut ctx);

        scan.close(&mut ctx).unwrap();
    }

    #[test]
    fn seq_scan_close_succeeds() {
        let (mut ctx, _temp) = setup_context();
        let table_id = TableId(1);

        let mut scan = SeqScanExec::new(table_id, vec!["id".into()]);

        scan.open(&mut ctx).unwrap();
        assert!(scan.close(&mut ctx).is_ok());
    }

    #[test]
    fn index_scan_delegates_to_seq_scan() {
        let (mut ctx, _temp) = setup_context();
        let table_id = TableId(1);

        // Insert rows
        let rows = vec![
            Row(vec![Value::Int(1), Value::Text("alice".into()), Value::Bool(true)]),
            Row(vec![Value::Int(2), Value::Text("bob".into()), Value::Bool(false)]),
        ];
        insert_test_rows(&mut ctx, table_id, rows).unwrap();

        let mut scan = IndexScanExec::new(
            table_id,
            "idx_users_id".into(),
            IndexPredicate::Eq {
                col: 0,
                value: ResolvedExpr::Literal(Value::Int(1)),
            },
            vec!["id".into(), "name".into(), "active".into()],
        );

        scan.open(&mut ctx).unwrap();
        // Should still return all rows (stub implementation uses SeqScan)
        assert_next_row(&mut scan, &mut ctx, Row(vec![Value::Int(1), Value::Text("alice".into()), Value::Bool(true)]));
        assert_next_row(&mut scan, &mut ctx, Row(vec![Value::Int(2), Value::Text("bob".into()), Value::Bool(false)]));
        assert_exhausted(&mut scan, &mut ctx);
        scan.close(&mut ctx).unwrap();
    }

    #[test]
    fn index_scan_schema_matches() {
        let table_id = TableId(1);

        let scan = IndexScanExec::new(
            table_id,
            "idx_users_id".into(),
            IndexPredicate::Eq {
                col: 0,
                value: ResolvedExpr::Literal(Value::Int(1)),
            },
            vec!["id".into(), "name".into()],
        );

        assert_eq!(scan.schema(), &["id", "name"]);
    }

    #[test]
    fn index_scan_open_succeeds() {
        let (mut ctx, _temp) = setup_context();
        let table_id = TableId(1);

        let mut scan = IndexScanExec::new(
            table_id,
            "idx_users_id".into(),
            IndexPredicate::Eq {
                col: 0,
                value: ResolvedExpr::Literal(Value::Int(1)),
            },
            vec!["id".into()],
        );

        assert!(scan.open(&mut ctx).is_ok());
        scan.close(&mut ctx).unwrap();
    }

    #[test]
    fn index_scan_close_succeeds() {
        let (mut ctx, _temp) = setup_context();
        let table_id = TableId(1);

        let mut scan = IndexScanExec::new(
            table_id,
            "idx_users_id".into(),
            IndexPredicate::Eq {
                col: 0,
                value: ResolvedExpr::Literal(Value::Int(1)),
            },
            vec!["id".into()],
        );

        scan.open(&mut ctx).unwrap();
        assert!(scan.close(&mut ctx).is_ok());
    }

    #[test]
    fn index_scan_with_range_predicate() {
        let (mut ctx, _temp) = setup_context();
        let table_id = TableId(1);

        // Insert rows
        let rows = vec![
            Row(vec![Value::Int(1), Value::Text("alice".into()), Value::Bool(true)]),
            Row(vec![Value::Int(2), Value::Text("bob".into()), Value::Bool(false)]),
            Row(vec![Value::Int(3), Value::Text("carol".into()), Value::Bool(true)]),
        ];
        insert_test_rows(&mut ctx, table_id, rows).unwrap();

        let mut scan = IndexScanExec::new(
            table_id,
            "idx_users_id".into(),
            IndexPredicate::Range {
                col: 0,
                low: ResolvedExpr::Literal(Value::Int(1)),
                high: ResolvedExpr::Literal(Value::Int(3)),
            },
            vec!["id".into(), "name".into(), "active".into()],
        );

        scan.open(&mut ctx).unwrap();
        // Should return all rows (stub implementation uses SeqScan)
        assert_next_row(&mut scan, &mut ctx, Row(vec![Value::Int(1), Value::Text("alice".into()), Value::Bool(true)]));
        assert_next_row(&mut scan, &mut ctx, Row(vec![Value::Int(2), Value::Text("bob".into()), Value::Bool(false)]));
        assert_next_row(&mut scan, &mut ctx, Row(vec![Value::Int(3), Value::Text("carol".into()), Value::Bool(true)]));
        assert_exhausted(&mut scan, &mut ctx);
        scan.close(&mut ctx).unwrap();
    }

    #[test]
    fn seq_scan_unknown_table_returns_error() {
        let (mut ctx, _temp) = setup_context();
        let table_id = TableId(999); // Non-existent table

        let mut scan = SeqScanExec::new(table_id, vec!["id".into()]);

        scan.open(&mut ctx).unwrap();
        let result = scan.next(&mut ctx);
        assert!(result.is_err());
    }

    #[test]
    fn seq_scan_empty_schema() {
        let table_id = TableId(1);

        let scan = SeqScanExec::new(table_id, vec![]);
        assert_eq!(scan.schema().len(), 0);
    }

    #[test]
    fn index_scan_empty_table() {
        let (mut ctx, _temp) = setup_context();
        let table_id = TableId(1);

        let mut scan = IndexScanExec::new(
            table_id,
            "idx_users_id".into(),
            IndexPredicate::Eq {
                col: 0,
                value: ResolvedExpr::Literal(Value::Int(1)),
            },
            vec!["id".into()],
        );

        scan.open(&mut ctx).unwrap();
        assert_exhausted(&mut scan, &mut ctx);
        scan.close(&mut ctx).unwrap();
    }

    #[test]
    fn index_scan_open_resets_state() {
        let (mut ctx, _temp) = setup_context();
        let table_id = TableId(1);

        // Insert rows
        let rows = vec![
            Row(vec![Value::Int(1), Value::Text("alice".into()), Value::Bool(true)]),
            Row(vec![Value::Int(2), Value::Text("bob".into()), Value::Bool(false)]),
        ];
        insert_test_rows(&mut ctx, table_id, rows).unwrap();

        let mut scan = IndexScanExec::new(
            table_id,
            "idx_users_id".into(),
            IndexPredicate::Eq {
                col: 0,
                value: ResolvedExpr::Literal(Value::Int(1)),
            },
            vec!["id".into(), "name".into(), "active".into()],
        );

        // First scan
        scan.open(&mut ctx).unwrap();
        assert_next_row(&mut scan, &mut ctx, Row(vec![Value::Int(1), Value::Text("alice".into()), Value::Bool(true)]));

        // Reset with open
        scan.open(&mut ctx).unwrap();
        assert_next_row(&mut scan, &mut ctx, Row(vec![Value::Int(1), Value::Text("alice".into()), Value::Bool(true)]));
        assert_next_row(&mut scan, &mut ctx, Row(vec![Value::Int(2), Value::Text("bob".into()), Value::Bool(false)]));
        assert_exhausted(&mut scan, &mut ctx);

        scan.close(&mut ctx).unwrap();
    }

    #[test]
    fn seq_scan_single_column_table() {
        let (mut ctx, _temp) = setup_context();

        // Create table with single column
        let temp_dir = tempfile::tempdir().unwrap();
        let mut catalog = create_test_catalog();
        catalog.create_table("numbers", vec![Column::new("value", SqlType::Int)]).unwrap();
        let table_id = catalog.table("numbers").unwrap().id;

        // Leak for 'static lifetime
        let catalog = Box::leak(Box::new(catalog));
        let pager = Box::leak(Box::new(buffer::FilePager::new(temp_dir.path(), 10)));
        let wal = Box::leak(Box::new(wal::Wal::open(temp_dir.path().join("test.wal")).unwrap()));

        let mut ctx = ExecutionContext::new(catalog, pager, wal, temp_dir.path().into());

        // Insert rows
        let rows = vec![
            Row(vec![Value::Int(10)]),
            Row(vec![Value::Int(20)]),
        ];
        insert_test_rows(&mut ctx, table_id, rows).unwrap();

        let mut scan = SeqScanExec::new(table_id, vec!["value".into()]);

        scan.open(&mut ctx).unwrap();
        assert_next_row(&mut scan, &mut ctx, Row(vec![Value::Int(10)]));
        assert_next_row(&mut scan, &mut ctx, Row(vec![Value::Int(20)]));
        assert_exhausted(&mut scan, &mut ctx);
        scan.close(&mut ctx).unwrap();
    }
}
