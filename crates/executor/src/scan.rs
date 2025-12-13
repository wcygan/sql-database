//! Scan operators: SeqScan and IndexScan.

use crate::filter::eval_resolved_expr;
use crate::{ExecutionContext, Executor};
use btree::BTreeIndex;
use catalog::IndexId;
use common::{DbResult, ExecutionStats, PageId, RecordId, Row, TableId};
use planner::IndexPredicate;
use std::time::Instant;
use storage::HeapTable;
use types::Value;

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
    stats: ExecutionStats,
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
            stats: ExecutionStats::default(),
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
        let start = Instant::now();

        // Reset state
        self.current_page = PageId(0);
        self.current_slot = 0;
        self.num_pages = None;
        self.stats = ExecutionStats::default();

        self.stats.open_time = start.elapsed();
        Ok(())
    }

    fn next(&mut self, ctx: &mut ExecutionContext) -> DbResult<Option<Row>> {
        let start = Instant::now();
        let row = self.fetch_next_row(ctx)?;
        self.stats.total_next_time += start.elapsed();

        if row.is_some() {
            self.stats.rows_produced += 1;
        }

        // Track pages scanned (only when we have the num_pages computed)
        if let Some(num_pages) = self.num_pages {
            self.stats.pages_scanned = num_pages;
        }

        Ok(row)
    }

    fn close(&mut self, _ctx: &mut ExecutionContext) -> DbResult<()> {
        let start = Instant::now();
        // Nothing to clean up for seq scan
        self.stats.close_time = start.elapsed();
        Ok(())
    }

    fn schema(&self) -> &[String] {
        &self.schema
    }

    fn stats(&self) -> Option<&ExecutionStats> {
        Some(&self.stats)
    }
}

/// Index scan operator - uses B+Tree index to find rows efficiently.
///
/// Uses a B+Tree index to find matching RecordIds, then fetches the
/// actual rows from the heap table.
pub struct IndexScanExec {
    table_id: TableId,
    index_name: String,
    predicate: IndexPredicate,
    schema: Vec<String>,
    /// RecordIds matching the predicate (populated on open)
    matching_rids: Vec<RecordId>,
    /// Current position in the matching_rids vector
    cursor: usize,
    /// Execution statistics
    stats: ExecutionStats,
}

#[bon::bon]
impl IndexScanExec {
    /// Create a new index scan operator using a builder pattern.
    ///
    /// # Example
    /// ```ignore
    /// let scan = IndexScanExec::builder()
    ///     .table_id(TableId(1))
    ///     .index_name("idx_users_id".into())
    ///     .predicate(IndexPredicate::Eq { col: 0, value: expr })
    ///     .schema(vec!["id".into(), "name".into()])
    ///     .build();
    /// ```
    #[builder]
    pub fn new(
        table_id: TableId,
        index_name: String,
        predicate: IndexPredicate,
        schema: Vec<String>,
    ) -> Self {
        Self {
            table_id,
            index_name,
            predicate,
            schema,
            matching_rids: Vec::new(),
            cursor: 0,
            stats: ExecutionStats::default(),
        }
    }

    /// Find the index ID from the catalog.
    fn find_index_id(&self, ctx: &ExecutionContext) -> DbResult<IndexId> {
        let table_meta = ctx.catalog.table_by_id(self.table_id)?;
        let index_meta = table_meta.index(&self.index_name)?;
        Ok(index_meta.id)
    }

    /// Evaluate the predicate value to get the search key.
    fn eval_predicate_value(&self, pred: &planner::ResolvedExpr) -> DbResult<Value> {
        // For index lookups, we need a literal value
        // Evaluate against an empty row since we only support literals
        let empty_row = Row::new(Vec::new());
        eval_resolved_expr(pred, &empty_row)
    }

    /// Query the B+Tree index for matching RecordIds.
    fn query_index(&self, ctx: &ExecutionContext) -> DbResult<Vec<RecordId>> {
        let index_id = self.find_index_id(ctx)?;
        let index_path = ctx.data_dir.join(format!("index_{}.idx", index_id.0));

        // Check if index file exists
        if !index_path.exists() {
            return Err(common::DbError::Storage(format!(
                "index file not found: {}",
                index_path.display()
            )));
        }

        let mut btree = BTreeIndex::open(&index_path, index_id)?;

        match &self.predicate {
            IndexPredicate::Eq { value, .. } => {
                let key_value = self.eval_predicate_value(value)?;
                btree.search(&[key_value])
            }
            IndexPredicate::Range { low, high, .. } => {
                let low_key = self.eval_predicate_value(low)?;
                let high_key = self.eval_predicate_value(high)?;
                btree.range_scan(Some(&[low_key]), Some(&[high_key]))
            }
        }
    }
}

impl Executor for IndexScanExec {
    fn open(&mut self, ctx: &mut ExecutionContext) -> DbResult<()> {
        let start = Instant::now();

        // Reset state
        self.cursor = 0;
        self.stats = ExecutionStats::default();

        // Query the index for matching RecordIds
        self.matching_rids = self.query_index(ctx)?;

        self.stats.open_time = start.elapsed();
        Ok(())
    }

    fn next(&mut self, ctx: &mut ExecutionContext) -> DbResult<Option<Row>> {
        let start = Instant::now();

        if self.cursor >= self.matching_rids.len() {
            self.stats.total_next_time += start.elapsed();
            return Ok(None);
        }

        let rid = self.matching_rids[self.cursor];
        self.cursor += 1;

        // Fetch the actual row from the heap table
        let mut heap_table = ctx.heap_table(self.table_id)?;
        let row = heap_table.get(rid)?;

        self.stats.rows_produced += 1;
        self.stats.total_next_time += start.elapsed();

        Ok(Some(row))
    }

    fn close(&mut self, _ctx: &mut ExecutionContext) -> DbResult<()> {
        let start = Instant::now();
        self.matching_rids.clear();
        self.stats.close_time = start.elapsed();
        Ok(())
    }

    fn schema(&self) -> &[String] {
        &self.schema
    }

    fn stats(&self) -> Option<&ExecutionStats> {
        Some(&self.stats)
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
    use crate::tests::helpers::{
        assert_exhausted, assert_next_row, create_context_from_catalog, create_test_catalog,
        setup_test_catalog_and_dir, setup_test_context,
    };
    use catalog::Column;
    use planner::ResolvedExpr;
    use types::{SqlType, Value};

    fn insert_test_rows(
        ctx: &mut ExecutionContext,
        table_id: TableId,
        rows: Vec<Row>,
    ) -> DbResult<()> {
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
        let (mut ctx, _temp) = setup_test_context();
        let table_id = TableId(1);

        let mut scan = SeqScanExec::new(table_id, vec!["id".into(), "name".into()]);

        scan.open(&mut ctx).unwrap();
        assert_exhausted(&mut scan, &mut ctx);
        scan.close(&mut ctx).unwrap();
    }

    #[test]
    fn seq_scan_single_row() {
        let (mut ctx, _temp) = setup_test_context();
        let table_id = TableId(1);

        // Insert a row
        let rows = vec![Row::new(vec![
            Value::Int(1),
            Value::Text("alice".into()),
            Value::Bool(true),
        ])];
        insert_test_rows(&mut ctx, table_id, rows).unwrap();

        let mut scan =
            SeqScanExec::new(table_id, vec!["id".into(), "name".into(), "active".into()]);

        scan.open(&mut ctx).unwrap();
        assert_next_row(
            &mut scan,
            &mut ctx,
            Row::new(vec![
                Value::Int(1),
                Value::Text("alice".into()),
                Value::Bool(true),
            ]),
        );
        assert_exhausted(&mut scan, &mut ctx);
        scan.close(&mut ctx).unwrap();
    }

    #[test]
    fn seq_scan_multiple_rows() {
        let (mut ctx, _temp) = setup_test_context();
        let table_id = TableId(1);

        // Insert multiple rows
        let rows = vec![
            Row::new(vec![
                Value::Int(1),
                Value::Text("alice".into()),
                Value::Bool(true),
            ]),
            Row::new(vec![
                Value::Int(2),
                Value::Text("bob".into()),
                Value::Bool(false),
            ]),
            Row::new(vec![
                Value::Int(3),
                Value::Text("carol".into()),
                Value::Bool(true),
            ]),
        ];
        insert_test_rows(&mut ctx, table_id, rows).unwrap();

        let mut scan =
            SeqScanExec::new(table_id, vec!["id".into(), "name".into(), "active".into()]);

        scan.open(&mut ctx).unwrap();
        assert_next_row(
            &mut scan,
            &mut ctx,
            Row::new(vec![
                Value::Int(1),
                Value::Text("alice".into()),
                Value::Bool(true),
            ]),
        );
        assert_next_row(
            &mut scan,
            &mut ctx,
            Row::new(vec![
                Value::Int(2),
                Value::Text("bob".into()),
                Value::Bool(false),
            ]),
        );
        assert_next_row(
            &mut scan,
            &mut ctx,
            Row::new(vec![
                Value::Int(3),
                Value::Text("carol".into()),
                Value::Bool(true),
            ]),
        );
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
        let (mut ctx, _temp) = setup_test_context();
        let table_id = TableId(1);

        // Insert rows
        let rows = vec![
            Row::new(vec![
                Value::Int(1),
                Value::Text("alice".into()),
                Value::Bool(true),
            ]),
            Row::new(vec![
                Value::Int(2),
                Value::Text("bob".into()),
                Value::Bool(false),
            ]),
        ];
        insert_test_rows(&mut ctx, table_id, rows).unwrap();

        let mut scan =
            SeqScanExec::new(table_id, vec!["id".into(), "name".into(), "active".into()]);

        // First scan
        scan.open(&mut ctx).unwrap();
        assert_next_row(
            &mut scan,
            &mut ctx,
            Row::new(vec![
                Value::Int(1),
                Value::Text("alice".into()),
                Value::Bool(true),
            ]),
        );

        // Reset with open
        scan.open(&mut ctx).unwrap();
        assert_next_row(
            &mut scan,
            &mut ctx,
            Row::new(vec![
                Value::Int(1),
                Value::Text("alice".into()),
                Value::Bool(true),
            ]),
        );
        assert_next_row(
            &mut scan,
            &mut ctx,
            Row::new(vec![
                Value::Int(2),
                Value::Text("bob".into()),
                Value::Bool(false),
            ]),
        );
        assert_exhausted(&mut scan, &mut ctx);

        scan.close(&mut ctx).unwrap();
    }

    #[test]
    fn seq_scan_close_succeeds() {
        let (mut ctx, _temp) = setup_test_context();
        let table_id = TableId(1);

        let mut scan = SeqScanExec::new(table_id, vec!["id".into()]);

        scan.open(&mut ctx).unwrap();
        assert!(scan.close(&mut ctx).is_ok());
    }

    #[test]
    fn index_scan_requires_existing_index() {
        let (mut ctx, _temp) = setup_test_context();
        let table_id = TableId(1);

        // Insert rows
        let rows = vec![Row::new(vec![
            Value::Int(1),
            Value::Text("alice".into()),
            Value::Bool(true),
        ])];
        insert_test_rows(&mut ctx, table_id, rows).unwrap();

        // Try to use a non-existent index
        let mut scan = IndexScanExec::builder()
            .table_id(table_id)
            .index_name("idx_nonexistent".into())
            .predicate(IndexPredicate::Eq {
                col: 0,
                value: ResolvedExpr::Literal(Value::Int(1)),
            })
            .schema(vec!["id".into(), "name".into(), "active".into()])
            .build();

        // Should fail because index doesn't exist
        let result = scan.open(&mut ctx);
        assert!(result.is_err(), "expected error for non-existent index");
    }

    #[test]
    fn index_scan_schema_matches() {
        let table_id = TableId(1);

        let scan = IndexScanExec::builder()
            .table_id(table_id)
            .index_name("idx_users_id".into())
            .predicate(IndexPredicate::Eq {
                col: 0,
                value: ResolvedExpr::Literal(Value::Int(1)),
            })
            .schema(vec!["id".into(), "name".into()])
            .build();

        assert_eq!(scan.schema(), &["id", "name"]);
    }

    #[test]
    fn index_scan_with_btree_index() {
        let (catalog, temp) = setup_test_catalog_and_dir();
        let table_id = TableId(1);

        // Create an index on the "id" column BEFORE creating context
        catalog
            .create_index()
            .table_name("users")
            .index_name("idx_users_id")
            .columns(&["id"])
            .kind(catalog::IndexKind::BTree)
            .call()
            .unwrap();

        // Build the empty index file first
        let index_id = catalog
            .table("users")
            .unwrap()
            .index("idx_users_id")
            .unwrap()
            .id;
        let index_path = temp.path().join(format!("index_{}.idx", index_id.0));
        let mut btree = btree::BTreeIndex::create(&index_path, index_id).unwrap();

        // Now create the context
        let mut ctx = create_context_from_catalog(catalog, &temp);

        // Insert rows
        let rows = vec![
            Row::new(vec![
                Value::Int(1),
                Value::Text("alice".into()),
                Value::Bool(true),
            ]),
            Row::new(vec![
                Value::Int(2),
                Value::Text("bob".into()),
                Value::Bool(false),
            ]),
            Row::new(vec![
                Value::Int(3),
                Value::Text("carol".into()),
                Value::Bool(true),
            ]),
        ];
        insert_test_rows(&mut ctx, table_id, rows).unwrap();

        // Scan heap and add entries to index
        let heap_path = temp.path().join("users.heap");
        let mut heap = storage::HeapFile::open(&heap_path, table_id.0).unwrap();
        for page_id in 0..10u64 {
            for slot in 0..100u16 {
                let rid = common::RecordId {
                    page_id: common::PageId(page_id),
                    slot,
                };
                if let Ok(row) = heap.get(rid) {
                    let key = vec![row.values[0].clone()]; // "id" column
                    btree.insert(key, rid).unwrap();
                }
            }
        }
        btree.flush().unwrap();

        // Now test the IndexScanExec
        let mut scan = IndexScanExec::builder()
            .table_id(table_id)
            .index_name("idx_users_id".into())
            .predicate(IndexPredicate::Eq {
                col: 0,
                value: ResolvedExpr::Literal(Value::Int(2)),
            })
            .schema(vec!["id".into(), "name".into(), "active".into()])
            .build();

        scan.open(&mut ctx).unwrap();
        // Should return only the row with id=2
        assert_next_row(
            &mut scan,
            &mut ctx,
            Row::new(vec![
                Value::Int(2),
                Value::Text("bob".into()),
                Value::Bool(false),
            ]),
        );
        assert_exhausted(&mut scan, &mut ctx);
        scan.close(&mut ctx).unwrap();
    }

    #[test]
    fn index_scan_range_with_btree() {
        let (catalog, temp) = setup_test_catalog_and_dir();
        let table_id = TableId(1);

        // Create an index on the "id" column
        catalog
            .create_index()
            .table_name("users")
            .index_name("idx_users_id")
            .columns(&["id"])
            .kind(catalog::IndexKind::BTree)
            .call()
            .unwrap();

        // Build the empty index file
        let index_id = catalog
            .table("users")
            .unwrap()
            .index("idx_users_id")
            .unwrap()
            .id;
        let index_path = temp.path().join(format!("index_{}.idx", index_id.0));
        let mut btree = btree::BTreeIndex::create(&index_path, index_id).unwrap();

        // Now create context
        let mut ctx = create_context_from_catalog(catalog, &temp);

        // Insert rows
        let rows = vec![
            Row::new(vec![
                Value::Int(1),
                Value::Text("alice".into()),
                Value::Bool(true),
            ]),
            Row::new(vec![
                Value::Int(2),
                Value::Text("bob".into()),
                Value::Bool(false),
            ]),
            Row::new(vec![
                Value::Int(3),
                Value::Text("carol".into()),
                Value::Bool(true),
            ]),
            Row::new(vec![
                Value::Int(4),
                Value::Text("dave".into()),
                Value::Bool(false),
            ]),
        ];
        insert_test_rows(&mut ctx, table_id, rows).unwrap();

        // Scan heap and add entries to index
        let heap_path = temp.path().join("users.heap");
        let mut heap = storage::HeapFile::open(&heap_path, table_id.0).unwrap();
        for page_id in 0..10u64 {
            for slot in 0..100u16 {
                let rid = common::RecordId {
                    page_id: common::PageId(page_id),
                    slot,
                };
                if let Ok(row) = heap.get(rid) {
                    let key = vec![row.values[0].clone()];
                    btree.insert(key, rid).unwrap();
                }
            }
        }
        btree.flush().unwrap();

        // Test range scan [2, 3]
        let mut scan = IndexScanExec::builder()
            .table_id(table_id)
            .index_name("idx_users_id".into())
            .predicate(IndexPredicate::Range {
                col: 0,
                low: ResolvedExpr::Literal(Value::Int(2)),
                high: ResolvedExpr::Literal(Value::Int(3)),
            })
            .schema(vec!["id".into(), "name".into(), "active".into()])
            .build();

        scan.open(&mut ctx).unwrap();
        // Should return rows with id in [2, 3]
        assert_next_row(
            &mut scan,
            &mut ctx,
            Row::new(vec![
                Value::Int(2),
                Value::Text("bob".into()),
                Value::Bool(false),
            ]),
        );
        assert_next_row(
            &mut scan,
            &mut ctx,
            Row::new(vec![
                Value::Int(3),
                Value::Text("carol".into()),
                Value::Bool(true),
            ]),
        );
        assert_exhausted(&mut scan, &mut ctx);
        scan.close(&mut ctx).unwrap();
    }

    #[test]
    fn seq_scan_unknown_table_returns_error() {
        let (mut ctx, _temp) = setup_test_context();
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
    fn index_scan_empty_table_with_index() {
        let (catalog, temp) = setup_test_catalog_and_dir();
        let table_id = TableId(1);

        // Create an index on the empty table
        catalog
            .create_index()
            .table_name("users")
            .index_name("idx_users_id")
            .columns(&["id"])
            .kind(catalog::IndexKind::BTree)
            .call()
            .unwrap();

        // Build an empty index file
        let index_id = catalog
            .table("users")
            .unwrap()
            .index("idx_users_id")
            .unwrap()
            .id;
        let index_path = temp.path().join(format!("index_{}.idx", index_id.0));
        let mut btree = btree::BTreeIndex::create(&index_path, index_id).unwrap();
        btree.flush().unwrap();

        let mut ctx = create_context_from_catalog(catalog, &temp);

        let mut scan = IndexScanExec::builder()
            .table_id(table_id)
            .index_name("idx_users_id".into())
            .predicate(IndexPredicate::Eq {
                col: 0,
                value: ResolvedExpr::Literal(Value::Int(1)),
            })
            .schema(vec!["id".into()])
            .build();

        scan.open(&mut ctx).unwrap();
        assert_exhausted(&mut scan, &mut ctx);
        scan.close(&mut ctx).unwrap();
    }

    #[test]
    fn index_scan_open_resets_state_with_index() {
        let (catalog, temp) = setup_test_catalog_and_dir();
        let table_id = TableId(1);

        // Create an index on the "id" column first (before inserting rows)
        catalog
            .create_index()
            .table_name("users")
            .index_name("idx_users_id")
            .columns(&["id"])
            .kind(catalog::IndexKind::BTree)
            .call()
            .unwrap();

        // Get index metadata
        let index_id = catalog
            .table("users")
            .unwrap()
            .index("idx_users_id")
            .unwrap()
            .id;
        let index_path = temp.path().join(format!("index_{}.idx", index_id.0));

        // Create context (catalog becomes immutable after this)
        let mut ctx = create_context_from_catalog(catalog, &temp);

        // Insert rows
        let rows = vec![
            Row::new(vec![
                Value::Int(1),
                Value::Text("alice".into()),
                Value::Bool(true),
            ]),
            Row::new(vec![
                Value::Int(2),
                Value::Text("bob".into()),
                Value::Bool(false),
            ]),
        ];
        insert_test_rows(&mut ctx, table_id, rows).unwrap();

        // Build the index file by scanning the heap
        let mut btree = btree::BTreeIndex::create(&index_path, index_id).unwrap();
        let heap_path = temp.path().join("users.heap");
        let mut heap = storage::HeapFile::open(&heap_path, table_id.0).unwrap();
        for page_id in 0..10u64 {
            for slot in 0..100u16 {
                let rid = common::RecordId {
                    page_id: common::PageId(page_id),
                    slot,
                };
                if let Ok(row) = heap.get(rid) {
                    let key = vec![row.values[0].clone()];
                    btree.insert(key, rid).unwrap();
                }
            }
        }
        btree.flush().unwrap();

        let mut scan = IndexScanExec::builder()
            .table_id(table_id)
            .index_name("idx_users_id".into())
            .predicate(IndexPredicate::Eq {
                col: 0,
                value: ResolvedExpr::Literal(Value::Int(1)),
            })
            .schema(vec!["id".into(), "name".into(), "active".into()])
            .build();

        // First scan
        scan.open(&mut ctx).unwrap();
        assert_next_row(
            &mut scan,
            &mut ctx,
            Row::new(vec![
                Value::Int(1),
                Value::Text("alice".into()),
                Value::Bool(true),
            ]),
        );
        assert_exhausted(&mut scan, &mut ctx);

        // Reset with open should reset cursor
        scan.open(&mut ctx).unwrap();
        assert_next_row(
            &mut scan,
            &mut ctx,
            Row::new(vec![
                Value::Int(1),
                Value::Text("alice".into()),
                Value::Bool(true),
            ]),
        );
        assert_exhausted(&mut scan, &mut ctx);

        scan.close(&mut ctx).unwrap();
    }

    #[test]
    fn seq_scan_single_column_table() {
        let (_ctx, _temp) = setup_test_context();

        // Create table with single column
        let temp_dir = tempfile::tempdir().unwrap();
        let mut catalog = create_test_catalog();
        catalog
            .create_table("numbers", vec![Column::new("value", SqlType::Int)], None)
            .unwrap();
        let table_id = catalog.table("numbers").unwrap().id;

        // Leak for 'static lifetime
        let catalog = Box::leak(Box::new(catalog));
        let pager = Box::leak(Box::new(buffer::FilePager::new(temp_dir.path(), 10)));
        let wal = Box::leak(Box::new(
            wal::Wal::open(temp_dir.path().join("test.wal")).unwrap(),
        ));

        let mut ctx = ExecutionContext::new(catalog, pager, wal, temp_dir.path().into());

        // Insert rows
        let rows = vec![
            Row::new(vec![Value::Int(10)]),
            Row::new(vec![Value::Int(20)]),
        ];
        insert_test_rows(&mut ctx, table_id, rows).unwrap();

        let mut scan = SeqScanExec::new(table_id, vec!["value".into()]);

        scan.open(&mut ctx).unwrap();
        assert_next_row(&mut scan, &mut ctx, Row::new(vec![Value::Int(10)]));
        assert_next_row(&mut scan, &mut ctx, Row::new(vec![Value::Int(20)]));
        assert_exhausted(&mut scan, &mut ctx);
        scan.close(&mut ctx).unwrap();
    }
}
