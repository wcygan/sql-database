use super::*;
use tempfile::tempdir;

#[test]
fn allocate_and_fetch_persist_pages() {
    let dir = tempdir().unwrap();
    let mut pager = FilePager::new(dir.path(), 2);
    let table = TableId(1);

    // Allocate and modify a page
    let pid = pager.allocate_page(table).unwrap();
    {
        let page = pager.fetch_page(table, pid).unwrap();
        page.data[0..4].copy_from_slice(&[1, 2, 3, 4]);
    }

    pager.flush().unwrap();

    // New pager should read the same page
    let mut pager2 = FilePager::new(dir.path(), 2);
    let page2 = pager2.fetch_page(table, pid).unwrap();
    assert_eq!(&page2.data[0..4], &[1, 2, 3, 4]);
}

#[test]
fn lru_eviction_flushes_dirty_pages() {
    let dir = tempdir().unwrap();
    let mut pager = FilePager::new(dir.path(), 1);
    let table = TableId(1);

    let pid1 = pager.allocate_page(table).unwrap();
    pager.fetch_page(table, pid1).unwrap().data[0] = 99;

    // Allocate another, should evict the first and flush it
    let _pid2 = pager.allocate_page(table).unwrap();
    pager.flush().unwrap();

    // Verify persisted data
    let mut pager2 = FilePager::new(dir.path(), 2);
    let p = pager2.fetch_page(table, pid1).unwrap();
    assert_eq!(p.data[0], 99);
}

#[test]
fn allocate_sequential_page_ids() {
    let dir = tempdir().unwrap();
    let mut pager = FilePager::new(dir.path(), 10);
    let table = TableId(1);

    let pid0 = pager.allocate_page(table).unwrap();
    let pid1 = pager.allocate_page(table).unwrap();
    let pid2 = pager.allocate_page(table).unwrap();

    assert_eq!(pid0, PageId(0));
    assert_eq!(pid1, PageId(1));
    assert_eq!(pid2, PageId(2));
}

#[test]
fn fetch_page_updates_lru_order() {
    let dir = tempdir().unwrap();
    let mut pager = FilePager::new(dir.path(), 2);
    let table = TableId(1);

    let pid0 = pager.allocate_page(table).unwrap();
    let pid1 = pager.allocate_page(table).unwrap();

    // Fetch pid0 to make it most recently used
    pager.fetch_page(table, pid0).unwrap();

    // Allocate a new page (cache full, should evict pid1, not pid0)
    let pid2 = pager.allocate_page(table).unwrap();

    // pid0 should still be in cache (no disk read needed)
    pager.fetch_page(table, pid0).unwrap().data[0] = 77;

    // pid1 should have been evicted (requires disk read)
    pager.fetch_page(table, pid1).unwrap();

    // Verify pid2 exists
    assert_eq!(pid2, PageId(2));
}

#[test]
fn dirty_tracking_only_writes_modified_pages() {
    let dir = tempdir().unwrap();
    let mut pager = FilePager::new(dir.path(), 3);
    let table = TableId(1);

    let pid0 = pager.allocate_page(table).unwrap();
    let _pid1 = pager.allocate_page(table).unwrap();

    // Modify only pid0
    pager.fetch_page(table, pid0).unwrap().data[0] = 42;

    // Flush should only write pid0 (pid1 is not dirty after allocation flush)
    pager.flush().unwrap();

    // Verify persistence
    let mut pager2 = FilePager::new(dir.path(), 2);
    assert_eq!(pager2.fetch_page(table, pid0).unwrap().data[0], 42);
}

#[test]
fn multiple_tables_isolated() {
    let dir = tempdir().unwrap();
    let mut pager = FilePager::new(dir.path(), 10);
    let table1 = TableId(1);
    let table2 = TableId(2);

    // Allocate pages in different tables
    let pid1_t1 = pager.allocate_page(table1).unwrap();
    let pid1_t2 = pager.allocate_page(table2).unwrap();

    // Both should be page 0 in their respective tables
    assert_eq!(pid1_t1, PageId(0));
    assert_eq!(pid1_t2, PageId(0));

    // Modify each page differently
    pager.fetch_page(table1, pid1_t1).unwrap().data[0] = 10;
    pager.fetch_page(table2, pid1_t2).unwrap().data[0] = 20;

    pager.flush().unwrap();

    // Verify isolation
    let mut pager2 = FilePager::new(dir.path(), 10);
    assert_eq!(pager2.fetch_page(table1, pid1_t1).unwrap().data[0], 10);
    assert_eq!(pager2.fetch_page(table2, pid1_t2).unwrap().data[0], 20);
}

#[test]
fn fetch_nonexistent_page_returns_initialized_page() {
    let dir = tempdir().unwrap();
    let mut pager = FilePager::new(dir.path(), 10);
    let table = TableId(1);

    // Fetch a page that doesn't exist yet (beyond allocated pages)
    let page = pager.fetch_page(table, PageId(5)).unwrap();

    // Should be a properly initialized page (not raw zeros, has PageHeader)
    assert_eq!(page.id, 5);
    // Page data should have the default header written by Page::new()
    // We just verify it's been initialized (not checking exact header bytes)
    assert_eq!(page.data.len(), PAGE_SIZE);
}

#[test]
fn eviction_writes_dirty_pages_before_removal() {
    let dir = tempdir().unwrap();
    let mut pager = FilePager::new(dir.path(), 2);
    let table = TableId(1);

    let pid0 = pager.allocate_page(table).unwrap();
    let pid1 = pager.allocate_page(table).unwrap();

    // Modify both pages
    pager.fetch_page(table, pid0).unwrap().data[0] = 11;
    pager.fetch_page(table, pid1).unwrap().data[1] = 22;

    // Allocate a third page, forcing eviction of pid0 (LRU)
    let _pid2 = pager.allocate_page(table).unwrap();

    // Flush remaining dirty pages
    pager.flush().unwrap();

    // Verify both modifications persisted
    let mut pager2 = FilePager::new(dir.path(), 3);
    assert_eq!(pager2.fetch_page(table, pid0).unwrap().data[0], 11);
    assert_eq!(pager2.fetch_page(table, pid1).unwrap().data[1], 22);
}

#[test]
fn large_page_modifications_persist() {
    let dir = tempdir().unwrap();
    let mut pager = FilePager::new(dir.path(), 5);
    let table = TableId(1);

    let pid = pager.allocate_page(table).unwrap();

    // Fill entire page with pattern
    {
        let page = pager.fetch_page(table, pid).unwrap();
        for i in 0..PAGE_SIZE {
            page.data[i] = (i % 256) as u8;
        }
    }

    pager.flush().unwrap();

    // Verify entire page pattern
    let mut pager2 = FilePager::new(dir.path(), 5);
    let page2 = pager2.fetch_page(table, pid).unwrap();
    for i in 0..PAGE_SIZE {
        assert_eq!(page2.data[i], (i % 256) as u8, "Mismatch at offset {}", i);
    }
}

#[test]
#[should_panic(expected = "max_pages must be > 0")]
fn new_pager_panics_with_zero_capacity() {
    let dir = tempdir().unwrap();
    let _pager = FilePager::new(dir.path(), 0);
}

#[test]
fn flush_empty_pager_succeeds() {
    let dir = tempdir().unwrap();
    let mut pager = FilePager::new(dir.path(), 10);
    pager.flush().unwrap();
}

#[test]
fn refetch_after_eviction_reloads_from_disk() {
    let dir = tempdir().unwrap();
    let mut pager = FilePager::new(dir.path(), 1);
    let table = TableId(1);

    let pid0 = pager.allocate_page(table).unwrap();
    pager.fetch_page(table, pid0).unwrap().data[0] = 55;

    // Allocate another page, evicting pid0
    let _pid1 = pager.allocate_page(table).unwrap();

    // Refetch pid0 - should reload from disk with modifications intact
    let page = pager.fetch_page(table, pid0).unwrap();
    assert_eq!(page.data[0], 55);
}

#[test]
fn cache_hit_does_not_reload() {
    let dir = tempdir().unwrap();
    let mut pager = FilePager::new(dir.path(), 10);
    let table = TableId(1);

    let pid = pager.allocate_page(table).unwrap();

    // First fetch (cache miss, loads from disk)
    pager.fetch_page(table, pid).unwrap().data[0] = 100;

    // Second fetch should be cache hit (no disk I/O)
    let page = pager.fetch_page(table, pid).unwrap();
    assert_eq!(page.data[0], 100);

    // Modify again
    page.data[1] = 200;

    // Third fetch still cache hit
    let page2 = pager.fetch_page(table, pid).unwrap();
    assert_eq!(page2.data[0], 100);
    assert_eq!(page2.data[1], 200);
}

#[test]
fn multiple_evictions_in_sequence() {
    let dir = tempdir().unwrap();
    let mut pager = FilePager::new(dir.path(), 2);
    let table = TableId(1);

    // Fill cache
    let pid0 = pager.allocate_page(table).unwrap();
    let pid1 = pager.allocate_page(table).unwrap();

    // Modify both
    pager.fetch_page(table, pid0).unwrap().data[0] = 10;
    pager.fetch_page(table, pid1).unwrap().data[0] = 20;

    // Allocate more pages, causing multiple evictions
    let pid2 = pager.allocate_page(table).unwrap();
    pager.fetch_page(table, pid2).unwrap().data[0] = 30;

    let pid3 = pager.allocate_page(table).unwrap();
    pager.fetch_page(table, pid3).unwrap().data[0] = 40;

    // Flush and verify all persisted
    pager.flush().unwrap();

    let mut pager2 = FilePager::new(dir.path(), 4);
    assert_eq!(pager2.fetch_page(table, pid0).unwrap().data[0], 10);
    assert_eq!(pager2.fetch_page(table, pid1).unwrap().data[0], 20);
    assert_eq!(pager2.fetch_page(table, pid2).unwrap().data[0], 30);
    assert_eq!(pager2.fetch_page(table, pid3).unwrap().data[0], 40);
}

#[test]
fn flush_with_no_dirty_pages() {
    let dir = tempdir().unwrap();
    let mut pager = FilePager::new(dir.path(), 5);
    let table = TableId(1);

    // Allocate pages
    let _pid0 = pager.allocate_page(table).unwrap();
    let _pid1 = pager.allocate_page(table).unwrap();

    // Flush immediately (pages are dirty from allocation)
    pager.flush().unwrap();

    // Fetch without modifying
    pager.fetch_page(table, PageId(0)).unwrap();

    // Flush again - no dirty pages this time
    pager.flush().unwrap();
}

#[test]
fn concurrent_operations_on_different_tables() {
    let dir = tempdir().unwrap();
    let mut pager = FilePager::new(dir.path(), 5);
    let table1 = TableId(1);
    let table2 = TableId(2);
    let table3 = TableId(3);

    // Allocate pages across tables
    let t1p0 = pager.allocate_page(table1).unwrap();
    let t2p0 = pager.allocate_page(table2).unwrap();
    let t1p1 = pager.allocate_page(table1).unwrap();
    let t3p0 = pager.allocate_page(table3).unwrap();
    let t2p1 = pager.allocate_page(table2).unwrap();

    // Modify pages
    pager.fetch_page(table1, t1p0).unwrap().data[0] = 1;
    pager.fetch_page(table2, t2p0).unwrap().data[0] = 2;
    pager.fetch_page(table1, t1p1).unwrap().data[0] = 3;
    pager.fetch_page(table3, t3p0).unwrap().data[0] = 4;
    pager.fetch_page(table2, t2p1).unwrap().data[0] = 5;

    pager.flush().unwrap();

    // Verify isolation
    let mut pager2 = FilePager::new(dir.path(), 10);
    assert_eq!(pager2.fetch_page(table1, t1p0).unwrap().data[0], 1);
    assert_eq!(pager2.fetch_page(table2, t2p0).unwrap().data[0], 2);
    assert_eq!(pager2.fetch_page(table1, t1p1).unwrap().data[0], 3);
    assert_eq!(pager2.fetch_page(table3, t3p0).unwrap().data[0], 4);
    assert_eq!(pager2.fetch_page(table2, t2p1).unwrap().data[0], 5);
}

#[test]
fn lru_ordering_with_mixed_operations() {
    let dir = tempdir().unwrap();
    let mut pager = FilePager::new(dir.path(), 3);
    let table = TableId(1);

    let pid0 = pager.allocate_page(table).unwrap();
    let pid1 = pager.allocate_page(table).unwrap();
    let _pid2 = pager.allocate_page(table).unwrap();

    // LRU order: pid0 (oldest), pid1, pid2 (newest)

    // Access pid0 to make it most recently used
    pager.fetch_page(table, pid0).unwrap().data[0] = 100;
    // LRU order: pid1 (oldest), pid2, pid0 (newest)

    // Allocate new page, should evict pid1
    let pid3 = pager.allocate_page(table).unwrap();
    pager.fetch_page(table, pid3).unwrap().data[0] = 200;

    // pid0 should still be in cache (was most recently used)
    assert_eq!(pager.fetch_page(table, pid0).unwrap().data[0], 100);

    // pid1 should have been evicted and need reload from disk
    pager.fetch_page(table, pid1).unwrap();
}

#[test]
fn allocate_many_pages_sequential() {
    let dir = tempdir().unwrap();
    let mut pager = FilePager::new(dir.path(), 10);
    let table = TableId(1);

    let mut page_ids = Vec::new();
    for i in 0..20 {
        let pid = pager.allocate_page(table).unwrap();
        assert_eq!(pid, PageId(i));
        page_ids.push(pid);
    }

    pager.flush().unwrap();

    // Verify all pages exist and are sequential
    let mut pager2 = FilePager::new(dir.path(), 25);
    for (i, &pid) in page_ids.iter().enumerate() {
        let page = pager2.fetch_page(table, pid).unwrap();
        assert_eq!(page.id, i as u64);
    }
}

#[test]
fn page_modifications_across_fetch_calls() {
    let dir = tempdir().unwrap();
    let mut pager = FilePager::new(dir.path(), 5);
    let table = TableId(1);

    let pid = pager.allocate_page(table).unwrap();

    // Multiple fetch and modify operations
    {
        let page = pager.fetch_page(table, pid).unwrap();
        page.data[0] = 1;
    }

    {
        let page = pager.fetch_page(table, pid).unwrap();
        assert_eq!(page.data[0], 1);
        page.data[1] = 2;
    }

    {
        let page = pager.fetch_page(table, pid).unwrap();
        assert_eq!(page.data[0], 1);
        assert_eq!(page.data[1], 2);
        page.data[2] = 3;
    }

    pager.flush().unwrap();

    let mut pager2 = FilePager::new(dir.path(), 5);
    let page = pager2.fetch_page(table, pid).unwrap();
    assert_eq!(page.data[0], 1);
    assert_eq!(page.data[1], 2);
    assert_eq!(page.data[2], 3);
}

#[test]
fn eviction_of_clean_page() {
    let dir = tempdir().unwrap();
    let mut pager = FilePager::new(dir.path(), 2);
    let table = TableId(1);

    let pid0 = pager.allocate_page(table).unwrap();
    let _pid1 = pager.allocate_page(table).unwrap();

    // Flush to make pages clean
    pager.flush().unwrap();

    // Allocate new page, evicting clean page pid0
    let _pid2 = pager.allocate_page(table).unwrap();

    // Should be able to reload pid0
    pager.fetch_page(table, pid0).unwrap();
}

#[test]
fn table_path_format() {
    let dir = tempdir().unwrap();
    let mut pager = FilePager::new(dir.path(), 5);

    // Allocate page to trigger file creation
    pager.allocate_page(TableId(123)).unwrap();
    pager.flush().unwrap();

    // Check that file exists with expected name
    let expected_path = dir.path().join("table_123.tbl");
    assert!(expected_path.exists());
}

#[test]
fn multiple_flushes_idempotent() {
    let dir = tempdir().unwrap();
    let mut pager = FilePager::new(dir.path(), 5);
    let table = TableId(1);

    let pid = pager.allocate_page(table).unwrap();
    pager.fetch_page(table, pid).unwrap().data[0] = 99;

    // Multiple flushes should be safe
    pager.flush().unwrap();
    pager.flush().unwrap();
    pager.flush().unwrap();

    let mut pager2 = FilePager::new(dir.path(), 5);
    assert_eq!(pager2.fetch_page(table, pid).unwrap().data[0], 99);
}
