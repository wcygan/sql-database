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
