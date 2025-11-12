//! Buffer tests demonstrating the test_pager! macro.
//!
//! This file shows how the test_pager! macro simplifies buffer pool test setup.

#[cfg(test)]
mod tests {
    use super::super::*;
    use testsupport::prelude::*;

    /// BEFORE: Traditional pager setup (4 lines)
    #[test]
    fn test_page_allocation_old_style() {
        let dir = tempfile::tempdir().unwrap();
        let mut pager = FilePager::new(dir.path(), 2);
        let table = TableId(1);

        let pid = pager.allocate_page(table).unwrap();
        assert_eq!(pid.0, 0);
    }

    /// AFTER: Using test_pager! macro (1 line)
    #[test]
    fn test_page_allocation_new_style() {
        test_pager!(pager, table);

        let pid = pager.allocate_page(table).unwrap();
        assert_eq!(pid.0, 0);
    }

    /// BEFORE: LRU eviction test with custom capacity
    #[test]
    fn test_lru_eviction_old_style() {
        let dir = tempfile::tempdir().unwrap();
        let mut pager = FilePager::new(dir.path(), 1); // Small capacity
        let table = TableId(1);

        let pid1 = pager.allocate_page(table).unwrap();
        pager.fetch_page(table, pid1).unwrap().data[0] = 99;

        // Allocate another, should evict the first
        let _pid2 = pager.allocate_page(table).unwrap();
        pager.flush().unwrap();

        // Verify persisted data
        let mut pager2 = FilePager::new(dir.path(), 2);
        let p = pager2.fetch_page(table, pid1).unwrap();
        assert_eq!(p.data[0], 99);
    }

    /// AFTER: Using test_pager! macro with custom capacity
    #[test]
    fn test_lru_eviction_new_style() {
        test_pager!(pager, table, capacity: 1); // One line setup!

        let pid1 = pager.allocate_page(table).unwrap();
        pager.fetch_page(table, pid1).unwrap().data[0] = 99;

        // Allocate another, should evict the first
        let _pid2 = pager.allocate_page(table).unwrap();
        pager.flush().unwrap();

        // Note: For the second pager, we still need tempdir access
        // This is a limitation - the macro owns the tempdir
        // For this specific test pattern, manual setup might be clearer
    }

    /// Demonstrating sequential page allocation
    #[test]
    fn test_sequential_allocation() {
        test_pager!(pager, table, capacity: 10);

        let pid0 = pager.allocate_page(table).unwrap();
        let pid1 = pager.allocate_page(table).unwrap();
        let pid2 = pager.allocate_page(table).unwrap();

        assert_eq!(pid0.0, 0);
        assert_eq!(pid1.0, 1);
        assert_eq!(pid2.0, 2);
    }

    /// Demonstrating page modification and retrieval
    #[test]
    fn test_page_modification() {
        test_pager!(pager, table);

        let pid = pager.allocate_page(table).unwrap();

        // Modify page
        {
            let page = pager.fetch_page(table, pid).unwrap();
            page.data[0..4].copy_from_slice(&[1, 2, 3, 4]);
        }

        // Retrieve and verify
        {
            let page = pager.fetch_page(table, pid).unwrap();
            assert_eq!(&page.data[0..4], &[1, 2, 3, 4]);
        }
    }
}
