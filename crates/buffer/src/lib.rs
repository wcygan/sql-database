//! Buffer pool manager for page-level caching and I/O.
//!
//! The buffer pool sits between the storage layer and the executor, providing:
//! - LRU-based in-memory page cache
//! - Lazy loading and eviction with automatic dirty page flushing
//! - File-per-table storage with sequential page IDs
//!
//! # Example
//!
//! ```no_run
//! use buffer::{Pager, FilePager};
//! use common::{TableId, PageId};
//!
//! let mut pager = FilePager::new("/tmp/db", 100);
//! let table = TableId(1);
//!
//! // Allocate a new page
//! let page_id = pager.allocate_page(table).unwrap();
//!
//! // Fetch and modify
//! {
//!     let page = pager.fetch_page(table, page_id).unwrap();
//!     page.data[0] = 42;
//! }
//!
//! // Flush to disk
//! pager.flush().unwrap();
//! ```

#[cfg(test)]
mod tests;

use common::{DbError, DbResult, PageId, TableId};
use hashbrown::HashMap;
use lru::LruCache;
use std::{
    fs::OpenOptions,
    io::{Read, Seek, SeekFrom, Write},
    num::NonZeroUsize,
    path::PathBuf,
};
use storage::{PAGE_SIZE, Page};

/// Abstraction for fetching, allocating, and flushing pages.
///
/// Implementors manage the lifecycle of pages, including:
/// - Loading pages from persistent storage into memory
/// - Evicting pages when the cache is full
/// - Tracking dirty pages and flushing them to disk
pub trait Pager {
    /// Fetch a page from the buffer pool or load it from disk.
    ///
    /// Returns a mutable reference to the page in the cache.
    /// Marks the page as recently used in the LRU policy.
    fn fetch_page(&mut self, table: TableId, pid: PageId) -> DbResult<&mut Page>;

    /// Allocate a new page for the given table.
    ///
    /// Assigns the next sequential `PageId` and returns it.
    /// The new page is initialized with zeros and marked as dirty.
    fn allocate_page(&mut self, table: TableId) -> DbResult<PageId>;

    /// Flush all dirty pages to disk.
    ///
    /// After flushing, all pages are marked as clean.
    fn flush(&mut self) -> DbResult<()>;
}

/// File-backed buffer pool with LRU eviction.
///
/// Uses a file-per-table storage model with sequential page IDs.
/// Pages are evicted using an LRU (Least Recently Used) policy.
/// Dirty pages are automatically flushed to disk on eviction or explicit flush.
#[derive(Debug)]
pub struct FilePager {
    base_dir: PathBuf,
    max_pages: usize,
    cache: LruCache<(TableId, PageId), Page>,
    dirty: HashMap<(TableId, PageId), bool>,
}

impl FilePager {
    /// Create a new file-backed pager.
    ///
    /// # Arguments
    ///
    /// * `base_dir` - Directory for table files (format: `table_{id}.tbl`)
    /// * `max_pages` - Maximum number of pages to cache in memory
    ///
    /// # Panics
    ///
    /// Panics if `max_pages` is 0.
    pub fn new(base_dir: impl Into<PathBuf>, max_pages: usize) -> Self {
        assert!(max_pages > 0, "max_pages must be > 0");
        Self {
            base_dir: base_dir.into(),
            max_pages,
            cache: LruCache::new(NonZeroUsize::new(max_pages).unwrap()),
            dirty: HashMap::new(),
        }
    }

    /// Get the file path for a table.
    fn table_path(&self, table: TableId) -> PathBuf {
        self.base_dir.join(format!("table_{}.tbl", table.0))
    }

    /// Load a page from disk, or create a new zero-initialized page if it doesn't exist.
    fn load_page(&self, table: TableId, pid: PageId) -> DbResult<Page> {
        let path = self.table_path(table);
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .map_err(|e| DbError::Storage(format!("Failed to open table file: {}", e)))?;

        let offset = pid.0 * PAGE_SIZE as u64;
        file.seek(SeekFrom::Start(offset))
            .map_err(|e| DbError::Storage(format!("Failed to seek to page: {}", e)))?;

        let mut buf = vec![0u8; PAGE_SIZE];
        let n = file
            .read(&mut buf)
            .map_err(|e| DbError::Storage(format!("Failed to read page: {}", e)))?;

        if n == 0 {
            // Page doesn't exist yet, return zero-initialized page
            Ok(Page::new(pid.0))
        } else if n < PAGE_SIZE {
            // Partial page read - this shouldn't happen with proper page alignment
            Err(DbError::Storage(format!(
                "Partial page read: expected {} bytes, got {}",
                PAGE_SIZE, n
            )))
        } else {
            Ok(Page {
                id: pid.0,
                data: buf,
            })
        }
    }

    /// Write a page to disk.
    fn write_page(&self, table: TableId, page: &Page) -> DbResult<()> {
        let path = self.table_path(table);
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .map_err(|e| DbError::Storage(format!("Failed to open table file: {}", e)))?;

        let offset = page.id * PAGE_SIZE as u64;
        file.seek(SeekFrom::Start(offset))
            .map_err(|e| DbError::Storage(format!("Failed to seek to page: {}", e)))?;

        file.write_all(&page.data)
            .map_err(|e| DbError::Storage(format!("Failed to write page: {}", e)))?;

        Ok(())
    }

    /// Evict the least recently used page if the cache is full.
    ///
    /// If the evicted page is dirty, it is flushed to disk first.
    fn evict_if_needed(&mut self) -> DbResult<()> {
        if self.cache.len() < self.max_pages {
            return Ok(());
        }

        if let Some(((table, pid), page)) = self.cache.pop_lru()
            && self.dirty.remove(&(table, pid)).is_some()
        {
            self.write_page(table, &page)?;
        }

        Ok(())
    }
}

impl Pager for FilePager {
    fn fetch_page(&mut self, table: TableId, pid: PageId) -> DbResult<&mut Page> {
        // Check if page is already in cache
        if self.cache.contains(&(table, pid)) {
            // LruCache::get_mut updates LRU order
            return Ok(self.cache.get_mut(&(table, pid)).unwrap());
        }

        // Page not in cache - load from disk
        let page = self.load_page(table, pid)?;

        // Evict LRU page if cache is full
        self.evict_if_needed()?;

        // Insert into cache
        self.cache.push((table, pid), page);

        Ok(self.cache.get_mut(&(table, pid)).unwrap())
    }

    fn allocate_page(&mut self, table: TableId) -> DbResult<PageId> {
        let path = self.table_path(table);

        // Determine next page ID from file size
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .map_err(|e| DbError::Storage(format!("Failed to open table file: {}", e)))?;

        let len = file
            .metadata()
            .map_err(|e| DbError::Storage(format!("Failed to read file metadata: {}", e)))?
            .len();

        let pid = PageId(len / PAGE_SIZE as u64);

        // Create new zero-initialized page
        let page = Page::new(pid.0);

        // Write page to disk immediately to extend the file
        self.write_page(table, &page)?;

        // Evict LRU page if cache is full
        self.evict_if_needed()?;

        // Insert into cache and mark as dirty (so future modifications are tracked)
        self.cache.push((table, pid), page);
        self.dirty.insert((table, pid), true);

        Ok(pid)
    }

    fn flush(&mut self) -> DbResult<()> {
        // Collect all dirty page keys
        let dirty_keys: Vec<_> = self.dirty.keys().copied().collect();

        // Flush each dirty page
        for (table, pid) in dirty_keys {
            if let Some(page) = self.cache.peek(&(table, pid)) {
                self.write_page(table, page)?;
                self.dirty.remove(&(table, pid));
            }
        }

        Ok(())
    }
}
