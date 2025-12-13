//! B+Tree page layout and serialization.
//!
//! This module provides the on-disk format for B+Tree nodes.

use storage::PAGE_SIZE;

/// An index page wraps raw page data for B+Tree operations.
#[derive(Debug, Clone)]
pub struct IndexPage {
    pub id: u64,
    pub data: Vec<u8>,
}

impl IndexPage {
    /// Create a new zeroed index page.
    pub fn new(id: u64) -> Self {
        Self {
            id,
            data: vec![0u8; PAGE_SIZE],
        }
    }

    /// Returns the available space for node data.
    pub fn capacity() -> usize {
        PAGE_SIZE
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_page_is_zeroed() {
        let page = IndexPage::new(42);
        assert_eq!(page.id, 42);
        assert_eq!(page.data.len(), PAGE_SIZE);
        assert!(page.data.iter().all(|&b| b == 0));
    }

    #[test]
    fn capacity_matches_page_size() {
        assert_eq!(IndexPage::capacity(), PAGE_SIZE);
    }
}
