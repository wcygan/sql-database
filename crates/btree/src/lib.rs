//! B+Tree index implementation for persistent on-disk indexes.
//!
//! This crate provides a page-based B+Tree structure that integrates with
//! the database's buffer pool for efficient key-based lookups.

mod node;
mod page;

pub use node::{BTreeNode, NodeType};
pub use page::IndexPage;

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

use bincode::config::{self, Config};
use bincode::serde::{decode_from_slice, encode_to_vec};
use catalog::IndexId;
use common::{DbError, DbResult, PageId, RecordId};
use storage::PAGE_SIZE;
use types::Value;

fn bincode_config() -> impl Config {
    config::legacy()
}

/// A persistent B+Tree index that stores key-value pairs on disk.
///
/// Keys are `Vec<Value>` (supporting composite keys) and values are `RecordId`
/// pointing to rows in the heap table.
#[derive(Debug)]
pub struct BTreeIndex {
    /// The index identifier from the catalog
    pub index_id: IndexId,
    /// Root page ID (0 for a new/empty index)
    root_page_id: PageId,
    /// The underlying file for this index
    file: File,
    /// Number of pages currently allocated
    num_pages: u64,
}

impl BTreeIndex {
    /// Create a new B+Tree index file at the given path.
    pub fn create(path: &Path, index_id: IndexId) -> DbResult<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;

        let mut index = Self {
            index_id,
            root_page_id: PageId(0),
            file,
            num_pages: 0,
        };

        // Allocate the root page as an empty leaf
        let root_page = index.allocate_page()?;
        let root_node = BTreeNode::new_leaf();
        index.write_node(root_page, &root_node)?;
        index.root_page_id = root_page;

        Ok(index)
    }

    /// Open an existing B+Tree index file.
    pub fn open(path: &Path, index_id: IndexId) -> DbResult<Self> {
        if !path.exists() {
            return Err(DbError::Storage(format!(
                "index file does not exist: {}",
                path.display()
            )));
        }

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .truncate(false)
            .open(path)?;

        let file_len = file.metadata()?.len();
        let num_pages = file_len / PAGE_SIZE as u64;

        if num_pages == 0 {
            return Err(DbError::Storage("index file is empty".into()));
        }

        // Root is always page 0
        Ok(Self {
            index_id,
            root_page_id: PageId(0),
            file,
            num_pages,
        })
    }

    /// Search for all RecordIds matching the given key.
    pub fn search(&mut self, key: &[Value]) -> DbResult<Vec<RecordId>> {
        let leaf_page_id = self.find_leaf(key)?;
        let leaf = self.read_node(leaf_page_id)?;

        match leaf {
            BTreeNode::Leaf { entries, .. } => {
                let mut results = Vec::new();
                for (k, rid) in &entries {
                    if k == key {
                        results.push(*rid);
                    }
                }
                Ok(results)
            }
            BTreeNode::Internal { .. } => {
                Err(DbError::Storage("find_leaf returned non-leaf node".into()))
            }
        }
    }

    /// Search for all RecordIds within the given key range (inclusive).
    pub fn range_scan(
        &mut self,
        low: Option<&[Value]>,
        high: Option<&[Value]>,
    ) -> DbResult<Vec<RecordId>> {
        // Find the starting leaf
        let start_key = low.unwrap_or(&[]);
        let mut leaf_page_id = self.find_leaf(start_key)?;
        let mut results = Vec::new();

        loop {
            let leaf = self.read_node(leaf_page_id)?;

            match leaf {
                BTreeNode::Leaf { entries, next_leaf } => {
                    for (k, rid) in &entries {
                        // Check lower bound
                        if let Some(lo) = low {
                            if k.as_slice() < lo {
                                continue;
                            }
                        }
                        // Check upper bound
                        if let Some(hi) = high {
                            if k.as_slice() > hi {
                                // Past upper bound, we're done
                                return Ok(results);
                            }
                        }
                        results.push(*rid);
                    }

                    // Move to next leaf if exists
                    match next_leaf {
                        Some(next) => leaf_page_id = next,
                        None => break,
                    }
                }
                BTreeNode::Internal { .. } => {
                    return Err(DbError::Storage("expected leaf node in range scan".into()));
                }
            }
        }

        Ok(results)
    }

    /// Insert a key-value pair into the index.
    pub fn insert(&mut self, key: Vec<Value>, rid: RecordId) -> DbResult<()> {
        let result = self.insert_recursive(self.root_page_id, key, rid)?;

        if let Some((new_key, new_child_page)) = result {
            // Root was split, create new root
            let new_root_page = self.allocate_page()?;
            let new_root = BTreeNode::Internal {
                keys: vec![new_key],
                children: vec![self.root_page_id, new_child_page],
            };
            self.write_node(new_root_page, &new_root)?;
            self.root_page_id = new_root_page;
        }

        Ok(())
    }

    /// Delete a key-value pair from the index.
    pub fn delete(&mut self, key: &[Value], rid: RecordId) -> DbResult<bool> {
        let leaf_page_id = self.find_leaf(key)?;
        let mut leaf = self.read_node(leaf_page_id)?;

        match &mut leaf {
            BTreeNode::Leaf { entries, .. } => {
                let original_len = entries.len();
                entries.retain(|(k, r)| !(k == key && r == &rid));
                let deleted = entries.len() < original_len;

                if deleted {
                    self.write_node(leaf_page_id, &leaf)?;
                }

                Ok(deleted)
            }
            BTreeNode::Internal { .. } => {
                Err(DbError::Storage("find_leaf returned non-leaf node".into()))
            }
        }
    }

    /// Returns all entries in the index (for debugging/testing).
    pub fn scan_all(&mut self) -> DbResult<Vec<(Vec<Value>, RecordId)>> {
        self.range_scan(None, None)?;

        // Find leftmost leaf
        let mut page_id = self.root_page_id;
        loop {
            let node = self.read_node(page_id)?;
            match node {
                BTreeNode::Internal { children, .. } => {
                    page_id = *children
                        .first()
                        .ok_or_else(|| DbError::Storage("internal node has no children".into()))?;
                }
                BTreeNode::Leaf { .. } => break,
            }
        }

        // Scan all leaves
        let mut results = Vec::new();
        loop {
            let leaf = self.read_node(page_id)?;
            match leaf {
                BTreeNode::Leaf { entries, next_leaf } => {
                    for (k, rid) in entries {
                        results.push((k, rid));
                    }
                    match next_leaf {
                        Some(next) => page_id = next,
                        None => break,
                    }
                }
                BTreeNode::Internal { .. } => {
                    return Err(DbError::Storage("expected leaf in scan".into()));
                }
            }
        }

        Ok(results)
    }

    /// Flush any pending writes to disk.
    pub fn flush(&mut self) -> DbResult<()> {
        self.file.flush()?;
        Ok(())
    }

    // ---- Private helpers ----

    /// Find the leaf page that should contain the given key.
    fn find_leaf(&mut self, key: &[Value]) -> DbResult<PageId> {
        let mut current = self.root_page_id;

        loop {
            let node = self.read_node(current)?;

            match node {
                BTreeNode::Internal { keys, children } => {
                    // Binary search for the child to follow
                    let idx = keys.partition_point(|k| k.as_slice() <= key);
                    current = children[idx];
                }
                BTreeNode::Leaf { .. } => {
                    return Ok(current);
                }
            }
        }
    }

    /// Recursively insert into the tree, returning a split key and new page if split occurred.
    fn insert_recursive(
        &mut self,
        page_id: PageId,
        key: Vec<Value>,
        rid: RecordId,
    ) -> DbResult<Option<(Vec<Value>, PageId)>> {
        let node = self.read_node(page_id)?;

        match node {
            BTreeNode::Internal { keys, children } => {
                // Find child to insert into
                let idx = keys.partition_point(|k| k.as_slice() <= key.as_slice());
                let child_page = children[idx];

                // Recurse
                let split_result = self.insert_recursive(child_page, key, rid)?;

                if let Some((new_key, new_child)) = split_result {
                    // Child was split, insert the new key and child
                    let mut new_keys = keys;
                    let mut new_children = children;
                    new_keys.insert(idx, new_key);
                    new_children.insert(idx + 1, new_child);

                    // Check if this node needs to split
                    if new_keys.len() > Self::max_internal_keys() {
                        let (left, split_key, right) =
                            self.split_internal(new_keys, new_children)?;
                        self.write_node(page_id, &left)?;
                        let right_page = self.allocate_page()?;
                        self.write_node(right_page, &right)?;
                        return Ok(Some((split_key, right_page)));
                    } else {
                        let updated = BTreeNode::Internal {
                            keys: new_keys,
                            children: new_children,
                        };
                        self.write_node(page_id, &updated)?;
                    }
                }

                Ok(None)
            }
            BTreeNode::Leaf {
                mut entries,
                next_leaf,
            } => {
                // Insert in sorted order
                let idx = entries.partition_point(|(k, _)| k.as_slice() <= key.as_slice());
                entries.insert(idx, (key, rid));

                // Check if leaf needs to split
                if entries.len() > Self::max_leaf_entries() {
                    let (left, right, split_key) = self.split_leaf(entries, next_leaf)?;
                    self.write_node(page_id, &left)?;
                    let right_page = self.allocate_page()?;

                    // Update left's next_leaf to point to right
                    if let BTreeNode::Leaf {
                        entries: left_entries,
                        ..
                    } = &left
                    {
                        let updated_left = BTreeNode::Leaf {
                            entries: left_entries.clone(),
                            next_leaf: Some(right_page),
                        };
                        self.write_node(page_id, &updated_left)?;
                    }

                    self.write_node(right_page, &right)?;
                    return Ok(Some((split_key, right_page)));
                }

                let updated = BTreeNode::Leaf { entries, next_leaf };
                self.write_node(page_id, &updated)?;
                Ok(None)
            }
        }
    }

    fn split_leaf(
        &self,
        entries: Vec<(Vec<Value>, RecordId)>,
        original_next: Option<PageId>,
    ) -> DbResult<(BTreeNode, BTreeNode, Vec<Value>)> {
        let mid = entries.len() / 2;
        let (left_entries, right_entries) = entries.split_at(mid);

        let split_key = right_entries
            .first()
            .map(|(k, _)| k.clone())
            .ok_or_else(|| DbError::Storage("split produced empty right leaf".into()))?;

        let left = BTreeNode::Leaf {
            entries: left_entries.to_vec(),
            next_leaf: None, // Will be updated by caller
        };

        let right = BTreeNode::Leaf {
            entries: right_entries.to_vec(),
            next_leaf: original_next,
        };

        Ok((left, right, split_key))
    }

    fn split_internal(
        &self,
        keys: Vec<Vec<Value>>,
        children: Vec<PageId>,
    ) -> DbResult<(BTreeNode, Vec<Value>, BTreeNode)> {
        let mid = keys.len() / 2;

        let left_keys: Vec<_> = keys[..mid].to_vec();
        let left_children: Vec<_> = children[..=mid].to_vec();

        let split_key = keys[mid].clone();

        let right_keys: Vec<_> = keys[mid + 1..].to_vec();
        let right_children: Vec<_> = children[mid + 1..].to_vec();

        let left = BTreeNode::Internal {
            keys: left_keys,
            children: left_children,
        };

        let right = BTreeNode::Internal {
            keys: right_keys,
            children: right_children,
        };

        Ok((left, split_key, right))
    }

    fn max_leaf_entries() -> usize {
        // Conservative estimate: ~100 entries per leaf
        // Each entry is roughly key + RecordId
        100
    }

    fn max_internal_keys() -> usize {
        // Conservative estimate: ~100 keys per internal node
        100
    }

    fn allocate_page(&mut self) -> DbResult<PageId> {
        let page_id = PageId(self.num_pages);
        self.num_pages += 1;

        // Extend the file
        let offset = page_id.0 * PAGE_SIZE as u64;
        self.file.seek(SeekFrom::Start(offset))?;
        self.file.write_all(&[0u8; PAGE_SIZE])?;

        Ok(page_id)
    }

    fn read_node(&mut self, page_id: PageId) -> DbResult<BTreeNode> {
        let offset = page_id.0 * PAGE_SIZE as u64;
        self.file.seek(SeekFrom::Start(offset))?;

        let mut buffer = vec![0u8; PAGE_SIZE];
        self.file.read_exact(&mut buffer)?;

        let (node, _): (BTreeNode, usize) = decode_from_slice(&buffer, bincode_config())
            .map_err(|e| DbError::Storage(format!("failed to decode btree node: {e}")))?;

        Ok(node)
    }

    fn write_node(&mut self, page_id: PageId, node: &BTreeNode) -> DbResult<()> {
        let bytes = encode_to_vec(node, bincode_config())
            .map_err(|e| DbError::Storage(format!("failed to encode btree node: {e}")))?;

        if bytes.len() > PAGE_SIZE {
            return Err(DbError::Storage(format!(
                "btree node too large: {} bytes (max {})",
                bytes.len(),
                PAGE_SIZE
            )));
        }

        let mut buffer = vec![0u8; PAGE_SIZE];
        buffer[..bytes.len()].copy_from_slice(&bytes);

        let offset = page_id.0 * PAGE_SIZE as u64;
        self.file.seek(SeekFrom::Start(offset))?;
        self.file.write_all(&buffer)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests;
