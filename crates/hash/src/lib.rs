//! Hash index implementation using static hashing with overflow chains.
//!
//! Provides O(1) average-case lookups for equality predicates.
//! Only supports exact key matches (no range queries).

use catalog::IndexId;
use common::{DbError, DbResult, PageId, RecordId};
use serde::{Deserialize, Serialize};
use std::collections::hash_map::DefaultHasher;
use std::fs::{File, OpenOptions};
use std::hash::{Hash, Hasher};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use types::Value;

/// Page size for hash index storage.
const PAGE_SIZE: usize = 4096;

/// Number of hash buckets (fixed for simplicity).
const NUM_BUCKETS: usize = 256;

/// Maximum entries per bucket before using overflow.
const MAX_BUCKET_ENTRIES: usize = 40;

/// Hash index using static hashing with overflow chains.
///
/// Layout:
/// - Page 0: Header (num_pages)
/// - Pages 1..257: Primary buckets (256 buckets)
/// - Pages 257+: Overflow buckets
pub struct HashIndex {
    /// Index identifier from catalog.
    pub index_id: IndexId,
    /// Underlying file for persistence.
    file: File,
    /// Total number of pages allocated.
    num_pages: u64,
}

/// A bucket page containing key-value entries.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct HashBucket {
    /// Key-RecordId pairs stored in this bucket.
    entries: Vec<(Vec<Value>, RecordId)>,
    /// Pointer to overflow bucket page (0 = none).
    overflow: u64,
}

/// Header stored at the beginning of the index file.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct HashHeader {
    /// Number of pages in use.
    num_pages: u64,
}

impl HashIndex {
    /// Create a new hash index file.
    pub fn create(path: &Path, index_id: IndexId) -> DbResult<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)
            .map_err(|e| DbError::Storage(format!("failed to create hash index: {}", e)))?;

        // Page 0 = header, pages 1..NUM_BUCKETS+1 = primary buckets
        let num_pages = 1 + NUM_BUCKETS as u64;

        let mut index = Self {
            index_id,
            file,
            num_pages,
        };

        // Write header
        index.write_header()?;

        // Write empty primary buckets
        let empty_bucket = HashBucket::default();
        for i in 0..NUM_BUCKETS {
            index.write_bucket(PageId(1 + i as u64), &empty_bucket)?;
        }

        Ok(index)
    }

    /// Open an existing hash index file.
    pub fn open(path: &Path, index_id: IndexId) -> DbResult<Self> {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .map_err(|e| DbError::Storage(format!("failed to open hash index: {}", e)))?;

        // Read header from page 0
        let mut buf = vec![0u8; PAGE_SIZE];
        file.seek(SeekFrom::Start(0))
            .map_err(|e| DbError::Storage(format!("seek error: {}", e)))?;
        file.read_exact(&mut buf)
            .map_err(|e| DbError::Storage(format!("read error: {}", e)))?;

        let header: HashHeader = bincode::serde::decode_from_slice(&buf, bincode::config::legacy())
            .map_err(|e| DbError::Storage(format!("failed to decode header: {}", e)))?
            .0;

        Ok(Self {
            index_id,
            file,
            num_pages: header.num_pages,
        })
    }

    /// Search for all RecordIds matching the given key.
    pub fn search(&mut self, key: &[Value]) -> DbResult<Vec<RecordId>> {
        let bucket_idx = self.bucket_index(key);
        let mut results = Vec::new();

        // Walk the chain of buckets
        let mut page_id = PageId(1 + bucket_idx as u64);
        loop {
            let bucket = self.read_bucket(page_id)?;

            for (k, rid) in &bucket.entries {
                if k == key {
                    results.push(*rid);
                }
            }

            if bucket.overflow == 0 {
                break;
            }
            page_id = PageId(bucket.overflow);
        }

        Ok(results)
    }

    /// Insert a key-RecordId pair into the index.
    pub fn insert(&mut self, key: Vec<Value>, rid: RecordId) -> DbResult<()> {
        let bucket_idx = self.bucket_index(&key);
        let primary_page = PageId(1 + bucket_idx as u64);

        // Find the last bucket in the chain with space
        let mut page_id = primary_page;
        loop {
            let mut bucket = self.read_bucket(page_id)?;

            if bucket.entries.len() < MAX_BUCKET_ENTRIES {
                // Has space, insert here
                bucket.entries.push((key, rid));
                self.write_bucket(page_id, &bucket)?;
                return Ok(());
            }

            if bucket.overflow == 0 {
                // No overflow bucket, create one
                let overflow_page = PageId(self.num_pages);
                self.num_pages += 1;

                // Update current bucket to point to overflow
                bucket.overflow = overflow_page.0;
                self.write_bucket(page_id, &bucket)?;

                // Write new overflow bucket with the entry
                let new_bucket = HashBucket {
                    entries: vec![(key, rid)],
                    overflow: 0,
                };
                self.write_bucket(overflow_page, &new_bucket)?;
                return Ok(());
            }

            page_id = PageId(bucket.overflow);
        }
    }

    /// Delete a key-RecordId pair from the index.
    ///
    /// Returns true if the entry was found and deleted.
    pub fn delete(&mut self, key: &[Value], rid: RecordId) -> DbResult<bool> {
        let bucket_idx = self.bucket_index(key);
        let mut page_id = PageId(1 + bucket_idx as u64);

        loop {
            let mut bucket = self.read_bucket(page_id)?;

            let original_len = bucket.entries.len();
            bucket.entries.retain(|(k, r)| !(k == key && *r == rid));

            if bucket.entries.len() < original_len {
                self.write_bucket(page_id, &bucket)?;
                return Ok(true);
            }

            if bucket.overflow == 0 {
                break;
            }
            page_id = PageId(bucket.overflow);
        }

        Ok(false)
    }

    /// Flush all changes to disk.
    pub fn flush(&mut self) -> DbResult<()> {
        self.write_header()?;
        self.file
            .sync_all()
            .map_err(|e| DbError::Storage(format!("sync error: {}", e)))?;
        Ok(())
    }

    /// Get bucket index from key hash.
    fn bucket_index(&self, key: &[Value]) -> usize {
        let hash = hash_key(key);
        (hash as usize) % NUM_BUCKETS
    }

    /// Read a bucket from disk.
    fn read_bucket(&mut self, page_id: PageId) -> DbResult<HashBucket> {
        let offset = page_id.0 * PAGE_SIZE as u64;
        self.file
            .seek(SeekFrom::Start(offset))
            .map_err(|e| DbError::Storage(format!("seek error: {}", e)))?;

        let mut buf = vec![0u8; PAGE_SIZE];
        self.file
            .read_exact(&mut buf)
            .map_err(|e| DbError::Storage(format!("read error: {}", e)))?;

        let bucket: HashBucket = bincode::serde::decode_from_slice(&buf, bincode::config::legacy())
            .map_err(|e| DbError::Storage(format!("failed to decode bucket: {}", e)))?
            .0;

        Ok(bucket)
    }

    /// Write a bucket to disk.
    fn write_bucket(&mut self, page_id: PageId, bucket: &HashBucket) -> DbResult<()> {
        let offset = page_id.0 * PAGE_SIZE as u64;
        self.file
            .seek(SeekFrom::Start(offset))
            .map_err(|e| DbError::Storage(format!("seek error: {}", e)))?;

        let encoded = bincode::serde::encode_to_vec(bucket, bincode::config::legacy())
            .map_err(|e| DbError::Storage(format!("failed to encode bucket: {}", e)))?;

        let mut buf = vec![0u8; PAGE_SIZE];
        buf[..encoded.len()].copy_from_slice(&encoded);

        self.file
            .write_all(&buf)
            .map_err(|e| DbError::Storage(format!("write error: {}", e)))?;

        Ok(())
    }

    /// Write header to page 0.
    fn write_header(&mut self) -> DbResult<()> {
        let header = HashHeader {
            num_pages: self.num_pages,
        };

        let encoded = bincode::serde::encode_to_vec(&header, bincode::config::legacy())
            .map_err(|e| DbError::Storage(format!("failed to encode header: {}", e)))?;

        let mut buf = vec![0u8; PAGE_SIZE];
        buf[..encoded.len()].copy_from_slice(&encoded);

        self.file
            .seek(SeekFrom::Start(0))
            .map_err(|e| DbError::Storage(format!("seek error: {}", e)))?;
        self.file
            .write_all(&buf)
            .map_err(|e| DbError::Storage(format!("write error: {}", e)))?;

        Ok(())
    }
}

/// Hash a composite key to a u64.
pub fn hash_key(key: &[Value]) -> u64 {
    let mut hasher = DefaultHasher::new();
    for val in key {
        match val {
            Value::Int(i) => {
                0u8.hash(&mut hasher); // Type tag
                i.hash(&mut hasher);
            }
            Value::Text(s) => {
                1u8.hash(&mut hasher);
                s.hash(&mut hasher);
            }
            Value::Bool(b) => {
                2u8.hash(&mut hasher);
                b.hash(&mut hasher);
            }
            Value::Null => {
                3u8.hash(&mut hasher);
            }
        }
    }
    hasher.finish()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn temp_index() -> (HashIndex, TempDir) {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("test.idx");
        let index = HashIndex::create(&path, IndexId(1)).unwrap();
        (index, temp)
    }

    #[test]
    fn create_empty_index() {
        let (index, _temp) = temp_index();
        assert_eq!(index.num_pages, 1 + NUM_BUCKETS as u64);
    }

    #[test]
    fn insert_and_search_single_key() {
        let (mut index, _temp) = temp_index();

        let key = vec![Value::Int(42)];
        let rid = RecordId {
            page_id: PageId(0),
            slot: 0,
        };

        index.insert(key.clone(), rid).unwrap();

        let results = index.search(&key).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0], rid);
    }

    #[test]
    fn insert_multiple_keys() {
        let (mut index, _temp) = temp_index();

        for i in 0..10 {
            let key = vec![Value::Int(i)];
            let rid = RecordId {
                page_id: PageId(0),
                slot: i as u16,
            };
            index.insert(key, rid).unwrap();
        }

        for i in 0..10 {
            let key = vec![Value::Int(i)];
            let results = index.search(&key).unwrap();
            assert_eq!(results.len(), 1);
            assert_eq!(results[0].slot, i as u16);
        }
    }

    #[test]
    fn duplicate_keys_return_multiple_rids() {
        let (mut index, _temp) = temp_index();

        let key = vec![Value::Int(42)];
        let rid1 = RecordId {
            page_id: PageId(0),
            slot: 0,
        };
        let rid2 = RecordId {
            page_id: PageId(0),
            slot: 1,
        };

        index.insert(key.clone(), rid1).unwrap();
        index.insert(key.clone(), rid2).unwrap();

        let results = index.search(&key).unwrap();
        assert_eq!(results.len(), 2);
        assert!(results.contains(&rid1));
        assert!(results.contains(&rid2));
    }

    #[test]
    fn delete_removes_entry() {
        let (mut index, _temp) = temp_index();

        let key = vec![Value::Int(42)];
        let rid = RecordId {
            page_id: PageId(0),
            slot: 0,
        };

        index.insert(key.clone(), rid).unwrap();
        assert_eq!(index.search(&key).unwrap().len(), 1);

        let deleted = index.delete(&key, rid).unwrap();
        assert!(deleted);
        assert_eq!(index.search(&key).unwrap().len(), 0);
    }

    #[test]
    fn delete_nonexistent_returns_false() {
        let (mut index, _temp) = temp_index();

        let key = vec![Value::Int(42)];
        let rid = RecordId {
            page_id: PageId(0),
            slot: 0,
        };

        let deleted = index.delete(&key, rid).unwrap();
        assert!(!deleted);
    }

    #[test]
    fn composite_key() {
        let (mut index, _temp) = temp_index();

        let key = vec![Value::Int(1), Value::Text("hello".into())];
        let rid = RecordId {
            page_id: PageId(0),
            slot: 0,
        };

        index.insert(key.clone(), rid).unwrap();

        let results = index.search(&key).unwrap();
        assert_eq!(results.len(), 1);

        // Different composite key should not match
        let other_key = vec![Value::Int(1), Value::Text("world".into())];
        let results = index.search(&other_key).unwrap();
        assert_eq!(results.len(), 0);
    }

    #[test]
    fn persistence_round_trip() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("test.idx");

        let key = vec![Value::Int(42)];
        let rid = RecordId {
            page_id: PageId(0),
            slot: 0,
        };

        // Create and insert
        {
            let mut index = HashIndex::create(&path, IndexId(1)).unwrap();
            index.insert(key.clone(), rid).unwrap();
            index.flush().unwrap();
        }

        // Reopen and verify
        {
            let mut index = HashIndex::open(&path, IndexId(1)).unwrap();
            let results = index.search(&key).unwrap();
            assert_eq!(results.len(), 1);
            assert_eq!(results[0], rid);
        }
    }

    #[test]
    fn overflow_bucket_handling() {
        let (mut index, _temp) = temp_index();

        // Insert enough keys to trigger overflow in at least one bucket
        for i in 0..200 {
            let key = vec![Value::Int(i)];
            let rid = RecordId {
                page_id: PageId(0),
                slot: i as u16,
            };
            index.insert(key, rid).unwrap();
        }

        // Verify all keys are still findable
        for i in 0..200 {
            let key = vec![Value::Int(i)];
            let results = index.search(&key).unwrap();
            assert_eq!(results.len(), 1, "key {} not found", i);
        }
    }

    #[test]
    fn many_inserts() {
        let (mut index, _temp) = temp_index();

        // Insert many keys
        for i in 0..500 {
            let key = vec![Value::Int(i)];
            let rid = RecordId {
                page_id: PageId(0),
                slot: (i % 100) as u16,
            };
            index.insert(key, rid).unwrap();
        }

        // Verify all keys are still findable
        for i in 0..500 {
            let key = vec![Value::Int(i)];
            let results = index.search(&key).unwrap();
            assert_eq!(results.len(), 1, "key {} not found", i);
        }
    }

    #[test]
    fn text_keys() {
        let (mut index, _temp) = temp_index();

        let key = vec![Value::Text("hello world".into())];
        let rid = RecordId {
            page_id: PageId(0),
            slot: 0,
        };

        index.insert(key.clone(), rid).unwrap();

        let results = index.search(&key).unwrap();
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn bool_keys() {
        let (mut index, _temp) = temp_index();

        let key_true = vec![Value::Bool(true)];
        let key_false = vec![Value::Bool(false)];
        let rid1 = RecordId {
            page_id: PageId(0),
            slot: 0,
        };
        let rid2 = RecordId {
            page_id: PageId(0),
            slot: 1,
        };

        index.insert(key_true.clone(), rid1).unwrap();
        index.insert(key_false.clone(), rid2).unwrap();

        assert_eq!(index.search(&key_true).unwrap().len(), 1);
        assert_eq!(index.search(&key_false).unwrap().len(), 1);
    }

    #[test]
    fn hash_key_different_types() {
        // Different types should produce different hashes
        let h1 = hash_key(&[Value::Int(1)]);
        let h2 = hash_key(&[Value::Text("1".into())]);
        let h3 = hash_key(&[Value::Bool(true)]);

        assert_ne!(h1, h2);
        assert_ne!(h2, h3);
        assert_ne!(h1, h3);
    }

    #[test]
    fn hash_key_composite_order_matters() {
        let h1 = hash_key(&[Value::Int(1), Value::Int(2)]);
        let h2 = hash_key(&[Value::Int(2), Value::Int(1)]);

        assert_ne!(h1, h2);
    }
}
