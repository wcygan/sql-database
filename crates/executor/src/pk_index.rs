//! In-memory primary key index for enforcing uniqueness constraints.
//!
//! The `PrimaryKeyIndex` maintains a BTreeMap from primary key values to RecordIds,
//! enabling efficient duplicate detection during INSERT operations. The index is built
//! lazily when a table is first accessed by scanning existing rows from storage.

use common::{ColumnId, DbError, DbResult, RecordId, Row};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
use types::Value;

/// In-memory index tracking primary key → RecordId mappings for uniqueness enforcement.
///
/// # Design
///
/// - Single-column PK: BTreeMap key is `vec![Value::Int(42)]`
/// - Composite PK: BTreeMap key is `vec![Value::Int(1), Value::Text("foo")]`
/// - Built on first table access by scanning heap file or loading from `.pk_idx` file
/// - Updated on every INSERT/DELETE and persisted to disk
///
/// # Example
///
/// ```ignore
/// let mut index = PrimaryKeyIndex::new(vec![0]); // PRIMARY KEY (id)
/// index.insert(vec![Value::Int(1)], RecordId { page_id: PageId(0), slot: 0 })?;
/// assert!(index.contains(&vec![Value::Int(1)]));
/// index.save_to_file(Path::new("table.pk_idx"))?;
/// ```
#[derive(Debug, Serialize, Deserialize)]
pub struct PrimaryKeyIndex {
    /// Column ordinals that comprise the primary key (in order)
    pk_columns: Vec<ColumnId>,
    /// Map from PK value tuple to RecordId
    index: BTreeMap<Vec<Value>, RecordId>,
}

impl PrimaryKeyIndex {
    /// Create a new primary key index for the given column ordinals.
    pub fn new(pk_columns: Vec<ColumnId>) -> Self {
        Self {
            pk_columns,
            index: BTreeMap::new(),
        }
    }

    /// Extract primary key values from a row based on configured PK columns.
    ///
    /// # Errors
    ///
    /// Returns `DbError::Executor` if any PK column ordinal is out of bounds.
    pub fn extract_key(&self, row: &Row) -> DbResult<Vec<Value>> {
        let mut key = Vec::with_capacity(self.pk_columns.len());
        for &col_id in &self.pk_columns {
            let idx = col_id as usize;
            let value = row
                .values
                .get(idx)
                .ok_or_else(|| {
                    DbError::Executor(format!(
                        "PK column {} out of bounds (row has {} columns)",
                        col_id,
                        row.values.len()
                    ))
                })?
                .clone();
            key.push(value);
        }
        Ok(key)
    }

    /// Check if a primary key value already exists in the index.
    pub fn contains(&self, key: &[Value]) -> bool {
        self.index.contains_key(key)
    }

    /// Insert a new primary key → RecordId mapping.
    ///
    /// # Errors
    ///
    /// Returns `DbError::Constraint` if the key already exists.
    pub fn insert(&mut self, key: Vec<Value>, rid: RecordId) -> DbResult<()> {
        if self.index.contains_key(&key) {
            return Err(DbError::Constraint(format!(
                "duplicate primary key value: {:?}",
                key
            )));
        }
        self.index.insert(key, rid);
        Ok(())
    }

    /// Remove a primary key mapping.
    ///
    /// Returns `true` if the key was present, `false` otherwise.
    pub fn remove(&mut self, key: &[Value]) -> bool {
        self.index.remove(key).is_some()
    }

    /// Get the number of entries in the index.
    pub fn len(&self) -> usize {
        self.index.len()
    }

    /// Check if the index is empty.
    pub fn is_empty(&self) -> bool {
        self.index.is_empty()
    }

    /// Get the primary key column ordinals.
    pub fn pk_columns(&self) -> &[ColumnId] {
        &self.pk_columns
    }

    /// Save the index to a file using bincode serialization.
    ///
    /// # Errors
    ///
    /// Returns `DbError::Storage` if serialization or file I/O fails.
    pub fn save_to_file(&self, path: &Path) -> DbResult<()> {
        let config = bincode::config::legacy();
        let bytes = bincode::serde::encode_to_vec(self, config)
            .map_err(|e| DbError::Storage(format!("Failed to serialize PK index: {}", e)))?;

        fs::write(path, bytes)
            .map_err(|e| DbError::Storage(format!("Failed to write PK index file: {}", e)))?;

        Ok(())
    }

    /// Load the index from a file using bincode deserialization.
    ///
    /// # Errors
    ///
    /// Returns `DbError::Storage` if file I/O or deserialization fails.
    pub fn load_from_file(path: &Path) -> DbResult<Self> {
        let bytes = fs::read(path)
            .map_err(|e| DbError::Storage(format!("Failed to read PK index file: {}", e)))?;

        let config = bincode::config::legacy();
        let (index, _len) = bincode::serde::decode_from_slice(&bytes, config)
            .map_err(|e| DbError::Storage(format!("Failed to deserialize PK index: {}", e)))?;

        Ok(index)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::PageId;
    use types::Value;

    #[test]
    fn new_index_is_empty() {
        let index = PrimaryKeyIndex::new(vec![0]);
        assert!(index.is_empty());
        assert_eq!(index.len(), 0);
    }

    #[test]
    fn extract_key_single_column() {
        let index = PrimaryKeyIndex::new(vec![0]);
        let row = Row::new(vec![Value::Int(42), Value::Text("foo".into())]);

        let key = index.extract_key(&row).unwrap();
        assert_eq!(key, vec![Value::Int(42)]);
    }

    #[test]
    fn extract_key_composite() {
        let index = PrimaryKeyIndex::new(vec![1, 0]);
        let row = Row::new(vec![Value::Int(42), Value::Text("foo".into())]);

        let key = index.extract_key(&row).unwrap();
        assert_eq!(key, vec![Value::Text("foo".into()), Value::Int(42)]);
    }

    #[test]
    fn extract_key_out_of_bounds() {
        let index = PrimaryKeyIndex::new(vec![5]);
        let row = Row::new(vec![Value::Int(42)]);

        let result = index.extract_key(&row);
        assert!(result.is_err());
        assert!(format!("{:?}", result).contains("out of bounds"));
    }

    #[test]
    fn insert_and_contains() {
        let mut index = PrimaryKeyIndex::new(vec![0]);
        let key = vec![Value::Int(1)];
        let rid = RecordId {
            page_id: PageId(0),
            slot: 0,
        };

        index.insert(key.clone(), rid).unwrap();
        assert!(index.contains(&key));
        assert_eq!(index.len(), 1);
    }

    #[test]
    fn insert_duplicate_returns_error() {
        let mut index = PrimaryKeyIndex::new(vec![0]);
        let key = vec![Value::Int(1)];
        let rid1 = RecordId {
            page_id: PageId(0),
            slot: 0,
        };
        let rid2 = RecordId {
            page_id: PageId(0),
            slot: 1,
        };

        index.insert(key.clone(), rid1).unwrap();
        let result = index.insert(key, rid2);

        assert!(result.is_err());
        assert!(format!("{:?}", result).contains("duplicate primary key"));
    }

    #[test]
    fn remove_existing_key() {
        let mut index = PrimaryKeyIndex::new(vec![0]);
        let key = vec![Value::Int(1)];
        let rid = RecordId {
            page_id: PageId(0),
            slot: 0,
        };

        index.insert(key.clone(), rid).unwrap();
        assert!(index.remove(&key));
        assert!(!index.contains(&key));
        assert_eq!(index.len(), 0);
    }

    #[test]
    fn remove_nonexistent_key() {
        let mut index = PrimaryKeyIndex::new(vec![0]);
        let key = vec![Value::Int(999)];

        assert!(!index.remove(&key));
    }

    #[test]
    fn composite_key_uniqueness() {
        let mut index = PrimaryKeyIndex::new(vec![0, 1]);
        let key1 = vec![Value::Int(1), Value::Text("a".into())];
        let key2 = vec![Value::Int(1), Value::Text("b".into())];
        let key3 = vec![Value::Int(2), Value::Text("a".into())];
        let rid = RecordId {
            page_id: PageId(0),
            slot: 0,
        };

        // All three keys should be allowed (different combinations)
        index.insert(key1.clone(), rid).unwrap();
        index.insert(key2.clone(), rid).unwrap();
        index.insert(key3.clone(), rid).unwrap();
        assert_eq!(index.len(), 3);

        // But duplicate of key1 should fail
        let result = index.insert(key1, rid);
        assert!(result.is_err());
    }

    #[test]
    fn serialize_empty_index() {
        use tempfile::NamedTempFile;

        let index = PrimaryKeyIndex::new(vec![0]);
        let temp_file = NamedTempFile::new().unwrap();

        // Save empty index
        index.save_to_file(temp_file.path()).unwrap();

        // Load and verify
        let loaded = PrimaryKeyIndex::load_from_file(temp_file.path()).unwrap();
        assert!(loaded.is_empty());
        assert_eq!(loaded.pk_columns(), &[0]);
    }

    #[test]
    fn serialize_single_key() {
        use tempfile::NamedTempFile;

        let mut index = PrimaryKeyIndex::new(vec![0]);
        let key = vec![Value::Int(42)];
        let rid = RecordId {
            page_id: PageId(5),
            slot: 3,
        };

        index.insert(key.clone(), rid).unwrap();

        let temp_file = NamedTempFile::new().unwrap();
        index.save_to_file(temp_file.path()).unwrap();

        // Load and verify
        let loaded = PrimaryKeyIndex::load_from_file(temp_file.path()).unwrap();
        assert_eq!(loaded.len(), 1);
        assert!(loaded.contains(&key));
        assert_eq!(loaded.pk_columns(), &[0]);
    }

    #[test]
    fn serialize_composite_keys() {
        use tempfile::NamedTempFile;

        let mut index = PrimaryKeyIndex::new(vec![1, 0]);
        let key1 = vec![Value::Text("alice".into()), Value::Int(1)];
        let key2 = vec![Value::Text("bob".into()), Value::Int(2)];
        let rid1 = RecordId {
            page_id: PageId(0),
            slot: 0,
        };
        let rid2 = RecordId {
            page_id: PageId(0),
            slot: 1,
        };

        index.insert(key1.clone(), rid1).unwrap();
        index.insert(key2.clone(), rid2).unwrap();

        let temp_file = NamedTempFile::new().unwrap();
        index.save_to_file(temp_file.path()).unwrap();

        // Load and verify
        let loaded = PrimaryKeyIndex::load_from_file(temp_file.path()).unwrap();
        assert_eq!(loaded.len(), 2);
        assert!(loaded.contains(&key1));
        assert!(loaded.contains(&key2));
        assert_eq!(loaded.pk_columns(), &[1, 0]);
    }

    #[test]
    fn load_from_missing_file_errors() {
        use std::path::Path;

        let result = PrimaryKeyIndex::load_from_file(Path::new("/nonexistent/path.pk_idx"));
        assert!(result.is_err());
        assert!(format!("{:?}", result).contains("Failed to read PK index file"));
    }

    #[test]
    fn load_from_corrupt_file_errors() {
        use std::io::Write;
        use tempfile::NamedTempFile;

        let mut temp_file = NamedTempFile::new().unwrap();
        // Write garbage data
        temp_file.write_all(b"invalid bincode data").unwrap();

        let result = PrimaryKeyIndex::load_from_file(temp_file.path());
        assert!(result.is_err());
        assert!(format!("{:?}", result).contains("Failed to deserialize PK index"));
    }

    #[test]
    fn round_trip_preserves_all_entries() {
        use tempfile::NamedTempFile;

        let mut index = PrimaryKeyIndex::new(vec![0]);

        // Insert 100 entries
        for i in 0..100 {
            let key = vec![Value::Int(i)];
            let rid = RecordId {
                page_id: PageId((i / 10) as u64),
                slot: (i % 10) as u16,
            };
            index.insert(key, rid).unwrap();
        }
        assert_eq!(index.len(), 100);

        // Save and load
        let temp_file = NamedTempFile::new().unwrap();
        index.save_to_file(temp_file.path()).unwrap();
        let loaded = PrimaryKeyIndex::load_from_file(temp_file.path()).unwrap();

        // Verify all entries preserved
        assert_eq!(loaded.len(), 100);
        for i in 0..100 {
            let key = vec![Value::Int(i)];
            assert!(loaded.contains(&key));
        }
    }
}
