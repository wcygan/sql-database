//! In-memory primary key index for enforcing uniqueness constraints.
//!
//! The `PrimaryKeyIndex` maintains a HashMap from primary key values to RecordIds,
//! enabling O(1) duplicate detection during INSERT operations. The index is built
//! lazily when a table is first accessed by scanning existing rows from storage.

use common::{ColumnId, DbError, DbResult, RecordId, Row};
use std::collections::HashMap;
use types::Value;

/// In-memory index tracking primary key → RecordId mappings for uniqueness enforcement.
///
/// # Design
///
/// - Single-column PK: HashMap key is `vec![Value::Int(42)]`
/// - Composite PK: HashMap key is `vec![Value::Int(1), Value::Text("foo")]`
/// - Built on first table access by scanning heap file
/// - Updated on every INSERT/DELETE
///
/// # Example
///
/// ```ignore
/// let mut index = PrimaryKeyIndex::new(vec![0]); // PRIMARY KEY (id)
/// index.insert(vec![Value::Int(1)], RecordId { page_id: PageId(0), slot: 0 })?;
/// assert!(index.contains(&vec![Value::Int(1)]));
/// ```
#[derive(Debug)]
pub struct PrimaryKeyIndex {
    /// Column ordinals that comprise the primary key (in order)
    pk_columns: Vec<ColumnId>,
    /// Map from PK value tuple to RecordId
    index: HashMap<Vec<Value>, RecordId>,
}

impl PrimaryKeyIndex {
    /// Create a new primary key index for the given column ordinals.
    pub fn new(pk_columns: Vec<ColumnId>) -> Self {
        Self {
            pk_columns,
            index: HashMap::new(),
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
}
