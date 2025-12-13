use super::*;
use common::{PageId, RecordId};
use tempfile::tempdir;
use types::Value;

#[test]
fn create_empty_index() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.idx");

    let mut index = BTreeIndex::create(&path, IndexId(1)).unwrap();
    let results = index.search(&[Value::Int(1)]).unwrap();
    assert!(results.is_empty());
}

#[test]
fn insert_and_search_single_key() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.idx");

    let mut index = BTreeIndex::create(&path, IndexId(1)).unwrap();

    let rid = RecordId {
        page_id: PageId(0),
        slot: 0,
    };
    index.insert(vec![Value::Int(42)], rid).unwrap();

    let results = index.search(&[Value::Int(42)]).unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0], rid);

    // Search for non-existent key
    let results = index.search(&[Value::Int(99)]).unwrap();
    assert!(results.is_empty());
}

#[test]
fn insert_multiple_keys_in_order() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.idx");

    let mut index = BTreeIndex::create(&path, IndexId(1)).unwrap();

    for i in 0..10 {
        let rid = RecordId {
            page_id: PageId(0),
            slot: i,
        };
        index.insert(vec![Value::Int(i as i64)], rid).unwrap();
    }

    for i in 0..10 {
        let results = index.search(&[Value::Int(i as i64)]).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].slot, i);
    }
}

#[test]
fn insert_multiple_keys_reverse_order() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.idx");

    let mut index = BTreeIndex::create(&path, IndexId(1)).unwrap();

    for i in (0..10).rev() {
        let rid = RecordId {
            page_id: PageId(0),
            slot: i,
        };
        index.insert(vec![Value::Int(i as i64)], rid).unwrap();
    }

    for i in 0..10 {
        let results = index.search(&[Value::Int(i as i64)]).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].slot, i);
    }
}

#[test]
fn duplicate_keys_allowed() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.idx");

    let mut index = BTreeIndex::create(&path, IndexId(1)).unwrap();

    // Insert same key with different RIDs
    for slot in 0..3 {
        let rid = RecordId {
            page_id: PageId(0),
            slot,
        };
        index.insert(vec![Value::Int(42)], rid).unwrap();
    }

    let results = index.search(&[Value::Int(42)]).unwrap();
    assert_eq!(results.len(), 3);
}

#[test]
fn delete_existing_key() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.idx");

    let mut index = BTreeIndex::create(&path, IndexId(1)).unwrap();

    let rid = RecordId {
        page_id: PageId(0),
        slot: 0,
    };
    index.insert(vec![Value::Int(42)], rid).unwrap();

    // Verify it exists
    let results = index.search(&[Value::Int(42)]).unwrap();
    assert_eq!(results.len(), 1);

    // Delete it
    let deleted = index.delete(&[Value::Int(42)], rid).unwrap();
    assert!(deleted);

    // Verify it's gone
    let results = index.search(&[Value::Int(42)]).unwrap();
    assert!(results.is_empty());
}

#[test]
fn delete_non_existent_key() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.idx");

    let mut index = BTreeIndex::create(&path, IndexId(1)).unwrap();

    let rid = RecordId {
        page_id: PageId(0),
        slot: 0,
    };
    let deleted = index.delete(&[Value::Int(42)], rid).unwrap();
    assert!(!deleted);
}

#[test]
fn range_scan_all() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.idx");

    let mut index = BTreeIndex::create(&path, IndexId(1)).unwrap();

    for i in 0..10 {
        let rid = RecordId {
            page_id: PageId(0),
            slot: i,
        };
        index.insert(vec![Value::Int(i as i64)], rid).unwrap();
    }

    let results = index.range_scan(None, None).unwrap();
    assert_eq!(results.len(), 10);
}

#[test]
fn range_scan_with_bounds() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.idx");

    let mut index = BTreeIndex::create(&path, IndexId(1)).unwrap();

    for i in 0..10 {
        let rid = RecordId {
            page_id: PageId(0),
            slot: i,
        };
        index.insert(vec![Value::Int(i as i64)], rid).unwrap();
    }

    // Range [3, 7]
    let low = vec![Value::Int(3)];
    let high = vec![Value::Int(7)];
    let results = index
        .range_scan(Some(low.as_slice()), Some(high.as_slice()))
        .unwrap();

    assert_eq!(results.len(), 5);
    for (i, rid) in results.iter().enumerate() {
        assert_eq!(rid.slot, (3 + i) as u16);
    }
}

#[test]
fn text_keys() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.idx");

    let mut index = BTreeIndex::create(&path, IndexId(1)).unwrap();

    let names = ["alice", "bob", "charlie", "dave"];
    for (slot, name) in names.iter().enumerate() {
        let rid = RecordId {
            page_id: PageId(0),
            slot: slot as u16,
        };
        index
            .insert(vec![Value::Text(name.to_string())], rid)
            .unwrap();
    }

    let results = index.search(&[Value::Text("charlie".to_string())]).unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].slot, 2);
}

#[test]
fn composite_keys() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.idx");

    let mut index = BTreeIndex::create(&path, IndexId(1)).unwrap();

    // Composite key: (department, employee_id)
    let entries = [
        (vec![Value::Text("eng".into()), Value::Int(1)], 0),
        (vec![Value::Text("eng".into()), Value::Int(2)], 1),
        (vec![Value::Text("sales".into()), Value::Int(1)], 2),
    ];

    for (key, slot) in &entries {
        let rid = RecordId {
            page_id: PageId(0),
            slot: *slot,
        };
        index.insert(key.clone(), rid).unwrap();
    }

    // Search for specific composite key
    let results = index
        .search(&[Value::Text("eng".into()), Value::Int(2)])
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].slot, 1);
}

#[test]
fn persistence_across_open() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.idx");

    // Create and insert
    {
        let mut index = BTreeIndex::create(&path, IndexId(1)).unwrap();
        for i in 0..5 {
            let rid = RecordId {
                page_id: PageId(0),
                slot: i,
            };
            index.insert(vec![Value::Int(i as i64)], rid).unwrap();
        }
        index.flush().unwrap();
    }

    // Reopen and verify
    {
        let mut index = BTreeIndex::open(&path, IndexId(1)).unwrap();
        for i in 0..5 {
            let results = index.search(&[Value::Int(i as i64)]).unwrap();
            assert_eq!(results.len(), 1);
            assert_eq!(results[0].slot, i);
        }
    }
}

#[test]
fn many_inserts_trigger_splits() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.idx");

    let mut index = BTreeIndex::create(&path, IndexId(1)).unwrap();

    // Insert enough keys to trigger leaf splits
    let count = 500;
    for i in 0..count {
        let rid = RecordId {
            page_id: PageId(i / 100),
            slot: (i % 100) as u16,
        };
        index.insert(vec![Value::Int(i as i64)], rid).unwrap();
    }

    // Verify all keys are searchable
    for i in 0..count {
        let results = index.search(&[Value::Int(i as i64)]).unwrap();
        assert_eq!(results.len(), 1, "key {} not found", i);
    }

    // Verify scan returns all
    let all = index.scan_all().unwrap();
    assert_eq!(all.len(), count as usize);
}
