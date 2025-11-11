use super::*;
use tempfile::tempdir;
use types::Value;

#[test]
fn insert_and_get_round_trip() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("heap.tbl");
    let mut table = HeapFile::open(&path, 1).unwrap();

    let row = Row(vec![
        Value::Int(1),
        Value::Text("Will".into()),
        Value::Int(27),
    ]);

    let rid = table.insert(&row).unwrap();
    let fetched = table.get(rid).unwrap();

    assert_eq!(fetched.0, row.0);
}

#[test]
fn delete_marks_slot_empty() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("heap.tbl");
    let mut table = HeapFile::open(&path, 1).unwrap();

    let row = Row(vec![Value::Int(1)]);
    let rid = table.insert(&row).unwrap();
    table.delete(rid).unwrap();

    let err = table.get(rid).unwrap_err();
    assert!(matches!(err, DbError::Storage(_)));
}

#[test]
fn large_rows_allocate_new_pages() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("heap.tbl");
    let mut table = HeapFile::open(&path, 1).unwrap();

    let big_payload = "x".repeat(PAGE_SIZE - 256);
    let row = Row(vec![Value::Text(big_payload.clone())]);

    let rid_a = table.insert(&row).unwrap();
    let rid_b = table.insert(&row).unwrap();

    assert!(rid_b.page_id.0 > rid_a.page_id.0);

    let fetched = table.get(rid_b).unwrap();
    assert_eq!(fetched.0, vec![Value::Text(big_payload)]);
}

#[test]
fn delete_twice_returns_error() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("heap.tbl");
    let mut table = HeapFile::open(&path, 1).unwrap();

    let row = Row(vec![Value::Int(7)]);
    let rid = table.insert(&row).unwrap();

    table.delete(rid).unwrap();
    let err = table.delete(rid).unwrap_err();
    assert!(matches!(err, DbError::Storage(_)));
}

#[test]
fn get_rejects_invalid_slot() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("heap.tbl");
    let mut table = HeapFile::open(&path, 1).unwrap();

    let row = Row(vec![Value::Int(1)]);
    let rid = table.insert(&row).unwrap();

    let bogus = RecordId {
        page_id: rid.page_id,
        slot: rid.slot + 5,
    };

    let err = table.get(bogus).unwrap_err();
    assert!(matches!(err, DbError::Storage(_)));
}
