use super::*;
use tempfile::tempdir;
use types::Value;

#[test]
fn insert_and_get_round_trip() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("heap.tbl");
    let mut table = HeapFile::open(&path, 1).unwrap();

    let row = Row::new(vec![
        Value::Int(1),
        Value::Text("Will".into()),
        Value::Int(27),
    ]);

    let rid = table.insert(&row).unwrap();
    let fetched = table.get(rid).unwrap();

    assert_eq!(fetched.values, row.values);
}

#[test]
fn delete_marks_slot_empty() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("heap.tbl");
    let mut table = HeapFile::open(&path, 1).unwrap();

    let row = Row::new(vec![Value::Int(1)]);
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
    let row = Row::new(vec![Value::Text(big_payload.clone())]);

    let rid_a = table.insert(&row).unwrap();
    let rid_b = table.insert(&row).unwrap();

    assert!(rid_b.page_id.0 > rid_a.page_id.0);

    let fetched = table.get(rid_b).unwrap();
    assert_eq!(fetched.values, vec![Value::Text(big_payload)]);
}

#[test]
fn delete_twice_returns_error() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("heap.tbl");
    let mut table = HeapFile::open(&path, 1).unwrap();

    let row = Row::new(vec![Value::Int(7)]);
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

    let row = Row::new(vec![Value::Int(1)]);
    let rid = table.insert(&row).unwrap();

    let bogus = RecordId {
        page_id: rid.page_id,
        slot: rid.slot + 5,
    };

    let err = table.get(bogus).unwrap_err();
    assert!(matches!(err, DbError::Storage(_)));
}

#[test]
fn page_slot_bounds_checks() {
    let mut page = Page::new(0);
    let err = page.read_slot(u16::MAX).unwrap_err();
    assert!(matches!(err, DbError::Storage(_)));

    let slot = Slot { offset: 0, len: 0 };
    let err = page.write_slot(u16::MAX, &slot).unwrap_err();
    assert!(matches!(err, DbError::Storage(_)));
}

#[test]
fn append_tuple_respects_size_and_capacity_limits() {
    let mut page = Page::new(0);
    let oversized = vec![0u8; u16::MAX as usize + 1];
    let err = page.append_tuple(&oversized).unwrap_err();
    assert!(format!("{err:?}").contains("exceeds maximum tuple size"));

    let mut page = Page::new(0);
    let massive = vec![0u8; PAGE_SIZE];
    let err = page.append_tuple(&massive).unwrap_err();
    assert!(format!("{err:?}").contains("page full"));
}

#[test]
fn append_tuple_rejects_slot_overflow() {
    let mut page = Page::new(0);
    let mut header = page.header().unwrap();
    header.num_slots = u16::MAX;
    page.write_header(&header).unwrap();

    let err = page.append_tuple(&[1u8]).unwrap_err();
    assert!(matches!(err, DbError::Storage(msg) if msg.contains("slot index overflow")));
}

#[test]
fn heapfile_update_rewrites_rows() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("heap.tbl");
    let mut table = HeapFile::open(&path, 1).unwrap();

    let original = Row::new(vec![Value::Int(1)]);
    let updated = Row::new(vec![Value::Int(2)]);
    let rid = table.insert(&original).unwrap();

    let new_rid = table.update(rid, &updated).unwrap();
    assert_eq!(new_rid, rid);

    let fetched = table.get(new_rid).unwrap();
    assert_eq!(fetched.values, updated.values);
}

#[test]
fn heapfile_update_relocates_when_needed() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("heap.tbl");
    let mut table = HeapFile::open(&path, 1).unwrap();

    let short = Row::new(vec![Value::Text("a".into())]);
    let rid = table.insert(&short).unwrap();

    let long = Row::new(vec![Value::Text(
        "a very long string that exceeds the slot".into(),
    )]);
    let new_rid = table.update(rid, &long).unwrap();
    assert_ne!(new_rid, rid);

    let fetched = table.get(new_rid).unwrap();
    assert_eq!(fetched.values, long.values);

    // Old rid should now be empty
    assert!(table.get(rid).is_err());
}

#[test]
fn ensure_page_exists_rejects_missing_pages() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("heap.tbl");
    let table = HeapFile::open(&path, 1).unwrap();

    let err = table.ensure_page_exists(0).unwrap_err();
    assert!(matches!(err, DbError::Storage(_)));
}

#[test]
fn read_page_returns_default_for_unallocated_ids() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("heap.tbl");
    let mut table = HeapFile::open(&path, 1).unwrap();

    let page_id = 1_000;
    let page = table.read_page(page_id).unwrap();
    let header = page.header().unwrap();

    assert_eq!(page.id, page_id);
    assert_eq!(header.num_slots, 0);
    assert_eq!(header.free_offset, PAGE_SIZE as u16);
}

#[test]
fn delete_rejects_slots_past_header_bounds() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("heap.tbl");
    let mut table = HeapFile::open(&path, 1).unwrap();

    let row = Row::new(vec![Value::Int(123)]);
    let rid = table.insert(&row).unwrap();

    let invalid = RecordId {
        page_id: rid.page_id,
        slot: rid.slot + 10,
    };

    let err = table.delete(invalid).unwrap_err();
    assert!(matches!(err, DbError::Storage(msg) if msg.contains("invalid slot")));
}
