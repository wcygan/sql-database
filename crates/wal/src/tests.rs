use super::*;
use common::{PageId, RecordId, TableId};
use tempfile::tempdir;
use types::Value::*;

#[test]
fn append_and_replay_records() {
    let dir = tempdir().unwrap();
    let file = dir.path().join("toydb.wal");

    let mut wal = Wal::open(&file).unwrap();
    let rec1 = WalRecord::Insert {
        table: TableId(1),
        row: vec![Int(1), Text("Will".into())],
        rid: RecordId {
            page_id: PageId(0),
            slot: 0,
        },
    };
    wal.append(&rec1).unwrap();
    wal.sync().unwrap();

    let rec2 = WalRecord::Delete {
        table: TableId(1),
        rid: RecordId {
            page_id: PageId(0),
            slot: 0,
        },
    };
    wal.append(&rec2).unwrap();
    wal.sync().unwrap();

    let replayed = Wal::replay(&file).unwrap();
    assert_eq!(replayed.len(), 2);

    match &replayed[0] {
        WalRecord::Insert { row, .. } => assert_eq!(row[0], Int(1)),
        _ => panic!("wrong record type"),
    }
}

#[test]
fn all_record_types_roundtrip() {
    let dir = tempdir().unwrap();
    let file = dir.path().join("toydb.wal");

    let mut wal = Wal::open(&file).unwrap();

    let records = vec![
        WalRecord::Insert {
            table: TableId(1),
            row: vec![Int(42), Text("test".into()), Bool(true)],
            rid: RecordId {
                page_id: PageId(0),
                slot: 1,
            },
        },
        WalRecord::Update {
            table: TableId(2),
            rid: RecordId {
                page_id: PageId(1),
                slot: 5,
            },
            new_row: vec![Int(100), Null],
        },
        WalRecord::Delete {
            table: TableId(3),
            rid: RecordId {
                page_id: PageId(2),
                slot: 10,
            },
        },
        WalRecord::CreateTable {
            name: "users".to_string(),
            table: TableId(4),
        },
        WalRecord::DropTable { table: TableId(5) },
    ];

    for rec in &records {
        wal.append(rec).unwrap();
    }
    wal.sync().unwrap();

    let replayed = Wal::replay(&file).unwrap();
    assert_eq!(replayed, records);
}

#[test]
fn truncate_clears_wal() {
    let dir = tempdir().unwrap();
    let file = dir.path().join("toydb.wal");

    let mut wal = Wal::open(&file).unwrap();

    // Append some records
    for i in 0..10 {
        let rec = WalRecord::Insert {
            table: TableId(1),
            row: vec![Int(i)],
            rid: RecordId {
                page_id: PageId(0),
                slot: i as u16,
            },
        };
        wal.append(&rec).unwrap();
    }
    wal.sync().unwrap();

    // Verify records exist
    let replayed = Wal::replay(&file).unwrap();
    assert_eq!(replayed.len(), 10);

    // Truncate
    wal.truncate().unwrap();

    // Verify WAL is empty
    let replayed = Wal::replay(&file).unwrap();
    assert_eq!(replayed.len(), 0);

    // Verify can still append after truncate
    let rec = WalRecord::Insert {
        table: TableId(1),
        row: vec![Int(999)],
        rid: RecordId {
            page_id: PageId(0),
            slot: 0,
        },
    };
    wal.append(&rec).unwrap();
    wal.sync().unwrap();

    let replayed = Wal::replay(&file).unwrap();
    assert_eq!(replayed.len(), 1);
}

#[test]
fn replay_empty_wal() {
    let dir = tempdir().unwrap();
    let file = dir.path().join("empty.wal");

    // Create empty WAL
    let _wal = Wal::open(&file).unwrap();

    // Replay should return empty vector
    let replayed = Wal::replay(&file).unwrap();
    assert_eq!(replayed.len(), 0);
}

#[test]
fn replay_nonexistent_file_returns_error() {
    let dir = tempdir().unwrap();
    let file = dir.path().join("nonexistent.wal");

    let result = Wal::replay(&file);
    assert!(matches!(result, Err(DbError::Wal(_))));
}

#[test]
fn large_batch_operations() {
    let dir = tempdir().unwrap();
    let file = dir.path().join("toydb.wal");

    let mut wal = Wal::open(&file).unwrap();

    // Append 1000 records
    let count = 1000;
    for i in 0..count {
        let rec = WalRecord::Insert {
            table: TableId(i / 100),
            row: vec![Int(i as i64), Text(format!("row_{}", i))],
            rid: RecordId {
                page_id: PageId(i / 10),
                slot: (i % 10) as u16,
            },
        };
        wal.append(&rec).unwrap();
    }
    wal.sync().unwrap();

    // Replay and verify
    let replayed = Wal::replay(&file).unwrap();
    assert_eq!(replayed.len(), count as usize);

    // Verify first and last records
    match &replayed[0] {
        WalRecord::Insert { row, .. } => {
            assert_eq!(row[0], Int(0));
            assert_eq!(row[1], Text("row_0".to_string()));
        }
        _ => panic!("wrong record type"),
    }

    match &replayed[count as usize - 1] {
        WalRecord::Insert { row, .. } => {
            assert_eq!(row[0], Int((count - 1) as i64));
            assert_eq!(row[1], Text(format!("row_{}", count - 1)));
        }
        _ => panic!("wrong record type"),
    }
}

#[test]
fn multiple_append_sync_cycles() {
    let dir = tempdir().unwrap();
    let file = dir.path().join("toydb.wal");

    let mut wal = Wal::open(&file).unwrap();

    // First batch
    for i in 0..5 {
        wal.append(&WalRecord::Insert {
            table: TableId(1),
            row: vec![Int(i)],
            rid: RecordId {
                page_id: PageId(0),
                slot: i as u16,
            },
        })
        .unwrap();
    }
    wal.sync().unwrap();

    // Second batch
    for i in 5..10 {
        wal.append(&WalRecord::Update {
            table: TableId(1),
            rid: RecordId {
                page_id: PageId(0),
                slot: (i - 5) as u16,
            },
            new_row: vec![Int(i)],
        })
        .unwrap();
    }
    wal.sync().unwrap();

    // Third batch
    for i in 0..5 {
        wal.append(&WalRecord::Delete {
            table: TableId(1),
            rid: RecordId {
                page_id: PageId(0),
                slot: i as u16,
            },
        })
        .unwrap();
    }
    wal.sync().unwrap();

    let replayed = Wal::replay(&file).unwrap();
    assert_eq!(replayed.len(), 15);
}

#[test]
fn persist_across_wal_instances() {
    let dir = tempdir().unwrap();
    let file = dir.path().join("toydb.wal");

    // First instance writes
    {
        let mut wal = Wal::open(&file).unwrap();
        wal.append(&WalRecord::Insert {
            table: TableId(1),
            row: vec![Int(1)],
            rid: RecordId {
                page_id: PageId(0),
                slot: 0,
            },
        })
        .unwrap();
        wal.sync().unwrap();
    }

    // Second instance appends more
    {
        let mut wal = Wal::open(&file).unwrap();
        wal.append(&WalRecord::Insert {
            table: TableId(1),
            row: vec![Int(2)],
            rid: RecordId {
                page_id: PageId(0),
                slot: 1,
            },
        })
        .unwrap();
        wal.sync().unwrap();
    }

    // Replay should show both
    let replayed = Wal::replay(&file).unwrap();
    assert_eq!(replayed.len(), 2);
}

#[test]
fn append_without_sync_then_replay() {
    let dir = tempdir().unwrap();
    let file = dir.path().join("toydb.wal");

    {
        let mut wal = Wal::open(&file).unwrap();
        wal.append(&WalRecord::Insert {
            table: TableId(1),
            row: vec![Int(1)],
            rid: RecordId {
                page_id: PageId(0),
                slot: 0,
            },
        })
        .unwrap();
        // No sync - but append() calls flush()
    }

    // Should still be readable (flush() was called)
    let replayed = Wal::replay(&file).unwrap();
    assert_eq!(replayed.len(), 1);
}

#[test]
fn create_table_and_drop_table_records() {
    let dir = tempdir().unwrap();
    let file = dir.path().join("toydb.wal");

    let mut wal = Wal::open(&file).unwrap();

    wal.append(&WalRecord::CreateTable {
        name: "users".to_string(),
        table: TableId(1),
    })
    .unwrap();

    wal.append(&WalRecord::CreateTable {
        name: "posts".to_string(),
        table: TableId(2),
    })
    .unwrap();

    wal.append(&WalRecord::DropTable { table: TableId(1) })
        .unwrap();

    wal.sync().unwrap();

    let replayed = Wal::replay(&file).unwrap();
    assert_eq!(replayed.len(), 3);

    match &replayed[0] {
        WalRecord::CreateTable { name, table } => {
            assert_eq!(name, "users");
            assert_eq!(*table, TableId(1));
        }
        _ => panic!("wrong record type"),
    }

    match &replayed[2] {
        WalRecord::DropTable { table } => {
            assert_eq!(*table, TableId(1));
        }
        _ => panic!("wrong record type"),
    }
}

#[test]
fn null_values_in_records() {
    let dir = tempdir().unwrap();
    let file = dir.path().join("toydb.wal");

    let mut wal = Wal::open(&file).unwrap();

    wal.append(&WalRecord::Insert {
        table: TableId(1),
        row: vec![Int(1), Null, Text("test".into()), Null],
        rid: RecordId {
            page_id: PageId(0),
            slot: 0,
        },
    })
    .unwrap();
    wal.sync().unwrap();

    let replayed = Wal::replay(&file).unwrap();
    match &replayed[0] {
        WalRecord::Insert { row, .. } => {
            assert_eq!(row[0], Int(1));
            assert_eq!(row[1], Null);
            assert_eq!(row[2], Text("test".into()));
            assert_eq!(row[3], Null);
        }
        _ => panic!("wrong record type"),
    }
}

#[test]
fn empty_row_in_insert() {
    let dir = tempdir().unwrap();
    let file = dir.path().join("toydb.wal");

    let mut wal = Wal::open(&file).unwrap();

    wal.append(&WalRecord::Insert {
        table: TableId(1),
        row: vec![],
        rid: RecordId {
            page_id: PageId(0),
            slot: 0,
        },
    })
    .unwrap();
    wal.sync().unwrap();

    let replayed = Wal::replay(&file).unwrap();
    match &replayed[0] {
        WalRecord::Insert { row, .. } => {
            assert!(row.is_empty());
        }
        _ => panic!("wrong record type"),
    }
}

#[test]
fn truncate_then_append_sequence() {
    let dir = tempdir().unwrap();
    let file = dir.path().join("toydb.wal");

    let mut wal = Wal::open(&file).unwrap();

    // First batch
    for i in 0..5 {
        wal.append(&WalRecord::Insert {
            table: TableId(1),
            row: vec![Int(i)],
            rid: RecordId {
                page_id: PageId(0),
                slot: i as u16,
            },
        })
        .unwrap();
    }
    wal.sync().unwrap();

    // Truncate
    wal.truncate().unwrap();

    // Second batch after truncate
    for i in 10..15 {
        wal.append(&WalRecord::Insert {
            table: TableId(2),
            row: vec![Int(i)],
            rid: RecordId {
                page_id: PageId(1),
                slot: (i - 10) as u16,
            },
        })
        .unwrap();
    }
    wal.sync().unwrap();

    let replayed = Wal::replay(&file).unwrap();
    assert_eq!(replayed.len(), 5);
    match &replayed[0] {
        WalRecord::Insert { table, row, .. } => {
            assert_eq!(*table, TableId(2));
            assert_eq!(row[0], Int(10));
        }
        _ => panic!("wrong record type"),
    }
}
