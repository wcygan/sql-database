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

// ============================================================================
// Error Handling and Corruption Tests
// ============================================================================

#[test]
fn corrupted_length_prefix() {
    let dir = tempdir().unwrap();
    let file = dir.path().join("corrupt.wal");

    // Write a valid record
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

    // Corrupt the file by appending garbage length prefix
    {
        use std::fs::OpenOptions;
        use std::io::Write;
        let mut file = OpenOptions::new().append(true).open(&file).unwrap();
        // Write invalid length that points beyond file
        let corrupt_len = u32::MAX;
        file.write_all(&corrupt_len.to_le_bytes()).unwrap();
    }

    // Replay should handle gracefully
    let result = Wal::replay(&file);
    assert!(matches!(result, Err(DbError::Wal(_))));
}

#[test]
fn partial_record_at_eof() {
    let dir = tempdir().unwrap();
    let file = dir.path().join("partial.wal");

    // Write a valid record
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

    // Append partial length prefix (only 2 bytes instead of 4)
    {
        use std::fs::OpenOptions;
        use std::io::Write;
        let mut file = OpenOptions::new().append(true).open(&file).unwrap();
        file.write_all(&[0x01, 0x02]).unwrap();
    }

    // Replay should stop at valid record, ignoring partial
    let replayed = Wal::replay(&file).unwrap();
    assert_eq!(replayed.len(), 1);
}

#[test]
fn corrupted_record_data() {
    let dir = tempdir().unwrap();
    let file = dir.path().join("corrupt_data.wal");

    // Write a valid record
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

    // Append length prefix with garbage data
    {
        use std::fs::OpenOptions;
        use std::io::Write;
        let mut file = OpenOptions::new().append(true).open(&file).unwrap();
        let len = 100u32;
        file.write_all(&len.to_le_bytes()).unwrap();
        // Write random garbage that can't deserialize
        file.write_all(&vec![0xFF; 100]).unwrap();
    }

    // Should fail to deserialize
    let result = Wal::replay(&file);
    assert!(matches!(result, Err(DbError::Wal(_))));
}

// ============================================================================
// Edge Case Tests
// ============================================================================

#[test]
fn very_large_record() {
    let dir = tempdir().unwrap();
    let file = dir.path().join("large.wal");

    let mut wal = Wal::open(&file).unwrap();

    // Create a record with a very large text field (1MB)
    let large_text = "x".repeat(1_000_000);
    wal.append(&WalRecord::Insert {
        table: TableId(1),
        row: vec![Text(large_text.clone())],
        rid: RecordId {
            page_id: PageId(0),
            slot: 0,
        },
    })
    .unwrap();
    wal.sync().unwrap();

    let replayed = Wal::replay(&file).unwrap();
    assert_eq!(replayed.len(), 1);
    match &replayed[0] {
        WalRecord::Insert { row, .. } => {
            assert_eq!(row[0], Text(large_text));
        }
        _ => panic!("wrong record type"),
    }
}

#[test]
fn unicode_and_special_characters() {
    let dir = tempdir().unwrap();
    let file = dir.path().join("unicode.wal");

    let mut wal = Wal::open(&file).unwrap();

    let test_strings = vec![
        "Hello, 世界!",          // Chinese
        "Привет мир",            // Russian
        "مرحبا بالعالم",         // Arabic
        "🚀🎉💻",                // Emojis
        "Tab\tNewline\nQuote\"", // Special chars
        "",                      // Empty string
    ];

    for (i, s) in test_strings.iter().enumerate() {
        wal.append(&WalRecord::Insert {
            table: TableId(1),
            row: vec![Text(s.to_string())],
            rid: RecordId {
                page_id: PageId(0),
                slot: i as u16,
            },
        })
        .unwrap();
    }
    wal.sync().unwrap();

    let replayed = Wal::replay(&file).unwrap();
    assert_eq!(replayed.len(), test_strings.len());

    for (i, rec) in replayed.iter().enumerate() {
        match rec {
            WalRecord::Insert { row, .. } => {
                assert_eq!(row[0], Text(test_strings[i].to_string()));
            }
            _ => panic!("wrong record type"),
        }
    }
}

#[test]
fn extreme_table_and_page_ids() {
    let dir = tempdir().unwrap();
    let file = dir.path().join("extreme_ids.wal");

    let mut wal = Wal::open(&file).unwrap();

    let extreme_values = vec![
        (TableId(0), PageId(0)),
        (TableId(u64::MAX), PageId(u64::MAX)),
        (TableId(1), PageId(u64::MAX / 2)),
    ];

    for (table, page) in extreme_values {
        wal.append(&WalRecord::Insert {
            table,
            row: vec![Int(42)],
            rid: RecordId {
                page_id: page,
                slot: 0,
            },
        })
        .unwrap();
    }
    wal.sync().unwrap();

    let replayed = Wal::replay(&file).unwrap();
    assert_eq!(replayed.len(), 3);
}

#[test]
fn record_with_many_fields() {
    let dir = tempdir().unwrap();
    let file = dir.path().join("many_fields.wal");

    let mut wal = Wal::open(&file).unwrap();

    // Create record with 1000 fields
    let mut row = Vec::new();
    for i in 0..1000 {
        row.push(Int(i));
    }

    wal.append(&WalRecord::Insert {
        table: TableId(1),
        row: row.clone(),
        rid: RecordId {
            page_id: PageId(0),
            slot: 0,
        },
    })
    .unwrap();
    wal.sync().unwrap();

    let replayed = Wal::replay(&file).unwrap();
    match &replayed[0] {
        WalRecord::Insert {
            row: replayed_row, ..
        } => {
            assert_eq!(replayed_row.len(), 1000);
            assert_eq!(replayed_row[0], Int(0));
            assert_eq!(replayed_row[999], Int(999));
        }
        _ => panic!("wrong record type"),
    }
}

#[test]
fn mixed_value_types_in_row() {
    let dir = tempdir().unwrap();
    let file = dir.path().join("mixed.wal");

    let mut wal = Wal::open(&file).unwrap();

    wal.append(&WalRecord::Insert {
        table: TableId(1),
        row: vec![
            Int(i64::MIN),
            Int(i64::MAX),
            Text("".to_string()),
            Text("very long string ".repeat(100)),
            Bool(true),
            Bool(false),
            Null,
            Int(0),
        ],
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
            assert_eq!(row.len(), 8);
            assert_eq!(row[0], Int(i64::MIN));
            assert_eq!(row[1], Int(i64::MAX));
            assert_eq!(row[4], Bool(true));
            assert_eq!(row[6], Null);
        }
        _ => panic!("wrong record type"),
    }
}

// ============================================================================
// Recovery and Idempotency Tests
// ============================================================================

#[test]
fn replay_multiple_times_idempotent() {
    let dir = tempdir().unwrap();
    let file = dir.path().join("idempotent.wal");

    {
        let mut wal = Wal::open(&file).unwrap();
        for i in 0..10 {
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
    }

    // Replay multiple times should give same result
    let replay1 = Wal::replay(&file).unwrap();
    let replay2 = Wal::replay(&file).unwrap();
    let replay3 = Wal::replay(&file).unwrap();

    assert_eq!(replay1, replay2);
    assert_eq!(replay2, replay3);
    assert_eq!(replay1.len(), 10);
}

#[test]
fn replay_then_continue_appending() {
    let dir = tempdir().unwrap();
    let file = dir.path().join("continue.wal");

    // First session: write records
    {
        let mut wal = Wal::open(&file).unwrap();
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
    }

    // Simulate recovery: replay
    let replayed = Wal::replay(&file).unwrap();
    assert_eq!(replayed.len(), 5);

    // Second session: continue appending
    {
        let mut wal = Wal::open(&file).unwrap();
        for i in 5..10 {
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
    }

    // Final replay should show all records
    let final_replay = Wal::replay(&file).unwrap();
    assert_eq!(final_replay.len(), 10);
}

#[test]
fn interleaved_record_types() {
    let dir = tempdir().unwrap();
    let file = dir.path().join("interleaved.wal");

    let mut wal = Wal::open(&file).unwrap();

    // Simulate realistic workload with mixed operations
    for i in 0..100 {
        let rec = match i % 5 {
            0 => WalRecord::Insert {
                table: TableId(i / 5),
                row: vec![Int(i as i64)],
                rid: RecordId {
                    page_id: PageId(i / 10),
                    slot: (i % 10) as u16,
                },
            },
            1 => WalRecord::Update {
                table: TableId(i / 5),
                rid: RecordId {
                    page_id: PageId(i / 10),
                    slot: (i % 10) as u16,
                },
                new_row: vec![Int(i as i64 * 2)],
            },
            2 => WalRecord::Delete {
                table: TableId(i / 5),
                rid: RecordId {
                    page_id: PageId(i / 10),
                    slot: (i % 10) as u16,
                },
            },
            3 => WalRecord::CreateTable {
                name: format!("table_{}", i),
                table: TableId(i),
            },
            _ => WalRecord::DropTable { table: TableId(i) },
        };
        wal.append(&rec).unwrap();
    }
    wal.sync().unwrap();

    let replayed = Wal::replay(&file).unwrap();
    assert_eq!(replayed.len(), 100);

    // Verify order is preserved
    for i in 0..100 {
        match (i % 5, &replayed[i]) {
            (0, WalRecord::Insert { .. }) => {}
            (1, WalRecord::Update { .. }) => {}
            (2, WalRecord::Delete { .. }) => {}
            (3, WalRecord::CreateTable { .. }) => {}
            (4, WalRecord::DropTable { .. }) => {}
            _ => panic!("record order not preserved"),
        }
    }
}

// ============================================================================
// Stress Tests
// ============================================================================

#[test]
fn stress_many_small_records() {
    let dir = tempdir().unwrap();
    let file = dir.path().join("stress_small.wal");

    let mut wal = Wal::open(&file).unwrap();

    // Write 10,000 small records
    let count = 10_000;
    for i in 0..count {
        wal.append(&WalRecord::Insert {
            table: TableId(1),
            row: vec![Int(i)],
            rid: RecordId {
                page_id: PageId((i / 100) as u64),
                slot: (i % 100) as u16,
            },
        })
        .unwrap();
    }
    wal.sync().unwrap();

    let replayed = Wal::replay(&file).unwrap();
    assert_eq!(replayed.len(), count as usize);
}

#[test]
fn stress_few_large_records() {
    let dir = tempdir().unwrap();
    let file = dir.path().join("stress_large.wal");

    let mut wal = Wal::open(&file).unwrap();

    // Write 10 records with 10KB each
    for i in 0..10 {
        let large_data = "x".repeat(10_000);
        wal.append(&WalRecord::Insert {
            table: TableId(1),
            row: vec![Text(large_data), Int(i)],
            rid: RecordId {
                page_id: PageId(i as u64),
                slot: 0,
            },
        })
        .unwrap();
    }
    wal.sync().unwrap();

    let replayed = Wal::replay(&file).unwrap();
    assert_eq!(replayed.len(), 10);
}

#[test]
fn stress_mixed_sizes() {
    let dir = tempdir().unwrap();
    let file = dir.path().join("stress_mixed.wal");

    let mut wal = Wal::open(&file).unwrap();

    // Mix of small, medium, and large records
    for i in 0..100 {
        let row = match i % 3 {
            0 => vec![Int(i)],                      // Small
            1 => vec![Text("medium ".repeat(100))], // Medium
            _ => vec![Text("large ".repeat(1000))], // Large
        };

        wal.append(&WalRecord::Insert {
            table: TableId(1),
            row,
            rid: RecordId {
                page_id: PageId((i / 10) as u64),
                slot: (i % 10) as u16,
            },
        })
        .unwrap();
    }
    wal.sync().unwrap();

    let replayed = Wal::replay(&file).unwrap();
    assert_eq!(replayed.len(), 100);
}

// ============================================================================
// Integration and Real-World Scenarios
// ============================================================================

#[test]
fn simulation_crash_recovery() {
    let dir = tempdir().unwrap();
    let file = dir.path().join("crash.wal");

    // Phase 1: Normal operations before "crash"
    {
        let mut wal = Wal::open(&file).unwrap();
        for i in 0..50 {
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
        // Simulate crash - WAL not closed properly
    }

    // Phase 2: Recovery - replay WAL
    let recovered = Wal::replay(&file).unwrap();
    assert_eq!(recovered.len(), 50);

    // Phase 3: Continue operations after recovery
    {
        let mut wal = Wal::open(&file).unwrap();
        for i in 50..100 {
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
    }

    let final_state = Wal::replay(&file).unwrap();
    assert_eq!(final_state.len(), 100);
}

#[test]
fn checkpoint_workflow() {
    let dir = tempdir().unwrap();
    let file = dir.path().join("checkpoint.wal");

    let mut wal = Wal::open(&file).unwrap();

    // Write initial batch
    for i in 0..100 {
        wal.append(&WalRecord::Insert {
            table: TableId(1),
            row: vec![Int(i)],
            rid: RecordId {
                page_id: PageId((i / 10) as u64),
                slot: (i % 10) as u16,
            },
        })
        .unwrap();
    }
    wal.sync().unwrap();

    // Simulate checkpoint: replay and apply to storage
    let records = Wal::replay(&file).unwrap();
    assert_eq!(records.len(), 100);
    // (In real system: apply all records to storage here)

    // After successful checkpoint, truncate WAL
    wal.truncate().unwrap();

    // Verify WAL is empty
    let after_checkpoint = Wal::replay(&file).unwrap();
    assert_eq!(after_checkpoint.len(), 0);

    // Continue with new operations
    for i in 100..150 {
        wal.append(&WalRecord::Insert {
            table: TableId(1),
            row: vec![Int(i)],
            rid: RecordId {
                page_id: PageId((i / 10) as u64),
                slot: (i % 10) as u16,
            },
        })
        .unwrap();
    }
    wal.sync().unwrap();

    let new_records = Wal::replay(&file).unwrap();
    assert_eq!(new_records.len(), 50);
}

#[test]
fn max_slot_value() {
    let dir = tempdir().unwrap();
    let file = dir.path().join("max_slot.wal");

    let mut wal = Wal::open(&file).unwrap();

    wal.append(&WalRecord::Insert {
        table: TableId(1),
        row: vec![Int(42)],
        rid: RecordId {
            page_id: PageId(0),
            slot: u16::MAX,
        },
    })
    .unwrap();
    wal.sync().unwrap();

    let replayed = Wal::replay(&file).unwrap();
    match &replayed[0] {
        WalRecord::Insert { rid, .. } => {
            assert_eq!(rid.slot, u16::MAX);
        }
        _ => panic!("wrong record type"),
    }
}
