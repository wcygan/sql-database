//! Property-based test generators using proptest.
//!
//! Provides `Arbitrary` implementations and strategies for generating
//! random test data for property-based testing of core database types.

use common::Row;
use proptest::prelude::*;
use types::{SqlType, Value};
use wal::WalRecord;

/// Strategy for generating random `Value` instances.
///
/// Generates a mix of Int, Text, Bool, and Null values.
pub fn arb_value() -> impl Strategy<Value = Value> {
    prop_oneof![
        any::<i64>().prop_map(Value::Int),
        "[a-z]{1,20}".prop_map(Value::Text),
        any::<bool>().prop_map(Value::Bool),
        Just(Value::Null),
    ]
}

/// Strategy for generating random `Row` instances.
///
/// Generates rows with 1-10 columns of random values.
///
/// # Example
///
/// ```
/// use proptest::prelude::*;
/// use testsupport::proptest_generators::arb_row;
///
/// proptest! {
///     #[test]
///     fn test_row_property(row in arb_row()) {
///         // Test invariants about rows
///         assert!(!row.values.is_empty());
///     }
/// }
/// ```
pub fn arb_row() -> impl Strategy<Value = Row> {
    prop::collection::vec(arb_value(), 1..10).prop_map(Row::new)
}

/// Strategy for generating random `Row` instances with a fixed number of columns.
///
/// # Example
///
/// ```
/// use proptest::prelude::*;
/// use testsupport::proptest_generators::arb_row_with_len;
///
/// proptest! {
///     #[test]
///     fn test_fixed_row(row in arb_row_with_len(3)) {
///         assert_eq!(row.values.len(), 3);
///     }
/// }
/// ```
pub fn arb_row_with_len(len: usize) -> impl Strategy<Value = Row> {
    prop::collection::vec(arb_value(), len).prop_map(Row::new)
}

/// Strategy for generating random `SqlType` instances.
pub fn arb_sql_type() -> impl Strategy<Value = SqlType> {
    prop_oneof![Just(SqlType::Int), Just(SqlType::Text), Just(SqlType::Bool),]
}

/// Strategy for generating WAL records for testing.
///
/// Generates Insert, Update, and Delete records with random data.
pub fn arb_wal_record() -> impl Strategy<Value = WalRecord> {
    prop_oneof![
        (any::<u64>(), arb_row(), any::<u64>(), any::<u16>()).prop_map(
            |(table_id, row, page_id, slot)| {
                WalRecord::Insert {
                    table: common::TableId(table_id),
                    row: row.into_values(),
                    rid: common::RecordId {
                        page_id: common::PageId(page_id),
                        slot
                    },
                }
            }
        ),
        (any::<u64>(), arb_row(), any::<u64>(), any::<u16>()).prop_map(
            |(table_id, row, page_id, slot)| {
                WalRecord::Update {
                    table: common::TableId(table_id),
                    new_row: row.into_values(),
                    rid: common::RecordId {
                        page_id: common::PageId(page_id),
                        slot
                    },
                }
            }
        ),
        (any::<u64>(), any::<u64>(), any::<u16>()).prop_map(|(table_id, page_id, slot)| {
            WalRecord::Delete {
                table: common::TableId(table_id),
                rid: common::RecordId {
                    page_id: common::PageId(page_id),
                    slot
                },
            }
        }),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    // Configure proptest to run fewer cases for faster tests
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(50))]

        #[test]
        fn prop_arb_value_always_valid(value in arb_value()) {
            // Every generated value should be one of the four variants
            match value {
                Value::Int(_) | Value::Text(_) | Value::Bool(_) | Value::Null => {}
            }
        }

        #[test]
        fn prop_arb_row_non_empty(row in arb_row()) {
            // Generated rows should never be empty
            assert!(!row.values.is_empty());
            assert!(row.values.len() <= 10);
        }

        #[test]
        fn prop_arb_row_with_len_matches(row in arb_row_with_len(5)) {
            // Fixed-length rows should match the requested length
            assert_eq!(row.values.len(), 5);
        }

        #[test]
        fn prop_value_equality_is_reflexive(value in arb_value()) {
            // Every value should equal itself
            assert_eq!(value, value);
        }

        #[test]
        fn prop_row_roundtrip_clone(row in arb_row()) {
            // Cloning a row should produce an equal row
            let cloned = row.clone();
            assert_eq!(row.values, cloned.values);
        }

        #[test]
        fn prop_wal_record_is_valid(record in arb_wal_record()) {
            // Every generated WAL record should be valid
            match record {
                WalRecord::Insert { table, row, rid } => {
                    assert!(!row.is_empty());
                    let _ = (table, rid); // Ensure fields exist
                }
                WalRecord::Update { table, new_row, rid } => {
                    assert!(!new_row.is_empty());
                    let _ = (table, rid);
                }
                WalRecord::Delete { table, rid } => {
                    let _ = (table, rid);
                }
                _ => {} // CreateTable, DropTable
            }
        }
    }
}

/// Property test helpers for common invariants.
///
/// Note: Serde roundtrip tests are commented out until Row/Value implement
/// bincode's Encode/Decode traits.
pub mod properties {
    // use super::*;
    // use bincode::config;
    //
    // /// Test that a row can be serialized and deserialized without loss.
    // pub fn assert_row_roundtrip_serde(row: &Row) {
    //     let encoded = bincode::encode_to_vec(row, config::legacy()).expect("encode failed");
    //     let (decoded, _): (Row, _) =
    //         bincode::decode_from_slice(&encoded, config::legacy()).expect("decode failed");
    //     assert_eq!(row.values, decoded.values, "Row roundtrip failed");
    // }
    //
    // /// Test that a value can be serialized and deserialized without loss.
    // pub fn assert_value_roundtrip_serde(value: &Value) {
    //     let encoded = bincode::encode_to_vec(value, config::legacy()).expect("encode failed");
    //     let (decoded, _): (Value, _) =
    //         bincode::decode_from_slice(&encoded, config::legacy()).expect("decode failed");
    //     assert_eq!(value, &decoded, "Value roundtrip failed");
    // }
}
