//! Raft log entry command types.
//!
//! Commands represent database operations that are replicated through the Raft log.
//! Unlike `WalRecord`, INSERT commands do not include the `rid` (record ID) since
//! it is assigned during state machine application.

use common::{RecordId, TableId};
use serde::{Deserialize, Serialize};
use types::Value;

/// A database command to be replicated through Raft consensus.
///
/// Each command represents a single database operation. Commands are serialized
/// into Raft log entries and applied to all replicas in the same order.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Command {
    /// Insert a new row into a table.
    /// The `rid` is assigned during state machine application.
    Insert { table_id: TableId, row: Vec<Value> },

    /// Update an existing row.
    Update {
        table_id: TableId,
        rid: RecordId,
        new_row: Vec<Value>,
    },

    /// Delete a row.
    Delete { table_id: TableId, rid: RecordId },

    /// Create a new table.
    CreateTable {
        name: String,
        table_id: TableId,
        columns: Vec<ColumnDef>,
        primary_key: Option<Vec<String>>,
    },

    /// Drop a table.
    DropTable { table_id: TableId },

    /// Create an index on a table.
    CreateIndex {
        table_id: TableId,
        index_name: String,
        columns: Vec<String>,
    },

    /// Drop an index.
    DropIndex {
        table_id: TableId,
        index_name: String,
    },
}

/// Column definition for table creation.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ColumnDef {
    pub name: String,
    pub type_name: String,
}

/// Response from applying a command to the state machine.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum CommandResponse {
    /// Successful insert, returns the assigned record ID.
    Insert { rid: RecordId },

    /// Successful update.
    Update { rows_affected: u64 },

    /// Successful delete.
    Delete { rows_affected: u64 },

    /// Successful DDL operation.
    Ddl,

    /// Operation failed.
    Error { message: String },
}

impl CommandResponse {
    /// Create a successful insert response.
    pub fn insert(rid: RecordId) -> Self {
        Self::Insert { rid }
    }

    /// Create a successful update response.
    pub fn update(rows_affected: u64) -> Self {
        Self::Update { rows_affected }
    }

    /// Create a successful delete response.
    pub fn delete(rows_affected: u64) -> Self {
        Self::Delete { rows_affected }
    }

    /// Create a successful DDL response.
    pub fn ddl() -> Self {
        Self::Ddl
    }

    /// Create an error response.
    pub fn error(message: impl Into<String>) -> Self {
        Self::Error {
            message: message.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::PageId;

    #[test]
    fn command_serialization_roundtrip() {
        let commands = vec![
            Command::Insert {
                table_id: TableId(1),
                row: vec![Value::Int(42), Value::Text("hello".to_string())],
            },
            Command::Update {
                table_id: TableId(2),
                rid: RecordId {
                    page_id: PageId(0),
                    slot: 5,
                },
                new_row: vec![Value::Bool(true)],
            },
            Command::Delete {
                table_id: TableId(3),
                rid: RecordId {
                    page_id: PageId(1),
                    slot: 10,
                },
            },
            Command::CreateTable {
                name: "users".to_string(),
                table_id: TableId(4),
                columns: vec![
                    ColumnDef {
                        name: "id".to_string(),
                        type_name: "INT".to_string(),
                    },
                    ColumnDef {
                        name: "name".to_string(),
                        type_name: "TEXT".to_string(),
                    },
                ],
                primary_key: Some(vec!["id".to_string()]),
            },
        ];

        for cmd in commands {
            let json = serde_json::to_string(&cmd).unwrap();
            let decoded: Command = serde_json::from_str(&json).unwrap();
            assert_eq!(cmd, decoded);
        }
    }

    #[test]
    fn response_variants() {
        let rid = RecordId {
            page_id: PageId(0),
            slot: 0,
        };

        let responses = vec![
            CommandResponse::insert(rid),
            CommandResponse::update(5),
            CommandResponse::delete(3),
            CommandResponse::ddl(),
            CommandResponse::error("something went wrong"),
        ];

        for resp in responses {
            let json = serde_json::to_string(&resp).unwrap();
            let decoded: CommandResponse = serde_json::from_str(&json).unwrap();
            assert_eq!(resp, decoded);
        }
    }
}
