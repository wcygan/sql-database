//! Raft log entry command types.
//!
//! Commands represent database operations that are replicated through the Raft log.
//! Unlike `WalRecord`, INSERT commands do not include the `rid` (record ID) since
//! it is assigned during state machine application.

use common::{RecordId, TableId};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::mpsc;
use types::Value;

/// Event sent when Raft applies an entry to the state machine.
///
/// These events can be used for monitoring replication in real-time.
#[derive(Clone, Debug)]
pub struct RaftActivityEvent {
    /// Log index of the applied entry
    pub log_index: u64,
    /// Term of the applied entry
    pub term: u64,
    /// Description of what was applied
    pub description: String,
}

impl RaftActivityEvent {
    /// Create a new activity event.
    pub fn new(log_index: u64, term: u64, description: impl Into<String>) -> Self {
        Self {
            log_index,
            term,
            description: description.into(),
        }
    }

    /// Create an event from a command.
    pub fn from_command(log_index: u64, term: u64, cmd: &Command) -> Self {
        let description = match cmd {
            Command::Insert { table_id, row } => {
                format!("INSERT table={} cols={}", table_id.0, row.len())
            }
            Command::Update { table_id, rid, .. } => {
                format!(
                    "UPDATE table={} rid={}:{}",
                    table_id.0, rid.page_id.0, rid.slot
                )
            }
            Command::Delete { table_id, rid } => {
                format!(
                    "DELETE table={} rid={}:{}",
                    table_id.0, rid.page_id.0, rid.slot
                )
            }
            Command::CreateTable { name, .. } => {
                format!("CREATE TABLE {}", name)
            }
            Command::DropTable { table_id } => {
                format!("DROP TABLE id={}", table_id.0)
            }
            Command::CreateIndex {
                table_id,
                index_name,
                ..
            } => {
                format!("CREATE INDEX {} on table={}", index_name, table_id.0)
            }
            Command::DropIndex {
                table_id,
                index_name,
            } => {
                format!("DROP INDEX {} on table={}", index_name, table_id.0)
            }
        };
        Self::new(log_index, term, description)
    }

    /// Create an event for a membership change.
    pub fn membership(log_index: u64, term: u64) -> Self {
        Self::new(log_index, term, "MEMBERSHIP CHANGE")
    }

    /// Create an event for a blank entry.
    pub fn blank(log_index: u64, term: u64) -> Self {
        Self::new(log_index, term, "BLANK (heartbeat)")
    }
}

/// Sender for Raft activity events.
pub type ActivitySender = Arc<mpsc::UnboundedSender<RaftActivityEvent>>;

/// Receiver for Raft activity events.
pub type ActivityReceiver = mpsc::UnboundedReceiver<RaftActivityEvent>;

/// Create a new activity event channel.
pub fn activity_channel() -> (ActivitySender, ActivityReceiver) {
    let (tx, rx) = mpsc::unbounded_channel();
    (Arc::new(tx), rx)
}

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
    fn activity_event_from_insert_command() {
        let cmd = Command::Insert {
            table_id: TableId(42),
            row: vec![Value::Int(1), Value::Text("test".into()), Value::Bool(true)],
        };
        let event = RaftActivityEvent::from_command(10, 5, &cmd);
        assert_eq!(event.log_index, 10);
        assert_eq!(event.term, 5);
        assert_eq!(event.description, "INSERT table=42 cols=3");
    }

    #[test]
    fn activity_event_from_update_command() {
        let cmd = Command::Update {
            table_id: TableId(7),
            rid: RecordId {
                page_id: PageId(3),
                slot: 15,
            },
            new_row: vec![Value::Int(999)],
        };
        let event = RaftActivityEvent::from_command(100, 12, &cmd);
        assert_eq!(event.log_index, 100);
        assert_eq!(event.term, 12);
        assert_eq!(event.description, "UPDATE table=7 rid=3:15");
    }

    #[test]
    fn activity_event_from_delete_command() {
        let cmd = Command::Delete {
            table_id: TableId(5),
            rid: RecordId {
                page_id: PageId(0),
                slot: 2,
            },
        };
        let event = RaftActivityEvent::from_command(50, 8, &cmd);
        assert_eq!(event.log_index, 50);
        assert_eq!(event.term, 8);
        assert_eq!(event.description, "DELETE table=5 rid=0:2");
    }

    #[test]
    fn activity_event_from_create_table_command() {
        let cmd = Command::CreateTable {
            name: "users".to_string(),
            table_id: TableId(1),
            columns: vec![],
            primary_key: None,
        };
        let event = RaftActivityEvent::from_command(1, 1, &cmd);
        assert_eq!(event.description, "CREATE TABLE users");
    }

    #[test]
    fn activity_event_from_drop_table_command() {
        let cmd = Command::DropTable {
            table_id: TableId(99),
        };
        let event = RaftActivityEvent::from_command(200, 15, &cmd);
        assert_eq!(event.description, "DROP TABLE id=99");
    }

    #[test]
    fn activity_event_from_create_index_command() {
        let cmd = Command::CreateIndex {
            table_id: TableId(3),
            index_name: "idx_users_email".to_string(),
            columns: vec!["email".to_string()],
        };
        let event = RaftActivityEvent::from_command(25, 3, &cmd);
        assert_eq!(event.description, "CREATE INDEX idx_users_email on table=3");
    }

    #[test]
    fn activity_event_from_drop_index_command() {
        let cmd = Command::DropIndex {
            table_id: TableId(3),
            index_name: "idx_users_email".to_string(),
        };
        let event = RaftActivityEvent::from_command(26, 3, &cmd);
        assert_eq!(event.description, "DROP INDEX idx_users_email on table=3");
    }

    #[test]
    fn activity_event_membership() {
        let event = RaftActivityEvent::membership(77, 9);
        assert_eq!(event.log_index, 77);
        assert_eq!(event.term, 9);
        assert_eq!(event.description, "MEMBERSHIP CHANGE");
    }

    #[test]
    fn activity_event_blank() {
        let event = RaftActivityEvent::blank(1000, 20);
        assert_eq!(event.log_index, 1000);
        assert_eq!(event.term, 20);
        assert_eq!(event.description, "BLANK (heartbeat)");
    }

    #[tokio::test]
    async fn activity_channel_sends_and_receives() {
        let (tx, mut rx) = activity_channel();

        // Send multiple events
        tx.send(RaftActivityEvent::new(1, 1, "test event 1"))
            .unwrap();
        tx.send(RaftActivityEvent::new(2, 1, "test event 2"))
            .unwrap();
        tx.send(RaftActivityEvent::blank(3, 1)).unwrap();

        // Receive and verify
        let e1 = rx.recv().await.unwrap();
        assert_eq!(e1.log_index, 1);
        assert_eq!(e1.description, "test event 1");

        let e2 = rx.recv().await.unwrap();
        assert_eq!(e2.log_index, 2);
        assert_eq!(e2.description, "test event 2");

        let e3 = rx.recv().await.unwrap();
        assert_eq!(e3.log_index, 3);
        assert_eq!(e3.description, "BLANK (heartbeat)");
    }

    #[tokio::test]
    async fn activity_channel_arc_sender_is_clonable() {
        let (tx, mut rx) = activity_channel();

        // Clone the sender (via Arc)
        let tx2 = tx.clone();

        // Send from both senders
        tx.send(RaftActivityEvent::new(1, 1, "from tx1")).unwrap();
        tx2.send(RaftActivityEvent::new(2, 1, "from tx2")).unwrap();

        // Both should be received
        let e1 = rx.recv().await.unwrap();
        let e2 = rx.recv().await.unwrap();

        assert_eq!(e1.description, "from tx1");
        assert_eq!(e2.description, "from tx2");
    }

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
