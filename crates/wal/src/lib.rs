//! Write-Ahead Log (WAL) for crash recovery and durability.
//!
//! The WAL ensures that every change to the database is first recorded in a durable,
//! sequential log before it's applied to storage. If the process crashes mid-update,
//! the WAL can replay ("redo") those operations to bring storage back to a consistent state.
//!
//! # Architecture
//!
//! - **Redo-only**: Simplifies recovery (no UNDO / transaction rollback needed)
//! - **Logical records**: Stable across page formats, easy to reason about
//! - **Length-prefixed framing**: Safe forward iteration and truncation
//! - **Fsync after batch**: Guarantees durability before acknowledgment
//! - **Single WAL file**: Simple for single-writer architecture
//!
//! # Example
//!
//! ```no_run
//! use wal::{Wal, WalRecord};
//! use common::{TableId, RecordId, PageId};
//! use types::Value;
//!
//! let mut wal = Wal::open("data/toydb.wal").unwrap();
//!
//! // Append a record
//! let record = WalRecord::Insert {
//!     table: TableId(1),
//!     row: vec![Value::Int(42), Value::Text("hello".into())],
//!     rid: RecordId { page_id: PageId(0), slot: 0 },
//! };
//! wal.append(&record).unwrap();
//! wal.sync().unwrap();
//!
//! // Replay on recovery
//! let records = Wal::replay("data/toydb.wal").unwrap();
//! for rec in records {
//!     // Apply each record to storage
//! }
//! ```

#[cfg(test)]
mod tests;

use bincode::config::{self, Config};
use bincode::serde::{decode_from_slice, encode_to_vec};
use common::{DbError, DbResult, RecordId, TableId};
use serde::{Deserialize, Serialize};
use std::{
    fs::{File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
};
use types::Value;

/// A logical change to the database that can be written to the WAL and replayed.
///
/// Each variant represents a different type of database operation:
/// - DML: Insert, Update, Delete
/// - DDL: CreateTable, DropTable
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum WalRecord {
    /// Insert a new row into a table.
    Insert {
        table: TableId,
        row: Vec<Value>,
        rid: RecordId,
    },
    /// Update an existing row.
    Update {
        table: TableId,
        rid: RecordId,
        new_row: Vec<Value>,
    },
    /// Delete a row.
    Delete { table: TableId, rid: RecordId },
    /// Create a new table.
    CreateTable { name: String, table: TableId },
    /// Drop a table.
    DropTable { table: TableId },
}

/// Write-Ahead Log manager.
///
/// Manages a single WAL file with append-only writes and sequential replay.
/// Records are length-prefixed (4-byte LE) for safe iteration.
#[derive(Debug)]
pub struct Wal {
    path: PathBuf,
    file: File,
}

impl Wal {
    /// Open or create a WAL file at the given path.
    ///
    /// The file is opened in append mode to preserve existing records.
    ///
    /// # Errors
    ///
    /// Returns `DbError::Wal` if the file cannot be opened.
    pub fn open(path: impl AsRef<Path>) -> DbResult<Self> {
        let path = path.as_ref().to_path_buf();
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .truncate(false)
            .open(&path)
            .map_err(|e| DbError::Wal(format!("Failed to open WAL file: {}", e)))?;

        Ok(Self { path, file })
    }

    /// Append a record to the WAL.
    ///
    /// The record is serialized with bincode and written with a 4-byte length prefix.
    /// After writing, the file buffer is flushed (but not fsynced - use `sync()` for durability).
    ///
    /// # Errors
    ///
    /// Returns `DbError::Wal` if serialization or writing fails.
    pub fn append(&mut self, rec: &WalRecord) -> DbResult<()> {
        let bytes = encode_to_vec(rec, bincode_config())
            .map_err(|e| DbError::Wal(format!("Failed to serialize record: {}", e)))?;

        let len = bytes.len() as u32;
        self.file
            .write_all(&len.to_le_bytes())
            .map_err(|e| DbError::Wal(format!("Failed to write length prefix: {}", e)))?;

        self.file
            .write_all(&bytes)
            .map_err(|e| DbError::Wal(format!("Failed to write record: {}", e)))?;

        self.file
            .flush()
            .map_err(|e| DbError::Wal(format!("Failed to flush WAL: {}", e)))?;

        Ok(())
    }

    /// Fsync the WAL to ensure durability.
    ///
    /// This guarantees that all appended records are persisted to disk.
    /// Must be called after `append()` to ensure crash recovery.
    ///
    /// # Errors
    ///
    /// Returns `DbError::Wal` if fsync fails.
    pub fn sync(&mut self) -> DbResult<()> {
        self.file
            .sync_all()
            .map_err(|e| DbError::Wal(format!("Failed to sync WAL: {}", e)))
    }

    /// Replay all records from the WAL file.
    ///
    /// Reads the WAL sequentially, deserializing each record.
    /// Stops at EOF or the first corrupted/incomplete record.
    ///
    /// # Errors
    ///
    /// Returns `DbError::Wal` if the file cannot be opened or deserialization fails.
    pub fn replay(path: impl AsRef<Path>) -> DbResult<Vec<WalRecord>> {
        let mut file = OpenOptions::new()
            .read(true)
            .open(path.as_ref())
            .map_err(|e| {
                if e.kind() == std::io::ErrorKind::NotFound {
                    // No WAL file means no records to replay
                    return DbError::Wal("WAL file not found (empty replay)".to_string());
                }
                DbError::Wal(format!("Failed to open WAL for replay: {}", e))
            })?;

        let mut records = Vec::new();

        loop {
            // Read length prefix
            let mut len_buf = [0u8; 4];
            match file.read_exact(&mut len_buf) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    // Normal EOF
                    break;
                }
                Err(e) => {
                    return Err(DbError::Wal(format!("Failed to read length prefix: {}", e)));
                }
            }

            let len = u32::from_le_bytes(len_buf);

            // Read record data
            let mut buf = vec![0u8; len as usize];
            file.read_exact(&mut buf)
                .map_err(|e| DbError::Wal(format!("Failed to read record data: {}", e)))?;

            // Deserialize
            let (rec, _bytes_read) = decode_from_slice(&buf, bincode_config())
                .map_err(|e| DbError::Wal(format!("Failed to deserialize record: {}", e)))?;

            records.push(rec);
        }

        Ok(records)
    }

    /// Truncate the WAL file, removing all records.
    ///
    /// Used after checkpointing when all WAL records have been applied to storage.
    ///
    /// # Errors
    ///
    /// Returns `DbError::Wal` if the file cannot be truncated.
    pub fn truncate(&mut self) -> DbResult<()> {
        // Close current file handle
        drop(std::mem::replace(
            &mut self.file,
            OpenOptions::new()
                .write(true)
                .truncate(true)
                .open(&self.path)
                .map_err(|e| DbError::Wal(format!("Failed to truncate WAL: {}", e)))?,
        ));

        // Reopen in append mode
        self.file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .truncate(false)
            .open(&self.path)
            .map_err(|e| DbError::Wal(format!("Failed to reopen WAL after truncate: {}", e)))?;

        Ok(())
    }
}

/// Get the bincode configuration for WAL serialization.
///
/// Uses little-endian, fixed-width integers for cross-platform compatibility.
fn bincode_config() -> impl Config {
    config::legacy()
}
