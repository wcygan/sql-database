//! Wire protocol for client-server communication.
//!
//! Defines the request/response message format and frame-based serialization.
//! Messages are length-prefixed using bincode encoding.

use common::Row;
use serde::{Deserialize, Serialize};

/// Request message sent from client to server.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ClientRequest {
    /// Execute a SQL statement and return results
    Execute { sql: String },
    /// Close the connection gracefully
    Close,
}

/// Response message sent from server to client.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ServerResponse {
    /// Query returned rows
    Rows { schema: Vec<String>, rows: Vec<Row> },
    /// DML operation affected N rows
    Count { affected: u64 },
    /// DDL or other operation with no result
    Empty,
    /// An error occurred
    Error { code: ErrorCode, message: String },
}

/// Error codes for protocol-level errors.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum ErrorCode {
    /// SQL parsing error
    ParseError,
    /// Query planning error
    PlanError,
    /// Execution error
    ExecutionError,
    /// Catalog error (table not found, etc.)
    CatalogError,
    /// Storage error (I/O, corruption, etc.)
    StorageError,
    /// WAL error
    WalError,
    /// Constraint violation (PK, etc.)
    ConstraintViolation,
    /// General I/O error
    IoError,
    /// Unknown error
    Unknown,
}

/// Frame format: [u32 length (little-endian)][bincode payload]
pub mod frame {
    use super::*;
    use bincode::config;
    use std::io::{self, Read, Write};

    const MAX_FRAME_SIZE: u32 = 64 * 1024 * 1024; // 64 MB

    /// Write a framed message.
    ///
    /// Format: [u32 length][bincode payload]
    pub fn write_message<W, T>(writer: &mut W, message: &T) -> io::Result<()>
    where
        W: Write,
        T: Serialize,
    {
        // Serialize the message
        let encoded = bincode::serde::encode_to_vec(message, config::standard())
            .map_err(|e| io::Error::other(format!("bincode encoding failed: {}", e)))?;

        // Check size limit
        let len = encoded.len() as u32;
        if len > MAX_FRAME_SIZE {
            return Err(io::Error::other(format!(
                "message too large: {} bytes (max {})",
                len, MAX_FRAME_SIZE
            )));
        }

        // Write length prefix (little-endian)
        writer.write_all(&len.to_le_bytes())?;

        // Write payload
        writer.write_all(&encoded)?;

        Ok(())
    }

    /// Read a framed message.
    ///
    /// Format: [u32 length][bincode payload]
    pub fn read_message<R, T>(reader: &mut R) -> io::Result<T>
    where
        R: Read,
        T: for<'de> Deserialize<'de>,
    {
        // Read length prefix
        let mut len_buf = [0u8; 4];
        reader.read_exact(&mut len_buf)?;
        let len = u32::from_le_bytes(len_buf);

        // Check size limit
        if len > MAX_FRAME_SIZE {
            return Err(io::Error::other(format!(
                "message too large: {} bytes (max {})",
                len, MAX_FRAME_SIZE
            )));
        }

        // Read payload
        let mut payload = vec![0u8; len as usize];
        reader.read_exact(&mut payload)?;

        // Deserialize
        let (message, _) = bincode::serde::decode_from_slice(&payload, config::standard())
            .map_err(|e| io::Error::other(format!("bincode decoding failed: {}", e)))?;

        Ok(message)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_round_trip_execute() {
        let req = ClientRequest::Execute {
            sql: "SELECT * FROM users".to_string(),
        };

        let mut buf = Vec::new();
        frame::write_message(&mut buf, &req).unwrap();

        let mut cursor = Cursor::new(buf);
        let decoded: ClientRequest = frame::read_message(&mut cursor).unwrap();

        match decoded {
            ClientRequest::Execute { sql } => assert_eq!(sql, "SELECT * FROM users"),
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn test_round_trip_response() {
        let resp = ServerResponse::Count { affected: 42 };

        let mut buf = Vec::new();
        frame::write_message(&mut buf, &resp).unwrap();

        let mut cursor = Cursor::new(buf);
        let decoded: ServerResponse = frame::read_message(&mut cursor).unwrap();

        match decoded {
            ServerResponse::Count { affected } => assert_eq!(affected, 42),
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn test_error_response() {
        let resp = ServerResponse::Error {
            code: ErrorCode::ParseError,
            message: "syntax error".to_string(),
        };

        let mut buf = Vec::new();
        frame::write_message(&mut buf, &resp).unwrap();

        let mut cursor = Cursor::new(buf);
        let decoded: ServerResponse = frame::read_message(&mut cursor).unwrap();

        match decoded {
            ServerResponse::Error { code, message } => {
                assert!(matches!(code, ErrorCode::ParseError));
                assert_eq!(message, "syntax error");
            }
            _ => panic!("wrong variant"),
        }
    }
}
