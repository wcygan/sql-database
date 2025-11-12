//! Error types for the client library.

use protocol::ErrorCode;
use thiserror::Error;

/// Result type alias using ClientError.
pub type Result<T> = std::result::Result<T, ClientError>;

/// Errors that can occur when using the client.
#[derive(Error, Debug)]
pub enum ClientError {
    /// Failed to establish connection to server
    #[error("connection error: {0}")]
    Connection(#[source] std::io::Error),

    /// Protocol-level error (framing, serialization)
    #[error("protocol error: {0}")]
    Protocol(#[source] std::io::Error),

    /// Database error from server
    #[error("database error ({code:?}): {message}")]
    Database { code: ErrorCode, message: String },
}

impl ClientError {
    /// Returns true if this is a connection error.
    pub fn is_connection_error(&self) -> bool {
        matches!(self, ClientError::Connection(_))
    }

    /// Returns true if this is a protocol error.
    pub fn is_protocol_error(&self) -> bool {
        matches!(self, ClientError::Protocol(_))
    }

    /// Returns true if this is a database error.
    pub fn is_database_error(&self) -> bool {
        matches!(self, ClientError::Database { .. })
    }

    /// Returns the error code if this is a database error.
    pub fn error_code(&self) -> Option<ErrorCode> {
        match self {
            ClientError::Database { code, .. } => Some(*code),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_connection_error() {
        let err = ClientError::Connection(std::io::Error::other("test"));
        assert!(err.is_connection_error());
        assert!(!err.is_protocol_error());
        assert!(!err.is_database_error());
        assert!(err.error_code().is_none());
    }

    #[test]
    fn test_protocol_error() {
        let err = ClientError::Protocol(std::io::Error::other("test"));
        assert!(!err.is_connection_error());
        assert!(err.is_protocol_error());
        assert!(!err.is_database_error());
        assert!(err.error_code().is_none());
    }

    #[test]
    fn test_database_error() {
        let err = ClientError::Database {
            code: ErrorCode::ParseError,
            message: "syntax error".to_string(),
        };
        assert!(!err.is_connection_error());
        assert!(!err.is_protocol_error());
        assert!(err.is_database_error());
        assert_eq!(err.error_code(), Some(ErrorCode::ParseError));
    }

    #[test]
    fn test_error_display() {
        let err = ClientError::Connection(std::io::Error::other("connection refused"));
        assert!(err.to_string().contains("connection error"));

        let err = ClientError::Protocol(std::io::Error::other("invalid frame"));
        assert!(err.to_string().contains("protocol error"));

        let err = ClientError::Database {
            code: ErrorCode::ExecutionError,
            message: "table not found".to_string(),
        };
        assert!(err.to_string().contains("database error"));
        assert!(err.to_string().contains("table not found"));
    }
}
