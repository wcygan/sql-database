//! Error mapping utilities for converting database errors to protocol error codes.

use common::DbError;
use protocol::ErrorCode;

/// Map a database error to a protocol error code.
///
/// This function attempts to downcast the anyhow::Error to DbError and map it
/// to the appropriate ErrorCode. If the error is not a DbError, it returns
/// ErrorCode::Unknown.
pub fn map_error_to_code(err: &anyhow::Error) -> ErrorCode {
    // Try to downcast to DbError
    if let Some(db_err) = err.downcast_ref::<DbError>() {
        match db_err {
            DbError::Parser(_) => ErrorCode::ParseError,
            DbError::Planner(_) => ErrorCode::PlanError,
            DbError::Executor(_) => ErrorCode::ExecutionError,
            DbError::Catalog(_) => ErrorCode::CatalogError,
            DbError::Storage(_) => ErrorCode::StorageError,
            DbError::Wal(_) => ErrorCode::WalError,
            DbError::Constraint(_) => ErrorCode::ConstraintViolation,
            DbError::Io(_) => ErrorCode::IoError,
        }
    } else {
        ErrorCode::Unknown
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::anyhow;

    #[test]
    fn test_map_parser_error() {
        let err = anyhow!(DbError::Parser("syntax error".into()));
        assert!(matches!(map_error_to_code(&err), ErrorCode::ParseError));
    }

    #[test]
    fn test_map_planner_error() {
        let err = anyhow!(DbError::Planner("table not found".into()));
        assert!(matches!(map_error_to_code(&err), ErrorCode::PlanError));
    }

    #[test]
    fn test_map_executor_error() {
        let err = anyhow!(DbError::Executor("division by zero".into()));
        assert!(matches!(map_error_to_code(&err), ErrorCode::ExecutionError));
    }

    #[test]
    fn test_map_catalog_error() {
        let err = anyhow!(DbError::Catalog("table already exists".into()));
        assert!(matches!(map_error_to_code(&err), ErrorCode::CatalogError));
    }

    #[test]
    fn test_map_storage_error() {
        let err = anyhow!(DbError::Storage("page not found".into()));
        assert!(matches!(map_error_to_code(&err), ErrorCode::StorageError));
    }

    #[test]
    fn test_map_wal_error() {
        let err = anyhow!(DbError::Wal("failed to sync".into()));
        assert!(matches!(map_error_to_code(&err), ErrorCode::WalError));
    }

    #[test]
    fn test_map_constraint_error() {
        let err = anyhow!(DbError::Constraint("primary key violation".into()));
        assert!(matches!(
            map_error_to_code(&err),
            ErrorCode::ConstraintViolation
        ));
    }

    #[test]
    fn test_map_io_error() {
        let err = anyhow!(DbError::Io(std::io::Error::other("disk full")));
        assert!(matches!(map_error_to_code(&err), ErrorCode::IoError));
    }

    #[test]
    fn test_map_unknown_error() {
        let err = anyhow!("some other error");
        assert!(matches!(map_error_to_code(&err), ErrorCode::Unknown));
    }
}
