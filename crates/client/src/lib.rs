//! Client library for connecting to the toy SQL database server.
//!
//! This crate provides a simple async API for executing SQL statements remotely.
//!
//! # Example
//!
//! ```no_run
//! use client::Client;
//!
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     let mut client = Client::connect("localhost:5432").await?;
//!
//!     let result = client.execute("CREATE TABLE users (id INT, name TEXT)").await?;
//!     println!("Created table");
//!
//!     let result = client.execute("INSERT INTO users VALUES (1, 'Alice')").await?;
//!     println!("Inserted {} row(s)", result.affected_count());
//!
//!     let result = client.execute("SELECT * FROM users").await?;
//!     if let Some((schema, rows)) = result.rows() {
//!         println!("Columns: {:?}", schema);
//!         println!("Rows: {}", rows.len());
//!     }
//!
//!     client.close().await?;
//!     Ok(())
//! }
//! ```

mod error;

pub use error::{ClientError, Result};

use common::Row;
use protocol::{ClientRequest, ServerResponse, frame};
use tokio::net::TcpStream;

/// Client for connecting to the database server.
pub struct Client {
    socket: TcpStream,
}

/// Result of executing a SQL statement.
#[derive(Debug, Clone)]
pub enum QueryResult {
    /// Query returned rows with schema
    Rows { schema: Vec<String>, rows: Vec<Row> },
    /// DML operation affected N rows
    Count { affected: u64 },
    /// DDL or other operation with no result
    Empty,
}

impl QueryResult {
    /// Returns the number of affected rows for DML operations, or 0 for other results.
    pub fn affected_count(&self) -> u64 {
        match self {
            QueryResult::Count { affected } => *affected,
            _ => 0,
        }
    }

    /// Returns the rows and schema if this is a Rows result, None otherwise.
    pub fn rows(&self) -> Option<(&Vec<String>, &Vec<Row>)> {
        match self {
            QueryResult::Rows { schema, rows } => Some((schema, rows)),
            _ => None,
        }
    }

    /// Returns true if this is an Empty result.
    pub fn is_empty(&self) -> bool {
        matches!(self, QueryResult::Empty)
    }
}

impl Client {
    /// Connect to the database server at the given address.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use client::Client;
    /// # async fn example() -> anyhow::Result<()> {
    /// let client = Client::connect("localhost:5432").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn connect(addr: &str) -> Result<Self> {
        let socket = TcpStream::connect(addr)
            .await
            .map_err(ClientError::Connection)?;

        Ok(Self { socket })
    }

    /// Execute a SQL statement and return the result.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use client::Client;
    /// # async fn example() -> anyhow::Result<()> {
    /// # let mut client = Client::connect("localhost:5432").await?;
    /// let result = client.execute("SELECT * FROM users").await?;
    /// if let Some((schema, rows)) = result.rows() {
    ///     println!("Got {} rows", rows.len());
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn execute(&mut self, sql: &str) -> Result<QueryResult> {
        // Send request
        let request = ClientRequest::Execute {
            sql: sql.to_string(),
        };
        frame::write_message_async(&mut self.socket, &request)
            .await
            .map_err(ClientError::Protocol)?;

        // Read response
        let response: ServerResponse = frame::read_message_async(&mut self.socket)
            .await
            .map_err(ClientError::Protocol)?;

        // Convert response to result
        match response {
            ServerResponse::Rows { schema, rows } => Ok(QueryResult::Rows { schema, rows }),
            ServerResponse::Count { affected } => Ok(QueryResult::Count { affected }),
            ServerResponse::Empty => Ok(QueryResult::Empty),
            ServerResponse::Error { code, message } => Err(ClientError::Database { code, message }),
        }
    }

    /// Close the connection gracefully.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use client::Client;
    /// # async fn example() -> anyhow::Result<()> {
    /// # let mut client = Client::connect("localhost:5432").await?;
    /// client.close().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn close(&mut self) -> Result<()> {
        let request = ClientRequest::Close;
        frame::write_message_async(&mut self.socket, &request)
            .await
            .map_err(ClientError::Protocol)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_query_result_affected_count() {
        let count = QueryResult::Count { affected: 42 };
        assert_eq!(count.affected_count(), 42);

        let rows = QueryResult::Rows {
            schema: vec![],
            rows: vec![],
        };
        assert_eq!(rows.affected_count(), 0);

        let empty = QueryResult::Empty;
        assert_eq!(empty.affected_count(), 0);
    }

    #[test]
    fn test_query_result_rows() {
        let rows = QueryResult::Rows {
            schema: vec!["id".to_string()],
            rows: vec![],
        };
        assert!(rows.rows().is_some());

        let count = QueryResult::Count { affected: 1 };
        assert!(count.rows().is_none());

        let empty = QueryResult::Empty;
        assert!(empty.rows().is_none());
    }

    #[test]
    fn test_query_result_is_empty() {
        let empty = QueryResult::Empty;
        assert!(empty.is_empty());

        let count = QueryResult::Count { affected: 1 };
        assert!(!count.is_empty());

        let rows = QueryResult::Rows {
            schema: vec![],
            rows: vec![],
        };
        assert!(!rows.is_empty());
    }
}
