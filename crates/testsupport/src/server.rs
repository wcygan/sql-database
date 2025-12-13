//! Helpers for spinning up a TCP server backed by a temporary database.
//!
//! The [`TestServer`] struct runs the full client-server stack in-process so
//! integration tests can exercise the public wire protocol without touching the
//! real filesystem. Each server instance owns an isolated temporary directory
//! and shuts itself down automatically when dropped.

use anyhow::Result;
use common::DbError;
use database::{activity_channel, ActivityReceiver, Database, QueryResult, RaftConfig};
use protocol::{frame, ClientRequest, ErrorCode, ServerResponse};
use std::sync::Arc;
use tempfile::TempDir;
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

/// In-process TCP server for end-to-end tests.
pub struct TestServer {
    address: String,
    _temp_dir: TempDir,
    task: JoinHandle<()>,
}

impl TestServer {
    /// Start a new server bound to `127.0.0.1` on a random port.
    pub async fn start() -> Result<Self> {
        let temp_dir = TempDir::new()?;
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?.to_string();

        let db = Arc::new(Database::new(temp_dir.path(), "catalog.json", "test.wal", 64).await?);

        let task = tokio::spawn(async move {
            if let Err(e) = run_server(listener, db).await {
                eprintln!("test server error: {e:?}");
            }
        });

        Ok(Self {
            address,
            _temp_dir: temp_dir,
            task,
        })
    }

    /// Return the socket address clients should dial.
    pub fn address(&self) -> &str {
        &self.address
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// In-process TCP server with Raft enabled for testing activity events.
pub struct TestServerWithRaft {
    address: String,
    _temp_dir: TempDir,
    task: JoinHandle<()>,
    activity_rx: ActivityReceiver,
}

impl TestServerWithRaft {
    /// Start a new server with Raft enabled bound to `127.0.0.1` on a random port.
    /// Returns the server and an activity receiver for monitoring Raft events.
    pub async fn start() -> Result<Self> {
        let temp_dir = TempDir::new()?;
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?.to_string();

        // Create activity channel
        let (tx, rx) = activity_channel();

        // Create Raft config with activity sender
        let raft_config = RaftConfig::single_node(1).with_activity_sender(tx);

        let db = Arc::new(
            Database::with_raft_config(
                temp_dir.path(),
                "catalog.json",
                "test.wal",
                64,
                Some(raft_config),
            )
            .await?,
        );

        let task = tokio::spawn(async move {
            if let Err(e) = run_server(listener, db).await {
                eprintln!("test server error: {e:?}");
            }
        });

        Ok(Self {
            address,
            _temp_dir: temp_dir,
            task,
            activity_rx: rx,
        })
    }

    /// Return the socket address clients should dial.
    pub fn address(&self) -> &str {
        &self.address
    }

    /// Get a mutable reference to the activity receiver for checking events.
    pub fn activity_receiver(&mut self) -> &mut ActivityReceiver {
        &mut self.activity_rx
    }
}

impl Drop for TestServerWithRaft {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn run_server(listener: TcpListener, db: Arc<Database>) -> Result<()> {
    loop {
        let (socket, _) = listener.accept().await?;
        let db = db.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_client(socket, db).await {
                eprintln!("test server client error: {e:?}");
            }
        });
    }
}

async fn handle_client(mut socket: TcpStream, db: Arc<Database>) -> Result<()> {
    loop {
        let request: ClientRequest = match frame::read_message_async(&mut socket).await {
            Ok(req) => req,
            Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(err) => return Err(err.into()),
        };

        match request {
            ClientRequest::Execute { sql } => {
                let result = db.execute(&sql).await;
                let response = match result {
                    Ok(QueryResult::Rows { schema, rows }) => ServerResponse::Rows { schema, rows },
                    Ok(QueryResult::Count { affected }) => ServerResponse::Count { affected },
                    Ok(QueryResult::Empty) => ServerResponse::Empty,
                    Err(err) => {
                        let code = map_error_to_code(&err);
                        ServerResponse::Error {
                            code,
                            message: err.to_string(),
                        }
                    }
                };
                frame::write_message_async(&mut socket, &response).await?;
            }
            ClientRequest::Close => break,
        }
    }

    Ok(())
}

fn map_error_to_code(err: &anyhow::Error) -> ErrorCode {
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
