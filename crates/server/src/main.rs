//! TCP server for the toy SQL database.
//!
//! Accepts client connections and executes SQL statements remotely using the
//! wire protocol defined in the `protocol` crate.

mod error;

use anyhow::Result;
use clap::Parser;
use database::{Database, QueryResult};
use protocol::{ClientRequest, ServerResponse, frame};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::signal;

const DEFAULT_HOST: &str = "127.0.0.1";
const DEFAULT_PORT: u16 = 5432;
const DEFAULT_DATA_DIR: &str = "./db_data";
const DEFAULT_CATALOG_FILE: &str = "catalog.json";
const DEFAULT_WAL_FILE: &str = "toydb.wal";
const DEFAULT_BUFFER_PAGES: usize = 256;

#[derive(Parser, Debug)]
#[command(name = "toydb-server", about = "TCP server for the toy SQL database")]
struct Args {
    /// Host address to bind to
    #[arg(long, default_value = DEFAULT_HOST)]
    host: String,

    /// Port to listen on
    #[arg(long, default_value_t = DEFAULT_PORT)]
    port: u16,

    /// Directory containing catalog, WAL, and table files
    #[arg(long, default_value = DEFAULT_DATA_DIR)]
    data_dir: PathBuf,

    /// Catalog filename within the data directory
    #[arg(long, default_value = DEFAULT_CATALOG_FILE)]
    catalog_file: String,

    /// WAL filename within the data directory
    #[arg(long, default_value = DEFAULT_WAL_FILE)]
    wal_file: String,

    /// Maximum number of pages held in the file pager cache
    #[arg(long, default_value_t = DEFAULT_BUFFER_PAGES)]
    buffer_pages: usize,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    // Initialize database
    let db = Arc::new(
        Database::new(
            &args.data_dir,
            &args.catalog_file,
            &args.wal_file,
            args.buffer_pages,
        )
        .await?,
    );

    // Bind TCP listener
    let addr = format!("{}:{}", args.host, args.port);
    let listener = TcpListener::bind(&addr).await?;

    println!("Server listening on {}", addr);
    println!("Data directory: {:?}", args.data_dir);
    println!("Buffer pool: {} pages", args.buffer_pages);
    println!();
    println!("Press Ctrl+C to shut down");

    // Spawn server task
    let server_task = tokio::spawn(run_server(listener, db));

    // Wait for shutdown signal
    signal::ctrl_c().await?;
    println!("\nShutdown signal received, stopping server...");

    // Abort server task
    server_task.abort();

    Ok(())
}

/// Run the server loop, accepting connections and spawning handlers.
async fn run_server(listener: TcpListener, db: Arc<Database>) -> Result<()> {
    loop {
        match listener.accept().await {
            Ok((socket, addr)) => {
                println!("New connection from {}", addr);
                let db_clone = db.clone();
                tokio::spawn(async move {
                    if let Err(e) = handle_client(socket, db_clone).await {
                        eprintln!("Error handling client {}: {}", addr, e);
                    }
                    println!("Connection closed: {}", addr);
                });
            }
            Err(e) => {
                eprintln!("Error accepting connection: {}", e);
            }
        }
    }
}

/// Handle a single client connection.
async fn handle_client(mut socket: TcpStream, db: Arc<Database>) -> Result<()> {
    // Get client address for logging
    let client_addr = socket
        .peer_addr()
        .map(|addr| addr.to_string())
        .unwrap_or_else(|_| "unknown".to_string());

    loop {
        // Read request from client
        let request: ClientRequest = match frame::read_message_async(&mut socket).await {
            Ok(req) => req,
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                // Client disconnected
                break;
            }
            Err(e) => {
                // Send error response and close connection
                let response = ServerResponse::Error {
                    code: protocol::ErrorCode::IoError,
                    message: format!("Failed to read request: {}", e),
                };
                let _ = frame::write_message_async(&mut socket, &response).await;
                return Err(e.into());
            }
        };

        // Handle request
        match request {
            ClientRequest::Execute { sql } => {
                // Log incoming request
                log_request(&client_addr, &sql);

                // Start timing
                let start = std::time::Instant::now();

                // Execute SQL
                let result = db.execute(&sql).await;

                // Convert result to response
                let response = match result {
                    Ok(QueryResult::Rows { schema, rows }) => {
                        let row_count = rows.len();
                        let resp = ServerResponse::Rows { schema, rows };
                        log_response(
                            &client_addr,
                            start.elapsed(),
                            &format!("{} rows", row_count),
                        );
                        resp
                    }
                    Ok(QueryResult::Count { affected }) => {
                        let resp = ServerResponse::Count { affected };
                        log_response(
                            &client_addr,
                            start.elapsed(),
                            &format!("{} affected", affected),
                        );
                        resp
                    }
                    Ok(QueryResult::Empty) => {
                        log_response(&client_addr, start.elapsed(), "DDL success");
                        ServerResponse::Empty
                    }
                    Err(e) => {
                        let msg = e.to_string();
                        log_response(&client_addr, start.elapsed(), &format!("Error: {}", msg));
                        ServerResponse::Error {
                            code: error::map_error_to_code(&e),
                            message: msg,
                        }
                    }
                };

                // Send response
                frame::write_message_async(&mut socket, &response).await?;
            }
            ClientRequest::Close => {
                // Client requested graceful close
                break;
            }
        }
    }

    Ok(())
}

/// Log an incoming SQL request.
fn log_request(client_addr: &str, sql: &str) {
    let truncated = if sql.len() > 100 {
        format!("{}...", &sql[..100])
    } else {
        sql.to_string()
    };
    println!("[{}] SQL: {}", client_addr, truncated);
}

/// Log a response with timing.
fn log_response(client_addr: &str, duration: std::time::Duration, result: &str) {
    println!("[{}] Completed in {:?}: {}", client_addr, duration, result);
}
