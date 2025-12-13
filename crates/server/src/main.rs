//! TCP server for the toy SQL database.
//!
//! Accepts client connections and executes SQL statements remotely using the
//! wire protocol defined in the `protocol` crate.
//!
//! # Raft Cluster Mode
//!
//! The server supports distributed consensus via Raft. To run a 3-node cluster:
//!
//! ```bash
//! # Terminal 1 - Node 1 (leader bootstrap)
//! cargo run -p server -- --node-id 1 --raft-addr 127.0.0.1:6001 \
//!     --peer 2,127.0.0.1:6002 --peer 3,127.0.0.1:6003 \
//!     --data-dir ./node1 --port 5001
//!
//! # Terminal 2 - Node 2
//! cargo run -p server -- --node-id 2 --raft-addr 127.0.0.1:6002 \
//!     --peer 1,127.0.0.1:6001 --peer 3,127.0.0.1:6003 \
//!     --data-dir ./node2 --port 5002
//!
//! # Terminal 3 - Node 3
//! cargo run -p server -- --node-id 3 --raft-addr 127.0.0.1:6003 \
//!     --peer 1,127.0.0.1:6001 --peer 2,127.0.0.1:6002 \
//!     --data-dir ./node3 --port 5003
//! ```

mod error;
mod tui;

use anyhow::Result;
use clap::Parser;
use database::{Database, QueryResult, RaftConfig};
use protocol::{ClientRequest, ServerResponse, frame};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::RwLock;

const DEFAULT_HOST: &str = "127.0.0.1";
const DEFAULT_PORT: u16 = 5432;
const DEFAULT_DATA_DIR: &str = "./db_data";
const DEFAULT_CATALOG_FILE: &str = "catalog.json";
const DEFAULT_WAL_FILE: &str = "toydb.wal";
const DEFAULT_BUFFER_PAGES: usize = 256;

#[derive(Parser, Debug)]
#[command(name = "toydb-server", about = "TCP server for the toy SQL database")]
struct Args {
    /// Host address to bind to for client connections
    #[arg(long, default_value = DEFAULT_HOST)]
    host: String,

    /// Port to listen on for client connections
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

    // --- Raft configuration ---
    /// Node ID for this server in the Raft cluster (1, 2, 3, etc.)
    /// Enables Raft consensus when set.
    #[arg(long)]
    node_id: Option<u64>,

    /// Address for Raft RPC communication (e.g., "127.0.0.1:6001").
    /// Required when node-id is set for multi-node clusters.
    #[arg(long)]
    raft_addr: Option<String>,

    /// Peer nodes in format "node_id,address" (e.g., "2,127.0.0.1:6002").
    /// Can be specified multiple times for each peer.
    #[arg(long = "peer", value_name = "ID,ADDR")]
    peers: Vec<String>,

    /// Use persistent Raft storage (survives restarts).
    /// Without this flag, Raft state is lost on restart.
    #[arg(long)]
    persistent: bool,

    /// Run in headless mode (static banner, no TUI).
    /// Useful for running in scripts or when stdout is not a TTY.
    #[arg(long)]
    headless: bool,
}

impl Args {
    /// Build RaftConfig from command-line arguments.
    fn raft_config(&self) -> Result<Option<RaftConfig>> {
        let Some(node_id) = self.node_id else {
            return Ok(None);
        };

        // Parse peers
        let peers: Result<Vec<(u64, String)>> = self
            .peers
            .iter()
            .map(|p| {
                let parts: Vec<&str> = p.splitn(2, ',').collect();
                if parts.len() != 2 {
                    anyhow::bail!("Invalid peer format '{}', expected 'node_id,address'", p);
                }
                let id: u64 = parts[0]
                    .parse()
                    .map_err(|_| anyhow::anyhow!("Invalid node ID in peer '{}'", p))?;
                Ok((id, parts[1].to_string()))
            })
            .collect();
        let peers = peers?;

        let config = if let Some(ref raft_addr) = self.raft_addr {
            // Multi-node cluster mode
            if self.persistent {
                RaftConfig::cluster_persistent(node_id, raft_addr.clone(), peers)
            } else {
                RaftConfig::cluster(node_id, raft_addr.clone(), peers)
            }
        } else if peers.is_empty() {
            // Single-node mode (no peers, no raft_addr needed)
            if self.persistent {
                RaftConfig::single_node_persistent(node_id)
            } else {
                RaftConfig::single_node(node_id)
            }
        } else {
            anyhow::bail!("--raft-addr is required when peers are specified");
        };

        Ok(Some(config))
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    // Build Raft configuration
    let raft_config = args.raft_config()?;

    // Initialize database
    let db = Arc::new(
        Database::with_raft_config(
            &args.data_dir,
            &args.catalog_file,
            &args.wal_file,
            args.buffer_pages,
            raft_config.clone(),
        )
        .await?,
    );

    // Bind TCP listener
    let addr = format!("{}:{}", args.host, args.port);
    let listener = TcpListener::bind(&addr).await?;

    if args.headless {
        // Headless mode: static banner + println logging
        run_headless(db, listener, &addr, &args, raft_config.as_ref()).await
    } else {
        // TUI mode: real-time status display
        run_tui_mode(db, listener, &addr, raft_config.as_ref()).await
    }
}

/// Run in headless mode with static banner.
async fn run_headless(
    db: Arc<Database>,
    listener: TcpListener,
    addr: &str,
    args: &Args,
    raft_config: Option<&RaftConfig>,
) -> Result<()> {
    println!("╔════════════════════════════════════════════════════════════╗");
    println!("║                    ToyDB Server Started                    ║");
    println!("╠════════════════════════════════════════════════════════════╣");
    println!("║  Client address:  {:43}║", addr);
    println!("║  Data directory:  {:43}║", format!("{:?}", args.data_dir));
    println!(
        "║  Buffer pool:     {:43}║",
        format!("{} pages", args.buffer_pages)
    );

    if let Some(config) = raft_config {
        // Wait briefly for leader election to complete before showing status
        if !config.peers.is_empty() {
            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
        }

        println!("╠════════════════════════════════════════════════════════════╣");
        println!("║  Raft enabled:    {:43}║", "yes");
        println!("║  Node ID:         {:43}║", config.node_id.to_string());
        if let Some(ref raft_addr) = config.listen_addr {
            println!("║  Raft address:    {:43}║", raft_addr);
        }
        println!(
            "║  Persistent:      {:43}║",
            if config.persistent_storage {
                "yes"
            } else {
                "no"
            }
        );
        if !config.peers.is_empty() {
            println!(
                "║  Peers:           {:43}║",
                format!("{} nodes", config.peers.len())
            );
            for (id, peer_addr) in &config.peers {
                println!("║    - Node {}:      {:43}║", id, peer_addr);
            }
        }
        let leader_status = if db.is_leader() {
            "LEADER ✓".to_string()
        } else if let Some(leader_id) = db.current_leader().await {
            format!("follower (leader: node {})", leader_id)
        } else {
            "waiting for quorum...".to_string()
        };
        println!("║  Leader status:   {:43}║", leader_status);
        println!(
            "║  Health endpoint: {:43}║",
            format!(
                "http://{}/health",
                config.listen_addr.as_deref().unwrap_or("N/A")
            )
        );
    } else {
        println!("║  Raft enabled:    {:43}║", "no (standalone mode)");
    }

    println!("╠════════════════════════════════════════════════════════════╣");
    println!("║  Press Ctrl+C to shut down                                 ║");
    println!("╚════════════════════════════════════════════════════════════╝");
    println!();

    // Spawn server task
    let server_task = tokio::spawn(run_server(listener, db));

    // Wait for shutdown signal
    tokio::signal::ctrl_c().await?;
    println!("\nShutdown signal received, stopping server...");

    // Abort server task
    server_task.abort();

    Ok(())
}

/// Run in TUI mode with real-time status display.
async fn run_tui_mode(
    db: Arc<Database>,
    listener: TcpListener,
    addr: &str,
    raft_config: Option<&RaftConfig>,
) -> Result<()> {
    let node_id = raft_config.map(|c| c.node_id).unwrap_or(1);
    let raft_addr = raft_config.and_then(|c| c.listen_addr.clone());
    let raft_enabled = raft_config.is_some();

    let state = Arc::new(RwLock::new(tui::TuiState::new(
        addr.to_string(),
        raft_addr,
        raft_enabled,
        node_id,
    )));

    tui::run_tui(db, listener, state).await
}

/// Run the server loop, accepting connections and spawning handlers.
/// Used only in headless mode.
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

/// Read a single request from the client.
/// Returns Ok(None) if client disconnected gracefully.
async fn read_client_request(socket: &mut TcpStream) -> Result<Option<ClientRequest>> {
    match frame::read_message_async(socket).await {
        Ok(request) => Ok(Some(request)),
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
            // Client disconnected gracefully
            Ok(None)
        }
        Err(e) => {
            // Send error response before closing
            let response = ServerResponse::Error {
                code: protocol::ErrorCode::IoError,
                message: format!("Failed to read request: {}", e),
            };
            let _ = frame::write_message_async(socket, &response).await;
            Err(e.into())
        }
    }
}

/// Execute SQL and convert the result to a server response.
/// Handles logging and timing internally.
async fn execute_sql_request(db: &Database, sql: &str, client_addr: &str) -> ServerResponse {
    log_request(client_addr, sql);
    let start = std::time::Instant::now();

    let result = db.execute(sql).await;

    match result {
        Ok(QueryResult::Rows { schema, rows }) => {
            let row_count = rows.len();
            log_response(client_addr, start.elapsed(), &format!("{} rows", row_count));
            ServerResponse::Rows { schema, rows }
        }
        Ok(QueryResult::Count { affected }) => {
            log_response(
                client_addr,
                start.elapsed(),
                &format!("{} affected", affected),
            );
            ServerResponse::Count { affected }
        }
        Ok(QueryResult::Empty) => {
            log_response(client_addr, start.elapsed(), "DDL success");
            ServerResponse::Empty
        }
        Err(e) => {
            let msg = e.to_string();
            log_response(client_addr, start.elapsed(), &format!("Error: {}", msg));
            ServerResponse::Error {
                code: error::map_error_to_code(&e),
                message: msg,
            }
        }
    }
}

/// Handle a single client connection.
/// Used only in headless mode.
async fn handle_client(mut socket: TcpStream, db: Arc<Database>) -> Result<()> {
    let client_addr = socket
        .peer_addr()
        .map(|addr| addr.to_string())
        .unwrap_or_else(|_| "unknown".to_string());

    loop {
        // Read next request
        let Some(request) = read_client_request(&mut socket).await? else {
            // Client disconnected
            break;
        };

        // Handle request
        match request {
            ClientRequest::Execute { sql } => {
                let response = execute_sql_request(&db, &sql, &client_addr).await;
                frame::write_message_async(&mut socket, &response).await?;
            }
            ClientRequest::Close => break,
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
