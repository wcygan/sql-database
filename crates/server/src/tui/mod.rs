//! TUI module for real-time server status display.

pub mod app;
mod ui;

pub use app::{ActivityKind, RaftMetrics, SharedTuiState, TuiState};

use anyhow::Result;
use crossterm::{
    event::{self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode, KeyModifiers},
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use database::Database;
use ratatui::{Terminal, backend::CrosstermBackend};
use std::io;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;

/// Tick rate for UI refresh (100ms = 10 FPS).
const TICK_RATE: Duration = Duration::from_millis(100);

/// How often to poll Raft metrics (200ms = 5 times/sec).
const METRICS_POLL_RATE: Duration = Duration::from_millis(200);

/// Initialize the terminal for TUI mode.
pub fn init() -> Result<Terminal<CrosstermBackend<io::Stdout>>> {
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
    let backend = CrosstermBackend::new(stdout);
    let terminal = Terminal::new(backend)?;
    Ok(terminal)
}

/// Restore the terminal to normal mode.
pub fn restore() -> Result<()> {
    disable_raw_mode()?;
    execute!(io::stdout(), LeaveAlternateScreen, DisableMouseCapture)?;
    Ok(())
}

/// Run the TUI event loop.
///
/// This spawns background tasks for:
/// - TCP server accepting connections
/// - Raft metrics polling
/// - Terminal event handling
///
/// The main loop handles rendering and event dispatch.
pub async fn run_tui(
    db: Arc<Database>,
    listener: TcpListener,
    state: SharedTuiState,
) -> Result<()> {
    // Set up panic hook to restore terminal
    let original_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |panic_info| {
        let _ = restore();
        original_hook(panic_info);
    }));

    // Initialize terminal
    let mut terminal = init()?;

    // Create shutdown channel
    let (shutdown_tx, _) = tokio::sync::broadcast::channel::<()>(1);

    // Spawn TCP server task
    let db_clone = db.clone();
    let state_clone = state.clone();
    let mut shutdown_rx1 = shutdown_tx.subscribe();
    tokio::spawn(async move {
        tokio::select! {
            _ = run_server_with_state(listener, db_clone, state_clone) => {}
            _ = shutdown_rx1.recv() => {}
        }
    });

    // Spawn Raft metrics poller task
    let db_clone = db.clone();
    let state_clone = state.clone();
    let mut shutdown_rx2 = shutdown_tx.subscribe();
    tokio::spawn(async move {
        tokio::select! {
            _ = poll_raft_metrics(db_clone, state_clone) => {}
            _ = shutdown_rx2.recv() => {}
        }
    });

    // Main event loop
    let mut tick_interval = tokio::time::interval(TICK_RATE);

    loop {
        // Draw UI
        {
            let state_lock = state.read().await;
            terminal.draw(|f| ui::render(f, &state_lock))?;
        }

        // Handle events with timeout
        tokio::select! {
            _ = tick_interval.tick() => {
                // Just triggers redraw
            }
            result = poll_terminal_event() => {
                if let Some(key) = result && should_quit(key) {
                    break;
                }
            }
        }
    }

    // Signal shutdown to background tasks
    let _ = shutdown_tx.send(());

    // Restore terminal
    restore()?;

    Ok(())
}

/// Poll for terminal events with a short timeout.
async fn poll_terminal_event() -> Option<event::KeyEvent> {
    // Use spawn_blocking since crossterm's poll is blocking
    tokio::task::spawn_blocking(|| {
        if event::poll(Duration::from_millis(50)).unwrap_or(false)
            && let Ok(Event::Key(key)) = event::read()
        {
            return Some(key);
        }
        None
    })
    .await
    .unwrap_or(None)
}

/// Check if a key event should quit the TUI.
fn should_quit(key: event::KeyEvent) -> bool {
    key.code == KeyCode::Char('q')
        || (key.code == KeyCode::Char('c') && key.modifiers.contains(KeyModifiers::CONTROL))
}

/// Poll Raft metrics and update TUI state.
async fn poll_raft_metrics(db: Arc<Database>, state: SharedTuiState) {
    let mut interval = tokio::time::interval(METRICS_POLL_RATE);

    loop {
        interval.tick().await;

        if let Some(raft) = db.raft_node() {
            let metrics = raft.metrics().borrow().clone();

            let mut state_lock = state.write().await;
            state_lock.raft_metrics = RaftMetrics {
                node_id: metrics.id,
                state: format!("{:?}", metrics.state),
                current_leader: metrics.current_leader,
                current_term: metrics.current_term,
                last_log_index: metrics.last_log_index.unwrap_or(0),
                commit_index: metrics.last_applied.map(|l| l.index).unwrap_or(0),
                last_applied: metrics.last_applied.map(|l| l.index),
            };
        }
    }
}

/// Run the TCP server, updating TUI state on connections/queries.
async fn run_server_with_state(
    listener: TcpListener,
    db: Arc<Database>,
    state: SharedTuiState,
) -> Result<()> {
    loop {
        match listener.accept().await {
            Ok((socket, addr)) => {
                let addr_str = addr.to_string();

                // Log connection
                {
                    let mut state_lock = state.write().await;
                    state_lock.connection_count += 1;
                    state_lock
                        .add_activity(format!("{} connected", addr_str), ActivityKind::Connection);
                }

                // Spawn handler
                let db_clone = db.clone();
                let state_clone = state.clone();
                let addr_clone = addr_str.clone();

                tokio::spawn(async move {
                    let result = handle_client_with_state(
                        socket,
                        db_clone,
                        state_clone.clone(),
                        &addr_clone,
                    )
                    .await;

                    // Log disconnection
                    {
                        let mut state_lock = state_clone.write().await;
                        state_lock.connection_count = state_lock.connection_count.saturating_sub(1);
                        state_lock.add_activity(
                            format!("{} disconnected", addr_clone),
                            ActivityKind::Disconnection,
                        );
                    }

                    if let Err(e) = result {
                        let mut state_lock = state_clone.write().await;
                        state_lock.add_activity(
                            format!("{}: error - {}", addr_clone, e),
                            ActivityKind::Error,
                        );
                    }
                });
            }
            Err(e) => {
                let mut state_lock = state.write().await;
                state_lock.add_activity(format!("Accept error: {}", e), ActivityKind::Error);
            }
        }
    }
}

/// Handle a single client connection, logging activity to TUI state.
async fn handle_client_with_state(
    mut socket: tokio::net::TcpStream,
    db: Arc<Database>,
    state: SharedTuiState,
    client_addr: &str,
) -> Result<()> {
    use protocol::{ClientRequest, ServerResponse, frame};

    loop {
        // Read request
        let request: ClientRequest = match frame::read_message_async(&mut socket).await {
            Ok(req) => req,
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e.into()),
        };

        match request {
            ClientRequest::Execute { sql } => {
                let start = std::time::Instant::now();
                let result = db.execute(&sql).await;
                let duration_ms = start.elapsed().as_millis() as u64;

                // Truncate SQL for display
                let truncated_sql = if sql.len() > 40 {
                    format!("{}...", &sql[..40])
                } else {
                    sql.clone()
                };

                let (response, result_info) = match result {
                    Ok(database::QueryResult::Rows { schema, rows }) => {
                        let info = format!("{} rows", rows.len());
                        (ServerResponse::Rows { schema, rows }, info)
                    }
                    Ok(database::QueryResult::Count { affected }) => {
                        let info = format!("{} affected", affected);
                        (ServerResponse::Count { affected }, info)
                    }
                    Ok(database::QueryResult::Empty) => (ServerResponse::Empty, "OK".to_string()),
                    Err(e) => {
                        let msg = e.to_string();
                        let info = format!("Error: {}", &msg);
                        (
                            ServerResponse::Error {
                                code: protocol::ErrorCode::ExecutionError,
                                message: msg,
                            },
                            info,
                        )
                    }
                };

                // Log query
                {
                    let mut state_lock = state.write().await;
                    state_lock.total_queries += 1;
                    state_lock.add_activity(
                        format!(
                            "{} {} ({}ms, {})",
                            client_addr, truncated_sql, duration_ms, result_info
                        ),
                        ActivityKind::Query,
                    );
                }

                frame::write_message_async(&mut socket, &response).await?;
            }
            ClientRequest::Close => break,
        }
    }

    Ok(())
}
