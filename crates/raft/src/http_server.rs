//! HTTP server for Raft RPC endpoints.
//!
//! This module provides HTTP endpoints for inter-node Raft communication.
//! Each node runs an HTTP server that handles AppendEntries, Vote, and InstallSnapshot RPCs.

use crate::type_config::TypeConfig;
use crate::{NodeId, RaftNode};
use axum::{extract::State, http::StatusCode, response::IntoResponse, routing::post, Json, Router};
use openraft::raft::{AppendEntriesRequest, InstallSnapshotRequest, VoteRequest};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;

/// Shared state for HTTP handlers.
#[derive(Clone)]
pub struct RaftHttpState {
    /// The Raft node instance.
    pub raft: Arc<RaftNode>,
}

impl RaftHttpState {
    /// Create new HTTP state with the given Raft node.
    pub fn new(raft: Arc<RaftNode>) -> Self {
        Self { raft }
    }
}

/// Create the Raft HTTP router with all RPC endpoints.
pub fn create_router(state: RaftHttpState) -> Router {
    Router::new()
        .route("/raft/append_entries", post(handle_append_entries))
        .route("/raft/vote", post(handle_vote))
        .route("/raft/install_snapshot", post(handle_install_snapshot))
        .route("/health", post(handle_health).get(handle_health))
        .with_state(state)
}

/// Start the Raft HTTP server on the given address.
///
/// Returns a handle that can be used to gracefully shutdown the server.
pub async fn start_server(
    addr: SocketAddr,
    state: RaftHttpState,
) -> Result<ServerHandle, std::io::Error> {
    let router = create_router(state);
    let listener = TcpListener::bind(addr).await?;
    let local_addr = listener.local_addr()?;

    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();

    let server_handle = tokio::spawn(async move {
        axum::serve(listener, router)
            .with_graceful_shutdown(async {
                let _ = shutdown_rx.await;
            })
            .await
    });

    Ok(ServerHandle {
        local_addr,
        shutdown_tx: Some(shutdown_tx),
        server_handle,
    })
}

/// Handle for managing a running Raft HTTP server.
pub struct ServerHandle {
    /// The address the server is listening on.
    pub local_addr: SocketAddr,
    /// Channel to signal shutdown.
    shutdown_tx: Option<tokio::sync::oneshot::Sender<()>>,
    /// The server task handle.
    server_handle: tokio::task::JoinHandle<Result<(), std::io::Error>>,
}

impl ServerHandle {
    /// Get the local address the server is listening on.
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Signal the server to shutdown gracefully.
    pub fn shutdown(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
    }

    /// Wait for the server to complete.
    pub async fn wait(self) -> Result<(), std::io::Error> {
        match self.server_handle.await {
            Ok(result) => result,
            Err(e) => Err(std::io::Error::other(format!(
                "Server task panicked: {}",
                e
            ))),
        }
    }
}

/// Handle AppendEntries RPC from leader.
async fn handle_append_entries(
    State(state): State<RaftHttpState>,
    Json(req): Json<AppendEntriesRequest<TypeConfig>>,
) -> impl IntoResponse {
    match state.raft.append_entries(req).await {
        Ok(response) => (StatusCode::OK, Json(response)).into_response(),
        Err(e) => {
            let error_msg = format!("AppendEntries failed: {:?}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, error_msg).into_response()
        }
    }
}

/// Handle Vote RPC during leader election.
async fn handle_vote(
    State(state): State<RaftHttpState>,
    Json(req): Json<VoteRequest<NodeId>>,
) -> impl IntoResponse {
    match state.raft.vote(req).await {
        Ok(response) => (StatusCode::OK, Json(response)).into_response(),
        Err(e) => {
            let error_msg = format!("Vote failed: {:?}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, error_msg).into_response()
        }
    }
}

/// Handle InstallSnapshot RPC for state transfer.
async fn handle_install_snapshot(
    State(state): State<RaftHttpState>,
    Json(req): Json<InstallSnapshotRequest<TypeConfig>>,
) -> impl IntoResponse {
    match state.raft.install_snapshot(req).await {
        Ok(response) => (StatusCode::OK, Json(response)).into_response(),
        Err(e) => {
            let error_msg = format!("InstallSnapshot failed: {:?}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, error_msg).into_response()
        }
    }
}

/// Health check endpoint.
async fn handle_health(State(state): State<RaftHttpState>) -> impl IntoResponse {
    let metrics = state.raft.metrics().borrow().clone();
    let health = serde_json::json!({
        "status": "healthy",
        "node_id": metrics.id,
        "state": format!("{:?}", metrics.state),
        "current_leader": metrics.current_leader,
        "current_term": metrics.current_term,
        "last_log_index": metrics.last_log_index,
        "last_applied": metrics.last_applied.map(|l| l.index),
    });
    (StatusCode::OK, Json(health))
}

#[cfg(test)]
mod tests {
    // Note: Full integration tests require a running Raft node.
    // These are placeholder tests for the module structure.

    #[test]
    fn router_creation_compiles() {
        // This test just verifies the module compiles correctly.
        // Actual HTTP testing would require a mock Raft node.
    }
}
