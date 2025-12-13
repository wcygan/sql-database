//! TUI application state and types.

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::RwLock;

/// Maximum entries in the activity log ring buffer.
const MAX_ACTIVITY_LOG: usize = 100;

/// Snapshot of Raft metrics for display.
#[derive(Clone, Default)]
#[allow(dead_code)] // Fields reserved for future TUI display enhancements
pub struct RaftMetrics {
    pub node_id: u64,
    /// "Leader", "Follower", "Candidate", "Learner"
    pub state: String,
    pub current_leader: Option<u64>,
    pub current_term: u64,
    pub last_log_index: u64,
    pub commit_index: u64,
    pub last_applied: Option<u64>,
}

/// Activity log entry.
#[derive(Clone)]
pub struct ActivityEntry {
    pub timestamp: Instant,
    pub message: String,
    pub kind: ActivityKind,
}

/// Kind of activity for color coding.
#[derive(Clone, Copy)]
pub enum ActivityKind {
    Connection,
    Disconnection,
    Query,
    Error,
    Raft,
}

/// Shared TUI state updated by various tasks.
pub struct TuiState {
    /// Current Raft metrics snapshot.
    pub raft_metrics: RaftMetrics,
    /// Active connection count.
    pub connection_count: usize,
    /// Total queries executed since start.
    pub total_queries: u64,
    /// Recent activity log (ring buffer).
    pub activity_log: VecDeque<ActivityEntry>,
    /// Server start time.
    pub start_time: Instant,
    /// Client address (host:port).
    pub client_addr: String,
    /// Raft address (if multi-node).
    pub raft_addr: Option<String>,
    /// Whether Raft is enabled.
    pub raft_enabled: bool,
}

impl TuiState {
    /// Create new TUI state.
    pub fn new(
        client_addr: String,
        raft_addr: Option<String>,
        raft_enabled: bool,
        node_id: u64,
    ) -> Self {
        Self {
            raft_metrics: RaftMetrics {
                node_id,
                ..Default::default()
            },
            connection_count: 0,
            total_queries: 0,
            activity_log: VecDeque::with_capacity(MAX_ACTIVITY_LOG),
            start_time: Instant::now(),
            client_addr,
            raft_addr,
            raft_enabled,
        }
    }

    /// Add an activity entry to the log.
    pub fn add_activity(&mut self, message: String, kind: ActivityKind) {
        if self.activity_log.len() >= MAX_ACTIVITY_LOG {
            self.activity_log.pop_front();
        }
        self.activity_log.push_back(ActivityEntry {
            timestamp: Instant::now(),
            message,
            kind,
        });
    }
}

/// Thread-safe shared TUI state.
pub type SharedTuiState = Arc<RwLock<TuiState>>;
