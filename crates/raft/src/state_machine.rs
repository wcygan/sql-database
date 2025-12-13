//! Raft state machine integration.
//!
//! For Milestone 1 (v0.9 without storage-v2), the state machine is combined
//! with the log storage in a single `RaftStorage` implementation.
//!
//! See `log_storage.rs` for the `apply_to_state_machine` implementation.
//!
//! ## Future: DatabaseStateMachine
//!
//! In later milestones, the state machine will apply commands to actual
//! database storage (catalog, heap files, indexes) instead of just
//! recording them in memory.

// Re-export the storage type which includes state machine functionality
pub use crate::log_storage::{MemRaftStore, StateMachineData, StateMachineStore};

// TODO: Implement DatabaseStateMachine in Milestone 2+
// This will apply commands to the actual database storage.
//
// ```rust
// pub struct DatabaseStateMachine {
//     catalog: Arc<RwLock<Catalog>>,
//     pager: Arc<Mutex<FilePager>>,
//     data_dir: PathBuf,
//     last_applied: Option<LogId>,
// }
//
// // With storage-v2 feature:
// impl RaftStateMachine<TypeConfig> for DatabaseStateMachine {
//     async fn apply(&mut self, entries: Vec<Entry>) -> Vec<CommandResponse> {
//         // Apply each entry to the database
//     }
// }
// ```
