//! Raft storage implementation (combined log + state machine for v0.9).
//!
//! For Milestone 1 (single-node mode), we use an in-memory implementation
//! that can be swapped for a persistent store in a later milestone.

use crate::command::{ActivitySender, RaftActivityEvent};
use crate::type_config::{Entry, LogId, SnapshotMeta, TypeConfig};
use crate::{Command, CommandResponse, NodeId};

use openraft::storage::{LogState, RaftLogReader, RaftSnapshotBuilder, Snapshot};
use openraft::{
    BasicNode, EntryPayload, OptionalSend, RaftLogId, RaftStorage, RaftTypeConfig, StorageError,
    StorageIOError, StoredMembership, Vote,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::io::Cursor;
use std::ops::RangeBounds;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Handler for applying commands to actual database storage.
///
/// This callback is invoked when Raft commits an entry to the state machine.
/// It receives the command and returns the result of applying it.
/// When not set, commands are recorded but not applied to storage.
pub type ApplyHandler = Arc<dyn Fn(&Command) -> CommandResponse + Send + Sync>;

/// The state machine state for our database.
/// For Milestone 1, this is a placeholder that doesn't actually apply to storage.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct StateMachineData {
    pub last_applied_log: Option<LogId>,
    pub last_membership: StoredMembership<NodeId, BasicNode>,
    /// Applied commands (for debugging/testing in Milestone 1)
    pub applied_commands: Vec<Command>,
}

/// In-memory Raft storage for Milestone 1.
pub struct MemRaftStore {
    /// Last purged log ID
    last_purged_log_id: RwLock<Option<LogId>>,

    /// The Raft log entries stored as JSON strings
    log: RwLock<BTreeMap<u64, String>>,

    /// The state machine state
    sm: RwLock<StateMachineData>,

    /// Current vote
    vote: RwLock<Option<Vote<NodeId>>>,

    /// Committed log ID (optional save)
    committed: RwLock<Option<LogId>>,

    /// Snapshot index counter
    snapshot_idx: RwLock<u64>,

    /// Current snapshot
    current_snapshot: RwLock<Option<StoredSnapshot>>,

    /// Optional handler for applying commands to actual database storage
    apply_handler: Option<ApplyHandler>,

    /// Optional sender for activity events (for TUI monitoring)
    activity_tx: Option<ActivitySender>,
}

/// Stored snapshot data
#[derive(Debug)]
pub struct StoredSnapshot {
    pub meta: SnapshotMeta,
    pub data: Vec<u8>,
}

/// Type alias for the store wrapped in Arc
pub type LogStore = Arc<MemRaftStore>;
pub type StateMachineStore = Arc<MemRaftStore>;

impl MemRaftStore {
    /// Create a new in-memory store without an apply handler.
    ///
    /// Commands will be recorded in the state machine but not applied to storage.
    pub fn new() -> Self {
        Self {
            last_purged_log_id: RwLock::new(None),
            log: RwLock::new(BTreeMap::new()),
            sm: RwLock::new(StateMachineData::default()),
            vote: RwLock::new(None),
            committed: RwLock::new(None),
            snapshot_idx: RwLock::new(0),
            current_snapshot: RwLock::new(None),
            apply_handler: None,
            activity_tx: None,
        }
    }

    /// Create a new in-memory store with an apply handler.
    ///
    /// The handler will be called when commands are committed to apply them
    /// to actual database storage.
    pub fn with_apply_handler(apply_handler: ApplyHandler) -> Self {
        Self {
            last_purged_log_id: RwLock::new(None),
            log: RwLock::new(BTreeMap::new()),
            sm: RwLock::new(StateMachineData::default()),
            vote: RwLock::new(None),
            committed: RwLock::new(None),
            snapshot_idx: RwLock::new(0),
            current_snapshot: RwLock::new(None),
            apply_handler: Some(apply_handler),
            activity_tx: None,
        }
    }

    /// Create a new in-memory store with an apply handler and activity sender.
    ///
    /// The handler will be called when commands are committed to apply them
    /// to actual database storage. Activity events will be sent to the provided channel.
    pub fn with_apply_handler_and_activity(
        apply_handler: ApplyHandler,
        activity_tx: ActivitySender,
    ) -> Self {
        Self {
            last_purged_log_id: RwLock::new(None),
            log: RwLock::new(BTreeMap::new()),
            sm: RwLock::new(StateMachineData::default()),
            vote: RwLock::new(None),
            committed: RwLock::new(None),
            snapshot_idx: RwLock::new(0),
            current_snapshot: RwLock::new(None),
            apply_handler: Some(apply_handler),
            activity_tx: Some(activity_tx),
        }
    }
}

impl Default for MemRaftStore {
    fn default() -> Self {
        Self::new()
    }
}

/// Create a new in-memory log store.
pub fn new_log_store() -> LogStore {
    Arc::new(MemRaftStore::default())
}

/// Create a new in-memory state machine store.
/// For v0.9 without storage-v2, this is the same as log store (combined).
pub fn new_state_machine_store() -> StateMachineStore {
    Arc::new(MemRaftStore::default())
}

// Implement RaftLogReader for Arc<MemRaftStore>
impl RaftLogReader<TypeConfig> for Arc<MemRaftStore> {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry>, StorageError<NodeId>> {
        let mut entries = vec![];
        let log = self.log.read().await;
        for (_, serialized) in log.range(range) {
            let ent: Entry =
                serde_json::from_str(serialized).map_err(|e| StorageIOError::read_logs(&e))?;
            entries.push(ent);
        }
        Ok(entries)
    }
}

// Implement RaftSnapshotBuilder for Arc<MemRaftStore>
impl RaftSnapshotBuilder<TypeConfig> for Arc<MemRaftStore> {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<NodeId>> {
        let sm = self.sm.read().await;
        let data = serde_json::to_vec(&*sm).map_err(|e| StorageIOError::read_state_machine(&e))?;

        let last_applied_log = sm.last_applied_log;
        let last_membership = sm.last_membership.clone();
        drop(sm);

        let mut idx = self.snapshot_idx.write().await;
        *idx += 1;
        let snapshot_idx = *idx;

        let snapshot_id = if let Some(last) = last_applied_log {
            format!("{}-{}-{}", last.leader_id, last.index, snapshot_idx)
        } else {
            format!("--{}", snapshot_idx)
        };

        let meta = SnapshotMeta {
            last_log_id: last_applied_log,
            last_membership,
            snapshot_id,
        };

        let snapshot = StoredSnapshot {
            meta: meta.clone(),
            data: data.clone(),
        };

        let mut current_snapshot = self.current_snapshot.write().await;
        *current_snapshot = Some(snapshot);

        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}

// Implement RaftStorage for Arc<MemRaftStore>
impl RaftStorage<TypeConfig> for Arc<MemRaftStore> {
    type LogReader = Self;
    type SnapshotBuilder = Self;

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError<NodeId>> {
        let log = self.log.read().await;
        let last_serialized = log.iter().next_back().map(|(_, ent)| ent);

        let last = match last_serialized {
            None => None,
            Some(serialized) => {
                let ent: Entry =
                    serde_json::from_str(serialized).map_err(|e| StorageIOError::read_logs(&e))?;
                Some(*ent.get_log_id())
            }
        };

        let last_purged = *self.last_purged_log_id.read().await;

        let last = match last {
            None => last_purged,
            Some(x) => Some(x),
        };

        Ok(LogState {
            last_purged_log_id: last_purged,
            last_log_id: last,
        })
    }

    async fn save_vote(&mut self, vote: &Vote<NodeId>) -> Result<(), StorageError<NodeId>> {
        let mut h = self.vote.write().await;
        *h = Some(*vote);
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<NodeId>>, StorageError<NodeId>> {
        Ok(*self.vote.read().await)
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId>,
    ) -> Result<(), StorageError<NodeId>> {
        let mut c = self.committed.write().await;
        *c = committed;
        Ok(())
    }

    async fn read_committed(&mut self) -> Result<Option<LogId>, StorageError<NodeId>> {
        Ok(*self.committed.read().await)
    }

    async fn last_applied_state(
        &mut self,
    ) -> Result<(Option<LogId>, StoredMembership<NodeId, BasicNode>), StorageError<NodeId>> {
        let sm = self.sm.read().await;
        Ok((sm.last_applied_log, sm.last_membership.clone()))
    }

    async fn delete_conflict_logs_since(
        &mut self,
        log_id: LogId,
    ) -> Result<(), StorageError<NodeId>> {
        let mut log = self.log.write().await;
        let keys: Vec<u64> = log.range(log_id.index..).map(|(k, _)| *k).collect();
        for key in keys {
            log.remove(&key);
        }
        Ok(())
    }

    async fn purge_logs_upto(&mut self, log_id: LogId) -> Result<(), StorageError<NodeId>> {
        {
            let mut ld = self.last_purged_log_id.write().await;
            *ld = Some(log_id);
        }
        {
            let mut log = self.log.write().await;
            let keys: Vec<u64> = log.range(..=log_id.index).map(|(k, _)| *k).collect();
            for key in keys {
                log.remove(&key);
            }
        }
        Ok(())
    }

    async fn append_to_log<I>(&mut self, entries: I) -> Result<(), StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry> + OptionalSend,
    {
        let mut log = self.log.write().await;
        for entry in entries {
            let s = serde_json::to_string(&entry)
                .map_err(|e| StorageIOError::write_log_entry(*entry.get_log_id(), &e))?;
            log.insert(entry.log_id.index, s);
        }
        Ok(())
    }

    async fn apply_to_state_machine(
        &mut self,
        entries: &[Entry],
    ) -> Result<Vec<CommandResponse>, StorageError<NodeId>> {
        let mut res = Vec::with_capacity(entries.len());
        let mut sm = self.sm.write().await;

        for entry in entries {
            sm.last_applied_log = Some(entry.log_id);
            let log_index = entry.log_id.index;
            let term = entry.log_id.leader_id.term;

            match &entry.payload {
                EntryPayload::Blank => {
                    // Send activity event for blank entry
                    if let Some(tx) = &self.activity_tx {
                        let _ = tx.send(RaftActivityEvent::blank(log_index, term));
                    }
                    res.push(CommandResponse::Ddl);
                }
                EntryPayload::Normal(cmd) => {
                    // Record the command for debugging/testing
                    sm.applied_commands.push(cmd.clone());

                    // Send activity event
                    if let Some(tx) = &self.activity_tx {
                        let _ = tx.send(RaftActivityEvent::from_command(log_index, term, cmd));
                    }

                    // If an apply handler is set, call it to apply to actual storage
                    if let Some(handler) = &self.apply_handler {
                        res.push(handler(cmd));
                    } else {
                        // No handler - just return Ddl (used in tests)
                        res.push(CommandResponse::Ddl);
                    }
                }
                EntryPayload::Membership(mem) => {
                    // Send activity event for membership change
                    if let Some(tx) = &self.activity_tx {
                        let _ = tx.send(RaftActivityEvent::membership(log_index, term));
                    }
                    sm.last_membership = StoredMembership::new(Some(entry.log_id), mem.clone());
                    res.push(CommandResponse::Ddl);
                }
            }
        }

        Ok(res)
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<<TypeConfig as RaftTypeConfig>::SnapshotData>, StorageError<NodeId>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta,
        snapshot: Box<<TypeConfig as RaftTypeConfig>::SnapshotData>,
    ) -> Result<(), StorageError<NodeId>> {
        let new_snapshot = StoredSnapshot {
            meta: meta.clone(),
            data: snapshot.into_inner(),
        };

        // Update state machine
        let new_sm: StateMachineData = serde_json::from_slice(&new_snapshot.data)
            .map_err(|e| StorageIOError::read_snapshot(Some(new_snapshot.meta.signature()), &e))?;
        let mut sm = self.sm.write().await;
        *sm = new_sm;

        // Update current snapshot
        let mut current_snapshot = self.current_snapshot.write().await;
        *current_snapshot = Some(new_snapshot);

        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<NodeId>> {
        let current_snapshot = self.current_snapshot.read().await;
        match &*current_snapshot {
            Some(snapshot) => {
                let data = snapshot.data.clone();
                Ok(Some(Snapshot {
                    meta: snapshot.meta.clone(),
                    snapshot: Box::new(Cursor::new(data)),
                }))
            }
            None => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Command;
    use common::TableId;
    use openraft::EntryPayload;
    use types::Value;

    fn make_entry(index: u64, term: u64, cmd: Command) -> Entry {
        Entry {
            log_id: LogId::new(openraft::CommittedLeaderId::new(term, 1), index),
            payload: EntryPayload::Normal(cmd),
        }
    }

    #[tokio::test]
    async fn test_memstore_basic() {
        let mut store = new_log_store();

        // Check initial state
        let state = store.get_log_state().await.unwrap();
        assert!(state.last_log_id.is_none());
    }

    #[tokio::test]
    async fn test_append_and_read() {
        let mut store = new_log_store();

        let cmd = Command::Insert {
            table_id: TableId(1),
            row: vec![Value::Int(42)],
        };
        let entry = make_entry(1, 1, cmd);

        // Append entry
        store.append_to_log(vec![entry]).await.unwrap();

        // Verify log state
        let state = store.get_log_state().await.unwrap();
        assert!(state.last_log_id.is_some());
        assert_eq!(state.last_log_id.unwrap().index, 1);

        // Read back entry
        let entries = store.try_get_log_entries(1..=1).await.unwrap();
        assert_eq!(entries.len(), 1);
    }
}
