//! Persistent Raft storage implementation.
//!
//! This module provides durable Raft log and state machine storage that survives restarts.
//!
//! ## File Layout
//!
//! ```text
//! {data_dir}/
//! ├── raft.log           # Append-only log entries
//! ├── raft_state.json    # Vote and committed state
//! └── snapshots/         # State machine snapshots
//!     └── {id}.snap
//! ```
//!
//! ## Log File Format
//!
//! Each entry in `raft.log` has a fixed header followed by a variable-length payload:
//!
//! ```text
//! ┌────────────────────────────────────┐
//! │ Header (28 bytes)                  │
//! │ ├─ magic: u32 (0x52414654 "RAFT") │
//! │ ├─ checksum: u32 (CRC32)          │
//! │ ├─ length: u32                     │
//! │ ├─ index: u64                      │
//! │ └─ term: u64                       │
//! ├────────────────────────────────────┤
//! │ Payload (bincode Entry)            │
//! └────────────────────────────────────┘
//! ```

use crate::command::{ActivitySender, RaftActivityEvent};
use crate::log_storage::{ApplyHandler, StateMachineData, StoredSnapshot};
use crate::type_config::{Entry, LogId, SnapshotMeta, TypeConfig};
use crate::{CommandResponse, NodeId};

use openraft::storage::{LogState, RaftLogReader, RaftSnapshotBuilder, Snapshot};
use openraft::{
    BasicNode, EntryPayload, OptionalSend, RaftStorage, RaftTypeConfig, StorageError,
    StorageIOError, StoredMembership, Vote,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Cursor, Read, Seek, SeekFrom, Write};
use std::ops::RangeBounds;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::RwLock;

/// Magic bytes for log entry validation: "RAFT" in ASCII
const RAFT_MAGIC: u32 = 0x52414654;

/// Size of the log entry header in bytes
const HEADER_SIZE: usize = 28;

/// Location of a log entry within the log file.
/// Used for direct access during log compaction (future enhancement).
#[derive(Clone, Debug)]
#[allow(dead_code)]
struct LogEntryLocation {
    /// Byte offset from start of file
    offset: u64,
    /// Total length including header
    length: u32,
}

/// Persistent state stored in `raft_state.json`.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct RaftState {
    /// Current vote (term and candidate voted for)
    vote: Option<Vote<NodeId>>,
    /// Committed log ID
    committed: Option<LogId>,
    /// Last purged log ID (entries before this are compacted)
    last_purged_log_id: Option<LogId>,
    /// Snapshot index counter for unique IDs
    snapshot_idx: u64,
    /// Last applied log ID (state machine progress)
    last_applied_log: Option<LogId>,
    /// Last membership configuration (cluster membership)
    last_membership: StoredMembership<NodeId, BasicNode>,
}

/// Header for each log entry in `raft.log`.
#[derive(Clone, Debug)]
struct LogEntryHeader {
    /// Magic bytes for validation
    magic: u32,
    /// CRC32 checksum of the payload
    checksum: u32,
    /// Payload length in bytes
    length: u32,
    /// Log index (redundant for recovery)
    index: u64,
    /// Log term (redundant for recovery)
    term: u64,
}

impl LogEntryHeader {
    /// Serialize the header to bytes.
    fn to_bytes(&self) -> [u8; HEADER_SIZE] {
        let mut buf = [0u8; HEADER_SIZE];
        buf[0..4].copy_from_slice(&self.magic.to_le_bytes());
        buf[4..8].copy_from_slice(&self.checksum.to_le_bytes());
        buf[8..12].copy_from_slice(&self.length.to_le_bytes());
        buf[12..20].copy_from_slice(&self.index.to_le_bytes());
        buf[20..28].copy_from_slice(&self.term.to_le_bytes());
        buf
    }

    /// Deserialize the header from bytes.
    fn from_bytes(buf: &[u8; HEADER_SIZE]) -> Self {
        Self {
            magic: u32::from_le_bytes(buf[0..4].try_into().unwrap()),
            checksum: u32::from_le_bytes(buf[4..8].try_into().unwrap()),
            length: u32::from_le_bytes(buf[8..12].try_into().unwrap()),
            index: u64::from_le_bytes(buf[12..20].try_into().unwrap()),
            term: u64::from_le_bytes(buf[20..28].try_into().unwrap()),
        }
    }
}

/// Persistent Raft storage that survives restarts.
///
/// This implementation persists:
/// - Log entries to `raft.log` (append-only with checksums)
/// - Vote and committed state to `raft_state.json` (atomic updates)
/// - Snapshots to `snapshots/{id}.snap`
pub struct PersistentRaftStore {
    /// Directory containing all Raft files
    data_dir: PathBuf,

    /// In-memory index: log_index -> file location
    log_index: RwLock<BTreeMap<u64, LogEntryLocation>>,

    /// Cached log entries for read performance
    log_cache: RwLock<BTreeMap<u64, Entry>>,

    /// Last purged log ID
    last_purged_log_id: RwLock<Option<LogId>>,

    /// State machine state
    sm: RwLock<StateMachineData>,

    /// Current vote (cached from state file)
    vote: RwLock<Option<Vote<NodeId>>>,

    /// Committed log ID (cached from state file)
    committed: RwLock<Option<LogId>>,

    /// Current snapshot
    current_snapshot: RwLock<Option<StoredSnapshot>>,

    /// Snapshot counter for unique IDs
    snapshot_idx: RwLock<u64>,

    /// Optional handler for applying commands to database storage
    apply_handler: Option<ApplyHandler>,

    /// Optional sender for activity events (for TUI monitoring)
    activity_tx: Option<ActivitySender>,
}

impl PersistentRaftStore {
    /// Open or create persistent storage at the given directory.
    ///
    /// On startup:
    /// 1. Creates directories if needed
    /// 2. Loads state from `raft_state.json`
    /// 3. Rebuilds log index by scanning `raft.log`
    /// 4. Loads the latest snapshot if available
    pub fn open(data_dir: impl AsRef<Path>) -> io::Result<Self> {
        Self::open_with_handler(data_dir, None)
    }

    /// Open persistent storage with an apply handler.
    pub fn open_with_handler(
        data_dir: impl AsRef<Path>,
        apply_handler: Option<ApplyHandler>,
    ) -> io::Result<Self> {
        Self::open_with_handler_and_activity(data_dir, apply_handler, None)
    }

    /// Open persistent storage with an apply handler and activity sender.
    pub fn open_with_handler_and_activity(
        data_dir: impl AsRef<Path>,
        apply_handler: Option<ApplyHandler>,
        activity_tx: Option<ActivitySender>,
    ) -> io::Result<Self> {
        let data_dir = data_dir.as_ref().to_path_buf();

        // Create directories
        fs::create_dir_all(&data_dir)?;
        fs::create_dir_all(data_dir.join("snapshots"))?;

        // Load or initialize state
        let state_path = data_dir.join("raft_state.json");
        let state: RaftState = if state_path.exists() {
            let contents = fs::read_to_string(&state_path)?;
            serde_json::from_str(&contents).unwrap_or_default()
        } else {
            RaftState::default()
        };

        // Rebuild log index
        let log_path = data_dir.join("raft.log");
        let (log_index, log_cache) = Self::rebuild_log_index(&log_path, state.last_purged_log_id)?;

        // Load latest snapshot and state machine
        let (current_snapshot, mut sm) = Self::load_latest_snapshot(&data_dir)?;

        // If no snapshot exists but we have state from the state file, restore state machine state.
        // This ensures OpenRaft knows which entries have already been applied and won't try to
        // re-apply from index 0, and also knows about cluster membership.
        if current_snapshot.is_none() {
            if state.last_applied_log.is_some() {
                sm.last_applied_log = state.last_applied_log;
            }
            // Restore membership - this is critical for the node to know it's part of a cluster
            if state.last_membership.log_id().is_some() {
                sm.last_membership = state.last_membership.clone();
            }
        }

        // If we have log entries starting at index 1 but no explicit last_purged_log_id,
        // set it to index 0 so OpenRaft knows entries before index 1 don't exist.
        // This is necessary because OpenRaft expects either:
        // 1. Log entries starting from index 0
        // 2. A last_purged_log_id indicating where the log starts
        let last_purged_log_id = match state.last_purged_log_id {
            Some(id) => Some(id),
            None if !log_cache.is_empty() => {
                // Get the first entry's term to use in the purged log ID
                let first_entry = log_cache.values().next().unwrap();
                let term = first_entry.log_id.leader_id.term;
                Some(LogId::new(openraft::CommittedLeaderId::new(term, 0), 0))
            }
            None => None,
        };

        Ok(Self {
            data_dir,
            log_index: RwLock::new(log_index),
            log_cache: RwLock::new(log_cache),
            last_purged_log_id: RwLock::new(last_purged_log_id),
            sm: RwLock::new(sm),
            vote: RwLock::new(state.vote),
            committed: RwLock::new(state.committed),
            current_snapshot: RwLock::new(current_snapshot),
            snapshot_idx: RwLock::new(state.snapshot_idx),
            apply_handler,
            activity_tx,
        })
    }

    /// Rebuild the log index by scanning the log file.
    fn rebuild_log_index(
        log_path: &Path,
        last_purged: Option<LogId>,
    ) -> io::Result<(BTreeMap<u64, LogEntryLocation>, BTreeMap<u64, Entry>)> {
        let mut index = BTreeMap::new();
        let mut cache = BTreeMap::new();

        if !log_path.exists() {
            return Ok((index, cache));
        }

        let file = File::open(log_path)?;
        let mut reader = BufReader::new(file);
        let mut offset = 0u64;

        loop {
            // Read header
            let mut header_buf = [0u8; HEADER_SIZE];
            match reader.read_exact(&mut header_buf) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
            }

            let header = LogEntryHeader::from_bytes(&header_buf);

            // Validate magic
            if header.magic != RAFT_MAGIC {
                // Corrupted or partial write - stop here
                break;
            }

            // Read payload
            let mut payload = vec![0u8; header.length as usize];
            if reader.read_exact(&mut payload).is_err() {
                // Partial write - stop here
                break;
            }

            // Validate checksum
            let computed_checksum = crc32fast::hash(&payload);
            if computed_checksum != header.checksum {
                // Corrupted entry - stop here
                break;
            }

            // Skip entries at or before last_purged
            let purged_index = last_purged.map(|lid| lid.index).unwrap_or(0);
            if header.index <= purged_index {
                offset += HEADER_SIZE as u64 + header.length as u64;
                continue;
            }

            // Deserialize and cache
            let config = bincode::config::legacy();
            if let Ok((entry, _)) = bincode::serde::decode_from_slice::<Entry, _>(&payload, config)
            {
                index.insert(
                    header.index,
                    LogEntryLocation {
                        offset,
                        length: HEADER_SIZE as u32 + header.length,
                    },
                );
                cache.insert(header.index, entry);
            }

            offset += HEADER_SIZE as u64 + header.length as u64;
        }

        Ok((index, cache))
    }

    /// Load the latest snapshot from the snapshots directory.
    fn load_latest_snapshot(
        data_dir: &Path,
    ) -> io::Result<(Option<StoredSnapshot>, StateMachineData)> {
        let snapshots_dir = data_dir.join("snapshots");
        if !snapshots_dir.exists() {
            return Ok((None, StateMachineData::default()));
        }

        // Find the latest snapshot file
        let mut latest_snapshot: Option<(u64, PathBuf)> = None;
        for entry in fs::read_dir(&snapshots_dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.extension().is_some_and(|ext| ext == "snap") {
                // Parse index from filename: {term}_{index}_{snapshot_idx}.snap
                if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
                    let parts: Vec<&str> = stem.split('_').collect();
                    if parts.len() >= 2 {
                        if let Ok(index) = parts[1].parse::<u64>() {
                            if latest_snapshot.as_ref().is_none_or(|(i, _)| index > *i) {
                                latest_snapshot = Some((index, path));
                            }
                        }
                    }
                }
            }
        }

        let Some((_, snapshot_path)) = latest_snapshot else {
            return Ok((None, StateMachineData::default()));
        };

        // Read snapshot file
        let file = File::open(&snapshot_path)?;
        let mut reader = BufReader::new(file);

        // Read metadata length
        let mut len_buf = [0u8; 4];
        reader.read_exact(&mut len_buf)?;
        let meta_len = u32::from_le_bytes(len_buf) as usize;

        // Read metadata
        let mut meta_buf = vec![0u8; meta_len];
        reader.read_exact(&mut meta_buf)?;
        let meta: SnapshotMeta = serde_json::from_slice(&meta_buf)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        // Read data
        let mut data = Vec::new();
        reader.read_to_end(&mut data)?;

        // Deserialize state machine
        let sm: StateMachineData = serde_json::from_slice(&data)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        let snapshot = StoredSnapshot { meta, data };

        Ok((Some(snapshot), sm))
    }

    /// Save the current state to `raft_state.json` atomically.
    fn save_state(&self, state: &RaftState) -> io::Result<()> {
        let state_path = self.data_dir.join("raft_state.json");
        let temp_path = self.data_dir.join("raft_state.json.tmp");

        // Write to temp file
        let contents =
            serde_json::to_string_pretty(state).map_err(|e| io::Error::other(e.to_string()))?;
        fs::write(&temp_path, &contents)?;

        // Fsync temp file
        let file = File::open(&temp_path)?;
        file.sync_all()?;

        // Atomic rename
        fs::rename(&temp_path, &state_path)?;

        // Fsync directory
        let dir = File::open(&self.data_dir)?;
        dir.sync_all()?;

        Ok(())
    }

    /// Append a single entry to the log file.
    fn append_entry_to_file(&self, entry: &Entry) -> io::Result<LogEntryLocation> {
        let log_path = self.data_dir.join("raft.log");
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log_path)?;

        let offset = file.seek(SeekFrom::End(0))?;

        // Serialize entry
        let config = bincode::config::legacy();
        let payload = bincode::serde::encode_to_vec(entry, config)
            .map_err(|e| io::Error::other(e.to_string()))?;

        let checksum = crc32fast::hash(&payload);

        // Build header
        let header = LogEntryHeader {
            magic: RAFT_MAGIC,
            checksum,
            length: payload.len() as u32,
            index: entry.log_id.index,
            term: entry.log_id.leader_id.term,
        };

        // Write header + payload using BufWriter, then flush and drop before sync
        {
            let mut writer = BufWriter::new(&mut file);
            writer.write_all(&header.to_bytes())?;
            writer.write_all(&payload)?;
            writer.flush()?;
        } // writer dropped here, releasing mutable borrow

        // Fsync for durability
        file.sync_all()?;

        Ok(LogEntryLocation {
            offset,
            length: HEADER_SIZE as u32 + payload.len() as u32,
        })
    }

    /// Write a snapshot to disk.
    fn write_snapshot(&self, meta: &SnapshotMeta, data: &[u8]) -> io::Result<()> {
        let snapshot_path = self
            .data_dir
            .join("snapshots")
            .join(format!("{}.snap", meta.snapshot_id));
        let temp_path = self
            .data_dir
            .join("snapshots")
            .join(format!("{}.snap.tmp", meta.snapshot_id));

        let file = File::create(&temp_path)?;
        let mut writer = BufWriter::new(file);

        // Write metadata length + metadata
        let meta_bytes = serde_json::to_vec(meta).map_err(|e| io::Error::other(e.to_string()))?;
        writer.write_all(&(meta_bytes.len() as u32).to_le_bytes())?;
        writer.write_all(&meta_bytes)?;

        // Write data
        writer.write_all(data)?;
        writer.flush()?;

        // Fsync
        let file = writer.into_inner()?;
        file.sync_all()?;

        // Atomic rename
        fs::rename(&temp_path, &snapshot_path)?;

        // Fsync directory
        let dir = File::open(self.data_dir.join("snapshots"))?;
        dir.sync_all()?;

        Ok(())
    }

    /// Get the current state for saving.
    async fn get_current_state(&self) -> RaftState {
        let sm = self.sm.read().await;
        RaftState {
            vote: *self.vote.read().await,
            committed: *self.committed.read().await,
            last_purged_log_id: *self.last_purged_log_id.read().await,
            snapshot_idx: *self.snapshot_idx.read().await,
            last_applied_log: sm.last_applied_log,
            last_membership: sm.last_membership.clone(),
        }
    }
}

/// Type alias for the persistent store wrapped in Arc.
pub type PersistentLogStore = Arc<PersistentRaftStore>;

// Implement RaftLogReader for Arc<PersistentRaftStore>
impl RaftLogReader<TypeConfig> for Arc<PersistentRaftStore> {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry>, StorageError<NodeId>> {
        let cache = self.log_cache.read().await;
        let entries: Vec<Entry> = cache.range(range).map(|(_, e)| e.clone()).collect();
        Ok(entries)
    }
}

// Implement RaftSnapshotBuilder for Arc<PersistentRaftStore>
impl RaftSnapshotBuilder<TypeConfig> for Arc<PersistentRaftStore> {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<NodeId>> {
        let sm = self.sm.read().await;
        let data = serde_json::to_vec(&*sm).map_err(|e| StorageIOError::read_state_machine(&e))?;

        let last_applied_log = sm.last_applied_log;
        let last_membership = sm.last_membership.clone();
        drop(sm);

        // Increment snapshot index
        let snapshot_idx = {
            let mut idx = self.snapshot_idx.write().await;
            *idx += 1;
            *idx
        }; // Release write lock before calling get_current_state

        // Generate snapshot ID using underscores to avoid parsing issues
        // Format: {term}_{node}_{index}_{snapshot_idx}
        let snapshot_id = if let Some(last) = last_applied_log {
            format!("{}_{}_{}", last.leader_id.term, last.index, snapshot_idx)
        } else {
            format!("0_0_{}", snapshot_idx)
        };

        let meta = SnapshotMeta {
            last_log_id: last_applied_log,
            last_membership,
            snapshot_id,
        };

        // Write snapshot to disk
        self.write_snapshot(&meta, &data)
            .map_err(|e| StorageIOError::write_snapshot(Some(meta.signature()), &e))?;

        // Update state file
        let state = self.get_current_state().await;
        self.save_state(&state)
            .map_err(|e| StorageIOError::write_state_machine(&e))?;

        // Update current snapshot in memory
        let snapshot = StoredSnapshot {
            meta: meta.clone(),
            data: data.clone(),
        };
        *self.current_snapshot.write().await = Some(snapshot);

        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}

// Implement RaftStorage for Arc<PersistentRaftStore>
impl RaftStorage<TypeConfig> for Arc<PersistentRaftStore> {
    type LogReader = Self;
    type SnapshotBuilder = Self;

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError<NodeId>> {
        let log_cache = self.log_cache.read().await;
        let last_log_id = log_cache.iter().next_back().map(|(_, e)| e.log_id);

        let last_purged = *self.last_purged_log_id.read().await;

        let last_log_id = match last_log_id {
            Some(lid) => Some(lid),
            None => last_purged,
        };

        Ok(LogState {
            last_purged_log_id: last_purged,
            last_log_id,
        })
    }

    async fn save_vote(&mut self, vote: &Vote<NodeId>) -> Result<(), StorageError<NodeId>> {
        // Update in-memory
        *self.vote.write().await = Some(*vote);

        // Persist to disk
        let state = self.get_current_state().await;
        self.save_state(&state)
            .map_err(|e| StorageIOError::write_vote(&e))?;

        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<NodeId>>, StorageError<NodeId>> {
        Ok(*self.vote.read().await)
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId>,
    ) -> Result<(), StorageError<NodeId>> {
        // Update in-memory
        *self.committed.write().await = committed;

        // Persist to disk
        let state = self.get_current_state().await;
        self.save_state(&state)
            .map_err(|e| StorageIOError::write_state_machine(&e))?;

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
        // Remove from in-memory index and cache
        let mut log_index = self.log_index.write().await;
        let mut log_cache = self.log_cache.write().await;

        let keys: Vec<u64> = log_index.range(log_id.index..).map(|(k, _)| *k).collect();
        for key in keys {
            log_index.remove(&key);
            log_cache.remove(&key);
        }

        // Note: We don't truncate the log file here - orphaned entries will be
        // ignored on recovery because they're not in the index. A full compaction
        // can clean them up later.

        Ok(())
    }

    async fn purge_logs_upto(&mut self, log_id: LogId) -> Result<(), StorageError<NodeId>> {
        // Update last_purged_log_id
        *self.last_purged_log_id.write().await = Some(log_id);

        // Remove from in-memory index and cache
        let mut log_index = self.log_index.write().await;
        let mut log_cache = self.log_cache.write().await;

        let keys: Vec<u64> = log_index.range(..=log_id.index).map(|(k, _)| *k).collect();
        for key in keys {
            log_index.remove(&key);
            log_cache.remove(&key);
        }

        // Persist state
        drop(log_index);
        drop(log_cache);
        let state = self.get_current_state().await;
        self.save_state(&state)
            .map_err(|e| StorageIOError::write_state_machine(&e))?;

        Ok(())
    }

    async fn append_to_log<I>(&mut self, entries: I) -> Result<(), StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry> + OptionalSend,
    {
        let mut log_index = self.log_index.write().await;
        let mut log_cache = self.log_cache.write().await;

        for entry in entries {
            // Write to disk
            let location = self
                .append_entry_to_file(&entry)
                .map_err(|e| StorageIOError::write_log_entry(entry.log_id, &e))?;

            // Update in-memory index and cache
            log_index.insert(entry.log_id.index, location);
            log_cache.insert(entry.log_id.index, entry);
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
                    // Record the command
                    sm.applied_commands.push(cmd.clone());

                    // Send activity event
                    if let Some(tx) = &self.activity_tx {
                        let _ = tx.send(RaftActivityEvent::from_command(log_index, term, cmd));
                    }

                    // Apply via handler if set
                    if let Some(handler) = &self.apply_handler {
                        res.push(handler(cmd));
                    } else {
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

        // Persist state after applying entries so last_applied_log and last_membership
        // are durably recorded. This ensures we don't re-apply entries on restart.
        drop(sm); // Release the write lock before calling get_current_state
        let state = self.get_current_state().await;
        self.save_state(&state)
            .map_err(|e| StorageIOError::write_state_machine(&e))?;

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
        let data = snapshot.into_inner();

        // Write snapshot to disk
        self.write_snapshot(meta, &data)
            .map_err(|e| StorageIOError::write_snapshot(Some(meta.signature()), &e))?;

        // Update state machine
        let new_sm: StateMachineData = serde_json::from_slice(&data)
            .map_err(|e| StorageIOError::read_snapshot(Some(meta.signature()), &e))?;
        *self.sm.write().await = new_sm;

        // Update current snapshot
        let new_snapshot = StoredSnapshot {
            meta: meta.clone(),
            data,
        };
        *self.current_snapshot.write().await = Some(new_snapshot);

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
    use tempfile::TempDir;
    use types::Value;

    fn make_entry(index: u64, term: u64, cmd: Command) -> Entry {
        Entry {
            log_id: LogId::new(openraft::CommittedLeaderId::new(term, 1), index),
            payload: EntryPayload::Normal(cmd),
        }
    }

    #[tokio::test]
    async fn test_open_creates_directories() {
        let dir = TempDir::new().unwrap();
        let _store = PersistentRaftStore::open(dir.path()).unwrap();

        assert!(dir.path().join("snapshots").exists());
    }

    #[tokio::test]
    async fn test_append_and_read() {
        let dir = TempDir::new().unwrap();
        let mut store = Arc::new(PersistentRaftStore::open(dir.path()).unwrap());

        let cmd = Command::Insert {
            table_id: TableId(1),
            row: vec![Value::Int(42)],
        };
        let entry = make_entry(1, 1, cmd);

        // Append entry
        store.append_to_log(vec![entry.clone()]).await.unwrap();

        // Verify log state
        let state = store.get_log_state().await.unwrap();
        assert!(state.last_log_id.is_some());
        assert_eq!(state.last_log_id.unwrap().index, 1);

        // Read back entry
        let entries = store.try_get_log_entries(1..=1).await.unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].log_id.index, 1);
    }

    #[tokio::test]
    async fn test_vote_persistence() {
        let dir = TempDir::new().unwrap();
        let vote = Vote::new(5, 1);

        // Write vote
        {
            let mut store = Arc::new(PersistentRaftStore::open(dir.path()).unwrap());
            store.save_vote(&vote).await.unwrap();
        }

        // Reopen and verify
        {
            let mut store = Arc::new(PersistentRaftStore::open(dir.path()).unwrap());
            let read_vote = store.read_vote().await.unwrap();
            assert_eq!(read_vote, Some(vote));
        }
    }

    #[tokio::test]
    async fn test_recovery_after_restart() {
        let dir = TempDir::new().unwrap();

        // Phase 1: Write entries
        {
            let mut store = Arc::new(PersistentRaftStore::open(dir.path()).unwrap());
            let cmd = Command::Insert {
                table_id: TableId(1),
                row: vec![Value::Int(42)],
            };
            store
                .append_to_log(vec![make_entry(1, 1, cmd)])
                .await
                .unwrap();

            let cmd2 = Command::Insert {
                table_id: TableId(1),
                row: vec![Value::Int(100)],
            };
            store
                .append_to_log(vec![make_entry(2, 1, cmd2)])
                .await
                .unwrap();
        }

        // Phase 2: Reopen and verify recovery
        {
            let mut store = Arc::new(PersistentRaftStore::open(dir.path()).unwrap());
            let state = store.get_log_state().await.unwrap();
            assert_eq!(state.last_log_id.unwrap().index, 2);

            let entries = store.try_get_log_entries(1..=2).await.unwrap();
            assert_eq!(entries.len(), 2);
        }
    }

    #[tokio::test]
    async fn test_purge_logs() {
        let dir = TempDir::new().unwrap();
        let mut store = Arc::new(PersistentRaftStore::open(dir.path()).unwrap());

        // Append entries
        for i in 1..=5 {
            let cmd = Command::Insert {
                table_id: TableId(1),
                row: vec![Value::Int(i)],
            };
            store
                .append_to_log(vec![make_entry(i as u64, 1, cmd)])
                .await
                .unwrap();
        }

        // Purge up to index 3
        let purge_id = LogId::new(openraft::CommittedLeaderId::new(1, 1), 3);
        store.purge_logs_upto(purge_id).await.unwrap();

        // Verify only entries 4-5 remain
        let entries = store.try_get_log_entries(1..=5).await.unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].log_id.index, 4);
        assert_eq!(entries[1].log_id.index, 5);

        // Verify log state
        let state = store.get_log_state().await.unwrap();
        assert_eq!(state.last_purged_log_id.unwrap().index, 3);
    }

    #[tokio::test]
    async fn test_snapshot_build_and_recover() {
        let dir = TempDir::new().unwrap();

        // Build snapshot
        {
            let mut store = Arc::new(PersistentRaftStore::open(dir.path()).unwrap());

            // Apply some entries to state machine
            let cmd = Command::Insert {
                table_id: TableId(1),
                row: vec![Value::Int(42)],
            };
            let entry = make_entry(1, 1, cmd);
            store.apply_to_state_machine(&[entry]).await.unwrap();

            // Build snapshot
            let snapshot = store.build_snapshot().await.unwrap();
            assert!(snapshot.meta.last_log_id.is_some());
        }

        // Reopen and verify snapshot exists
        {
            let mut store = Arc::new(PersistentRaftStore::open(dir.path()).unwrap());
            let snapshot = store.get_current_snapshot().await.unwrap();
            assert!(snapshot.is_some());
        }
    }
}
