use anyhow::{Context, Result};
use buffer::FilePager;
use catalog::{Catalog, Column, IndexKind};
use executor::{build_executor, execute_dml, execute_query, ExecutionContext};
use openraft::storage::{Adaptor, RaftLogStorage, RaftStateMachine};
use openraft::Raft;
use parser::{parse_sql, Statement};
use planner::{PhysicalPlan, Planner, PlanningContext, ResolvedExpr};
use raft::{
    ApplyHandler, ClusterConfig, Command, CommandResponse, HttpNetworkFactory, MemRaftStore,
    NetworkFactory, PersistentRaftStore, RaftHttpState, RaftNode, ServerHandle, TypeConfig,
};
use std::{
    collections::BTreeMap,
    fs,
    ops::DerefMut,
    path::{Path, PathBuf},
    sync::Arc,
};
use storage::HeapTable;
use tokio::sync::{Mutex, RwLock};
use types::Value;
use wal::{Wal, WalRecord};

/// Result type for database operations that may include query results.
#[derive(Debug)]
pub enum QueryResult {
    /// Query returned rows
    Rows {
        schema: Vec<String>,
        rows: Vec<common::Row>,
    },
    /// DML operation affected N rows
    Count { affected: u64 },
    /// DDL or other operation with no result
    Empty,
}

/// Configuration for Raft consensus mode.
#[derive(Clone, Debug)]
pub struct RaftConfig {
    /// Node ID for this database instance.
    pub node_id: u64,
    /// Whether Raft consensus is enabled.
    pub enabled: bool,
    /// Address this node listens on for Raft RPCs (e.g., "127.0.0.1:5001").
    /// Required for multi-node clusters.
    pub listen_addr: Option<String>,
    /// Peer nodes in the cluster: (node_id, address) pairs.
    /// Empty for single-node mode.
    pub peers: Vec<(u64, String)>,
    /// Whether to use persistent storage (survives restarts).
    /// When false, uses in-memory storage (data lost on restart).
    pub persistent_storage: bool,
}

impl Default for RaftConfig {
    fn default() -> Self {
        Self {
            node_id: 1,
            enabled: false,
            listen_addr: None,
            peers: Vec::new(),
            persistent_storage: false,
        }
    }
}

impl RaftConfig {
    /// Create a new Raft config for single-node mode (in-memory storage).
    pub fn single_node(node_id: u64) -> Self {
        Self {
            node_id,
            enabled: true,
            listen_addr: None,
            peers: Vec::new(),
            persistent_storage: false,
        }
    }

    /// Create a new Raft config for single-node mode with persistent storage.
    pub fn single_node_persistent(node_id: u64) -> Self {
        Self {
            node_id,
            enabled: true,
            listen_addr: None,
            peers: Vec::new(),
            persistent_storage: true,
        }
    }

    /// Enable or disable persistent storage.
    pub fn with_persistent_storage(mut self, enabled: bool) -> Self {
        self.persistent_storage = enabled;
        self
    }

    /// Create a new Raft config for multi-node cluster (in-memory storage).
    ///
    /// # Arguments
    /// * `node_id` - This node's ID
    /// * `listen_addr` - Address to listen on (e.g., "127.0.0.1:5001")
    /// * `peers` - Other nodes in the cluster: (node_id, address) pairs
    pub fn cluster(
        node_id: u64,
        listen_addr: impl Into<String>,
        peers: Vec<(u64, String)>,
    ) -> Self {
        Self {
            node_id,
            enabled: true,
            listen_addr: Some(listen_addr.into()),
            peers,
            persistent_storage: false,
        }
    }

    /// Create a new Raft config for multi-node cluster with persistent storage.
    pub fn cluster_persistent(
        node_id: u64,
        listen_addr: impl Into<String>,
        peers: Vec<(u64, String)>,
    ) -> Self {
        Self {
            node_id,
            enabled: true,
            listen_addr: Some(listen_addr.into()),
            peers,
            persistent_storage: true,
        }
    }

    /// Check if this is a multi-node cluster configuration.
    pub fn is_multi_node(&self) -> bool {
        !self.peers.is_empty() && self.listen_addr.is_some()
    }
}

/// Async database wrapper for multi-threaded server use.
///
/// This is the main entry point for executing SQL statements.
/// Resources are wrapped in Arc/RwLock/Mutex for safe concurrent access.
/// All I/O operations are performed in spawn_blocking to avoid blocking the async runtime.
pub struct Database {
    data_dir: Arc<PathBuf>,
    catalog_path: Arc<PathBuf>,
    wal_path: Arc<PathBuf>,
    buffer_pages: usize,
    catalog: Arc<RwLock<Catalog>>,
    pager: Arc<Mutex<FilePager>>,
    wal: Arc<Mutex<Wal>>,
    /// Raft consensus node (None if Raft is disabled)
    raft: Option<Arc<RaftNode>>,
    /// HTTP server handle for Raft RPCs (multi-node mode only)
    #[allow(dead_code)]
    http_server: Option<ServerHandle>,
    /// Node ID for Raft
    node_id: u64,
}

impl Database {
    /// Create a new async database instance.
    ///
    /// Creates the data directory if it doesn't exist, loads the catalog,
    /// initializes the pager, and opens the WAL.
    /// All I/O operations are performed in spawn_blocking.
    pub async fn new(
        data_dir: &Path,
        catalog_file: &str,
        wal_file: &str,
        buffer_pages: usize,
    ) -> Result<Self> {
        Self::with_raft_config(data_dir, catalog_file, wal_file, buffer_pages, None).await
    }

    /// Create a new async database instance with optional Raft configuration.
    pub async fn with_raft_config(
        data_dir: &Path,
        catalog_file: &str,
        wal_file: &str,
        buffer_pages: usize,
        raft_config: Option<RaftConfig>,
    ) -> Result<Self> {
        let data_dir_owned = data_dir.to_path_buf();
        let catalog_file_owned = catalog_file.to_string();
        let wal_file_owned = wal_file.to_string();

        let (catalog, pager, wal, catalog_path, wal_path) =
            tokio::task::spawn_blocking(move || {
                fs::create_dir_all(&data_dir_owned).with_context(|| {
                    format!(
                        "failed to create data directory {}",
                        data_dir_owned.display()
                    )
                })?;

                let catalog_path = data_dir_owned.join(&catalog_file_owned);
                let wal_path = data_dir_owned.join(&wal_file_owned);
                let catalog = Catalog::load(&catalog_path).map_err(anyhow::Error::from)?;
                let pager = FilePager::new(&data_dir_owned, buffer_pages);
                let wal = Wal::open(&wal_path).map_err(anyhow::Error::from)?;

                Ok::<_, anyhow::Error>((catalog, pager, wal, catalog_path, wal_path))
            })
            .await??;

        let data_dir_arc = Arc::new(data_dir.to_path_buf());
        let catalog_arc = Arc::new(RwLock::new(catalog));

        // Initialize Raft if configured
        let (raft, http_server, node_id) = if let Some(config) = raft_config.filter(|c| c.enabled) {
            let (raft_node, server) =
                Self::init_raft(&config, catalog_arc.clone(), data_dir_arc.clone()).await?;
            (Some(Arc::new(raft_node)), server, config.node_id)
        } else {
            (None, None, 1)
        };

        Ok(Self {
            data_dir: data_dir_arc,
            catalog_path: Arc::new(catalog_path),
            wal_path: Arc::new(wal_path),
            buffer_pages,
            catalog: catalog_arc,
            pager: Arc::new(Mutex::new(pager)),
            wal: Arc::new(Mutex::new(wal)),
            raft,
            http_server,
            node_id,
        })
    }

    /// Initialize Raft consensus for this database.
    ///
    /// For single-node mode: Uses stub NetworkFactory and initializes immediately.
    /// For multi-node mode: Uses HTTP-based network and starts HTTP server.
    /// Storage type determined by `config.persistent_storage`.
    async fn init_raft(
        config: &RaftConfig,
        catalog: Arc<RwLock<Catalog>>,
        data_dir: Arc<PathBuf>,
    ) -> Result<(RaftNode, Option<ServerHandle>)> {
        let node_id = config.node_id;

        // Create apply handler that applies commands to actual storage
        let apply_handler = Self::create_apply_handler(catalog, data_dir.clone());

        // Create Raft config
        let raft_config = Arc::new(openraft::Config {
            cluster_name: "sql-database".to_string(),
            election_timeout_min: 150,
            election_timeout_max: 300,
            heartbeat_interval: 50,
            ..Default::default()
        });

        // Create Raft node with appropriate storage type
        if config.persistent_storage {
            // Persistent storage - survives restarts
            let raft_data_dir = data_dir.join("raft");

            // Check if this is a restart (state file exists)
            let is_restart = raft_data_dir.join("raft_state.json").exists();

            let store = Arc::new(
                PersistentRaftStore::open_with_handler(&raft_data_dir, Some(apply_handler))
                    .map_err(|e| {
                        anyhow::anyhow!("failed to open persistent Raft storage: {}", e)
                    })?,
            );

            let (log_store, state_machine) =
                Adaptor::<TypeConfig, Arc<PersistentRaftStore>>::new(store);

            if config.is_multi_node() {
                Self::init_raft_multi_node(
                    config,
                    raft_config,
                    log_store,
                    state_machine,
                    is_restart,
                )
                .await
            } else {
                Self::init_raft_single_node(
                    node_id,
                    raft_config,
                    log_store,
                    state_machine,
                    is_restart,
                )
                .await
            }
        } else {
            // In-memory storage - faster but lost on restart (always fresh)
            let store = Arc::new(MemRaftStore::with_apply_handler(apply_handler));

            let (log_store, state_machine) = Adaptor::<TypeConfig, Arc<MemRaftStore>>::new(store);

            if config.is_multi_node() {
                Self::init_raft_multi_node(config, raft_config, log_store, state_machine, false)
                    .await
            } else {
                Self::init_raft_single_node(node_id, raft_config, log_store, state_machine, false)
                    .await
            }
        }
    }

    /// Initialize Raft in single-node mode (no network required).
    ///
    /// If `is_restart` is true, skip initialization (cluster already exists).
    async fn init_raft_single_node(
        node_id: u64,
        raft_config: Arc<openraft::Config>,
        log_store: impl RaftLogStorage<TypeConfig> + 'static,
        state_machine: impl RaftStateMachine<TypeConfig> + 'static,
        is_restart: bool,
    ) -> Result<(RaftNode, Option<ServerHandle>)> {
        // Create stub network factory
        let network = NetworkFactory::new(node_id);

        // Create Raft node
        let raft = Raft::<TypeConfig>::new(node_id, raft_config, network, log_store, state_machine)
            .await
            .map_err(|e| anyhow::anyhow!("failed to create Raft node: {}", e))?;

        // Only initialize on fresh start, not on restart
        if !is_restart {
            // Initialize as single-node cluster
            let mut members = BTreeMap::new();
            members.insert(node_id, openraft::BasicNode::default());

            raft.initialize(members)
                .await
                .map_err(|e| anyhow::anyhow!("failed to initialize Raft cluster: {}", e))?;
        }

        // Wait for the node to become leader
        Self::wait_for_leader(&raft, node_id).await?;

        Ok((raft, None))
    }

    /// Initialize Raft in multi-node mode with HTTP transport.
    ///
    /// If `is_restart` is true, skip initialization (cluster already exists).
    async fn init_raft_multi_node(
        config: &RaftConfig,
        raft_config: Arc<openraft::Config>,
        log_store: impl RaftLogStorage<TypeConfig> + 'static,
        state_machine: impl RaftStateMachine<TypeConfig> + 'static,
        is_restart: bool,
    ) -> Result<(RaftNode, Option<ServerHandle>)> {
        let node_id = config.node_id;
        let listen_addr = config
            .listen_addr
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("listen_addr required for multi-node mode"))?;

        // Build cluster config with this node and all peers
        let mut cluster_config = ClusterConfig::new();
        cluster_config.add_node(node_id, format!("http://{}", listen_addr));
        for (peer_id, peer_addr) in &config.peers {
            // Ensure peer addresses have http:// prefix
            let addr = if peer_addr.starts_with("http://") || peer_addr.starts_with("https://") {
                peer_addr.clone()
            } else {
                format!("http://{}", peer_addr)
            };
            cluster_config.add_node(*peer_id, addr);
        }

        // Create HTTP network factory
        let network = HttpNetworkFactory::new(node_id, cluster_config);

        // Create Raft node
        let raft = Raft::<TypeConfig>::new(node_id, raft_config, network, log_store, state_machine)
            .await
            .map_err(|e| anyhow::anyhow!("failed to create Raft node: {}", e))?;

        // Start HTTP server for Raft RPCs
        let raft_arc = Arc::new(raft);
        let http_state = RaftHttpState::new(raft_arc.clone());
        let addr: std::net::SocketAddr = listen_addr
            .parse()
            .map_err(|e| anyhow::anyhow!("invalid listen address '{}': {}", listen_addr, e))?;

        let server_handle = raft::start_server(addr, http_state)
            .await
            .map_err(|e| anyhow::anyhow!("failed to start Raft HTTP server: {}", e))?;

        // For the first node (node_id == 1), initialize cluster membership on fresh start
        // Other nodes will join via membership change requests
        if node_id == 1 && !is_restart {
            let mut members = BTreeMap::new();
            members.insert(node_id, openraft::BasicNode::default());
            // Add all peer nodes to initial membership
            for (peer_id, _) in &config.peers {
                members.insert(*peer_id, openraft::BasicNode::default());
            }

            // Try to initialize - may fail if already initialized
            let _ = raft_arc.initialize(members).await;
        }

        // Wait for leader election (on restart, previous leader should re-elect)
        if node_id == 1 {
            Self::wait_for_leader(&raft_arc, node_id).await?;
        }

        // Extract the inner Raft from Arc (we need to return owned RaftNode)
        // This is safe because we just created it and hold the only reference
        let raft =
            Arc::try_unwrap(raft_arc).map_err(|_| anyhow::anyhow!("failed to unwrap Raft node"))?;

        Ok((raft, Some(server_handle)))
    }

    /// Wait for this node to become leader (or timeout).
    async fn wait_for_leader(raft: &RaftNode, expected_leader: u64) -> Result<()> {
        let mut attempts = 0;
        // Wait up to 2 seconds (200 * 10ms) for leader election.
        // On restart, the node needs to complete an election cycle which may take
        // election_timeout_max (300ms) plus some processing time.
        loop {
            let metrics = raft.metrics().borrow().clone();
            if metrics.current_leader == Some(expected_leader) {
                return Ok(());
            }
            attempts += 1;
            if attempts > 200 {
                return Err(anyhow::anyhow!(
                    "timeout waiting for node {} to become leader",
                    expected_leader
                ));
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        }
    }

    /// Check if this node is the Raft leader.
    ///
    /// For non-Raft mode, this always returns true.
    /// For Raft mode, this checks the current Raft metrics.
    pub fn is_leader(&self) -> bool {
        if let Some(ref raft) = self.raft {
            let metrics = raft.metrics().borrow().clone();
            metrics.current_leader == Some(self.node_id)
        } else {
            // Non-Raft mode is always "leader"
            true
        }
    }

    /// Get the current leader node ID, if known.
    pub async fn current_leader(&self) -> Option<u64> {
        if let Some(ref raft) = self.raft {
            let metrics = raft.metrics().borrow().clone();
            metrics.current_leader
        } else {
            Some(self.node_id)
        }
    }

    /// Check if this node is the leader, and return an error if not.
    ///
    /// Returns the current leader's node ID in the error message if known.
    fn require_leader(&self) -> Result<()> {
        if !self.is_raft_enabled() {
            return Ok(()); // Non-Raft mode is always allowed
        }

        if let Some(ref raft) = self.raft {
            let metrics = raft.metrics().borrow().clone();
            if metrics.current_leader == Some(self.node_id) {
                return Ok(());
            }

            // Not the leader - return error with leader info if known
            let leader_info = metrics
                .current_leader
                .map(|id| format!(" (current leader: node {})", id))
                .unwrap_or_default();
            Err(anyhow::anyhow!(
                "not the leader: this node is {}, cannot accept writes{}",
                self.node_id,
                leader_info
            ))
        } else {
            Ok(())
        }
    }

    /// Check if Raft consensus is enabled.
    pub fn is_raft_enabled(&self) -> bool {
        self.raft.is_some()
    }

    /// Execute a SQL statement and return results.
    ///
    /// This is the main entry point for SQL execution.
    /// Handles DDL (CREATE/DROP TABLE/INDEX) and delegates DML/queries to executor.
    pub async fn execute(&self, sql: &str) -> Result<QueryResult> {
        let statements = parse_sql(sql).map_err(anyhow::Error::from)?;

        if statements.is_empty() {
            return Ok(QueryResult::Empty);
        }

        if statements.len() > 1 {
            anyhow::bail!("multiple statements not supported yet");
        }

        let stmt = statements.into_iter().next().unwrap();
        self.execute_statement(stmt).await
    }

    /// Execute a single parsed statement.
    async fn execute_statement(&self, stmt: Statement) -> Result<QueryResult> {
        match stmt {
            Statement::CreateTable {
                name,
                columns,
                primary_key,
            } => self.execute_create_table(name, columns, primary_key).await,

            Statement::DropTable { name } => self.execute_drop_table(name).await,

            Statement::CreateIndex {
                name,
                table,
                column,
            } => self.execute_create_index(name, table, column).await,

            Statement::DropIndex { name } => self.execute_drop_index(name).await,

            Statement::Explain { query, analyze } => self.execute_explain(*query, analyze).await,

            other => self.execute_query_or_dml(other).await,
        }
    }

    /// Execute CREATE TABLE statement.
    async fn execute_create_table(
        &self,
        name: String,
        columns: Vec<parser::ColumnDef>,
        primary_key: Option<Vec<String>>,
    ) -> Result<QueryResult> {
        // CPU-bound work: map columns and validate primary key
        let catalog_columns: Vec<Column> = columns
            .iter()
            .map(|col| {
                let ty = map_sql_type(&col.ty)?;
                Ok(Column::new(col.name.clone(), ty))
            })
            .collect::<Result<Vec<_>>>()?;

        let primary_key_ordinals = if let Some(pk_names) = primary_key {
            let mut ordinals = Vec::new();
            for pk_name in &pk_names {
                let ordinal = columns
                    .iter()
                    .position(|col| col.name.eq_ignore_ascii_case(pk_name))
                    .ok_or_else(|| {
                        anyhow::anyhow!(
                            "PRIMARY KEY column '{}' not found in table columns",
                            pk_name
                        )
                    })? as u16;
                ordinals.push(ordinal);
            }
            Some(ordinals)
        } else {
            None
        };

        // Clone Arc references for spawn_blocking
        let catalog = self.catalog.clone();
        let catalog_path = self.catalog_path.clone();
        let wal = self.wal.clone();

        tokio::task::spawn_blocking(move || {
            // Acquire write lock on catalog (exclusive access)
            let mut catalog_lock = catalog.blocking_write();

            let table_id = catalog_lock
                .create_table(&name, catalog_columns, primary_key_ordinals)
                .map_err(anyhow::Error::from)?;

            // Persist catalog to disk (blocking I/O)
            catalog_lock
                .save(&catalog_path)
                .map_err(anyhow::Error::from)?;

            drop(catalog_lock); // Release catalog lock

            // Log WAL (exclusive access, blocking I/O)
            let mut wal_lock = wal.blocking_lock();
            wal_lock
                .append(&WalRecord::CreateTable {
                    name: name.clone(),
                    table: table_id,
                })
                .and_then(|_| wal_lock.sync())
                .map_err(anyhow::Error::from)?;

            Ok(QueryResult::Empty)
        })
        .await?
    }

    /// Execute DROP TABLE statement.
    async fn execute_drop_table(&self, name: String) -> Result<QueryResult> {
        let catalog = self.catalog.clone();
        let catalog_path = self.catalog_path.clone();
        let data_dir = self.data_dir.clone();
        let wal = self.wal.clone();

        tokio::task::spawn_blocking(move || {
            // Acquire write lock on catalog
            let mut catalog_lock = catalog.blocking_write();

            let table_id = catalog_lock.table(&name).map_err(anyhow::Error::from)?.id;
            catalog_lock
                .drop_table(&name)
                .map_err(anyhow::Error::from)?;

            // Persist catalog
            catalog_lock
                .save(&catalog_path)
                .map_err(anyhow::Error::from)?;

            drop(catalog_lock);

            // Remove heap file (blocking I/O)
            let path = data_dir.join(format!("{name}.heap"));
            if path.exists() {
                fs::remove_file(&path)
                    .with_context(|| format!("failed to remove heap file {}", path.display()))?;
            }

            // Log WAL
            let mut wal_lock = wal.blocking_lock();
            wal_lock
                .append(&WalRecord::DropTable { table: table_id })
                .and_then(|_| wal_lock.sync())
                .map_err(anyhow::Error::from)?;

            Ok(QueryResult::Empty)
        })
        .await?
    }

    /// Execute CREATE INDEX statement.
    async fn execute_create_index(
        &self,
        name: String,
        table: String,
        column: String,
    ) -> Result<QueryResult> {
        let catalog = self.catalog.clone();
        let catalog_path = self.catalog_path.clone();

        tokio::task::spawn_blocking(move || {
            let mut catalog_lock = catalog.blocking_write();

            catalog_lock
                .create_index()
                .table_name(&table)
                .index_name(&name)
                .columns(&[column.as_str()])
                .kind(IndexKind::BTree)
                .call()
                .map_err(anyhow::Error::from)?;

            catalog_lock
                .save(&catalog_path)
                .map_err(anyhow::Error::from)?;

            Ok(QueryResult::Empty)
        })
        .await?
    }

    /// Execute DROP INDEX statement.
    async fn execute_drop_index(&self, name: String) -> Result<QueryResult> {
        let catalog = self.catalog.clone();
        let catalog_path = self.catalog_path.clone();

        tokio::task::spawn_blocking(move || {
            let mut catalog_lock = catalog.blocking_write();

            let table_name = catalog_lock
                .tables()
                .find(|table| table.index(&name).is_ok())
                .map(|table| table.name.clone())
                .ok_or_else(|| anyhow::anyhow!("index '{}' not found", name))?;

            catalog_lock
                .drop_index(&table_name, &name)
                .map_err(anyhow::Error::from)?;

            catalog_lock
                .save(&catalog_path)
                .map_err(anyhow::Error::from)?;

            Ok(QueryResult::Empty)
        })
        .await?
    }

    /// Execute EXPLAIN or EXPLAIN ANALYZE statement.
    async fn execute_explain(&self, query: Statement, analyze: bool) -> Result<QueryResult> {
        let catalog = self.catalog.clone();
        let pager = self.pager.clone();
        let wal = self.wal.clone();
        let data_dir = self.data_dir.clone();

        tokio::task::spawn_blocking(move || {
            let catalog_lock = catalog.blocking_read();
            let mut planning_ctx = PlanningContext::new(&catalog_lock);
            let plan = Planner::plan(query, &mut planning_ctx).map_err(anyhow::Error::from)?;

            if analyze {
                // EXPLAIN ANALYZE: Execute the query and collect statistics
                let plan_description = planner::explain_physical(&plan);

                let mut pager_lock = pager.blocking_lock();
                let mut wal_lock = wal.blocking_lock();
                let mut ctx = ExecutionContext::new(
                    &catalog_lock,
                    pager_lock.deref_mut(),
                    wal_lock.deref_mut(),
                    data_dir.as_ref().clone(),
                );

                let mut executor = build_executor(plan).map_err(anyhow::Error::from)?;
                executor.open(&mut ctx).map_err(anyhow::Error::from)?;

                let mut row_count = 0;
                while executor
                    .next(&mut ctx)
                    .map_err(anyhow::Error::from)?
                    .is_some()
                {
                    row_count += 1;
                }
                executor.close(&mut ctx).map_err(anyhow::Error::from)?;

                // Format the output
                let mut output = String::new();
                output.push_str("EXPLAIN ANALYZE:\n");
                output.push_str(&plan_description);
                output.push_str("\n\nExecution Statistics:\n");
                output.push_str(&executor::format_explain_analyze(
                    executor.as_ref(),
                    "Query",
                ));
                output.push_str(&format!("\nTotal rows: {}", row_count));

                Ok(QueryResult::Rows {
                    schema: vec!["Explain".to_string()],
                    rows: vec![common::Row::new(vec![Value::Text(output)])],
                })
            } else {
                // EXPLAIN: Just show the plan
                let description = planner::explain_physical(&plan);
                Ok(QueryResult::Rows {
                    schema: vec!["Explain".to_string()],
                    rows: vec![common::Row::new(vec![Value::Text(description)])],
                })
            }
        })
        .await?
    }

    /// Execute a query or DML statement (SELECT, INSERT, UPDATE, DELETE).
    async fn execute_query_or_dml(&self, stmt: Statement) -> Result<QueryResult> {
        // If Raft is enabled and this is a DML statement, route through Raft
        if self.is_raft_enabled() && is_dml_statement(&stmt) {
            // Check that we're the leader before accepting writes
            self.require_leader()?;
            return self.execute_dml_via_raft(stmt).await;
        }

        // Otherwise use the standard synchronous executor path
        let catalog = self.catalog.clone();
        let pager = self.pager.clone();
        let wal = self.wal.clone();
        let data_dir = self.data_dir.clone();

        tokio::task::spawn_blocking(move || {
            // Acquire read lock on catalog (shared access for queries/DML)
            let catalog_lock = catalog.blocking_read();
            let mut planning_ctx = PlanningContext::new(&catalog_lock);
            let plan = Planner::plan(stmt, &mut planning_ctx).map_err(anyhow::Error::from)?;

            // Acquire exclusive locks on pager and WAL
            let mut pager_lock = pager.blocking_lock();
            let mut wal_lock = wal.blocking_lock();
            let mut ctx = ExecutionContext::new(
                &catalog_lock,
                pager_lock.deref_mut(),
                wal_lock.deref_mut(),
                data_dir.as_ref().clone(),
            );

            match plan {
                PhysicalPlan::Insert { .. }
                | PhysicalPlan::Update { .. }
                | PhysicalPlan::Delete { .. } => {
                    let count = execute_dml(plan, &mut ctx).map_err(anyhow::Error::from)?;
                    Ok(QueryResult::Count { affected: count })
                }
                ref query_plan => {
                    let schema = infer_schema(query_plan);
                    let rows = execute_query(plan, &mut ctx).map_err(anyhow::Error::from)?;
                    Ok(QueryResult::Rows { schema, rows })
                }
            }
        })
        .await?
    }

    /// Reset the database by removing all data files and reinitializing.
    pub async fn reset(&self) -> Result<()> {
        let data_dir = self.data_dir.clone();
        let catalog_path = self.catalog_path.clone();
        let wal_path = self.wal_path.clone();
        let catalog = self.catalog.clone();
        let pager = self.pager.clone();
        let wal = self.wal.clone();
        let buffer_pages = self.buffer_pages;

        tokio::task::spawn_blocking(move || {
            // Remove all table files (.tbl) and heap files (.heap)
            let entries = fs::read_dir(&*data_dir)
                .with_context(|| format!("failed to read data directory {}", data_dir.display()))?;

            for entry in entries.flatten() {
                let path = entry.path();
                if let Some(ext) = path.extension() {
                    if ext == "heap" || ext == "tbl" {
                        fs::remove_file(&path)
                            .with_context(|| format!("failed to remove file {}", path.display()))?;
                    }
                }
            }

            // Remove catalog file if it exists
            if catalog_path.exists() {
                fs::remove_file(&*catalog_path).with_context(|| {
                    format!("failed to remove catalog {}", catalog_path.display())
                })?;
            }

            // Remove WAL file (need to close and reopen)
            {
                let mut wal_lock = wal.blocking_lock();
                // Close the WAL by dropping the old one
                *wal_lock = Wal::open(&**wal_path).map_err(anyhow::Error::from)?;
            }
            if wal_path.exists() {
                fs::remove_file(&**wal_path)
                    .with_context(|| format!("failed to remove WAL {}", wal_path.display()))?;
            }

            // Reinitialize catalog
            {
                let mut catalog_lock = catalog.blocking_write();
                *catalog_lock = Catalog::load(&catalog_path).map_err(anyhow::Error::from)?;
            }

            // Reinitialize pager (clear buffer pool)
            {
                let mut pager_lock = pager.blocking_lock();
                *pager_lock = FilePager::new(&**data_dir, buffer_pages);
            }

            // Reinitialize WAL
            {
                let mut wal_lock = wal.blocking_lock();
                *wal_lock = Wal::open(&**wal_path).map_err(anyhow::Error::from)?;
            }

            Ok(())
        })
        .await?
    }

    /// Get a clone of the catalog Arc for async access.
    ///
    /// Use this to read catalog metadata in async contexts.
    /// For synchronous access within spawn_blocking, use catalog.blocking_read().
    pub fn catalog(&self) -> Arc<RwLock<Catalog>> {
        self.catalog.clone()
    }

    /// Get the data directory path.
    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    /// Get the Raft node, if Raft is enabled.
    pub fn raft_node(&self) -> Option<&Arc<RaftNode>> {
        self.raft.as_ref()
    }

    /// Get the node ID.
    pub fn node_id(&self) -> u64 {
        self.node_id
    }

    /// Write a command through Raft consensus.
    ///
    /// This routes the command through Raft for consensus, and the state machine
    /// applies it to actual storage when committed.
    ///
    /// # Errors
    /// Returns an error if Raft is not enabled or if the Raft write fails.
    async fn raft_write(&self, cmd: Command) -> Result<CommandResponse> {
        let raft = self
            .raft
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Raft not enabled"))?;
        raft.client_write(cmd)
            .await
            .map(|res| res.data)
            .map_err(|e| anyhow::anyhow!("Raft write failed: {}", e))
    }

    /// Execute a DML statement through Raft consensus.
    ///
    /// For INSERT: Converts directly to a Command and writes through Raft.
    /// For UPDATE/DELETE: First scans to find matching rows, then sends individual
    /// commands for each row through Raft.
    async fn execute_dml_via_raft(&self, stmt: Statement) -> Result<QueryResult> {
        match stmt {
            Statement::Insert { table, values } => {
                let cmd = self.insert_to_command(&table, &values).await?;
                let response = self.raft_write(cmd).await?;
                match response {
                    CommandResponse::Insert { .. } => Ok(QueryResult::Count { affected: 1 }),
                    CommandResponse::Error { message } => Err(anyhow::anyhow!("{}", message)),
                    _ => Ok(QueryResult::Empty),
                }
            }
            Statement::Update {
                table,
                assignments,
                selection,
            } => {
                self.execute_update_via_raft(table, assignments, selection)
                    .await
            }
            Statement::Delete { table, selection } => {
                self.execute_delete_via_raft(table, selection).await
            }
            _ => Err(anyhow::anyhow!(
                "statement not supported through Raft: {:?}",
                stmt
            )),
        }
    }

    /// Execute UPDATE through Raft by scanning for matching rows first.
    async fn execute_update_via_raft(
        &self,
        table: String,
        assignments: Vec<(String, expr::Expr)>,
        selection: Option<expr::Expr>,
    ) -> Result<QueryResult> {
        // Get table metadata
        let (table_id, schema_names) = {
            let catalog_lock = self.catalog.read().await;
            let table_meta = catalog_lock
                .table(&table)
                .map_err(|e| anyhow::anyhow!("table lookup failed: {}", e))?;
            let schema_names: Vec<String> = table_meta
                .schema
                .columns()
                .iter()
                .map(|c| c.name.clone())
                .collect();
            (table_meta.id, schema_names)
        };

        // Resolve assignments: column name -> (column_id, new_value)
        let resolved_assignments: Vec<(u16, Value)> = assignments
            .iter()
            .map(|(col_name, expr)| {
                let col_idx = schema_names
                    .iter()
                    .position(|n| n == col_name)
                    .ok_or_else(|| anyhow::anyhow!("column '{}' not found", col_name))?
                    as u16;
                let value = eval_literal_expr(expr)?;
                Ok((col_idx, value))
            })
            .collect::<Result<Vec<_>>>()?;

        // Find matching rows by executing a scan
        let matching_rows = self
            .find_matching_rows(table_id, &schema_names, selection)
            .await?;

        // For each matching row, send an UPDATE command through Raft
        let mut affected = 0u64;
        for (rid, old_row) in matching_rows {
            // Build new row by applying assignments
            let mut new_values = old_row.values.clone();
            for (col_idx, value) in &resolved_assignments {
                new_values[*col_idx as usize] = value.clone();
            }

            let cmd = Command::Update {
                table_id,
                rid,
                new_row: new_values,
            };

            let response = self.raft_write(cmd).await?;
            match response {
                CommandResponse::Update { rows_affected } => affected += rows_affected,
                CommandResponse::Error { message } => return Err(anyhow::anyhow!("{}", message)),
                _ => {}
            }
        }

        Ok(QueryResult::Count { affected })
    }

    /// Execute DELETE through Raft by scanning for matching rows first.
    async fn execute_delete_via_raft(
        &self,
        table: String,
        selection: Option<expr::Expr>,
    ) -> Result<QueryResult> {
        // Get table metadata
        let (table_id, schema_names) = {
            let catalog_lock = self.catalog.read().await;
            let table_meta = catalog_lock
                .table(&table)
                .map_err(|e| anyhow::anyhow!("table lookup failed: {}", e))?;
            let schema_names: Vec<String> = table_meta
                .schema
                .columns()
                .iter()
                .map(|c| c.name.clone())
                .collect();
            (table_meta.id, schema_names)
        };

        // Find matching rows by executing a scan
        let matching_rows = self
            .find_matching_rows(table_id, &schema_names, selection)
            .await?;

        // For each matching row, send a DELETE command through Raft
        let mut affected = 0u64;
        for (rid, _row) in matching_rows {
            let cmd = Command::Delete { table_id, rid };

            let response = self.raft_write(cmd).await?;
            match response {
                CommandResponse::Delete { rows_affected } => affected += rows_affected,
                CommandResponse::Error { message } => return Err(anyhow::anyhow!("{}", message)),
                _ => {}
            }
        }

        Ok(QueryResult::Count { affected })
    }

    /// Find rows matching an optional predicate, returning their RIDs and data.
    async fn find_matching_rows(
        &self,
        table_id: common::TableId,
        schema_names: &[String],
        selection: Option<expr::Expr>,
    ) -> Result<Vec<(common::RecordId, common::Row)>> {
        let catalog = self.catalog.clone();
        let pager = self.pager.clone();
        let wal = self.wal.clone();
        let data_dir = self.data_dir.clone();
        let schema_names = schema_names.to_vec();

        tokio::task::spawn_blocking(move || {
            let catalog_lock = catalog.blocking_read();
            let mut pager_lock = pager.blocking_lock();
            let mut wal_lock = wal.blocking_lock();
            let mut ctx = ExecutionContext::new(
                &catalog_lock,
                pager_lock.deref_mut(),
                wal_lock.deref_mut(),
                data_dir.as_ref().clone(),
            );

            // Build a scan plan with optional filter
            let plan = if let Some(pred) = selection {
                // Resolve the predicate expression
                let resolved_pred = resolve_expr_for_scan(&pred, &schema_names)?;
                PhysicalPlan::Filter {
                    input: Box::new(PhysicalPlan::SeqScan {
                        table_id,
                        schema: schema_names.clone(),
                    }),
                    predicate: resolved_pred,
                }
            } else {
                PhysicalPlan::SeqScan {
                    table_id,
                    schema: schema_names.clone(),
                }
            };

            // Execute the scan
            let mut executor = build_executor(plan).map_err(anyhow::Error::from)?;
            executor.open(&mut ctx).map_err(anyhow::Error::from)?;

            let mut results = Vec::new();
            while let Some(row) = executor.next(&mut ctx).map_err(anyhow::Error::from)? {
                if let Some(rid) = row.rid() {
                    results.push((rid, row));
                }
            }
            executor.close(&mut ctx).map_err(anyhow::Error::from)?;

            Ok(results)
        })
        .await?
    }

    /// Convert an INSERT statement to a Raft Command.
    ///
    /// This resolves table names to IDs and evaluates value expressions.
    async fn insert_to_command(&self, table: &str, values: &[expr::Expr]) -> Result<Command> {
        let catalog_lock = self.catalog.read().await;
        let table_meta = catalog_lock
            .table(table)
            .map_err(|e| anyhow::anyhow!("table lookup failed: {}", e))?;
        let table_id = table_meta.id;

        // Evaluate value expressions (they should all be literals for now)
        let row_values: Vec<Value> = values
            .iter()
            .map(eval_literal_expr)
            .collect::<Result<Vec<_>>>()?;

        Ok(Command::Insert {
            table_id,
            row: row_values,
        })
    }

    /// Create the apply handler for Raft state machine.
    ///
    /// This handler is called when Raft commits a command, and it applies
    /// the command to actual database storage.
    ///
    /// Note: This uses block_in_place to allow blocking catalog access from async context.
    fn create_apply_handler(catalog: Arc<RwLock<Catalog>>, data_dir: Arc<PathBuf>) -> ApplyHandler {
        Arc::new(move |cmd: &Command| {
            // Use block_in_place to safely call blocking operations
            // from within an async runtime context
            tokio::task::block_in_place(|| match cmd {
                Command::Insert { table_id, row } => {
                    // Get table metadata
                    let catalog_lock = catalog.blocking_read();
                    let table_meta = match catalog_lock.table_by_id(*table_id) {
                        Ok(t) => t,
                        Err(e) => {
                            return CommandResponse::error(format!("table lookup failed: {}", e))
                        }
                    };

                    // Open heap file and insert
                    let file_path = data_dir.join(format!("{}.heap", table_meta.name));
                    let mut heap_file = match storage::HeapFile::open(&file_path, table_id.0) {
                        Ok(h) => h,
                        Err(e) => {
                            return CommandResponse::error(format!(
                                "failed to open heap file: {}",
                                e
                            ))
                        }
                    };

                    let rid = match heap_file.insert(&common::Row::new(row.clone())) {
                        Ok(r) => r,
                        Err(e) => return CommandResponse::error(format!("insert failed: {}", e)),
                    };

                    CommandResponse::insert(rid)
                }
                Command::Update {
                    table_id,
                    rid,
                    new_row,
                } => {
                    // Get table metadata
                    let catalog_lock = catalog.blocking_read();
                    let table_meta = match catalog_lock.table_by_id(*table_id) {
                        Ok(t) => t,
                        Err(e) => {
                            return CommandResponse::error(format!("table lookup failed: {}", e))
                        }
                    };

                    // Open heap file and update
                    let file_path = data_dir.join(format!("{}.heap", table_meta.name));
                    let mut heap_file = match storage::HeapFile::open(&file_path, table_id.0) {
                        Ok(h) => h,
                        Err(e) => {
                            return CommandResponse::error(format!(
                                "failed to open heap file: {}",
                                e
                            ))
                        }
                    };

                    let new_row_obj = common::Row::new(new_row.clone());
                    match heap_file.update(*rid, &new_row_obj) {
                        Ok(_) => CommandResponse::update(1),
                        Err(e) => CommandResponse::error(format!("update failed: {}", e)),
                    }
                }
                Command::Delete { table_id, rid } => {
                    // Get table metadata
                    let catalog_lock = catalog.blocking_read();
                    let table_meta = match catalog_lock.table_by_id(*table_id) {
                        Ok(t) => t,
                        Err(e) => {
                            return CommandResponse::error(format!("table lookup failed: {}", e))
                        }
                    };

                    // Open heap file and delete
                    let file_path = data_dir.join(format!("{}.heap", table_meta.name));
                    let mut heap_file = match storage::HeapFile::open(&file_path, table_id.0) {
                        Ok(h) => h,
                        Err(e) => {
                            return CommandResponse::error(format!(
                                "failed to open heap file: {}",
                                e
                            ))
                        }
                    };

                    match heap_file.delete(*rid) {
                        Ok(_) => CommandResponse::delete(1),
                        Err(e) => CommandResponse::error(format!("delete failed: {}", e)),
                    }
                }
                Command::CreateTable { .. }
                | Command::DropTable { .. }
                | Command::CreateIndex { .. }
                | Command::DropIndex { .. } => {
                    // DDL operations are handled separately
                    CommandResponse::Ddl
                }
            })
        })
    }
}

/// Map parser SQL type string to internal SqlType.
fn map_sql_type(raw: &str) -> Result<types::SqlType> {
    match raw.trim().to_uppercase().as_str() {
        "INT" | "INTEGER" => Ok(types::SqlType::Int),
        "TEXT" | "STRING" | "VARCHAR" => Ok(types::SqlType::Text),
        "BOOL" | "BOOLEAN" => Ok(types::SqlType::Bool),
        other => Err(anyhow::anyhow!("unsupported SQL type '{}'", other)),
    }
}

/// Infer the output schema from a physical plan.
fn infer_schema(plan: &PhysicalPlan) -> Vec<String> {
    match plan {
        PhysicalPlan::SeqScan { schema, .. } => schema.clone(),
        PhysicalPlan::IndexScan { schema, .. } => schema.clone(),
        PhysicalPlan::Filter { input, .. } => infer_schema(input),
        PhysicalPlan::Project { columns, .. } => {
            columns.iter().map(|(name, _)| name.clone()).collect()
        }
        PhysicalPlan::Sort { input, .. } => infer_schema(input),
        PhysicalPlan::Limit { input, .. } => infer_schema(input),
        PhysicalPlan::Insert { .. } | PhysicalPlan::Update { .. } | PhysicalPlan::Delete { .. } => {
            vec![]
        }
    }
}

/// Evaluate a literal expression from the parser.
///
/// This handles the AST Expr type from the parser and converts it to a Value.
fn eval_literal_expr(e: &expr::Expr) -> Result<Value> {
    match e {
        expr::Expr::Literal(val) => Ok(val.clone()),
        expr::Expr::Unary { op, expr: inner } => {
            let inner_val = eval_literal_expr(inner)?;
            match (op, inner_val) {
                (expr::UnaryOp::Not, Value::Bool(b)) => Ok(Value::Bool(!b)),
                (expr::UnaryOp::Not, _) => Err(anyhow::anyhow!("NOT requires boolean operand")),
            }
        }
        _ => Err(anyhow::anyhow!(
            "only literal expressions supported in Raft mode, got {:?}",
            e
        )),
    }
}

/// Check if a statement is a DML (INSERT/UPDATE/DELETE) operation.
fn is_dml_statement(stmt: &Statement) -> bool {
    matches!(
        stmt,
        Statement::Insert { .. } | Statement::Update { .. } | Statement::Delete { .. }
    )
}

/// Resolve a parser expression to a planner ResolvedExpr for use in scans.
///
/// This converts column names to column IDs based on the schema.
fn resolve_expr_for_scan(expr: &expr::Expr, schema: &[String]) -> Result<ResolvedExpr> {
    match expr {
        expr::Expr::Literal(val) => Ok(ResolvedExpr::Literal(val.clone())),
        expr::Expr::Column(name) => {
            let col_idx = schema
                .iter()
                .position(|n| n == name)
                .ok_or_else(|| anyhow::anyhow!("column '{}' not found in schema", name))?;
            Ok(ResolvedExpr::Column(col_idx as u16))
        }
        expr::Expr::Unary { op, expr: inner } => {
            let resolved_inner = resolve_expr_for_scan(inner, schema)?;
            Ok(ResolvedExpr::Unary {
                op: *op,
                expr: Box::new(resolved_inner),
            })
        }
        expr::Expr::Binary { left, op, right } => {
            let resolved_left = resolve_expr_for_scan(left, schema)?;
            let resolved_right = resolve_expr_for_scan(right, schema)?;
            Ok(ResolvedExpr::Binary {
                left: Box::new(resolved_left),
                op: *op,
                right: Box::new(resolved_right),
            })
        }
    }
}
