use anyhow::{Context, Result};
use buffer::FilePager;
use catalog::{Catalog, Column, IndexKind};
use executor::{build_executor, execute_dml, execute_query, ExecutionContext};
use parser::{parse_sql, Statement};
use planner::{PhysicalPlan, Planner, PlanningContext};
use std::{
    fs,
    path::{Path, PathBuf},
    sync::Arc,
};
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

        Ok(Self {
            data_dir: Arc::new(data_dir.to_path_buf()),
            catalog_path: Arc::new(catalog_path),
            wal_path: Arc::new(wal_path),
            buffer_pages,
            catalog: Arc::new(RwLock::new(catalog)),
            pager: Arc::new(Mutex::new(pager)),
            wal: Arc::new(Mutex::new(wal)),
        })
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
            let mut planning_ctx = PlanningContext::new(&*catalog_lock);
            let plan = Planner::plan(query, &mut planning_ctx).map_err(anyhow::Error::from)?;

            if analyze {
                // EXPLAIN ANALYZE: Execute the query and collect statistics
                let plan_description = planner::explain_physical(&plan);

                let mut pager_lock = pager.blocking_lock();
                let mut wal_lock = wal.blocking_lock();
                let mut ctx = ExecutionContext::new(
                    &*catalog_lock,
                    &mut *pager_lock,
                    &mut *wal_lock,
                    (*data_dir).clone(),
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
        let catalog = self.catalog.clone();
        let pager = self.pager.clone();
        let wal = self.wal.clone();
        let data_dir = self.data_dir.clone();

        tokio::task::spawn_blocking(move || {
            // Acquire read lock on catalog (shared access for queries/DML)
            let catalog_lock = catalog.blocking_read();
            let mut planning_ctx = PlanningContext::new(&*catalog_lock);
            let plan = Planner::plan(stmt, &mut planning_ctx).map_err(anyhow::Error::from)?;

            // Acquire exclusive locks on pager and WAL
            let mut pager_lock = pager.blocking_lock();
            let mut wal_lock = wal.blocking_lock();
            let mut ctx = ExecutionContext::new(
                &*catalog_lock,
                &mut *pager_lock,
                &mut *wal_lock,
                (*data_dir).clone(),
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
                *catalog_lock = Catalog::load(&**catalog_path).map_err(anyhow::Error::from)?;
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
