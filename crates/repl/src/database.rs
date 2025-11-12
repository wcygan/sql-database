use anyhow::{Context, Result};
use buffer::FilePager;
use catalog::Catalog;
use common::DbResult;
use executor::ExecutionContext;
use std::{
    fs,
    path::{Path, PathBuf},
};
use wal::{Wal, WalRecord};

/// Database state management for the REPL.
///
/// Encapsulates the catalog, pager, and WAL for a database instance.
#[derive(Debug)]
pub struct DatabaseState {
    pub data_dir: PathBuf,
    pub catalog_path: PathBuf,
    #[allow(dead_code)]
    pub wal_path: PathBuf,
    #[allow(dead_code)]
    pub buffer_pages: usize,
    pub catalog: Catalog,
    pub pager: FilePager,
    pub wal: Wal,
}

impl DatabaseState {
    /// Create a new database state.
    ///
    /// Creates the data directory if it doesn't exist, loads the catalog,
    /// initializes the pager, and opens the WAL.
    pub fn new(
        data_dir: &Path,
        catalog_file: &str,
        wal_file: &str,
        buffer_pages: usize,
    ) -> Result<Self> {
        fs::create_dir_all(data_dir)
            .with_context(|| format!("failed to create data directory {}", data_dir.display()))?;

        let catalog_path = data_dir.join(catalog_file);
        let wal_path = data_dir.join(wal_file);
        let catalog = Catalog::load(&catalog_path).map_err(anyhow::Error::from)?;
        let pager = FilePager::new(data_dir, buffer_pages);
        let wal = Wal::open(&wal_path).map_err(anyhow::Error::from)?;

        Ok(Self {
            data_dir: data_dir.to_path_buf(),
            catalog_path,
            wal_path,
            buffer_pages,
            catalog,
            pager,
            wal,
        })
    }

    /// Persist the catalog to disk.
    pub fn persist_catalog(&self) -> Result<()> {
        self.catalog
            .save(&self.catalog_path)
            .map_err(anyhow::Error::from)
    }

    /// Append a record to the WAL and sync.
    pub fn log_wal(&mut self, record: WalRecord) -> Result<()> {
        self.wal
            .append(&record)
            .and_then(|_| self.wal.sync())
            .map_err(anyhow::Error::from)
    }

    /// Remove the heap file for a table.
    pub fn remove_heap_file(&self, table_name: &str) -> Result<()> {
        let path = self.data_dir.join(format!("{table_name}.heap"));
        if path.exists() {
            fs::remove_file(&path)
                .with_context(|| format!("failed to remove heap file {}", path.display()))?;
        }
        Ok(())
    }

    /// Execute a function with an execution context.
    pub fn with_execution_context<T, F>(&mut self, f: F) -> Result<T>
    where
        F: FnOnce(&mut ExecutionContext<'_>) -> DbResult<T>,
    {
        let mut ctx = ExecutionContext::new(
            &self.catalog,
            &mut self.pager,
            &mut self.wal,
            self.data_dir.clone(),
        );
        f(&mut ctx).map_err(anyhow::Error::from)
    }

    /// Reset the database by removing all data files and reinitializing.
    pub fn reset(&mut self) -> Result<()> {
        // Remove all table files (.tbl) and heap files (.heap)
        let entries = fs::read_dir(&self.data_dir).with_context(|| {
            format!("failed to read data directory {}", self.data_dir.display())
        })?;

        for entry in entries.flatten() {
            let path = entry.path();
            if let Some(ext) = path.extension()
                && (ext == "heap" || ext == "tbl")
            {
                fs::remove_file(&path)
                    .with_context(|| format!("failed to remove file {}", path.display()))?;
            }
        }

        // Remove catalog file if it exists
        if self.catalog_path.exists() {
            fs::remove_file(&self.catalog_path).with_context(|| {
                format!("failed to remove catalog {}", self.catalog_path.display())
            })?;
        }

        // Remove WAL file (close it first by replacing with a temp instance)
        drop(std::mem::replace(&mut self.wal, Wal::open(&self.wal_path)?));
        if self.wal_path.exists() {
            fs::remove_file(&self.wal_path)
                .with_context(|| format!("failed to remove WAL {}", self.wal_path.display()))?;
        }

        // Reinitialize catalog
        self.catalog = Catalog::load(&self.catalog_path).map_err(anyhow::Error::from)?;

        // Reinitialize pager (clear buffer pool)
        self.pager = FilePager::new(&self.data_dir, self.buffer_pages);

        // Reinitialize WAL
        self.wal = Wal::open(&self.wal_path).map_err(anyhow::Error::from)?;

        Ok(())
    }
}
