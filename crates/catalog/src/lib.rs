use std::{fs, path::Path};

use ahash::RandomState;
use common::{ColumnId, DbError, DbResult, TableId};
use hashbrown::HashMap;
use serde::{Deserialize, Serialize};
use types::SqlType;
use uuid::Uuid;

type Map<K, V> = HashMap<K, V, RandomState>;

/// Unique identifier for an index definition stored in the catalog.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct IndexId(pub u64);

/// Persistent catalog that stores table schemas and index metadata.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Catalog {
    tables: Vec<TableMeta>,
    next_table_id: u64,
    next_index_id: u64,
    #[serde(skip)]
    #[serde(default)]
    table_name_index: Map<String, usize>,
    #[serde(skip)]
    #[serde(default)]
    table_id_index: Map<TableId, usize>,
}

impl Catalog {
    /// Create an empty catalog.
    pub fn new() -> Self {
        let mut catalog = Self {
            tables: Vec::new(),
            next_table_id: 1,
            next_index_id: 1,
            table_name_index: Map::default(),
            table_id_index: Map::default(),
        };
        catalog.rebuild_indexes();
        catalog
    }

    /// Load a catalog from disk, returning an empty catalog if the file does not exist.
    pub fn load(path: &Path) -> DbResult<Self> {
        if !path.exists() {
            return Ok(Self::new());
        }
        let data = fs::read_to_string(path)?;
        let mut catalog: Catalog = serde_json::from_str(&data)
            .map_err(|err| DbError::Catalog(format!("invalid catalog file: {err}")))?;
        catalog.rebuild_indexes();
        Ok(catalog)
    }

    /// Persist the catalog contents as pretty JSON.
    pub fn save(&self, path: &Path) -> DbResult<()> {
        let data = serde_json::to_string_pretty(self)
            .map_err(|err| DbError::Catalog(format!("serialize failed: {err}")))?;
        fs::write(path, data)?;
        Ok(())
    }

    /// Returns an immutable reference to a table by name.
    pub fn table(&self, name: &str) -> DbResult<&TableMeta> {
        let idx = self
            .table_name_index
            .get(name)
            .copied()
            .ok_or_else(|| DbError::Catalog(format!("unknown table '{name}'")))?;
        self.tables
            .get(idx)
            .ok_or_else(|| DbError::Catalog(format!("unknown table '{name}'")))
    }

    /// Returns an immutable reference to a table by identifier.
    pub fn table_by_id(&self, id: TableId) -> DbResult<&TableMeta> {
        let idx = self
            .table_id_index
            .get(&id)
            .copied()
            .ok_or_else(|| DbError::Catalog(format!("unknown table id {}", id.0)))?;
        self.tables
            .get(idx)
            .ok_or_else(|| DbError::Catalog(format!("unknown table id {}", id.0)))
    }

    /// Create a new table with the provided columns, returning its identifier.
    pub fn create_table(&mut self, name: &str, columns: Vec<Column>) -> DbResult<TableId> {
        if self.table_name_index.contains_key(name) {
            return Err(DbError::Catalog(format!("table '{name}' already exists")));
        }
        let schema = TableSchema::try_new(columns)?;
        let table_id = TableId(self.next_table_id);
        self.next_table_id += 1;
        let table = TableMeta::new(table_id, name.to_string(), schema);
        self.tables.push(table);
        self.rebuild_indexes();
        Ok(table_id)
    }

    /// Remove a table and its associated indexes.
    pub fn drop_table(&mut self, name: &str) -> DbResult<()> {
        let idx = self
            .table_name_index
            .get(name)
            .copied()
            .ok_or_else(|| DbError::Catalog(format!("unknown table '{name}'")))?;
        self.tables.remove(idx);
        self.rebuild_indexes();
        Ok(())
    }

    /// Create an index over the given table columns, returning its identifier.
    pub fn create_index(
        &mut self,
        table_name: &str,
        index_name: &str,
        columns: &[&str],
        kind: IndexKind,
    ) -> DbResult<IndexId> {
        if columns.is_empty() {
            return Err(DbError::Catalog(
                "index must reference at least one column".into(),
            ));
        }
        let resolved = {
            let table = self.table(table_name)?;
            let mut resolved = Vec::with_capacity(columns.len());
            for name in columns {
                let ordinal = table.schema.column_index(name).ok_or_else(|| {
                    DbError::Catalog(format!("unknown column '{name}' on table '{table_name}'"))
                })?;
                resolved.push(ordinal);
            }
            resolved
        };
        let index_id = IndexId(self.next_index_id);
        self.next_index_id += 1;
        let table = self.table_mut(table_name)?;
        table.add_index(IndexMeta {
            id: index_id,
            name: index_name.to_string(),
            columns: resolved,
            kind,
            storage: StorageDescriptor::new(),
        })?;
        Ok(index_id)
    }

    /// Drop an index attached to a table.
    pub fn drop_index(&mut self, table_name: &str, index_name: &str) -> DbResult<()> {
        let table = self.table_mut(table_name)?;
        table.remove_index(index_name)
    }

    /// Immutable iterator over all tables.
    pub fn tables(&self) -> impl Iterator<Item = &TableMeta> {
        self.tables.iter()
    }

    pub fn table_mut(&mut self, name: &str) -> DbResult<&mut TableMeta> {
        let id = self
            .table_name_index
            .get(name)
            .copied()
            .ok_or_else(|| DbError::Catalog(format!("unknown table '{name}'")))?;
        self.tables
            .get_mut(id)
            .ok_or_else(|| DbError::Catalog(format!("unknown table '{name}'")))
    }

    fn rebuild_indexes(&mut self) {
        self.table_name_index.clear();
        self.table_id_index.clear();
        for (idx, table) in self.tables.iter_mut().enumerate() {
            self.table_name_index.insert(table.name.clone(), idx);
            self.table_id_index.insert(table.id, idx);
            table.rebuild_index_lookup();
        }
    }
}

impl Default for Catalog {
    fn default() -> Self {
        Self::new()
    }
}

/// Metadata describing a registered table.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TableMeta {
    pub id: TableId,
    pub name: String,
    pub schema: TableSchema,
    pub storage: StorageDescriptor,
    pub indexes: Vec<IndexMeta>,
    #[serde(skip)]
    #[serde(default)]
    index_name_lookup: Map<String, usize>,
    #[serde(skip)]
    #[serde(default)]
    index_id_lookup: Map<IndexId, usize>,
}

impl TableMeta {
    fn new(id: TableId, name: String, schema: TableSchema) -> Self {
        let mut table = Self {
            id,
            name,
            schema,
            storage: StorageDescriptor::new(),
            indexes: Vec::new(),
            index_name_lookup: Map::default(),
            index_id_lookup: Map::default(),
        };
        table.rebuild_index_lookup();
        table
    }

    fn add_index(&mut self, index: IndexMeta) -> DbResult<()> {
        if self.index_name_lookup.contains_key(&index.name) {
            return Err(DbError::Catalog(format!(
                "index '{}' already exists on table '{}'",
                index.name, self.name
            )));
        }
        self.indexes.push(index);
        self.rebuild_index_lookup();
        Ok(())
    }

    fn remove_index(&mut self, index_name: &str) -> DbResult<()> {
        let idx = self
            .index_name_lookup
            .get(index_name)
            .copied()
            .ok_or_else(|| {
                DbError::Catalog(format!(
                    "index '{index_name}' does not exist on table '{}'",
                    self.name
                ))
            })?;
        self.indexes.remove(idx);
        self.rebuild_index_lookup();
        Ok(())
    }

    /// Lookup an index by name.
    pub fn index(&self, name: &str) -> DbResult<&IndexMeta> {
        let idx = self.index_name_lookup.get(name).copied().ok_or_else(|| {
            DbError::Catalog(format!(
                "index '{name}' does not exist on table '{}'",
                self.name
            ))
        })?;
        self.indexes
            .get(idx)
            .ok_or_else(|| DbError::Catalog(format!("index '{name}' missing on '{}'", self.name)))
    }

    /// Lookup an index by identifier.
    pub fn index_by_id(&self, id: IndexId) -> DbResult<&IndexMeta> {
        let idx = self.index_id_lookup.get(&id).copied().ok_or_else(|| {
            DbError::Catalog(format!("unknown index id {} on '{}'", id.0, self.name))
        })?;
        self.indexes.get(idx).ok_or_else(|| {
            DbError::Catalog(format!(
                "index id {} missing in table '{}'",
                id.0, self.name
            ))
        })
    }

    /// Returns true if an index with the provided name exists.
    pub fn has_index(&self, index_name: &str) -> bool {
        self.index_name_lookup.contains_key(index_name)
    }

    /// Returns all indexes defined on this table.
    pub fn indexes(&self) -> &[IndexMeta] {
        &self.indexes
    }

    fn rebuild_index_lookup(&mut self) {
        self.index_name_lookup.clear();
        self.index_id_lookup.clear();
        for (idx, index) in self.indexes.iter().enumerate() {
            self.index_name_lookup.insert(index.name.clone(), idx);
            self.index_id_lookup.insert(index.id, idx);
        }
    }
}

/// Column layout for a table, along with helpful lookup structures.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct TableSchema {
    pub columns: Vec<Column>,
    pub name_to_ordinal: Map<String, ColumnId>,
}

impl TableSchema {
    pub fn try_new(columns: Vec<Column>) -> DbResult<Self> {
        if columns.is_empty() {
            return Err(DbError::Catalog(
                "table must contain at least one column".into(),
            ));
        }
        if columns.len() > u16::MAX as usize {
            return Err(DbError::Catalog(
                "too many columns for a single table".into(),
            ));
        }
        let mut name_to_ordinal = Map::default();
        for (idx, column) in columns.iter().enumerate() {
            let ordinal = idx as ColumnId;
            if name_to_ordinal
                .insert(column.name.clone(), ordinal)
                .is_some()
            {
                return Err(DbError::Catalog(format!(
                    "duplicate column '{}' found while building schema",
                    column.name
                )));
            }
        }
        Ok(Self {
            columns,
            name_to_ordinal,
        })
    }

    /// Returns the ordinal for a column name.
    pub fn column_index(&self, name: &str) -> Option<ColumnId> {
        self.name_to_ordinal.get(name).copied()
    }

    /// Returns the SQL type for the provided ordinal.
    pub fn column_type(&self, ordinal: ColumnId) -> Option<&SqlType> {
        self.columns.get(ordinal as usize).map(|c| &c.ty)
    }
}

/// Describes a logical column within a table schema.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Column {
    pub name: String,
    pub ty: SqlType,
}

impl Column {
    pub fn new(name: impl Into<String>, ty: SqlType) -> Self {
        Self {
            name: name.into(),
            ty,
        }
    }
}

/// Metadata describing a table index.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct IndexMeta {
    pub id: IndexId,
    pub name: String,
    pub columns: Vec<ColumnId>,
    pub kind: IndexKind,
    pub storage: StorageDescriptor,
}

/// Supported index implementations.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum IndexKind {
    BTree,
    Hash,
    Bitmap,
    Trie,
}

/// Links catalog entries to physical storage artifacts, such as heap files.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct StorageDescriptor {
    pub file_id: Uuid,
}

impl StorageDescriptor {
    pub fn new() -> Self {
        Self {
            file_id: Uuid::new_v4(),
        }
    }
}

impl Default for StorageDescriptor {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn sample_columns() -> Vec<Column> {
        vec![
            Column::new("id", SqlType::Int),
            Column::new("name", SqlType::Text),
            Column::new("age", SqlType::Int),
        ]
    }

    #[test]
    fn create_and_lookup_table() {
        let mut catalog = Catalog::new();
        let table_id = catalog.create_table("users", sample_columns()).unwrap();

        assert_eq!(table_id, TableId(1));

        let table = catalog.table("users").unwrap();
        assert_eq!(table.schema.column_index("name"), Some(1));
        assert_eq!(table.schema.column_type(0), Some(&SqlType::Int));

        let same_table = catalog.table_by_id(table_id).unwrap();
        assert_eq!(same_table.name, "users");
    }

    #[test]
    fn rejects_duplicate_tables() {
        let mut catalog = Catalog::new();
        catalog.create_table("users", sample_columns()).unwrap();
        let err = catalog.create_table("users", sample_columns()).unwrap_err();

        assert!(matches!(err, DbError::Catalog(_)));
        assert!(format!("{err}").contains("already exists"));
    }

    #[test]
    fn rejects_duplicate_columns() {
        let mut catalog = Catalog::new();
        let err = catalog
            .create_table(
                "bad",
                vec![
                    Column::new("id", SqlType::Int),
                    Column::new("id", SqlType::Int),
                ],
            )
            .unwrap_err();
        assert!(format!("{err}").contains("duplicate column"));
    }

    #[test]
    fn create_and_drop_index() {
        let mut catalog = Catalog::new();
        catalog.create_table("users", sample_columns()).unwrap();

        let index_id = catalog
            .create_index("users", "idx_users_name", &["name"], IndexKind::BTree)
            .unwrap();
        assert_eq!(index_id, IndexId(1));

        let table = catalog.table("users").unwrap();
        assert!(table.has_index("idx_users_name"));
        assert_eq!(table.index("idx_users_name").unwrap().columns, vec![1u16]);

        catalog
            .drop_index("users", "idx_users_name")
            .expect("index drop succeeds");
        assert!(!catalog.table("users").unwrap().has_index("idx_users_name"));
    }

    #[test]
    fn index_creation_validates_columns() {
        let mut catalog = Catalog::new();
        catalog.create_table("users", sample_columns()).unwrap();

        let err = catalog
            .create_index("users", "idx_missing", &["missing"], IndexKind::Hash)
            .unwrap_err();
        assert!(format!("{err}").contains("unknown column"));
    }

    #[test]
    fn persistence_round_trip() {
        let mut catalog = Catalog::new();
        catalog.create_table("users", sample_columns()).unwrap();
        catalog
            .create_index("users", "idx_users_name", &["name"], IndexKind::Hash)
            .unwrap();

        let dir = tempdir().unwrap();
        let path = dir.path().join("catalog.json");
        catalog.save(&path).unwrap();

        let loaded = Catalog::load(&path).unwrap();
        let table = loaded.table("users").unwrap();
        assert!(table.has_index("idx_users_name"));
        assert_eq!(table.index("idx_users_name").unwrap().columns, vec![1u16]);
        assert_eq!(loaded.table_by_id(TableId(1)).unwrap().name, "users");
    }

    #[test]
    fn drop_table_removes_metadata() {
        let mut catalog = Catalog::new();
        catalog.create_table("users", sample_columns()).unwrap();
        catalog
            .create_index("users", "idx_users_name", &["name"], IndexKind::Hash)
            .unwrap();

        catalog.drop_table("users").unwrap();
        assert!(catalog.table("users").is_err());
        assert!(catalog.table_by_id(TableId(1)).is_err());

        // Adding a table after drop reuses metadata safely but increments ids.
        let next_id = catalog.create_table("orders", sample_columns()).unwrap();
        assert_eq!(next_id, TableId(2));
    }
}
