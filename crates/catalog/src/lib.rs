use std::{fs, path::Path};

use ahash::RandomState;
use common::{ColumnId, DbError, DbResult, TableId};
use hashbrown::{HashMap, HashSet};
use serde::{Deserialize, Serialize};
use types::SqlType;
use uuid::Uuid;

type Map<K, V> = HashMap<K, V, RandomState>;
type Set<T> = HashSet<T, RandomState>;

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
    #[serde(skip)]
    #[serde(default)]
    index_name_index: Map<String, TableId>,
}

const RESERVED_TABLE_NAMES: &[&str] = &["_catalog", "sqlite_master"];
const RESERVED_INDEX_NAMES: &[&str] = &["_primary"];

#[bon::bon]
impl Catalog {
    /// Create an empty catalog.
    pub fn new() -> Self {
        let mut catalog = Self {
            tables: Vec::new(),
            next_table_id: 1,
            next_index_id: 1,
            table_name_index: Map::default(),
            table_id_index: Map::default(),
            index_name_index: Map::default(),
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
    pub fn create_table(
        &mut self,
        name: &str,
        columns: Vec<Column>,
        primary_key: Option<Vec<ColumnId>>,
    ) -> DbResult<TableId> {
        Self::validate_table_name(name)?;
        if self.table_name_index.contains_key(name) {
            return Err(DbError::Catalog(format!("table '{name}' already exists")));
        }
        let schema = TableSchema::try_new(columns)?;
        let table_id = TableId(self.next_table_id);
        self.next_table_id += 1;
        let mut table = TableMeta::new(table_id, name.to_string(), schema);

        if let Some(pk_columns) = primary_key {
            table.set_primary_key(pk_columns)?;
        }

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
    ///
    /// # Example
    /// ```ignore
    /// catalog.create_index()
    ///     .table_name("users")
    ///     .index_name("idx_users_email")
    ///     .columns(&["email"])
    ///     .kind(IndexKind::BTree)
    ///     .call()?;
    /// ```
    #[builder]
    pub fn create_index(
        &mut self,
        table_name: &str,
        index_name: &str,
        columns: &[&str],
        kind: IndexKind,
    ) -> DbResult<IndexId> {
        Self::validate_index_name(index_name)?;
        if self.index_name_index.contains_key(index_name) {
            return Err(DbError::Catalog(format!(
                "index '{index_name}' already exists in catalog"
            )));
        }
        if columns.is_empty() {
            return Err(DbError::Catalog(
                "index must reference at least one column".into(),
            ));
        }
        let resolved = {
            let table = self.table(table_name)?;
            let mut resolved = Vec::with_capacity(columns.len());
            let mut seen = Set::default();
            for name in columns {
                let ordinal = table.schema.column_index(name).ok_or_else(|| {
                    DbError::Catalog(format!("unknown column '{name}' on table '{table_name}'"))
                })?;
                if !seen.insert(ordinal) {
                    return Err(DbError::Catalog(format!(
                        "index '{index_name}' references column '{name}' multiple times"
                    )));
                }
                let col_ty = table
                    .schema
                    .column_type(ordinal)
                    .ok_or_else(|| DbError::Catalog("column ordinal out of range".into()))?;
                if !kind.supports_type(col_ty) {
                    return Err(DbError::Catalog(format!(
                        "{kind:?} indexes cannot be built on column '{name}' of type {:?}",
                        col_ty
                    )));
                }
                resolved.push(ordinal);
            }
            resolved
        };
        let index_id = IndexId(self.next_index_id);
        self.next_index_id += 1;
        let table_id = {
            let table = self.table_mut(table_name)?;
            let id = table.id;
            table.add_index(IndexMeta {
                id: index_id,
                name: index_name.to_string(),
                columns: resolved,
                kind,
                storage: StorageDescriptor::new(),
            })?;
            id
        };
        self.index_name_index
            .insert(index_name.to_string(), table_id);
        Ok(index_id)
    }

    /// Drop an index attached to a table.
    pub fn drop_index(&mut self, table_name: &str, index_name: &str) -> DbResult<()> {
        {
            let table = self.table_mut(table_name)?;
            table.remove_index(index_name)?;
        }
        self.index_name_index.remove(index_name);
        Ok(())
    }

    /// Immutable iterator over all tables.
    pub fn tables(&self) -> impl Iterator<Item = &TableMeta> {
        self.tables.iter()
    }

    /// Returns a vector of table names, useful for inspection.
    pub fn table_names(&self) -> Vec<&str> {
        self.tables.iter().map(|t| t.name.as_str()).collect()
    }

    /// Returns lightweight summaries for all registered tables.
    pub fn table_summaries(&self) -> Vec<TableSummary> {
        self.tables
            .iter()
            .map(|t| TableSummary {
                id: t.id,
                name: t.name.clone(),
                column_count: t.schema.columns.len() as u16,
                index_count: t.indexes.len() as u16,
            })
            .collect()
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
        self.index_name_index.clear();
        for (idx, table) in self.tables.iter_mut().enumerate() {
            self.table_name_index.insert(table.name.clone(), idx);
            self.table_id_index.insert(table.id, idx);
            table.rebuild_index_lookup();
            for index in &table.indexes {
                self.index_name_index.insert(index.name.clone(), table.id);
            }
        }
    }

    fn validate_table_name(name: &str) -> DbResult<()> {
        if name.trim().is_empty() {
            return Err(DbError::Catalog("table name cannot be empty".into()));
        }
        if RESERVED_TABLE_NAMES
            .iter()
            .any(|reserved| reserved.eq_ignore_ascii_case(name))
        {
            return Err(DbError::Catalog(format!(
                "table name '{name}' is reserved for internal use"
            )));
        }
        Ok(())
    }

    fn validate_index_name(name: &str) -> DbResult<()> {
        if name.trim().is_empty() {
            return Err(DbError::Catalog("index name cannot be empty".into()));
        }
        if RESERVED_INDEX_NAMES
            .iter()
            .any(|reserved| reserved.eq_ignore_ascii_case(name))
        {
            return Err(DbError::Catalog(format!(
                "index name '{name}' is reserved for internal use"
            )));
        }
        Ok(())
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
    /// Primary key columns (ordinals). None if table has no PRIMARY KEY constraint.
    /// Empty Vec is invalid; use None for no constraint.
    pub primary_key: Option<Vec<ColumnId>>,
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
            primary_key: None,
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

    /// Sets the primary key columns for this table. Validates that:
    /// - All column ordinals exist in the schema
    /// - The list is non-empty
    /// - No duplicate columns
    pub fn set_primary_key(&mut self, columns: Vec<ColumnId>) -> DbResult<()> {
        if columns.is_empty() {
            return Err(DbError::Catalog(
                "primary key must include at least one column".to_string(),
            ));
        }

        let num_columns = self.schema.columns().len();
        for &col_id in &columns {
            if (col_id as usize) >= num_columns {
                return Err(DbError::Catalog(format!(
                    "primary key column ordinal {} out of bounds (table has {} columns)",
                    col_id, num_columns
                )));
            }
        }

        // Check for duplicate columns
        let mut seen = std::collections::HashSet::new();
        for &col_id in &columns {
            if !seen.insert(col_id) {
                return Err(DbError::Catalog(format!(
                    "duplicate column {} in primary key",
                    col_id
                )));
            }
        }

        self.primary_key = Some(columns);
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

    /// Returns read-only access to the column definitions.
    pub fn columns(&self) -> &[Column] {
        self.schema.columns()
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

    /// Returns immutable access to the underlying columns.
    pub fn columns(&self) -> &[Column] {
        self.columns.as_slice()
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

impl IndexKind {
    fn supports_type(&self, ty: &SqlType) -> bool {
        match self {
            IndexKind::BTree => matches!(ty, SqlType::Int | SqlType::Text | SqlType::Bool),
            IndexKind::Hash => matches!(ty, SqlType::Int | SqlType::Text | SqlType::Bool),
            IndexKind::Bitmap => matches!(ty, SqlType::Bool),
            IndexKind::Trie => matches!(ty, SqlType::Text),
        }
    }
}

/// Lightweight description for external inspection calls.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TableSummary {
    pub id: TableId,
    pub name: String,
    pub column_count: u16,
    pub index_count: u16,
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
            Column::new("active", SqlType::Bool),
        ]
    }

    fn sample_table_meta(name: &str) -> TableMeta {
        TableMeta::new(
            TableId(1),
            name.to_string(),
            TableSchema::try_new(sample_columns()).unwrap(),
        )
    }

    fn sample_index_meta(id: u64, name: &str, columns: Vec<ColumnId>) -> IndexMeta {
        IndexMeta {
            id: IndexId(id),
            name: name.to_string(),
            columns,
            kind: IndexKind::BTree,
            storage: StorageDescriptor::new(),
        }
    }

    #[test]
    fn create_and_lookup_table() {
        let mut catalog = Catalog::new();
        let table_id = catalog
            .create_table("users", sample_columns(), None)
            .unwrap();

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
        catalog
            .create_table("users", sample_columns(), None)
            .unwrap();
        let err = catalog
            .create_table("users", sample_columns(), None)
            .unwrap_err();

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
                None,
            )
            .unwrap_err();
        assert!(format!("{err}").contains("duplicate column"));
    }

    #[test]
    fn create_and_drop_index() {
        let mut catalog = Catalog::new();
        catalog
            .create_table("users", sample_columns(), None)
            .unwrap();

        let index_id = catalog
            .create_index()
            .table_name("users")
            .index_name("idx_users_name")
            .columns(&["name"])
            .kind(IndexKind::BTree)
            .call()
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
        catalog
            .create_table("users", sample_columns(), None)
            .unwrap();

        let err = catalog
            .create_index()
            .table_name("users")
            .index_name("idx_missing")
            .columns(&["missing"])
            .kind(IndexKind::Hash)
            .call()
            .unwrap_err();
        assert!(format!("{err}").contains("unknown column"));
    }

    #[test]
    fn persistence_round_trip() {
        let mut catalog = Catalog::new();
        catalog
            .create_table("users", sample_columns(), None)
            .unwrap();
        catalog
            .create_index()
            .table_name("users")
            .index_name("idx_users_name")
            .columns(&["name"])
            .kind(IndexKind::Hash)
            .call()
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
        catalog
            .create_table("users", sample_columns(), None)
            .unwrap();
        catalog
            .create_index()
            .table_name("users")
            .index_name("idx_users_name")
            .columns(&["name"])
            .kind(IndexKind::Hash)
            .call()
            .unwrap();

        catalog.drop_table("users").unwrap();
        assert!(catalog.table("users").is_err());
        assert!(catalog.table_by_id(TableId(1)).is_err());

        // Adding a table after drop reuses metadata safely but increments ids.
        let next_id = catalog
            .create_table("orders", sample_columns(), None)
            .unwrap();
        assert_eq!(next_id, TableId(2));
    }

    #[test]
    fn reserved_table_names_rejected() {
        let mut catalog = Catalog::new();
        let err = catalog
            .create_table("_catalog", sample_columns(), None)
            .expect_err("reserved name rejected");
        assert!(format!("{err}").contains("reserved"));
    }

    #[test]
    fn reserved_index_names_rejected() {
        let mut catalog = Catalog::new();
        catalog
            .create_table("users", sample_columns(), None)
            .unwrap();
        let err = catalog
            .create_index()
            .table_name("users")
            .index_name("_primary")
            .columns(&["id"])
            .kind(IndexKind::BTree)
            .call()
            .expect_err("reserved index name rejected");
        assert!(format!("{err}").contains("reserved"));
    }

    #[test]
    fn load_returns_empty_catalog_for_missing_path() {
        let dir = tempdir().unwrap();
        let missing_path = dir.path().join("catalog.json");
        let mut catalog =
            Catalog::load(&missing_path).expect("missing catalog file yields empty catalog");
        assert!(catalog.table_names().is_empty());

        // Newly loaded catalog should behave like a fresh catalog instance.
        let new_table_id = catalog
            .create_table("users", sample_columns(), None)
            .expect("table creation works");
        assert_eq!(new_table_id, TableId(1));
    }

    #[test]
    fn index_creation_rejects_empty_column_list() {
        let mut catalog = Catalog::new();
        catalog
            .create_table("users", sample_columns(), None)
            .unwrap();

        let err = catalog
            .create_index()
            .table_name("users")
            .index_name("idx_empty")
            .columns(&[])
            .kind(IndexKind::Hash)
            .call()
            .unwrap_err();
        assert_eq!(
            format!("{err}"),
            "catalog: index must reference at least one column"
        );
    }

    #[test]
    fn index_names_unique_across_tables() {
        let mut catalog = Catalog::new();
        catalog
            .create_table("users", sample_columns(), None)
            .unwrap();
        catalog
            .create_table("orders", sample_columns(), None)
            .unwrap();
        catalog
            .create_index()
            .table_name("users")
            .index_name("idx_shared")
            .columns(&["id"])
            .kind(IndexKind::BTree)
            .call()
            .unwrap();
        let err = catalog
            .create_index()
            .table_name("orders")
            .index_name("idx_shared")
            .columns(&["id"])
            .kind(IndexKind::Hash)
            .call()
            .expect_err("duplicate index name rejected");
        assert!(format!("{err}").contains("already exists"));
    }

    #[test]
    fn index_rejects_duplicate_columns() {
        let mut catalog = Catalog::new();
        catalog
            .create_table("users", sample_columns(), None)
            .unwrap();
        let err = catalog
            .create_index()
            .table_name("users")
            .index_name("idx_dup")
            .columns(&["name", "name"])
            .kind(IndexKind::BTree)
            .call()
            .expect_err("duplicate column reference rejected");
        assert!(format!("{err}").contains("multiple times"));
    }

    #[test]
    fn bitmap_index_requires_bool_columns() {
        let mut catalog = Catalog::new();
        catalog
            .create_table("users", sample_columns(), None)
            .unwrap();

        catalog
            .create_index()
            .table_name("users")
            .index_name("idx_active_bitmap")
            .columns(&["active"])
            .kind(IndexKind::Bitmap)
            .call()
            .expect("bool column accepted");

        let err = catalog
            .create_index()
            .table_name("users")
            .index_name("idx_name_bitmap")
            .columns(&["name"])
            .kind(IndexKind::Bitmap)
            .call()
            .expect_err("non-bool rejected");
        assert!(format!("{err}").contains("cannot be built"));
    }

    #[test]
    fn trie_index_requires_text_columns() {
        let mut catalog = Catalog::new();
        catalog
            .create_table("users", sample_columns(), None)
            .unwrap();

        catalog
            .create_index()
            .table_name("users")
            .index_name("idx_name_trie")
            .columns(&["name"])
            .kind(IndexKind::Trie)
            .call()
            .expect("text column accepted");

        let err = catalog
            .create_index()
            .table_name("users")
            .index_name("idx_age_trie")
            .columns(&["age"])
            .kind(IndexKind::Trie)
            .call()
            .expect_err("non-text rejected");
        assert!(format!("{err}").contains("cannot be built"));
    }

    #[test]
    fn table_name_and_summary_helpers() {
        let mut catalog = Catalog::new();
        catalog
            .create_table("users", sample_columns(), None)
            .unwrap();
        catalog
            .create_table("orders", sample_columns(), None)
            .unwrap();
        catalog
            .create_index()
            .table_name("users")
            .index_name("idx_users_name")
            .columns(&["name"])
            .kind(IndexKind::BTree)
            .call()
            .unwrap();
        let names = catalog.table_names();
        assert_eq!(names, vec!["users", "orders"]);

        let summaries = catalog.table_summaries();
        assert_eq!(summaries.len(), 2);
        let users_summary = summaries
            .iter()
            .find(|s| s.name == "users")
            .expect("users summary present");
        assert_eq!(users_summary.index_count, 1);
        assert_eq!(users_summary.column_count, 4);
    }

    #[test]
    fn tables_iterator_and_summaries_reflect_current_state() {
        let mut catalog = Catalog::new();
        catalog
            .create_table("users", sample_columns(), None)
            .unwrap();
        catalog
            .create_table("orders", sample_columns(), None)
            .unwrap();

        let iterated_names: Vec<_> = catalog.tables().map(|t| t.name.clone()).collect();
        assert_eq!(iterated_names, vec!["users", "orders"]);

        let names = catalog.table_names();
        assert_eq!(names, vec!["users", "orders"]);

        let summaries = catalog.table_summaries();
        assert_eq!(summaries.len(), 2);
        assert!(summaries.iter().all(|summary| summary.column_count == 4));
    }

    #[test]
    fn validate_table_name_rejects_empty_and_reserved() {
        let empty_err = Catalog::validate_table_name(" ").unwrap_err();
        assert_eq!(
            format!("{empty_err}"),
            "catalog: table name cannot be empty"
        );

        let reserved_err = Catalog::validate_table_name("SQLite_Master").unwrap_err();
        assert_eq!(
            format!("{reserved_err}"),
            "catalog: table name 'SQLite_Master' is reserved for internal use"
        );
    }

    #[test]
    fn validate_index_name_rejects_empty_and_reserved() {
        let empty_err = Catalog::validate_index_name("").unwrap_err();
        assert_eq!(
            format!("{empty_err}"),
            "catalog: index name cannot be empty"
        );

        let reserved_err = Catalog::validate_index_name("_PRIMARY").unwrap_err();
        assert_eq!(
            format!("{reserved_err}"),
            "catalog: index name '_PRIMARY' is reserved for internal use"
        );
    }

    #[test]
    fn table_meta_detects_duplicate_index_definitions() {
        let mut table = sample_table_meta("users");
        let index = sample_index_meta(1, "idx_users_name", vec![0]);
        table.add_index(index.clone()).unwrap();
        let err = table.add_index(index).unwrap_err();
        assert_eq!(
            format!("{err}"),
            "catalog: index 'idx_users_name' already exists on table 'users'"
        );
    }

    #[test]
    fn table_meta_missing_index_operations_fail() {
        let mut table = sample_table_meta("users");

        let remove_err = table.remove_index("missing").unwrap_err();
        assert_eq!(
            format!("{remove_err}"),
            "catalog: index 'missing' does not exist on table 'users'"
        );

        let lookup_err = table.index("missing").unwrap_err();
        assert_eq!(
            format!("{lookup_err}"),
            "catalog: index 'missing' does not exist on table 'users'"
        );

        let id_err = table.index_by_id(IndexId(42)).unwrap_err();
        assert_eq!(
            format!("{id_err}"),
            "catalog: unknown index id 42 on 'users'"
        );
    }

    #[test]
    fn table_meta_accessors_expose_indexes_and_columns() {
        let mut table = sample_table_meta("users");
        let index = sample_index_meta(1, "idx_users_name", vec![0]);
        table.add_index(index).unwrap();

        assert_eq!(table.indexes().len(), 1);
        assert_eq!(table.indexes()[0].name, "idx_users_name");
        assert_eq!(table.columns(), table.schema.columns());
    }

    #[test]
    fn table_schema_requires_non_empty_columns() {
        let err = TableSchema::try_new(vec![]).unwrap_err();
        assert_eq!(
            format!("{err}"),
            "catalog: table must contain at least one column"
        );
    }

    #[test]
    fn table_schema_rejects_excessive_column_counts() {
        let column_count = u16::MAX as usize + 1;
        let columns = vec![Column::new("overflow", SqlType::Int); column_count];
        let err = TableSchema::try_new(columns).unwrap_err();
        assert_eq!(
            format!("{err}"),
            "catalog: too many columns for a single table"
        );
    }

    #[test]
    fn storage_descriptor_default_uses_new() {
        let descriptor = StorageDescriptor::default();
        assert_ne!(descriptor.file_id, Uuid::nil());
    }

    #[test]
    fn create_table_with_single_column_primary_key() {
        let mut catalog = Catalog::new();
        let table_id = catalog
            .create_table("users", sample_columns(), Some(vec![0]))
            .unwrap();

        let table = catalog.table("users").unwrap();
        assert_eq!(table.primary_key, Some(vec![0]));
        assert_eq!(table_id, TableId(1));
    }

    #[test]
    fn create_table_with_composite_primary_key() {
        let mut catalog = Catalog::new();
        let table_id = catalog
            .create_table("users", sample_columns(), Some(vec![0, 1]))
            .unwrap();

        let table = catalog.table("users").unwrap();
        assert_eq!(table.primary_key, Some(vec![0, 1]));
        assert_eq!(table_id, TableId(1));
    }

    #[test]
    fn create_table_with_no_primary_key() {
        let mut catalog = Catalog::new();
        let table_id = catalog
            .create_table("users", sample_columns(), None)
            .unwrap();

        let table = catalog.table("users").unwrap();
        assert_eq!(table.primary_key, None);
        assert_eq!(table_id, TableId(1));
    }

    #[test]
    fn primary_key_rejects_empty_column_list() {
        let mut catalog = Catalog::new();
        let err = catalog
            .create_table("users", sample_columns(), Some(vec![]))
            .unwrap_err();

        assert_eq!(
            format!("{err}"),
            "catalog: primary key must include at least one column"
        );
    }

    #[test]
    fn primary_key_rejects_out_of_bounds_column() {
        let mut catalog = Catalog::new();
        let err = catalog
            .create_table("users", sample_columns(), Some(vec![99]))
            .unwrap_err();

        assert!(format!("{err}").contains("out of bounds"));
        assert!(format!("{err}").contains("99"));
    }

    #[test]
    fn primary_key_rejects_duplicate_columns() {
        let mut catalog = Catalog::new();
        let err = catalog
            .create_table("users", sample_columns(), Some(vec![0, 1, 0]))
            .unwrap_err();

        assert_eq!(
            format!("{err}"),
            "catalog: duplicate column 0 in primary key"
        );
    }

    #[test]
    fn set_primary_key_validates_column_bounds() {
        let mut table = sample_table_meta("users");
        let err = table.set_primary_key(vec![10]).unwrap_err();

        assert!(format!("{err}").contains("out of bounds"));
    }

    #[test]
    fn set_primary_key_validates_no_duplicates() {
        let mut table = sample_table_meta("users");
        let err = table.set_primary_key(vec![0, 0]).unwrap_err();

        assert!(format!("{err}").contains("duplicate column"));
    }

    #[test]
    fn primary_key_persists_through_save_load() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("catalog.json");

        let mut catalog = Catalog::new();
        catalog
            .create_table("users", sample_columns(), Some(vec![0, 1]))
            .unwrap();
        catalog.save(&path).unwrap();

        let loaded = Catalog::load(&path).unwrap();
        let table = loaded.table("users").unwrap();
        assert_eq!(table.primary_key, Some(vec![0, 1]));
    }
}
