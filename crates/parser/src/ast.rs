use expr::Expr;

/// Index type (algorithm) for CREATE INDEX.
#[derive(Clone, Debug, PartialEq, Default)]
pub enum IndexType {
    #[default]
    BTree,
    Hash,
}

/// Sort direction for ORDER BY clauses.
#[derive(Clone, Debug, PartialEq)]
pub enum SortDirection {
    Asc,
    Desc,
}

/// ORDER BY expression specifying column and sort direction.
#[derive(Clone, Debug, PartialEq)]
pub struct OrderByExpr {
    pub column: String,
    pub direction: SortDirection,
}

/// Join type for multi-table queries.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum JoinType {
    Inner,
}

/// Table reference with optional alias.
///
/// Examples:
/// - `TableRef { name: "users", alias: None }` - `users`
/// - `TableRef { name: "users", alias: Some("u") }` - `users u` or `users AS u`
#[derive(Clone, Debug, PartialEq)]
pub struct TableRef {
    /// Table name.
    pub name: String,
    /// Optional alias (e.g., `u` in `users u`).
    pub alias: Option<String>,
}

impl TableRef {
    /// Returns the alias if present, otherwise the table name.
    /// This is used for schema prefixing in joins.
    pub fn effective_name(&self) -> &str {
        self.alias.as_deref().unwrap_or(&self.name)
    }
}

/// Join clause for multi-table queries.
///
/// Represents `JOIN table ON condition`.
#[derive(Clone, Debug, PartialEq)]
pub struct JoinClause {
    /// Type of join (INNER, LEFT, etc.)
    pub join_type: JoinType,
    /// Right-hand table reference.
    pub table: TableRef,
    /// Join condition (ON clause).
    pub condition: Expr,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Statement {
    CreateTable {
        name: String,
        columns: Vec<ColumnDef>,
        primary_key: Option<Vec<String>>,
    },
    DropTable {
        name: String,
    },
    CreateIndex {
        name: String,
        table: String,
        column: String,
        index_type: IndexType,
    },
    DropIndex {
        name: String,
    },
    Insert {
        table: String,
        values: Vec<Expr>,
    },
    Select {
        columns: Vec<SelectItem>,
        /// Primary FROM table with optional alias.
        from: TableRef,
        /// JOIN clauses (may be empty for single-table queries).
        joins: Vec<JoinClause>,
        selection: Option<Expr>,
        order_by: Vec<OrderByExpr>,
        limit: Option<u64>,
        offset: Option<u64>,
    },
    Update {
        table: String,
        assignments: Vec<(String, Expr)>,
        selection: Option<Expr>,
    },
    Delete {
        table: String,
        selection: Option<Expr>,
    },
    Explain {
        query: Box<Statement>,
        analyze: bool,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub struct ColumnDef {
    pub name: String,
    pub ty: String,
}

#[derive(Clone, Debug, PartialEq)]
pub enum SelectItem {
    Wildcard,
    Column(String),
}
