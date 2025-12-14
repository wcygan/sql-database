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
        table: String,
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
