use expr::Expr;

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
