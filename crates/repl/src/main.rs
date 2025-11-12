use anyhow::{Context, Result, anyhow};
use buffer::FilePager;
use catalog::{Catalog, Column, IndexKind};
use clap::{Parser, ValueEnum};
use common::{
    RecordBatch, Row,
    pretty::{self, TableStyleKind},
};
use executor::{ExecutionContext, execute_dml, execute_query};
use parser::{ColumnDef, Statement, parse_sql};
use planner::{PhysicalPlan, Planner, PlanningContext};
use rustyline::{DefaultEditor, error::ReadlineError};
use std::{
    fs,
    path::{Path, PathBuf},
};
use tabled::Tabled;
use types::SqlType;
use wal::{Wal, WalRecord};

const DEFAULT_DATA_DIR: &str = "./db_data";
const DEFAULT_CATALOG_FILE: &str = "catalog.json";
const DEFAULT_WAL_FILE: &str = "toydb.wal";
const HISTORY_FILE: &str = ".toydb-repl-history";

#[derive(Parser, Debug)]
#[command(
    name = "toydb-repl",
    about = "Interactive SQL console for the toy database"
)]
struct Args {
    /// Directory containing catalog, WAL, and table files
    #[arg(long, default_value = DEFAULT_DATA_DIR)]
    data_dir: PathBuf,
    /// Catalog filename within the data directory
    #[arg(long, default_value = DEFAULT_CATALOG_FILE)]
    catalog_file: String,
    /// WAL filename within the data directory
    #[arg(long, default_value = DEFAULT_WAL_FILE)]
    wal_file: String,
    /// Maximum number of pages held in the file pager cache
    #[arg(long, default_value_t = 256)]
    buffer_pages: usize,
    /// Table rendering style for query output
    #[arg(long, value_enum, default_value_t = CliTableStyle::Modern)]
    style: CliTableStyle,
    /// Execute the provided SQL and exit instead of starting the REPL
    #[arg(short = 'e', long = "execute")]
    execute: Option<String>,
}

#[derive(Copy, Clone, Debug, ValueEnum)]
enum CliTableStyle {
    Modern,
    Ascii,
    Plain,
}

impl From<CliTableStyle> for TableStyleKind {
    fn from(value: CliTableStyle) -> Self {
        match value {
            CliTableStyle::Modern => TableStyleKind::Modern,
            CliTableStyle::Ascii => TableStyleKind::Ascii,
            CliTableStyle::Plain => TableStyleKind::Plain,
        }
    }
}

struct App {
    state: DatabaseState,
    style: TableStyleKind,
}

impl App {
    fn new(args: Args) -> Result<Self> {
        let style = args.style.into();
        let state = DatabaseState::new(
            &args.data_dir,
            &args.catalog_file,
            &args.wal_file,
            args.buffer_pages,
        )?;
        Ok(Self { state, style })
    }

    fn run(mut self, execute: Option<String>) -> Result<()> {
        if let Some(sql) = execute {
            self.run_sql(&sql)?;
            return Ok(());
        }
        self.run_repl()
    }

    fn run_sql(&mut self, sql: &str) -> Result<()> {
        let statements = parse_sql(sql).map_err(anyhow::Error::from)?;
        for stmt in statements {
            self.execute_statement(stmt)?;
        }
        Ok(())
    }

    fn execute_statement(&mut self, stmt: Statement) -> Result<()> {
        match stmt {
            Statement::CreateTable {
                name,
                columns,
                primary_key,
            } => self.create_table(name, columns, primary_key),
            Statement::DropTable { name } => self.drop_table(name),
            Statement::CreateIndex {
                name,
                table,
                column,
            } => self.create_index(name, table, column),
            Statement::DropIndex { name } => self.drop_index(name),
            other => self.execute_planned(other),
        }
    }

    fn create_table(
        &mut self,
        name: String,
        columns: Vec<ColumnDef>,
        primary_key: Option<Vec<String>>,
    ) -> Result<()> {
        let catalog_columns: Vec<Column> = columns
            .iter()
            .map(|col| {
                let ty = map_sql_type(&col.ty).with_context(|| {
                    format!("unknown type '{}' for column {}", col.ty, col.name)
                })?;
                Ok(Column::new(col.name.clone(), ty))
            })
            .collect::<Result<Vec<_>>>()?;

        // Convert primary key column names to ordinals
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

        let table_id = self
            .state
            .catalog
            .create_table(&name, catalog_columns, primary_key_ordinals)
            .map_err(anyhow::Error::from)?;
        self.state.persist_catalog()?;
        self.state.log_wal(WalRecord::CreateTable {
            name: name.clone(),
            table: table_id,
        })?;
        println!("Created table '{name}' (id = {}).", table_id.0);
        Ok(())
    }

    fn drop_table(&mut self, name: String) -> Result<()> {
        let table_id = self
            .state
            .catalog
            .table(&name)
            .map_err(anyhow::Error::from)?
            .id;
        self.state
            .catalog
            .drop_table(&name)
            .map_err(anyhow::Error::from)?;
        self.state.persist_catalog()?;
        self.state.remove_heap_file(&name)?;
        self.state
            .log_wal(WalRecord::DropTable { table: table_id })?;
        println!("Dropped table '{name}'.");
        Ok(())
    }

    fn create_index(&mut self, name: String, table: String, column: String) -> Result<()> {
        self.state
            .catalog
            .create_index()
            .table_name(&table)
            .index_name(&name)
            .columns(&[column.as_str()])
            .kind(IndexKind::BTree)
            .call()
            .map_err(anyhow::Error::from)?;
        self.state.persist_catalog()?;
        println!("Created index '{name}' on '{table}'.");
        Ok(())
    }

    fn drop_index(&mut self, name: String) -> Result<()> {
        let table_name = self
            .state
            .catalog
            .tables()
            .find(|table| table.index(&name).is_ok())
            .map(|table| table.name.clone())
            .ok_or_else(|| anyhow!("index '{name}' not found"))?;

        self.state
            .catalog
            .drop_index(&table_name, &name)
            .map_err(anyhow::Error::from)?;
        self.state.persist_catalog()?;
        println!("Dropped index '{name}' on '{table_name}'.");
        Ok(())
    }

    fn execute_planned(&mut self, stmt: Statement) -> Result<()> {
        let mut planning_ctx = PlanningContext::new(&self.state.catalog);
        let plan = Planner::plan(stmt, &mut planning_ctx).map_err(anyhow::Error::from)?;

        match plan {
            PhysicalPlan::Insert { .. }
            | PhysicalPlan::Update { .. }
            | PhysicalPlan::Delete { .. } => {
                let count = self
                    .state
                    .with_execution_context(|ctx| execute_dml(plan, ctx))?;
                println!("{count} row(s) affected.");
            }
            other => {
                let schema = infer_schema(&other);
                let rows: Vec<Row> = self
                    .state
                    .with_execution_context(|ctx| execute_query(other, ctx))?;
                let batch = RecordBatch {
                    columns: schema,
                    rows,
                };
                let rendered = pretty::render_record_batch(&batch, self.style);
                println!("{rendered}");
            }
        }

        Ok(())
    }

    fn run_repl(&mut self) -> Result<()> {
        let mut editor = DefaultEditor::new()?;
        let history_path = self.history_path();
        if history_path.exists() {
            let _ = editor.load_history(&history_path);
        }

        println!("Connected. Type SQL statements terminated by ';' or use .help or .examples");

        let mut buffer = String::new();
        loop {
            let prompt = if buffer.is_empty() {
                "toydb> "
            } else {
                "...> "
            };
            match editor.readline(prompt) {
                Ok(line) => {
                    let trimmed = line.trim();

                    if buffer.is_empty() {
                        match self.handle_meta_command(trimmed)? {
                            MetaOutcome::Exit => break,
                            MetaOutcome::Continue => continue,
                            MetaOutcome::NotMeta => {}
                        }
                    }

                    if trimmed.is_empty() {
                        continue;
                    }

                    buffer.push_str(trimmed);
                    buffer.push('\n');

                    if trimmed.ends_with(';') {
                        let statement_block = buffer.trim();
                        if !statement_block.is_empty() {
                            if let Err(err) = self.run_sql(statement_block) {
                                eprintln!("error: {err}");
                            } else {
                                editor.add_history_entry(statement_block).ok();
                            }
                        }
                        buffer.clear();
                    }
                }
                Err(ReadlineError::Interrupted) => {
                    println!("^C");
                    buffer.clear();
                    continue;
                }
                Err(ReadlineError::Eof) => {
                    println!("Goodbye.");
                    break;
                }
                Err(err) => return Err(err.into()),
            }
        }

        if let Some(parent) = history_path.parent() {
            fs::create_dir_all(parent).ok();
        }
        let _ = editor.save_history(&history_path);
        Ok(())
    }

    fn handle_meta_command(&mut self, line: &str) -> Result<MetaOutcome> {
        let command = line.trim();
        match command {
            "" => Ok(MetaOutcome::Continue),
            ".help" | "\\?" => {
                println!(
                    "Available commands:\n  .tables          List tables\n  .schema <table>  Show table schema\n  .examples        Show SQL examples\n  .reset           Reset database (clear all data)\n  .quit/.exit      Exit the REPL"
                );
                Ok(MetaOutcome::Continue)
            }
            ".tables" | "\\dt" => {
                self.show_tables()?;
                Ok(MetaOutcome::Continue)
            }
            cmd if cmd.starts_with(".schema") || cmd.starts_with("\\d") => {
                let parts: Vec<&str> = cmd.split_whitespace().collect();
                if parts.len() < 2 {
                    eprintln!("usage: .schema <table>");
                } else if let Err(err) = self.show_schema(parts[1]) {
                    eprintln!("error: {err}");
                }
                Ok(MetaOutcome::Continue)
            }
            ".examples" => {
                self.show_examples();
                Ok(MetaOutcome::Continue)
            }
            ".reset" => {
                self.reset_database()?;
                Ok(MetaOutcome::Continue)
            }
            ".quit" | ".exit" | "\\q" => Ok(MetaOutcome::Exit),
            _ => Ok(MetaOutcome::NotMeta),
        }
    }

    fn show_tables(&self) -> Result<()> {
        #[derive(Clone, Tabled)]
        struct TableRow {
            #[tabled(rename = "Id")]
            id: u64,
            #[tabled(rename = "Name")]
            name: String,
            #[tabled(rename = "Columns")]
            columns: u16,
            #[tabled(rename = "Indexes")]
            indexes: u16,
        }

        let rows: Vec<TableRow> = self
            .state
            .catalog
            .table_summaries()
            .into_iter()
            .map(|summary| TableRow {
                id: summary.id.0,
                name: summary.name,
                columns: summary.column_count,
                indexes: summary.index_count,
            })
            .collect();

        if rows.is_empty() {
            println!("No tables found.");
            return Ok(());
        }

        let rendered = pretty::render_structured_rows(&rows, self.style);
        println!("{rendered}");
        Ok(())
    }

    fn show_schema(&self, table_name: &str) -> Result<()> {
        let table = self
            .state
            .catalog
            .table(table_name)
            .map_err(anyhow::Error::from)?;

        #[derive(Clone, Tabled)]
        struct ColumnRow {
            #[tabled(rename = "Column")]
            name: String,
            #[tabled(rename = "Type")]
            ty: String,
        }

        let rows: Vec<ColumnRow> = table
            .schema
            .columns()
            .iter()
            .map(|col| ColumnRow {
                name: col.name.clone(),
                ty: format!("{:?}", col.ty),
            })
            .collect();

        if rows.is_empty() {
            println!("Table '{table_name}' has no columns.");
            return Ok(());
        }

        let rendered = pretty::render_structured_rows(&rows, self.style);
        println!("{rendered}");
        Ok(())
    }

    fn history_path(&self) -> PathBuf {
        self.state.data_dir.join(HISTORY_FILE)
    }

    fn reset_database(&mut self) -> Result<()> {
        println!("Resetting database...");
        self.state.reset()?;
        println!("Database reset complete. All data cleared.");
        Ok(())
    }

    fn show_examples(&self) {
        println!("SQL Examples:\n");
        println!("DDL - Data Definition Language:");
        println!("  CREATE TABLE users (id INT, name TEXT, active BOOL);");
        println!("  CREATE TABLE users (id INT PRIMARY KEY, email TEXT);");
        println!("  DROP TABLE users;");
        println!("  CREATE INDEX idx_name ON users (name);");
        println!("  DROP INDEX idx_name;\n");
        println!("DML - Data Manipulation Language:");
        println!("  INSERT INTO users VALUES (1, 'Alice', true);");
        println!("  INSERT INTO users VALUES (2, 'Bob', false);");
        println!("  SELECT * FROM users;");
        println!("  SELECT id, name FROM users WHERE active = true;");
        println!("  UPDATE users SET active = false WHERE id = 1;");
        println!("  DELETE FROM users WHERE id = 2;\n");
        println!("Meta Commands:");
        println!("  .tables          List all tables");
        println!("  .schema users    Show table schema");
        println!("  .examples        Show this help");
        println!("  .reset           Clear all data");
        println!("  .help            Show available commands");
        println!("  .quit            Exit the REPL\n");
        println!("Supported Types: INT, TEXT, BOOL");
    }
}

#[derive(Debug)]
struct DatabaseState {
    data_dir: PathBuf,
    catalog_path: PathBuf,
    wal_path: PathBuf,
    buffer_pages: usize,
    catalog: Catalog,
    pager: FilePager,
    wal: Wal,
}

impl DatabaseState {
    fn new(
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

    fn persist_catalog(&self) -> Result<()> {
        self.catalog
            .save(&self.catalog_path)
            .map_err(anyhow::Error::from)
    }

    fn log_wal(&mut self, record: WalRecord) -> Result<()> {
        self.wal
            .append(&record)
            .and_then(|_| self.wal.sync())
            .map_err(anyhow::Error::from)
    }

    fn remove_heap_file(&self, table_name: &str) -> Result<()> {
        let path = self.data_dir.join(format!("{table_name}.heap"));
        if path.exists() {
            fs::remove_file(&path)
                .with_context(|| format!("failed to remove heap file {}", path.display()))?;
        }
        Ok(())
    }

    fn with_execution_context<T, F>(&mut self, f: F) -> Result<T>
    where
        F: FnOnce(&mut ExecutionContext<'_>) -> common::DbResult<T>,
    {
        let mut ctx = ExecutionContext::new(
            &self.catalog,
            &mut self.pager,
            &mut self.wal,
            self.data_dir.clone(),
        );
        f(&mut ctx).map_err(anyhow::Error::from)
    }

    fn reset(&mut self) -> Result<()> {
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

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
enum MetaOutcome {
    NotMeta,
    Continue,
    Exit,
}

fn map_sql_type(raw: &str) -> Result<SqlType> {
    match raw.trim().to_uppercase().as_str() {
        "INT" | "INTEGER" => Ok(SqlType::Int),
        "TEXT" | "STRING" | "VARCHAR" => Ok(SqlType::Text),
        "BOOL" | "BOOLEAN" => Ok(SqlType::Bool),
        other => Err(anyhow!("unsupported SQL type '{other}'")),
    }
}

fn infer_schema(plan: &PhysicalPlan) -> Vec<String> {
    match plan {
        PhysicalPlan::SeqScan { schema, .. } => schema.clone(),
        PhysicalPlan::IndexScan { schema, .. } => schema.clone(),
        PhysicalPlan::Filter { input, .. } => infer_schema(input),
        PhysicalPlan::Project { columns, .. } => {
            columns.iter().map(|(name, _)| name.clone()).collect()
        }
        PhysicalPlan::Insert { .. } | PhysicalPlan::Update { .. } | PhysicalPlan::Delete { .. } => {
            vec![]
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::TableId;

    #[test]
    fn sql_type_mapping_supports_aliases() {
        assert!(matches!(map_sql_type("int").unwrap(), SqlType::Int));
        assert!(matches!(map_sql_type("INTEGER").unwrap(), SqlType::Int));
        assert!(matches!(map_sql_type("text").unwrap(), SqlType::Text));
        assert!(matches!(map_sql_type("bool").unwrap(), SqlType::Bool));
        assert!(map_sql_type("unknown").is_err());
    }

    #[test]
    fn infer_schema_follows_projection() {
        let plan = PhysicalPlan::Project {
            input: Box::new(PhysicalPlan::SeqScan {
                table_id: TableId(1),
                schema: vec!["id".into(), "name".into()],
            }),
            columns: vec![("name".into(), 1)],
        };

        let schema = infer_schema(&plan);
        assert_eq!(schema, vec!["name"]);
    }
}

fn main() -> Result<()> {
    let args = Args::parse();
    let execute = args.execute.clone();
    let app = App::new(args)?;
    app.run(execute)?;
    Ok(())
}
