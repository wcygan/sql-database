use crate::database::DatabaseState;
use anyhow::{Context, Result};
use catalog::Column;
use common::RecordBatch;
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use executor::{execute_dml, execute_query};
use parser::{ColumnDef, Statement, parse_sql};
use planner::{PhysicalPlan, Planner, PlanningContext};
use std::time::{Duration, Instant};
use tui_textarea::TextArea;
use types::SqlType;
use wal::WalRecord;

pub struct App<'a> {
    pub db: DatabaseState,
    pub editor: TextArea<'a>,
    pub results: Option<RecordBatch>,
    pub status_message: Option<String>,
    pub execution_time: Option<Duration>,
    pub command_history: Vec<String>,
    pub results_scroll: u16,
}

impl<'a> App<'a> {
    pub fn new(db: DatabaseState) -> Self {
        let mut editor = TextArea::default();
        editor.set_block(
            ratatui::widgets::Block::default()
                .borders(ratatui::widgets::Borders::ALL)
                .title("Current Input (Enter to execute, .help for commands, Ctrl+Q to quit)"),
        );

        Self {
            db,
            editor,
            results: None,
            status_message: None,
            execution_time: None,
            command_history: Vec::new(),
            results_scroll: 0,
        }
    }

    pub fn handle_key(&mut self, key: KeyEvent) -> Result<bool> {
        // Scroll results with Up/Down arrows
        if key.code == KeyCode::Up && self.results.is_some() {
            self.results_scroll = self.results_scroll.saturating_sub(1);
            return Ok(false);
        }
        if key.code == KeyCode::Down && self.results.is_some() {
            self.results_scroll = self.results_scroll.saturating_add(1);
            return Ok(false);
        }

        // Execute SQL on Enter (without modifiers)
        if key.code == KeyCode::Enter && key.modifiers.is_empty() {
            self.execute_sql()?;
            return Ok(false);
        }

        // Clear editor and history on Ctrl+C
        if key.code == KeyCode::Char('c') && key.modifiers.contains(KeyModifiers::CONTROL) {
            self.editor = TextArea::default();
            self.editor.set_block(
                ratatui::widgets::Block::default()
                    .borders(ratatui::widgets::Borders::ALL)
                    .title("Current Input (Enter to execute, .help for commands, Ctrl+Q to quit)"),
            );
            self.results = None;
            self.status_message = None;
            self.execution_time = None;
            self.command_history.clear();
            self.results_scroll = 0;
            return Ok(false);
        }

        // Pass to editor
        self.editor.input(key);
        Ok(false)
    }

    fn execute_sql(&mut self) -> Result<()> {
        // Get current input text
        let current_input = self
            .editor
            .lines()
            .join("\n")
            .trim()
            .to_string();

        if current_input.is_empty() {
            return Ok(());
        }

        // Add to history
        self.command_history.push(current_input.clone());

        // Check for meta commands
        if current_input.starts_with('.') {
            self.handle_meta_command(&current_input)?;
        } else {
            let start = Instant::now();

            match self.execute_sql_inner(&current_input) {
                Ok(()) => {
                    self.execution_time = Some(start.elapsed());
                    self.status_message = Some("Success".to_string());
                }
                Err(e) => {
                    self.results = None;
                    self.execution_time = None;
                    self.status_message = Some(format!("Error: {}", e));
                }
            }
        }

        // Reset scroll position for new results
        self.results_scroll = 0;

        // Clear the editor for next input
        self.editor = TextArea::default();
        self.editor.set_block(
            ratatui::widgets::Block::default()
                .borders(ratatui::widgets::Borders::ALL)
                .title("Current Input (Enter to execute, .help for commands, Ctrl+Q to quit)"),
        );

        Ok(())
    }

    fn execute_sql_inner(&mut self, sql: &str) -> Result<()> {
        let statements = parse_sql(sql).map_err(anyhow::Error::from)?;

        for stmt in statements {
            match stmt {
                Statement::CreateTable {
                    name,
                    columns,
                    primary_key,
                } => self.create_table(name, columns, primary_key)?,
                Statement::DropTable { name } => self.drop_table(name)?,
                Statement::CreateIndex {
                    name,
                    table,
                    column,
                } => self.create_index(name, table, column)?,
                Statement::DropIndex { name } => self.drop_index(name)?,
                other => self.execute_planned(other)?,
            }
        }

        Ok(())
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
            .db
            .catalog
            .create_table(&name, catalog_columns, primary_key_ordinals)
            .map_err(anyhow::Error::from)?;
        self.db.persist_catalog()?;
        self.db.log_wal(WalRecord::CreateTable {
            name: name.clone(),
            table: table_id,
        })?;

        self.results = None;
        self.status_message = Some(format!("Created table '{}' (id = {})", name, table_id.0));
        Ok(())
    }

    fn drop_table(&mut self, name: String) -> Result<()> {
        let table_id = self
            .db
            .catalog
            .table(&name)
            .map_err(anyhow::Error::from)?
            .id;
        self.db
            .catalog
            .drop_table(&name)
            .map_err(anyhow::Error::from)?;
        self.db.persist_catalog()?;
        self.db.remove_heap_file(&name)?;
        self.db.log_wal(WalRecord::DropTable { table: table_id })?;

        self.results = None;
        self.status_message = Some(format!("Dropped table '{}'", name));
        Ok(())
    }

    fn create_index(&mut self, name: String, table: String, column: String) -> Result<()> {
        use catalog::IndexKind;
        self.db
            .catalog
            .create_index()
            .table_name(&table)
            .index_name(&name)
            .columns(&[column.as_str()])
            .kind(IndexKind::BTree)
            .call()
            .map_err(anyhow::Error::from)?;
        self.db.persist_catalog()?;

        self.results = None;
        self.status_message = Some(format!("Created index '{}' on '{}'", name, table));
        Ok(())
    }

    fn drop_index(&mut self, name: String) -> Result<()> {
        let table_name = self
            .db
            .catalog
            .tables()
            .find(|table| table.index(&name).is_ok())
            .map(|table| table.name.clone())
            .ok_or_else(|| anyhow::anyhow!("index '{}' not found", name))?;

        self.db
            .catalog
            .drop_index(&table_name, &name)
            .map_err(anyhow::Error::from)?;
        self.db.persist_catalog()?;

        self.results = None;
        self.status_message = Some(format!("Dropped index '{}' on '{}'", name, table_name));
        Ok(())
    }

    fn execute_planned(&mut self, stmt: Statement) -> Result<()> {
        let mut planning_ctx = PlanningContext::new(&self.db.catalog);
        let plan = Planner::plan(stmt, &mut planning_ctx).map_err(anyhow::Error::from)?;

        match plan {
            PhysicalPlan::Insert { .. }
            | PhysicalPlan::Update { .. }
            | PhysicalPlan::Delete { .. } => {
                let count = self
                    .db
                    .with_execution_context(|ctx| execute_dml(plan, ctx))?;
                self.results = None;
                self.status_message = Some(format!("{} row(s) affected", count));
            }
            other => {
                let schema = infer_schema(&other);
                let rows = self
                    .db
                    .with_execution_context(|ctx| execute_query(other, ctx))?;
                let batch = RecordBatch {
                    columns: schema,
                    rows,
                };
                self.results = Some(batch);
                self.status_message = None;
            }
        }

        Ok(())
    }

    fn handle_meta_command(&mut self, command: &str) -> Result<()> {
        let parts: Vec<&str> = command.split_whitespace().collect();

        match parts[0] {
            ".help" => {
                let help_text = vec![
                    "Meta Commands:".to_string(),
                    "  .help              Show this help".to_string(),
                    "  .tables            List all tables".to_string(),
                    "  .schema <table>    Show table schema".to_string(),
                    "  .reset             Reset database (clear all data)".to_string(),
                    "".to_string(),
                    "Keyboard Shortcuts:".to_string(),
                    "  Enter              Execute SQL or meta command".to_string(),
                    "  Ctrl+C             Clear editor and results".to_string(),
                    "  Ctrl+Q             Quit application".to_string(),
                    "".to_string(),
                    "SQL Support:".to_string(),
                    "  DDL: CREATE TABLE, DROP TABLE, CREATE INDEX, DROP INDEX".to_string(),
                    "  DML: INSERT, SELECT, UPDATE, DELETE".to_string(),
                    "  Types: INT, TEXT, BOOL".to_string(),
                ];

                self.results = Some(RecordBatch {
                    columns: vec!["Help".to_string()],
                    rows: help_text
                        .into_iter()
                        .map(|line| common::Row::new(vec![types::Value::Text(line)]))
                        .collect(),
                });
                self.status_message = Some("Help displayed".to_string());
                self.execution_time = None;
            }
            ".tables" => {
                let summaries = self.db.catalog.table_summaries();
                if summaries.is_empty() {
                    self.results = None;
                    self.status_message = Some("No tables found".to_string());
                } else {
                    let rows: Vec<common::Row> = summaries
                        .into_iter()
                        .map(|s| {
                            common::Row::new(vec![
                                types::Value::Int(s.id.0 as i64),
                                types::Value::Text(s.name),
                                types::Value::Int(s.column_count as i64),
                                types::Value::Int(s.index_count as i64),
                            ])
                        })
                        .collect();

                    self.results = Some(RecordBatch {
                        columns: vec![
                            "ID".to_string(),
                            "Name".to_string(),
                            "Columns".to_string(),
                            "Indexes".to_string(),
                        ],
                        rows,
                    });
                    self.status_message = Some(format!(
                        "{} table(s) found",
                        self.results.as_ref().unwrap().rows.len()
                    ));
                }
                self.execution_time = None;
            }
            ".schema" => {
                if parts.len() < 2 {
                    self.results = None;
                    self.status_message = Some("Usage: .schema <table>".to_string());
                } else {
                    let table_name = parts[1];
                    match self.db.catalog.table(table_name) {
                        Ok(table) => {
                            let rows: Vec<common::Row> = table
                                .schema
                                .columns()
                                .iter()
                                .map(|col| {
                                    common::Row::new(vec![
                                        types::Value::Text(col.name.clone()),
                                        types::Value::Text(format!("{:?}", col.ty)),
                                    ])
                                })
                                .collect();

                            self.results = Some(RecordBatch {
                                columns: vec!["Column".to_string(), "Type".to_string()],
                                rows,
                            });
                            self.status_message =
                                Some(format!("Schema for table '{}'", table_name));
                        }
                        Err(e) => {
                            self.results = None;
                            self.status_message = Some(format!("Error: {}", e));
                        }
                    }
                }
                self.execution_time = None;
            }
            ".reset" => {
                match self.db.reset() {
                    Ok(()) => {
                        self.results = None;
                        self.status_message =
                            Some("Database reset complete. All data cleared.".to_string());
                    }
                    Err(e) => {
                        self.results = None;
                        self.status_message = Some(format!("Error resetting database: {}", e));
                    }
                }
                self.execution_time = None;
            }
            _ => {
                self.results = None;
                self.status_message = Some(format!("Unknown command: {}. Try .help", parts[0]));
                self.execution_time = None;
            }
        }

        Ok(())
    }
}

fn map_sql_type(raw: &str) -> Result<SqlType> {
    match raw.trim().to_uppercase().as_str() {
        "INT" | "INTEGER" => Ok(SqlType::Int),
        "TEXT" | "STRING" | "VARCHAR" => Ok(SqlType::Text),
        "BOOL" | "BOOLEAN" => Ok(SqlType::Bool),
        other => Err(anyhow::anyhow!("unsupported SQL type '{}'", other)),
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
