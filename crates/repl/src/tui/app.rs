use anyhow::Result;
use common::RecordBatch;
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use database::{Database, QueryResult};
use std::time::{Duration, Instant};
use tokio::runtime::Handle;
use tui_textarea::TextArea;

pub struct App<'a> {
    pub db: Database,
    pub runtime_handle: Handle,
    pub editor: TextArea<'a>,
    pub results: Option<RecordBatch>,
    pub status_message: Option<String>,
    pub execution_time: Option<Duration>,
    pub command_history: Vec<String>,
    pub results_scroll: u16,
}

impl<'a> App<'a> {
    pub fn new(db: Database, runtime_handle: Handle) -> Self {
        let mut editor = TextArea::default();
        editor.set_block(
            ratatui::widgets::Block::default()
                .borders(ratatui::widgets::Borders::ALL)
                .title("Current Input (Enter to execute, .help for commands, Ctrl+Q to quit)"),
        );

        Self {
            db,
            runtime_handle,
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
        let current_input = self.editor.lines().join("\n").trim().to_string();

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
        let result = self.runtime_handle.block_on(self.db.execute(sql))?;

        match result {
            QueryResult::Rows { schema, rows } => {
                self.results = Some(RecordBatch {
                    columns: schema,
                    rows,
                });
                self.status_message = None;
            }
            QueryResult::Count { affected } => {
                self.results = None;
                self.status_message = Some(format!("{} row(s) affected", affected));
            }
            QueryResult::Empty => {
                self.results = None;
                self.status_message = Some("Success".to_string());
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
                    "  .examples          Show SQL examples".to_string(),
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
                let catalog = self.db.catalog();
                let catalog_lock = catalog.blocking_read();
                let summaries = catalog_lock.table_summaries();
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
                    let catalog = self.db.catalog();
                    let catalog_lock = catalog.blocking_read();
                    match catalog_lock.table(table_name) {
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
                match self.runtime_handle.block_on(self.db.reset()) {
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
            ".examples" => {
                let examples = vec![
                    "SQL Examples".to_string(),
                    "".to_string(),
                    "DDL - Data Definition Language:".to_string(),
                    "  CREATE TABLE users (id INT, name TEXT, active BOOL);".to_string(),
                    "  CREATE TABLE products (id INT, name TEXT, PRIMARY KEY (id));".to_string(),
                    "  DROP TABLE users;".to_string(),
                    "  CREATE INDEX idx_name ON users (name);".to_string(),
                    "  DROP INDEX idx_name;".to_string(),
                    "".to_string(),
                    "DML - Data Manipulation Language:".to_string(),
                    "  INSERT INTO users VALUES (1, 'Alice', true);".to_string(),
                    "  INSERT INTO users VALUES (2, 'Bob', false);".to_string(),
                    "  SELECT * FROM users;".to_string(),
                    "  SELECT id, name FROM users WHERE active = true;".to_string(),
                    "  UPDATE users SET active = false WHERE id = 1;".to_string(),
                    "  DELETE FROM users WHERE id = 2;".to_string(),
                    "".to_string(),
                    "Query Analysis:".to_string(),
                    "  EXPLAIN SELECT * FROM users;".to_string(),
                    "  EXPLAIN ANALYZE SELECT * FROM users WHERE active = true;".to_string(),
                    "".to_string(),
                    "Supported Types:".to_string(),
                    "  INT, TEXT, BOOL".to_string(),
                ];

                self.results = Some(RecordBatch {
                    columns: vec!["Examples".to_string()],
                    rows: examples
                        .into_iter()
                        .map(|line| common::Row::new(vec![types::Value::Text(line)]))
                        .collect(),
                });
                self.status_message = Some("SQL examples displayed".to_string());
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
