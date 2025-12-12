use super::meta_commands::{MetaCommandResult, parse_command};
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
        let cmd = match parse_command(command) {
            Ok(c) => c,
            Err(e) => {
                self.results = None;
                self.status_message = Some(e);
                self.execution_time = None;
                return Ok(());
            }
        };

        match cmd.execute(&self.db, &self.runtime_handle) {
            MetaCommandResult::Results { batch, status } => {
                self.results = Some(batch);
                self.status_message = Some(status);
                self.execution_time = None;
            }
            MetaCommandResult::Message(msg) => {
                self.results = None;
                self.status_message = Some(msg);
                self.execution_time = None;
            }
            MetaCommandResult::Error(err) => {
                self.results = None;
                self.status_message = Some(format!("Error: {}", err));
                self.execution_time = None;
            }
        }

        Ok(())
    }
}
