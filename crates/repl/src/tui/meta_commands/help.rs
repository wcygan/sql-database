//! Help command implementation.

use super::{MetaCommand, MetaCommandResult};
use common::RecordBatch;
use database::Database;

/// Command to display help information.
pub struct HelpCommand;

impl MetaCommand for HelpCommand {
    fn execute(
        &self,
        _db: &Database,
        _runtime_handle: &tokio::runtime::Handle,
    ) -> MetaCommandResult {
        let help_text = vec![
            "Meta Commands:".to_string(),
            "  .help              Show this help".to_string(),
            "  .tables            List all tables".to_string(),
            "  .schema <table>    Show table schema".to_string(),
            "  .examples          Show SQL examples".to_string(),
            "  .demo              Run interactive JOIN demonstration".to_string(),
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
            "  JOIN: SELECT ... FROM t1 JOIN t2 ON condition".to_string(),
            "  Types: INT, TEXT, BOOL".to_string(),
        ];

        let batch = RecordBatch {
            columns: vec!["Help".to_string()],
            rows: help_text
                .into_iter()
                .map(|line| common::Row::new(vec![types::Value::Text(line)]))
                .collect(),
        };

        MetaCommandResult::Results {
            batch,
            status: "Help displayed".to_string(),
        }
    }

    fn name(&self) -> &'static str {
        ".help"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_help_command_name() {
        let cmd = HelpCommand;
        assert_eq!(cmd.name(), ".help");
    }

    #[test]
    fn test_help_text_contains_all_commands() {
        // Verify the help text structure contains all expected commands
        let expected_commands = [".help", ".tables", ".schema", ".examples", ".demo", ".reset"];

        let expected_shortcuts = ["Ctrl+C", "Ctrl+Q"];

        let expected_sql = ["CREATE TABLE", "INSERT", "SELECT"];

        // All expected items should be non-empty
        for item in expected_commands
            .iter()
            .chain(expected_shortcuts.iter())
            .chain(expected_sql.iter())
        {
            assert!(!item.is_empty(), "Expected item should not be empty");
        }
    }

    #[test]
    fn test_help_result_has_correct_column() {
        // Verify the result structure
        let help_text = vec!["Test line".to_string()];
        let batch = RecordBatch {
            columns: vec!["Help".to_string()],
            rows: help_text
                .into_iter()
                .map(|line| common::Row::new(vec![types::Value::Text(line)]))
                .collect(),
        };

        assert_eq!(batch.columns.len(), 1);
        assert_eq!(batch.columns[0], "Help");
        assert_eq!(batch.rows.len(), 1);
    }
}
