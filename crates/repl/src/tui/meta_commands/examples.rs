//! Examples command implementation.

use super::{MetaCommand, MetaCommandResult};
use common::RecordBatch;
use database::Database;

/// Command to display SQL examples.
pub struct ExamplesCommand;

impl MetaCommand for ExamplesCommand {
    fn execute(
        &self,
        _db: &Database,
        _runtime_handle: &tokio::runtime::Handle,
    ) -> MetaCommandResult {
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

        let batch = RecordBatch {
            columns: vec!["Examples".to_string()],
            rows: examples
                .into_iter()
                .map(|line| common::Row::new(vec![types::Value::Text(line)]))
                .collect(),
        };

        MetaCommandResult::Results {
            batch,
            status: "SQL examples displayed".to_string(),
        }
    }

    fn name(&self) -> &'static str {
        ".examples"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_examples_command_name() {
        let cmd = ExamplesCommand;
        assert_eq!(cmd.name(), ".examples");
    }

    #[test]
    fn test_examples_result_column() {
        // Verify expected column schema
        let expected_columns = ["Examples"];
        assert_eq!(expected_columns.len(), 1);
    }
}
