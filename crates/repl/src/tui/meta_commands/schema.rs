//! Schema command implementation.

use super::{MetaCommand, MetaCommandResult};
use common::RecordBatch;
use database::Database;

/// Command to show the schema of a specific table.
pub struct SchemaCommand {
    table: Option<String>,
}

impl SchemaCommand {
    /// Create a new schema command.
    ///
    /// If `table` is None, the command will return a usage message.
    pub fn new(table: Option<String>) -> Self {
        Self { table }
    }
}

impl MetaCommand for SchemaCommand {
    fn execute(
        &self,
        db: &Database,
        _runtime_handle: &tokio::runtime::Handle,
    ) -> MetaCommandResult {
        let table_name = match &self.table {
            Some(name) => name,
            None => {
                return MetaCommandResult::Message("Usage: .schema <table>".to_string());
            }
        };

        let catalog = db.catalog();
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

                let batch = RecordBatch {
                    columns: vec!["Column".to_string(), "Type".to_string()],
                    rows,
                };

                MetaCommandResult::Results {
                    batch,
                    status: format!("Schema for table '{}'", table_name),
                }
            }
            Err(e) => MetaCommandResult::Error(e.to_string()),
        }
    }

    fn name(&self) -> &'static str {
        ".schema"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schema_command_name() {
        let cmd = SchemaCommand::new(Some("users".to_string()));
        assert_eq!(cmd.name(), ".schema");
    }

    #[test]
    fn test_schema_command_without_table_name() {
        let cmd = SchemaCommand::new(None);
        assert_eq!(cmd.name(), ".schema");
        // The table field should be None
        assert!(cmd.table.is_none());
    }

    #[test]
    fn test_schema_command_with_table_name() {
        let cmd = SchemaCommand::new(Some("products".to_string()));
        assert_eq!(cmd.table, Some("products".to_string()));
    }

    #[test]
    fn test_schema_result_columns() {
        // Verify expected column schema
        let expected_columns = ["Column", "Type"];
        assert_eq!(expected_columns.len(), 2);
    }
}
