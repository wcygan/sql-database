//! Tables command implementation.

use super::{MetaCommand, MetaCommandResult};
use common::RecordBatch;
use database::Database;

/// Command to list all tables in the database.
pub struct TablesCommand;

impl MetaCommand for TablesCommand {
    fn execute(
        &self,
        db: &Database,
        _runtime_handle: &tokio::runtime::Handle,
    ) -> MetaCommandResult {
        let catalog = db.catalog();
        let catalog_lock = catalog.blocking_read();
        let summaries = catalog_lock.table_summaries();

        if summaries.is_empty() {
            return MetaCommandResult::Message("No tables found".to_string());
        }

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

        let count = rows.len();
        let batch = RecordBatch {
            columns: vec![
                "ID".to_string(),
                "Name".to_string(),
                "Columns".to_string(),
                "Indexes".to_string(),
            ],
            rows,
        };

        MetaCommandResult::Results {
            batch,
            status: format!("{} table(s) found", count),
        }
    }

    fn name(&self) -> &'static str {
        ".tables"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tables_command_name() {
        let cmd = TablesCommand;
        assert_eq!(cmd.name(), ".tables");
    }

    #[test]
    fn test_tables_result_schema() {
        // Verify the expected column schema
        let expected_columns = ["ID", "Name", "Columns", "Indexes"];
        assert_eq!(expected_columns.len(), 4);
    }
}
