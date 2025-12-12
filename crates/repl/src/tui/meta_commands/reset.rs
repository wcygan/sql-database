//! Reset command implementation.

use super::{MetaCommand, MetaCommandResult};
use database::Database;

/// Command to reset the database (clear all data).
pub struct ResetCommand;

impl MetaCommand for ResetCommand {
    fn execute(&self, db: &Database, runtime_handle: &tokio::runtime::Handle) -> MetaCommandResult {
        match runtime_handle.block_on(db.reset()) {
            Ok(()) => {
                MetaCommandResult::Message("Database reset complete. All data cleared.".to_string())
            }
            Err(e) => MetaCommandResult::Error(format!("Error resetting database: {}", e)),
        }
    }

    fn name(&self) -> &'static str {
        ".reset"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_reset_command_name() {
        let cmd = ResetCommand;
        assert_eq!(cmd.name(), ".reset");
    }
}
