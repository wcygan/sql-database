//! Meta command infrastructure for REPL.
//!
//! Meta commands are special commands that start with `.` and provide
//! functionality like showing help, listing tables, and displaying schemas.

mod demo;
mod examples;
mod help;
mod reset;
mod schema;
mod tables;

pub use demo::DemoCommand;
pub use examples::ExamplesCommand;
pub use help::HelpCommand;
pub use reset::ResetCommand;
pub use schema::SchemaCommand;
pub use tables::TablesCommand;

use common::RecordBatch;
use database::Database;

/// Result of executing a meta command.
#[derive(Debug)]
pub enum MetaCommandResult {
    /// Display results as a table with a status message.
    Results { batch: RecordBatch, status: String },
    /// Display a simple message (no table).
    Message(String),
    /// Command had an error.
    Error(String),
}

/// Trait for meta command implementations.
///
/// Each meta command (e.g., `.help`, `.tables`) implements this trait
/// to provide a consistent interface for execution.
pub trait MetaCommand: Send + Sync {
    /// Execute the command using the database context.
    ///
    /// The `runtime_handle` is provided for commands that need async execution
    /// (like `.reset` which calls async database methods).
    fn execute(&self, db: &Database, runtime_handle: &tokio::runtime::Handle) -> MetaCommandResult;

    /// Get the command name (e.g., ".help").
    ///
    /// Used by tests to verify command parsing. May be used for
    /// logging/debugging in the future.
    #[allow(dead_code)]
    fn name(&self) -> &'static str;
}

/// Parse a command string into a MetaCommand instance.
///
/// Returns an error string if the command is unknown or malformed.
pub fn parse_command(input: &str) -> Result<Box<dyn MetaCommand>, String> {
    let parts: Vec<&str> = input.split_whitespace().collect();

    match parts.first().copied() {
        Some(".help") => Ok(Box::new(HelpCommand)),
        Some(".tables") => Ok(Box::new(TablesCommand)),
        Some(".schema") => {
            let table = parts.get(1).map(|s| (*s).to_string());
            Ok(Box::new(SchemaCommand::new(table)))
        }
        Some(".examples") => Ok(Box::new(ExamplesCommand)),
        Some(".demo") => Ok(Box::new(DemoCommand)),
        Some(".reset") => Ok(Box::new(ResetCommand)),
        Some(cmd) => Err(format!("Unknown command: {}. Try .help", cmd)),
        None => Err("Empty command".to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_help_command() {
        let cmd = parse_command(".help").expect("should parse .help");
        assert_eq!(cmd.name(), ".help");
    }

    #[test]
    fn test_parse_tables_command() {
        let cmd = parse_command(".tables").expect("should parse .tables");
        assert_eq!(cmd.name(), ".tables");
    }

    #[test]
    fn test_parse_schema_command_without_table() {
        let cmd = parse_command(".schema").expect("should parse .schema");
        assert_eq!(cmd.name(), ".schema");
    }

    #[test]
    fn test_parse_schema_command_with_table() {
        let cmd = parse_command(".schema users").expect("should parse .schema users");
        assert_eq!(cmd.name(), ".schema");
    }

    #[test]
    fn test_parse_examples_command() {
        let cmd = parse_command(".examples").expect("should parse .examples");
        assert_eq!(cmd.name(), ".examples");
    }

    #[test]
    fn test_parse_demo_command() {
        let cmd = parse_command(".demo").expect("should parse .demo");
        assert_eq!(cmd.name(), ".demo");
    }

    #[test]
    fn test_parse_reset_command() {
        let cmd = parse_command(".reset").expect("should parse .reset");
        assert_eq!(cmd.name(), ".reset");
    }

    #[test]
    fn test_parse_unknown_command() {
        match parse_command(".unknown") {
            Ok(_) => panic!("expected error for unknown command"),
            Err(e) => assert!(e.contains("Unknown command"), "error was: {}", e),
        }
    }

    #[test]
    fn test_parse_empty_command() {
        match parse_command("") {
            Ok(_) => panic!("expected error for empty command"),
            Err(e) => assert!(e.contains("Empty command"), "error was: {}", e),
        }
    }

    #[test]
    fn test_parse_whitespace_only_command() {
        match parse_command("   ") {
            Ok(_) => panic!("expected error for whitespace-only command"),
            Err(e) => assert!(e.contains("Empty command"), "error was: {}", e),
        }
    }
}
