//! Demo command implementation - demonstrates JOIN functionality.

use super::{MetaCommand, MetaCommandResult};
use common::RecordBatch;
use database::Database;

/// Command to run an interactive JOIN demonstration.
///
/// Creates sample tables (students, enrollments, courses), populates them
/// with data, and executes JOIN queries to show the feature in action.
pub struct DemoCommand;

impl DemoCommand {
    /// Execute a SQL statement and return the result or error message.
    fn exec(
        db: &Database,
        runtime_handle: &tokio::runtime::Handle,
        sql: &str,
    ) -> Result<database::QueryResult, String> {
        runtime_handle
            .block_on(db.execute(sql))
            .map_err(|e| format!("Error: {}", e))
    }

    /// Build the demo output showing executed queries and results.
    fn build_demo_output(
        db: &Database,
        runtime_handle: &tokio::runtime::Handle,
    ) -> Result<RecordBatch, String> {
        let mut output_lines: Vec<String> = vec![
            "=== JOIN Demonstration ===".to_string(),
            String::new(),
            "Step 1: Creating tables...".to_string(),
            String::new(),
        ];

        // Drop existing demo tables if they exist (ignore errors)
        let _ = Self::exec(db, runtime_handle, "DROP TABLE demo_enrollments");
        let _ = Self::exec(db, runtime_handle, "DROP TABLE demo_students");
        let _ = Self::exec(db, runtime_handle, "DROP TABLE demo_courses");

        let create_students =
            "CREATE TABLE demo_students (id INT, name TEXT, PRIMARY KEY (id))";
        output_lines.push(format!("  > {}", create_students));
        Self::exec(db, runtime_handle, create_students)?;

        let create_courses =
            "CREATE TABLE demo_courses (id INT, title TEXT, PRIMARY KEY (id))";
        output_lines.push(format!("  > {}", create_courses));
        Self::exec(db, runtime_handle, create_courses)?;

        let create_enrollments =
            "CREATE TABLE demo_enrollments (student_id INT, course_id INT)";
        output_lines.push(format!("  > {}", create_enrollments));
        Self::exec(db, runtime_handle, create_enrollments)?;

        output_lines.push("".to_string());

        // Step 2: Insert data
        output_lines.push("Step 2: Inserting sample data...".to_string());
        output_lines.push("".to_string());

        let inserts = [
            "INSERT INTO demo_students VALUES (1, 'Alice')",
            "INSERT INTO demo_students VALUES (2, 'Bob')",
            "INSERT INTO demo_students VALUES (3, 'Carol')",
            "INSERT INTO demo_courses VALUES (101, 'Database Systems')",
            "INSERT INTO demo_courses VALUES (102, 'Algorithms')",
            "INSERT INTO demo_courses VALUES (103, 'Operating Systems')",
            "INSERT INTO demo_enrollments VALUES (1, 101)",
            "INSERT INTO demo_enrollments VALUES (1, 102)",
            "INSERT INTO demo_enrollments VALUES (2, 101)",
            "INSERT INTO demo_enrollments VALUES (3, 102)",
            "INSERT INTO demo_enrollments VALUES (3, 103)",
        ];

        for sql in inserts {
            output_lines.push(format!("  > {}", sql));
            Self::exec(db, runtime_handle, sql)?;
        }

        output_lines.push("".to_string());

        // Step 3: Show JOIN queries
        output_lines.push("Step 3: Running JOIN queries...".to_string());
        output_lines.push("".to_string());

        // Query 1: Students with their enrollments
        let join_query1 = "SELECT s.name, e.course_id FROM demo_students s JOIN demo_enrollments e ON s.id = e.student_id";
        output_lines.push(format!("Query 1: {}", join_query1));
        output_lines.push("".to_string());

        match Self::exec(db, runtime_handle, join_query1)? {
            database::QueryResult::Rows { schema, rows } => {
                // Format header
                output_lines.push(format!("  | {} |", schema.join(" | ")));
                output_lines.push(format!("  |{}|", "-".repeat(schema.len() * 12)));
                // Format rows
                for row in &rows {
                    let values: Vec<String> = row
                        .values
                        .iter()
                        .map(|v| format!("{:?}", v))
                        .collect();
                    output_lines.push(format!("  | {} |", values.join(" | ")));
                }
                output_lines.push(format!("  ({} rows)", rows.len()));
            }
            _ => output_lines.push("  (no results)".to_string()),
        }

        output_lines.push("".to_string());

        // Query 2: Three-way join (students -> enrollments -> courses)
        let join_query2 = "SELECT s.name, c.title FROM demo_students s JOIN demo_enrollments e ON s.id = e.student_id JOIN demo_courses c ON e.course_id = c.id";
        output_lines.push(format!("Query 2: {}", join_query2));
        output_lines.push("".to_string());

        match Self::exec(db, runtime_handle, join_query2)? {
            database::QueryResult::Rows { schema, rows } => {
                // Format header
                output_lines.push(format!("  | {} |", schema.join(" | ")));
                output_lines.push(format!("  |{}|", "-".repeat(schema.len() * 15)));
                // Format rows
                for row in &rows {
                    let values: Vec<String> = row
                        .values
                        .iter()
                        .map(|v| format!("{:?}", v))
                        .collect();
                    output_lines.push(format!("  | {} |", values.join(" | ")));
                }
                output_lines.push(format!("  ({} rows)", rows.len()));
            }
            _ => output_lines.push("  (no results)".to_string()),
        }

        output_lines.push("".to_string());
        output_lines.push("=== Demo Complete ===".to_string());
        output_lines.push("".to_string());
        output_lines.push("Tables created: demo_students, demo_courses, demo_enrollments".to_string());
        output_lines.push("You can now run your own queries on these tables!".to_string());

        // Convert to RecordBatch
        let batch = RecordBatch {
            columns: vec!["Demo Output".to_string()],
            rows: output_lines
                .into_iter()
                .map(|line| common::Row::new(vec![types::Value::Text(line)]))
                .collect(),
        };

        Ok(batch)
    }
}

impl MetaCommand for DemoCommand {
    fn execute(
        &self,
        db: &Database,
        runtime_handle: &tokio::runtime::Handle,
    ) -> MetaCommandResult {
        match Self::build_demo_output(db, runtime_handle) {
            Ok(batch) => MetaCommandResult::Results {
                batch,
                status: "JOIN demo completed successfully".to_string(),
            },
            Err(e) => MetaCommandResult::Error(e),
        }
    }

    fn name(&self) -> &'static str {
        ".demo"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_demo_command_name() {
        let cmd = DemoCommand;
        assert_eq!(cmd.name(), ".demo");
    }
}
