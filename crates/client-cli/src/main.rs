//! Command-line client for the toy SQL database.
//!
//! Supports both execute mode (one-shot queries) and interactive mode (REPL).

use anyhow::Result;
use clap::Parser;
use client::{Client, QueryResult};
use common::{RecordBatch, pretty};
use rustyline::DefaultEditor;
use rustyline::error::ReadlineError;

const DEFAULT_HOST: &str = "localhost";
const DEFAULT_PORT: u16 = 5432;

#[derive(Parser, Debug)]
#[command(name = "toydb-client", about = "SQL client for the toy database")]
struct Args {
    /// Host address to connect to
    #[arg(long, default_value = DEFAULT_HOST)]
    host: String,

    /// Port to connect to
    #[arg(long, default_value_t = DEFAULT_PORT)]
    port: u16,

    /// Execute the provided SQL and exit
    #[arg(short = 'e', long)]
    execute: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let addr = format!("{}:{}", args.host, args.port);

    // Connect to server
    let mut client = Client::connect(&addr).await?;
    println!("Connected to {}", addr);

    // Execute or interactive mode
    if let Some(sql) = args.execute {
        // Execute mode: run SQL and exit
        execute_and_print(&mut client, &sql).await?;
    } else {
        // Interactive mode: REPL
        interactive_loop(&mut client).await?;
    }

    // Close connection
    client.close().await?;
    Ok(())
}

/// Execute a SQL statement and print the result.
async fn execute_and_print(client: &mut Client, sql: &str) -> Result<()> {
    let result = client.execute(sql).await?;
    print_result(&result);
    Ok(())
}

/// Process a line of input from the REPL.
/// Returns true to continue the loop, false to exit.
async fn process_line(client: &mut Client, line: &str) -> bool {
    // Skip empty lines
    if line.is_empty() {
        return true;
    }

    // Handle meta-commands
    if line == ".quit" || line == ".exit" {
        return false;
    }

    if line == ".help" || line == "help" {
        print_help();
        return true;
    }

    if line == ".examples" || line == "examples" {
        print_examples();
        return true;
    }

    // Handle demo command - runs all examples in sequence
    if line == "demo" {
        run_demo(client).await;
        return true;
    }

    // Handle example-* commands that show SQL and execute it
    if let Some(sql) = get_example_sql(line) {
        run_example(client, sql).await;
        return true;
    }

    // Execute SQL
    match client.execute(line).await {
        Ok(result) => print_result(&result),
        Err(e) => eprintln!("Error: {}", e),
    }

    true
}

/// Handle readline errors.
/// Returns true to continue the loop, false to exit.
fn handle_readline_error(error: ReadlineError) -> bool {
    match error {
        ReadlineError::Interrupted => {
            println!("^C");
            false
        }
        ReadlineError::Eof => {
            println!("^D");
            false
        }
        err => {
            eprintln!("Error: {:?}", err);
            false
        }
    }
}

/// Run an interactive REPL loop.
async fn interactive_loop(client: &mut Client) -> Result<()> {
    let mut rl = DefaultEditor::new()?;

    println!();
    println!("Type 'help' for commands, 'examples' for SQL syntax, or '.quit' to exit");
    println!();

    loop {
        let readline = rl.readline("> ");

        let should_continue = match readline {
            Ok(line) => {
                let line = line.trim();
                let _ = rl.add_history_entry(line);
                process_line(client, line).await
            }
            Err(e) => handle_readline_error(e),
        };

        if !should_continue {
            break;
        }
    }

    Ok(())
}

/// Print a query result.
fn print_result(result: &QueryResult) {
    match result {
        QueryResult::Rows { schema, rows } => {
            let batch = RecordBatch {
                columns: schema.clone(),
                rows: rows.clone(),
            };
            let rendered = pretty::render_record_batch(&batch, pretty::TableStyleKind::Modern);
            println!("{}", rendered);
        }
        QueryResult::Count { affected } => {
            println!("{} row(s) affected", affected);
        }
        QueryResult::Empty => {
            println!("Success");
        }
    }
}

/// Get SQL for an example command, or None if not an example command.
fn get_example_sql(cmd: &str) -> Option<&'static str> {
    match cmd {
        "example-create" => Some("CREATE TABLE users (id INT PRIMARY KEY, name TEXT, active BOOL)"),
        "example-insert" | "example-insert1" => Some("INSERT INTO users VALUES (1, 'alice', true)"),
        "example-insert2" => Some("INSERT INTO users VALUES (2, 'bob', false)"),
        "example-insert3" => Some("INSERT INTO users VALUES (3, 'charlie', true)"),
        "example-select" => Some("SELECT * FROM users"),
        "example-select-where" => Some("SELECT name FROM users WHERE active = true"),
        "example-update" => Some("UPDATE users SET active = true WHERE id = 2"),
        "example-delete" => Some("DELETE FROM users WHERE id = 3"),
        "example-index" => Some("CREATE INDEX idx_name ON users (name)"),
        "example-drop-index" => Some("DROP INDEX idx_name"),
        "example-drop" => Some("DROP TABLE users"),
        "example-all" | "demo" => None, // Special: handled separately
        _ => None,
    }
}

/// Run an example command: show the SQL, then execute it.
async fn run_example(client: &mut Client, sql: &'static str) {
    println!("── Executing ─────────────────────────────────────────────────");
    println!("  {}", sql);
    println!("───────────────────────────────────────────────────────────────");
    match client.execute(sql).await {
        Ok(result) => print_result(&result),
        Err(e) => eprintln!("Error: {}", e),
    }
}

/// Run a full demo of all example commands.
async fn run_demo(client: &mut Client) {
    println!("╔════════════════════════════════════════════════════════════╗");
    println!("║                    ToyDB Interactive Demo                  ║");
    println!("╚════════════════════════════════════════════════════════════╝");
    println!();

    let steps: &[(&str, &str)] = &[
        (
            "1. Create a table",
            "CREATE TABLE users (id INT PRIMARY KEY, name TEXT, active BOOL)",
        ),
        (
            "2. Insert first row",
            "INSERT INTO users VALUES (1, 'alice', true)",
        ),
        (
            "3. Insert second row",
            "INSERT INTO users VALUES (2, 'bob', false)",
        ),
        (
            "4. Insert third row",
            "INSERT INTO users VALUES (3, 'charlie', true)",
        ),
        ("5. Select all rows", "SELECT * FROM users"),
        (
            "6. Select with WHERE",
            "SELECT name FROM users WHERE active = true",
        ),
        (
            "7. Update a row",
            "UPDATE users SET active = true WHERE id = 2",
        ),
        ("8. Verify update", "SELECT * FROM users"),
        ("9. Delete a row", "DELETE FROM users WHERE id = 3"),
        ("10. Verify delete", "SELECT * FROM users"),
        (
            "11. Create an index",
            "CREATE INDEX idx_name ON users (name)",
        ),
        ("12. Drop the index", "DROP INDEX idx_name"),
        ("13. Clean up - drop table", "DROP TABLE users"),
    ];

    for (description, sql) in steps {
        println!("┌─ {} ─", description);
        println!("│  {}", sql);
        println!("└────────────────────────────────────────────────────────────");
        match client.execute(sql).await {
            Ok(result) => print_result(&result),
            Err(e) => eprintln!("Error: {}", e),
        }
        println!();
    }

    println!("Demo complete!");
}

/// Print help message.
fn print_help() {
    println!("╔════════════════════════════════════════════════════════════╗");
    println!("║                      ToyDB Client Help                     ║");
    println!("╠════════════════════════════════════════════════════════════╣");
    println!("║  Commands:                                                 ║");
    println!("║    help              Show this help message                ║");
    println!("║    examples          Show SQL syntax examples              ║");
    println!("║    demo              Run full interactive demo             ║");
    println!("║    .quit, .exit      Exit the client                       ║");
    println!("║                                                            ║");
    println!("║  Interactive Examples (show SQL then execute):             ║");
    println!("║    example-create       CREATE TABLE users (...)           ║");
    println!("║    example-insert       INSERT INTO users VALUES (1, ...)  ║");
    println!("║    example-insert2      INSERT INTO users VALUES (2, ...)  ║");
    println!("║    example-insert3      INSERT INTO users VALUES (3, ...)  ║");
    println!("║    example-select       SELECT * FROM users                ║");
    println!("║    example-select-where SELECT with WHERE clause           ║");
    println!("║    example-update       UPDATE users SET ...               ║");
    println!("║    example-delete       DELETE FROM users WHERE ...        ║");
    println!("║    example-index        CREATE INDEX on users              ║");
    println!("║    example-drop-index   DROP INDEX                         ║");
    println!("║    example-drop         DROP TABLE users                   ║");
    println!("║                                                            ║");
    println!("║  SQL Statements:                                           ║");
    println!("║    CREATE TABLE, DROP TABLE, CREATE INDEX, DROP INDEX      ║");
    println!("║    INSERT, SELECT, UPDATE, DELETE                          ║");
    println!("║                                                            ║");
    println!("║  Type 'examples' for detailed SQL syntax                   ║");
    println!("╚════════════════════════════════════════════════════════════╝");
}

/// Print SQL examples.
fn print_examples() {
    println!("╔════════════════════════════════════════════════════════════╗");
    println!("║                      SQL Examples                          ║");
    println!("╠════════════════════════════════════════════════════════════╣");
    println!("║  CREATE TABLE                                              ║");
    println!("║    CREATE TABLE users (                                    ║");
    println!("║      id INT PRIMARY KEY,                                   ║");
    println!("║      name TEXT,                                            ║");
    println!("║      active BOOL                                           ║");
    println!("║    );                                                      ║");
    println!("║                                                            ║");
    println!("║  INSERT                                                    ║");
    println!("║    INSERT INTO users VALUES (1, 'alice', true);            ║");
    println!("║    INSERT INTO users VALUES (2, 'bob', false);             ║");
    println!("║                                                            ║");
    println!("║  SELECT                                                    ║");
    println!("║    SELECT * FROM users;                                    ║");
    println!("║    SELECT name FROM users WHERE active = true;             ║");
    println!("║    SELECT id, name FROM users WHERE id > 1;                ║");
    println!("║                                                            ║");
    println!("║  UPDATE                                                    ║");
    println!("║    UPDATE users SET active = true WHERE id = 2;            ║");
    println!("║                                                            ║");
    println!("║  DELETE                                                    ║");
    println!("║    DELETE FROM users WHERE active = false;                 ║");
    println!("║                                                            ║");
    println!("║  CREATE INDEX                                              ║");
    println!("║    CREATE INDEX idx_name ON users (name);                  ║");
    println!("║                                                            ║");
    println!("║  DROP                                                      ║");
    println!("║    DROP INDEX idx_name;                                    ║");
    println!("║    DROP TABLE users;                                       ║");
    println!("╚════════════════════════════════════════════════════════════╝");
}
