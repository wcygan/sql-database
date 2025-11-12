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

/// Run an interactive REPL loop.
async fn interactive_loop(client: &mut Client) -> Result<()> {
    let mut rl = DefaultEditor::new()?;

    println!();
    println!("Type SQL statements or .quit to exit");
    println!();

    loop {
        // Read line
        let readline = rl.readline("> ");

        match readline {
            Ok(line) => {
                let line = line.trim();

                if line.is_empty() {
                    continue;
                }

                // Add to history
                let _ = rl.add_history_entry(line);

                // Check for meta commands
                if line == ".quit" || line == ".exit" {
                    break;
                }

                if line == ".help" {
                    print_help();
                    continue;
                }

                // Execute SQL
                match client.execute(line).await {
                    Ok(result) => {
                        print_result(&result);
                    }
                    Err(e) => {
                        eprintln!("Error: {}", e);
                    }
                }
            }
            Err(ReadlineError::Interrupted) => {
                println!("^C");
                break;
            }
            Err(ReadlineError::Eof) => {
                println!("^D");
                break;
            }
            Err(err) => {
                eprintln!("Error: {:?}", err);
                break;
            }
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

/// Print help message.
fn print_help() {
    println!("Commands:");
    println!("  .help    Show this help");
    println!("  .quit    Exit the client");
    println!();
    println!("Or enter SQL statements to execute them.");
}
