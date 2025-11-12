mod tui;

use anyhow::Result;
use clap::Parser;
use database::{Database, QueryResult};
use std::path::PathBuf;

const DEFAULT_DATA_DIR: &str = "./db_data";
const DEFAULT_CATALOG_FILE: &str = "catalog.json";
const DEFAULT_WAL_FILE: &str = "toydb.wal";

#[derive(Parser, Debug)]
#[command(
    name = "toydb-repl",
    about = "Interactive SQL console for the toy database"
)]
struct Args {
    /// Directory containing catalog, WAL, and table files
    #[arg(long, default_value = DEFAULT_DATA_DIR)]
    data_dir: PathBuf,
    /// Catalog filename within the data directory
    #[arg(long, default_value = DEFAULT_CATALOG_FILE)]
    catalog_file: String,
    /// WAL filename within the data directory
    #[arg(long, default_value = DEFAULT_WAL_FILE)]
    wal_file: String,
    /// Maximum number of pages held in the file pager cache
    #[arg(long, default_value_t = 256)]
    buffer_pages: usize,
    /// Execute the provided SQL and exit instead of starting the TUI
    #[arg(short = 'e', long = "execute")]
    execute: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let db = Database::new(
        &args.data_dir,
        &args.catalog_file,
        &args.wal_file,
        args.buffer_pages,
    )
    .await?;

    if let Some(sql) = args.execute {
        // Execute mode: run SQL and exit without TUI
        execute_and_exit(db, &sql).await?;
    } else {
        // TUI mode: interactive terminal UI
        // Get the runtime handle before moving into spawn_blocking
        let handle = tokio::runtime::Handle::current();
        let app = tui::App::new(db, handle);
        // Run the TUI in a blocking task since it uses blocking I/O for terminal events
        tokio::task::spawn_blocking(move || tui::run(app))
            .await??;
    }

    Ok(())
}

async fn execute_and_exit(db: Database, sql: &str) -> Result<()> {
    use common::{RecordBatch, pretty::{self, TableStyleKind}};

    let result = db.execute(sql).await?;

    match result {
        QueryResult::Rows { schema, rows } => {
            let batch = RecordBatch { columns: schema, rows };
            let rendered = pretty::render_record_batch(&batch, TableStyleKind::Modern);
            println!("{}", rendered);
        }
        QueryResult::Count { affected } => {
            println!("{} row(s) affected.", affected);
        }
        QueryResult::Empty => {
            // For DDL operations, no output
        }
    }

    Ok(())
}
