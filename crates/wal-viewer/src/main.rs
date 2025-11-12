use anyhow::{Context, Result};
use clap::{Parser, ValueEnum};
use common::{
    TableId,
    pretty::{self, TableStyleKind},
};
use serde::Serialize;
use std::path::PathBuf;
use wal::{Wal, WalRecord};

fn main() {
    if let Err(err) = run() {
        eprintln!("error: {err}");
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    let args = Args::parse();

    let records = Wal::replay(&args.wal_path)
        .with_context(|| format!("failed to read WAL at {}", args.wal_path.display()))?;

    let filtered: Vec<(usize, WalRecord)> = records
        .into_iter()
        .enumerate()
        .filter(|(_, rec)| matches_table(args.table, rec))
        .collect();

    let limited: Vec<(usize, WalRecord)> = filtered
        .into_iter()
        .skip(args.offset)
        .take(args.limit.unwrap_or(usize::MAX))
        .collect();

    if limited.is_empty() {
        println!("No matching WAL records found.");
        return Ok(());
    }

    match args.format {
        OutputFormat::Table => {
            let style: TableStyleKind = args.style.into();
            println!("{}", render_wal_records(&limited, style));
        }
        OutputFormat::Json => {
            let json_rows: Vec<JsonWalRecord<'_>> = limited
                .iter()
                .map(|(idx, rec)| JsonWalRecord {
                    idx: *idx,
                    record: rec,
                })
                .collect();
            println!("{}", serde_json::to_string_pretty(&json_rows)?);
        }
    }

    Ok(())
}

fn matches_table(filter: Option<u64>, record: &WalRecord) -> bool {
    match filter {
        None => true,
        Some(tid) => match record {
            WalRecord::Insert { table, .. }
            | WalRecord::Update { table, .. }
            | WalRecord::Delete { table, .. }
            | WalRecord::CreateTable { table, .. }
            | WalRecord::DropTable { table, .. } => table.0 == tid,
        },
    }
}

#[derive(Parser, Debug)]
#[command(name = "wal-viewer")]
#[command(about = "Inspect and pretty-print WAL entries", long_about = None)]
struct Args {
    /// Path to the WAL file to inspect
    wal_path: PathBuf,
    /// Filter records by table id
    #[arg(short, long)]
    table: Option<u64>,
    /// Output format (table or json)
    #[arg(long, value_enum, default_value_t = OutputFormat::Table)]
    format: OutputFormat,
    /// Style used for table rendering
    #[arg(long, value_enum, default_value_t = CliTableStyle::Modern)]
    style: CliTableStyle,
    /// Maximum number of records to display
    #[arg(long)]
    limit: Option<usize>,
    /// Number of matching records to skip before printing
    #[arg(long, default_value_t = 0)]
    offset: usize,
}

#[derive(Copy, Clone, Debug, ValueEnum)]
enum OutputFormat {
    Table,
    Json,
}

#[derive(Copy, Clone, Debug, ValueEnum)]
enum CliTableStyle {
    Modern,
    Ascii,
    Plain,
}

const WAL_HEADERS: [&str; 5] = ["Idx", "Op", "Table", "RID", "Data"];

impl From<CliTableStyle> for TableStyleKind {
    fn from(value: CliTableStyle) -> Self {
        match value {
            CliTableStyle::Modern => TableStyleKind::Modern,
            CliTableStyle::Ascii => TableStyleKind::Ascii,
            CliTableStyle::Plain => TableStyleKind::Plain,
        }
    }
}

fn format_table(table: &TableId) -> String {
    table.0.to_string()
}

#[derive(Serialize)]
struct JsonWalRecord<'a> {
    idx: usize,
    record: &'a WalRecord,
}

fn render_wal_records(records: &[(usize, WalRecord)], style: TableStyleKind) -> String {
    if records.is_empty() {
        return "<empty>".into();
    }

    let rows = records
        .iter()
        .map(|(idx, record)| wal_record_to_cells(*idx, record))
        .collect();

    pretty::render_string_table(&WAL_HEADERS, rows, style)
}

fn wal_record_to_cells(idx: usize, record: &WalRecord) -> Vec<String> {
    let (op, table, rid, data) = match record {
        WalRecord::Insert { table, row, rid } => (
            "INSERT".into(),
            format_table(table),
            pretty::format_record_id(rid),
            pretty::format_row(row),
        ),
        WalRecord::Update {
            table,
            rid,
            new_row,
        } => (
            "UPDATE".into(),
            format_table(table),
            pretty::format_record_id(rid),
            pretty::format_row(new_row),
        ),
        WalRecord::Delete { table, rid } => (
            "DELETE".into(),
            format_table(table),
            pretty::format_record_id(rid),
            "-".into(),
        ),
        WalRecord::CreateTable { name, table } => (
            "CREATE".into(),
            format!("{} ({})", name, table.0),
            "-".into(),
            "-".into(),
        ),
        WalRecord::DropTable { table } => {
            ("DROP".into(), format_table(table), "-".into(), "-".into())
        }
    };

    vec![idx.to_string(), op, table, rid, data]
}
