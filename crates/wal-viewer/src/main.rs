use anyhow::{Context, Result};
use clap::{Parser, ValueEnum};
use common::{
    TableId,
    pretty::{self, TableStyleKind},
};
use serde::Serialize;
use std::path::PathBuf;
use tabled::Tabled;
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
            let rows: Vec<WalRow> = limited
                .iter()
                .map(|(idx, rec)| WalRow::from_record(*idx, rec))
                .collect();
            let style: TableStyleKind = args.style.into();
            println!("{}", pretty::render_structured_rows(&rows, style));
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

impl From<CliTableStyle> for TableStyleKind {
    fn from(value: CliTableStyle) -> Self {
        match value {
            CliTableStyle::Modern => TableStyleKind::Modern,
            CliTableStyle::Ascii => TableStyleKind::Ascii,
            CliTableStyle::Plain => TableStyleKind::Plain,
        }
    }
}

#[derive(Clone, Debug, Tabled)]
struct WalRow {
    #[tabled(rename = "Idx")]
    idx: usize,
    #[tabled(rename = "Op")]
    op: String,
    #[tabled(rename = "Table")]
    table: String,
    #[tabled(rename = "RID")]
    rid: String,
    #[tabled(rename = "Data")]
    data: String,
}

impl WalRow {
    fn from_record(idx: usize, record: &WalRecord) -> Self {
        match record {
            WalRecord::Insert { table, row, rid } => Self {
                idx,
                op: "INSERT".into(),
                table: format_table(table),
                rid: pretty::format_record_id(rid),
                data: pretty::format_row(row),
            },
            WalRecord::Update {
                table,
                rid,
                new_row,
            } => Self {
                idx,
                op: "UPDATE".into(),
                table: format_table(table),
                rid: pretty::format_record_id(rid),
                data: pretty::format_row(new_row),
            },
            WalRecord::Delete { table, rid } => Self {
                idx,
                op: "DELETE".into(),
                table: format_table(table),
                rid: pretty::format_record_id(rid),
                data: "-".into(),
            },
            WalRecord::CreateTable { name, table } => Self {
                idx,
                op: "CREATE".into(),
                table: format!("{} ({})", name, table.0),
                rid: "-".into(),
                data: "-".into(),
            },
            WalRecord::DropTable { table } => Self {
                idx,
                op: "DROP".into(),
                table: format_table(table),
                rid: "-".into(),
                data: "-".into(),
            },
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
