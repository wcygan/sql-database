mod database;
mod tui;

use anyhow::Result;
use clap::Parser;
use database::DatabaseState;
use planner::PhysicalPlan;
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

fn main() -> Result<()> {
    let args = Args::parse();

    let db = DatabaseState::new(
        &args.data_dir,
        &args.catalog_file,
        &args.wal_file,
        args.buffer_pages,
    )?;

    if let Some(sql) = args.execute {
        // Execute mode: run SQL and exit without TUI
        execute_and_exit(db, &sql)?;
    } else {
        // TUI mode: interactive terminal UI
        let app = tui::App::new(db);
        tui::run(app)?;
    }

    Ok(())
}

fn execute_and_exit(mut db: DatabaseState, sql: &str) -> Result<()> {
    use catalog::{Column, IndexKind};
    use common::{
        RecordBatch,
        pretty::{self, TableStyleKind},
    };
    use executor::{execute_dml, execute_query};
    use parser::{Statement, parse_sql};
    use planner::{PhysicalPlan, Planner, PlanningContext};
    use wal::WalRecord;

    let statements = parse_sql(sql).map_err(anyhow::Error::from)?;

    for stmt in statements {
        match stmt {
            Statement::CreateTable {
                name,
                columns,
                primary_key,
            } => {
                let catalog_columns: Vec<Column> = columns
                    .iter()
                    .map(|col| {
                        let ty = map_sql_type(&col.ty)?;
                        Ok(Column::new(col.name.clone(), ty))
                    })
                    .collect::<Result<Vec<_>>>()?;

                let primary_key_ordinals = if let Some(pk_names) = primary_key {
                    let mut ordinals = Vec::new();
                    for pk_name in &pk_names {
                        let ordinal = columns
                            .iter()
                            .position(|col| col.name.eq_ignore_ascii_case(pk_name))
                            .ok_or_else(|| {
                                anyhow::anyhow!(
                                    "PRIMARY KEY column '{}' not found in table columns",
                                    pk_name
                                )
                            })? as u16;
                        ordinals.push(ordinal);
                    }
                    Some(ordinals)
                } else {
                    None
                };

                let table_id = db
                    .catalog
                    .create_table(&name, catalog_columns, primary_key_ordinals)
                    .map_err(anyhow::Error::from)?;
                db.persist_catalog()?;
                db.log_wal(WalRecord::CreateTable {
                    name: name.clone(),
                    table: table_id,
                })?;
                println!("Created table '{}' (id = {}).", name, table_id.0);
            }
            Statement::DropTable { name } => {
                let table_id = db.catalog.table(&name).map_err(anyhow::Error::from)?.id;
                db.catalog.drop_table(&name).map_err(anyhow::Error::from)?;
                db.persist_catalog()?;
                db.remove_heap_file(&name)?;
                db.log_wal(WalRecord::DropTable { table: table_id })?;
                println!("Dropped table '{}'.", name);
            }
            Statement::CreateIndex {
                name,
                table,
                column,
            } => {
                db.catalog
                    .create_index()
                    .table_name(&table)
                    .index_name(&name)
                    .columns(&[column.as_str()])
                    .kind(IndexKind::BTree)
                    .call()
                    .map_err(anyhow::Error::from)?;
                db.persist_catalog()?;
                println!("Created index '{}' on '{}'.", name, table);
            }
            Statement::DropIndex { name } => {
                let table_name = db
                    .catalog
                    .tables()
                    .find(|table| table.index(&name).is_ok())
                    .map(|table| table.name.clone())
                    .ok_or_else(|| anyhow::anyhow!("index '{}' not found", name))?;

                db.catalog
                    .drop_index(&table_name, &name)
                    .map_err(anyhow::Error::from)?;
                db.persist_catalog()?;
                println!("Dropped index '{}' on '{}'.", name, table_name);
            }
            Statement::Explain { query, analyze } => {
                let mut planning_ctx = PlanningContext::new(&db.catalog);
                let plan = Planner::plan(*query, &mut planning_ctx).map_err(anyhow::Error::from)?;

                if analyze {
                    // EXPLAIN ANALYZE: Execute the query and show statistics
                    let plan_description = planner::explain_physical(&plan);

                    db.with_execution_context(|ctx| {
                        let mut executor = executor::build_executor(plan)?;
                        executor.open(ctx)?;

                        // Consume all rows to collect statistics
                        let mut row_count = 0;
                        while executor.next(ctx)?.is_some() {
                            row_count += 1;
                        }
                        executor.close(ctx)?;

                        // Display plan with statistics
                        println!("EXPLAIN ANALYZE:");
                        println!("{}", plan_description);
                        println!();
                        println!("Execution Statistics:");
                        println!("{}", executor::format_explain_analyze(executor.as_ref(), "Query"));
                        println!("Total rows: {}", row_count);

                        Ok::<(), common::DbError>(())
                    })?;
                } else {
                    // EXPLAIN: Just show the plan without executing
                    println!("EXPLAIN:");
                    println!("{}", planner::explain_physical(&plan));
                }
            }
            other => {
                let mut planning_ctx = PlanningContext::new(&db.catalog);
                let plan = Planner::plan(other, &mut planning_ctx).map_err(anyhow::Error::from)?;

                match plan {
                    PhysicalPlan::Insert { .. }
                    | PhysicalPlan::Update { .. }
                    | PhysicalPlan::Delete { .. } => {
                        let count = db.with_execution_context(|ctx| execute_dml(plan, ctx))?;
                        println!("{} row(s) affected.", count);
                    }
                    ref query_plan => {
                        let schema = infer_schema(query_plan);
                        let rows = db.with_execution_context(|ctx| execute_query(plan, ctx))?;
                        let batch = RecordBatch {
                            columns: schema,
                            rows,
                        };
                        let rendered = pretty::render_record_batch(&batch, TableStyleKind::Modern);
                        println!("{}", rendered);
                    }
                }
            }
        }
    }

    Ok(())
}

fn map_sql_type(raw: &str) -> Result<types::SqlType> {
    match raw.trim().to_uppercase().as_str() {
        "INT" | "INTEGER" => Ok(types::SqlType::Int),
        "TEXT" | "STRING" | "VARCHAR" => Ok(types::SqlType::Text),
        "BOOL" | "BOOLEAN" => Ok(types::SqlType::Bool),
        other => Err(anyhow::anyhow!("unsupported SQL type '{}'", other)),
    }
}

fn infer_schema(plan: &PhysicalPlan) -> Vec<String> {
    match plan {
        PhysicalPlan::SeqScan { schema, .. } => schema.clone(),
        PhysicalPlan::IndexScan { schema, .. } => schema.clone(),
        PhysicalPlan::Filter { input, .. } => infer_schema(input),
        PhysicalPlan::Project { columns, .. } => {
            columns.iter().map(|(name, _)| name.clone()).collect()
        }
        PhysicalPlan::Insert { .. } | PhysicalPlan::Update { .. } | PhysicalPlan::Delete { .. } => {
            vec![]
        }
    }
}
