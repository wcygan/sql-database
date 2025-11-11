use super::*;
use std::io;

#[test]
fn config_defaults_are_sane() {
    let cfg = Config::default();
    assert_eq!(cfg.page_size, 4096);
    assert_eq!(cfg.buffer_pool_pages, 256);
    assert!(cfg.wal_enabled);
}

#[test]
fn db_error_formats_cleanly() {
    let err = DbError::Storage("corruption".into());
    assert!(format!("{err}").contains("storage"));
}

#[test]
fn recordbatch_consistency() {
    let rb = RecordBatch {
        columns: vec!["id".into()],
        rows: vec![Row(vec![Value::Int(1)])],
    };
    assert_eq!(rb.columns.len(), 1);
    assert_eq!(rb.rows[0].0.len(), 1);
}

#[test]
fn io_error_converts() {
    let e = io::Error::other("oops");
    let db_err: DbError = e.into();
    assert!(matches!(db_err, DbError::Io(_)));
}
