use crate::{RecordBatch, RecordId, Row};
use tabled::{Table, Tabled, settings};
use types::Value;

/// Predefined output styles that map to `tabled` styles.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum TableStyleKind {
    Modern,
    Ascii,
    Plain,
}

impl Default for TableStyleKind {
    fn default() -> Self {
        Self::Modern
    }
}

impl TableStyleKind {
    fn apply(self, table: &mut Table) {
        match self {
            Self::Modern => table.with(settings::Style::modern()),
            Self::Ascii => table.with(settings::Style::ascii()),
            Self::Plain => table.with(settings::Style::empty()),
        };
    }
}

/// Render a `RecordBatch` into a human-friendly table string.
pub fn render_record_batch(batch: &RecordBatch, style: TableStyleKind) -> String {
    if batch.columns.is_empty() && batch.rows.is_empty() {
        return "<empty>".into();
    }

    #[derive(Clone, Tabled)]
    struct PrintableRow {
        #[tabled(rename = "Values")]
        values: String,
    }

    let mut rows: Vec<PrintableRow> = Vec::new();
    if !batch.columns.is_empty() {
        rows.push(PrintableRow {
            values: format!("Columns: {}", batch.columns.join(", ")),
        });
    }

    rows.extend(batch.rows.iter().map(|Row(values)| PrintableRow {
        values: format_row(values),
    }));

    render_structured_rows(&rows, style)
}

/// Render any `Tabled` rows with the provided style.
pub fn render_structured_rows<T>(rows: &[T], style: TableStyleKind) -> String
where
    T: Tabled + Clone,
{
    if rows.is_empty() {
        return "<empty>".into();
    }

    let mut table = Table::new(rows.to_vec());
    style.apply(&mut table);
    table.to_string()
}

/// Format a full row into a comma-separated string.
pub fn format_row(values: &[Value]) -> String {
    values
        .iter()
        .map(format_value)
        .collect::<Vec<_>>()
        .join(", ")
}

/// Format a single value for display.
pub fn format_value(value: &Value) -> String {
    match value {
        Value::Int(v) => v.to_string(),
        Value::Text(text) => format!("'{}'", text),
        Value::Bool(b) => b.to_string(),
        Value::Null => "NULL".into(),
    }
}

/// Format a `RecordId` as `(page_id, slot)`.
pub fn format_record_id(rid: &RecordId) -> String {
    format!("({}, {})", rid.page_id.0, rid.slot)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_batch_with_columns_renders_metadata_row() {
        let batch = RecordBatch {
            columns: vec!["id".into(), "name".into()],
            rows: vec![Row(vec![Value::Int(1), Value::Text("Ada".into())])],
        };

        let rendered = render_record_batch(&batch, TableStyleKind::Modern);
        assert!(rendered.contains("Columns: id, name"));
        assert!(rendered.contains("'Ada'"));
    }

    #[test]
    fn empty_batches_render_placeholder() {
        let batch = RecordBatch {
            columns: vec![],
            rows: vec![],
        };

        assert_eq!(
            render_record_batch(&batch, TableStyleKind::Plain),
            "<empty>"
        );
    }
}
