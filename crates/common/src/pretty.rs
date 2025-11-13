use crate::{RecordBatch, RecordId, Row};
use tabled::{Table, Tabled, builder::Builder, settings};
use types::Value;

/// Predefined output styles that map to `tabled` styles.
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub enum TableStyleKind {
    #[default]
    Modern,
    Ascii,
    Plain,
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
    match (batch.columns.is_empty(), batch.rows.is_empty()) {
        (true, true) => "<empty>".into(),
        (true, false) => render_value_only_rows(&batch.rows, style),
        (false, _) => {
            let mut builder = Builder::default();
            builder.push_record(batch.columns.iter().cloned());

            for row in &batch.rows {
                builder.push_record(row.values.iter().map(format_value));
            }

            let mut table = builder.build();
            style.apply(&mut table);
            table.to_string()
        }
    }
}

/// Render arbitrary string rows with the provided style.
pub fn render_string_table(
    headers: &[&str],
    rows: Vec<Vec<String>>,
    style: TableStyleKind,
) -> String {
    if headers.is_empty() && rows.is_empty() {
        return "<empty>".into();
    }

    let mut builder = Builder::default();

    if !headers.is_empty() {
        builder.push_record(headers.iter().copied());
    }

    for row in rows {
        builder.push_record(row);
    }

    let mut table = builder.build();
    style.apply(&mut table);
    table.to_string()
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

fn render_value_only_rows(rows: &[Row], style: TableStyleKind) -> String {
    #[derive(Clone, Tabled)]
    struct ValueOnlyRow {
        #[tabled(rename = "Values")]
        values: String,
    }

    if rows.is_empty() {
        return "<empty>".into();
    }

    let data: Vec<ValueOnlyRow> = rows
        .iter()
        .map(|row| ValueOnlyRow {
            values: format_row(&row.values),
        })
        .collect();

    render_structured_rows(&data, style)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_batch_with_columns_renders_headers() {
        let batch = RecordBatch {
            columns: vec!["id".into(), "name".into()],
            rows: vec![Row::new(vec![Value::Int(1), Value::Text("Ada".into())])],
        };

        let rendered = render_record_batch(&batch, TableStyleKind::Modern);
        assert!(rendered.contains("id"));
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
