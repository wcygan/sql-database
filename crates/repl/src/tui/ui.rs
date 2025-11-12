use super::app::App;
use ratatui::{
    Frame,
    layout::{Constraint, Direction, Layout},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Cell, Paragraph, Row, Table},
};
use types::Value;

pub fn render(f: &mut Frame, app: &mut App) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage(40), // Editor area (split into history + input)
            Constraint::Percentage(55), // Results
            Constraint::Length(3),      // Status bar
        ])
        .split(f.area());

    // Split editor area into history and input (with minimum heights)
    let editor_chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Min(5),      // Command history (takes remaining space)
            Constraint::Length(3),   // Current input (fixed 3 lines including border)
        ])
        .split(chunks[0]);

    // Render command history
    render_history(f, editor_chunks[0], app);

    // Render current input editor
    f.render_widget(&app.editor, editor_chunks[1]);

    // Render results
    if let Some(ref batch) = app.results {
        render_results(f, chunks[1], batch);
    } else {
        let empty = Paragraph::new("No results")
            .block(Block::default().borders(Borders::ALL).title("Results"));
        f.render_widget(empty, chunks[1]);
    }

    // Render status bar
    render_status(f, chunks[2], app);
}

fn render_history(f: &mut Frame, area: ratatui::layout::Rect, app: &App) {
    let history_text = if app.command_history.is_empty() {
        vec![Line::from("")]
    } else {
        app.command_history
            .iter()
            .map(|cmd| Line::from(cmd.as_str()))
            .collect()
    };

    let history = Paragraph::new(history_text)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title("Command History"),
        )
        .style(Style::default().fg(Color::DarkGray));

    f.render_widget(history, area);
}

fn render_results(f: &mut Frame, area: ratatui::layout::Rect, batch: &common::RecordBatch) {
    let header_cells = batch.columns.iter().map(|c| {
        Cell::from(c.as_str()).style(
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        )
    });
    let header = Row::new(header_cells).height(1).bottom_margin(1);

    let rows = batch.rows.iter().map(|row| {
        let cells = row.values.iter().map(|v| Cell::from(format_value(v)));
        Row::new(cells).height(1)
    });

    // Calculate column widths
    let col_count = batch.columns.len();
    let widths: Vec<Constraint> = if col_count > 0 {
        vec![Constraint::Percentage((100 / col_count as u16).max(10)); col_count]
    } else {
        vec![]
    };

    let table = Table::new(rows, widths).header(header).block(
        Block::default()
            .borders(Borders::ALL)
            .title(format!("Results ({} rows)", batch.rows.len())),
    );

    f.render_widget(table, area);
}

fn render_status(f: &mut Frame, area: ratatui::layout::Rect, app: &App) {
    let status_text = if let Some(ref msg) = app.status_message {
        if let Some(duration) = app.execution_time {
            format!("{} ({:?})", msg, duration)
        } else {
            msg.clone()
        }
    } else {
        "Ready".to_string()
    };

    let status_style = if app
        .status_message
        .as_ref()
        .is_some_and(|m| m.starts_with("Error"))
    {
        Style::default().fg(Color::Red)
    } else {
        Style::default().fg(Color::Green)
    };

    let status = Paragraph::new(Line::from(vec![
        Span::raw("Status: "),
        Span::styled(status_text, status_style),
        Span::raw(" | "),
        Span::raw(format!("Database: {}", app.db.data_dir.display())),
    ]))
    .block(Block::default().borders(Borders::ALL).title("Status"));

    f.render_widget(status, area);
}

fn format_value(value: &Value) -> String {
    match value {
        Value::Int(i) => i.to_string(),
        Value::Text(s) => format!("'{}'", s),
        Value::Bool(b) => b.to_string(),
        Value::Null => "NULL".to_string(),
    }
}
