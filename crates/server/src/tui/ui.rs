//! TUI rendering functions using Ratatui.

use super::app::{ActivityKind, TuiState};
use ratatui::{
    Frame,
    layout::{Constraint, Direction, Layout},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Paragraph, Wrap},
};

/// Render the entire TUI.
pub fn render(f: &mut Frame, state: &TuiState) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .margin(1)
        .constraints([
            Constraint::Length(5), // Cluster status
            Constraint::Length(3), // Connections
            Constraint::Min(5),    // Activity log (fills remaining)
            Constraint::Length(1), // Help
        ])
        .split(f.area());

    // Main border
    let main_block = Block::default()
        .title(" ToyDB Server ")
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Cyan));
    f.render_widget(main_block, f.area());

    render_cluster_status(f, chunks[0], state);
    render_connections(f, chunks[1], state);
    render_activity_log(f, chunks[2], state);
    render_help(f, chunks[3]);
}

/// Render the cluster status panel.
fn render_cluster_status(f: &mut Frame, area: ratatui::layout::Rect, state: &TuiState) {
    let block = Block::default()
        .title(" Cluster Status ")
        .borders(Borders::ALL);

    let uptime = state.start_time.elapsed();
    let uptime_str = format!(
        "{:02}:{:02}:{:02}",
        uptime.as_secs() / 3600,
        (uptime.as_secs() % 3600) / 60,
        uptime.as_secs() % 60
    );

    let state_color = match state.raft_metrics.state.as_str() {
        "Leader" => Color::Green,
        "Follower" => Color::Yellow,
        "Candidate" => Color::Magenta,
        _ => Color::White,
    };

    let state_display = if state.raft_metrics.state == "Leader" {
        format!("{} \u{2713}", state.raft_metrics.state) // ✓
    } else {
        state.raft_metrics.state.clone()
    };

    let lines = if state.raft_enabled {
        vec![
            Line::from(vec![
                Span::raw("Node ID: "),
                Span::styled(
                    state.raft_metrics.node_id.to_string(),
                    Style::default().fg(Color::Cyan),
                ),
                Span::raw("          State: "),
                Span::styled(
                    state_display,
                    Style::default()
                        .fg(state_color)
                        .add_modifier(Modifier::BOLD),
                ),
                Span::raw("        Uptime: "),
                Span::raw(&uptime_str),
            ]),
            Line::from(vec![
                Span::raw("Leader: "),
                Span::styled(
                    state
                        .raft_metrics
                        .current_leader
                        .map(|l| format!("node {}", l))
                        .unwrap_or_else(|| "none".into()),
                    Style::default().fg(Color::Yellow),
                ),
                Span::raw("        Term: "),
                Span::raw(state.raft_metrics.current_term.to_string()),
                Span::raw("            Commit: "),
                Span::raw(state.raft_metrics.commit_index.to_string()),
            ]),
            Line::from(vec![
                Span::raw("Client: "),
                Span::styled(&state.client_addr, Style::default().fg(Color::Cyan)),
                Span::raw("    Raft: "),
                Span::styled(
                    state.raft_addr.as_deref().unwrap_or("N/A"),
                    Style::default().fg(Color::Cyan),
                ),
            ]),
        ]
    } else {
        vec![
            Line::from(vec![
                Span::raw("Mode: "),
                Span::styled("Standalone (no Raft)", Style::default().fg(Color::Yellow)),
                Span::raw("          Uptime: "),
                Span::raw(&uptime_str),
            ]),
            Line::from(vec![
                Span::raw("Client: "),
                Span::styled(&state.client_addr, Style::default().fg(Color::Cyan)),
            ]),
        ]
    };

    let paragraph = Paragraph::new(lines).block(block);
    f.render_widget(paragraph, area);
}

/// Render the connections panel.
fn render_connections(f: &mut Frame, area: ratatui::layout::Rect, state: &TuiState) {
    let block = Block::default()
        .title(" Connections ")
        .borders(Borders::ALL);

    let line = Line::from(vec![
        Span::raw("Active: "),
        Span::styled(
            state.connection_count.to_string(),
            Style::default()
                .fg(Color::Green)
                .add_modifier(Modifier::BOLD),
        ),
        Span::raw("           Total Queries: "),
        Span::styled(
            state.total_queries.to_string(),
            Style::default().fg(Color::Cyan),
        ),
    ]);

    let paragraph = Paragraph::new(line).block(block);
    f.render_widget(paragraph, area);
}

/// Render the activity log panel.
fn render_activity_log(f: &mut Frame, area: ratatui::layout::Rect, state: &TuiState) {
    let block = Block::default()
        .title(" Activity Log ")
        .borders(Borders::ALL);

    let visible_lines = (area.height as usize).saturating_sub(2);

    let lines: Vec<Line> = state
        .activity_log
        .iter()
        .rev() // Most recent first for selection
        .take(visible_lines)
        .collect::<Vec<_>>()
        .into_iter()
        .rev() // Back to chronological for display
        .map(|entry| {
            let elapsed = entry.timestamp.duration_since(state.start_time);
            let timestamp = format!(
                "[{:02}:{:02}:{:02}]",
                elapsed.as_secs() / 3600,
                (elapsed.as_secs() % 3600) / 60,
                elapsed.as_secs() % 60
            );

            let color = match entry.kind {
                ActivityKind::Connection => Color::Green,
                ActivityKind::Disconnection => Color::Yellow,
                ActivityKind::Query => Color::Cyan,
                ActivityKind::Error => Color::Red,
                ActivityKind::Raft => Color::Magenta,
            };

            Line::from(vec![
                Span::styled(timestamp, Style::default().fg(Color::DarkGray)),
                Span::raw(" "),
                Span::styled(&entry.message, Style::default().fg(color)),
            ])
        })
        .collect();

    let paragraph = Paragraph::new(lines).block(block).wrap(Wrap { trim: true });
    f.render_widget(paragraph, area);
}

/// Render the help footer.
fn render_help(f: &mut Frame, area: ratatui::layout::Rect) {
    let line = Line::from(vec![
        Span::raw(" Press "),
        Span::styled(
            "q",
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        ),
        Span::raw(" or "),
        Span::styled(
            "Ctrl+C",
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        ),
        Span::raw(" to quit"),
    ]);

    let paragraph = Paragraph::new(line);
    f.render_widget(paragraph, area);
}
