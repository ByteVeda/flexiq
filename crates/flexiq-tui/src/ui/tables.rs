//! The five per-view tables.

use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Cell, Paragraph, Row, Table, TableState};
use ratatui::Frame;

use super::{job_status_style, wf_state_style};
use crate::app::App;
use crate::util::{fmt_age, fmt_age_opt, truncate};

fn header(cells: &[&str]) -> Row<'static> {
    Row::new(
        cells
            .iter()
            .map(|c| Cell::from(c.to_string()))
            .collect::<Vec<_>>(),
    )
    .style(Style::default().add_modifier(Modifier::BOLD))
}

fn state(selected: usize) -> TableState {
    TableState::default().with_selected(Some(selected))
}

fn block(title: String) -> Block<'static> {
    Block::default().borders(Borders::ALL).title(title)
}

fn render_table(f: &mut Frame, area: Rect, table: Table, app: &App) {
    // Record the data-row rect (inside the border, below the header) so mouse
    // clicks map to a row: top border + header = 2 rows above the first row.
    app.hit.borrow_mut().rows = Some(Rect {
        x: area.x + 1,
        y: area.y + 2,
        width: area.width.saturating_sub(2),
        height: area.height.saturating_sub(3),
    });
    f.render_stateful_widget(
        table
            .row_highlight_style(Style::default().add_modifier(Modifier::REVERSED))
            .highlight_symbol("▌"),
        area,
        &mut state(app.selected),
    );
}

pub fn jobs(f: &mut Frame, area: Rect, app: &App) {
    let filter = app.job_filter.map(|s| s.as_str()).unwrap_or("all");
    let title = format!(" Jobs [{}]  ({}) ", filter, app.jobs.len());

    let rows = app.jobs.iter().map(|j| {
        Row::new(vec![
            Cell::from(short(&j.id)),
            Cell::from(Span::styled(j.status.as_str(), job_status_style(j.status))),
            Cell::from(truncate(&j.task_name, 22)),
            Cell::from(truncate(&j.queue, 12)),
            Cell::from(format!("{}/{}", j.retry_count, j.max_retries)),
            Cell::from(fmt_age(j.created_at)),
            Cell::from(truncate(j.error.as_deref().unwrap_or("-"), 40)),
        ])
    });
    let widths = [
        Constraint::Length(8),
        Constraint::Length(9),
        Constraint::Length(22),
        Constraint::Length(12),
        Constraint::Length(6),
        Constraint::Length(6),
        Constraint::Min(10),
    ];
    let table = Table::new(rows, widths)
        .header(header(&[
            "ID", "STATUS", "TASK", "QUEUE", "TRY", "AGE", "ERROR",
        ]))
        .block(block(title));
    render_table(f, area, table, app);
}

pub fn dead(f: &mut Frame, area: Rect, app: &App) {
    let title = format!(" Dead Letters  ({}) ", app.dead.len());
    let rows = app.dead.iter().map(|d| {
        Row::new(vec![
            Cell::from(short(&d.id)),
            Cell::from(truncate(&d.task_name, 22)),
            Cell::from(truncate(&d.queue, 12)),
            Cell::from(format!("{}", d.retry_count)),
            Cell::from(format!("{}", d.dlq_retry_count)),
            Cell::from(fmt_age(d.failed_at)),
            Cell::from(truncate(d.error.as_deref().unwrap_or("-"), 40)),
        ])
    });
    let widths = [
        Constraint::Length(8),
        Constraint::Length(22),
        Constraint::Length(12),
        Constraint::Length(4),
        Constraint::Length(4),
        Constraint::Length(6),
        Constraint::Min(10),
    ];
    let table = Table::new(rows, widths)
        .header(header(&[
            "ID", "TASK", "QUEUE", "TRY", "DLQ", "AGE", "ERROR",
        ]))
        .block(block(title));
    render_table(f, area, table, app);
}

pub fn workers(f: &mut Frame, area: Rect, app: &App) {
    let title = format!(" Workers  ({}) ", app.workers.len());
    let rows = app.workers.iter().map(|w| {
        Row::new(vec![
            Cell::from(short(&w.worker_id)),
            Cell::from(truncate(&w.status, 10)),
            Cell::from(truncate(&w.queues, 20)),
            Cell::from(format!("{}", w.threads)),
            Cell::from(fmt_age(w.last_heartbeat)),
            Cell::from(truncate(w.hostname.as_deref().unwrap_or("-"), 18)),
            Cell::from(w.pid.map(|p| p.to_string()).unwrap_or_else(|| "-".into())),
        ])
    });
    let widths = [
        Constraint::Length(10),
        Constraint::Length(10),
        Constraint::Length(20),
        Constraint::Length(4),
        Constraint::Length(10),
        Constraint::Length(18),
        Constraint::Min(6),
    ];
    let table = Table::new(rows, widths)
        .header(header(&[
            "WORKER",
            "STATUS",
            "QUEUES",
            "THR",
            "HEARTBEAT",
            "HOST",
            "PID",
        ]))
        .block(block(title));
    render_table(f, area, table, app);
}

pub fn workflows(f: &mut Frame, area: Rect, app: &App) {
    let title = format!(" Workflow Runs  ({}) ", app.runs.len());
    let rows = app.runs.iter().map(|r| {
        Row::new(vec![
            Cell::from(short(&r.id)),
            Cell::from(truncate(&r.definition_id, 16)),
            Cell::from(Span::styled(r.state.as_str(), wf_state_style(r.state))),
            Cell::from(fmt_age(r.created_at)),
            Cell::from(fmt_age_opt(r.completed_at)),
            Cell::from(truncate(r.error.as_deref().unwrap_or("-"), 34)),
        ])
    });
    let widths = [
        Constraint::Length(8),
        Constraint::Length(16),
        Constraint::Length(22),
        Constraint::Length(7),
        Constraint::Length(9),
        Constraint::Min(10),
    ];
    let table = Table::new(rows, widths)
        .header(header(&[
            "ID",
            "DEFINITION",
            "STATE",
            "AGE",
            "DONE",
            "ERROR",
        ]))
        .block(block(title));
    render_table(f, area, table, app);
}

pub fn stats(f: &mut Frame, area: Rect, app: &App) {
    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(4), Constraint::Min(3)])
        .split(area);

    // Overall summary line.
    let o = &app.stats.overall;
    let summary = Paragraph::new(vec![
        Line::from(Span::styled(
            "Totals across all queues",
            Style::default().add_modifier(Modifier::BOLD),
        )),
        Line::from(format!(
            "pending {}   running {}   complete {}   failed {}   dead {}   cancelled {}",
            o.pending, o.running, o.completed, o.failed, o.dead, o.cancelled
        )),
    ])
    .block(block(" Overview ".to_string()));
    f.render_widget(summary, rows[0]);

    // Per-queue table (selectable; Stats actions target the selected queue).
    let title = format!(" Queues  ({}) ", app.stats.per_queue.len());
    let table_rows = app.stats.per_queue.iter().map(|(name, s)| {
        let paused = if app.is_paused(name) { "paused" } else { "" };
        Row::new(vec![
            Cell::from(truncate(name, 20)),
            Cell::from(Span::styled(
                paused.to_string(),
                Style::default().fg(ratatui::style::Color::Yellow),
            )),
            Cell::from(format!("{}", s.pending)),
            Cell::from(format!("{}", s.running)),
            Cell::from(format!("{}", s.completed)),
            Cell::from(format!("{}", s.failed)),
            Cell::from(format!("{}", s.dead)),
            Cell::from(format!("{}", s.cancelled)),
        ])
    });
    let widths = [
        Constraint::Length(20),
        Constraint::Length(7),
        Constraint::Length(8),
        Constraint::Length(8),
        Constraint::Length(9),
        Constraint::Length(7),
        Constraint::Length(6),
        Constraint::Length(10),
    ];
    let table = Table::new(table_rows, widths)
        .header(header(&[
            "QUEUE", "STATE", "PEND", "RUN", "DONE", "FAIL", "DEAD", "CANCEL",
        ]))
        .block(block(title));
    render_table(f, rows[1], table, app);
}

fn short(id: &str) -> String {
    id.chars().take(8).collect()
}
