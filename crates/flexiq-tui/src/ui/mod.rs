//! Rendering. `draw` is the single entry point; it lays out the tab bar, the
//! active view's body (optionally split with a detail pane), a footer, and any
//! modal overlay (confirm / help).

mod detail;
mod tables;

use ratatui::layout::{Alignment, Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Clear, Paragraph, Wrap};
use ratatui::Frame;

use flexiq_core::JobStatus;
use flexiq_workflows::{WorkflowNodeStatus, WorkflowState};

use crate::app::{App, InputMode};
use crate::source::View;

pub fn draw(f: &mut Frame, app: &App) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3), // tab bar
            Constraint::Min(3),    // body
            Constraint::Length(2), // footer (keys + notice)
        ])
        .split(f.area());

    render_tabs(f, chunks[0], app);
    render_body(f, chunks[1], app);
    render_footer(f, chunks[2], app);

    match &app.input {
        InputMode::Confirm(action) => render_confirm(f, &action.prompt()),
        InputMode::Help => render_help(f),
        InputMode::Normal => {}
    }
}

fn render_tabs(f: &mut Frame, area: Rect, app: &App) {
    // Rendered as manual spans (not the `Tabs` widget) so we can record the
    // exact screen span of each label for click handling.
    let block = Block::default().borders(Borders::ALL).title(" flexiq-tui ");
    let inner = block.inner(area);
    f.render_widget(block, area);

    let mut spans = Vec::new();
    let mut hits = Vec::new();
    let mut x = inner.x;
    for (i, v) in View::ALL.iter().enumerate() {
        if i > 0 {
            let div = " │ ";
            spans.push(Span::styled(div, Style::default().fg(Color::DarkGray)));
            x += div.chars().count() as u16;
        }
        let label = format!(" {}·{} ", i + 1, v.title());
        let w = label.chars().count() as u16;
        let style = if *v == app.view {
            Style::default()
                .fg(Color::Black)
                .bg(Color::Cyan)
                .add_modifier(Modifier::BOLD)
        } else {
            Style::default()
        };
        hits.push((x, x + w, *v));
        spans.push(Span::styled(label, style));
        x += w;
    }
    {
        let mut hit = app.hit.borrow_mut();
        hit.tabs = hits;
        hit.tabs_row = inner.y;
    }
    f.render_widget(Paragraph::new(Line::from(spans)), inner);
}

fn render_body(f: &mut Frame, area: Rect, app: &App) {
    let split_detail =
        app.detail_open && matches!(app.view, View::Jobs | View::Workflows | View::DeadLetters);

    if split_detail {
        let cols = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Percentage(55), Constraint::Percentage(45)])
            .split(area);
        render_view(f, cols[0], app);
        detail::render(f, cols[1], app);
    } else {
        render_view(f, area, app);
    }
}

fn render_view(f: &mut Frame, area: Rect, app: &App) {
    match app.view {
        View::Stats => tables::stats(f, area, app),
        View::Jobs => tables::jobs(f, area, app),
        View::DeadLetters => tables::dead(f, area, app),
        View::Workers => tables::workers(f, area, app),
        View::Workflows => tables::workflows(f, area, app),
    }
}

fn render_footer(f: &mut Frame, area: Rect, app: &App) {
    let keys = match app.view {
        View::Stats => "1-5/Tab view · j/k move · p pause/resume · r refresh · ? help · q quit",
        View::Jobs => {
            "j/k move · Enter detail · f filter · c cancel · R replay · r refresh · ? help · q quit"
        }
        View::DeadLetters => {
            "j/k move · Enter detail · T retry · d delete · P purge-all · ? help · q quit"
        }
        View::Workers => "1-5/Tab view · j/k move · r refresh · ? help · q quit",
        View::Workflows => "j/k move · Enter DAG · r refresh · ? help · q quit",
    };
    let notice = app
        .notice
        .as_ref()
        .map(|n| {
            Line::from(Span::styled(
                n.text.clone(),
                Style::default().fg(if n.error { Color::Red } else { Color::Green }),
            ))
        })
        .unwrap_or_else(|| Line::from(""));

    let para = Paragraph::new(vec![
        Line::from(Span::styled(keys, Style::default().fg(Color::DarkGray))),
        notice,
    ]);
    f.render_widget(para, area);
}

fn render_confirm(f: &mut Frame, prompt: &str) {
    let area = centered_rect(56, 22, f.area());
    f.render_widget(Clear, area);
    let block = Block::default()
        .borders(Borders::ALL)
        .title(" Confirm ")
        .border_style(Style::default().fg(Color::Yellow));
    let text = vec![
        Line::from(prompt.to_string()),
        Line::from(""),
        Line::from(Span::styled(
            "[y] yes    [n] no",
            Style::default().add_modifier(Modifier::BOLD),
        )),
    ];
    let para = Paragraph::new(text)
        .block(block)
        .alignment(Alignment::Center)
        .wrap(Wrap { trim: true });
    f.render_widget(para, area);
}

fn render_help(f: &mut Frame) {
    let area = centered_rect(64, 70, f.area());
    f.render_widget(Clear, area);
    let lines = vec![
        Line::from(Span::styled("Navigation", help_head())),
        Line::from("  1-5 / Tab / Shift-Tab   switch view"),
        Line::from("  j / k / ↓ / ↑           move selection"),
        Line::from("  g / G                   jump to top / bottom"),
        Line::from("  Enter                   open detail (Jobs, Dead, Workflows)"),
        Line::from("  Esc                     close detail"),
        Line::from("  r                       refresh now"),
        Line::from(""),
        Line::from(Span::styled("Actions", help_head())),
        Line::from("  Jobs:  c cancel · R replay · f cycle status filter"),
        Line::from("  Dead:  T retry · d delete · P purge all"),
        Line::from("  Stats: p pause / resume selected queue"),
        Line::from("  (every action asks for confirmation)"),
        Line::from(""),
        Line::from(Span::styled("Mouse", help_head())),
        Line::from("  click a tab to switch · click a row to select"),
        Line::from("  click the selected row to open detail · wheel scrolls"),
        Line::from(""),
        Line::from(Span::styled("General", help_head())),
        Line::from("  ? this help · q quit · Ctrl-C quit"),
        Line::from(""),
        Line::from(Span::styled(
            "Note: opening a SQLite DB runs migrations + an archive sweep.",
            Style::default().fg(Color::DarkGray),
        )),
    ];
    let para = Paragraph::new(lines).block(
        Block::default()
            .borders(Borders::ALL)
            .title(" Help ")
            .border_style(Style::default().fg(Color::Cyan)),
    );
    f.render_widget(para, area);
}

fn help_head() -> Style {
    Style::default()
        .fg(Color::Cyan)
        .add_modifier(Modifier::BOLD)
}

// ── shared style helpers (used across table/detail renderers) ────────────

pub(crate) fn job_status_style(status: JobStatus) -> Style {
    let color = match status {
        JobStatus::Pending => Color::Gray,
        JobStatus::Running => Color::Cyan,
        JobStatus::Complete => Color::Green,
        JobStatus::Failed => Color::Red,
        JobStatus::Dead => Color::Magenta,
        JobStatus::Cancelled => Color::DarkGray,
    };
    Style::default().fg(color)
}

pub(crate) fn wf_state_style(state: WorkflowState) -> Style {
    let color = match state {
        WorkflowState::Pending | WorkflowState::Paused => Color::Gray,
        WorkflowState::Running | WorkflowState::Compensating => Color::Cyan,
        WorkflowState::Completed => Color::Green,
        WorkflowState::CompletedWithFailures => Color::Yellow,
        WorkflowState::Failed | WorkflowState::CompensationFailed => Color::Red,
        WorkflowState::Cancelled => Color::DarkGray,
        WorkflowState::Compensated => Color::Magenta,
    };
    Style::default().fg(color)
}

pub(crate) fn wf_node_style(status: WorkflowNodeStatus) -> Style {
    let color = match status {
        WorkflowNodeStatus::Completed | WorkflowNodeStatus::CacheHit => Color::Green,
        WorkflowNodeStatus::Running | WorkflowNodeStatus::Compensating => Color::Cyan,
        WorkflowNodeStatus::Failed | WorkflowNodeStatus::CompensationFailed => Color::Red,
        WorkflowNodeStatus::Skipped => Color::DarkGray,
        WorkflowNodeStatus::Compensated => Color::Magenta,
        WorkflowNodeStatus::WaitingApproval => Color::Yellow,
        WorkflowNodeStatus::Pending => Color::Gray,
    };
    Style::default().fg(color)
}

/// A centered rect `pct_x` × `pct_y` percent of `area`.
fn centered_rect(pct_x: u16, pct_y: u16, area: Rect) -> Rect {
    let vert = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage((100 - pct_y) / 2),
            Constraint::Percentage(pct_y),
            Constraint::Percentage((100 - pct_y) / 2),
        ])
        .split(area);
    Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage((100 - pct_x) / 2),
            Constraint::Percentage(pct_x),
            Constraint::Percentage((100 - pct_x) / 2),
        ])
        .split(vert[1])[1]
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    use flexiq_core::QueueStats;
    use ratatui::backend::TestBackend;
    use ratatui::Terminal;

    use crate::source::{DeadRow, JobRow, WorkerView, WorkflowRunRow};

    fn buf_text(t: &Terminal<TestBackend>) -> String {
        t.backend()
            .buffer()
            .content()
            .iter()
            .map(|c| c.symbol())
            .collect()
    }

    fn populated() -> App {
        let mut app = App::new(Duration::from_millis(1000));
        app.stats.overall = QueueStats {
            pending: 1,
            running: 2,
            completed: 3,
            ..Default::default()
        };
        app.stats.per_queue = vec![("default".into(), QueueStats::default())];
        app.stats.paused = vec!["default".into()];
        app.jobs = vec![JobRow {
            id: "0192abcd-0000-7000-8000-000000000000".into(),
            task_name: "send_email".into(),
            queue: "default".into(),
            status: JobStatus::Running,
            priority: 0,
            retry_count: 1,
            max_retries: 3,
            created_at: 0,
            scheduled_at: 0,
            started_at: Some(0),
            completed_at: None,
            error: None,
            cancel_requested: false,
        }];
        app.dead = vec![DeadRow {
            id: "dead0001".into(),
            original_job_id: "job0001".into(),
            task_name: "resize".into(),
            queue: "media".into(),
            error: Some("boom".into()),
            retry_count: 3,
            dlq_retry_count: 0,
            failed_at: 0,
        }];
        app.workers = vec![WorkerView {
            worker_id: "worker-a".into(),
            status: "active".into(),
            queues: "default".into(),
            threads: 4,
            last_heartbeat: 0,
            hostname: Some("host1".into()),
            pid: Some(42),
        }];
        app.runs = vec![WorkflowRunRow {
            id: "run00001".into(),
            definition_id: "etl".into(),
            state: WorkflowState::Running,
            created_at: 0,
            started_at: Some(0),
            completed_at: None,
            error: None,
            parent_run_id: None,
        }];
        app
    }

    fn render(app: &App) -> Terminal<TestBackend> {
        let mut terminal = Terminal::new(TestBackend::new(120, 30)).unwrap();
        terminal.draw(|f| draw(f, app)).unwrap();
        terminal
    }

    #[test]
    fn every_view_renders_without_panic() {
        let mut app = populated();
        for view in View::ALL {
            app.view = view;
            let t = render(&app);
            let text = buf_text(&t);
            // Tab bar always present.
            assert!(text.contains("flexiq-tui"));
        }
    }

    #[test]
    fn view_specific_content_shows() {
        let mut app = populated();

        app.view = View::Jobs;
        assert!(buf_text(&render(&app)).contains("STATUS"));

        app.view = View::DeadLetters;
        assert!(buf_text(&render(&app)).contains("Dead Letters"));

        app.view = View::Workers;
        assert!(buf_text(&render(&app)).contains("HEARTBEAT"));

        app.view = View::Workflows;
        assert!(buf_text(&render(&app)).contains("DEFINITION"));

        app.view = View::Stats;
        assert!(buf_text(&render(&app)).contains("Totals"));
    }

    #[test]
    fn detail_and_overlays_render() {
        let mut app = populated();
        app.view = View::Jobs;
        app.detail_open = true;
        app.loading_detail = true;
        assert!(buf_text(&render(&app)).contains("loading"));

        app.detail_open = false;
        app.input = InputMode::Help;
        assert!(buf_text(&render(&app)).contains("Navigation"));

        app.input = InputMode::Confirm(crate::event::PendingAction::PurgeDead);
        assert!(buf_text(&render(&app)).contains("Confirm"));
    }
}
