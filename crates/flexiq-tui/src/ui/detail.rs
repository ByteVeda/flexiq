//! Detail pane for the Jobs, Workflows, and Dead-Letters views.

use std::collections::BTreeMap;

use ratatui::layout::Rect;
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Paragraph, Wrap};
use ratatui::Frame;

use super::{job_status_style, wf_node_style};
use crate::app::App;
use crate::source::{JobDetail, View};
use crate::util::{fmt_age, fmt_age_opt};

pub fn render(f: &mut Frame, area: Rect, app: &App) {
    let (title, lines) = match app.view {
        View::Jobs => (" Job ", job_lines(app)),
        View::Workflows => (" Workflow Nodes ", workflow_lines(app)),
        View::DeadLetters => (" Dead Letter ", dead_lines(app)),
        _ => (" Detail ", vec![Line::from("")]),
    };
    let para = Paragraph::new(lines)
        .block(Block::default().borders(Borders::ALL).title(title))
        .wrap(Wrap { trim: false })
        .scroll((app.detail_scroll, 0));
    f.render_widget(para, area);
}

fn field(label: &str, value: String) -> Line<'static> {
    Line::from(vec![
        Span::styled(
            format!("{label:<11}"),
            Style::default().add_modifier(Modifier::BOLD),
        ),
        Span::raw(value),
    ])
}

fn section(title: &str) -> Line<'static> {
    Line::from(Span::styled(
        title.to_string(),
        Style::default()
            .fg(ratatui::style::Color::Cyan)
            .add_modifier(Modifier::BOLD),
    ))
}

fn job_lines(app: &App) -> Vec<Line<'static>> {
    if app.loading_detail {
        return vec![Line::from("loading…")];
    }
    let Some(d) = &app.job_detail else {
        return vec![Line::from("job not found")];
    };
    let JobDetail {
        row,
        metadata,
        notes,
        namespace,
        errors,
        logs,
    } = d;

    let mut lines = vec![
        field("id", row.id.clone()),
        Line::from(vec![
            Span::styled("status     ", Style::default().add_modifier(Modifier::BOLD)),
            Span::styled(row.status.as_str(), job_status_style(row.status)),
            Span::raw(if row.cancel_requested {
                "  (cancel requested)"
            } else {
                ""
            }),
        ]),
        field("task", row.task_name.clone()),
        field("queue", row.queue.clone()),
        field("priority", row.priority.to_string()),
        field(
            "retries",
            format!("{}/{}", row.retry_count, row.max_retries),
        ),
        field("created", fmt_age(row.created_at)),
        field("scheduled", fmt_age(row.scheduled_at)),
        field("started", fmt_age_opt(row.started_at)),
        field("completed", fmt_age_opt(row.completed_at)),
        field("namespace", namespace.clone().unwrap_or_else(|| "-".into())),
    ];
    if let Some(m) = metadata {
        lines.push(field("metadata", m.clone()));
    }
    if let Some(n) = notes {
        lines.push(field("notes", n.clone()));
    }
    if let Some(e) = &row.error {
        lines.push(Line::from(""));
        lines.push(section("Error"));
        lines.push(Line::from(e.clone()));
    }

    if !errors.is_empty() {
        lines.push(Line::from(""));
        lines.push(section("Attempts"));
        for e in errors {
            lines.push(Line::from(format!(
                "#{} · {} · {}",
                e.attempt,
                fmt_age(e.failed_at),
                e.error
            )));
        }
    }

    if !logs.is_empty() {
        lines.push(Line::from(""));
        lines.push(section("Logs"));
        for l in logs {
            lines.push(Line::from(format!(
                "[{}] {} · {}",
                l.level,
                fmt_age(l.logged_at),
                l.message
            )));
        }
    }
    lines
}

fn workflow_lines(app: &App) -> Vec<Line<'static>> {
    if app.loading_detail {
        return vec![Line::from("loading…")];
    }
    if app.wf_dag.is_empty() {
        return vec![Line::from("no nodes")];
    }
    let mut lines = Vec::new();
    if let Some(r) = app.selected_run() {
        lines.push(field("run", r.id.clone()));
        lines.push(field("definition", r.definition_id.clone()));
        lines.push(field("started", fmt_age_opt(r.started_at)));
        if let Some(parent) = &r.parent_run_id {
            lines.push(field("parent", parent.clone()));
        }
        lines.push(Line::from(""));
    }
    lines.push(section("DAG"));
    lines.push(Line::from(Span::styled(
        "nodes in the same stage run in parallel · ← shows dependencies",
        Style::default().fg(ratatui::style::Color::DarkGray),
    )));

    // Group by topological stage (depth = longest path from a root). Nodes in a
    // stage have no dependency on each other, so listing them together — with
    // each node's exact predecessors after `←` — avoids the false "nesting" a
    // depth-indented tree implies for fan-in/fan-out graphs.
    let mut by_stage: BTreeMap<usize, Vec<&crate::source::DagNode>> = BTreeMap::new();
    for n in &app.wf_dag {
        by_stage.entry(n.depth).or_default().push(n);
    }
    for (stage, nodes) in &by_stage {
        lines.push(Line::from(Span::styled(
            format!("stage {stage}"),
            Style::default().fg(ratatui::style::Color::Cyan),
        )));
        for n in nodes {
            let status = n.status.map(|s| s.as_str()).unwrap_or("pending");
            let style = n
                .status
                .map(wf_node_style)
                .unwrap_or_else(|| Style::default().fg(ratatui::style::Color::Gray));
            let mut spans = vec![
                Span::raw("  "),
                Span::styled("● ", style),
                Span::styled(
                    format!("{:<14}", n.name),
                    Style::default().add_modifier(Modifier::BOLD),
                ),
                Span::styled(format!("{status:<11}"), style),
            ];
            if !n.predecessors.is_empty() {
                spans.push(Span::styled(
                    format!("← {}", n.predecessors.join(", ")),
                    Style::default().fg(ratatui::style::Color::DarkGray),
                ));
            }
            lines.push(Line::from(spans));
            if let Some(err) = &n.error {
                lines.push(Line::from(Span::styled(
                    format!("    {err}"),
                    Style::default().fg(ratatui::style::Color::Red),
                )));
            }
        }
    }
    lines
}

fn dead_lines(app: &App) -> Vec<Line<'static>> {
    let Some(d) = app.selected_dead() else {
        return vec![Line::from("no selection")];
    };
    let mut lines = vec![
        field("id", d.id.clone()),
        field("original", d.original_job_id.clone()),
        field("task", d.task_name.clone()),
        field("queue", d.queue.clone()),
        field("retries", d.retry_count.to_string()),
        field("dlq tries", d.dlq_retry_count.to_string()),
        field("failed", fmt_age(d.failed_at)),
        Line::from(""),
        section("Error"),
        Line::from(d.error.clone().unwrap_or_else(|| "-".into())),
    ];
    lines.push(Line::from(""));
    lines.push(Line::from(Span::styled(
        "T retry · d delete",
        Style::default().fg(ratatui::style::Color::DarkGray),
    )));
    lines
}
