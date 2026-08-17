//! UI-thread state and input handling. Holds the latest snapshot of each view
//! and translates key presses into [`Cmd`]s for the worker.

use std::cell::RefCell;
use std::sync::mpsc::Sender;
use std::time::{Duration, Instant};

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers, MouseButton, MouseEvent, MouseEventKind};
use flexiq_core::JobStatus;
use flexiq_core::QueueStats;
use ratatui::layout::Rect;

use crate::event::{Cmd, FetchReq, Msg, PendingAction};
use crate::source::{
    DagNode, DeadRow, JobDetail, JobRow, StatsSnapshot, View, WorkerView, WorkflowRunRow,
};

/// Clickable regions recorded during rendering and consumed by mouse handling.
#[derive(Default)]
pub struct Hit {
    /// `(x_start, x_end_exclusive, view)` span of each tab label.
    pub tabs: Vec<(u16, u16, View)>,
    /// Screen row the tab labels are drawn on — a click must land on it (not
    /// merely in a tab's columns) to count as a tab click.
    pub tabs_row: u16,
    /// Screen rect of the active view's selectable data rows (each row height 1).
    pub rows: Option<Rect>,
}

pub enum InputMode {
    Normal,
    Confirm(PendingAction),
    Help,
}

pub struct Notice {
    pub text: String,
    pub error: bool,
}

pub struct App {
    pub view: View,
    pub refresh: Duration,
    pub input: InputMode,
    pub should_quit: bool,

    pub stats: StatsSnapshot,
    pub jobs: Vec<JobRow>,
    pub job_filter: Option<JobStatus>,
    pub dead: Vec<DeadRow>,
    pub workers: Vec<WorkerView>,
    pub runs: Vec<WorkflowRunRow>,

    pub selected: usize,
    pub detail_open: bool,
    pub loading_detail: bool,
    pub job_detail: Option<JobDetail>,
    pub wf_dag: Vec<DagNode>,
    /// Id (job or run) the open detail was requested for, so late responses for
    /// a since-changed selection are dropped.
    detail_id: Option<String>,
    /// Vertical scroll offset of the detail pane, in lines (read by the renderer).
    pub detail_scroll: u16,

    pub notice: Option<Notice>,
    /// Clickable regions from the last render; interior-mutable so `draw(&App)`
    /// can record them.
    pub hit: RefCell<Hit>,
    last_refresh: Instant,
    /// A view-data fetch is outstanding; suppresses auto-refresh so a slow
    /// backend can't accumulate stale requests behind user actions.
    awaiting: bool,
}

impl App {
    pub fn new(refresh: Duration) -> Self {
        Self {
            view: View::Stats,
            refresh,
            input: InputMode::Normal,
            should_quit: false,
            stats: StatsSnapshot::default(),
            jobs: Vec::new(),
            job_filter: None,
            dead: Vec::new(),
            workers: Vec::new(),
            runs: Vec::new(),
            selected: 0,
            detail_open: false,
            loading_detail: false,
            job_detail: None,
            wf_dag: Vec::new(),
            detail_id: None,
            detail_scroll: 0,
            notice: None,
            hit: RefCell::new(Hit::default()),
            last_refresh: Instant::now(),
            awaiting: false,
        }
    }

    pub fn current_len(&self) -> usize {
        match self.view {
            View::Stats => self.stats.per_queue.len(),
            View::Jobs => self.jobs.len(),
            View::DeadLetters => self.dead.len(),
            View::Workers => self.workers.len(),
            View::Workflows => self.runs.len(),
        }
    }

    pub fn selected_job(&self) -> Option<&JobRow> {
        self.jobs.get(self.selected)
    }
    pub fn selected_dead(&self) -> Option<&DeadRow> {
        self.dead.get(self.selected)
    }
    pub fn selected_run(&self) -> Option<&WorkflowRunRow> {
        self.runs.get(self.selected)
    }
    pub fn selected_queue(&self) -> Option<&(String, QueueStats)> {
        self.stats.per_queue.get(self.selected)
    }

    /// Whether the given queue is currently paused (for the Stats toggle).
    pub fn is_paused(&self, queue: &str) -> bool {
        self.stats.paused.iter().any(|q| q == queue)
    }

    /// Request a fresh fetch for the active view. Always resets the refresh
    /// clock so auto-refresh and manual refresh share one cadence.
    pub fn request_active(&mut self, tx: &Sender<Cmd>) {
        self.last_refresh = Instant::now();
        self.awaiting = true;
        let req = match self.view {
            View::Stats => FetchReq::Stats,
            View::Jobs => FetchReq::Jobs(self.job_filter),
            View::DeadLetters => FetchReq::Dead,
            View::Workers => FetchReq::Workers,
            View::Workflows => FetchReq::Runs,
        };
        let _ = tx.send(Cmd::Fetch(req));
    }

    /// Auto-refresh only when idle (not confirming, in help, or reading detail),
    /// so those interactions aren't disrupted.
    pub fn maybe_auto_refresh(&mut self, tx: &Sender<Cmd>) {
        if matches!(self.input, InputMode::Normal)
            && !self.detail_open
            && !self.awaiting
            && self.last_refresh.elapsed() >= self.refresh
        {
            self.request_active(tx);
        }
    }

    /// Apply a worker message. Returns `true` when a follow-up list refresh is
    /// warranted (after a successful mutation).
    pub fn apply(&mut self, msg: Msg) -> bool {
        // A completed view-data fetch (or a failed one) clears the in-flight flag.
        if matches!(
            msg,
            Msg::Stats(_)
                | Msg::Jobs(_)
                | Msg::Dead(_)
                | Msg::Workers(_)
                | Msg::Runs(_)
                | Msg::Error(_)
        ) {
            self.awaiting = false;
        }
        match msg {
            Msg::Stats(s) => self.stats = s,
            Msg::Jobs(v) => self.jobs = v,
            Msg::Dead(v) => self.dead = v,
            Msg::Workers(v) => self.workers = v,
            Msg::Runs(v) => self.runs = v,
            Msg::JobDetail(id, d) => {
                if self.detail_open && self.detail_id.as_deref() == Some(id.as_str()) {
                    self.job_detail = d.map(|b| *b);
                    self.loading_detail = false;
                }
            }
            Msg::WorkflowDag(run_id, dag) => {
                if self.detail_open && self.detail_id.as_deref() == Some(run_id.as_str()) {
                    self.wf_dag = dag;
                    self.loading_detail = false;
                }
            }
            Msg::ActionOk(text) => {
                self.notice = Some(Notice { text, error: false });
                return true;
            }
            Msg::Error(text) => {
                self.notice = Some(Notice { text, error: true });
                self.loading_detail = false;
            }
        }
        self.clamp_selection();
        false
    }

    fn clamp_selection(&mut self) {
        let len = self.current_len();
        if len == 0 {
            self.selected = 0;
        } else if self.selected >= len {
            self.selected = len - 1;
        }
    }

    pub fn on_key(&mut self, key: KeyEvent, tx: &Sender<Cmd>) {
        match &self.input {
            InputMode::Help => {
                self.input = InputMode::Normal;
            }
            InputMode::Confirm(action) => match key.code {
                KeyCode::Char('y') | KeyCode::Char('Y') | KeyCode::Enter => {
                    let _ = tx.send(Cmd::Act(action.clone()));
                    self.input = InputMode::Normal;
                }
                _ => self.input = InputMode::Normal,
            },
            InputMode::Normal => self.on_normal_key(key, tx),
        }
    }

    pub fn on_mouse(&mut self, me: MouseEvent, tx: &Sender<Cmd>) {
        // Mouse drives only Normal mode; modal overlays are keyboard-only.
        if !matches!(self.input, InputMode::Normal) {
            return;
        }
        match me.kind {
            MouseEventKind::ScrollDown => self.nav(1),
            MouseEventKind::ScrollUp => self.nav(-1),
            MouseEventKind::Down(MouseButton::Left) => self.on_click(me.column, me.row, tx),
            _ => {}
        }
    }

    fn on_click(&mut self, col: u16, row: u16, tx: &Sender<Cmd>) {
        // Tab bar: click a label to switch view. Require both the tab row and a
        // label's columns — otherwise a click far below a tab would switch views.
        // (Copy out of the RefCell borrow before mutating self.)
        let tab = {
            let hit = self.hit.borrow();
            if row == hit.tabs_row {
                hit.tabs
                    .iter()
                    .find(|(x0, x1, _)| col >= *x0 && col < *x1)
                    .map(|(_, _, v)| *v)
            } else {
                None
            }
        };
        if let Some(view) = tab {
            self.set_view(view, tx);
            return;
        }
        // Data row: first click selects it; clicking the already-selected row
        // opens its detail.
        let rows = self.hit.borrow().rows;
        if let Some(area) = rows {
            let inside = col >= area.x
                && col < area.x + area.width
                && row >= area.y
                && row < area.y + area.height;
            if inside {
                let idx = (row - area.y) as usize;
                if idx < self.current_len() {
                    if self.selected == idx && !self.detail_open {
                        self.open_detail(tx);
                    } else {
                        self.selected = idx;
                    }
                }
            }
        }
    }

    fn on_normal_key(&mut self, key: KeyEvent, tx: &Sender<Cmd>) {
        // Global quit.
        if key.code == KeyCode::Char('q')
            || (key.code == KeyCode::Char('c') && key.modifiers.contains(KeyModifiers::CONTROL))
        {
            // Ctrl-c always quits; bare `q` closes detail first, else quits.
            if key.code == KeyCode::Char('q') && self.detail_open {
                self.detail_open = false;
            } else {
                self.should_quit = true;
            }
            return;
        }

        match key.code {
            KeyCode::Char('?') => self.input = InputMode::Help,
            KeyCode::Esc => {
                self.detail_open = false;
            }
            KeyCode::Tab | KeyCode::Right => self.switch_view(1, tx),
            KeyCode::BackTab | KeyCode::Left => self.switch_view(-1, tx),
            KeyCode::Char(c @ '1'..='5') => {
                let idx = c as usize - '1' as usize;
                self.set_view(View::ALL[idx], tx);
            }
            KeyCode::Char('r') => self.request_active(tx),
            KeyCode::Char('j') | KeyCode::Down => self.nav(1),
            KeyCode::Char('k') | KeyCode::Up => self.nav(-1),
            KeyCode::Char('g') => {
                if self.detail_open {
                    self.detail_scroll = 0;
                } else {
                    self.selected = 0;
                }
            }
            KeyCode::Char('G') if !self.detail_open => {
                self.selected = self.current_len().saturating_sub(1);
            }
            KeyCode::Enter => self.open_detail(tx),
            _ => self.on_view_action_key(key, tx),
        }
    }

    fn on_view_action_key(&mut self, key: KeyEvent, tx: &Sender<Cmd>) {
        if self.detail_open {
            return;
        }
        match self.view {
            View::Jobs => match key.code {
                KeyCode::Char('f') => {
                    self.job_filter = cycle_filter(self.job_filter);
                    self.selected = 0;
                    self.request_active(tx);
                }
                KeyCode::Char('c') => {
                    if let Some(job) = self.selected_job() {
                        self.confirm(PendingAction::CancelJob { id: job.id.clone() });
                    }
                }
                KeyCode::Char('R') => {
                    if let Some(job) = self.selected_job() {
                        self.confirm(PendingAction::ReplayJob { id: job.id.clone() });
                    }
                }
                _ => {}
            },
            View::DeadLetters => match key.code {
                KeyCode::Char('T') | KeyCode::Char('t') => {
                    if let Some(d) = self.selected_dead() {
                        self.confirm(PendingAction::RetryDead { id: d.id.clone() });
                    }
                }
                KeyCode::Char('d') => {
                    if let Some(d) = self.selected_dead() {
                        self.confirm(PendingAction::DeleteDead { id: d.id.clone() });
                    }
                }
                KeyCode::Char('P') if !self.dead.is_empty() => {
                    self.confirm(PendingAction::PurgeDead);
                }
                _ => {}
            },
            View::Stats => {
                if key.code == KeyCode::Char('p') {
                    if let Some((queue, _)) = self.selected_queue() {
                        let queue = queue.clone();
                        let action = if self.is_paused(&queue) {
                            PendingAction::ResumeQueue(queue)
                        } else {
                            PendingAction::PauseQueue(queue)
                        };
                        self.confirm(action);
                    }
                }
            }
            View::Workers | View::Workflows => {}
        }
    }

    fn confirm(&mut self, action: PendingAction) {
        self.input = InputMode::Confirm(action);
    }

    fn move_selection(&mut self, delta: i32) {
        let len = self.current_len();
        if len == 0 {
            return;
        }
        let next = self.selected as i32 + delta;
        self.selected = next.clamp(0, len as i32 - 1) as usize;
    }

    /// Vertical movement: scroll the detail pane when it's open, otherwise move
    /// the list selection. Keeps wheel/j/k doing the expected thing in context.
    fn nav(&mut self, delta: i32) {
        if self.detail_open {
            self.detail_scroll = (self.detail_scroll as i32 + delta).max(0) as u16;
        } else {
            self.move_selection(delta);
        }
    }

    fn switch_view(&mut self, delta: i32, tx: &Sender<Cmd>) {
        let cur = View::ALL.iter().position(|v| *v == self.view).unwrap_or(0);
        let len = View::ALL.len() as i32;
        let idx = ((cur as i32 + delta) % len + len) % len;
        self.set_view(View::ALL[idx as usize], tx);
    }

    fn set_view(&mut self, view: View, tx: &Sender<Cmd>) {
        if view == self.view {
            return;
        }
        self.view = view;
        self.selected = 0;
        self.detail_open = false;
        self.request_active(tx);
    }

    fn open_detail(&mut self, tx: &Sender<Cmd>) {
        self.detail_scroll = 0;
        // Clone the id first so the selection borrow ends before we mutate self.
        match self.view {
            View::Jobs => {
                if let Some(id) = self.selected_job().map(|j| j.id.clone()) {
                    self.job_detail = None;
                    self.loading_detail = true;
                    self.detail_open = true;
                    self.detail_id = Some(id.clone());
                    let _ = tx.send(Cmd::Fetch(FetchReq::JobDetail(id)));
                }
            }
            View::Workflows => {
                if let Some((run_id, definition_id)) = self
                    .selected_run()
                    .map(|r| (r.id.clone(), r.definition_id.clone()))
                {
                    self.wf_dag = Vec::new();
                    self.loading_detail = true;
                    self.detail_open = true;
                    self.detail_id = Some(run_id.clone());
                    let _ = tx.send(Cmd::Fetch(FetchReq::WorkflowDag {
                        run_id,
                        definition_id,
                    }));
                }
            }
            View::DeadLetters => {
                if self.selected_dead().is_some() {
                    self.detail_open = true;
                }
            }
            View::Stats | View::Workers => {}
        }
    }
}

fn cycle_filter(current: Option<JobStatus>) -> Option<JobStatus> {
    match current {
        None => Some(JobStatus::Pending),
        Some(JobStatus::Pending) => Some(JobStatus::Running),
        Some(JobStatus::Running) => Some(JobStatus::Complete),
        Some(JobStatus::Complete) => Some(JobStatus::Failed),
        Some(JobStatus::Failed) => Some(JobStatus::Dead),
        Some(JobStatus::Dead) => Some(JobStatus::Cancelled),
        Some(JobStatus::Cancelled) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc;

    fn app() -> App {
        App::new(Duration::from_secs(1))
    }

    fn jr(id: &str) -> JobRow {
        JobRow {
            id: id.into(),
            task_name: "t".into(),
            queue: "q".into(),
            status: JobStatus::Pending,
            priority: 0,
            retry_count: 0,
            max_retries: 0,
            created_at: 0,
            scheduled_at: 0,
            started_at: None,
            completed_at: None,
            error: None,
            cancel_requested: false,
        }
    }

    fn set_tabs(app: &App) {
        let mut h = app.hit.borrow_mut();
        h.tabs = vec![(1, 10, View::Stats), (13, 21, View::Jobs)];
        h.tabs_row = 1;
    }

    #[test]
    fn click_tab_switches_view() {
        let (tx, _rx) = mpsc::channel();
        let mut app = app();
        set_tabs(&app);
        app.on_click(15, 1, &tx); // inside the Jobs label span, on the tab row
        assert_eq!(app.view, View::Jobs);
    }

    #[test]
    fn click_outside_any_hitbox_is_ignored() {
        let (tx, _rx) = mpsc::channel();
        let mut app = app();
        set_tabs(&app);
        app.on_click(11, 1, &tx); // in the divider gap
        assert_eq!(app.view, View::Stats);
    }

    #[test]
    fn click_below_tab_row_does_not_switch() {
        let (tx, _rx) = mpsc::channel();
        let mut app = app();
        set_tabs(&app);
        app.on_click(15, 5, &tx); // Jobs columns, but far below the tab row
        assert_eq!(app.view, View::Stats);
    }

    #[test]
    fn click_row_selects_then_second_click_opens_detail() {
        let (tx, _rx) = mpsc::channel();
        let mut app = app();
        app.view = View::Jobs;
        app.jobs = vec![jr("a"), jr("b"), jr("c")];
        app.hit.borrow_mut().rows = Some(Rect {
            x: 1,
            y: 5,
            width: 40,
            height: 10,
        });

        app.on_click(10, 6, &tx); // row index 1 (y = 5 + 1)
        assert_eq!(app.selected, 1);
        assert!(!app.detail_open);

        app.on_click(10, 6, &tx); // same row again → open detail
        assert!(app.detail_open);
    }

    #[test]
    fn scroll_moves_selection() {
        use crossterm::event::{MouseEvent, MouseEventKind};
        let (tx, _rx) = mpsc::channel();
        let mut app = app();
        app.view = View::Jobs;
        app.jobs = vec![jr("a"), jr("b")];
        let scroll = |kind| MouseEvent {
            kind,
            column: 0,
            row: 0,
            modifiers: crossterm::event::KeyModifiers::NONE,
        };
        app.on_mouse(scroll(MouseEventKind::ScrollDown), &tx);
        assert_eq!(app.selected, 1);
        app.on_mouse(scroll(MouseEventKind::ScrollUp), &tx);
        assert_eq!(app.selected, 0);
    }

    #[test]
    fn auto_refresh_coalesces_while_awaiting() {
        let (tx, rx) = mpsc::channel();
        let mut app = App::new(Duration::from_millis(0));
        app.view = View::Jobs;

        app.request_active(&tx); // one fetch, marks awaiting
        app.maybe_auto_refresh(&tx); // suppressed while awaiting
        app.maybe_auto_refresh(&tx); // still suppressed
        assert_eq!(rx.try_iter().count(), 1);

        app.apply(Msg::Jobs(Vec::new())); // response clears awaiting
        app.maybe_auto_refresh(&tx); // now allowed
        assert_eq!(rx.try_iter().count(), 1);
    }

    #[test]
    fn stale_detail_response_is_dropped() {
        let mut app = app();
        app.detail_open = true;
        app.loading_detail = true;
        app.detail_id = Some("right".into());

        app.apply(Msg::JobDetail("wrong".into(), None)); // for a stale selection
        assert!(app.loading_detail); // ignored

        app.apply(Msg::JobDetail("right".into(), None)); // matches
        assert!(!app.loading_detail); // applied
    }

    #[test]
    fn nav_scrolls_detail_when_open_else_moves_selection() {
        let mut app = app();
        app.detail_open = true;
        app.nav(1);
        app.nav(1);
        assert_eq!(app.detail_scroll, 2);
        app.nav(-5); // clamps at 0
        assert_eq!(app.detail_scroll, 0);

        app.detail_open = false;
        app.view = View::Jobs;
        app.jobs = vec![jr("a"), jr("b")];
        app.nav(1);
        assert_eq!(app.selected, 1);
        assert_eq!(app.detail_scroll, 0);
    }
}
