//! TUI state and input handling. No I/O and no drawing here, so it can be
//! driven from tests.
//!
//! The interaction model is the one people already have in their hands from
//! chat-style terminal tools: **typing always goes to the input box**, arrows
//! and the mouse move around, `/` opens commands, `esc` steps back. There are
//! no single-letter shortcuts to memorize — or to hit by accident while
//! typing a filter.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use ratatui::crossterm::event::{
    KeyCode, KeyEvent, KeyModifiers, MouseButton, MouseEvent, MouseEventKind,
};
use ratatui::layout::{Position, Rect};
use serde_json::Value;

use crate::client::pb;
use crate::output::{self, items, str_of};
use crate::query;

/// Events kept in memory for the open context. Live arrivals past this drop
/// the oldest; scrolling back reloads them on demand.
pub const MAX_EVENTS: usize = 5_000;
/// Events fetched per page, both for the first fill and for scrolling back.
pub const PAGE: usize = 200;
/// Rows the wheel moves per notch.
const WHEEL: isize = 3;
/// Rows kept visible beyond the selection when it nears an edge.
const SCROLL_PADDING: usize = 2;
/// Typing pauses this long before a filter is sent to the server.
const FILTER_DEBOUNCE: Duration = Duration::from_millis(300);
/// The selection rests this long before its tags are fetched, so holding
/// an arrow key doesn't fire a request per row.
const TAGS_DEBOUNCE: Duration = Duration::from_millis(120);
const TOAST: Duration = Duration::from_secs(2);
/// How long a newly arrived row stays marked as fresh.
pub const FRESH_FOR: Duration = Duration::from_millis(1400);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum View {
    Events,
    Clients,
    Handlers,
    Processors,
    Cluster,
}

impl View {
    pub const ALL: [View; 5] = [
        View::Events,
        View::Clients,
        View::Handlers,
        View::Processors,
        View::Cluster,
    ];

    pub fn title(self) -> &'static str {
        match self {
            View::Events => "Events",
            View::Clients => "Clients",
            View::Handlers => "Handlers",
            View::Processors => "Processors",
            View::Cluster => "Cluster",
        }
    }

    fn index(self) -> usize {
        View::ALL.iter().position(|v| *v == self).unwrap_or(0)
    }
}

/// What the run loop should do after an input event or tick.
#[derive(Debug, PartialEq, Eq)]
pub enum Action {
    Quit,
    SwitchProfile(String),
    /// Fetch the page of events before this sequence.
    LoadOlder {
        before: i64,
    },
    /// Fetch this event's tags for the preview.
    LoadTags {
        sequence: i64,
    },
    /// Put this text on the system clipboard.
    Copy(String),
}

/// A server-side filter on the event list.
#[derive(Debug, Clone, PartialEq)]
pub struct Filter {
    pub text: String,
    pub criteria: Vec<pb::Criterion>,
}

/// One entry of the `/` command palette.
#[derive(Debug, Clone, PartialEq)]
pub struct Command {
    pub label: String,
    pub hint: String,
    run: Run,
}

#[derive(Debug, Clone, PartialEq)]
enum Run {
    Context(String),
    Profile(String),
    Filter(String),
    View(View),
    Live,
    Oldest,
    ClearFilter,
    Copy,
    Help,
    Quit,
}

/// Screen regions recorded while drawing, so the mouse can be hit-tested
/// against exactly what is on screen.
#[derive(Debug, Default)]
pub struct Hits {
    pub tabs: Vec<(Rect, View)>,
    pub list: Rect,
    /// Index of the row drawn on the first line of `list`.
    pub list_top: usize,
    pub preview: Rect,
    /// Clickable tags in the preview → the filter they apply.
    pub tags: Vec<(Rect, String)>,
    pub context: Rect,
}

pub struct App {
    pub profile_name: String,
    pub profiles: Vec<String>,
    pub endpoint: String,
    /// Who the credential says we are (decoded locally from a JWT).
    pub identity: Option<String>,

    pub snapshot: Option<Value>,
    pub error: Option<String>,

    pub view: View,
    /// The input box. Starts with `/` = the command palette is open.
    pub input: String,
    pub input_error: Option<String>,
    pub palette_index: usize,
    pub show_help: bool,
    /// The preview takes the whole body (enter), for reading long payloads.
    pub zoom: bool,
    pub preview_scroll: u16,
    pub toast: Option<(String, Instant)>,
    pub tick: usize,

    // ── the open context's events ──
    pub events_context: String,
    pub filter: Option<Filter>,
    /// Bumped whenever context or filter changes; answers from an older
    /// generation are dropped.
    pub generation: u64,
    pub events: Vec<pb::SequencedEvent>,
    pub events_error: Option<String>,
    /// Selected event. `None` = live: pinned to the newest.
    pub cursor: Option<usize>,
    /// First visible row. `None` = the view follows the selection/newest.
    pub top: Option<usize>,
    pub unseen: usize,
    pub loading_older: bool,
    pub reached_start: bool,
    /// `true` until the first answer for this generation arrives.
    pub loading: bool,
    pub tags: HashMap<i64, Result<Vec<String>, String>>,
    /// Events from this sequence on arrived live, at this instant.
    pub fresh: Option<(i64, Instant)>,

    /// Selected row per non-event view.
    pub selected: [usize; View::ALL.len()],

    pub hits: Hits,
    /// Height of the list on the last draw: what "a page" means.
    pub page_rows: usize,

    filter_due: Option<Instant>,
    tags_due: Option<Instant>,
}

impl App {
    pub fn new(
        profile_name: String,
        profiles: Vec<String>,
        endpoint: String,
        context: String,
    ) -> Self {
        Self {
            profile_name,
            profiles,
            endpoint,
            identity: None,
            snapshot: None,
            error: None,
            view: View::Events,
            input: String::new(),
            input_error: None,
            palette_index: 0,
            show_help: false,
            zoom: false,
            preview_scroll: 0,
            toast: None,
            tick: 0,
            events_context: context,
            filter: None,
            generation: 0,
            events: Vec::new(),
            events_error: None,
            cursor: None,
            top: None,
            unseen: 0,
            loading_older: false,
            reached_start: false,
            loading: true,
            tags: HashMap::new(),
            fresh: None,
            selected: [0; View::ALL.len()],
            hits: Hits::default(),
            page_rows: 20,
            filter_due: None,
            tags_due: None,
        }
    }

    // ─────────────────────────── data arriving ───────────────────────────

    pub fn set_snapshot(&mut self, result: Result<Value, String>) {
        match result {
            Ok(snapshot) => {
                self.snapshot = Some(snapshot);
                self.error = None;
            }
            Err(error) => self.error = Some(error),
        }
        for view in View::ALL {
            let len = self.rows(view).len();
            let slot = &mut self.selected[view.index()];
            *slot = (*slot).min(len.saturating_sub(1));
        }
    }

    pub fn contexts(&self) -> Vec<Value> {
        self.snapshot
            .as_ref()
            .map(|s| items(&s["contexts"]).to_vec())
            .unwrap_or_default()
    }

    pub fn is_fresh(&self, sequence: i64) -> bool {
        self.fresh
            .is_some_and(|(since, at)| sequence >= since && at.elapsed() < FRESH_FOR)
    }

    /// Values worth lighting up in payloads: what the filter asks for.
    pub fn filter_needles(&self) -> Vec<String> {
        let Some(filter) = &self.filter else {
            return vec![];
        };
        filter
            .criteria
            .iter()
            .flat_map(|c| {
                c.tags
                    .iter()
                    .map(|t| String::from_utf8_lossy(&t.value).into_owned())
            })
            .filter(|v| v.len() >= 2)
            .collect()
    }

    /// (tail, head) of the open context: the extent of its whole log.
    pub fn log_span(&self) -> Option<(i64, i64)> {
        let context = self
            .contexts()
            .into_iter()
            .find(|c| str_of(c, "name") == self.events_context)?;
        let head = (output::num_of(&context, "head") as i64)
            // The snapshot trails the event feed by up to a second.
            .max(self.events.last().map(|e| e.sequence + 1).unwrap_or(0));
        Some((output::num_of(&context, "tail") as i64, head))
    }

    /// Rows of a non-event view, narrowed by whatever is typed.
    pub fn rows(&self, view: View) -> Vec<Value> {
        let Some(snapshot) = &self.snapshot else {
            return vec![];
        };
        let rows: Vec<Value> = match view {
            View::Events => vec![],
            View::Cluster => items(&snapshot["cluster"]["raft"]["nodes"]).to_vec(),
            View::Clients => items(&snapshot["clients"]).to_vec(),
            View::Processors => items(&snapshot["processors"]).to_vec(),
            View::Handlers => [("command", "commands"), ("query", "queries")]
                .iter()
                .flat_map(|(kind, key)| {
                    items(&snapshot[*key]).iter().map(move |row| {
                        let mut row = row.clone();
                        row["kind"] = Value::String(kind.to_string());
                        row
                    })
                })
                .collect(),
        };
        let needle = self.input.trim().to_lowercase();
        if view != self.view || needle.is_empty() || self.palette_open() {
            return rows;
        }
        rows.into_iter()
            .filter(|row| row.to_string().to_lowercase().contains(&needle))
            .collect()
    }

    #[cfg(test)]
    pub fn selected_row(&self) -> Option<Value> {
        self.rows(self.view)
            .into_iter()
            .nth(self.selected[self.view.index()])
    }

    /// Points the event list at a context and filter, discarding what was
    /// loaded. Returns the new generation for the loader to stamp answers.
    pub fn open_events(&mut self, context: String, filter: Option<Filter>) -> u64 {
        self.events_context = context;
        self.filter = filter;
        self.generation += 1;
        self.events.clear();
        self.events_error = None;
        self.cursor = None;
        self.top = None;
        self.unseen = 0;
        self.loading_older = false;
        self.reached_start = false;
        self.loading = true;
        self.zoom = false;
        self.preview_scroll = 0;
        self.fresh = None;
        self.generation
    }

    /// Newly read events, in order, newer than everything held.
    pub fn push_events(&mut self, generation: u64, events: Vec<pb::SequencedEvent>) {
        if generation != self.generation {
            return; // An answer for a context or filter we've moved on from.
        }
        // The first fill is history; everything after it arrived just now.
        if !self.loading
            && let Some(first) = events.first()
        {
            let since = match self.fresh {
                Some((seq, at)) if at.elapsed() < FRESH_FOR => seq,
                _ => first.sequence,
            };
            self.fresh = Some((since, Instant::now()));
        }
        self.loading = false;
        self.events_error = None;
        if self.cursor.is_some() || self.top.is_some() {
            self.unseen += events.len();
        }
        self.events.extend(events);
        if self.events.len() > MAX_EVENTS {
            let excess = self.events.len() - MAX_EVENTS;
            self.events.drain(..excess);
            self.cursor = self.cursor.map(|c| c.saturating_sub(excess));
            self.top = self.top.map(|t| t.saturating_sub(excess));
            self.reached_start = false; // what was dropped can be paged back in
        }
        self.want_tags();
    }

    /// An older page, to go in front of everything held. An empty page with
    /// `reached_start` is how the loader says "that was everything".
    pub fn prepend_older(
        &mut self,
        generation: u64,
        older: Vec<pb::SequencedEvent>,
        reached_start: bool,
    ) {
        if generation != self.generation {
            return;
        }
        self.loading = false;
        self.loading_older = false;
        self.reached_start = reached_start;
        let added = older.len();
        self.events.splice(0..0, older);
        // Keep the same rows selected and on screen.
        self.cursor = self.cursor.map(|c| c + added);
        self.top = self.top.map(|t| t + added);
    }

    pub fn set_events_error(&mut self, generation: u64, error: String) {
        if generation == self.generation {
            self.events_error = Some(error);
            self.loading = false;
            self.loading_older = false;
        }
    }

    pub fn set_tags(&mut self, sequence: i64, tags: Result<Vec<String>, String>) {
        if self.tags.len() > 512 {
            self.tags.clear();
        }
        self.tags.insert(sequence, tags);
    }

    // ───────────────────────────── events ─────────────────────────────

    pub fn is_live(&self) -> bool {
        self.cursor.is_none() && self.top.is_none()
    }

    pub fn selected_index(&self) -> Option<usize> {
        (!self.events.is_empty()).then(|| self.cursor.unwrap_or(self.events.len() - 1))
    }

    pub fn selected_event(&self) -> Option<&pb::SequencedEvent> {
        self.events.get(self.selected_index()?)
    }

    /// First visible row for a list `height` rows tall.
    pub fn viewport_top(&self, height: usize) -> usize {
        let bottom_aligned = self.events.len().saturating_sub(height);
        self.top.unwrap_or(bottom_aligned).min(bottom_aligned)
    }

    fn go_live(&mut self) {
        self.cursor = None;
        self.top = None;
        self.unseen = 0;
        self.want_tags();
    }

    /// Moves the selection; the view follows it. `delta` < 0 = older.
    fn move_selection(&mut self, delta: isize) -> Option<Action> {
        let last = self.events.len().checked_sub(1)?;
        let current = self.selected_index()? as isize;
        let target = current + delta;
        self.preview_scroll = 0;
        if target >= last as isize {
            self.go_live();
            return None;
        }
        let target = target.max(0) as usize;
        self.cursor = Some(target);

        // Bring the selection into view, with a little context around it.
        let height = self.page_rows.max(1);
        let top = self.viewport_top(height);
        let padding = SCROLL_PADDING.min(height / 3);
        if target < top + padding {
            self.top = Some(target.saturating_sub(padding));
        } else if target + padding >= top + height {
            self.top = Some((target + padding + 1).saturating_sub(height));
        } else {
            self.top = Some(top);
        }
        self.want_tags();
        if target <= padding {
            return self.load_older();
        }
        None
    }

    /// Moves the view without touching the selection (the wheel).
    fn scroll_view(&mut self, delta: isize) -> Option<Action> {
        let height = self.page_rows.max(1);
        let bottom_aligned = self.events.len().saturating_sub(height);
        let top = self.viewport_top(height) as isize + delta;
        if top >= bottom_aligned as isize {
            // Scrolled back down to the newest rows: resume following, unless
            // a parked selection says the user is still reading.
            self.top = None;
            if self.cursor.is_none() {
                self.unseen = 0;
            }
            return None;
        }
        self.top = Some(top.max(0) as usize);
        if top <= 0 { self.load_older() } else { None }
    }

    fn load_older(&mut self) -> Option<Action> {
        // A filtered list is loaded whole, from the start of the context.
        if self.filter.is_some() || self.reached_start || self.loading_older {
            return None;
        }
        let first = self.events.first()?;
        if first.sequence <= 0 {
            return None;
        }
        self.loading_older = true;
        Some(Action::LoadOlder {
            before: first.sequence,
        })
    }

    fn want_tags(&mut self) {
        self.tags_due = Some(Instant::now() + TAGS_DEBOUNCE);
    }

    /// Applies filter text now. `Err` = the text doesn't parse.
    fn apply_filter(&mut self, text: &str) -> Result<(), String> {
        let criteria = query::parse_filter(text).map_err(|e| format!("{e:#}"))?;
        let filter = (!criteria.is_empty()).then(|| Filter {
            text: text.trim().to_string(),
            criteria,
        });
        if filter != self.filter {
            let context = self.events_context.clone();
            self.open_events(context, filter);
        }
        Ok(())
    }

    fn set_filter_text(&mut self, text: &str) {
        self.view = View::Events;
        self.input = text.to_string();
        self.input_error = self.apply_filter(text).err();
        self.filter_due = None;
    }

    // ───────────────────────────── palette ─────────────────────────────

    pub fn palette_open(&self) -> bool {
        self.input.starts_with('/')
    }

    /// Commands matching what is typed after the `/`.
    pub fn palette(&self) -> Vec<Command> {
        let mut all = Vec::new();
        let mut push = |label: String, hint: &str, run: Run| {
            all.push(Command {
                label,
                hint: hint.to_string(),
                run,
            })
        };
        // What the selection offers comes first: it is the likeliest intent.
        if self.view == View::Events
            && let Some(event) = self.selected_event()
        {
            if let Some(Ok(tags)) = self.tags.get(&event.sequence) {
                for tag in tags {
                    push(
                        format!("filter {tag}"),
                        "every event with this tag",
                        Run::Filter(tag.clone()),
                    );
                }
            }
            if let Some(name) = event.event.as_ref().map(|e| e.name.clone())
                && !name.contains(char::is_whitespace)
            {
                push(
                    format!("filter type={name}"),
                    "every event of this type",
                    Run::Filter(format!("type={name}")),
                );
            }
            push("copy".into(), "copy the selected event as JSON", Run::Copy);
        }
        for context in self.contexts() {
            let name = str_of(&context, "name").to_string();
            let count = output::num_of(&context, "head");
            let hint = format!("{count} event{}", if count == 1 { "" } else { "s" });
            push(format!("context {name}"), &hint, Run::Context(name));
        }
        push(
            "live".into(),
            "jump to the newest event and follow",
            Run::Live,
        );
        push(
            "oldest".into(),
            "jump to the oldest loaded event",
            Run::Oldest,
        );
        push("clear".into(), "remove the filter", Run::ClearFilter);
        for view in View::ALL {
            push(
                format!("view {}", view.title().to_lowercase()),
                "switch view",
                Run::View(view),
            );
        }
        for profile in &self.profiles {
            if *profile != self.profile_name {
                push(
                    format!("profile {profile}"),
                    "reconnect with this profile",
                    Run::Profile(profile.clone()),
                );
            }
        }
        push("help".into(), "keys and tips", Run::Help);
        push("quit".into(), "leave kronos", Run::Quit);

        let needle = self.input.trim_start_matches('/').trim().to_lowercase();
        // Every typed word must appear: "ctx ord" style narrowing.
        all.into_iter()
            .filter(|c| {
                let label = c.label.to_lowercase();
                needle.split_whitespace().all(|word| label.contains(word))
            })
            .collect()
    }

    fn run(&mut self, command: Command) -> Option<Action> {
        self.input.clear();
        self.input_error = None;
        self.palette_index = 0;
        match command.run {
            Run::Context(name) => {
                self.view = View::Events;
                if name != self.events_context {
                    self.open_events(name, None);
                }
            }
            Run::Profile(name) => return Some(Action::SwitchProfile(name)),
            Run::Filter(text) => self.set_filter_text(&text),
            Run::View(view) => self.view = view,
            Run::Live => self.go_live(),
            Run::Oldest => return self.move_selection(isize::MIN / 2),
            Run::ClearFilter => self.set_filter_text(""),
            Run::Copy => return self.copy_selected(),
            Run::Help => self.show_help = true,
            Run::Quit => return Some(Action::Quit),
        }
        None
    }

    fn copy_selected(&mut self) -> Option<Action> {
        let event = self.selected_event()?;
        let mut value = output::event_json(event);
        if let Some(Ok(tags)) = self.tags.get(&event.sequence) {
            value["tags"] = tags.iter().cloned().collect();
        }
        self.toast = Some((format!("copied event {}", event.sequence), Instant::now()));
        Some(Action::Copy(
            serde_json::to_string_pretty(&value).unwrap_or_default(),
        ))
    }

    // ────────────────────────────── input ──────────────────────────────

    /// Called a few times a second: debounced work and expiring notices.
    pub fn on_tick(&mut self) -> Option<Action> {
        self.tick = self.tick.wrapping_add(1);
        let now = Instant::now();
        if self.toast.as_ref().is_some_and(|(_, at)| now - *at > TOAST) {
            self.toast = None;
        }
        if self.fresh.is_some_and(|(_, at)| now - at > FRESH_FOR) {
            self.fresh = None;
        }
        if self.filter_due.is_some_and(|due| now >= due) {
            self.filter_due = None;
            // While typing, a half-written filter is not a mistake yet: the
            // hint stays neutral until enter asks for a verdict.
            let text = self.input.clone();
            if self.apply_filter(&text).is_ok() {
                self.input_error = None;
            }
        }
        if self.tags_due.is_some_and(|due| now >= due) {
            self.tags_due = None;
            if self.view == View::Events
                && let Some(event) = self.selected_event()
                && !self.tags.contains_key(&event.sequence)
            {
                return Some(Action::LoadTags {
                    sequence: event.sequence,
                });
            }
        }
        None
    }

    fn input_changed(&mut self) {
        self.input_error = None;
        self.palette_index = 0;
        if self.palette_open() {
            return;
        }
        match self.view {
            View::Events => self.filter_due = Some(Instant::now() + FILTER_DEBOUNCE),
            view => self.selected[view.index()] = 0,
        }
    }

    pub fn on_key(&mut self, key: KeyEvent) -> Option<Action> {
        let ctrl = key.modifiers.contains(KeyModifiers::CONTROL);
        if self.show_help {
            self.show_help = false;
            return None;
        }
        match key.code {
            // ctrl+c backs out of text first, the way a shell does.
            KeyCode::Char('c') if ctrl && !self.input.is_empty() => {
                self.input.clear();
                self.input_changed();
                if self.view == View::Events {
                    self.set_filter_text("");
                }
            }
            KeyCode::Char('c') | KeyCode::Char('d') if ctrl => return Some(Action::Quit),
            KeyCode::Char('y') if ctrl => return self.copy_selected(),
            KeyCode::Char('u') if ctrl => {
                self.input.clear();
                self.input_changed();
            }
            KeyCode::Char('w') if ctrl => {
                let trimmed = self.input.trim_end();
                let cut = trimmed
                    .rfind(char::is_whitespace)
                    .map(|i| i + 1)
                    .unwrap_or(0);
                self.input.truncate(cut);
                self.input_changed();
            }
            KeyCode::Esc => self.step_back(),
            KeyCode::Tab if self.palette_open() => {
                if let Some(command) = self.palette().into_iter().nth(self.palette_index) {
                    self.input = format!("/{}", command.label);
                }
            }
            KeyCode::Tab => self.switch_view(1),
            KeyCode::BackTab => self.switch_view(-1),
            KeyCode::Enter => return self.on_enter(),
            KeyCode::Up => return self.on_arrow(-1),
            KeyCode::Down => return self.on_arrow(1),
            KeyCode::PageUp => return self.on_arrow(-(self.page_rows.max(2) as isize - 1)),
            KeyCode::PageDown => return self.on_arrow(self.page_rows.max(2) as isize - 1),
            KeyCode::Home => return self.on_arrow(isize::MIN / 2),
            KeyCode::End => return self.on_arrow(isize::MAX / 2),
            KeyCode::Left if self.zoom => return self.move_selection(-1),
            KeyCode::Right if self.zoom => return self.move_selection(1),
            KeyCode::Backspace => {
                self.input.pop();
                self.input_changed();
            }
            KeyCode::Char('?') if self.input.is_empty() => self.show_help = true,
            KeyCode::Char(c) if !ctrl => {
                self.input.push(c);
                self.input_changed();
            }
            _ => {}
        }
        None
    }

    /// `esc`: undo the most recent "deeper", one layer at a time.
    fn step_back(&mut self) {
        if self.palette_open() {
            self.input.clear();
            self.input_changed();
        } else if self.zoom {
            self.zoom = false;
            self.preview_scroll = 0;
        } else if !self.input.is_empty() || self.filter.is_some() {
            self.input.clear();
            self.input_changed();
            if self.view == View::Events {
                self.set_filter_text("");
            }
        } else if !self.is_live() {
            self.go_live();
        }
    }

    fn switch_view(&mut self, delta: isize) {
        let count = View::ALL.len() as isize;
        let next = (self.view.index() as isize + delta).rem_euclid(count);
        self.view = View::ALL[next as usize];
        self.zoom = false;
        // The box shows each view's own narrowing text.
        self.input = match (self.view, &self.filter) {
            (View::Events, Some(filter)) => filter.text.clone(),
            _ => String::new(),
        };
        self.input_error = None;
    }

    fn on_enter(&mut self) -> Option<Action> {
        if self.palette_open() {
            return match self.palette().into_iter().nth(self.palette_index) {
                Some(command) => self.run(command),
                None => {
                    self.input_error = Some("no such command — esc to cancel".into());
                    None
                }
            };
        }
        if self.view == View::Events {
            // The box isn't what's applied (still typing, or it didn't
            // parse): enter means "apply this now, and tell me if it's wrong".
            let applied = self.filter.as_ref().map(|f| f.text.as_str()).unwrap_or("");
            if self.input.trim() != applied {
                self.filter_due = None;
                let text = self.input.clone();
                self.input_error = self.apply_filter(&text).err();
                return None;
            }
            if self.selected_event().is_some() {
                self.zoom = !self.zoom;
                self.preview_scroll = 0;
            }
        }
        None
    }

    fn on_arrow(&mut self, delta: isize) -> Option<Action> {
        if self.palette_open() {
            let len = self.palette().len().max(1) as isize;
            self.palette_index =
                (self.palette_index as isize + delta.signum()).rem_euclid(len) as usize;
            return None;
        }
        if self.zoom {
            self.preview_scroll = self
                .preview_scroll
                .saturating_add_signed(delta.clamp(-30, 30) as i16);
            return None;
        }
        match self.view {
            View::Events => self.move_selection(delta),
            view => {
                let len = self.rows(view).len();
                if len > 0 {
                    let slot = &mut self.selected[view.index()];
                    *slot = (*slot as isize + delta).clamp(0, len as isize - 1) as usize;
                }
                None
            }
        }
    }

    pub fn on_mouse(&mut self, mouse: MouseEvent) -> Option<Action> {
        let at = Position::new(mouse.column, mouse.row);
        match mouse.kind {
            MouseEventKind::ScrollUp | MouseEventKind::ScrollDown => {
                let delta = if mouse.kind == MouseEventKind::ScrollUp {
                    -WHEEL
                } else {
                    WHEEL
                };
                if self.hits.preview.contains(at) || self.zoom {
                    self.preview_scroll = self.preview_scroll.saturating_add_signed(delta as i16);
                    None
                } else if self.view == View::Events {
                    self.scroll_view(delta)
                } else {
                    self.on_arrow(delta.signum())
                }
            }
            MouseEventKind::Down(MouseButton::Left) => {
                if let Some((_, view)) = self.hits.tabs.iter().find(|(r, _)| r.contains(at)) {
                    let delta = view.index() as isize - self.view.index() as isize;
                    self.switch_view(delta);
                } else if let Some((_, tag)) = self.hits.tags.iter().find(|(r, _)| r.contains(at)) {
                    let tag = tag.clone();
                    self.zoom = false;
                    self.set_filter_text(&tag);
                } else if self.hits.context.contains(at) {
                    self.input = "/context ".into();
                    self.input_changed();
                } else if self.hits.list.contains(at) && !self.zoom {
                    let index = self.hits.list_top + (at.y - self.hits.list.y) as usize;
                    match self.view {
                        View::Events if index < self.events.len() => {
                            let last = self.events.len() - 1;
                            self.top = Some(self.viewport_top(self.page_rows.max(1)));
                            self.cursor = (index < last).then_some(index);
                            if index == last {
                                self.top = None;
                                self.unseen = 0;
                            }
                            self.preview_scroll = 0;
                            self.want_tags();
                        }
                        View::Events => {}
                        view if index < self.rows(view).len() => {
                            self.selected[view.index()] = index;
                        }
                        _ => {}
                    }
                }
                None
            }
            _ => None,
        }
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use serde_json::json;

    pub fn fixture() -> Value {
        json!({
            "node": { "name": "kronosdb-0", "version": "0.9.0", "uptime_secs": 3725,
                      "ready": true, "tls": true, "auth": ["oidc:google (https://accounts.google.com)"] },
            "cluster": { "node_id": 1, "claim": { "epoch": 4, "leader_id": 1, "term": 7, "writable": true },
                "raft": { "state": "Leader", "leader_id": 1, "term": 7, "last_log_index": 40, "last_applied_index": 40,
                    "nodes": [ { "id": 1, "addr": "kronosdb-0:50051", "voter": true, "leader": true },
                               { "id": 2, "addr": "kronosdb-1:50051", "voter": true, "leader": false } ] } },
            "contexts": [ { "name": "default", "head": 10, "tail": 0, "local_tail": 10, "durable_tail": 10, "data_bytes": 2048, "poisoned": false },
                          { "name": "orders", "head": 5000, "tail": 0, "local_tail": 5000, "durable_tail": 4990, "data_bytes": 1048576, "poisoned": false } ],
            "clients": [ { "client_id": "c-1", "component": "OrderService", "version": "1.2.0",
                           "connected_secs": 90, "last_heartbeat_ms": 120, "streaming": true },
                         { "client_id": "c-2", "component": "BillingService", "version": "0.4.0",
                           "connected_secs": 30, "last_heartbeat_ms": 80, "streaming": true } ],
            "commands": [ { "bus": "default", "name": "PlaceOrder", "dispatched": 12, "failed": 1, "avg_duration_us": 2500,
                            "handlers": [ { "client_id": "c-1", "component": "OrderService", "load_factor": 100, "available_permits": 990 } ] } ],
            "queries": [ { "bus": "default", "name": "FindOrder", "dispatched": 3, "failed": 0, "avg_duration_us": 900, "handlers": [] } ],
            "subscriptions": [],
            "processors": [ { "name": "order-projection", "mode": "tracking", "streaming": true,
                "instances": [ { "running": true, "error": false, "segments": [
                    { "segment_id": 0, "one_part_of": 1, "caught_up": true, "replaying": false, "token_position": 4999, "error_state": "" } ] } ] } ],
        })
    }

    pub fn key(code: KeyCode) -> KeyEvent {
        KeyEvent::new(code, KeyModifiers::NONE)
    }

    pub fn ctrl(c: char) -> KeyEvent {
        KeyEvent::new(KeyCode::Char(c), KeyModifiers::CONTROL)
    }

    pub fn event(sequence: i64, name: &str) -> pb::SequencedEvent {
        pb::SequencedEvent {
            sequence,
            event: Some(pb::Event {
                name: name.into(),
                payload: format!(r#"{{"n":{sequence}}}"#).into_bytes(),
                timestamp: 1_789_899_641_000,
                ..Default::default()
            }),
        }
    }

    /// 100 events (100..200) in `orders`, a 10-row list.
    pub fn app() -> App {
        let mut app = App::new(
            "prod".into(),
            vec!["prod".into(), "staging".into()],
            "http://localhost:50051".into(),
            "orders".into(),
        );
        app.set_snapshot(Ok(fixture()));
        let generation = app.open_events("orders".into(), None);
        app.push_events(
            generation,
            (100..200).map(|s| event(s, "OrderPlaced")).collect(),
        );
        app.page_rows = 10;
        app
    }

    pub fn type_text(app: &mut App, text: &str) {
        for c in text.chars() {
            app.on_key(key(KeyCode::Char(c)));
        }
    }

    fn settle(app: &mut App) -> Option<Action> {
        std::thread::sleep(FILTER_DEBOUNCE + Duration::from_millis(30));
        app.on_tick()
    }

    fn wheel(app: &mut App, kind: MouseEventKind, column: u16, row: u16) -> Option<Action> {
        app.on_mouse(MouseEvent {
            kind,
            column,
            row,
            modifiers: KeyModifiers::NONE,
        })
    }

    #[test]
    fn arrows_move_the_selection_and_the_view_follows() {
        let mut app = app();
        assert!(app.is_live());
        assert_eq!(app.selected_event().unwrap().sequence, 199);
        assert_eq!(app.viewport_top(10), 90);

        app.on_key(key(KeyCode::Up));
        assert_eq!(app.selected_event().unwrap().sequence, 198);
        assert!(!app.is_live());

        // A page up keeps the selection inside the view, with padding.
        app.on_key(key(KeyCode::PageUp));
        let (selected, top) = (app.selected_index().unwrap(), app.viewport_top(10));
        assert_eq!(selected, 89);
        assert!(
            top + SCROLL_PADDING <= selected && selected < top + 10,
            "{selected} in {top}.."
        );

        // New events while reading: counted, and nothing moves.
        let generation = app.generation;
        app.push_events(
            generation,
            vec![event(200, "OrderPaid"), event(201, "OrderPaid")],
        );
        assert_eq!((app.unseen, app.selected_index()), (2, Some(89)));

        app.on_key(key(KeyCode::End));
        assert!(app.is_live() && app.unseen == 0);
        assert_eq!(app.selected_event().unwrap().sequence, 201);
    }

    #[test]
    fn the_wheel_moves_the_view_not_the_selection() {
        let mut app = app();
        app.hits.list = Rect::new(0, 5, 80, 10);
        wheel(&mut app, MouseEventKind::ScrollUp, 10, 8);
        assert_eq!(app.viewport_top(10), 87);
        assert_eq!(
            app.selected_event().unwrap().sequence,
            199,
            "selection untouched"
        );

        // Scrolling back to the bottom resumes following.
        for _ in 0..3 {
            wheel(&mut app, MouseEventKind::ScrollDown, 10, 8);
        }
        assert!(app.is_live());

        // Click a row: it becomes the selection.
        app.hits.list_top = 90;
        wheel(&mut app, MouseEventKind::Down(MouseButton::Left), 10, 7);
        assert_eq!(app.selected_event().unwrap().sequence, 192);
    }

    #[test]
    fn reaching_the_top_pages_in_history_once() {
        let mut app = app();
        assert_eq!(
            app.on_key(key(KeyCode::Home)),
            Some(Action::LoadOlder { before: 100 })
        );
        assert_eq!(app.on_key(key(KeyCode::Up)), None, "one request in flight");

        let generation = app.generation;
        app.prepend_older(
            generation,
            (50..100).map(|s| event(s, "X")).collect(),
            false,
        );
        assert_eq!(
            app.selected_event().unwrap().sequence,
            100,
            "same event selected"
        );
        assert_eq!(app.viewport_top(10), 50, "same rows on screen");

        app.prepend_older(generation, vec![], true);
        app.on_key(key(KeyCode::Home));
        assert_eq!(app.on_key(key(KeyCode::Up)), None, "nothing older exists");
    }

    #[test]
    fn typing_filters_after_a_pause_and_enter_gives_the_verdict() {
        let mut app = app();
        type_text(&mut app, "orderId");
        let before = app.generation;
        settle(&mut app);
        // Half-typed: not applied, and not scolded either.
        assert_eq!((app.generation, &app.input_error), (before, &None));

        app.on_key(key(KeyCode::Enter));
        assert!(app.input_error.as_ref().unwrap().contains("key=value"));

        type_text(&mut app, "=o-1 type=OrderPaid");
        assert!(app.input_error.is_none(), "typing clears the complaint");
        settle(&mut app);
        let filter = app.filter.as_ref().expect("applied after the pause");
        assert_eq!(filter.text, "orderId=o-1 type=OrderPaid");
        assert!(app.generation > before && app.events.is_empty() && app.loading);

        // Answers to the old, unfiltered read are ignored.
        app.push_events(before, vec![event(1, "Stale")]);
        assert!(app.events.is_empty());

        // esc peels one layer: the filter goes, the list reloads.
        app.on_key(key(KeyCode::Esc));
        assert!(app.filter.is_none() && app.input.is_empty());
    }

    #[test]
    fn letters_are_never_shortcuts() {
        let mut app = app();
        type_text(&mut app, "quantity=5 jk");
        assert_eq!(app.input, "quantity=5 jk");
        assert_eq!(app.on_key(ctrl('c')), None, "first ctrl+c clears the box");
        assert!(app.input.is_empty());
        assert_eq!(app.on_key(ctrl('c')), Some(Action::Quit));
    }

    #[test]
    fn palette_offers_the_selection_first_and_narrows_by_words() {
        let mut app = app();
        app.set_tags(199, Ok(vec!["orderId=o-7".into(), "region=eu".into()]));
        type_text(&mut app, "/");
        let labels: Vec<String> = app.palette().into_iter().map(|c| c.label).collect();
        assert_eq!(
            &labels[..4],
            [
                "filter orderId=o-7",
                "filter region=eu",
                "filter type=OrderPlaced",
                "copy"
            ]
        );
        assert!(labels.contains(&"profile staging".to_string()));
        assert!(
            !labels.contains(&"profile prod".to_string()),
            "already on it"
        );

        type_text(&mut app, "con def");
        let narrowed = app.palette();
        assert_eq!(narrowed.len(), 1);
        assert_eq!(narrowed[0].label, "context default");
        app.on_key(key(KeyCode::Enter));
        assert_eq!(app.events_context, "default");
        assert!(app.input.is_empty() && app.loading);

        type_text(&mut app, "/profile");
        assert_eq!(
            app.on_key(key(KeyCode::Enter)),
            Some(Action::SwitchProfile("staging".into()))
        );

        type_text(&mut app, "/nonsense");
        app.on_key(key(KeyCode::Enter));
        assert!(app.input_error.is_some());
        app.on_key(key(KeyCode::Esc));
        assert!(app.input.is_empty());
    }

    #[test]
    fn filter_from_the_selection_and_copy() {
        let mut app = app();
        app.set_tags(199, Ok(vec!["orderId=o-7".into()]));
        type_text(&mut app, "/filter order");
        app.on_key(key(KeyCode::Enter));
        assert_eq!(app.filter.as_ref().unwrap().text, "orderId=o-7");
        assert_eq!(app.input, "orderId=o-7", "the box shows what is applied");

        let generation = app.generation;
        app.push_events(generation, vec![event(7, "OrderPlaced")]);
        match app.on_key(ctrl('y')) {
            Some(Action::Copy(text)) => assert!(text.contains("\"sequence\": 7")),
            other => panic!("expected a copy, got {other:?}"),
        }
        assert!(app.toast.as_ref().unwrap().0.contains("copied event 7"));
    }

    #[test]
    fn enter_zooms_and_esc_steps_back_in_order() {
        let mut app = app();
        app.on_key(key(KeyCode::Up));
        app.on_key(key(KeyCode::Enter));
        assert!(app.zoom);
        app.on_key(key(KeyCode::Down));
        assert_eq!(
            app.preview_scroll, 1,
            "arrows read the payload while zoomed"
        );
        app.on_key(key(KeyCode::Left));
        assert_eq!(
            app.selected_event().unwrap().sequence,
            197,
            "←/→ walk events"
        );

        app.on_key(key(KeyCode::Esc));
        assert!(!app.zoom && !app.is_live());
        app.on_key(key(KeyCode::Esc));
        assert!(app.is_live(), "last esc returns to live");
    }

    #[test]
    fn tags_load_after_the_selection_rests() {
        let mut app = app();
        app.on_key(key(KeyCode::Up));
        app.on_key(key(KeyCode::Up));
        assert_eq!(app.on_tick(), None, "still moving");
        std::thread::sleep(TAGS_DEBOUNCE + Duration::from_millis(30));
        assert_eq!(app.on_tick(), Some(Action::LoadTags { sequence: 197 }));
        app.set_tags(197, Ok(vec![]));
        assert_eq!(app.on_tick(), None, "cached");
    }

    #[test]
    fn other_views_narrow_as_you_type() {
        let mut app = app();
        app.on_key(key(KeyCode::Tab));
        assert_eq!(app.view, View::Clients);
        assert_eq!(app.rows(View::Clients).len(), 2);
        type_text(&mut app, "billing");
        assert_eq!(app.rows(View::Clients).len(), 1);
        assert_eq!(str_of(&app.selected_row().unwrap(), "client_id"), "c-2");

        app.on_key(key(KeyCode::BackTab));
        assert_eq!(app.view, View::Events);
        assert!(app.input.is_empty(), "each view has its own text");
    }

    #[test]
    fn buffer_is_bounded_and_positions_survive_trimming() {
        let mut app = app();
        app.on_key(key(KeyCode::Up));
        let sequence = app.selected_event().unwrap().sequence;
        let generation = app.generation;
        app.push_events(
            generation,
            (200..200 + MAX_EVENTS as i64)
                .map(|s| event(s, "X"))
                .collect(),
        );
        assert_eq!(app.events.len(), MAX_EVENTS);
        // The parked event itself was trimmed away; the cursor stays valid.
        assert!(app.selected_event().unwrap().sequence > sequence);
        assert!(!app.reached_start);
    }
}
