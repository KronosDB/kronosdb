//! The terminal UI: what bare `kronos` opens. Typing filters, arrows and the
//! mouse move around, `/` opens commands, `esc` steps back — see `app.rs`
//! for the interaction model and `ui.rs` for the look.
//!
//! One loop owns the terminal and the state; everything slow (admin API
//! polls, event reads, the token command) runs in tasks that report back
//! over a channel, so a hung network never freezes the keyboard.

mod app;
mod theme;
mod ui;

use std::io::Write as _;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use anyhow::Result;
use base64::Engine as _;
use ratatui::crossterm::event::{
    self, DisableMouseCapture, EnableMouseCapture, Event, KeyEvent, KeyEventKind, MouseEvent,
};
use ratatui::crossterm::execute;
use serde_json::Value;
use tokio::sync::{mpsc, watch};

use crate::client::{Connection, pb};
use crate::output;
use crate::profile::ConfigFile;
use crate::token::jwt_claims;
use app::{Action, App, MAX_EVENTS, PAGE};

const POLL_INTERVAL: Duration = Duration::from_secs(1);
/// Drives debounced work and the spinner.
const TICK: Duration = Duration::from_millis(100);

/// What the event loader should be following.
#[derive(Clone, PartialEq)]
struct Target {
    generation: u64,
    context: String,
    criteria: Vec<pb::Criterion>,
}

enum Msg {
    Key(KeyEvent),
    Mouse(MouseEvent),
    Resize,
    Tick,
    Snapshot(Result<Value, String>),
    Identity(String),
    /// (generation, newer events)
    Events(u64, Vec<pb::SequencedEvent>),
    /// (generation, older page, reached the start)
    Older(u64, Vec<pb::SequencedEvent>, bool),
    EventsError(u64, String),
    /// (sequence, its tags)
    Tags(i64, Result<Vec<String>, String>),
}

/// Messages carry the session they came from; after a profile switch,
/// stragglers from the old connection are dropped.
type Tagged = (u64, Msg);

pub async fn run(conn: Connection, cfg: ConfigFile, context: String) -> Result<()> {
    let (tx, mut rx) = mpsc::unbounded_channel::<Tagged>();

    // crossterm's reader blocks, so it gets a thread. Session 0 = "any".
    let stop = Arc::new(AtomicBool::new(false));
    let input = {
        let (tx, stop) = (tx.clone(), Arc::clone(&stop));
        std::thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                if !event::poll(Duration::from_millis(100)).unwrap_or(false) {
                    continue;
                }
                let msg = match event::read() {
                    // Repeat matters: holding an arrow should keep moving.
                    Ok(Event::Key(key)) if key.kind != KeyEventKind::Release => Msg::Key(key),
                    Ok(Event::Mouse(mouse)) => Msg::Mouse(mouse),
                    Ok(Event::Resize(..)) => Msg::Resize,
                    _ => continue,
                };
                if tx.send((0, msg)).is_err() {
                    break;
                }
            }
        })
    };

    let mut terminal = ratatui::init();
    // ratatui's panic hook restores the screen but knows nothing about the
    // mouse; without this a crash leaves the shell printing escape codes on
    // every click.
    let previous_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        let _ = execute!(std::io::stdout(), DisableMouseCapture);
        previous_hook(info);
    }));
    let _ = execute!(std::io::stdout(), EnableMouseCapture);
    let ticker = {
        let tx = tx.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(TICK);
            loop {
                interval.tick().await;
                if tx.send((0, Msg::Tick)).is_err() {
                    return;
                }
            }
        })
    };
    let mut conn = Arc::new(conn);
    let mut context = context;
    let mut session = 1u64;
    let result = loop {
        match run_session(&mut terminal, &mut rx, &tx, session, &conn, &cfg, &context).await {
            Ok(Action::SwitchProfile(next)) => {
                match cfg
                    .resolve(Some(&next))
                    .and_then(|(name, profile)| Connection::open(name, profile))
                {
                    Ok(opened) => {
                        context = opened.profile.context().to_string();
                        conn = Arc::new(opened);
                        session += 1;
                    }
                    Err(error) => break Err(error),
                }
            }
            Ok(_) => break Ok(()),
            Err(error) => break Err(error),
        }
    };

    ticker.abort();
    stop.store(true, Ordering::Relaxed);
    let _ = execute!(std::io::stdout(), DisableMouseCapture);
    ratatui::restore();
    let _ = input.join();
    result
}

async fn run_session(
    terminal: &mut ratatui::DefaultTerminal,
    rx: &mut mpsc::UnboundedReceiver<Tagged>,
    tx: &mpsc::UnboundedSender<Tagged>,
    session: u64,
    conn: &Arc<Connection>,
    cfg: &ConfigFile,
    context: &str,
) -> Result<Action> {
    let mut app = App::new(
        conn.profile_name.clone(),
        cfg.profiles.keys().cloned().collect(),
        conn.profile.endpoint.clone(),
        context.to_string(),
    );
    let target_of = |app: &App| Target {
        generation: app.generation,
        context: app.events_context.clone(),
        criteria: app
            .filter
            .as_ref()
            .map(|f| f.criteria.clone())
            .unwrap_or_default(),
    };
    app.open_events(context.to_string(), None);
    let (target_tx, target_rx) = watch::channel(target_of(&app));

    let tasks = [
        tokio::spawn(poll_snapshot(Arc::clone(conn), tx.clone(), session)),
        tokio::spawn(follow_events(
            Arc::clone(conn),
            tx.clone(),
            session,
            target_rx,
        )),
        tokio::spawn(resolve_identity(Arc::clone(conn), tx.clone(), session)),
    ];

    let action = 'session: loop {
        terminal.draw(|frame| ui::draw(frame, &mut app))?;

        // Wait for one message, then take everything else already queued
        // and apply it all before drawing again. A trackpad flick delivers
        // dozens of wheel events in a burst; drawing once per event would
        // leave the screen minutes behind the hand.
        let Some(first) = rx.recv().await else {
            break Action::Quit;
        };
        let mut batch = vec![first];
        while let Ok(next) = rx.try_recv() {
            batch.push(next);
        }

        for (from, msg) in batch {
            if from != 0 && from != session {
                continue;
            }
            let action = match msg {
                Msg::Key(key) => app.on_key(key),
                Msg::Mouse(mouse) => app.on_mouse(mouse),
                Msg::Tick => app.on_tick(),
                Msg::Resize => None,
                Msg::Snapshot(result) => {
                    app.set_snapshot(result);
                    None
                }
                Msg::Identity(identity) => {
                    app.identity = Some(identity);
                    None
                }
                Msg::Events(generation, events) => {
                    app.push_events(generation, events);
                    None
                }
                Msg::Older(generation, events, reached_start) => {
                    app.prepend_older(generation, events, reached_start);
                    None
                }
                Msg::EventsError(generation, error) => {
                    app.set_events_error(generation, error);
                    None
                }
                Msg::Tags(sequence, tags) => {
                    app.set_tags(sequence, tags);
                    None
                }
            };
            match action {
                None => {}
                Some(Action::LoadOlder { before }) => {
                    tokio::spawn(load_older(
                        Arc::clone(conn),
                        tx.clone(),
                        session,
                        app.generation,
                        app.events_context.clone(),
                        before,
                    ));
                }
                Some(Action::LoadTags { sequence }) => {
                    tokio::spawn(load_tags(
                        Arc::clone(conn),
                        tx.clone(),
                        session,
                        app.events_context.clone(),
                        sequence,
                    ));
                }
                Some(Action::Copy(text)) => copy_to_clipboard(&text),
                Some(action) => break 'session action,
            }
        }
        // Context or filter changed: retarget the loader.
        if target_tx.borrow().generation != app.generation {
            let _ = target_tx.send(target_of(&app));
        }
    };

    for task in tasks {
        task.abort();
    }
    Ok(action)
}

/// OSC 52: asks the terminal itself to set the clipboard, so it works over
/// SSH and needs no platform clipboard code. Ghostty, kitty, iTerm2, WezTerm
/// and tmux (with `set-clipboard on`) honour it.
fn copy_to_clipboard(text: &str) {
    let encoded = base64::engine::general_purpose::STANDARD.encode(text);
    let mut out = std::io::stdout();
    let _ = write!(out, "\x1b]52;c;{encoded}\x07");
    let _ = out.flush();
}

async fn poll_snapshot(conn: Arc<Connection>, tx: mpsc::UnboundedSender<Tagged>, session: u64) {
    loop {
        let result = conn
            .admin_get("/api/v1/snapshot")
            .await
            .map_err(|e| format!("{e:#}"));
        if tx.send((session, Msg::Snapshot(result))).is_err() {
            return;
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

async fn resolve_identity(conn: Arc<Connection>, tx: mpsc::UnboundedSender<Tagged>, session: u64) {
    // Runs the token command if there is one — off the UI loop, since
    // gcloud takes its time.
    let Ok(Some(token)) = conn.tokens.token().await else {
        return;
    };
    if let Some(claims) = jwt_claims(&token)
        && let Some(who) = claims
            .get("email")
            .or_else(|| claims.get("sub"))
            .and_then(Value::as_str)
    {
        let _ = tx.send((session, Msg::Identity(who.to_string())));
    }
}

/// Follows whatever the UI has open. Polls `Source` from the last seen
/// position: no stream flow-control to manage, and a second of latency is
/// invisible at reading speed.
///
/// Unfiltered, it starts one page back from the head and the UI pages
/// further on demand. Filtered, it reads matches from the start of the
/// context — a filter is a question about the whole log, not the last page.
async fn follow_events(
    conn: Arc<Connection>,
    tx: mpsc::UnboundedSender<Tagged>,
    session: u64,
    mut target_rx: watch::Receiver<Target>,
) {
    let mut target = target_rx.borrow_and_update().clone();
    let mut next: Option<i64> = None;
    loop {
        let read = async {
            // `at_start`: this read began at the oldest event there is, so
            // the UI has nothing older to page in.
            let (from, at_start) = match next {
                Some(from) => (from, false),
                None => {
                    let tail = conn.tail(&target.context).await?;
                    if target.criteria.is_empty() {
                        let head = conn.head(&target.context).await?;
                        let from = (head - PAGE as i64).max(tail);
                        (from, from <= tail)
                    } else {
                        (tail, true)
                    }
                }
            };
            let events = conn
                .source(&target.context, from, &target.criteria, MAX_EVENTS)
                .await?;
            anyhow::Ok((from, at_start, events))
        };
        let pause = match read.await {
            Ok((from, at_start, events)) => {
                next = Some(events.last().map(|e| e.sequence + 1).unwrap_or(from));
                let busy = !events.is_empty();
                if at_start {
                    let _ = tx.send((session, Msg::Older(target.generation, vec![], true)));
                }
                if busy
                    && tx
                        .send((session, Msg::Events(target.generation, events)))
                        .is_err()
                {
                    return;
                }
                if busy { 250 } else { 1000 }
            }
            Err(error) => {
                let msg = Msg::EventsError(target.generation, format!("{error:#}"));
                if tx.send((session, msg)).is_err() {
                    return;
                }
                3000
            }
        };
        tokio::select! {
            changed = target_rx.changed() => {
                if changed.is_err() {
                    return;
                }
                target = target_rx.borrow_and_update().clone();
                next = None;
            }
            _ = tokio::time::sleep(Duration::from_millis(pause)) => {}
        }
    }
}

/// One page of history ending just before `before`.
async fn load_older(
    conn: Arc<Connection>,
    tx: mpsc::UnboundedSender<Tagged>,
    session: u64,
    generation: u64,
    context: String,
    before: i64,
) {
    let read = async {
        let tail = conn.tail(&context).await?;
        let from = (before - PAGE as i64).max(tail);
        let wanted = (before - from) as usize;
        let mut events = conn.source(&context, from, &[], wanted).await?;
        events.retain(|e| e.sequence < before);
        anyhow::Ok((events, from <= tail))
    };
    let msg = match read.await {
        Ok((events, reached_start)) => Msg::Older(generation, events, reached_start),
        Err(error) => Msg::EventsError(generation, format!("{error:#}")),
    };
    let _ = tx.send((session, msg));
}

async fn load_tags(
    conn: Arc<Connection>,
    tx: mpsc::UnboundedSender<Tagged>,
    session: u64,
    context: String,
    sequence: i64,
) {
    let tags = conn
        .tags(&context, sequence)
        .await
        .map(|tags| tags.iter().map(output::tag_text).collect())
        .map_err(|e| format!("{e:#}"));
    let _ = tx.send((session, Msg::Tags(sequence, tags)));
}
