//! Drawing. One calm screen: brand and connection on top, views as tabs, a
//! list with its preview beside it, and the input box — always there — at
//! the bottom. Chrome is kept to a rule and a rounded box; colour means
//! something (gold = you are here, teal/amber/red = state).
//!
//! Drawing also records where clickable things ended up (`app.hits`), which
//! is what makes the mouse exact instead of approximate.

use ratatui::Frame;
use ratatui::layout::{Alignment, Constraint, Layout, Rect};
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{
    Block, BorderType, Borders, Clear, Paragraph, Scrollbar, ScrollbarOrientation, ScrollbarState,
    Wrap,
};
use serde_json::Value;

use super::app::{App, Hits, View};
use super::theme::{self, SPINNER};
use crate::commands::processor_row;
use crate::output::{self, bool_of, items, num_of, str_of};

/// Below this body width the preview pane is dropped (enter still zooms).
const PREVIEW_MIN_WIDTH: u16 = 100;

pub fn draw(frame: &mut Frame, app: &mut App) {
    app.hits = Hits::default();
    let [header, tabs, body, input, footer] = Layout::vertical([
        Constraint::Length(1),
        Constraint::Length(2),
        Constraint::Min(3),
        Constraint::Length(3),
        Constraint::Length(1),
    ])
    .areas(frame.area());

    draw_header(frame, app, header);
    draw_tabs(frame, app, tabs);
    if app.snapshot.is_none() && app.error.is_none() && app.events.is_empty() {
        draw_splash(frame, app, body);
        draw_input(frame, app, input);
        draw_footer(frame, app, footer);
        return;
    }
    match app.view {
        View::Events => draw_events_view(frame, app, body),
        _ => draw_resource_view(frame, app, body),
    }
    draw_input(frame, app, input);
    draw_footer(frame, app, footer);
    if app.palette_open() {
        draw_palette(frame, app, input);
    }
    if app.show_help {
        draw_help(frame);
    }
}

// ───────────────────────────── chrome ─────────────────────────────

fn draw_header(frame: &mut Frame, app: &App, area: Rect) {
    // A band, not a line: the one place the product's colours are solid.
    frame.render_widget(
        Paragraph::new("").style(Style::new().bg(theme::SURFACE)),
        area,
    );
    let mut left = vec![
        Span::styled(" ◆ KRONOS", theme::pill(theme::GOLD)),
        Span::styled("DB ", theme::pill(theme::GOLD).fg(theme::SURFACE)),
        Span::styled(
            format!("  {}", app.profile_name),
            theme::text().bg(theme::SURFACE),
        ),
        Span::styled(
            format!("  {}", app.endpoint),
            theme::dim().bg(theme::SURFACE),
        ),
    ];
    if let Some(identity) = &app.identity {
        left.push(Span::styled(
            format!("  {identity}"),
            theme::dim().bg(theme::SURFACE),
        ));
    }
    frame.render_widget(Paragraph::new(Line::from(left)), area);

    let right = match (&app.error, &app.snapshot) {
        (Some(error), _) => vec![
            Span::styled(" OFFLINE ", theme::pill(theme::RED)),
            Span::styled(format!(" {error} "), theme::bad().bg(theme::SURFACE)),
        ],
        (None, None) => vec![Span::styled(
            format!("{} connecting ", SPINNER[app.tick % SPINNER.len()]),
            theme::dim().bg(theme::SURFACE),
        )],
        (None, Some(snapshot)) => {
            let (node, raft) = (&snapshot["node"], &snapshot["cluster"]["raft"]);
            let ready = bool_of(node, "ready");
            let role = str_of(raft, "state").to_lowercase();
            vec![
                Span::styled(
                    format!("{} · term {} ", str_of(node, "name"), num_of(raft, "term")),
                    theme::dim().bg(theme::SURFACE),
                ),
                Span::styled(
                    format!(" {} ", if ready { role.as_str() } else { "not ready" }),
                    theme::pill(if ready { theme::TEAL } else { theme::RED }),
                ),
                Span::styled(" ", theme::dim().bg(theme::SURFACE)),
            ]
        }
    };
    frame.render_widget(
        Paragraph::new(Line::from(right)).alignment(Alignment::Right),
        area,
    );
}
fn draw_tabs(frame: &mut Frame, app: &mut App, area: Rect) {
    let mut spans = vec![Span::raw(" ")];
    let mut x = area.x + 1;
    for view in View::ALL {
        let count = match view {
            View::Events => app.contexts().len(),
            _ => {
                // Counts ignore what's typed: they describe the server.
                let typed = std::mem::take(&mut app.input);
                let count = app.rows(view).len();
                app.input = typed;
                count
            }
        };
        let label = format!(" {} ", view.title());
        let badge = format!("{count} ");
        let width = (label.chars().count() + badge.chars().count()) as u16;
        app.hits.tabs.push((Rect::new(x, area.y, width, 1), view));
        if view == app.view {
            spans.push(Span::styled(label, theme::pill(theme::GOLD)));
            spans.push(Span::styled(
                badge,
                theme::pill(theme::GOLD).fg(theme::SURFACE),
            ));
        } else {
            spans.push(Span::styled(label, theme::dim()));
            spans.push(Span::styled(
                badge,
                if count > 0 {
                    theme::accent()
                } else {
                    theme::faint()
                },
            ));
        }
        spans.push(Span::raw(" "));
        x += width + 1;
    }
    let block = Block::new()
        .borders(Borders::BOTTOM)
        .border_style(Style::new().fg(theme::BORDER));
    frame.render_widget(Paragraph::new(Line::from(spans)).block(block), area);
}
fn draw_input(frame: &mut Frame, app: &App, area: Rect) {
    let active = !app.input.is_empty();
    let block = Block::bordered()
        .border_type(BorderType::Rounded)
        .border_style(Style::new().fg(if active { theme::GOLD } else { theme::BORDER }));
    let mut spans = vec![Span::styled(" › ", theme::accent())];
    if app.input.is_empty() {
        spans.push(Span::styled("█ ", theme::faint()));
        spans.push(Span::styled(
            match app.view {
                View::Events => "type to filter   orderId=o-1 type=OrderPaid",
                _ => "type to narrow this list",
            },
            theme::faint(),
        ));
    } else {
        spans.push(Span::styled(app.input.clone(), theme::text()));
        spans.push(Span::styled("█", theme::accent()));
    }
    frame.render_widget(Paragraph::new(Line::from(spans)).block(block), area);
}

fn draw_footer(frame: &mut Frame, app: &App, area: Rect) {
    let left = if let Some(error) = &app.input_error {
        Span::styled(format!("  {error}"), theme::bad())
    } else if let Some((toast, _)) = &app.toast {
        Span::styled(format!(" ✓ {toast} "), theme::pill(theme::TEAL))
    } else {
        Span::styled(
            if app.palette_open() {
                "  ↑↓ choose · enter run · tab complete · esc cancel"
            } else if app.zoom {
                "  ↑↓ scroll · ←→ previous/next event · ctrl+y copy · esc back"
            } else if app.view == View::Events {
                "  ↑↓ select · enter read · / commands · ? help · esc back · tab views"
            } else {
                "  ↑↓ select · / commands · ? help · tab views"
            },
            theme::faint(),
        )
    };
    frame.render_widget(Paragraph::new(Line::from(left)), area);

    if app.view == View::Events
        && let Some(index) = app.selected_index()
    {
        let position = match (app.log_span(), &app.filter, app.events.get(index)) {
            (_, Some(_), _) => format!("match {} of {} ", index + 1, app.events.len()),
            (Some((tail, head)), None, Some(event)) if head > tail => format!(
                "event {} of {} · {}% ",
                event.sequence - tail + 1,
                head - tail,
                (event.sequence - tail + 1) * 100 / (head - tail)
            ),
            _ => format!("{} of {} ", index + 1, app.events.len()),
        };
        frame.render_widget(
            Paragraph::new(Span::styled(position, theme::dim())).alignment(Alignment::Right),
            area,
        );
    }
}

fn draw_palette(frame: &mut Frame, app: &App, input: Rect) {
    let commands = app.palette();
    let rows = commands.len().clamp(1, 9) as u16;
    let height = rows + 2;
    let area = Rect::new(
        input.x,
        input.y.saturating_sub(height),
        input.width.min(110),
        height.min(input.y),
    );
    // Keep the highlighted command on screen in a long list.
    let first = app.palette_index.saturating_sub(rows as usize - 1);
    let mut lines: Vec<Line> = commands
        .iter()
        .enumerate()
        .skip(first)
        .take(rows as usize)
        .map(|(index, command)| {
            let selected = index == app.palette_index;
            let line = Line::from(vec![
                Span::styled(if selected { " ▌ " } else { "   " }, theme::accent()),
                Span::styled(
                    format!("/{:<28}", command.label),
                    if selected {
                        theme::heading()
                    } else {
                        theme::text()
                    },
                ),
                Span::styled(command.hint.clone(), theme::dim()),
            ]);
            if selected {
                line.style(Style::new().bg(theme::SELECTED))
            } else {
                line
            }
        })
        .collect();
    if lines.is_empty() {
        lines.push(Line::styled("   no matching command", theme::faint()));
    }
    frame.render_widget(Clear, area);
    frame.render_widget(
        Paragraph::new(lines).block(
            Block::bordered()
                .border_type(BorderType::Rounded)
                .border_style(theme::accent())
                .title(Span::styled(" commands ", theme::accent())),
        ),
        area,
    );
}

fn draw_help(frame: &mut Frame) {
    let key = |keys: &str, what: &str| {
        Line::from(vec![
            Span::styled(format!("  {keys:<16}"), theme::accent()),
            Span::styled(what.to_string(), theme::text()),
        ])
    };
    let lines = vec![
        Line::styled(" Just type", theme::heading()),
        key("any text", "filters what you're looking at, as you type"),
        key(
            "orderId=o-1",
            "events carrying that tag (several = all of them)",
        ),
        key(
            "type=OrderPaid",
            "events of that type (several = any of them)",
        ),
        key("/", "commands: switch context, profile, copy, …"),
        Line::raw(""),
        Line::styled(" Move", theme::heading()),
        key(
            "↑ ↓  pgup pgdn",
            "select · the wheel scrolls without selecting",
        ),
        key("home  end", "oldest loaded · back to live"),
        key("enter", "read the selected event full-width"),
        key("← →", "previous / next event while reading"),
        key("tab", "next view · click a tab, a row, or a tag"),
        Line::raw(""),
        Line::styled(" Always", theme::heading()),
        key("esc", "step back: close, un-zoom, clear filter, go live"),
        key("ctrl+y", "copy the selected event as JSON"),
        key("ctrl+c", "clear the box, then quit"),
        Line::raw(""),
        Line::styled(
            "  Everything here is also a command: kronos --help",
            theme::faint(),
        ),
    ];
    let area = frame.area();
    let width = 74.min(area.width);
    let height = (lines.len() as u16 + 2).min(area.height);
    let popup = Rect::new(
        area.x + (area.width - width) / 2,
        area.y + (area.height - height) / 2,
        width,
        height,
    );
    frame.render_widget(Clear, popup);
    frame.render_widget(
        Paragraph::new(lines).block(
            Block::bordered()
                .border_type(BorderType::Rounded)
                .border_style(theme::accent())
                .title(Span::styled(" kronos ", theme::heading())),
        ),
        popup,
    );
}

/// Splits a body into list and preview, or gives the list everything.
fn split(area: Rect, zoom: bool) -> (Rect, Option<Rect>) {
    if zoom {
        return (Rect::default(), Some(area));
    }
    if area.width < PREVIEW_MIN_WIDTH {
        return (area, None);
    }
    let [list, preview] =
        Layout::horizontal([Constraint::Percentage(58), Constraint::Min(0)]).areas(area);
    (list, Some(preview))
}

/// The list's right edge: a rule that is also the scrollbar, so the list
/// and its preview are divided by one line, not two. `len`/`top` are in
/// whatever unit describes the whole thing being scrolled.
fn scrollbar(frame: &mut Frame, area: Rect, len: usize, height: usize, top: usize) {
    if len <= height {
        let rule = vec![Line::styled("│", Style::new().fg(theme::BORDER)); area.height as usize];
        let edge = Rect::new(area.right().saturating_sub(1), area.y, 1, area.height);
        frame.render_widget(Paragraph::new(rule), edge);
        return;
    }
    let mut state = ScrollbarState::new(len - height).position(top.min(len - height));
    frame.render_stateful_widget(
        Scrollbar::new(ScrollbarOrientation::VerticalRight)
            .begin_symbol(None)
            .end_symbol(None)
            .track_symbol(Some("│"))
            .track_style(Style::new().fg(theme::BORDER))
            .thumb_symbol("┃")
            .thumb_style(theme::accent()),
        area,
        &mut state,
    );
}

// ───────────────────────────── events ─────────────────────────────

fn draw_events_view(frame: &mut Frame, app: &mut App, area: Rect) {
    let [status, body] = Layout::vertical([Constraint::Length(1), Constraint::Min(1)]).areas(area);
    draw_events_status(frame, app, status);

    let (list, preview) = split(body, app.zoom);
    if list.height > 0 {
        draw_event_list(frame, app, list);
    }
    if let Some(preview) = preview {
        draw_event_preview(frame, app, preview);
    }
}

/// Where you are and what the list is doing, in one line.
/// Where you are and what the list is doing, in one line.
fn draw_events_status(frame: &mut Frame, app: &mut App, area: Rect) {
    let context = app
        .contexts()
        .into_iter()
        .find(|c| str_of(c, "name") == app.events_context);
    let name = format!(" {} ▾ ", app.events_context);
    app.hits.context = Rect::new(area.x, area.y, name.chars().count() as u16, 1);
    let mut left = vec![Span::styled(name, theme::heading())];
    if let Some(context) = &context {
        left.push(Span::styled(
            format!(
                " {} events · {}",
                num_of(context, "head").saturating_sub(num_of(context, "tail")),
                output::bytes(num_of(context, "data_bytes"))
            ),
            theme::dim(),
        ));
        if bool_of(context, "poisoned") {
            left.push(Span::styled(" POISONED ", theme::pill(theme::RED)));
        }
    }
    if let Some(filter) = &app.filter {
        left.push(Span::styled("   ", theme::dim()));
        left.push(Span::styled(
            format!(" {} ", filter.text),
            theme::highlight(),
        ));
        left.push(Span::styled(
            format!(
                " {} match{}",
                app.events.len(),
                if app.events.len() == 1 { "" } else { "es" }
            ),
            theme::dim(),
        ));
    }
    frame.render_widget(Paragraph::new(Line::from(left)), area);

    let spinner = SPINNER[app.tick % SPINNER.len()];
    // The live dot breathes; a paused list is amber; loading spins.
    let right = if app.loading {
        vec![Span::styled(
            format!(" {spinner} LOADING "),
            theme::pill(theme::MUTED),
        )]
    } else if app.loading_older {
        vec![Span::styled(
            format!(" {spinner} OLDER "),
            theme::pill(theme::MUTED),
        )]
    } else if app.is_live() {
        let dot = if (app.tick / 5).is_multiple_of(2) {
            "●"
        } else {
            "◉"
        };
        vec![Span::styled(
            format!(" {dot} LIVE "),
            theme::pill(theme::TEAL),
        )]
    } else {
        let mut spans = Vec::new();
        if app.unseen > 0 {
            spans.push(Span::styled(
                format!("{} new · end for live ", app.unseen),
                theme::warn(),
            ));
        } else if app.viewport_top(app.page_rows.max(1)) == 0 && app.reached_start {
            spans.push(Span::styled(
                "start of the log · end for live ",
                theme::dim(),
            ));
        } else {
            spans.push(Span::styled("end for live ", theme::dim()));
        }
        spans.push(Span::styled(" ⏸ READING ", theme::pill(theme::AMBER)));
        spans
    };
    let mut right = right;
    right.push(Span::raw(" "));
    frame.render_widget(
        Paragraph::new(Line::from(right)).alignment(Alignment::Right),
        area,
    );
}
fn draw_event_list(frame: &mut Frame, app: &mut App, area: Rect) {
    let [header, rows] = Layout::vertical([Constraint::Length(1), Constraint::Min(1)]).areas(area);
    frame.render_widget(
        Paragraph::new(Span::styled(
            format!("  {:>9}  {:<8}  {:<24}  PAYLOAD", "SEQ", "TIME", "TYPE"),
            theme::faint(),
        )),
        header,
    );

    let height = rows.height as usize;
    app.page_rows = height;
    if let Some(error) = &app.events_error {
        frame.render_widget(
            Paragraph::new(vec![
                Line::raw(""),
                Line::styled(format!("  {error}"), theme::bad()),
            ])
            .wrap(Wrap { trim: false }),
            rows,
        );
        return;
    }
    if app.events.is_empty() {
        let message = match (&app.filter, app.loading) {
            (_, true) => String::new(),
            (Some(filter), _) => {
                format!("  Nothing matches {} — esc clears the filter", filter.text)
            }
            (None, _) => "  No events in this context yet".to_string(),
        };
        frame.render_widget(
            Paragraph::new(vec![Line::raw(""), Line::styled(message, theme::dim())]),
            rows,
        );
        return;
    }

    let top = app.viewport_top(height);
    let selected = app.selected_index();
    let payload_width = (rows.width as usize).saturating_sub(52);
    let needles = app.filter_needles();
    let lines: Vec<Line> = app
        .events
        .iter()
        .enumerate()
        .skip(top)
        .take(height)
        .map(|(index, event)| {
            let inner = event.event.clone().unwrap_or_default();
            let is_selected = Some(index) == selected;
            let fresh = app.is_fresh(event.sequence);
            let time = output::timestamp(inner.timestamp);
            let color = theme::type_color(&inner.name);
            let payload_style = if is_selected {
                theme::text()
            } else {
                theme::dim()
            };
            let mut spans = vec![
                Span::styled(if is_selected { "▌ " } else { "  " }, theme::accent()),
                Span::styled(format!("{:>9}  ", event.sequence), theme::dim()),
                // The date lives in the preview; the list shows the clock.
                Span::styled(
                    format!("{:<8}  ", time.get(6..).unwrap_or(&time)),
                    theme::faint(),
                ),
                Span::styled(
                    format!("{:<24.24}  ", inner.name),
                    Style::new().fg(color).add_modifier(if is_selected {
                        Modifier::BOLD
                    } else {
                        Modifier::empty()
                    }),
                ),
            ];
            spans.extend(highlight(
                &output::payload_preview(&inner.payload, payload_width),
                &needles,
                payload_style,
            ));
            let line = Line::from(spans);
            if is_selected {
                line.style(Style::new().bg(theme::SELECTED))
            } else if fresh {
                line.style(Style::new().bg(theme::FRESH))
            } else {
                line
            }
        })
        .collect();
    frame.render_widget(Paragraph::new(lines), rows);
    app.hits.list = rows;
    app.hits.list_top = top;
    // Unfiltered, the bar describes the whole log, not just what's loaded:
    // that is the "where am I" people actually mean.
    match (app.log_span(), app.events.get(top)) {
        (Some((tail, head)), Some(first)) if app.filter.is_none() => scrollbar(
            frame,
            rows,
            (head - tail).max(0) as usize,
            height,
            (first.sequence - tail).max(0) as usize,
        ),
        _ => scrollbar(frame, rows, app.events.len(), height, top),
    }
}

/// Splits `text` so that every occurrence of a needle is lit up — the
/// filter's values inside a payload, an event's tag values in its preview.
fn highlight(text: &str, needles: &[String], base: Style) -> Vec<Span<'static>> {
    let mut spans = Vec::new();
    let mut rest = text;
    while !rest.is_empty() {
        // Earliest match wins; ties go to the longest needle.
        let next = needles
            .iter()
            .filter_map(|n| rest.find(n.as_str()).map(|at| (at, n.len())))
            .min_by_key(|(at, len)| (*at, std::cmp::Reverse(*len)));
        match next {
            Some((at, len)) => {
                if at > 0 {
                    spans.push(Span::styled(rest[..at].to_string(), base));
                }
                spans.push(Span::styled(
                    rest[at..at + len].to_string(),
                    theme::highlight(),
                ));
                rest = &rest[at + len..];
            }
            None => {
                spans.push(Span::styled(rest.to_string(), base));
                break;
            }
        }
    }
    spans
}

/// The screen before anything has arrived: the mark, and what we're doing.
fn draw_splash(frame: &mut Frame, app: &App, area: Rect) {
    let lines = vec![
        Line::from(vec![
            Span::styled("◆ ", theme::accent()),
            Span::styled("K R O N O S ", theme::text().add_modifier(Modifier::BOLD)),
            Span::styled("D B", theme::heading()),
        ]),
        Line::raw(""),
        Line::styled(
            format!(
                "{} connecting to {} · {}",
                SPINNER[app.tick % SPINNER.len()],
                app.profile_name,
                app.endpoint
            ),
            theme::dim(),
        ),
    ];
    let height = lines.len() as u16;
    let y = area.y + area.height.saturating_sub(height) / 2;
    frame.render_widget(
        Paragraph::new(lines).alignment(Alignment::Center),
        Rect::new(area.x, y, area.width, height.min(area.height)),
    );
}

/// `  "key": value,` with the key, strings, and literals told apart.
fn json_line(line: &str, needles: &[String]) -> Line<'static> {
    let indent_len = line.len() - line.trim_start().len();
    let (indent, rest) = line.split_at(indent_len);
    let mut spans = vec![Span::raw(indent.to_string())];
    let value_style = |value: &str| {
        let bare = value.trim_end_matches(',');
        if bare.starts_with('"') {
            theme::text()
        } else if matches!(bare, "{" | "}" | "[" | "]" | "{}" | "[]") {
            theme::faint()
        } else {
            Style::new().fg(theme::AMBER) // numbers, true/false/null
        }
    };
    match rest.split_once("\": ") {
        Some((key, value)) if rest.starts_with('"') => {
            spans.push(Span::styled(
                format!("{key}\""),
                Style::new().fg(theme::BLUE),
            ));
            spans.push(Span::styled(": ", theme::faint()));
            spans.extend(highlight(value, needles, value_style(value)));
        }
        _ => spans.push(Span::styled(rest.to_string(), value_style(rest))),
    }
    Line::from(spans)
}

fn draw_event_preview(frame: &mut Frame, app: &mut App, area: Rect) {
    // No border of its own: the list's scrollbar is the divider.
    let inner_area = area;
    app.hits.preview = area;
    let Some(event) = app.selected_event().cloned() else {
        return;
    };
    let inner = event.event.clone().unwrap_or_default();

    let tag_values: Vec<String> = match app.tags.get(&event.sequence) {
        Some(Ok(tags)) => tags
            .iter()
            .filter_map(|t| t.split_once('=').map(|(_, v)| v.to_string()))
            .filter(|v| v.len() >= 2)
            .collect(),
        _ => vec![],
    };
    let mut lines = vec![
        Line::from(vec![
            Span::styled(format!(" #{} ", event.sequence), theme::dim()),
            Span::styled(
                format!(" {} ", inner.name),
                theme::pill(theme::type_color(&inner.name)),
            ),
        ]),
        Line::styled(
            format!(
                " {} UTC · v{}",
                output::timestamp(inner.timestamp),
                if inner.version.is_empty() {
                    "—"
                } else {
                    &inner.version
                }
            ),
            theme::dim(),
        ),
        Line::styled(format!(" {}", inner.identifier), theme::faint()),
        Line::raw(""),
        Line::styled(" TAGS", theme::faint()),
    ];
    match app.tags.get(&event.sequence).cloned() {
        None => lines.push(Line::styled(
            format!(" {} ", SPINNER[app.tick % SPINNER.len()]),
            theme::faint(),
        )),
        Some(Err(error)) => lines.push(Line::styled(format!(" {error}"), theme::bad())),
        Some(Ok(tags)) if tags.is_empty() => lines.push(Line::styled(" none", theme::faint())),
        Some(Ok(tags)) => {
            for tag in tags {
                // Clickable: a tag is the natural next question.
                let y = inner_area.y as i32 + lines.len() as i32 - app.preview_scroll as i32;
                if y >= inner_area.y as i32 && y < (inner_area.y + inner_area.height) as i32 {
                    let width = (tag.chars().count() as u16 + 1).min(inner_area.width);
                    app.hits
                        .tags
                        .push((Rect::new(inner_area.x, y as u16, width, 1), tag.clone()));
                }
                lines.push(Line::from(Span::styled(
                    format!(" {tag}"),
                    theme::accent().add_modifier(Modifier::UNDERLINED),
                )));
            }
        }
    }
    if !inner.metadata.is_empty() {
        lines.push(Line::raw(""));
        lines.push(Line::styled(" METADATA", theme::faint()));
        let mut metadata: Vec<_> = inner.metadata.iter().collect();
        metadata.sort();
        for (key, value) in metadata {
            lines.push(Line::from(vec![
                Span::styled(format!(" {key} "), Style::new().fg(theme::BLUE)),
                Span::styled(value.clone(), theme::text()),
            ]));
        }
    }
    lines.push(Line::raw(""));
    lines.push(Line::styled(" PAYLOAD", theme::faint()));
    match std::str::from_utf8(&inner.payload) {
        Ok(text) => match serde_json::from_str::<Value>(text)
            .ok()
            .and_then(|json| serde_json::to_string_pretty(&json).ok())
        {
            Some(pretty) => lines.extend(pretty.lines().map(|l| {
                let mut line = json_line(l, &tag_values);
                line.spans.insert(0, Span::raw(" "));
                line
            })),
            None => lines.push(Line::styled(format!(" {text}"), theme::text())),
        },
        Err(_) => lines.push(Line::styled(
            format!(" {} bytes, not UTF-8", inner.payload.len()),
            theme::dim(),
        )),
    }
    // Don't scroll past the end into emptiness.
    let max_scroll = (lines.len() as u16).saturating_sub(inner_area.height.min(3));
    app.preview_scroll = app.preview_scroll.min(max_scroll);
    frame.render_widget(
        Paragraph::new(lines)
            .wrap(Wrap { trim: false })
            .scroll((app.preview_scroll, 0)),
        inner_area,
    );
}

// ─────────────────────────── other views ───────────────────────────

fn draw_resource_view(frame: &mut Frame, app: &mut App, area: Rect) {
    let rows = app.rows(app.view);
    let (list, preview) = split(area, false);
    let index = View::ALL.iter().position(|v| *v == app.view).unwrap_or(0);
    let selected = app.selected[index];

    let [header, body] = Layout::vertical([Constraint::Length(1), Constraint::Min(1)]).areas(list);
    frame.render_widget(
        Paragraph::new(Span::styled(
            match app.view {
                View::Clients => "  COMPONENT                 CLIENT",
                View::Handlers => "  KIND  NAME                        HANDLERS",
                View::Processors => "  PROCESSOR                   STATE",
                _ => "  ID  ADDRESS                   ROLE",
            },
            theme::faint(),
        )),
        header,
    );
    if rows.is_empty() {
        let message = if !app.input.trim().is_empty() && !app.palette_open() {
            format!(
                "  Nothing here matches \"{}\" — esc clears",
                app.input.trim()
            )
        } else {
            match app.view {
                View::Clients => "  No clients are connected to this node",
                View::Handlers => "  No command or query handlers are registered",
                View::Processors => "  No event processors have reported in",
                _ => "  Nothing to show yet",
            }
            .to_string()
        };
        frame.render_widget(
            Paragraph::new(vec![Line::raw(""), Line::styled(message, theme::dim())]),
            body,
        );
    } else {
        let height = body.height as usize;
        let top = selected.saturating_sub(height.saturating_sub(1));
        let lines: Vec<Line> = rows
            .iter()
            .enumerate()
            .skip(top)
            .take(height)
            .map(|(i, row)| {
                let mut line = row_line(app.view, row);
                line.spans.insert(
                    0,
                    Span::styled(if i == selected { "▌ " } else { "  " }, theme::accent()),
                );
                if i == selected {
                    line.style(Style::new().bg(theme::SELECTED))
                } else {
                    line
                }
            })
            .collect();
        frame.render_widget(Paragraph::new(lines), body);
        app.hits.list = body;
        app.hits.list_top = top;
        scrollbar(frame, body, rows.len(), height, top);
    }

    if let Some(preview) = preview {
        let inner = preview;
        let lines = match (app.view, rows.get(selected), &app.snapshot) {
            (View::Cluster, _, Some(snapshot)) => cluster_lines(app, snapshot),
            (View::Clients, Some(row), Some(snapshot)) => client_lines(snapshot, row),
            (View::Handlers, Some(row), _) => handler_lines(row),
            (View::Processors, Some(row), _) => processor_lines(row),
            _ => vec![],
        };
        frame.render_widget(Paragraph::new(lines).wrap(Wrap { trim: false }), inner);
    }
}

fn state_span(ok: bool, good: &str, bad: &str) -> Span<'static> {
    if ok {
        Span::styled(good.to_string(), theme::good())
    } else {
        Span::styled(bad.to_string(), theme::bad())
    }
}

fn kv(key: &str, value: impl Into<String>) -> Line<'static> {
    Line::from(vec![
        Span::styled(format!(" {key:<14}"), theme::dim()),
        Span::styled(value.into(), theme::text()),
    ])
}

fn section(text: &str) -> Line<'static> {
    Line::styled(format!(" {}", text.to_uppercase()), theme::faint())
}

fn row_line(view: View, row: &Value) -> Line<'static> {
    match view {
        View::Clients => Line::from(vec![
            state_span(bool_of(row, "streaming"), "● ", "○ "),
            Span::styled(format!("{:<24}", str_of(row, "component")), theme::text()),
            Span::styled(str_of(row, "client_id").to_string(), theme::dim()),
        ]),
        View::Handlers => {
            let handlers = items(&row["handlers"]).len();
            Line::from(vec![
                Span::styled(
                    if str_of(row, "kind") == "query" {
                        "query "
                    } else {
                        "cmd   "
                    },
                    theme::dim(),
                ),
                Span::styled(format!("{:<28}", str_of(row, "name")), theme::text()),
                state_span(handlers > 0, &format!("×{handlers}"), "no handler"),
            ])
        }
        View::Processors => {
            let state = processor_row(row)[5].clone();
            let style = match state.as_str() {
                "ERROR" => theme::bad(),
                "caught up" => theme::good(),
                "paused" => theme::dim(),
                _ => theme::warn(),
            };
            Line::from(vec![
                Span::styled(format!("{:<28}", str_of(row, "name")), theme::text()),
                Span::styled(state, style),
            ])
        }
        _ => Line::from(vec![
            Span::styled(format!("{:<4}", num_of(row, "id")), theme::dim()),
            Span::styled(format!("{:<26}", str_of(row, "addr")), theme::text()),
            if bool_of(row, "leader") {
                Span::styled("leader", theme::accent())
            } else if bool_of(row, "voter") {
                Span::styled("voter", theme::dim())
            } else {
                Span::styled("learner", theme::dim())
            },
        ]),
    }
}

fn cluster_lines(app: &App, snapshot: &Value) -> Vec<Line<'static>> {
    let (node, cluster) = (&snapshot["node"], &snapshot["cluster"]);
    let raft = &cluster["raft"];
    let auth: Vec<&str> = items(&node["auth"])
        .iter()
        .filter_map(Value::as_str)
        .collect();
    let mut lines = vec![
        section("node"),
        kv("name", str_of(node, "name")),
        kv("version", str_of(node, "version")),
        kv("uptime", output::duration(num_of(node, "uptime_secs"))),
        Line::from(vec![
            Span::styled(format!(" {:<14}", "ready"), theme::dim()),
            state_span(bool_of(node, "ready"), "yes", "no — no leader known"),
        ]),
        kv("tls", if bool_of(node, "tls") { "on" } else { "off" }),
        Line::from(vec![
            Span::styled(format!(" {:<14}", "auth"), theme::dim()),
            if auth.is_empty() {
                Span::styled("none — open access", theme::bad())
            } else {
                Span::styled(auth.join(", "), theme::text())
            },
        ]),
        Line::raw(""),
        section("raft · control plane"),
        kv("state", str_of(raft, "state")),
        kv("term", num_of(raft, "term").to_string()),
        kv(
            "leader",
            raft.get("leader_id")
                .and_then(Value::as_u64)
                .map(|id| format!("node {id}"))
                .unwrap_or_else(|| "unknown".into()),
        ),
        kv(
            "log",
            format!(
                "applied {} / last {}",
                num_of(raft, "last_applied_index"),
                num_of(raft, "last_log_index")
            ),
        ),
        Line::raw(""),
        section("replication · data plane"),
        kv("epoch", num_of(&cluster["claim"], "epoch").to_string()),
        Line::from(vec![
            Span::styled(format!(" {:<14}", "write gate"), theme::dim()),
            state_span(
                bool_of(&cluster["claim"], "writable"),
                "open",
                "closed — appends are refused",
            ),
        ]),
        Line::raw(""),
        section("you"),
        kv("profile", app.profile_name.clone()),
    ];
    if let Some(identity) = &app.identity {
        lines.push(kv("identity", identity.clone()));
    }
    lines
}

fn client_lines(snapshot: &Value, row: &Value) -> Vec<Line<'static>> {
    let client_id = str_of(row, "client_id");
    let mut lines = vec![
        Line::styled(format!(" {}", str_of(row, "component")), theme::heading()),
        Line::styled(format!(" {client_id}"), theme::faint()),
        Line::raw(""),
        kv("version", str_of(row, "version")),
        kv("connected", output::duration(num_of(row, "connected_secs"))),
        kv(
            "heartbeat",
            format!("{}ms ago", num_of(row, "last_heartbeat_ms")),
        ),
        Line::from(vec![
            Span::styled(format!(" {:<14}", "stream"), theme::dim()),
            state_span(bool_of(row, "streaming"), "open", "closed"),
        ]),
    ];
    // What this client does for the cluster.
    for (title, key) in [
        ("handles commands", "commands"),
        ("handles queries", "queries"),
    ] {
        let names: Vec<String> = items(&snapshot[key])
            .iter()
            .filter(|d| {
                items(&d["handlers"])
                    .iter()
                    .any(|h| str_of(h, "client_id") == client_id)
            })
            .map(|d| format!(" {}  ({})", str_of(d, "name"), str_of(d, "bus")))
            .collect();
        if !names.is_empty() {
            lines.push(Line::raw(""));
            lines.push(section(title));
            lines.extend(names.into_iter().map(|n| Line::styled(n, theme::text())));
        }
    }
    lines
}

fn handler_lines(row: &Value) -> Vec<Line<'static>> {
    let mut lines = vec![
        Line::styled(format!(" {}", str_of(row, "name")), theme::heading()),
        Line::styled(
            format!(" {} on bus {}", str_of(row, "kind"), str_of(row, "bus")),
            theme::faint(),
        ),
        Line::raw(""),
        kv("dispatched", num_of(row, "dispatched").to_string()),
        kv("succeeded", num_of(row, "succeeded").to_string()),
        kv("failed", num_of(row, "failed").to_string()),
        kv("no handler", num_of(row, "no_handler").to_string()),
        kv("no permits", num_of(row, "no_permits").to_string()),
        kv(
            "avg duration",
            format!("{:.2}ms", num_of(row, "avg_duration_us") as f64 / 1000.0),
        ),
        Line::raw(""),
        section("handlers"),
    ];
    let handlers = items(&row["handlers"]);
    if handlers.is_empty() {
        lines.push(Line::styled(
            " none — dispatches fail with no-handler",
            theme::bad(),
        ));
    }
    for h in handlers {
        let permits = h
            .get("available_permits")
            .and_then(Value::as_i64)
            .unwrap_or(0);
        // Permits before the client id: ids are long, and what gets clipped
        // on a narrow terminal should be the part nobody reads.
        lines.push(Line::from(vec![
            Span::styled(
                format!(
                    " {:<22} load {:<4} ",
                    str_of(h, "component"),
                    num_of(h, "load_factor")
                ),
                theme::text(),
            ),
            state_span(
                permits > 0,
                &format!("{permits:>6} permits"),
                "     SATURATED",
            ),
            Span::styled(format!("  {}", str_of(h, "client_id")), theme::faint()),
        ]));
    }
    lines
}

fn processor_lines(row: &Value) -> Vec<Line<'static>> {
    let mut lines = vec![
        Line::styled(format!(" {}", str_of(row, "name")), theme::heading()),
        Line::styled(
            format!(
                " {}{}",
                str_of(row, "mode"),
                if bool_of(row, "streaming") {
                    " · streaming"
                } else {
                    ""
                }
            ),
            theme::faint(),
        ),
    ];
    for (index, instance) in items(&row["instances"]).iter().enumerate() {
        lines.push(Line::raw(""));
        lines.push(Line::from(vec![
            Span::styled(format!(" INSTANCE {}  ", index + 1), theme::faint()),
            if bool_of(instance, "error") {
                Span::styled("error", theme::bad())
            } else {
                state_span(bool_of(instance, "running"), "running", "paused")
            },
        ]));
        for s in items(&instance["segments"]) {
            let state = if !str_of(s, "error_state").is_empty() {
                Span::styled(str_of(s, "error_state").to_string(), theme::bad())
            } else if bool_of(s, "replaying") {
                Span::styled("replaying", theme::warn())
            } else {
                state_span(bool_of(s, "caught_up"), "caught up", "catching up")
            };
            lines.push(Line::from(vec![
                Span::styled(
                    format!(
                        " segment {}/{}   position {:<12} ",
                        num_of(s, "segment_id"),
                        num_of(s, "one_part_of"),
                        s.get("token_position").and_then(Value::as_i64).unwrap_or(0)
                    ),
                    theme::text(),
                ),
                state,
            ]));
        }
    }
    lines
}

#[cfg(test)]
mod tests {
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;
    use ratatui::crossterm::event::{
        KeyCode, KeyModifiers, MouseButton, MouseEvent, MouseEventKind,
    };

    use super::*;
    use crate::tui::app::tests::{app, event, key, type_text};

    fn render_at(app: &mut App, width: u16, height: u16) -> String {
        let mut terminal = Terminal::new(TestBackend::new(width, height)).unwrap();
        terminal.draw(|frame| draw(frame, app)).unwrap();
        let buffer = terminal.backend().buffer().clone();
        buffer
            .content()
            .chunks(width as usize)
            .map(|row| row.iter().map(|cell| cell.symbol()).collect::<String>())
            .collect::<Vec<_>>()
            .join("\n")
    }

    fn render(app: &mut App) -> String {
        render_at(app, 140, 40)
    }

    fn assert_shows(screen: &str, expected: &[&str]) {
        for text in expected {
            assert!(screen.contains(text), "missing {text:?} in:\n{screen}");
        }
    }

    fn click(app: &mut App, rect: Rect) {
        app.on_mouse(MouseEvent {
            kind: MouseEventKind::Down(MouseButton::Left),
            column: rect.x,
            row: rect.y,
            modifiers: KeyModifiers::NONE,
        });
    }

    #[test]
    fn the_events_screen() {
        let mut app = app();
        app.identity = Some("theo@karma.life".into());
        app.set_tags(199, Ok(vec!["orderId=o-7".into()]));
        let screen = render(&mut app);
        assert_shows(
            &screen,
            &[
                "◆ KRONOSDB   prod  http://localhost:50051  theo@karma.life",
                "kronosdb-0 · term 7  leader",
                " Events 2 ",
                " Clients 2 ",
                " orders ▾  5000 events · 1.0 MB",
                "● LIVE",
                "SEQ  TIME      TYPE",
                "▌       199  10:20:41  OrderPlaced",
                "#199  OrderPlaced ",
                "09-20 10:20:41 UTC",
                " orderId=o-7",
                "\"n\": 199",
                "› █ type to filter",
                "event 200 of 5000 · 4%",
            ],
        );
        // The list fills the body and says where you are in it.
        assert!(screen.contains('┃'), "a scrollbar thumb is drawn");
    }

    #[test]
    fn reading_back_shows_where_you_are() {
        let mut app = app();
        render(&mut app); // learn the list height
        app.on_key(key(KeyCode::PageUp));
        let generation = app.generation;
        app.push_events(generation, vec![event(200, "OrderPaid")]);
        let screen = render(&mut app);
        assert_shows(&screen, &["1 new · end for live", "READING"]);
        let sequence = app.selected_event().unwrap().sequence;
        assert_shows(&screen, &[&format!("event {} of 5000", sequence + 1)]);
    }

    #[test]
    fn typing_a_filter_and_the_empty_result() {
        let mut app = app();
        type_text(&mut app, "orderId=o-1");
        assert_shows(&render(&mut app), &["› orderId=o-1█"]);
        app.on_key(key(KeyCode::Enter));
        assert_shows(&render(&mut app), &[" orderId=o-1 ", "LOADING"]);
        let generation = app.generation;
        app.prepend_older(generation, vec![], true);
        assert_shows(
            &render(&mut app),
            &["Nothing matches orderId=o-1 — esc clears the filter"],
        );

        app.on_key(key(KeyCode::Esc));
        type_text(&mut app, "oops");
        app.on_key(key(KeyCode::Enter));
        assert_shows(&render(&mut app), &["is not key=value"]);
    }

    #[test]
    fn palette_and_help() {
        let mut app = app();
        type_text(&mut app, "/con");
        assert_shows(
            &render(&mut app),
            &[
                "commands",
                "▌ /context default",
                "/context orders",
                "5000 events",
                "↑↓ choose · enter run",
            ],
        );
        app.on_key(key(KeyCode::Esc));
        app.on_key(key(KeyCode::Char('?')));
        assert_shows(
            &render(&mut app),
            &["Just type", "step back: close, un-zoom"],
        );
    }

    #[test]
    fn clicking_tabs_rows_and_tags() {
        let mut app = app();
        app.set_tags(199, Ok(vec!["orderId=o-7".into()]));
        render(&mut app);

        let row = Rect::new(app.hits.list.x + 4, app.hits.list.y + 2, 1, 1);
        let expected = app.events[app.hits.list_top + 2].sequence;
        click(&mut app, row);
        assert_eq!(app.selected_event().unwrap().sequence, expected);

        app.on_key(key(KeyCode::End));
        render(&mut app);
        let tag = app.hits.tags[0].0;
        click(&mut app, tag);
        assert_eq!(app.filter.as_ref().unwrap().text, "orderId=o-7");

        render(&mut app);
        let clients = app.hits.tabs[1].0;
        click(&mut app, clients);
        assert_eq!(app.view, View::Clients);
        assert_shows(
            &render(&mut app),
            &[
                "● OrderService",
                "BillingService",
                "type to narrow this list",
            ],
        );
    }

    #[test]
    fn zoom_reads_full_width_and_views_explain_emptiness() {
        let mut app = app();
        app.on_key(key(KeyCode::Up));
        app.on_key(key(KeyCode::Enter));
        let screen = render(&mut app);
        assert_shows(&screen, &["#198  OrderPlaced ", "←→ previous/next event"]);
        assert!(!screen.contains("SEQ  TIME"), "the list steps aside");
        app.on_key(key(KeyCode::Esc));

        for (tabs, expected) in [
            (2, "PlaceOrder"),
            (1, "order-projection"),
            (1, "write gate"),
        ] {
            for _ in 0..tabs {
                app.on_key(key(KeyCode::Tab));
            }
            assert_shows(&render(&mut app), &[expected]);
        }

        app.on_key(key(KeyCode::Tab)); // back round to Events
        app.on_key(key(KeyCode::Tab));
        type_text(&mut app, "zzz");
        assert_shows(
            &render(&mut app),
            &["Nothing here matches \"zzz\" — esc clears"],
        );
    }

    #[test]
    fn errors_and_small_terminals() {
        let mut app = app();
        app.set_events_error(
            app.generation,
            "permission denied: dashboard lacks Read".into(),
        );
        assert_shows(
            &render(&mut app),
            &["permission denied: dashboard lacks Read"],
        );
        app.set_snapshot(Err("connection refused".into()));
        assert_shows(&render(&mut app), &["OFFLINE  connection refused"]);

        // Narrow: no preview pane, still usable. Tiny: no panics.
        let narrow = render_at(&mut app, 80, 24);
        assert!(!narrow.contains("PAYLOAD\n") && narrow.contains("KRONOS"));
        for (width, height) in [(30, 8), (12, 5), (4, 2)] {
            for zoom in [false, true] {
                app.zoom = zoom;
                app.show_help = zoom;
                app.input = if zoom { "/".into() } else { String::new() };
                render_at(&mut app, width, height);
            }
        }
    }
}
