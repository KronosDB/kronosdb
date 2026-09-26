//! The KronosDB look, taken from the admin console's design tokens
//! (`kronosdb-server/assets/app.css`) so the two read as one product.
//!
//! The terminal's own background is left alone — only surfaces the UI owns
//! (selection, the input box, popups) are painted.

use ratatui::style::{Color, Modifier, Style};

pub const GOLD: Color = Color::Rgb(0xc8, 0xa4, 0x4e);
pub const TEXT: Color = Color::Rgb(0xe9, 0xe9, 0xec);
pub const TEXT2: Color = Color::Rgb(0x86, 0x86, 0x8f);
pub const MUTED: Color = Color::Rgb(0x4e, 0x4e, 0x58);
pub const BORDER: Color = Color::Rgb(0x23, 0x23, 0x30);
/// Gold at ~14% over the base: the selected row.
pub const SELECTED: Color = Color::Rgb(0x2a, 0x25, 0x18);
pub const BLUE: Color = Color::Rgb(0x6b, 0x8f, 0xd4);
pub const TEAL: Color = Color::Rgb(0x5e, 0xea, 0xd4);
pub const AMBER: Color = Color::Rgb(0xfb, 0xbf, 0x24);
pub const RED: Color = Color::Rgb(0xf8, 0x71, 0x71);
pub const LAVENDER: Color = Color::Rgb(0xb4, 0x9a, 0xe6);
pub const ROSE: Color = Color::Rgb(0xe8, 0x93, 0xb5);
/// The console's base — text on a coloured pill.
pub const BASE: Color = Color::Rgb(0x08, 0x08, 0x0b);
/// Header band.
pub const SURFACE: Color = Color::Rgb(0x13, 0x13, 0x18);
/// A row that arrived within the last moment.
pub const FRESH: Color = Color::Rgb(0x14, 0x2a, 0x28);
/// A payload fragment that matches the filter or a tag.
pub const HIGHLIGHT: Color = Color::Rgb(0x3d, 0x33, 0x14);

/// Each event type keeps one colour for the whole session, so a stream of
/// mixed types reads as bands instead of a wall of text. Six hues, all
/// legible on the dark base; a stable hash picks one.
pub fn type_color(name: &str) -> Color {
    const PALETTE: [Color; 6] = [GOLD, BLUE, TEAL, AMBER, LAVENDER, ROSE];
    let mut hash: u32 = 0x811c_9dc5;
    for byte in name.bytes() {
        hash ^= byte as u32;
        hash = hash.wrapping_mul(0x0100_0193);
    }
    PALETTE[(hash % PALETTE.len() as u32) as usize]
}

/// Solid label: dark text on a coloured block.
pub fn pill(color: Color) -> Style {
    Style::new().fg(BASE).bg(color).add_modifier(Modifier::BOLD)
}

/// Text that matched something the user asked about.
pub fn highlight() -> Style {
    Style::new()
        .fg(GOLD)
        .bg(HIGHLIGHT)
        .add_modifier(Modifier::BOLD)
}

pub fn text() -> Style {
    Style::new().fg(TEXT)
}

pub fn dim() -> Style {
    Style::new().fg(TEXT2)
}

pub fn faint() -> Style {
    Style::new().fg(MUTED)
}

pub fn accent() -> Style {
    Style::new().fg(GOLD)
}

pub fn heading() -> Style {
    Style::new().fg(GOLD).add_modifier(Modifier::BOLD)
}

pub fn good() -> Style {
    Style::new().fg(TEAL)
}

pub fn warn() -> Style {
    Style::new().fg(AMBER)
}

pub fn bad() -> Style {
    Style::new().fg(RED)
}

/// Frames of the activity spinner.
pub const SPINNER: [&str; 10] = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"];
