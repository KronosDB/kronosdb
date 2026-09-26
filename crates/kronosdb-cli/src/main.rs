//! `kronos` — command-line client for KronosDB. Run bare for the terminal
//! UI; every subcommand is headless and scriptable (`-o json`).

mod client;
mod commands;
mod output;
mod profile;
mod query;
mod token;
mod tui;

use std::path::PathBuf;

use anyhow::Result;
use clap::{Args, Parser, Subcommand, ValueEnum};

use client::Connection;
use profile::ConfigFile;

#[derive(Parser)]
#[command(name = "kronos", version, about, arg_required_else_help = false)]
struct Cli {
    /// Connection profile (default: current-profile from the config file).
    #[arg(long, short, global = true, env = "KRONOS_PROFILE")]
    profile: Option<String>,

    /// Event store context (default: the profile's, else "default").
    #[arg(long, short, global = true, env = "KRONOS_CONTEXT")]
    context: Option<String>,

    /// Output format for headless commands.
    #[arg(long, short, global = true, value_enum, default_value = "table")]
    output: Output,

    /// Colour in the TUI. The default is "always": NO_COLOR is honoured by
    /// the headless commands (which print none), but in the TUI colour is
    /// the interface — a LIVE or READING pill without it says nothing.
    #[arg(
        long,
        global = true,
        value_enum,
        default_value = "always",
        env = "KRONOS_COLOR"
    )]
    color: ColorMode,

    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum ColorMode {
    Always,
    Never,
}

#[derive(Clone, Copy, ValueEnum)]
pub enum Output {
    Table,
    /// One JSON document.
    Json,
    /// One JSON object per line — the form to pipe into jq or a script.
    Ndjson,
}

impl Output {
    pub fn is_json(self) -> bool {
        !matches!(self, Output::Table)
    }
}

#[derive(Subcommand)]
enum Command {
    /// Open the terminal UI (what bare `kronos` does).
    Tui,
    /// Manage connection profiles (~/.config/kronosdb/config.toml).
    #[command(subcommand)]
    Profile(ProfileCommand),
    /// Who the server says you are, and what you may do.
    Whoami,
    /// Query events with a DCB query literal — the same shape kronos-ts uses:
    /// `kronos query '{"tags": {"orderId": "o-1"}, "types": ["OrderPlaced"]}'`.
    ///
    /// An array of items is an OR; within an item `types` is any-of and
    /// `tags` all-of. `{}` (the default) is every event. Pipe `-o ndjson`
    /// into jq or a script to fold state.
    Query {
        #[arg(default_value = "{}")]
        expr: String,
        /// Start at this sequence (default: the beginning).
        #[arg(long, default_value = "0")]
        from: i64,
        /// Stop after this many events.
        #[arg(long)]
        limit: Option<usize>,
        /// Include each event's tags (one extra lookup per event).
        #[arg(long)]
        tags: bool,
        /// Keep running and print new matches as they are committed.
        #[arg(long, short)]
        follow: bool,
    },
    /// Credential helpers.
    #[command(subcommand)]
    Auth(AuthCommand),
    /// Node, Raft and membership summary.
    Status,
    /// Event store contexts.
    #[command(subcommand)]
    Contexts(ContextsCommand),
    /// Connected clients.
    Clients,
    /// Command handlers (or query handlers with --queries).
    Handlers {
        #[arg(long)]
        queries: bool,
    },
    /// Open subscription queries.
    Subscriptions,
    /// Event processors and their segments.
    Processors,
    /// Read and append events.
    #[command(subcommand)]
    Events(EventsCommand),
}

#[derive(Subcommand)]
pub enum ProfileCommand {
    /// List profiles.
    Ls,
    /// Make a profile the default.
    Use { name: String },
    /// Create or update a profile.
    Set(Box<ProfileSetArgs>),
    /// Delete a profile.
    Rm { name: String },
}

#[derive(Args)]
pub struct ProfileSetArgs {
    pub name: String,
    /// gRPC endpoint, http:// or https://.
    #[arg(long)]
    pub endpoint: Option<String>,
    /// Admin HTTP base URL.
    #[arg(long)]
    pub admin: Option<String>,
    /// Default event store context.
    #[arg(long)]
    pub context: Option<String>,
    /// Command that prints a token, e.g. 'gcloud auth print-identity-token'.
    #[arg(long)]
    pub token_command: Option<String>,
    /// File holding a token (re-read when the server rejects it).
    #[arg(long)]
    pub token_file: Option<PathBuf>,
    /// Inline static token.
    #[arg(long)]
    pub token: Option<String>,
    #[arg(long)]
    pub ca_file: Option<PathBuf>,
    /// Client certificate for mTLS (with --key-file).
    #[arg(long)]
    pub cert_file: Option<PathBuf>,
    #[arg(long)]
    pub key_file: Option<PathBuf>,
    /// Verify the server certificate against this name.
    #[arg(long)]
    pub tls_domain: Option<String>,
}

#[derive(Subcommand)]
enum AuthCommand {
    /// Print the bearer token (for curl/grpcurl).
    Token,
}

#[derive(Subcommand)]
enum ContextsCommand {
    Ls,
    Create { name: String },
}

#[derive(Subcommand)]
pub enum EventsCommand {
    /// Show the most recent events; -f keeps following.
    Tail {
        #[arg(long = "lines", short = 'n', default_value = "20")]
        lines: usize,
        #[arg(long, short)]
        follow: bool,
        /// Start at this sequence instead of the last N.
        #[arg(long)]
        from: Option<i64>,
        /// Only events carrying this tag (key=value, repeatable: all must match).
        #[arg(long = "tag")]
        tags: Vec<String>,
        /// Only these event types (repeatable: any may match).
        #[arg(long = "type")]
        names: Vec<String>,
    },
    /// One event with its tags, as JSON.
    Get { sequence: i64 },
    /// Append a single event (unconditionally).
    Append {
        /// Event type.
        #[arg(long = "type")]
        name: String,
        #[arg(long = "tag")]
        tags: Vec<String>,
        #[arg(long)]
        payload: Option<String>,
        #[arg(long)]
        payload_file: Option<PathBuf>,
        /// Event identifier (default: a fresh UUID).
        #[arg(long)]
        id: Option<String>,
        #[arg(long, default_value = "1")]
        version: String,
    },
}

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("error: {error:#}");
        std::process::exit(1);
    }
}

async fn run() -> Result<()> {
    // The workspace compiles both rustls providers in (see kronosdb-server's
    // main); pick one before the first TLS handshake.
    let _ = rustls::crypto::ring::default_provider().install_default();

    let cli = Cli::parse();
    let command = cli.command.unwrap_or(Command::Tui);
    if let Command::Profile(command) = command {
        return commands::profile(command, cli.output);
    }

    let cfg = ConfigFile::load()?;
    let (name, profile) = cfg.resolve(cli.profile.as_deref())?;
    let context = cli.context.unwrap_or_else(|| profile.context().to_string());
    let conn = Connection::open(name, profile)?;

    match command {
        Command::Tui => {
            // crossterm strips every colour when NO_COLOR is set; decide
            // explicitly instead of inheriting whatever the shell exports.
            ratatui::crossterm::style::force_color_output(cli.color == ColorMode::Always);
            tui::run(conn, cfg, context).await
        }
        Command::Profile(_) => unreachable!("handled above"),
        Command::Whoami => commands::whoami(&conn, cli.output).await,
        Command::Query {
            expr,
            from,
            limit,
            tags,
            follow,
        } => {
            commands::query(
                &conn, &context, &expr, from, limit, tags, follow, cli.output,
            )
            .await
        }
        Command::Auth(AuthCommand::Token) => commands::auth_token(&conn).await,
        Command::Status => commands::status(&conn, cli.output).await,
        Command::Contexts(ContextsCommand::Ls) => commands::contexts(&conn, cli.output).await,
        Command::Contexts(ContextsCommand::Create { name }) => {
            commands::create_context(&conn, &name).await
        }
        Command::Clients => commands::clients(&conn, cli.output).await,
        Command::Handlers { queries } => commands::handlers(&conn, queries, cli.output).await,
        Command::Subscriptions => commands::subscriptions(&conn, cli.output).await,
        Command::Processors => commands::processors(&conn, cli.output).await,
        Command::Events(command) => commands::events(&conn, &context, command, cli.output).await,
    }
}
