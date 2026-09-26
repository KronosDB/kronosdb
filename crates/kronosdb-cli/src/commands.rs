//! Headless commands. Every one prints a table by default and the raw
//! data with `-o json`, so scripts never have to parse tables.

use std::time::Duration;

use anyhow::{Context as _, Result, bail};
use serde_json::{Value, json};

use crate::client::{Connection, pb};
use crate::output::{self, bool_of, items, num_of, str_of, table};
use crate::profile::{ConfigFile, Profile, config_path};
use crate::query;
use crate::token::jwt_claims;
use crate::{EventsCommand, Output, ProfileCommand, ProfileSetArgs};

fn print_json(value: &Value) {
    println!(
        "{}",
        serde_json::to_string_pretty(value).unwrap_or_default()
    );
}

fn yes_no(value: bool) -> String {
    if value { "yes" } else { "no" }.to_string()
}

// ───────────────────────────── profiles ─────────────────────────────

pub fn profile(command: ProfileCommand, output: Output) -> Result<()> {
    let mut cfg = ConfigFile::load()?;
    match command {
        ProfileCommand::Ls => {
            if output.is_json() {
                print_json(&serde_json::to_value(&cfg)?);
                return Ok(());
            }
            if cfg.profiles.is_empty() {
                println!(
                    "No profiles in {} — using built-in \"local\" (127.0.0.1:50051).\n\
                     Add one:  kronos profile set prod --endpoint http://localhost:50051 \\\n\
                     \x20           --admin http://localhost:9240 \\\n\
                     \x20           --token-command 'gcloud auth print-identity-token'",
                    config_path().display()
                );
                return Ok(());
            }
            let current = cfg.resolve(None).ok().map(|(name, _)| name);
            let rows: Vec<Vec<String>> = cfg
                .profiles
                .iter()
                .map(|(name, p)| {
                    vec![
                        if current.as_deref() == Some(name) {
                            "*"
                        } else {
                            ""
                        }
                        .to_string(),
                        name.clone(),
                        p.endpoint.clone(),
                        p.admin.clone().unwrap_or_default(),
                        crate::token::TokenSource::for_profile(name, p).describe(),
                    ]
                })
                .collect();
            println!(
                "{}",
                table(&["", "NAME", "ENDPOINT", "ADMIN", "CREDENTIAL"], &rows)
            );
        }
        ProfileCommand::Use { name } => {
            if !cfg.profiles.contains_key(&name) && name != "local" {
                bail!("no profile named {name:?}");
            }
            cfg.current_profile = Some(name.clone());
            cfg.save()?;
            println!("Switched to profile {name:?}.");
        }
        ProfileCommand::Set(args) => {
            let name = args.name.clone();
            let existing = cfg.profiles.remove(&name);
            let profile = apply_set(existing, *args)?;
            profile.validate(&name)?;
            cfg.profiles.insert(name.clone(), profile);
            if cfg.current_profile.is_none() {
                cfg.current_profile = Some(name.clone());
            }
            cfg.save()?;
            println!("Saved profile {name:?} to {}.", config_path().display());
        }
        ProfileCommand::Rm { name } => {
            if cfg.profiles.remove(&name).is_none() {
                bail!("no profile named {name:?}");
            }
            if cfg.current_profile.as_deref() == Some(&name) {
                cfg.current_profile = None;
            }
            cfg.save()?;
            println!("Removed profile {name:?}.");
        }
    }
    Ok(())
}

fn apply_set(existing: Option<Profile>, args: ProfileSetArgs) -> Result<Profile> {
    let mut profile = match (existing, &args.endpoint) {
        (Some(profile), _) => profile,
        (None, Some(_)) => Profile::default(),
        (None, None) => bail!("a new profile needs --endpoint"),
    };
    if let Some(endpoint) = args.endpoint {
        profile.endpoint = endpoint;
    }
    if let Some(admin) = args.admin {
        profile.admin = Some(admin);
    }
    if let Some(context) = args.context {
        profile.context = Some(context);
    }
    // Setting one credential replaces whichever was there.
    if args.token.is_some() || args.token_file.is_some() || args.token_command.is_some() {
        profile.token = args.token;
        profile.token_file = args.token_file;
        profile.token_command = args.token_command;
    }
    if let Some(path) = args.ca_file {
        profile.ca_file = Some(path);
    }
    if let Some(path) = args.cert_file {
        profile.cert_file = Some(path);
    }
    if let Some(path) = args.key_file {
        profile.key_file = Some(path);
    }
    if let Some(domain) = args.tls_domain {
        profile.tls_domain = Some(domain);
    }
    Ok(profile)
}

// ─────────────────────────────── auth ───────────────────────────────

/// Asks the server who this credential is and what it may do — the answer
/// that matters, since the server is what resolves grants. The token's own
/// claims are only used for the expiry line.
pub async fn whoami(conn: &Connection, output: Output) -> Result<()> {
    let me = conn.whoami().await?;
    let expires_in = match conn.tokens.token().await? {
        Some(token) => jwt_claims(&token)
            .and_then(|claims| claims.get("exp")?.as_u64())
            .map(|exp| {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_secs())
                    .unwrap_or(0);
                exp.saturating_sub(now)
            }),
        None => None,
    };
    let scope = |all: bool, names: &[String]| {
        if all {
            "*".to_string()
        } else {
            names.join(",")
        }
    };

    if output.is_json() {
        print_json(&json!({
            "profile": conn.profile_name,
            "endpoint": conn.profile.endpoint,
            "credential": conn.tokens.describe(),
            "subject": me.subject,
            "source": me.source,
            "authentication_enabled": me.authentication_enabled,
            "expires_in_secs": expires_in,
            "grants": me.grants.iter().map(|g| json!({
                "roles": g.roles,
                "contexts": if g.all_contexts { json!("*") } else { json!(g.contexts) },
                "buses": if g.all_buses { json!("*") } else { json!(g.buses) },
            })).collect::<Vec<_>>(),
        }));
        return Ok(());
    }

    println!("{}  (via {})", me.subject, me.source);
    println!(
        "profile     {} → {}",
        conn.profile_name, conn.profile.endpoint
    );
    println!("credential  {}", conn.tokens.describe());
    if let Some(secs) = expires_in {
        println!("expires     in {}", output::duration(secs));
    }
    if !me.authentication_enabled {
        println!(
            "\nThe server has no authentication configured: everyone is anonymous with full access."
        );
    } else if me.grants.is_empty() {
        println!(
            "\nAuthenticated, but no grant matches this identity — every request will be denied."
        );
    } else {
        let rows: Vec<Vec<String>> = me
            .grants
            .iter()
            .map(|g| {
                vec![
                    g.roles.join(","),
                    scope(g.all_contexts, &g.contexts),
                    scope(g.all_buses, &g.buses),
                ]
            })
            .collect();
        println!("\n{}", table(&["ROLES", "CONTEXTS", "BUSES"], &rows));
    }
    Ok(())
}

pub async fn auth_token(conn: &Connection) -> Result<()> {
    match conn.tokens.token().await? {
        Some(token) => println!("{token}"),
        None => bail!(
            "profile {:?} has no credential configured",
            conn.profile_name
        ),
    }
    Ok(())
}

// ───────────────────────────── topology ─────────────────────────────

pub async fn status(conn: &Connection, output: Output) -> Result<()> {
    let snapshot = conn.admin_get("/api/v1/snapshot").await?;
    if output.is_json() {
        print_json(&json!({ "node": snapshot["node"], "cluster": snapshot["cluster"] }));
        return Ok(());
    }
    let (node, cluster) = (&snapshot["node"], &snapshot["cluster"]);
    let auth: Vec<&str> = items(&node["auth"])
        .iter()
        .filter_map(Value::as_str)
        .collect();
    println!(
        "{} v{}  up {}  {}",
        str_of(node, "name"),
        str_of(node, "version"),
        output::duration(num_of(node, "uptime_secs")),
        if bool_of(node, "ready") {
            "READY"
        } else {
            "NOT READY"
        },
    );
    println!(
        "auth: {}   tls: {}",
        if auth.is_empty() {
            "none (open)".into()
        } else {
            auth.join(", ")
        },
        yes_no(bool_of(node, "tls")),
    );
    let raft = &cluster["raft"];
    println!(
        "raft: {} term {}  leader {}  write gate {}",
        str_of(raft, "state"),
        num_of(raft, "term"),
        raft.get("leader_id")
            .and_then(Value::as_u64)
            .map(|id| id.to_string())
            .unwrap_or_else(|| "unknown".into()),
        if bool_of(&cluster["claim"], "writable") {
            "open"
        } else {
            "closed"
        },
    );
    let rows: Vec<Vec<String>> = items(&raft["nodes"])
        .iter()
        .map(|n| {
            vec![
                num_of(n, "id").to_string(),
                str_of(n, "addr").to_string(),
                if bool_of(n, "voter") {
                    "voter"
                } else {
                    "learner"
                }
                .to_string(),
                if bool_of(n, "leader") { "leader" } else { "" }.to_string(),
            ]
        })
        .collect();
    println!("\n{}", table(&["ID", "ADDR", "ROLE", ""], &rows));
    Ok(())
}

pub async fn contexts(conn: &Connection, output: Output) -> Result<()> {
    let data = conn.admin_get("/api/v1/contexts").await?;
    if output.is_json() {
        print_json(&data);
        return Ok(());
    }
    let rows: Vec<Vec<String>> = items(&data)
        .iter()
        .map(|c| {
            vec![
                str_of(c, "name").to_string(),
                num_of(c, "head").to_string(),
                num_of(c, "tail").to_string(),
                num_of(c, "local_tail")
                    .saturating_sub(num_of(c, "durable_tail"))
                    .to_string(),
                output::bytes(num_of(c, "data_bytes")),
                num_of(c, "dcb_violations").to_string(),
                if bool_of(c, "poisoned") {
                    "POISONED"
                } else {
                    "ok"
                }
                .to_string(),
            ]
        })
        .collect();
    println!(
        "{}",
        table(
            &[
                "CONTEXT",
                "HEAD",
                "TAIL",
                "UNSYNCED",
                "SIZE",
                "DCB-REJECTS",
                "STATE"
            ],
            &rows
        )
    );
    Ok(())
}

pub async fn create_context(conn: &Connection, name: &str) -> Result<()> {
    conn.admin_post("/api/contexts", &json!({ "name": name }))
        .await?;
    println!("Created context {name:?}.");
    Ok(())
}

pub async fn clients(conn: &Connection, output: Output) -> Result<()> {
    let data = conn.admin_get("/api/v1/clients").await?;
    if output.is_json() {
        print_json(&data);
        return Ok(());
    }
    let rows: Vec<Vec<String>> = items(&data)
        .iter()
        .map(|c| {
            vec![
                str_of(c, "component").to_string(),
                str_of(c, "client_id").to_string(),
                str_of(c, "version").to_string(),
                output::duration(num_of(c, "connected_secs")),
                format!("{}ms", num_of(c, "last_heartbeat_ms")),
                yes_no(bool_of(c, "streaming")),
            ]
        })
        .collect();
    println!(
        "{}",
        table(
            &[
                "COMPONENT",
                "CLIENT",
                "VERSION",
                "CONNECTED",
                "HEARTBEAT",
                "STREAM"
            ],
            &rows
        )
    );
    Ok(())
}

pub async fn handlers(conn: &Connection, queries: bool, output: Output) -> Result<()> {
    let path = if queries {
        "/api/v1/queries"
    } else {
        "/api/v1/commands"
    };
    let data = conn.admin_get(path).await?;
    if output.is_json() {
        print_json(&data);
        return Ok(());
    }
    let rows: Vec<Vec<String>> = items(&data)
        .iter()
        .map(|d| {
            let handlers = items(&d["handlers"]);
            let permits: i64 = handlers
                .iter()
                .filter_map(|h| h.get("available_permits")?.as_i64())
                .sum();
            vec![
                str_of(d, "bus").to_string(),
                str_of(d, "name").to_string(),
                handlers.len().to_string(),
                permits.to_string(),
                num_of(d, "dispatched").to_string(),
                num_of(d, "failed").to_string(),
                format!("{:.1}ms", num_of(d, "avg_duration_us") as f64 / 1000.0),
            ]
        })
        .collect();
    println!(
        "{}",
        table(
            &[
                "BUS",
                if queries { "QUERY" } else { "COMMAND" },
                "HANDLERS",
                "PERMITS",
                "DISPATCHED",
                "FAILED",
                "AVG"
            ],
            &rows
        )
    );
    Ok(())
}

pub async fn subscriptions(conn: &Connection, output: Output) -> Result<()> {
    let data = conn.admin_get("/api/v1/subscriptions").await?;
    if output.is_json() {
        print_json(&data);
        return Ok(());
    }
    let rows: Vec<Vec<String>> = items(&data)
        .iter()
        .map(|s| {
            vec![
                str_of(s, "bus").to_string(),
                str_of(s, "query").to_string(),
                str_of(s, "subscriber_component").to_string(),
                str_of(s, "handler_client_id").to_string(),
                output::duration(num_of(s, "open_secs")),
            ]
        })
        .collect();
    println!(
        "{}",
        table(&["BUS", "QUERY", "SUBSCRIBER", "HANDLER", "OPEN"], &rows)
    );
    Ok(())
}

pub async fn processors(conn: &Connection, output: Output) -> Result<()> {
    let data = conn.admin_get("/api/v1/processors").await?;
    if output.is_json() {
        print_json(&data);
        return Ok(());
    }
    let rows: Vec<Vec<String>> = items(&data).iter().map(processor_row).collect();
    println!(
        "{}",
        table(
            &[
                "PROCESSOR",
                "MODE",
                "INSTANCES",
                "SEGMENTS",
                "POSITION",
                "STATE"
            ],
            &rows
        )
    );
    Ok(())
}

pub fn processor_row(p: &Value) -> Vec<String> {
    let instances = items(&p["instances"]);
    let segments: Vec<&Value> = instances
        .iter()
        .flat_map(|i| items(&i["segments"]))
        .collect();
    let min_position = segments
        .iter()
        .filter_map(|s| s.get("token_position")?.as_i64())
        .min();
    let state = if instances.iter().any(|i| bool_of(i, "error")) {
        "ERROR"
    } else if segments.iter().any(|s| bool_of(s, "replaying")) {
        "replaying"
    } else if !segments.is_empty() && segments.iter().all(|s| bool_of(s, "caught_up")) {
        "caught up"
    } else if instances.iter().any(|i| bool_of(i, "running")) {
        "running"
    } else {
        "paused"
    };
    vec![
        str_of(p, "name").to_string(),
        str_of(p, "mode").to_string(),
        instances.len().to_string(),
        segments.len().to_string(),
        min_position.map(|p| p.to_string()).unwrap_or_default(),
        state.to_string(),
    ]
}

// ────────────────────────────── events ──────────────────────────────

pub fn event_line(event: &pb::SequencedEvent, payload_width: usize) -> String {
    let inner = event.event.clone().unwrap_or_default();
    format!(
        "{:>10}  {:<28}  {}",
        event.sequence,
        inner.name,
        output::payload_preview(&inner.payload, payload_width)
    )
}

/// Runs an inline query (see `query.rs` for the language). Events stream out
/// as they are read, so `-o ndjson` composes with jq or a script for folding
/// state; `-o json` collects them into one document instead.
#[allow(clippy::too_many_arguments)]
pub async fn query(
    conn: &Connection,
    context: &str,
    expr: &str,
    from: i64,
    limit: Option<usize>,
    with_tags: bool,
    follow: bool,
    output: Output,
) -> Result<()> {
    if follow && matches!(output, Output::Json) {
        bail!("--follow never ends, so it cannot produce one JSON document: use -o ndjson");
    }
    let criteria = query::parse(expr)?;
    let mut remaining = limit.unwrap_or(usize::MAX);
    let mut next = from;
    let mut collected = Vec::new();
    loop {
        let events = conn.source(context, next, &criteria, remaining).await?;
        for event in &events {
            next = event.sequence + 1;
            if matches!(output, Output::Table) {
                println!("{}", event_line(event, 100));
                continue;
            }
            let mut value = output::event_json(event);
            if with_tags {
                let tags = conn.tags(context, event.sequence).await?;
                value["tags"] = tags.iter().map(output::tag_text).collect();
            }
            match output {
                Output::Ndjson => println!("{value}"),
                _ => collected.push(value),
            }
        }
        remaining -= events.len();
        if !follow || remaining == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(if events.is_empty() {
            1000
        } else {
            200
        }))
        .await;
    }
    if matches!(output, Output::Json) {
        print_json(&Value::Array(collected));
    }
    Ok(())
}

pub async fn events(
    conn: &Connection,
    context: &str,
    command: EventsCommand,
    output: Output,
) -> Result<()> {
    match command {
        EventsCommand::Tail {
            lines,
            follow,
            from,
            tags,
            names,
        } => {
            let criteria = query::from_flags(&names, &tags)?;
            let mut next = match from {
                Some(from) => from,
                None => {
                    let (head, tail) = (conn.head(context).await?, conn.tail(context).await?);
                    // With a filter, "last N matching" needs a scan from the
                    // start; unfiltered, the window is just arithmetic.
                    if criteria.is_empty() {
                        (head - lines as i64).max(tail)
                    } else {
                        tail
                    }
                }
            };
            let mut first = true;
            loop {
                let mut events = conn.source(context, next, &criteria, usize::MAX).await?;
                if first && from.is_none() && events.len() > lines {
                    events.drain(..events.len() - lines);
                }
                first = false;
                for event in &events {
                    if output.is_json() {
                        println!("{}", output::event_json(event));
                    } else {
                        println!("{}", event_line(event, 100));
                    }
                    next = event.sequence + 1;
                }
                if !follow {
                    return Ok(());
                }
                // Polling Source, not the Stream RPC: no flow-control state
                // to manage, and a second of latency is fine for eyeballs.
                tokio::time::sleep(Duration::from_millis(if events.is_empty() {
                    1000
                } else {
                    200
                }))
                .await;
            }
        }
        EventsCommand::Get { sequence } => {
            let events = conn.source(context, sequence, &[], 1).await?;
            let Some(event) = events.first().filter(|e| e.sequence == sequence) else {
                bail!("no event at sequence {sequence} in context {context:?}");
            };
            let tags = conn.tags(context, sequence).await?;
            let mut value = output::event_json(event);
            value["tags"] = tags.iter().map(output::tag_text).collect();
            print_json(&value);
            Ok(())
        }
        EventsCommand::Append {
            name,
            tags,
            payload,
            payload_file,
            id,
            version,
        } => {
            let payload = match (payload, payload_file) {
                (Some(text), None) => text.into_bytes(),
                (None, Some(path)) => {
                    std::fs::read(&path).with_context(|| format!("reading {}", path.display()))?
                }
                (None, None) => Vec::new(),
                (Some(_), Some(_)) => bail!("--payload and --payload-file are exclusive"),
            };
            let response = conn
                .append(
                    context,
                    pb::AppendRequest {
                        condition: None,
                        events: vec![pb::TaggedEvent {
                            event: Some(pb::Event {
                                identifier: id.unwrap_or_else(|| uuid::Uuid::new_v4().to_string()),
                                timestamp: std::time::SystemTime::now()
                                    .duration_since(std::time::UNIX_EPOCH)?
                                    .as_millis() as i64,
                                name,
                                version,
                                payload,
                                metadata: Default::default(),
                            }),
                            tags: tags
                                .iter()
                                .map(|t| query::parse_tag(t))
                                .collect::<Result<_>>()?,
                        }],
                    },
                )
                .await?;
            if output.is_json() {
                print_json(
                    &json!({ "sequence": response.first_sequence, "count": response.count }),
                );
            } else {
                println!("Appended at sequence {}.", response.first_sequence);
            }
            Ok(())
        }
    }
}
