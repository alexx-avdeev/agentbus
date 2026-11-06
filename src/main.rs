mod bus;
mod message;
mod serve;

use anyhow::{bail, Context, Result};
use chrono::{DateTime, Utc};
use clap::{Args, Parser, Subcommand};
use std::collections::BTreeSet;
use std::io::Read;
use std::time::Duration;

use bus::{Bus, Event, Offset};
use message::{Message, KIND_BYE, KIND_HELLO, KIND_ROSTER};

/// Message bus for agents working together across machines, on top of RabbitMQ.
#[derive(Parser)]
#[command(name = "agentbus", version, about)]
struct Cli {
    /// AMQP connection URL
    #[arg(long, env = "AMQP_URL", default_value = "amqp://guest:guest@localhost:5672/%2f", global = true)]
    url: String,
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Args, Clone)]
struct Who {
    /// Project (topic) the agent works on
    #[arg(long, short = 'p', env = "AGENTBUS_PROJECT")]
    project: String,
    /// This agent's name
    #[arg(long, short = 'a', env = "AGENTBUS_AGENT")]
    agent: String,
}

#[derive(Subcommand)]
enum Cmd {
    /// Create (or find) the project topology, create this agent's queue, say hello.
    /// Pass --roster to act as coordinator and announce who is expected.
    Join {
        #[command(flatten)]
        who: Who,
        /// Short description of this agent's role, sent with the hello
        #[arg(long, default_value = "")]
        role: String,
        /// Comma-separated list of every agent expected on the project (coordinator only)
        #[arg(long, value_delimiter = ',')]
        roster: Option<Vec<String>>,
        /// Log retention for a new project stream, e.g. 7D, 12h
        #[arg(long, default_value = "7D")]
        retention: String,
    },
    /// Block until every agent on the latest roster has said hello.
    Wait {
        #[arg(long, short = 'p', env = "AGENTBUS_PROJECT")]
        project: String,
        /// Give up after this long
        #[arg(long, default_value = "10m", value_parser = humantime::parse_duration)]
        timeout: Duration,
    },
    /// Send a message to one agent (--to) or to everyone (--broadcast).
    Send {
        #[command(flatten)]
        who: Who,
        #[arg(long, conflicts_with = "broadcast")]
        to: Option<String>,
        #[arg(long)]
        broadcast: bool,
        /// Thread id; a new one is generated when omitted
        #[arg(long, short = 't')]
        thread: Option<String>,
        /// Id of the message this replies to
        #[arg(long)]
        reply_to: Option<String>,
        /// task, question, answer, result, done, note, ... (free form)
        #[arg(long, short = 'k', default_value = "note")]
        kind: String,
        /// Message body; reads stdin when omitted or "-"
        body: Option<String>,
    },
    /// Pull new messages addressed to this agent. Prints one JSON object per line.
    Recv {
        #[command(flatten)]
        who: Who,
        /// How long to wait for the first message
        #[arg(long, default_value = "30s", value_parser = humantime::parse_duration)]
        wait: Duration,
        /// Stop after this many messages
        #[arg(long, default_value_t = 10)]
        max: usize,
        /// Human-readable output instead of JSON lines
        #[arg(long)]
        pretty: bool,
    },
    /// Read the project log (every message, including control messages).
    Log {
        #[arg(long, short = 'p', env = "AGENTBUS_PROJECT")]
        project: String,
        /// Only messages newer than this, e.g. 2h, 30m
        #[arg(long, value_parser = humantime::parse_duration)]
        since: Option<Duration>,
        /// Only this thread (prefix match)
        #[arg(long, short = 't')]
        thread: Option<String>,
        /// Only this sender
        #[arg(long)]
        from: Option<String>,
        /// Only this kind
        #[arg(long, short = 'k')]
        kind: Option<String>,
        /// Keep reading as new messages arrive
        #[arg(long, short = 'f')]
        follow: bool,
        /// Skip history: only messages published from now on (implies --follow)
        #[arg(long, conflicts_with = "since")]
        new: bool,
        /// JSON lines instead of text
        #[arg(long)]
        json: bool,
    },
    /// Announce that this agent is leaving the project.
    Leave {
        #[command(flatten)]
        who: Who,
        #[arg(default_value = "")]
        body: String,
    },
    /// Serve a browser view of one or more project logs.
    Serve {
        /// Project(s) to show; repeatable
        #[arg(long, short = 'p', env = "AGENTBUS_PROJECT", required = true)]
        project: Vec<String>,
        #[arg(long, default_value = "127.0.0.1:8080")]
        bind: String,
        /// Load history newer than this (default: everything retained)
        #[arg(long, value_parser = humantime::parse_duration)]
        since: Option<Duration>,
    },
}

fn validate_name(what: &str, s: &str) -> Result<()> {
    if s.is_empty() || !s.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_') {
        bail!("{what} must be [A-Za-z0-9_-]+, got {s:?}");
    }
    Ok(())
}

fn since_offset(since: Option<Duration>) -> Offset {
    match since {
        Some(d) => Offset::Since(Utc::now() - chrono::Duration::from_std(d).unwrap_or_default()),
        None => Offset::First,
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_writer(std::io::stderr)
        .init();
    let cli = Cli::parse();

    match cli.cmd {
        Cmd::Join { who, role, roster, retention } => {
            validate_name("project", &who.project)?;
            validate_name("agent", &who.agent)?;
            let bus = Bus::connect(&cli.url).await?;
            bus.confirms().await?;
            bus.ensure_project(&who.project, &retention).await?;
            bus.ensure_agent(&who.project, &who.agent).await?;
            if let Some(list) = roster {
                let mut agents: BTreeSet<String> = list.into_iter().filter(|s| !s.is_empty()).collect();
                agents.insert(who.agent.clone());
                for a in &agents {
                    validate_name("roster entry", a)?;
                }
                let mut m = Message::new(&who.project, &who.agent, KIND_ROSTER, String::new());
                m.data = serde_json::json!({ "agents": agents });
                bus.publish(&m).await?;
                eprintln!("roster published: {}", agents.iter().cloned().collect::<Vec<_>>().join(", "));
            }
            let hello = Message::new(&who.project, &who.agent, KIND_HELLO, role);
            bus.publish(&hello).await?;
            println!("{}", serde_json::json!({
                "project": who.project, "agent": who.agent,
                "queue": bus::queue_name(&who.project, &who.agent),
                "stream": bus::stream_name(&who.project),
            }));
        }

        Cmd::Wait { project, timeout: limit } => {
            let bus = Bus::connect(&cli.url).await?;
            let mut roster: Option<BTreeSet<String>> = None;
            let mut hellos: BTreeSet<String> = BTreeSet::new();
            // Only the newest roster counts, so the whole retained log must be read
            // before deciding: the decision is deferred until the first idle tick.
            let mut caught_up = false;
            let run = bus.replay(&project, Offset::First, Some(Duration::from_millis(500)), |ev| {
                match ev {
                    Event::Idle => caught_up = true,
                    Event::Msg(m) => match m.kind.as_str() {
                        KIND_ROSTER => {
                            roster = m.roster().map(|v| v.into_iter().collect());
                            hellos.clear();
                        }
                        KIND_HELLO => {
                            hellos.insert(m.from.clone());
                        }
                        _ => {}
                    },
                }
                match &roster {
                    Some(r) if caught_up => !r.iter().all(|a| hellos.contains(a)),
                    _ => true,
                }
            });
            match tokio::time::timeout(limit, run).await {
                Ok(res) => {
                    res?;
                    let r = roster.unwrap_or_default();
                    println!("{}", serde_json::json!({ "ready": true, "agents": r }));
                }
                Err(_) => {
                    let missing: Vec<_> = roster
                        .as_ref()
                        .map(|r| r.iter().filter(|a| !hellos.contains(*a)).cloned().collect())
                        .unwrap_or_default();
                    println!("{}", serde_json::json!({ "ready": false, "roster_seen": roster.is_some(), "missing": missing }));
                    bail!(
                        "timed out after {} waiting for: {}",
                        humantime::format_duration(limit),
                        if roster.is_none() { "a roster message".to_string() } else { missing.join(", ") }
                    );
                }
            }
        }

        Cmd::Send { who, to, broadcast, thread, reply_to, kind, body } => {
            validate_name("agent", &who.agent)?;
            if to.is_none() && !broadcast {
                bail!("pass --to <agent> or --broadcast");
            }
            if matches!(kind.as_str(), KIND_HELLO | KIND_ROSTER | KIND_BYE) {
                bail!("{kind} is a control message; use join / leave");
            }
            let body = match body.as_deref() {
                Some("-") | None => {
                    let mut s = String::new();
                    std::io::stdin().read_to_string(&mut s).context("reading body from stdin")?;
                    s
                }
                Some(b) => b.to_string(),
            };
            let bus = Bus::connect(&cli.url).await?;
            bus.confirms().await?;
            let mut m = Message::new(&who.project, &who.agent, &kind, body.trim_end().to_string());
            m.to = to;
            if let Some(t) = thread {
                m.thread = t;
            }
            m.reply_to = reply_to;
            bus.publish(&m).await?;
            println!("{}", serde_json::json!({ "id": m.id, "thread": m.thread, "to": m.to, "kind": m.kind }));
        }

        Cmd::Recv { who, wait, max, pretty } => {
            let bus = Bus::connect(&cli.url).await?;
            bus.ensure_project(&who.project, "7D").await?;
            bus.ensure_agent(&who.project, &who.agent).await?;
            let msgs = bus.recv(&who.project, &who.agent, wait, max.max(1)).await?;
            for m in &msgs {
                if pretty {
                    println!("{}", m.pretty());
                } else {
                    println!("{}", serde_json::to_string(m)?);
                }
            }
            if msgs.is_empty() {
                eprintln!("no messages within {}", humantime::format_duration(wait));
            }
        }

        Cmd::Log { project, since, thread, from, kind, follow, new, json } => {
            let bus = Bus::connect(&cli.url).await?;
            let idle = if follow || new { None } else { Some(Duration::from_millis(800)) };
            let offset = if new { Offset::Next } else { since_offset(since) };
            bus.replay(&project, offset, idle, |ev| {
                let Event::Msg(m) = ev else { return false };
                let keep = thread.as_ref().is_none_or(|t| m.thread.starts_with(t.as_str()))
                    && from.as_ref().is_none_or(|f| &m.from == f)
                    && kind.as_ref().is_none_or(|k| &m.kind == k);
                if keep {
                    if json {
                        println!("{}", serde_json::to_string(&m).unwrap_or_default());
                    } else {
                        println!("{}", m.pretty());
                    }
                }
                true
            })
            .await?;
        }

        Cmd::Leave { who, body } => {
            let bus = Bus::connect(&cli.url).await?;
            bus.confirms().await?;
            let m = Message::new(&who.project, &who.agent, KIND_BYE, body);
            bus.publish(&m).await?;
            println!("{}", serde_json::json!({ "left": who.agent, "project": who.project }));
        }

        Cmd::Serve { project, bind, since } => {
            let start: Option<DateTime<Utc>> = since.map(|d| Utc::now() - chrono::Duration::from_std(d).unwrap_or_default());
            serve::run(&cli.url, project, &bind, start).await?;
        }
    }
    Ok(())
}
