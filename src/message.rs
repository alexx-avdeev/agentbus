//! Wire format shared by every agent on the bus.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Control-plane message kinds. Anything else is a work message.
pub const KIND_HELLO: &str = "hello";
pub const KIND_ROSTER: &str = "roster";
pub const KIND_BYE: &str = "bye";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    pub id: String,
    pub ts: DateTime<Utc>,
    pub project: String,
    pub from: String,
    /// `None` means broadcast to every agent on the project.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub to: Option<String>,
    pub thread: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reply_to: Option<String>,
    pub kind: String,
    pub body: String,
    /// Structured payload, used by control messages (e.g. the roster list).
    #[serde(default, skip_serializing_if = "serde_json::Value::is_null")]
    pub data: serde_json::Value,
}

impl Message {
    pub fn new(project: &str, from: &str, kind: &str, body: String) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            ts: Utc::now(),
            project: project.to_string(),
            from: from.to_string(),
            to: None,
            thread: uuid::Uuid::new_v4().to_string(),
            reply_to: None,
            kind: kind.to_string(),
            body,
            data: serde_json::Value::Null,
        }
    }

    pub fn is_control(&self) -> bool {
        matches!(self.kind.as_str(), KIND_HELLO | KIND_ROSTER | KIND_BYE)
    }

    /// Routing key on the topic exchange.
    pub fn routing_key(&self) -> String {
        if self.is_control() {
            format!("{}.control", self.project)
        } else {
            match &self.to {
                Some(agent) => format!("{}.direct.{}", self.project, agent),
                None => format!("{}.broadcast", self.project),
            }
        }
    }

    /// Roster members carried by a `roster` message.
    pub fn roster(&self) -> Option<Vec<String>> {
        if self.kind != KIND_ROSTER {
            return None;
        }
        self.data
            .get("agents")
            .and_then(|v| v.as_array())
            .map(|a| a.iter().filter_map(|x| x.as_str().map(str::to_string)).collect())
    }

    /// One-line human rendering, used by `log` and `recv --pretty`.
    pub fn pretty(&self) -> String {
        let to = self.to.as_deref().unwrap_or("*");
        let thread = &self.thread[..self.thread.len().min(8)];
        let mut out = format!(
            "{} [{}] {} -> {} ({}) #{}",
            self.ts.with_timezone(&chrono::Local).format("%Y-%m-%d %H:%M:%S"),
            self.kind,
            self.from,
            to,
            self.project,
            thread
        );
        if let Some(r) = &self.reply_to {
            out.push_str(&format!(" re:{}", &r[..r.len().min(8)]));
        }
        if let Some(agents) = self.roster() {
            out.push_str(&format!("\n    roster: {}", agents.join(", ")));
        }
        if !self.body.is_empty() {
            for line in self.body.lines() {
                out.push_str("\n    ");
                out.push_str(line);
            }
        }
        out
    }
}
