//! RabbitMQ topology and the thin publish / consume / replay layer.
//!
//! Topology (one topic exchange shared by all projects):
//!   exchange  `agentbus`                         topic, durable
//!   stream    `agentbus.log.<project>`           bound to `<project>.#`
//!   queue     `agentbus.<project>.<agent>`       quorum, bound to
//!                                                `<project>.direct.<agent>` and `<project>.broadcast`
//! Routing keys:
//!   `<project>.direct.<agent>`   point to point
//!   `<project>.broadcast`        every agent queue + the stream
//!   `<project>.control`          hello / roster / bye, stream only

use anyhow::{Context, Result};
use futures_util::StreamExt;
use lapin::{
    options::*,
    types::{AMQPValue, FieldTable, LongString, ShortString},
    BasicProperties, Channel, Connection, ConnectionProperties, ExchangeKind,
};
use std::time::Duration;
use tokio::time::timeout;

use crate::message::Message;

pub const EXCHANGE: &str = "agentbus";

pub fn stream_name(project: &str) -> String {
    format!("agentbus.log.{project}")
}

pub fn queue_name(project: &str, agent: &str) -> String {
    format!("agentbus.{project}.{agent}")
}

pub struct Bus {
    _conn: Connection,
    pub channel: Channel,
}

/// What a stream replay hands to its callback.
pub enum Event {
    Msg(Message),
    /// No message arrived within the idle window: the reader has caught up.
    Idle,
}

/// Where to start reading a stream.
#[derive(Debug, Clone)]
pub enum Offset {
    First,
    Next,
    Since(chrono::DateTime<chrono::Utc>),
}

impl Offset {
    fn amqp(&self) -> AMQPValue {
        match self {
            Offset::First => AMQPValue::LongString(LongString::from("first")),
            Offset::Next => AMQPValue::LongString(LongString::from("next")),
            Offset::Since(t) => AMQPValue::Timestamp(t.timestamp().max(0) as u64),
        }
    }
}

fn table(pairs: &[(&str, AMQPValue)]) -> FieldTable {
    let mut t = FieldTable::default();
    for (k, v) in pairs {
        t.insert(ShortString::from(*k), v.clone());
    }
    t
}

fn ls(s: &str) -> AMQPValue {
    AMQPValue::LongString(LongString::from(s))
}

impl Bus {
    pub async fn connect(url: &str) -> Result<Self> {
        let conn = Connection::connect(url, ConnectionProperties::default())
            .await
            .with_context(|| format!("connecting to {url}"))?;
        let channel = conn.create_channel().await?;
        Ok(Self {
            _conn: conn,
            channel,
        })
    }

    /// Idempotent: declares the exchange and the project's log stream.
    pub async fn ensure_project(&self, project: &str, retention: &str) -> Result<()> {
        self.channel
            .exchange_declare(
                ShortString::from(EXCHANGE),
                ExchangeKind::Topic,
                ExchangeDeclareOptions {
                    durable: true,
                    ..Default::default()
                },
                FieldTable::default(),
            )
            .await
            .context("declaring exchange")?;

        let stream = stream_name(project);
        self.channel
            .queue_declare(
                ShortString::from(stream.as_str()),
                QueueDeclareOptions {
                    durable: true,
                    ..Default::default()
                },
                table(&[
                    ("x-queue-type", ls("stream")),
                    ("x-max-age", ls(retention)),
                    ("x-stream-max-segment-size-bytes", AMQPValue::LongLongInt(50_000_000)),
                ]),
            )
            .await
            .with_context(|| format!("declaring stream {stream}"))?;
        self.channel
            .queue_bind(
                ShortString::from(stream.as_str()),
                ShortString::from(EXCHANGE),
                ShortString::from(format!("{project}.#")),
                QueueBindOptions::default(),
                FieldTable::default(),
            )
            .await
            .context("binding stream")?;
        Ok(())
    }

    /// Idempotent: declares the agent's own queue and its two bindings.
    pub async fn ensure_agent(&self, project: &str, agent: &str) -> Result<()> {
        let queue = queue_name(project, agent);
        self.channel
            .queue_declare(
                ShortString::from(queue.as_str()),
                QueueDeclareOptions {
                    durable: true,
                    ..Default::default()
                },
                table(&[
                    ("x-queue-type", ls("quorum")),
                    // drop the queue after a week with no consumer
                    ("x-expires", AMQPValue::LongLongInt(7 * 24 * 3600 * 1000)),
                ]),
            )
            .await
            .with_context(|| format!("declaring queue {queue}"))?;
        for key in [
            format!("{project}.direct.{agent}"),
            format!("{project}.broadcast"),
        ] {
            self.channel
                .queue_bind(
                    ShortString::from(queue.as_str()),
                    ShortString::from(EXCHANGE),
                    ShortString::from(key),
                    QueueBindOptions::default(),
                    FieldTable::default(),
                )
                .await
                .context("binding queue")?;
        }
        Ok(())
    }

    pub async fn publish(&self, msg: &Message) -> Result<()> {
        let payload = serde_json::to_vec(msg)?;
        let headers = table(&[
            ("from", ls(&msg.from)),
            ("kind", ls(&msg.kind)),
            ("thread", ls(&msg.thread)),
        ]);
        let props = BasicProperties::default()
            .with_content_type(ShortString::from("application/json"))
            .with_message_id(ShortString::from(msg.id.as_str()))
            .with_timestamp(msg.ts.timestamp().max(0) as u64)
            .with_delivery_mode(2)
            .with_headers(headers);
        self.channel
            .basic_publish(
                ShortString::from(EXCHANGE),
                ShortString::from(msg.routing_key()),
                BasicPublishOptions::default(),
                &payload,
                props,
            )
            .await?
            .await
            .context("publish confirm")?;
        Ok(())
    }

    /// Enable publisher confirms so `publish` only returns once the broker has the message.
    pub async fn confirms(&self) -> Result<()> {
        self.channel
            .confirm_select(ConfirmSelectOptions::default())
            .await?;
        Ok(())
    }

    /// Pull work messages from the agent's queue. Waits up to `wait` for the
    /// first one, then drains whatever is immediately available up to `max`.
    /// Messages the agent sent itself (its own broadcasts) are acked and dropped.
    pub async fn recv(
        &self,
        project: &str,
        agent: &str,
        wait: Duration,
        max: usize,
    ) -> Result<Vec<Message>> {
        let queue = queue_name(project, agent);
        self.channel
            .basic_qos(max.clamp(1, u16::MAX as usize) as u16, BasicQosOptions::default())
            .await?;
        let mut consumer = self
            .channel
            .basic_consume(
                ShortString::from(queue.as_str()),
                ShortString::from(""),
                BasicConsumeOptions::default(),
                FieldTable::default(),
            )
            .await
            .with_context(|| format!("consuming {queue}"))?;

        let mut out = Vec::new();
        let drain = Duration::from_millis(300);
        loop {
            let budget = if out.is_empty() { wait } else { drain };
            let next = match timeout(budget, consumer.next()).await {
                Ok(Some(Ok(d))) => d,
                Ok(Some(Err(e))) => return Err(e.into()),
                Ok(None) | Err(_) => break,
            };
            match serde_json::from_slice::<Message>(&next.data) {
                Ok(m) => {
                    next.acker.ack(BasicAckOptions::default()).await?;
                    if m.from != agent {
                        out.push(m);
                    }
                }
                Err(e) => {
                    tracing::warn!(error = %e, "dropping undecodable message");
                    next.acker.ack(BasicAckOptions::default()).await?;
                }
            }
            if out.len() >= max {
                break;
            }
        }
        Ok(out)
    }

    /// Read the project's log stream from `offset`, calling `f` for each event.
    /// `Event::Idle` fires whenever `idle` passes with no new message (never
    /// when `idle` is `None`). Returns when `f` returns `false`.
    pub async fn replay<F>(&self, project: &str, offset: Offset, idle: Option<Duration>, mut f: F) -> Result<()>
    where
        F: FnMut(Event) -> bool,
    {
        let stream = stream_name(project);
        self.channel
            .basic_qos(100, BasicQosOptions::default())
            .await?;
        let mut consumer = self
            .channel
            .basic_consume(
                ShortString::from(stream.as_str()),
                ShortString::from(""),
                BasicConsumeOptions::default(),
                table(&[("x-stream-offset", offset.amqp())]),
            )
            .await
            .with_context(|| format!("consuming stream {stream} (does the project exist?)"))?;

        loop {
            let delivery = match idle {
                Some(d) => match timeout(d, consumer.next()).await {
                    Ok(x) => x,
                    Err(_) => {
                        if f(Event::Idle) {
                            continue;
                        }
                        break;
                    }
                },
                None => consumer.next().await,
            };
            let delivery = match delivery {
                Some(Ok(d)) => d,
                Some(Err(e)) => return Err(e.into()),
                None => break,
            };
            delivery.acker.ack(BasicAckOptions::default()).await?;
            if let Ok(m) = serde_json::from_slice::<Message>(&delivery.data) {
                if !f(Event::Msg(m)) {
                    break;
                }
            }
        }
        Ok(())
    }
}
