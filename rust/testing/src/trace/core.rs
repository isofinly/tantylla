use anyhow::Context;
use serde_json::Value;
use std::sync::Arc;
use tokio::net::UdpSocket;
use tokio::sync::Mutex;
use tokio::time::{Duration, timeout};

// =========================================================================
// Trace Collection (Debug-only instrumentation)
// =========================================================================

#[derive(Clone, Debug)]
pub struct TraceEvent {
    pub source: String,
    pub payload: Value,
}

#[derive(Clone, Default)]
pub struct TraceCollector {
    events: Arc<Mutex<Vec<TraceEvent>>>,
}

#[derive(Clone, Debug, Default)]
pub struct TraceSequence {
    steps: Vec<TraceSequenceStep>,
}

#[derive(Clone, Debug)]
pub struct TraceSequenceStep {
    source: Option<String>,
    event: String,
}

impl TraceCollector {
    pub async fn bind(addr: &str) -> anyhow::Result<(Self, UdpSocket)> {
        let socket = UdpSocket::bind(addr)
            .await
            .context("binding trace socket")?;
        Ok((Self::default(), socket))
    }

    pub async fn listen(&self, socket: UdpSocket) {
        let mut buf = vec![0u8; 65_536];
        loop {
            let Ok((len, _)) = socket.recv_from(&mut buf).await else {
                break;
            };

            let payload = match serde_json::from_slice::<Value>(&buf[..len]) {
                Ok(value) => value,
                Err(_) => continue,
            };

            let source = payload
                .get("source")
                .and_then(|value| value.as_str())
                .unwrap_or("unknown")
                .to_string();

            let event = TraceEvent { source, payload };
            let mut events = self.events.lock().await;
            events.push(event);
        }
    }

    pub async fn snapshot(&self) -> Vec<TraceEvent> {
        let events = self.events.lock().await;
        events.clone()
    }

    pub async fn wait_for_event_name(
        &self,
        event_name: &str,
        timeout_secs_max: u64,
    ) -> anyhow::Result<TraceEvent> {
        self.wait_for_event(
            |event| event.payload.get("event").and_then(|value| value.as_str()) == Some(event_name),
            timeout_secs_max,
        )
        .await
        .with_context(|| format!("waiting for trace event {}", event_name))
    }

    pub async fn wait_for_event_from_source(
        &self,
        source: &str,
        event_name: &str,
        timeout_secs_max: u64,
    ) -> anyhow::Result<TraceEvent> {
        self.wait_for_event(
            |event| {
                event.source == source
                    && event.payload.get("event").and_then(|value| value.as_str())
                        == Some(event_name)
            },
            timeout_secs_max,
        )
        .await
        .with_context(|| format!("waiting for {} trace event from {}", event_name, source))
    }

    pub async fn wait_for_event(
        &self,
        predicate: impl Fn(&TraceEvent) -> bool,
        timeout_secs_max: u64,
    ) -> anyhow::Result<TraceEvent> {
        let deadline = Duration::from_secs(timeout_secs_max);
        timeout(deadline, async {
            loop {
                let events = self.events.lock().await;
                if let Some(event) = events.iter().find(|event| predicate(event)) {
                    return event.clone();
                }
                drop(events);
                tokio::task::yield_now().await;
            }
        })
        .await
        .context("waiting for trace event")
    }

    pub async fn wait_for_sequence(
        &self,
        sequence: &TraceSequence,
        timeout_secs_max: u64,
    ) -> anyhow::Result<Vec<TraceEvent>> {
        let deadline = Duration::from_secs(timeout_secs_max);
        timeout(deadline, async {
            loop {
                let events = self.snapshot().await;
                if let Some(matched) = match_sequence(&events, sequence) {
                    return matched;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .context("waiting for trace sequence")
    }
}

impl TraceSequence {
    pub fn new() -> Self {
        Self { steps: Vec::new() }
    }

    pub fn event(mut self, event: impl Into<String>) -> Self {
        self.steps.push(TraceSequenceStep {
            source: None,
            event: event.into(),
        });
        self
    }

    pub fn event_from_source(
        mut self,
        source: impl Into<String>,
        event: impl Into<String>,
    ) -> Self {
        self.steps.push(TraceSequenceStep {
            source: Some(source.into()),
            event: event.into(),
        });
        self
    }
}

fn match_sequence(events: &[TraceEvent], sequence: &TraceSequence) -> Option<Vec<TraceEvent>> {
    let mut matched = Vec::with_capacity(sequence.steps.len());
    let mut cursor = 0;

    for step in &sequence.steps {
        let mut found = None;
        for (idx, event) in events.iter().enumerate().skip(cursor) {
            if step.matches(event) {
                found = Some((idx, event.clone()));
                break;
            }
        }

        let (idx, event) = found?;
        matched.push(event);
        cursor = idx + 1;
    }

    Some(matched)
}

impl TraceSequenceStep {
    fn matches(&self, event: &TraceEvent) -> bool {
        if let Some(source) = &self.source {
            if source != &event.source {
                return false;
            }
        }

        event.payload.get("event").and_then(|value| value.as_str()) == Some(self.event.as_str())
    }
}
