use anyhow::Context;
use serde_json::Value;
use std::str::FromStr;
use std::sync::Arc;
use tantylla_common::tracing::events::{TestEvent, TestEventSource, TracePayload};
use tokio::net::UdpSocket;
use tokio::sync::Mutex;
use tokio::time::{Duration, timeout};

// =========================================================================
// Trace Collection (Debug-only instrumentation)
// =========================================================================

#[derive(Clone, Debug)]
pub struct TraceEvent {
    pub source: TestEventSource,
    pub payload: TracePayload,
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
    source: Option<TestEventSource>,
    event: TestEvent,
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

            let json = match serde_json::from_slice::<Value>(&buf[..len]) {
                Ok(value) => value,
                Err(_) => continue,
            };

            let source = json
                .get("source")
                .and_then(|value| value.as_str())
                .unwrap_or("Unspecified")
                .to_string();
            let source = TestEventSource::from_str(&source)
                .context("Failed to create event from source string")
                .unwrap();

            let payload = match serde_json::from_slice::<TracePayload>(&buf[..len]) {
                Ok(p) => p,
                Err(e) => {
                    tracing::warn!(error = %e, json = ?json, "failed to parse trace payload");
                    continue;
                }
            };

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
        event_name: TestEvent,
        timeout_secs_max: u64,
    ) -> anyhow::Result<TraceEvent> {
        let result = self
            .wait_for_event(|event| event.discriminant() == event_name, timeout_secs_max)
            .await;

        match result {
            Ok(event) => Ok(event),
            Err(err) => {
                let summary = self.last_events_summary().await;
                Err(err).with_context(|| {
                    format!(
                        "waiting for trace event {}. observed: {}",
                        <TestEvent as Into<&str>>::into(event_name),
                        summary
                    )
                })
            }
        }
    }

    pub async fn wait_for_event_from_source(
        &self,
        source: TestEventSource,
        event_name: TestEvent,
        timeout_secs_max: u64,
    ) -> anyhow::Result<TraceEvent> {
        let result = self
            .wait_for_event(
                |event| event.source == source && event.discriminant() == event_name,
                timeout_secs_max,
            )
            .await;

        match result {
            Ok(event) => Ok(event),
            Err(err) => {
                let summary = self.last_events_summary().await;
                Err(err).with_context(|| {
                    format!(
                        "waiting for {} trace event from {:?}. observed: {}",
                        event_name, source, summary
                    )
                })
            }
        }
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
        let (matched, _cursor) = self
            .wait_for_sequence_after(sequence, 0, timeout_secs_max)
            .await?;
        Ok(matched)
    }

    /// Like [`wait_for_sequence`] but begins scanning from `start_from` (a
    /// previously returned cursor) and returns the new cursor position after
    /// the last matched event.  The cursor is the index *past* the last matched
    /// event, so the caller can pass it straight back on the next call to avoid
    /// re-matching events from a prior phase.
    ///
    /// Use this when a single test calls `wait_for_sequence` more than once on
    /// the same `TraceCollector` — passing the cursor forward prevents the
    /// second call from satisfying itself against events that were already
    /// consumed by the first call.
    pub async fn wait_for_sequence_after(
        &self,
        sequence: &TraceSequence,
        start_from: usize,
        timeout_secs_max: u64,
    ) -> anyhow::Result<(Vec<TraceEvent>, usize)> {
        let deadline = Duration::from_secs(timeout_secs_max);
        let result = timeout(deadline, async {
            loop {
                let events = self.snapshot().await;
                if let Some((matched, next_cursor)) =
                    match_sequence_from(&events, sequence, start_from)
                {
                    return (matched, next_cursor);
                }
                tokio::task::yield_now().await;
            }
        })
        .await;

        match result {
            Ok(pair) => Ok(pair),
            Err(err) => {
                let summary = self.last_events_summary().await;
                Err(err).with_context(|| {
                    format!(
                        "waiting for trace sequence (from cursor {}): [{}]. observed: {}",
                        start_from,
                        sequence.describe(),
                        summary
                    )
                })
            }
        }
    }

    async fn last_events_summary(&self) -> String {
        let events = self.events.lock().await;
        if events.is_empty() {
            return "no events".to_string();
        }

        let tail = events.iter().rev().take(8).rev();
        let mut entries = Vec::new();
        for event in tail {
            let name = event.discriminant();
            let source = <TestEventSource as Into<&str>>::into(event.source);
            entries.push(format!("{}:{}", source, name));
        }

        format!(
            "last {} of {} -> {}",
            entries.len(),
            events.len(),
            entries.join(", ")
        )
    }
}

impl TraceEvent {
    pub fn discriminant(&self) -> TestEvent {
        TestEvent::from(&self.payload)
    }
}

impl TraceSequence {
    pub fn new() -> Self {
        Self { steps: Vec::new() }
    }
    /// See [`tantylla_common::test_tracing::TestEvent`] for available events
    pub fn event(mut self, event: impl Into<TestEvent>) -> Self {
        self.steps.push(TraceSequenceStep {
            source: None,
            event: event.into(),
        });
        self
    }

    /// See [`tantylla_common::test_tracing::TestEventSource`] for available sources
    ///
    /// See [`tantylla_common::test_tracing::TestEvent`] for available events
    pub fn event_from_source(
        mut self,
        source: impl Into<TestEventSource>,
        event: impl Into<TestEvent>,
    ) -> Self {
        self.steps.push(TraceSequenceStep {
            source: Some(source.into()),
            event: event.into(),
        });
        self
    }

    fn describe(&self) -> String {
        self.steps
            .iter()
            .map(|step| step.describe())
            .collect::<Vec<_>>()
            .join(", ")
    }
}

/// Matches `sequence` against `events` starting at `start_from`.
///
/// Returns the matched events and the index *after* the last matched event so
/// the caller can use it as the `start_from` argument for the next call.
fn match_sequence_from(
    events: &[TraceEvent],
    sequence: &TraceSequence,
    start_from: usize,
) -> Option<(Vec<TraceEvent>, usize)> {
    let mut matched = Vec::with_capacity(sequence.steps.len());
    let mut cursor = start_from;

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

    Some((matched, cursor))
}

impl TraceSequenceStep {
    fn matches(&self, event: &TraceEvent) -> bool {
        if let Some(source) = &self.source
            && source != &event.source
        {
            return false;
        }

        event.discriminant() == self.event
    }

    fn describe(&self) -> String {
        let event = <TestEvent as Into<&str>>::into(self.event);
        if let Some(source) = &self.source {
            let source = <TestEventSource as Into<&str>>::into(*source);
            format!("{}:{}", source, event)
        } else {
            event.to_string()
        }
    }
}
