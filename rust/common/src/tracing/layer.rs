use anyhow::{Context, Result};
use serde_json::{Map, Value};
use std::net::{SocketAddr, UdpSocket};
use std::sync::Arc;
use tracing::field::{Field, Visit};
use tracing::{Event, Subscriber};
use tracing_subscriber::Layer;
use tracing_subscriber::layer::Context as LayerContext;
use tracing_subscriber::registry::LookupSpan;

#[derive(Debug, Clone)]
pub struct TestEventLayer {
    socket: Arc<UdpSocket>,
    target: SocketAddr,
}

impl TestEventLayer {
    pub fn connect(port: u16) -> Result<Self> {
        let target = format!("127.0.0.1:{}", port)
            .parse::<SocketAddr>()
            .context("parsing test event socket address")?;
        let socket = UdpSocket::bind("0.0.0.0:0").context("binding test event socket")?;

        Ok(Self {
            socket: Arc::new(socket),
            target,
        })
    }
}

#[derive(Default, Debug, Clone)]
struct SpanFields {
    fields: Map<String, Value>,
}

#[derive(Default, Debug)]
struct JsonVisitor {
    fields: Map<String, Value>,
}

impl JsonVisitor {
    fn into_map(self) -> Map<String, Value> {
        self.fields
    }
}

impl Visit for JsonVisitor {
    fn record_i64(&mut self, field: &Field, value: i64) {
        self.fields
            .insert(field.name().to_string(), Value::from(value));
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        self.fields
            .insert(field.name().to_string(), Value::from(value));
    }

    fn record_f64(&mut self, field: &Field, value: f64) {
        self.fields
            .insert(field.name().to_string(), Value::from(value));
    }

    fn record_bool(&mut self, field: &Field, value: bool) {
        self.fields
            .insert(field.name().to_string(), Value::from(value));
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        self.fields
            .insert(field.name().to_string(), Value::from(value));
    }

    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.fields.insert(
            field.name().to_string(),
            Value::from(format!("{:?}", value)),
        );
    }
}

impl<S> Layer<S> for TestEventLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(
        &self,
        attrs: &tracing::span::Attributes<'_>,
        id: &tracing::Id,
        ctx: LayerContext<'_, S>,
    ) {
        let mut visitor = JsonVisitor::default();
        attrs.record(&mut visitor);

        if let Some(span) = ctx.span(id) {
            span.extensions_mut().insert(SpanFields {
                fields: visitor.fields,
            });
        }
    }

    fn on_record(
        &self,
        id: &tracing::Id,
        values: &tracing::span::Record<'_>,
        ctx: LayerContext<'_, S>,
    ) {
        if let Some(span) = ctx.span(id) {
            let mut visitor = JsonVisitor::default();
            values.record(&mut visitor);

            let mut extensions = span.extensions_mut();
            if extensions.get_mut::<SpanFields>().is_none() {
                extensions.insert(SpanFields::default());
            }
            let fields = extensions
                .get_mut::<SpanFields>()
                .expect("SpanFields present");
            for (key, value) in visitor.fields {
                fields.fields.insert(key, value);
            }
        }
    }

    fn on_event(&self, event: &Event<'_>, ctx: LayerContext<'_, S>) {
        if event.metadata().target() != "test_event" {
            return;
        }

        let mut visitor = JsonVisitor::default();
        event.record(&mut visitor);
        let mut payload = visitor.into_map();

        if let Some(scope) = ctx.event_scope(event) {
            for span in scope.from_root() {
                if let Some(fields) = span.extensions().get::<SpanFields>() {
                    for (key, value) in &fields.fields {
                        payload.entry(key.clone()).or_insert_with(|| value.clone());
                    }
                }
            }
        }

        let payload = Value::Object(payload);
        if let Ok(bytes) = serde_json::to_vec(&payload) {
            let _ = self.socket.send_to(&bytes, self.target);
        }
    }
}
