use crate::router::core::Router;
use async_trait::async_trait;
use scylla_cdc::consumer::CDCRow;
use std::sync::Arc;
use tantylla_common::tracing::events::{TestEvent, TestEventSource};

#[derive(Clone)]
pub(crate) struct Consumer {
    router: Arc<Router>,
}

#[async_trait]
impl scylla_cdc::consumer::Consumer for Consumer {
    async fn consume_cdc(&mut self, data: CDCRow<'_>) -> anyhow::Result<()> {
        tracing::debug!(
            target: "test_event",
            source = %TestEventSource::Ingestor,
            event = %TestEvent::CdcRowReceived,
            operation = format!("{:?}", data.operation)
        );
        match self.router.route(&data).await {
            Ok(_) => Ok(()),
            Err(e) => {
                tracing::error!(error = ?e, "Failed to route CDC row");
                Err(e)
            }
        }
    }
}

pub(crate) struct ConsumerFactory {
    router: Arc<Router>,
}

impl ConsumerFactory {
    pub fn new(router: Arc<Router>) -> Self {
        Self { router }
    }
}

#[async_trait]
impl scylla_cdc::consumer::ConsumerFactory for ConsumerFactory {
    async fn new_consumer(&self) -> Box<dyn scylla_cdc::consumer::Consumer> {
        Box::new(Consumer {
            router: self.router.clone(),
        })
    }
}
