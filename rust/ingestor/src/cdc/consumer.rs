use crate::router::{
    common::{PendingRangeStart, RoutingAction},
    core::Router,
};
use anyhow::Context;
use async_trait::async_trait;
use scylla_cdc::consumer::CDCRow;
use tantylla_common::tracing::events::{TestEvent, TestEventSource};
use tracing::warn;

/// A CDC consumer that owns all per-stream mutable state.
///
/// `scylla_cdc` creates one [`Consumer`] per VNode group via
/// [`ConsumerFactory::new_consumer`].
pub(crate) struct Consumer {
    router: Router,
    /// In-flight start bound of the current range-delete pair, if any.
    ///
    /// ScyllaDB CDC guarantees that a start-bound row (op 5/6) is immediately
    /// followed by the matching end-bound row (op 7/8) within the same stream
    pending_range_start: Option<PendingRangeStart>,
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

        let action = RoutingAction::determine(&data).inspect_err(|e| {
            tracing::debug!(
                target: "test_event",
                source = %TestEventSource::Ingestor,
                event = %TestEvent::CdcRowRouteFailure,
                error = %e,
            );
        })?;

        match action {
            RoutingAction::Skip => {
                tracing::debug!("Skipping PreImage CDC row (informational only)");
                Ok(())
            }

            RoutingAction::RangeDeleteStart => {
                let pending = self
                    .router
                    .extract_range_delete_start(&data)
                    .context("extract range-delete start bound")?;

                if self.pending_range_start.replace(pending).is_some() {
                    warn!(
                        "Overwriting an unconsumed range-delete start bound; \
                         this may indicate a CDC ordering anomaly"
                    );
                }

                Ok(())
            }

            RoutingAction::RangeDeleteEnd => {
                let start = self
                    .pending_range_start
                    .take()
                    .context("received range-delete end bound without a matching start bound")?;

                self.router
                    .commit_range_delete(start, &data)
                    .await
                    .context("commit range delete")?;

                Ok(())
            }

            RoutingAction::Forward { op_type } => self
                .router
                .route_forward(&data, op_type)
                .await
                .inspect_err(|e| {
                    tracing::error!(error = ?e, "Failed to route CDC row");
                }),
        }
    }
}

pub(crate) struct ConsumerFactory {
    router: Router,
}

impl ConsumerFactory {
    pub fn new(router: Router) -> Self {
        Self { router }
    }
}

#[async_trait]
impl scylla_cdc::consumer::ConsumerFactory for ConsumerFactory {
    async fn new_consumer(&self) -> Box<dyn scylla_cdc::consumer::Consumer> {
        Box::new(Consumer {
            router: self.router.clone(),
            pending_range_start: None,
        })
    }
}
