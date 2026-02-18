use crate::cluster::TestCluster;
use crate::trace::TraceSequence;
use anyhow::{Context, Result, bail, ensure};
use futures::FutureExt;
use tantylla_common::tracing::events::{TestEvent, TestEventSource, TracePayload};
use uuid::Uuid;

#[tokio::test]
async fn e2e_cdc_to_gateway_search() -> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info")
        .with_test_writer()
        .try_init();

    let cluster = TestCluster::builder()
        .enable_instrumentation(true)
        .build()
        .await
        .context("building test cluster")?;

    cluster
        .scoped(|cluster| {
            async move {
                let trace_collector = cluster
                    .trace_collector()
                    .context("fetching trace collector")?;

                let startup_sequence = TraceSequence::new()
                    .event_from_source(TestEventSource::Node, TestEvent::Startup)
                    .event_from_source(TestEventSource::Gateway, TestEvent::Startup)
                    .event_from_source(TestEventSource::Ingestor, TestEvent::Startup);
                trace_collector
                    .wait_for_sequence(&startup_sequence, 20)
                    .await
                    .context("waiting for startup sequence")?;

                let doc_id = format!("doc-{}", Uuid::new_v4());
                cluster.insert_document(&doc_id, "hello", "world").await?;

                let ingestion_sequence = TraceSequence::new()
                    .event(TestEvent::CdcRowReceived)
                    .event(TestEvent::CdcRowRouted)
                    .event(TestEvent::IndexBatchResponse);
                let matched_events = trace_collector
                    .wait_for_sequence(&ingestion_sequence, 20)
                    .await?;

                let index_event = matched_events
                    .last()
                    .context("missing index_batch_response event")?;

                if let TracePayload::IndexBatchResponse {
                    processed_count,
                    skipped_count,
                    success,
                } = index_event.payload
                {
                    tracing::info!(
                        processed_count,
                        skipped_count,
                        success,
                        "Index batch response"
                    );
                    ensure!(processed_count == 1, "processed more than one document");
                    ensure!(skipped_count == 0, "skipped more than zero documents");
                } else {
                    bail!("unexpected payload type for index_batch_response");
                }

                // TODO: Verify it
                let checkpoint_timestamp = cluster
                    .wait_for_checkpoint(20)
                    .await
                    .context("waiting for checkpoint")?;

                let gateway = cluster.gateway()?;
                let (response, query) = gateway
                    .search_until_hits(&["document.title:hello", "title:hello", "hello"], 20)
                    .await?;

                tracing::info!(
                    query,
                    hit_count = response.total_hits,
                    "Gateway query matched"
                );
                ensure!(!response.hits.is_empty(), "gateway returned no hits");

                Ok(())
            }
            .boxed()
        })
        .await
}

#[tokio::test]
async fn e2e_gateway_failure_on_missing_node() -> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info")
        .with_test_writer()
        .try_init();

    let cluster = TestCluster::builder()
        .enable_instrumentation(true)
        .build()
        .await
        .context("building test cluster")?;

    cluster
        .scoped(|cluster| {
            async move {
                let trace_collector = cluster
                    .trace_collector()
                    .context("fetching trace collector")?;

                cluster
                    .terminate_node(0)
                    .await
                    .context("terminating search node")?;

                let gateway = cluster.gateway()?;
                let response = gateway
                    .search(&crate::cluster::gateway::SearchRequest {
                        query: "title:hello".to_string(),
                        limit: 10,
                        offset: 0,
                        consistency: 2,
                    })
                    .await;

                match response {
                    Err(e) if e.to_string().contains("500") => {
                        // Expected failure status check happens inside search() currently or we can handle it here
                        // search() currently bails on !success, so we expect Err
                        ensure!(
                            e.to_string().contains("Consistency ALL failed"),
                            "unexpected error: {}",
                            e
                        );
                    }
                    Ok(_) => bail!("expected gateway failure, but it succeeded"),
                    Err(e) => bail!("unexpected error: {}", e),
                }

                let failure_sequence = TraceSequence::new().event(TestEvent::SearchFailure);
                trace_collector
                    .wait_for_sequence(&failure_sequence, 20)
                    .await?;

                Ok(())
            }
            .boxed()
        })
        .await
}
