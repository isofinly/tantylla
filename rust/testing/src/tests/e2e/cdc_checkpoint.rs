use crate::cluster::TestCluster;
use crate::trace::TraceSequence;
use anyhow::{Context, Result, bail};
use futures::FutureExt;
use tantylla_common::tracing::events::{TestEvent, TestEventSource};
use tokio::time::{Duration, sleep};
use uuid::Uuid;

#[tokio::test]
async fn e2e_checkpoint_not_advanced_on_partial_flush_failure() -> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info")
        .with_test_writer()
        .try_init();

    let cluster = TestCluster::builder()
        .with_search_nodes(2)
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
                    .event_from_source(TestEventSource::Node, TestEvent::Startup)
                    .event_from_source(TestEventSource::Gateway, TestEvent::Startup)
                    .event_from_source(TestEventSource::Ingestor, TestEvent::Startup);
                trace_collector
                    .wait_for_sequence(&startup_sequence, 30)
                    .await
                    .context("waiting for startup sequence")?;

                let baseline_id = format!("doc-{}", Uuid::new_v4());
                cluster
                    .insert_document(&baseline_id, "issue_008_baseline", "baseline body")
                    .await
                    .context("inserting baseline document")?;

                let gateway = cluster.gateway().context("building gateway client")?;
                gateway
                    .search_until_hits(&["document.title:issue_008_baseline"], 30)
                    .await
                    .context("waiting for baseline document to become searchable")?;

                cluster
                    .terminate_node(0)
                    .await
                    .context("terminating search node 0 to inject partial-flush fault")?;

                // 20 documents spread across 2 nodes (one is up, one is down)
                // to make a partial flush
                let target_title = format!("issue_008_target_{}", Uuid::new_v4().simple());
                for i in 0..20_u32 {
                    let doc_id = format!("doc-008-{}-{}", i, Uuid::new_v4());
                    cluster
                        .insert_document(&doc_id, &target_title, "target body")
                        .await
                        .with_context(|| format!("inserting target document {}", i))?;
                }

                trace_collector
                    .wait_for_event_from_source(
                        TestEventSource::Ingestor,
                        TestEvent::BatchFlushFailed,
                        30,
                    )
                    .await
                    .context("waiting for BatchFlushFailed event from ingestor")?;

                cluster
                    .terminate_ingestor(0)
                    .await
                    .context("terminating ingestor before restart")?;

                cluster
                    .restart_node(0)
                    .await
                    .context("restarting search node 0")?;

                // The ingestor resumes from the pre-fault checkpoint and
                // re-routes all 20 target documents.
                cluster
                    .restart_ingestor(0)
                    .await
                    .context("restarting ingestor")?;

                // Expect all 20 docs to appear because the checkpoint
                // was not advanced past them.
                let query = format!("document.title:{}", target_title);
                for attempt in 0..40_u32 {
                    let response = gateway
                        .search(&crate::cluster::gateway::SearchRequest {
                            query: query.clone(),
                            limit: 25,
                            offset: 0,
                            consistency: 1,
                            default_fields: vec![],
                            facet_fields: vec![],
                            boost_fields: vec![],
                            group_by_partition: false,
                        })
                        .await
                        .context("polling gateway for target documents")?;

                    tracing::info!(
                        attempt,
                        total_hits = response.total_hits,
                        "polling for issue_008 target documents"
                    );

                    if response.total_hits >= 20 {
                        return Ok(());
                    }

                    sleep(Duration::from_millis(500)).await;
                }

                bail!(
                    "issue #008 regression: expected all 20 target documents to be searchable \
                     after ingestor restart, but the checkpoint appears to have advanced past \
                     the undelivered data"
                );
            }
            .boxed()
        })
        .await
}
