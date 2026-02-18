use crate::cluster::TestCluster;
use crate::trace::TraceSequence;
use anyhow::{Context, Result, ensure};
use serde_json::Value;
use tantylla_common::tracing::events::{TestEvent, TestEventSource};
use tokio::time::{Duration, sleep};
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

    let test_result = async {
        let trace_collector = cluster
            .trace_collector()
            .context("fetching trace collector")?;

        let startup_sequence = TraceSequence::new()
            .event_from_source(TestEventSource::Node, TestEvent::Startup)
            .event_from_source(TestEventSource::Gateway, TestEvent::Startup)
            .event_from_source(TestEventSource::Ingestor, TestEvent::Startup);
        trace_collector
            .wait_for_sequence(&startup_sequence, 2)
            .await
            .context("waiting for startup sequence")?;

        let doc_id = format!("doc-{}", Uuid::new_v4());
        let title = "hello";
        let body = "world";

        let insert_cql = format!(
            "INSERT INTO {}.{} (doc_id, title, body) VALUES (?, ?, ?)",
            cluster.keyspace(),
            cluster.table_name(),
        );
        cluster
            .session()
            .query_unpaged(insert_cql, (doc_id.as_str(), title, body))
            .await
            .context("inserting document")?;

        sleep(Duration::from_millis(250)).await;

        let cdc_table = format!("{}_scylla_cdc_log", cluster.table_name());
        let cdc_op_query = format!(
            "SELECT \"cdc$operation\" FROM {}.{} LIMIT 1",
            cluster.keyspace(),
            cdc_table,
        );
        let cdc_op_result = cluster
            .session()
            .query_unpaged(cdc_op_query, ())
            .await
            .context("reading cdc operation")?;
        let cdc_rows = cdc_op_result
            .into_rows_result()
            .context("parsing cdc operation rows")?;
        let mut cdc_iter = cdc_rows
            .rows::<(i8,)>()
            .context("decoding cdc operation row")?;
        let cdc_op_code = cdc_iter.next().map(|row| row.map(|(op,)| op)).transpose()?;
        tracing::info!(cdc_op_code = ?cdc_op_code, "Observed CDC operation code");

        let ingestion_sequence = TraceSequence::new()
            .event(TestEvent::CdcRowReceived)
            .event(TestEvent::CdcRowRouted)
            .event(TestEvent::IndexBatchResponse);
        let matched_events = trace_collector
            .wait_for_sequence(&ingestion_sequence, 20)
            .await?;
        let received = matched_events
            .first()
            .context("missing cdc_row_received event")?;
        tracing::info!(operation = ?received.payload.get("operation"), "CDC row received");

        let index_event = matched_events
            .last()
            .context("missing index_batch_response event")?;
        let processed = index_event
            .payload
            .get("processed_count")
            .and_then(|value| value.as_u64())
            .unwrap_or(0);
        let skipped = index_event
            .payload
            .get("skipped_count")
            .and_then(|value| value.as_u64())
            .unwrap_or(0);
        tracing::info!(
            processed,
            skipped,
            success = ?index_event.payload.get("success"),
            "Index batch response"
        );
        ensure!(
            processed + skipped >= 1,
            "unexpected index response: {:?}",
            index_event.payload
        );

        let checkpoint = cluster
            .wait_for_checkpoint(20)
            .await
            .context("waiting for checkpoint")?;
        tracing::info!(
            checkpoint_micros = checkpoint.as_micros(),
            "Checkpoint committed"
        );

        let gateway_addr = cluster
            .gateway_addrs()
            .first()
            .context("missing gateway address")?;
        let client = reqwest::Client::new();

        sleep(Duration::from_secs(1)).await;

        let query_candidates = [
            "document.title:hello",
            "title:hello",
            "hello",
            "document:hello",
        ];
        let mut last_response: Option<Value> = None;
        let mut query_matched = None;

        for query in query_candidates {
            for _ in 0..20 {
                let response = client
                    .post(format!("http://{}/api/v1/search", gateway_addr))
                    .json(&serde_json::json!({
                        "query": query,
                        "limit": 10,
                        "offset": 0,
                        "consistency": 1,
                    }))
                    .send()
                    .await
                    .with_context(|| format!("sending gateway search with query {}", query))?;
                let status = response.status();
                let body = response
                    .text()
                    .await
                    .context("reading gateway response body")?;
                if body.trim().is_empty() {
                    anyhow::bail!("gateway returned empty body with status {}", status);
                }
                let payload = serde_json::from_str::<Value>(&body)
                    .with_context(|| format!("parsing gateway response: {}", body))?;
                let total_hits = payload
                    .get("total_hits")
                    .and_then(|value| value.as_u64())
                    .unwrap_or(0);
                if total_hits > 0 {
                    last_response = Some(payload);
                    query_matched = Some(query);
                    break;
                }
                last_response = Some(payload);
                sleep(Duration::from_millis(500)).await;
            }
            if query_matched.is_some() {
                break;
            }
        }

        let response = last_response.context("missing gateway response")?;
        let hits = response
            .get("hits")
            .and_then(|value| value.as_array())
            .cloned()
            .unwrap_or_default();
        ensure!(!hits.is_empty(), "gateway returned no hits: {}", response);
        tracing::info!(query = ?query_matched, "Gateway query matched");

        Ok(())
    }
    .await;

    cluster
        .shutdown()
        .await
        .context("shutting down test cluster")?;

    test_result
}

#[tokio::test]
async fn e2e_gateway_failure_on_missing_node() -> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info")
        .with_test_writer()
        .try_init();

    let mut cluster = TestCluster::builder()
        .enable_instrumentation(true)
        .build()
        .await
        .context("building test cluster")?;

    let test_result = async {
        let trace_collector = cluster
            .trace_collector()
            .context("fetching trace collector")?;

        cluster
            .terminate_node(0)
            .await
            .context("terminating search node")?;

        let gateway_addr = cluster
            .gateway_addrs()
            .first()
            .context("missing gateway address")?;
        let client = reqwest::Client::new();

        let response = client
            .post(format!("http://{}/api/v1/search", gateway_addr))
            .json(&serde_json::json!({
                "query": "title:hello",
                "limit": 10,
                "offset": 0,
                "consistency": 2,
            }))
            .send()
            .await
            .context("sending gateway search")?;

        ensure!(
            response.status().as_u16() == 500,
            "expected gateway failure, got {}",
            response.status()
        );

        let payload = response
            .json::<Value>()
            .await
            .context("parsing gateway response")?;
        let error_message = payload
            .get("message")
            .and_then(|value| value.as_str())
            .unwrap_or_default();
        ensure!(
            error_message.contains("Consistency ALL failed"),
            "unexpected error payload: {}",
            payload
        );

        let failure_sequence = TraceSequence::new().event(TestEvent::SearchFailure);
        trace_collector
            .wait_for_sequence(&failure_sequence, 20)
            .await?;

        Ok(())
    }
    .await;

    cluster
        .shutdown()
        .await
        .context("shutting down test cluster")?;

    test_result
}
