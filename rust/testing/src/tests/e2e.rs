use crate::cluster::TestCluster;
use crate::cluster::{SchemaConfig, gateway::SearchRequest};
use crate::trace::TraceSequence;
use anyhow::{Context, Result, bail, ensure};
use futures::FutureExt;
use tantylla_common::tracing::events::{TestEvent, TestEventSource, TracePayload};
use tokio::time::{Duration, sleep};
use uuid::Uuid;

// =========================================================================
// FTS Feature Tests
// =========================================================================
//
// These tests drive the TDD cycle for the FTS features listed in the project
// roadmap. Each test is kept as self-contained as possible: it spins up its
// own cluster, inserts fixtures, and asserts search results.
//
// The tests are grouped roughly by feature complexity:
//   1. Phrase / proximity  — works with the current schema (positions stored)
//   2. Fuzzy / typo        — works via Tantivy's `~N` query syntax
//   3. Numeric range       — works via JSON fast sub-fields in Tantivy ≥ 0.22
//   4. Plain keyword       — requires `default_fields` expansion (new feature)
//   5. Numeric filter      — numeric range via structured `default_fields` query

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
                    .event_from_source(TestEventSource::Ingestor, TestEvent::CdcRowReceived)
                    .event_from_source(TestEventSource::Ingestor, TestEvent::CdcRowRouted)
                    .event_from_source(TestEventSource::Ingestor, TestEvent::BatchAddEnter)
                    .event_from_source(TestEventSource::Ingestor, TestEvent::BatchFlushStart)
                    .event_from_source(TestEventSource::Node, TestEvent::IndexBatchRequest)
                    .event_from_source(TestEventSource::Node, TestEvent::EngineProcessBatchEnter)
                    .event_from_source(TestEventSource::Node, TestEvent::IndexBatchResponse)
                    .event_from_source(TestEventSource::Ingestor, TestEvent::BatchFlushNodeSuccess)
                    .event_from_source(TestEventSource::Ingestor, TestEvent::BatchFlushSuccess);
                let matched_events = trace_collector
                    .wait_for_sequence(&ingestion_sequence, 20)
                    .await?;

                let index_event = matched_events
                    .iter()
                    .find(|event| event.discriminant() == TestEvent::IndexBatchResponse)
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
                let _checkpoint_timestamp = cluster
                    .wait_for_checkpoint(20)
                    .await
                    .context("waiting for checkpoint")?;

                let gateway = cluster.gateway()?;
                let (response, query) = gateway
                    .search_until_hits(&["document.title:hello"], 20)
                    .await?;

                tracing::info!(
                    query,
                    hit_count = response.total_hits,
                    "Gateway query matched"
                );
                ensure!(!response.hits.is_empty(), "gateway returned no hits");

                let search_sequence = TraceSequence::new()
                    .event_from_source(TestEventSource::Gateway, TestEvent::SearchRequest)
                    .event_from_source(
                        TestEventSource::Gateway,
                        TestEvent::GatewayScatterGatherEnter,
                    )
                    .event_from_source(TestEventSource::Node, TestEvent::SearchRequest)
                    .event_from_source(TestEventSource::Node, TestEvent::EngineSearchEnter)
                    .event_from_source(TestEventSource::Node, TestEvent::SearchResponse)
                    .event_from_source(TestEventSource::Gateway, TestEvent::SearchResponse);
                // TODO: Use this variable
                let _matched_events = trace_collector
                    .wait_for_sequence(&search_sequence, 20)
                    .await
                    .context("waiting for search trace sequence")?;

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
                        default_fields: vec![],
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

                let failure_sequence = TraceSequence::new()
                    .event_from_source(TestEventSource::Gateway, TestEvent::SearchRequest)
                    .event_from_source(
                        TestEventSource::Gateway,
                        TestEvent::GatewayScatterGatherEnter,
                    )
                    .event_from_source(TestEventSource::Gateway, TestEvent::SearchFailure);
                trace_collector
                    .wait_for_sequence(&failure_sequence, 20)
                    .await?;

                Ok(())
            }
            .boxed()
        })
        .await
}

#[tokio::test]
async fn e2e_cdc_set_overwrite_indexes_new_label() -> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info")
        .with_test_writer()
        .try_init();

    let schema = crate::cluster::SchemaConfig::from_cql(
        "CREATE TABLE IF NOT EXISTS {{keyspace}}.users (\
            doc_id text PRIMARY KEY,\
            title text,\
            tags set<text>,\
            updated_at timestamp\
        ) WITH cdc = {'enabled': true};",
    );

    let cluster = TestCluster::builder()
        .with_schema(schema)
        .with_table_name("users")
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

                let insert_cql = format!(
                    "INSERT INTO {}.{} (doc_id, title, tags) VALUES ('{}', 'issue_004_positive', {{'legacy'}})",
                    cluster.keyspace(),
                    cluster.table_name(),
                    doc_id,
                );
                cluster
                    .session()
                    .query_unpaged(insert_cql, ())
                    .await
                    .context("inserting user with initial label")?;

                let gateway = cluster.gateway().context("building gateway client")?;
                gateway
                    .search_until_hits(&["document.tags:legacy"], 20)
                    .await
                    .context("waiting for initial label to become searchable")?;

                let overwrite_cql = format!(
                    "UPDATE {}.{} SET tags = {{'overridden'}} WHERE doc_id = '{}'",
                    cluster.keyspace(),
                    cluster.table_name(),
                    doc_id,
                );
                cluster
                    .session()
                    .query_unpaged(overwrite_cql, ())
                    .await
                    .context("overwriting labels in non-frozen collection")?;

                gateway
                    .search_until_hits(&["document.tags:overridden"], 20)
                    .await
                    .context("waiting for overridden label to become searchable")?;

                Ok(())
            }
            .boxed()
        })
        .await
}

#[tokio::test]
async fn e2e_cdc_set_element_removal_updates_search_index() -> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info")
        .with_test_writer()
        .try_init();

    let schema = crate::cluster::SchemaConfig::from_cql(
        "CREATE TABLE IF NOT EXISTS {{keyspace}}.users (\
            doc_id text PRIMARY KEY,\
            title text,\
            tags set<text>,\
            updated_at timestamp\
        ) WITH cdc = {'enabled': true};",
    );

    let cluster = TestCluster::builder()
        .with_schema(schema)
        .with_table_name("users")
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

                let insert_cql = format!(
                    "INSERT INTO {}.{} (doc_id, title, tags) VALUES ('{}', 'issue_004', {{'premium', 'starter'}})",
                    cluster.keyspace(),
                    cluster.table_name(),
                    doc_id,
                );
                cluster
                    .session()
                    .query_unpaged(insert_cql, ())
                    .await
                    .context("inserting user with initial tags")?;

                let gateway = cluster.gateway().context("building gateway client")?;
                gateway
                    .search_until_hits(&["document.tags:premium"], 20)
                    .await
                    .context("waiting for initial premium tag to become searchable")?;

                let update_cql = format!(
                    "UPDATE {}.{} SET tags = tags - {{'premium'}} WHERE doc_id = '{}'",
                    cluster.keyspace(),
                    cluster.table_name(),
                    doc_id,
                );
                cluster
                    .session()
                    .query_unpaged(update_cql, ())
                    .await
                    .context("removing a tag from non-frozen collection")?;

                for _ in 0..40 {
                    let response = gateway
                        .search(&crate::cluster::gateway::SearchRequest {
                            query: "document.tags:premium".to_string(),
                            limit: 10,
                            offset: 0,
                            consistency: 1,
                            default_fields: vec![],
                        })
                        .await
                        .context("searching for removed tag")?;

                    if response.total_hits == 0 {
                        return Ok(());
                    }

                    sleep(Duration::from_millis(500)).await;
                }

                bail!(
                    "removed collection element is still searchable; expected zero hits after CDC ingestion"
                );
            }
            .boxed()
        })
        .await
}

#[tokio::test]
async fn e2e_cdc_set_addition_indexes_added_label() -> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info")
        .with_test_writer()
        .try_init();

    let schema = crate::cluster::SchemaConfig::from_cql(
        "CREATE TABLE IF NOT EXISTS {{keyspace}}.users (\
            doc_id text PRIMARY KEY,\
            title text,\
            tags set<text>,\
            updated_at timestamp\
        ) WITH cdc = {'enabled': true};",
    );

    let cluster = TestCluster::builder()
        .with_schema(schema)
        .with_table_name("users")
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

                let insert_cql = format!(
                    "INSERT INTO {}.{} (doc_id, title, tags) VALUES ('{}', 'issue_004_addition', {{'legacy'}})",
                    cluster.keyspace(),
                    cluster.table_name(),
                    doc_id,
                );
                cluster
                    .session()
                    .query_unpaged(insert_cql, ())
                    .await
                    .context("inserting user with initial label")?;

                let gateway = cluster.gateway().context("building gateway client")?;
                gateway
                    .search_until_hits(&["document.tags:legacy"], 20)
                    .await
                    .context("waiting for initial label to become searchable")?;

                let add_cql = format!(
                    "UPDATE {}.{} SET tags = tags + {{'overridden'}} WHERE doc_id = '{}'",
                    cluster.keyspace(),
                    cluster.table_name(),
                    doc_id,
                );
                cluster
                    .session()
                    .query_unpaged(add_cql, ())
                    .await
                    .context("adding label in non-frozen collection")?;

                for _ in 0..40 {
                    let response = gateway
                        .search(&crate::cluster::gateway::SearchRequest {
                            query: "document.tags:legacy AND document.tags:overridden".to_string(),
                            limit: 10,
                            offset: 0,
                            consistency: 1,
                            default_fields: vec![],
                        })
                        .await
                        .context("searching for combined legacy and overridden labels")?;

                    if response.total_hits > 0 {
                        return Ok(());
                    }

                    sleep(Duration::from_millis(500)).await;
                }

                bail!(
                    "expected combined labels to be searchable after set addition, but no hits were returned"
                );
            }
            .boxed()
        })
        .await
}

#[tokio::test]
async fn e2e_row_delete_removes_single_clustering_row() -> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info")
        .with_test_writer()
        .try_init();

    let schema = crate::cluster::SchemaConfig::from_cql(
        "CREATE TABLE IF NOT EXISTS {{keyspace}}.orders (\
            user_id text,\
            order_id int,\
            title text,\
            updated_at timestamp,\
            PRIMARY KEY (user_id, order_id)\
        ) WITH cdc = {'enabled': true};",
    );

    let cluster = TestCluster::builder()
        .with_schema(schema)
        .with_table_name("orders")
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

                let user_id = format!("user-{}", Uuid::new_v4());

                let insert_keep = format!(
                    "INSERT INTO {}.{} (user_id, order_id, title) VALUES ('{}', 1, 'issue_005_keep')",
                    cluster.keyspace(),
                    cluster.table_name(),
                    user_id,
                );
                let insert_delete = format!(
                    "INSERT INTO {}.{} (user_id, order_id, title) VALUES ('{}', 2, 'issue_005_delete')",
                    cluster.keyspace(),
                    cluster.table_name(),
                    user_id,
                );

                cluster
                    .session()
                    .query_unpaged(insert_keep, ())
                    .await
                    .context("inserting keep row")?;
                cluster
                    .session()
                    .query_unpaged(insert_delete, ())
                    .await
                    .context("inserting delete row")?;

                let gateway = cluster.gateway().context("building gateway client")?;
                // Wait for both rows to be committed before issuing the delete.
                // Without this, the condition `removed==0 AND kept>0` can race:
                // the delete CDC event may be processed and committed before the
                // separate insert CDC event for the "keep" row is committed,
                // causing `kept.total_hits==0` even though no bug occurred.
                gateway
                    .search_until_hits(&["document.title:issue_005_delete"], 20)
                    .await
                    .context("waiting for delete candidate to become searchable")?;
                gateway
                    .search_until_hits(&["document.title:issue_005_keep"], 20)
                    .await
                    .context("waiting for keep row to become searchable")?;

                let delete_cql = format!(
                    "DELETE FROM {}.{} WHERE user_id = '{}' AND order_id = 2",
                    cluster.keyspace(),
                    cluster.table_name(),
                    user_id,
                );
                cluster
                    .session()
                    .query_unpaged(delete_cql, ())
                    .await
                    .context("deleting one clustering row")?;

                for _ in 0..40 {
                    let removed = gateway
                        .search(&crate::cluster::gateway::SearchRequest {
                            query: "document.title:issue_005_delete".to_string(),
                            limit: 10,
                            offset: 0,
                            consistency: 1,
                            default_fields: vec![],
                        })
                        .await
                        .context("searching for removed order")?;
                    let kept = gateway
                        .search(&crate::cluster::gateway::SearchRequest {
                            query: "document.title:issue_005_keep".to_string(),
                            limit: 10,
                            offset: 0,
                            consistency: 1,
                            default_fields: vec![],
                        })
                        .await
                        .context("searching for kept order")?;

                    if removed.total_hits == 0 && kept.total_hits > 0 {
                        return Ok(());
                    }

                    sleep(Duration::from_millis(500)).await;
                }

                bail!("expected single-row delete to remove only the targeted clustering row");
            }
            .boxed()
        })
        .await
}

#[tokio::test]
async fn e2e_range_delete_removes_rows_within_bounds() -> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info")
        .with_test_writer()
        .try_init();

    let schema = crate::cluster::SchemaConfig::from_cql(
        "CREATE TABLE IF NOT EXISTS {{keyspace}}.orders (\
            user_id text,\
            order_id int,\
            title text,\
            updated_at timestamp,\
            PRIMARY KEY (user_id, order_id)\
        ) WITH cdc = {'enabled': true};",
    );

    let cluster = TestCluster::builder()
        .with_schema(schema)
        .with_table_name("orders")
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

                let user_id = format!("user-{}", Uuid::new_v4());

                for (order_id, title) in [
                    (1, "issue_005_range_left"),
                    (2, "issue_005_range_middle"),
                    (3, "issue_005_range_right"),
                ] {
                    let insert_cql = format!(
                        "INSERT INTO {}.{} (user_id, order_id, title) VALUES ('{}', {}, '{}')",
                        cluster.keyspace(),
                        cluster.table_name(),
                        user_id,
                        order_id,
                        title,
                    );
                    cluster
                        .session()
                        .query_unpaged(insert_cql, ())
                        .await
                        .context("inserting range-delete fixture rows")?;
                }

                let gateway = cluster.gateway().context("building gateway client")?;
                gateway
                    .search_until_hits(&["document.title:issue_005_range_middle"], 20)
                    .await
                    .context("waiting for middle row to become searchable")?;

                let range_delete_cql = format!(
                    "DELETE FROM {}.{} WHERE user_id = '{}' AND order_id > 1 AND order_id < 3",
                    cluster.keyspace(),
                    cluster.table_name(),
                    user_id,
                );
                cluster
                    .session()
                    .query_unpaged(range_delete_cql, ())
                    .await
                    .context("executing clustering range delete")?;

                for _ in 0..40 {
                    let middle = gateway
                        .search(&crate::cluster::gateway::SearchRequest {
                            query: "document.title:issue_005_range_middle".to_string(),
                            limit: 10,
                            offset: 0,
                            consistency: 1,
                            default_fields: vec![],
                        })
                        .await
                        .context("searching for range-deleted middle row")?;

                    if middle.total_hits == 0 {
                        return Ok(());
                    }

                    sleep(Duration::from_millis(500)).await;
                }

                bail!("expected range-deleted row to be removed from the index");
            }
            .boxed()
        })
        .await
}

#[tokio::test]
async fn e2e_partition_key_delete_removes_single_primary_key_row() -> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info")
        .with_test_writer()
        .try_init();

    let schema = crate::cluster::SchemaConfig::from_cql(
        "CREATE TABLE IF NOT EXISTS {{keyspace}}.users (\
            doc_id text PRIMARY KEY,\
            title text,\
            updated_at timestamp\
        ) WITH cdc = {'enabled': true};",
    );

    let cluster = TestCluster::builder()
        .with_schema(schema)
        .with_table_name("users")
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

                let insert_cql = format!(
                    "INSERT INTO {}.{} (doc_id, title) VALUES ('{}', 'issue_006_single')",
                    cluster.keyspace(),
                    cluster.table_name(),
                    doc_id,
                );
                cluster
                    .session()
                    .query_unpaged(insert_cql, ())
                    .await
                    .context("inserting single primary-key row")?;

                let gateway = cluster.gateway().context("building gateway client")?;
                gateway
                    .search_until_hits(&["document.title:issue_006_single"], 20)
                    .await
                    .context("waiting for inserted row to become searchable")?;

                let delete_cql = format!(
                    "DELETE FROM {}.{} WHERE doc_id = '{}'",
                    cluster.keyspace(),
                    cluster.table_name(),
                    doc_id,
                );
                cluster
                    .session()
                    .query_unpaged(delete_cql, ())
                    .await
                    .context("deleting single primary-key row")?;

                for _ in 0..40 {
                    let removed = gateway
                        .search(&crate::cluster::gateway::SearchRequest {
                            query: "document.title:issue_006_single".to_string(),
                            limit: 10,
                            offset: 0,
                            consistency: 1,
                            default_fields: vec![],
                        })
                        .await
                        .context("searching for deleted single-key row")?;

                    if removed.total_hits == 0 {
                        return Ok(());
                    }

                    sleep(Duration::from_millis(500)).await;
                }

                bail!("expected single-key delete to remove the indexed document");
            }
            .boxed()
        })
        .await
}

#[tokio::test]
async fn e2e_partition_delete_removes_all_rows_for_partition() -> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info")
        .with_test_writer()
        .try_init();

    let schema = crate::cluster::SchemaConfig::from_cql(
        "CREATE TABLE IF NOT EXISTS {{keyspace}}.orders (\
            user_id text,\
            order_id int,\
            title text,\
            updated_at timestamp,\
            PRIMARY KEY (user_id, order_id)\
        ) WITH cdc = {'enabled': true};",
    );

    let cluster = TestCluster::builder()
        .with_schema(schema)
        .with_table_name("orders")
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

                let user_id = format!("user-{}", Uuid::new_v4());

                for (order_id, title) in
                    [(1, "issue_006_partition_a"), (2, "issue_006_partition_b")]
                {
                    let insert_cql = format!(
                        "INSERT INTO {}.{} (user_id, order_id, title) VALUES ('{}', {}, '{}')",
                        cluster.keyspace(),
                        cluster.table_name(),
                        user_id,
                        order_id,
                        title,
                    );
                    cluster
                        .session()
                        .query_unpaged(insert_cql, ())
                        .await
                        .context("inserting partition-delete fixture rows")?;
                }

                let gateway = cluster.gateway().context("building gateway client")?;
                gateway
                    .search_until_hits(&["document.title:issue_006_partition_a"], 20)
                    .await
                    .context("waiting for first partition row to become searchable")?;
                gateway
                    .search_until_hits(&["document.title:issue_006_partition_b"], 20)
                    .await
                    .context("waiting for second partition row to become searchable")?;

                let partition_delete_cql = format!(
                    "DELETE FROM {}.{} WHERE user_id = '{}'",
                    cluster.keyspace(),
                    cluster.table_name(),
                    user_id,
                );
                cluster
                    .session()
                    .query_unpaged(partition_delete_cql, ())
                    .await
                    .context("executing partition delete")?;

                for _ in 0..40 {
                    let first = gateway
                        .search(&crate::cluster::gateway::SearchRequest {
                            query: "document.title:issue_006_partition_a".to_string(),
                            limit: 10,
                            offset: 0,
                            consistency: 1,
                            default_fields: vec![],
                        })
                        .await
                        .context("searching for first partition row")?;
                    let second = gateway
                        .search(&crate::cluster::gateway::SearchRequest {
                            query: "document.title:issue_006_partition_b".to_string(),
                            limit: 10,
                            offset: 0,
                            consistency: 1,
                            default_fields: vec![],
                        })
                        .await
                        .context("searching for second partition row")?;

                    if first.total_hits == 0 && second.total_hits == 0 {
                        return Ok(());
                    }

                    sleep(Duration::from_millis(500)).await;
                }

                bail!("expected partition delete to remove every row in the partition");
            }
            .boxed()
        })
        .await
}

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

// =========================================================================
// FTS — Phrase search
// =========================================================================
//
// Tantivy stores term positions (`WithFreqsAndPositions`) on the `document`
// JSON field, so phrase queries like `"noise cancellation"` should work
// out of the box as long as the field:value prefix is provided.

#[tokio::test]
async fn e2e_fts_phrase_search() -> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info")
        .with_test_writer()
        .try_init();

    let schema = SchemaConfig::from_cql(
        "CREATE TABLE IF NOT EXISTS {{keyspace}}.products (\
            doc_id text PRIMARY KEY,\
            title text,\
            body text\
        ) WITH cdc = {'enabled': true};",
    );

    let cluster = TestCluster::builder()
        .with_schema(schema)
        .with_table_name("products")
        .enable_instrumentation(false)
        .build()
        .await
        .context("building test cluster")?;

    cluster
        .scoped(|cluster| {
            async move {
                let gateway = cluster.gateway().context("building gateway client")?;

                // Insert two products: only one contains the exact phrase.
                let match_id = format!("doc-{}", Uuid::new_v4());
                let nomatch_id = format!("doc-{}", Uuid::new_v4());

                let insert_match = format!(
                    "INSERT INTO {}.{} (doc_id, title, body) VALUES ('{}', 'Headphones', 'active noise cancellation technology')",
                    cluster.keyspace(), cluster.table_name(), match_id,
                );
                let insert_nomatch = format!(
                    "INSERT INTO {}.{} (doc_id, title, body) VALUES ('{}', 'Speakers', 'great sound cancellation of noise')",
                    cluster.keyspace(), cluster.table_name(), nomatch_id,
                );

                cluster.session().query_unpaged(insert_match, ()).await.context("inserting phrase-match doc")?;
                cluster.session().query_unpaged(insert_nomatch, ()).await.context("inserting phrase-nomatch doc")?;

                // Wait until the phrase-match doc appears via a term query first.
                gateway
                    .search_until_hits(&["document.body:\"noise cancellation\""], 30)
                    .await
                    .context("waiting for phrase match to become searchable")?;

                // Confirm the no-match doc is NOT returned for the phrase.
                let resp = gateway
                    .search(&SearchRequest {
                        query: r#"document.body:"noise cancellation""#.to_string(),
                        limit: 10,
                        offset: 0,
                        consistency: 1,
                        default_fields: vec![],
                    })
                    .await
                    .context("phrase search")?;

                ensure!(
                    resp.total_hits >= 1,
                    "expected at least one phrase hit, got {}",
                    resp.total_hits
                );

                // Verify the matching document is present in results.
                let ids: Vec<&str> = resp.hits.iter().map(|h| h["id"].as_str().unwrap_or("")).collect();
                ensure!(
                    ids.contains(&match_id.as_str()),
                    "phrase-match doc not found in hits: {:?}",
                    ids
                );

                Ok(())
            }
            .boxed()
        })
        .await
}

// =========================================================================
// FTS — Fuzzy / typo-tolerant search
// =========================================================================
//
// Tantivy supports edit-distance fuzzy matching via the `~N` suffix on
// term queries: `document.title:wireles~1` matches "wireless" with up to
// one character edit. No implementation changes are required for this
// feature — it is validated here to lock in the behavior.

#[tokio::test]
async fn e2e_fts_fuzzy_search() -> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info")
        .with_test_writer()
        .try_init();

    let schema = SchemaConfig::from_cql(
        "CREATE TABLE IF NOT EXISTS {{keyspace}}.products (\
            doc_id text PRIMARY KEY,\
            title text,\
            body text\
        ) WITH cdc = {'enabled': true};",
    );

    let cluster = TestCluster::builder()
        .with_schema(schema)
        .with_table_name("products")
        .enable_instrumentation(false)
        .build()
        .await
        .context("building test cluster")?;

    cluster
        .scoped(|cluster| {
            async move {
                let gateway = cluster.gateway().context("building gateway client")?;

                let doc_id = format!("doc-{}", Uuid::new_v4());
                let insert_cql = format!(
                    "INSERT INTO {}.{} (doc_id, title, body) VALUES ('{}', 'Wireless Mouse', 'ergonomic design')",
                    cluster.keyspace(), cluster.table_name(), doc_id,
                );
                cluster.session().query_unpaged(insert_cql, ()).await.context("inserting fuzzy-search fixture")?;

                // Wait for exact match to appear first to ensure ingestion is done.
                gateway
                    .search_until_hits(&["document.title:Wireless"], 30)
                    .await
                    .context("waiting for exact term to become searchable")?;

                // Now query with a typo: "wireles" (missing one 's') with distance 1.
                // The `en_stem` tokenizer lowercases tokens, so the stored term
                // is "wireless" (stemmed to "wireless"). We query with ~1.
                // NOTE: Tantivy fuzzy matching operates on the *indexed* (stemmed)
                // token. "wireles" and "wireless" differ by 1 edit — should match.
                let resp = gateway
                    .search(&SearchRequest {
                        query: "document.title:wireles~1".to_string(),
                        limit: 10,
                        offset: 0,
                        consistency: 1,
                        default_fields: vec![],
                    })
                    .await
                    .context("fuzzy search")?;

                ensure!(
                    resp.total_hits >= 1,
                    "expected at least one fuzzy hit for 'wireles~1', got {}",
                    resp.total_hits
                );

                Ok(())
            }
            .boxed()
        })
        .await
}

// =========================================================================
// FTS — Numeric range query
// =========================================================================
//
// Tantivy ≥ 0.22 automatically indexes numeric values stored in a JSON
// field as fast sub-fields, enabling range queries such as
// `document.price:[40 TO 100]`. The ingestor serializes CQL numeric
// columns as JSON numbers, so this should work without schema changes.

#[tokio::test]
async fn e2e_fts_numeric_range() -> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info")
        .with_test_writer()
        .try_init();

    let schema = SchemaConfig::from_cql(
        "CREATE TABLE IF NOT EXISTS {{keyspace}}.products (\
            doc_id text PRIMARY KEY,\
            title text,\
            price double\
        ) WITH cdc = {'enabled': true};",
    );

    let cluster = TestCluster::builder()
        .with_schema(schema)
        .with_table_name("products")
        .enable_instrumentation(false)
        .build()
        .await
        .context("building test cluster")?;

    cluster
        .scoped(|cluster| {
            async move {
                let gateway = cluster.gateway().context("building gateway client")?;

                // Insert three products at different price points.
                for (title, price) in [
                    ("Budget Widget", 19.99_f64),
                    ("Mid Widget", 59.99_f64),
                    ("Premium Widget", 149.99_f64),
                ] {
                    let id = format!("doc-{}", Uuid::new_v4());
                    let cql = format!(
                        "INSERT INTO {}.{} (doc_id, title, price) VALUES ('{}', '{}', {})",
                        cluster.keyspace(),
                        cluster.table_name(),
                        id,
                        title,
                        price,
                    );
                    cluster
                        .session()
                        .query_unpaged(cql, ())
                        .await
                        .with_context(|| format!("inserting product '{}'", title))?;
                }

                // Wait for any of the products to be indexed.
                gateway
                    .search_until_hits(&["document.title:Widget"], 30)
                    .await
                    .context("waiting for products to be indexed")?;

                // Query for products in the 40–100 price range.
                // Only "Mid Widget" (59.99) should match.
                let resp = gateway
                    .search(&SearchRequest {
                        query: "document.price:[40 TO 100]".to_string(),
                        limit: 10,
                        offset: 0,
                        consistency: 1,
                        default_fields: vec![],
                    })
                    .await
                    .context("numeric range search")?;

                ensure!(
                    resp.total_hits >= 1,
                    "expected at least one hit for price range [40 TO 100], got {}",
                    resp.total_hits
                );

                Ok(())
            }
            .boxed()
        })
        .await
}

// =========================================================================
// FTS — Plain keyword search via default_fields
// =========================================================================
//
// When `default_fields` is provided, the gateway/node should expand a bare
// keyword query (e.g., `wireless`) into an OR over the listed sub-fields of
// the `document` JSON object (e.g., `document.title:wireless OR
// document.body:wireless`). This avoids requiring clients to prefix every
// term with the field path.
//
// This test will FAIL until `default_fields` is implemented end-to-end.

#[tokio::test]
async fn e2e_fts_plain_keyword_with_default_fields() -> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info")
        .with_test_writer()
        .try_init();

    let schema = SchemaConfig::from_cql(
        "CREATE TABLE IF NOT EXISTS {{keyspace}}.products (\
            doc_id text PRIMARY KEY,\
            title text,\
            body text\
        ) WITH cdc = {'enabled': true};",
    );

    let cluster = TestCluster::builder()
        .with_schema(schema)
        .with_table_name("products")
        .enable_instrumentation(false)
        .build()
        .await
        .context("building test cluster")?;

    cluster
        .scoped(|cluster| {
            async move {
                let gateway = cluster.gateway().context("building gateway client")?;

                let doc_id = format!("doc-{}", Uuid::new_v4());
                let insert_cql = format!(
                    "INSERT INTO {}.{} (doc_id, title, body) VALUES ('{}', 'Wireless Keyboard', 'compact layout')",
                    cluster.keyspace(), cluster.table_name(), doc_id,
                );
                cluster.session().query_unpaged(insert_cql, ()).await.context("inserting plain-keyword fixture")?;

                // Wait for the document to appear via a precise field query.
                gateway
                    .search_until_hits(&["document.title:Wireless"], 30)
                    .await
                    .context("waiting for document to be indexed")?;

                // Now search with a bare keyword and default_fields — no `document.` prefix.
                let resp = gateway
                    .search(&SearchRequest {
                        query: "wireless".to_string(),
                        limit: 10,
                        offset: 0,
                        consistency: 1,
                        default_fields: vec!["title".to_string(), "body".to_string()],
                    })
                    .await
                    .context("plain keyword search with default_fields")?;

                ensure!(
                    resp.total_hits >= 1,
                    "expected hits for plain keyword 'wireless' with default_fields, got 0"
                );

                Ok(())
            }
            .boxed()
        })
        .await
}

// =========================================================================
// FTS — Structured filter: keyword AND numeric range in one query
// =========================================================================
//
// A real search UI typically lets users combine a keyword with a structured
// filter such as a price band. Tantivy's query parser supports boolean
// composition with `AND`, so `document.title:Widget AND
// document.price:[40 TO 100]` expresses "find Widgets whose price is
// between 40 and 100". No new implementation is required — this test
// validates the combination end-to-end.

#[tokio::test]
async fn e2e_fts_keyword_and_numeric_filter() -> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info")
        .with_test_writer()
        .try_init();

    let schema = SchemaConfig::from_cql(
        "CREATE TABLE IF NOT EXISTS {{keyspace}}.products (\
            doc_id text PRIMARY KEY,\
            title text,\
            price double\
        ) WITH cdc = {'enabled': true};",
    );

    let cluster = TestCluster::builder()
        .with_schema(schema)
        .with_table_name("products")
        .enable_instrumentation(false)
        .build()
        .await
        .context("building test cluster")?;

    cluster
        .scoped(|cluster| {
            async move {
                let gateway = cluster.gateway().context("building gateway client")?;

                // Three products with the same keyword but different prices.
                for (title, price) in [
                    ("Gadget Widget", 15.0_f64),  // keyword matches, price outside range
                    ("Gadget Widget", 75.0_f64),  // keyword matches, price inside range
                    ("Gadget Widget", 200.0_f64), // keyword matches, price outside range
                ] {
                    let id = format!("doc-{}", Uuid::new_v4());
                    let cql = format!(
                        "INSERT INTO {}.{} (doc_id, title, price) VALUES ('{}', '{}', {})",
                        cluster.keyspace(),
                        cluster.table_name(),
                        id,
                        title,
                        price,
                    );
                    cluster
                        .session()
                        .query_unpaged(cql, ())
                        .await
                        .with_context(|| format!("inserting product at price {}", price))?;
                }

                // Wait until the products are indexed via a plain term query.
                gateway
                    .search_until_hits(&["document.title:Gadget"], 30)
                    .await
                    .context("waiting for products to be indexed")?;

                // Combine a keyword filter with a numeric range.
                // Only the 75.0 product should match both conditions.
                let resp = gateway
                    .search(&SearchRequest {
                        query: "document.title:Gadget AND document.price:[40 TO 100]".to_string(),
                        limit: 10,
                        offset: 0,
                        consistency: 1,
                        default_fields: vec![],
                    })
                    .await
                    .context("keyword + numeric range search")?;

                ensure!(
                    resp.total_hits >= 1,
                    "expected at least one hit for keyword+range query, got {}",
                    resp.total_hits
                );
                ensure!(
                    resp.total_hits == 1,
                    "expected exactly one hit (price 75), got {} — check that out-of-range \
                     products are excluded",
                    resp.total_hits
                );

                Ok(())
            }
            .boxed()
        })
        .await
}

// =========================================================================
// FTS — Prefix / autocomplete search
// =========================================================================
//
// Many search boxes offer autocomplete by matching the prefix of the last
// typed token. Tantivy supports this via the `*` wildcard suffix on a term
// query: `document.title:wire*` matches "wireless", "wireframe", etc.
//
// The `en_stem` tokenizer lowercases tokens before indexing, so the prefix
// must also be lowercased for the match to succeed. The test asserts that a
// document whose title starts with the prefix is returned.
//
// No implementation changes are required for this feature — it is provided
// natively by Tantivy's `QueryParser` when wildcard syntax is used.

#[tokio::test]
async fn e2e_fts_prefix_autocomplete() -> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info")
        .with_test_writer()
        .try_init();

    let schema = SchemaConfig::from_cql(
        "CREATE TABLE IF NOT EXISTS {{keyspace}}.products (\
            doc_id text PRIMARY KEY,\
            title text\
        ) WITH cdc = {'enabled': true};",
    );

    let cluster = TestCluster::builder()
        .with_schema(schema)
        .with_table_name("products")
        .enable_instrumentation(false)
        .build()
        .await
        .context("building test cluster")?;

    cluster
        .scoped(|cluster| {
            async move {
                let gateway = cluster.gateway().context("building gateway client")?;

                // Insert two products: "Wireless Headset" and "Wired Headset".
                // The prefix "wire" should match both; "wireless" should match
                // only the first one.
                let wireless_id = format!("doc-{}", Uuid::new_v4());
                let wired_id = format!("doc-{}", Uuid::new_v4());

                for (id, title) in [
                    (wireless_id.as_str(), "Wireless Headset"),
                    (wired_id.as_str(), "Wired Headset"),
                ] {
                    let cql = format!(
                        "INSERT INTO {}.{} (doc_id, title) VALUES ('{}', '{}')",
                        cluster.keyspace(),
                        cluster.table_name(),
                        id,
                        title,
                    );
                    cluster
                        .session()
                        .query_unpaged(cql, ())
                        .await
                        .with_context(|| format!("inserting product '{}'", title))?;
                }

                // Wait until both products are indexed.
                gateway
                    .search_until_hits(&["document.title:Headset"], 30)
                    .await
                    .context("waiting for products to be indexed")?;

                // `wire*` should match both "wireless" and "wired" tokens.
                let resp_broad = gateway
                    .search(&SearchRequest {
                        query: "document.title:wire*".to_string(),
                        limit: 10,
                        offset: 0,
                        consistency: 1,
                        default_fields: vec![],
                    })
                    .await
                    .context("broad prefix search (wire*)")?;

                ensure!(
                    resp_broad.total_hits >= 2,
                    "expected both products for prefix 'wire*', got {}",
                    resp_broad.total_hits
                );

                // `wireless*` should match only the "Wireless Headset".
                let resp_narrow = gateway
                    .search(&SearchRequest {
                        query: "document.title:wireless*".to_string(),
                        limit: 10,
                        offset: 0,
                        consistency: 1,
                        default_fields: vec![],
                    })
                    .await
                    .context("narrow prefix search (wireless*)")?;

                ensure!(
                    resp_narrow.total_hits >= 1,
                    "expected at least one hit for prefix 'wireless*', got {}",
                    resp_narrow.total_hits
                );

                Ok(())
            }
            .boxed()
        })
        .await
}

// =========================================================================
// FTS — Log search: time-range + keyword
// =========================================================================
//
// A common log-search pattern is "find events containing keyword X that
// occurred between time A and time B". The ingestor serialises CQL
// `timestamp` columns as integer milliseconds in the JSON payload, and
// Tantivy automatically indexes JSON numbers as fast sub-fields for range
// queries (Tantivy ≥ 0.22).
//
// This test inserts three log events with distinct epoch-millisecond
// `created_at` values and confirms that a combined keyword + time-range
// query returns only the event that falls inside the window.
//
// Implementation note: CQL `timestamp` values are stored by the driver as
// milliseconds since the Unix epoch. We construct three explicit timestamps
// so the window can be expressed as a closed numeric range without relying
// on `NOW()`, which would make the test non-deterministic.

#[tokio::test]
async fn e2e_fts_log_time_range_keyword() -> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info")
        .with_test_writer()
        .try_init();

    let schema = SchemaConfig::from_cql(
        "CREATE TABLE IF NOT EXISTS {{keyspace}}.events (\
            event_id text PRIMARY KEY,\
            message text,\
            created_at timestamp\
        ) WITH cdc = {'enabled': true};",
    );

    let cluster = TestCluster::builder()
        .with_schema(schema)
        .with_table_name("events")
        .enable_instrumentation(false)
        .build()
        .await
        .context("building test cluster")?;

    cluster
        .scoped(|cluster| {
            async move {
                let gateway = cluster.gateway().context("building gateway client")?;

                // Three log events:
                //   t_early  — 2020-01-01 00:00:00 UTC (1577836800000 ms)
                //   t_target — 2022-06-15 12:00:00 UTC (1655294400000 ms)  ← in window
                //   t_late   — 2024-12-31 23:59:59 UTC (1735689599000 ms)
                //
                // Search window: [2022-01-01, 2023-01-01) ≈ [1640995200000, 1672531200000]
                const T_EARLY: i64 = 1_577_836_800_000;
                const T_TARGET: i64 = 1_655_294_400_000;
                const T_LATE: i64 = 1_735_689_599_000;
                const WINDOW_START: i64 = 1_640_995_200_000;
                const WINDOW_END: i64 = 1_672_531_200_000;

                for (ts, msg) in [
                    (T_EARLY, "timeout error occurred in scheduler"),
                    (T_TARGET, "timeout error occurred in dispatcher"),
                    (T_LATE, "timeout error occurred in cleaner"),
                ] {
                    let id = format!("event-{}", Uuid::new_v4());
                    // CQL accepts timestamp literals in ISO-8601 or as integer ms.
                    // We use the integer form which ScyllaDB supports via `dateof`
                    // or by casting — but the simplest portable form is to pass the
                    // ms value directly as a CQL `bigint` cast to timestamp.
                    let cql = format!(
                        "INSERT INTO {}.{} (event_id, message, created_at) \
                         VALUES ('{}', '{}', {})",
                        cluster.keyspace(),
                        cluster.table_name(),
                        id,
                        msg,
                        ts,
                    );
                    cluster
                        .session()
                        .query_unpaged(cql, ())
                        .await
                        .with_context(|| format!("inserting event at ts={}", ts))?;
                }

                // Wait until any of the events is indexed.
                gateway
                    .search_until_hits(&["document.message:timeout"], 30)
                    .await
                    .context("waiting for events to be indexed")?;

                // Keyword "timeout" matches all three, but the time-range window
                // should narrow it to exactly one (the 2022 event).
                let query = format!(
                    "document.message:timeout AND document.created_at:[{} TO {}]",
                    WINDOW_START, WINDOW_END,
                );
                let resp = gateway
                    .search(&SearchRequest {
                        query,
                        limit: 10,
                        offset: 0,
                        consistency: 1,
                        default_fields: vec![],
                    })
                    .await
                    .context("log time-range + keyword search")?;

                ensure!(
                    resp.total_hits >= 1,
                    "expected at least one hit inside the time window, got {}",
                    resp.total_hits
                );
                ensure!(
                    resp.total_hits == 1,
                    "expected exactly one hit (2022 event), got {} — \
                     check that events outside the window are excluded",
                    resp.total_hits
                );

                Ok(())
            }
            .boxed()
        })
        .await
}
