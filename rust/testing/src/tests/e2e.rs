use crate::cluster::TestCluster;
use crate::trace::TraceSequence;
use anyhow::{Context, Result, bail, ensure};
use futures::FutureExt;
use tantylla_common::tracing::events::{TestEvent, TestEventSource, TracePayload};
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
                gateway
                    .search_until_hits(&["document.title:issue_005_delete"], 20)
                    .await
                    .context("waiting for delete candidate to become searchable")?;

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
                        })
                        .await
                        .context("searching for removed order")?;
                    let kept = gateway
                        .search(&crate::cluster::gateway::SearchRequest {
                            query: "document.title:issue_005_keep".to_string(),
                            limit: 10,
                            offset: 0,
                            consistency: 1,
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
                    .context("waiting for partition fixture rows to become searchable")?;

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
                        })
                        .await
                        .context("searching for first partition row")?;
                    let second = gateway
                        .search(&crate::cluster::gateway::SearchRequest {
                            query: "document.title:issue_006_partition_b".to_string(),
                            limit: 10,
                            offset: 0,
                            consistency: 1,
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
