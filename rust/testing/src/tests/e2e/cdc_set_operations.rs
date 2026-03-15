use crate::cluster::SchemaConfig;
use crate::cluster::TestCluster;
use crate::trace::TraceSequence;
use anyhow::{Context, Result, bail};
use futures::FutureExt;
use tantylla_common::tracing::events::{TestEvent, TestEventSource};
use tokio::time::{Duration, sleep};
use uuid::Uuid;

#[tokio::test]
async fn e2e_cdc_set_overwrite_indexes_new_label() -> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info")
        .with_test_writer()
        .try_init();

    let schema = SchemaConfig::from_cql(
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

    let schema = SchemaConfig::from_cql(
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
                            facet_fields: vec![],
                            boost_fields: vec![],
                            group_by_partition: false,
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

    let schema = SchemaConfig::from_cql(
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
                            facet_fields: vec![],
                            boost_fields: vec![],
                            group_by_partition: false,
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
