use crate::cluster::TestCluster;
use crate::cluster::SchemaConfig;
use crate::trace::TraceSequence;
use anyhow::{Context, Result, bail};
use futures::FutureExt;
use tantylla_common::tracing::events::{TestEvent, TestEventSource};
use tokio::time::{Duration, sleep};
use uuid::Uuid;

#[tokio::test]
async fn e2e_row_delete_removes_single_clustering_row() -> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info")
        .with_test_writer()
        .try_init();

    let schema = SchemaConfig::from_cql(
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
                            facet_fields: vec![],
                            boost_fields: vec![],
                            group_by_partition: false,
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
                            facet_fields: vec![],
                            boost_fields: vec![],
                            group_by_partition: false,
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

    let schema = SchemaConfig::from_cql(
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
                            facet_fields: vec![],
                            boost_fields: vec![],
                            group_by_partition: false,
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

    let schema = SchemaConfig::from_cql(
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
                            facet_fields: vec![],
                            boost_fields: vec![],
                            group_by_partition: false,
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

    let schema = SchemaConfig::from_cql(
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
                            facet_fields: vec![],
                            boost_fields: vec![],
                            group_by_partition: false,
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
                            facet_fields: vec![],
                            boost_fields: vec![],
                            group_by_partition: false,
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