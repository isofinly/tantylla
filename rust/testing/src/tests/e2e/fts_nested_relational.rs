use crate::cluster::gateway::SearchRequest;
use crate::cluster::{SchemaConfig, TestCluster};
use anyhow::{Context, Result, ensure};
use futures::FutureExt;
use tokio::time::{Duration, sleep};
use uuid::Uuid;

#[tokio::test]
async fn e2e_fts_nested_relational_group_by_partition() -> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info")
        .with_test_writer()
        .try_init();

    let schema = SchemaConfig::from_cql(
        "CREATE TABLE IF NOT EXISTS {{keyspace}}.orders (\
            user_id text,\
            order_id int,\
            title text,\
            PRIMARY KEY (user_id, order_id)\
        ) WITH cdc = {'enabled': true};",
    );

    let cluster = TestCluster::builder()
        .with_schema(schema)
        .with_table_name("orders")
        .enable_instrumentation(false)
        .build()
        .await
        .context("building test cluster")?;

    cluster
        .scoped(|cluster| {
            async move {
                let gateway = cluster.gateway().context("building gateway client")?;

                let user_a = format!("user-a-{}", Uuid::new_v4());
                let user_b = format!("user-b-{}", Uuid::new_v4());

                // Insert two Widget orders for each user (4 matching rows total).
                for (user_id, order_id, title) in [
                    (user_a.as_str(), 1, "Widget Alpha"),
                    (user_a.as_str(), 2, "Widget Alpha"),
                    (user_b.as_str(), 1, "Widget Beta"),
                    (user_b.as_str(), 2, "Widget Beta"),
                ] {
                    let cql = format!(
                        "INSERT INTO {}.{} (user_id, order_id, title) \
                         VALUES ('{}', {}, '{}')",
                        cluster.keyspace(),
                        cluster.table_name(),
                        user_id,
                        order_id,
                        title,
                    );
                    cluster
                        .session()
                        .query_unpaged(cql, ())
                        .await
                        .with_context(|| {
                            format!("inserting order for {} order_id={}", user_id, order_id)
                        })?;
                }

                // Wait for at least one Widget row to be indexed.
                gateway
                    .search_until_hits(&["document.title:Widget"], 30)
                    .await
                    .context("waiting for Widget orders to be indexed")?;

                // Without grouping: expect raw document count >= 4.
                // We poll because not all 4 rows may be committed yet.
                let mut raw_hits = 0u64;
                for _ in 0..40 {
                    let resp = gateway
                        .search(&SearchRequest {
                            query: "document.title:Widget".to_string(),
                            limit: 25,
                            offset: 0,
                            consistency: 1,
                            default_fields: vec![],
                            facet_fields: vec![],
                            boost_fields: vec![],
                            group_by_partition: false,
                        })
                        .await
                        .context("raw (ungrouped) Widget search")?;

                    if resp.total_hits >= 4 {
                        raw_hits = resp.total_hits;
                        break;
                    }
                    sleep(Duration::from_millis(500)).await;
                }
                ensure!(
                    raw_hits >= 4,
                    "expected >= 4 raw hits (one per Widget order row), got {}",
                    raw_hits
                );

                // With grouping: expect exactly 2 hits (one per user partition).
                let grouped_resp = gateway
                    .search(&SearchRequest {
                        query: "document.title:Widget".to_string(),
                        limit: 25,
                        offset: 0,
                        consistency: 1,
                        default_fields: vec![],
                        facet_fields: vec![],
                        boost_fields: vec![],
                        group_by_partition: true,
                    })
                    .await
                    .context("grouped (group_by_partition) Widget search")?;

                ensure!(
                    grouped_resp.hits.len() == 2,
                    "expected exactly 2 grouped hits (one per user), got {} \
                     — group_by_partition deduplication may not be working",
                    grouped_resp.hits.len()
                );

                // Verify the two hits represent distinct partitions (user_a
                // and user_b) rather than two rows from the same partition.
                // The `partition_key` stored field holds the ScyllaDB
                // partition key for each document.
                let partition_keys: Vec<&str> = grouped_resp
                    .hits
                    .iter()
                    .map(|h| h["partition_key"].as_str().unwrap_or(""))
                    .collect();

                ensure!(
                    partition_keys.contains(&user_a.as_str()),
                    "user_a partition missing from grouped hits: {:?}",
                    partition_keys
                );
                ensure!(
                    partition_keys.contains(&user_b.as_str()),
                    "user_b partition missing from grouped hits: {:?}",
                    partition_keys
                );

                Ok(())
            }
            .boxed()
        })
        .await
}
