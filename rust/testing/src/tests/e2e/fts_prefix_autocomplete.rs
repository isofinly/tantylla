use crate::cluster::gateway::SearchRequest;
use crate::cluster::{SchemaConfig, TestCluster};
use anyhow::{Context, Result, ensure};
use futures::FutureExt;
use tokio::time::{Duration, sleep};
use uuid::Uuid;

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

                // Wait until BOTH products are indexed. The broad prefix
                // assertion (`wire*` → 2 hits) is only valid when both
                // docs have been committed; waiting for just one "Headset"
                // creates a race where only one doc may be indexed.
                for _ in 0..60 {
                    let resp = gateway
                        .search(&SearchRequest {
                            query: "document.title:Headset".to_string(),
                            limit: 10,
                            offset: 0,
                            consistency: 1,
                            default_fields: vec![],
                            facet_fields: vec![],
                            boost_fields: vec![],
                            group_by_partition: false,
                        })
                        .await
                        .context("polling for both Headset products")?;

                    if resp.total_hits >= 2 {
                        break;
                    }
                    sleep(Duration::from_millis(500)).await;
                }

                // `wire*` should match both "wireless" and "wired" tokens.
                let resp_broad = gateway
                    .search(&SearchRequest {
                        query: "document.title:wire*".to_string(),
                        limit: 10,
                        offset: 0,
                        consistency: 1,
                        default_fields: vec![],
                        facet_fields: vec![],
                        boost_fields: vec![],
                        group_by_partition: false,
                    })
                    .await
                    .context("broad prefix search (wire*)")?;

                ensure!(
                    resp_broad.total_hits == 2,
                    "expected exactly 2 products for prefix 'wire*', got {}",
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
                        facet_fields: vec![],
                        boost_fields: vec![],
                        group_by_partition: false,
                    })
                    .await
                    .context("narrow prefix search (wireless*)")?;

                ensure!(
                    resp_narrow.total_hits == 1,
                    "expected exactly one hit for prefix 'wireless*', got {} — \
                     'wired' should not match 'wireless*'",
                    resp_narrow.total_hits
                );

                Ok(())
            }
            .boxed()
        })
        .await
}
