use crate::cluster::{SchemaConfig, TestCluster};
use crate::cluster::gateway::SearchRequest;
use anyhow::{Context, Result, ensure};
use futures::FutureExt;
use tokio::time::{Duration, sleep};
use uuid::Uuid;

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

                // Wait for ALL three products to be indexed before running
                // the range query. Asserting `== 1` is only meaningful when
                // the out-of-range products have already been committed;
                // otherwise a single in-range hit could pass vacuously.
                for _ in 0..60 {
                    let resp = gateway
                        .search(&SearchRequest {
                            query: "document.title:Widget".to_string(),
                            limit: 10,
                            offset: 0,
                            consistency: 1,
                            default_fields: vec![],
                            facet_fields: vec![],
                            boost_fields: vec![],
                            group_by_partition: false,
                        })
                        .await
                        .context("polling for all Widget products")?;

                    if resp.total_hits >= 3 {
                        break;
                    }
                    sleep(Duration::from_millis(500)).await;
                }

                // Query for products in the 40–100 price range.
                // Only "Mid Widget" (59.99) should match.
                let resp = gateway
                    .search(&SearchRequest {
                        query: "document.price:[40 TO 100]".to_string(),
                        limit: 10,
                        offset: 0,
                        consistency: 1,
                        default_fields: vec![],
                        facet_fields: vec![],
                        boost_fields: vec![],
                        group_by_partition: false,
                    })
                    .await
                    .context("numeric range search")?;

                ensure!(
                    resp.total_hits == 1,
                    "expected exactly one hit for price range [40 TO 100] (Mid Widget), got {}",
                    resp.total_hits
                );

                Ok(())
            }
            .boxed()
        })
        .await
}