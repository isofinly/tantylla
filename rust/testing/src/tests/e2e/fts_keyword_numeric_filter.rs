use crate::cluster::gateway::SearchRequest;
use crate::cluster::{SchemaConfig, TestCluster};
use anyhow::{Context, Result, ensure};
use futures::FutureExt;
use tokio::time::{Duration, sleep};
use uuid::Uuid;

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

                // Wait until ALL three products are indexed so the filter
                // assertion is meaningful. With only one doc indexed, an
                // `== 1` check passes vacuously.
                for _ in 0..60 {
                    let resp = gateway
                        .search(&SearchRequest {
                            query: "document.title:Gadget".to_string(),
                            limit: 10,
                            offset: 0,
                            consistency: 1,
                            default_fields: vec![],
                            facet_fields: vec![],
                            boost_fields: vec![],
                            group_by_partition: false,
                        })
                        .await
                        .context("polling for all Gadget products")?;

                    if resp.total_hits >= 3 {
                        break;
                    }
                    sleep(Duration::from_millis(500)).await;
                }

                // Combine a keyword filter with a numeric range.
                // Only the 75.0 product should match both conditions.
                let resp = gateway
                    .search(&SearchRequest {
                        query: "document.title:Gadget AND document.price:[40 TO 100]".to_string(),
                        limit: 10,
                        offset: 0,
                        consistency: 1,
                        default_fields: vec![],
                        facet_fields: vec![],
                        boost_fields: vec![],
                        group_by_partition: false,
                    })
                    .await
                    .context("keyword + numeric range search")?;

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
