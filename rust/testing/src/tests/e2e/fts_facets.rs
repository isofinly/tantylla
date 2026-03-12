use crate::cluster::{SchemaConfig, TestCluster};
use crate::cluster::gateway::SearchRequest;
use anyhow::{Context, Result, bail};
use futures::FutureExt;
use tokio::time::Duration;
use uuid::Uuid;

#[tokio::test]
async fn e2e_fts_facets() -> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info")
        .with_test_writer()
        .try_init();

    let schema = SchemaConfig::from_cql(
        "CREATE TABLE IF NOT EXISTS {{keyspace}}.products (\
            doc_id text PRIMARY KEY,\
            title text,\
            category text\
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

                // Insert three products: two in "electronics", one in "books".
                for (title, category) in [
                    ("Wireless Headphones", "electronics"),
                    ("Smart Speaker", "electronics"),
                    ("Programming Book", "books"),
                ] {
                    let id = format!("doc-{}", Uuid::new_v4());
                    let cql = format!(
                        "INSERT INTO {}.{} (doc_id, title, category) VALUES ('{}', '{}', '{}')",
                        cluster.keyspace(),
                        cluster.table_name(),
                        id,
                        title,
                        category,
                    );
                    cluster
                        .session()
                        .query_unpaged(cql, ())
                        .await
                        .with_context(|| {
                            format!("inserting product '{}' in '{}'", title, category)
                        })?;
                }

                // Wait until at least the "books" category document is indexed
                // before we begin polling for facet results.
                gateway
                    .search_until_hits(&["document.category:books"], 30)
                    .await
                    .context("waiting for 'books' product to be indexed")?;

                // Poll until all three documents are indexed AND the facet
                // counts match the expected distribution.  The two passes
                // (TopDocs + DocSetCollector) are both executed inside the
                // node on every search request, so we simply retry until the
                // counts converge.
                let query = "document.category:electronics OR document.category:books".to_string();

                for attempt in 0..40_u32 {
                    let resp = gateway
                        .search(&SearchRequest {
                            query: query.clone(),
                            limit: 10,
                            offset: 0,
                            consistency: 1,
                            default_fields: vec![],
                            facet_fields: vec!["category".to_string()],
                            boost_fields: vec![],
                            group_by_partition: false,
                        })
                        .await
                        .context("facet search")?;

                    tracing::debug!(attempt, total_hits = resp.total_hits, "facet search poll");

                    // Locate the "category" facet result from the response.
                    let facet = resp.facets.iter().find(|f| f.field == "category");

                    if let Some(facet) = facet {
                        let electronics = facet
                            .buckets
                            .iter()
                            .find(|b| b.value.to_lowercase() == "electronics")
                            .map_or(0, |b| b.count);
                        let books = facet
                            .buckets
                            .iter()
                            .find(|b| b.value.to_lowercase() == "books")
                            .map_or(0, |b| b.count);

                        if electronics == 2 && books == 1 {
                            return Ok(());
                        }

                        tracing::debug!(
                            attempt,
                            electronics,
                            books,
                            "facet counts not yet converged"
                        );
                    }

                    tokio::time::sleep(Duration::from_millis(500)).await;
                }

                bail!(
                    "facet aggregation did not converge: expected category buckets \
                     electronics=2 and books=1 after all documents were indexed"
                );
            }
            .boxed()
        })
        .await
}