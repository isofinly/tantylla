use crate::cluster::gateway::SearchRequest;
use crate::cluster::{SchemaConfig, TestCluster};
use anyhow::{Context, Result, ensure};
use futures::FutureExt;
use uuid::Uuid;

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
                        facet_fields: vec![],
                        boost_fields: vec![],
                        group_by_partition: false,
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
