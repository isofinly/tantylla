use crate::cluster::{SchemaConfig, TestCluster};
use crate::cluster::gateway::SearchRequest;
use anyhow::{Context, Result, ensure};
use futures::FutureExt;
use uuid::Uuid;

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
                        facet_fields: vec![],
                        boost_fields: vec![],
                        group_by_partition: false,
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