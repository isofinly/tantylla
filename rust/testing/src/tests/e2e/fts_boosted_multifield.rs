use crate::cluster::{SchemaConfig, TestCluster};
use crate::cluster::gateway::{BoostField, SearchRequest};
use anyhow::{Context, Result, ensure};
use futures::FutureExt;
use uuid::Uuid;

#[tokio::test]
async fn e2e_fts_boosted_multifield() -> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info")
        .with_test_writer()
        .try_init();

    let schema = SchemaConfig::from_cql(
        "CREATE TABLE IF NOT EXISTS {{keyspace}}.articles (\
            doc_id text PRIMARY KEY,\
            title text,\
            body text\
        ) WITH cdc = {'enabled': true};",
    );

    let cluster = TestCluster::builder()
        .with_schema(schema)
        .with_table_name("articles")
        .enable_instrumentation(false)
        .build()
        .await
        .context("building test cluster")?;

    cluster
        .scoped(|cluster| {
            async move {
                let gateway = cluster.gateway().context("building gateway client")?;

                // Doc A: keyword "rocket" in title — the high-boost field.
                // Doc B: keyword "rocket" in body only — the low-boost field.
                let doc_a_id = format!("doc-{}", Uuid::new_v4());
                let doc_b_id = format!("doc-{}", Uuid::new_v4());

                let insert_a = format!(
                    "INSERT INTO {}.{} (doc_id, title, body) \
                     VALUES ('{}', 'Rocket Engine Design', 'general propulsion overview')",
                    cluster.keyspace(),
                    cluster.table_name(),
                    doc_a_id,
                );
                let insert_b = format!(
                    "INSERT INTO {}.{} (doc_id, title, body) \
                     VALUES ('{}', 'General Propulsion Overview', 'rocket engine design details')",
                    cluster.keyspace(),
                    cluster.table_name(),
                    doc_b_id,
                );

                cluster
                    .session()
                    .query_unpaged(insert_a, ())
                    .await
                    .context("inserting title-match doc (doc A)")?;
                cluster
                    .session()
                    .query_unpaged(insert_b, ())
                    .await
                    .context("inserting body-match doc (doc B)")?;

                // Wait for both documents to be indexed before asserting ranking.
                gateway
                    .search_until_hits(&["document.title:Rocket"], 30)
                    .await
                    .context("waiting for doc A to be indexed")?;
                gateway
                    .search_until_hits(&["document.title:Propulsion"], 30)
                    .await
                    .context("waiting for doc B to be indexed")?;

                // Query with boost_fields so title matches outrank body matches.
                // The bare query "rocket" expands to:
                //   (document.title:rocket^5 OR document.body:rocket^1)
                let resp = gateway
                    .search(&SearchRequest {
                        query: "rocket".to_string(),
                        limit: 10,
                        offset: 0,
                        consistency: 1,
                        default_fields: vec![],
                        facet_fields: vec![],
                        boost_fields: vec![
                            BoostField {
                                field: "title".to_string(),
                                boost: 5.0,
                            },
                            BoostField {
                                field: "body".to_string(),
                                boost: 1.0,
                            },
                        ],
                        group_by_partition: false,
                    })
                    .await
                    .context("boosted multi-field search")?;

                ensure!(
                    resp.total_hits >= 2,
                    "expected at least 2 hits for 'rocket', got {}",
                    resp.total_hits
                );

                // Doc A (title match, boost 5) must outrank Doc B (body match, boost 1).
                let first_id = resp.hits[0]["id"].as_str().unwrap_or("");
                ensure!(
                    first_id == doc_a_id.as_str(),
                    "expected title-match doc (doc A) to rank first, but got id={} \
                     — boost_fields may not be expanding correctly",
                    first_id
                );

                Ok(())
            }
            .boxed()
        })
        .await
}