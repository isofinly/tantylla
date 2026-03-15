use crate::cluster::gateway::SearchRequest;
use crate::cluster::{SchemaConfig, TestCluster};
use anyhow::{Context, Result, ensure};
use futures::FutureExt;
use uuid::Uuid;

#[tokio::test]
async fn e2e_fts_phrase_search() -> Result<()> {
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

                // Insert two products: only one contains the exact phrase.
                let match_id = format!("doc-{}", Uuid::new_v4());
                let nomatch_id = format!("doc-{}", Uuid::new_v4());

                let insert_match = format!(
                    "INSERT INTO {}.{} (doc_id, title, body) VALUES ('{}', 'Headphones', 'active noise cancellation technology')",
                    cluster.keyspace(), cluster.table_name(), match_id,
                );
                let insert_nomatch = format!(
                    "INSERT INTO {}.{} (doc_id, title, body) VALUES ('{}', 'Speakers', 'great sound cancellation of noise')",
                    cluster.keyspace(), cluster.table_name(), nomatch_id,
                );

                cluster.session().query_unpaged(insert_match, ()).await.context("inserting phrase-match doc")?;
                cluster.session().query_unpaged(insert_nomatch, ()).await.context("inserting phrase-nomatch doc")?;

                // Wait for BOTH documents to be indexed before running the
                // phrase query. Waiting only for the phrase-match doc would
                // let the negative assertion pass vacuously if the no-match
                // doc hasn't been committed yet.
                gateway
                    .search_until_hits(&["document.title:Headphones"], 30)
                    .await
                    .context("waiting for phrase-match doc to be indexed")?;
                gateway
                    .search_until_hits(&["document.title:Speakers"], 30)
                    .await
                    .context("waiting for phrase-nomatch doc to be indexed")?;

                // Confirm the phrase query returns only the matching doc.
                let resp = gateway
                    .search(&SearchRequest {
                        query: r#"document.body:"noise cancellation""#.to_string(),
                        limit: 10,
                        offset: 0,
                        consistency: 1,
                        default_fields: vec![],
                        facet_fields: vec![],
                        boost_fields: vec![],
                        group_by_partition: false,
                    })
                    .await
                    .context("phrase search")?;

                ensure!(
                    resp.total_hits == 1,
                    "expected exactly one phrase hit, got {} — \
                     phrase ordering may not be enforced",
                    resp.total_hits
                );

                // Verify the matching document is present in results.
                let ids: Vec<&str> = resp.hits.iter().map(|h| h["id"].as_str().unwrap_or("")).collect();
                ensure!(
                    ids.contains(&match_id.as_str()),
                    "phrase-match doc not found in hits: {:?}",
                    ids
                );

                // Verify the no-match document is NOT returned.
                // "cancellation of noise" contains the same words but not as
                // a contiguous phrase, so it must be excluded.
                ensure!(
                    !ids.contains(&nomatch_id.as_str()),
                    "phrase-nomatch doc incorrectly returned in hits: {:?}",
                    ids
                );

                Ok(())
            }
            .boxed()
        })
        .await
}
