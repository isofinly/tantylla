use crate::cluster::gateway::SearchRequest;
use crate::cluster::{SchemaConfig, TestCluster};
use anyhow::{Context, Result, ensure};
use futures::FutureExt;
use uuid::Uuid;

#[tokio::test]
async fn e2e_fts_negative_no_false_positives() -> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info")
        .with_test_writer()
        .try_init();

    let schema = SchemaConfig::from_cql(
        "CREATE TABLE IF NOT EXISTS {{keyspace}}.items (\
            doc_id text PRIMARY KEY,\
            title text,\
            body text,\
            price double\
        ) WITH cdc = {'enabled': true};",
    );

    let cluster = TestCluster::builder()
        .with_schema(schema)
        .with_table_name("items")
        .enable_instrumentation(false)
        .build()
        .await
        .context("building test cluster")?;

    cluster
        .scoped(|cluster| {
            async move {
                let gateway = cluster.gateway().context("building gateway client")?;

                // ── Fixture: one document used by all three sub-cases ─────────
                let doc_id = format!("doc-{}", Uuid::new_v4());
                let insert_cql = format!(
                    "INSERT INTO {}.{} (doc_id, title, body, price) \
                     VALUES ('{}', 'Bluetooth Speaker', 'noise cancellation module', 250.0)",
                    cluster.keyspace(),
                    cluster.table_name(),
                    doc_id,
                );
                cluster
                    .session()
                    .query_unpaged(insert_cql, ())
                    .await
                    .context("inserting negative-test fixture")?;

                // Wait for the document to be indexed before running any
                // negative queries.
                gateway
                    .search_until_hits(&["document.title:Bluetooth"], 30)
                    .await
                    .context("waiting for fixture to be indexed")?;

                // ── Sub-case 1: phrase word-order mismatch ─────────────────
                //
                // "cancellation noise" has the words in reverse order; the
                // index stores positions for "noise cancellation", so this
                // phrase must return zero hits.
                let resp_phrase = gateway
                    .search(&SearchRequest {
                        query: r#"document.body:"cancellation noise""#.to_string(),
                        limit: 10,
                        offset: 0,
                        consistency: 1,
                        default_fields: vec![],
                        facet_fields: vec![],
                        boost_fields: vec![],
                        group_by_partition: false,
                    })
                    .await
                    .context("phrase word-order mismatch search")?;

                ensure!(
                    resp_phrase.total_hits == 0,
                    "reversed phrase 'cancellation noise' should return 0 hits, got {}",
                    resp_phrase.total_hits
                );

                // ── Sub-case 2: fuzzy edit distance exceeded ───────────────
                //
                // "blutoth" differs from "bluetooth" by 2 edits; a ~1 query
                // must not match it.
                let resp_fuzzy = gateway
                    .search(&SearchRequest {
                        query: "document.title:blutoth~1".to_string(),
                        limit: 10,
                        offset: 0,
                        consistency: 1,
                        default_fields: vec![],
                        facet_fields: vec![],
                        boost_fields: vec![],
                        group_by_partition: false,
                    })
                    .await
                    .context("fuzzy over-distance search")?;

                ensure!(
                    resp_fuzzy.total_hits == 0,
                    "fuzzy query 'blutoth~1' (distance 2 from 'bluetooth') should return 0 hits, got {}",
                    resp_fuzzy.total_hits
                );

                // ── Sub-case 3: numeric range exclusion ───────────────────
                //
                // The fixture has price 250.0; a range of [10 TO 100] must
                // return zero hits.
                let resp_range = gateway
                    .search(&SearchRequest {
                        query: "document.price:[10 TO 100]".to_string(),
                        limit: 10,
                        offset: 0,
                        consistency: 1,
                        default_fields: vec![],
                        facet_fields: vec![],
                        boost_fields: vec![],
                        group_by_partition: false,
                    })
                    .await
                    .context("out-of-range numeric search")?;

                ensure!(
                    resp_range.total_hits == 0,
                    "price range [10 TO 100] should return 0 hits (fixture is 250.0), got {}",
                    resp_range.total_hits
                );

                Ok(())
            }
            .boxed()
        })
        .await
}
