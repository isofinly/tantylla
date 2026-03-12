use crate::cluster::{SchemaConfig, TestCluster};
use crate::cluster::gateway::SearchRequest;
use anyhow::{Context, Result, ensure};
use futures::FutureExt;
use tokio::time::{Duration, sleep};
use uuid::Uuid;

#[tokio::test]
async fn e2e_fts_log_time_range_keyword() -> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info")
        .with_test_writer()
        .try_init();

    let schema = SchemaConfig::from_cql(
        "CREATE TABLE IF NOT EXISTS {{keyspace}}.events (\
            event_id text PRIMARY KEY,\
            message text,\
            created_at timestamp\
        ) WITH cdc = {'enabled': true};",
    );

    let cluster = TestCluster::builder()
        .with_schema(schema)
        .with_table_name("events")
        .enable_instrumentation(false)
        .build()
        .await
        .context("building test cluster")?;

    cluster
        .scoped(|cluster| {
            async move {
                let gateway = cluster.gateway().context("building gateway client")?;

                // Three log events:
                //   t_early  — 2020-01-01 00:00:00 UTC (1577836800000 ms)
                //   t_target — 2022-06-15 12:00:00 UTC (1655294400000 ms)  ← in window
                //   t_late   — 2024-12-31 23:59:59 UTC (1735689599000 ms)
                //
                // Search window: [2022-01-01, 2023-01-01) ≈ [1640995200000, 1672531200000]
                const T_EARLY: i64 = 1_577_836_800_000;
                const T_TARGET: i64 = 1_655_294_400_000;
                const T_LATE: i64 = 1_735_689_599_000;
                const WINDOW_START: i64 = 1_640_995_200_000;
                const WINDOW_END: i64 = 1_672_531_200_000;

                for (ts, msg) in [
                    (T_EARLY, "timeout error occurred in scheduler"),
                    (T_TARGET, "timeout error occurred in dispatcher"),
                    (T_LATE, "timeout error occurred in cleaner"),
                ] {
                    let id = format!("event-{}", Uuid::new_v4());
                    // CQL accepts timestamp literals in ISO-8601 or as integer ms.
                    // We use the integer form which ScyllaDB supports via `dateof`
                    // or by casting — but the simplest portable form is to pass the
                    // ms value directly as a CQL `bigint` cast to timestamp.
                    let cql = format!(
                        "INSERT INTO {}.{} (event_id, message, created_at) \
                         VALUES ('{}', '{}', {})",
                        cluster.keyspace(),
                        cluster.table_name(),
                        id,
                        msg,
                        ts,
                    );
                    cluster
                        .session()
                        .query_unpaged(cql, ())
                        .await
                        .with_context(|| format!("inserting event at ts={}", ts))?;
                }

                // Wait until ALL three events are indexed. The combined
                // keyword + time-range assertion (`== 1`) is only valid
                // when all three events have been committed; otherwise the
                // out-of-window events might not yet exist in the index.
                for _ in 0..60 {
                    let resp = gateway
                        .search(&SearchRequest {
                            query: "document.message:timeout".to_string(),
                            limit: 10,
                            offset: 0,
                            consistency: 1,
                            default_fields: vec![],
                            facet_fields: vec![],
                            boost_fields: vec![],
                            group_by_partition: false,
                        })
                        .await
                        .context("polling for all timeout events")?;

                    if resp.total_hits >= 3 {
                        break;
                    }
                    sleep(Duration::from_millis(500)).await;
                }

                // Keyword "timeout" matches all three, but the time-range window
                // should narrow it to exactly one (the 2022 event).
                let query = format!(
                    "document.message:timeout AND document.created_at:[{} TO {}]",
                    WINDOW_START, WINDOW_END,
                );
                let resp = gateway
                    .search(&SearchRequest {
                        query,
                        limit: 10,
                        offset: 0,
                        consistency: 1,
                        default_fields: vec![],
                        facet_fields: vec![],
                        boost_fields: vec![],
                        group_by_partition: false,
                    })
                    .await
                    .context("log time-range + keyword search")?;

                ensure!(
                    resp.total_hits == 1,
                    "expected exactly one hit (2022 event) inside the time window, got {} — \
                     check that events outside the window are excluded",
                    resp.total_hits
                );

                Ok(())
            }
            .boxed()
        })
        .await
}