CREATE OR REPLACE TABLE resources AS
SELECT
    CASE
        WHEN name LIKE '%competitor%' THEN 'elasticsearch-stack'
        WHEN name LIKE '%tantylla%'   THEN 'tantylla-stack'
    END AS stack,
    regexp_extract(filename, 'resources-([^-]+)-', 1) AS phase,
    name                                              AS container,
    strptime(ts, '%Y-%m-%dT%H:%M:%S.%fZ')            AS sampled_at,
    CAST(cpu_pct      AS DOUBLE) AS cpu_pct,
    CAST(mem_used_mb  AS DOUBLE) AS mem_used_mb,
    CAST(mem_limit_mb AS DOUBLE) AS mem_limit_mb,
    round(mem_used_mb / nullif(mem_limit_mb, 0) * 100.0, 2) AS mem_pct,
    CAST(net_rx_mb    AS DOUBLE) AS net_rx_mb,
    CAST(net_tx_mb    AS DOUBLE) AS net_tx_mb,
    CAST(blk_read_mb  AS DOUBLE) AS blk_read_mb,
    CAST(blk_write_mb AS DOUBLE) AS blk_write_mb
FROM read_json(
    'data/output/resources-*.ndjson',
    format      := 'newline_delimited',
    auto_detect := true,
    filename    := true
)
WHERE cpu_pct > 0
  AND CASE
          WHEN name LIKE '%competitor%' THEN 'elasticsearch-stack'
          WHEN name LIKE '%tantylla%'   THEN 'tantylla-stack'
      END IS NOT NULL;

CREATE OR REPLACE TABLE ingest_runs AS
SELECT
    r.stack,
    r.system,
    filename,
    benchmark,
    CAST(doc_count                 AS INTEGER) AS doc_count,
    CAST(scylla_write_secs         AS DOUBLE)  AS scylla_write_secs,
    CAST(scylla_write_docs_per_sec AS DOUBLE)  AS scylla_write_docs_per_sec,
    CAST(sliding_window_secs       AS DOUBLE)  AS sliding_window_secs,
    CAST(r.total_secs              AS DOUBLE)  AS total_secs,
    CAST(r.peak_docs_per_sec       AS DOUBLE)  AS peak_docs_per_sec,
    CAST(r.avg_docs_per_sec        AS DOUBLE)  AS avg_docs_per_sec,
    CAST(r.total_secs AS DOUBLE) - CAST(scylla_write_secs AS DOUBLE) AS lag_secs
FROM read_json('data/output/ingest-results-*.json', auto_detect := true, filename := true),
     unnest(results) AS t(r);

CREATE OR REPLACE TABLE ingest_visibility_samples AS
SELECT
    r.stack,
    r.system,
    filename,
    benchmark,
    CAST(doc_count         AS INTEGER) AS doc_count,
    CAST(s.elapsed_secs    AS DOUBLE)  AS elapsed_secs,
    CAST(s.visible_count   AS INTEGER) AS visible_count
FROM read_json('data/output/ingest-results-*.json', auto_detect := true, filename := true),
     unnest(results)   AS t(r),
     unnest(r.samples) AS u(s);

CREATE OR REPLACE TABLE search_runs AS
SELECT
    stack,
    filename,
    regexp_extract(filename, '(ingest|search|benchmark)', 1) AS bench_type,
    CAST(qps               AS DOUBLE)  AS qps,
    CAST(p50               AS DOUBLE)  AS p50_ms,
    CAST(p95               AS DOUBLE)  AS p95_ms,
    CAST(p99               AS DOUBLE)  AS p99_ms,
    CAST(mean              AS DOUBLE)  AS mean_ms,
    CAST(total_hits        AS BIGINT)  AS total_hits,
    CAST(latency_errors    AS INTEGER) AS latency_errors,
    CAST(throughput_errors AS INTEGER) AS throughput_errors
FROM read_json('data/output/benchmark-results-*.json', auto_detect := true, filename := true);

CREATE OR REPLACE TABLE search_latency_samples AS
SELECT
    stack,
    filename,
    regexp_extract(filename, '(ingest|search|benchmark)', 1) AS bench_type,
    CAST(latency_ms AS DOUBLE) AS latency_ms
FROM read_json('data/output/benchmark-results-*.json', auto_detect := true, filename := true),
     unnest(latencies_ms) AS t(latency_ms);

SELECT 'resources'                AS tbl, count(*) AS rows FROM resources
UNION ALL
SELECT 'ingest_runs',               count(*) FROM ingest_runs
UNION ALL
SELECT 'ingest_visibility_samples', count(*) FROM ingest_visibility_samples
UNION ALL
SELECT 'search_runs',               count(*) FROM search_runs
UNION ALL
SELECT 'search_latency_samples',    count(*) FROM search_latency_samples
ORDER BY tbl;
