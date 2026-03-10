CREATE OR REPLACE TEMP VIEW resource_agg AS
SELECT
    stack,
    phase,
    round(avg(cpu_pct),            2) AS cpu_avg_pct,
    round(max(cpu_pct),            2) AS cpu_peak_pct,
    round(avg(mem_pct),            2) AS mem_avg_pct,
    round(avg(mem_used_mb) / 1024, 3) AS mem_avg_gb,
    round(max(mem_used_mb) / 1024, 3) AS mem_peak_gb
FROM resources
GROUP BY stack, phase;

CREATE OR REPLACE TEMP VIEW ingest_agg AS
SELECT
    stack,
    benchmark,
    round(avg(avg_docs_per_sec),  2) AS avg_dps,
    round(avg(peak_docs_per_sec), 2) AS peak_dps,
    round(avg(lag_secs),          2) AS avg_lag_secs
FROM ingest_runs
GROUP BY stack, benchmark;

CREATE OR REPLACE TEMP VIEW search_agg AS
SELECT
    stack,
    round(avg(qps),     2) AS avg_qps,
    round(avg(mean_ms), 3) AS avg_mean_ms,
    round(avg(p99_ms),  3) AS avg_p99_ms
FROM search_runs
GROUP BY stack;

SELECT
    i.stack,
    i.benchmark,
    i.avg_dps,
    i.peak_dps,
    i.avg_lag_secs,
    r.cpu_avg_pct,
    r.cpu_peak_pct,
    r.mem_avg_gb,

    round(i.avg_dps  / nullif(r.cpu_avg_pct,  0), 2) AS docs_per_cpu_pct,
    round(i.avg_dps  / nullif(r.mem_avg_gb,   0), 2) AS docs_per_gb_ram,
    round(i.peak_dps / nullif(r.cpu_peak_pct, 0), 2) AS peak_docs_per_peak_cpu_pct

FROM ingest_agg  i
JOIN resource_agg r ON i.stack = r.stack AND r.phase = 'ingest'
ORDER BY i.benchmark, i.stack;

SELECT
    s.stack,
    s.avg_qps,
    s.avg_mean_ms,
    s.avg_p99_ms,
    r.cpu_avg_pct,
    r.mem_avg_gb,

    round(s.avg_qps    / nullif(r.cpu_avg_pct, 0), 3) AS qps_per_cpu_pct,
    round(s.avg_p99_ms * r.mem_avg_gb,             3) AS p99_x_ram_score,
    round(s.avg_p99_ms / nullif(s.avg_mean_ms, 0), 2) AS p99_to_mean_ratio

FROM search_agg   s
JOIN resource_agg r ON s.stack = r.stack AND r.phase = 'search'
ORDER BY s.stack;

SELECT
    i.benchmark,

    -- Ingest
    max(CASE WHEN i.stack = 'elasticsearch-stack' THEN i.avg_dps END)      AS es_avg_dps,
    max(CASE WHEN i.stack = 'tantylla-stack'      THEN i.avg_dps END)      AS own_avg_dps,
    max(CASE WHEN i.stack = 'elasticsearch-stack' THEN i.avg_lag_secs END) AS es_lag_secs,
    max(CASE WHEN i.stack = 'tantylla-stack'      THEN i.avg_lag_secs END) AS own_lag_secs,

    -- Search
    max(CASE WHEN s.stack = 'elasticsearch-stack' THEN s.avg_qps END)      AS es_avg_qps,
    max(CASE WHEN s.stack = 'tantylla-stack'      THEN s.avg_qps END)      AS own_avg_qps,
    max(CASE WHEN s.stack = 'elasticsearch-stack' THEN s.avg_p99_ms END)   AS es_p99_ms,
    max(CASE WHEN s.stack = 'tantylla-stack'      THEN s.avg_p99_ms END)   AS own_p99_ms,

    -- Resources (search phase only — ingest-phase data not yet collected)
    max(CASE WHEN rs.stack = 'elasticsearch-stack' THEN rs.cpu_avg_pct END) AS es_search_cpu_avg_pct,
    max(CASE WHEN rs.stack = 'tantylla-stack'      THEN rs.cpu_avg_pct END) AS own_search_cpu_avg_pct,
    max(CASE WHEN rs.stack = 'elasticsearch-stack' THEN rs.mem_avg_gb END)  AS es_search_mem_avg_gb,
    max(CASE WHEN rs.stack = 'tantylla-stack'      THEN rs.mem_avg_gb END)  AS own_search_mem_avg_gb

FROM ingest_agg i
CROSS JOIN search_agg s
JOIN resource_agg rs ON rs.phase = 'search'
GROUP BY i.benchmark
ORDER BY i.benchmark;
