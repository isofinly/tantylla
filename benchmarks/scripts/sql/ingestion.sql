SELECT
    stack,
    benchmark,
    max(doc_count) AS doc_count,
    round(avg(scylla_write_secs), 2) AS scylla_write_secs_mean,
    round(avg(scylla_write_docs_per_sec), 2) AS scylla_write_dps_mean,

    round(avg(peak_docs_per_sec), 2) AS peak_dps_mean,
    round(stddev(peak_docs_per_sec), 2) AS peak_dps_stddev,
    round(avg(avg_docs_per_sec), 2) AS avg_dps_mean,
    round(stddev(avg_docs_per_sec), 2) AS avg_dps_stddev,

    round(avg(total_secs), 2) AS total_secs_mean,
    round(avg(lag_secs), 2) AS lag_secs_mean,
    round(stddev(lag_secs), 2) AS lag_secs_stddev,
    round(min(lag_secs), 2) AS lag_secs_min,
    round(max(lag_secs), 2) AS lag_secs_max,

    count(*) AS runs
FROM ingest_runs
GROUP BY stack, benchmark
ORDER BY benchmark, stack;

SELECT
    stack,
    benchmark,
    round(min(CASE WHEN visible_count >= doc_count * 0.50 THEN elapsed_secs END), 2) AS secs_to_50pct,
    round(min(CASE WHEN visible_count >= doc_count * 0.90 THEN elapsed_secs END), 2) AS secs_to_90pct,
    round(min(CASE WHEN visible_count >= doc_count THEN elapsed_secs END), 2) AS secs_to_100pct
FROM ingest_visibility_samples
GROUP BY stack, benchmark
ORDER BY benchmark, stack;

SELECT
    stack,
    benchmark,
    round(elapsed_secs, 0) AS elapsed_secs,
    round(avg(visible_count), 0) AS avg_visible,
    round(avg(visible_count) / max(doc_count) * 100, 1) AS avg_visible_pct
FROM ingest_visibility_samples
GROUP BY stack, benchmark, round(elapsed_secs, 0)
ORDER BY benchmark, stack, elapsed_secs;
