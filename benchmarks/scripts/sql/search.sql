SELECT
    stack,
    bench_type,
    count(*) AS runs,

    round(avg(qps), 2) AS qps_mean,
    round(stddev(qps), 2) AS qps_stddev,
    round(min(qps), 2) AS qps_min,
    round(max(qps), 2) AS qps_max,

    round(avg(p50_ms), 3) AS p50_mean_ms,
    round(avg(p95_ms), 3) AS p95_mean_ms,
    round(avg(p99_ms), 3) AS p99_mean_ms,
    round(avg(mean_ms), 3) AS mean_latency_ms,

    sum(total_hits) AS total_hits,
    sum(latency_errors) AS latency_errors,
    sum(throughput_errors) AS throughput_errors,
    round(
        sum(latency_errors)::DOUBLE
        / nullif(sum(latency_errors) + sum(total_hits), 0) * 100,
        4
    ) AS error_rate_pct

FROM search_runs
GROUP BY stack, bench_type
ORDER BY bench_type, stack;

SELECT
    stack,
    bench_type,
    round(quantile_cont(latency_ms, 0.50),  3)  AS p50_ms,
    round(quantile_cont(latency_ms, 0.75),  3)  AS p75_ms,
    round(quantile_cont(latency_ms, 0.90),  3)  AS p90_ms,
    round(quantile_cont(latency_ms, 0.95),  3)  AS p95_ms,
    round(quantile_cont(latency_ms, 0.99),  3)  AS p99_ms,
    round(quantile_cont(latency_ms, 0.999), 3)  AS p999_ms,
    round(avg(latency_ms), 3)  AS mean_ms,
    round(stddev(latency_ms), 3)  AS stddev_ms,
    round(min(latency_ms), 3)  AS min_ms,
    round(max(latency_ms), 3)  AS max_ms,
    count(*) AS total_samples
FROM search_latency_samples
GROUP BY stack, bench_type
ORDER BY bench_type, stack;
