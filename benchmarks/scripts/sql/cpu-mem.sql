SELECT
    stack,
    phase,
    container,
    count(*) AS samples,

    round(avg(cpu_pct), 2) AS cpu_avg_pct,
    round(max(cpu_pct), 2) AS cpu_peak_pct,
    round(stddev(cpu_pct), 2) AS cpu_stddev_pct,

    round(avg(mem_pct), 2) AS mem_avg_pct,
    round(max(mem_pct), 2) AS mem_peak_pct,
    round(avg(mem_used_mb) / 1024, 3) AS mem_avg_gb,
    round(max(mem_used_mb) / 1024, 3) AS mem_peak_gb,
    round(avg(mem_limit_mb) / 1024, 3) AS mem_limit_gb,

    round(max(net_rx_mb), 1) AS net_rx_total_mb,
    round(max(net_tx_mb), 1) AS net_tx_total_mb,
    round(max(blk_read_mb), 1) AS blk_read_total_mb,
    round(max(blk_write_mb), 1) AS blk_write_total_mb

FROM resources
GROUP BY stack, phase, container
ORDER BY phase, stack, container;

SELECT
    stack,
    phase,
    round(sum(cpu_avg), 2) AS stack_cpu_avg_pct,
    round(max(cpu_peak), 2) AS stack_cpu_peak_pct,
    round(sum(mem_avg), 3) AS stack_mem_avg_gb,
    round(max(mem_peak), 3) AS stack_mem_peak_gb
FROM (
    SELECT
        stack,
        phase,
        container,
        avg(cpu_pct) AS cpu_avg,
        max(cpu_pct) AS cpu_peak,
        avg(mem_used_mb) / 1024 AS mem_avg,
        max(mem_used_mb) / 1024 AS mem_peak
    FROM resources
    GROUP BY stack, phase, container
)
GROUP BY stack, phase
ORDER BY phase, stack;
