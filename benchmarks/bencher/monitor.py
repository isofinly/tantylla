"""monitor.py: Docker container resource sampler.

Replaces scripts/monitor-resources.sh.

ResourceMonitor runs in a daemon background thread, polling
`docker stats` via the Docker SDK at a configurable interval and
appending one NDJSON record per container per sample to an output file.

All numeric fields are bare floats (no unit strings) so DuckDB can
read them directly without regexp parsing.  Memory and network values
are in megabytes; CPU and memory percentage values are plain floats.

Output schema (one JSON object per line):
    {
      "ts":           "2026-03-05T10:00:01.350Z",
      "name":         "bench-competitor-elasticsearch",
      "cpu_pct":      12.34,
      "mem_used_mb":  1260.8,
      "mem_limit_mb": 15360.0,
      "mem_pct":      8.21,
      "net_rx_mb":    101.0,
      "net_tx_mb":    159.0,
      "blk_read_mb":  0.0,
      "blk_write_mb": 113.0
    }
"""

import json
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import docker.errors

import docker

# =========================================================================
# Stat parsing helpers
# =========================================================================


def _parse_cpu_pct(stats: dict) -> float:
    """Derive CPU usage percentage from raw cgroup counters.

    Docker computes: delta_cpu / delta_system * num_cpus * 100.
    This matches what `docker stats` displays.
    """
    cpu = stats.get("cpu_stats", {})
    pre = stats.get("precpu_stats", {})

    cpu_delta = cpu.get("cpu_usage", {}).get("total_usage", 0) - pre.get(
        "cpu_usage", {}
    ).get("total_usage", 0)
    sys_delta = cpu.get("system_cpu_usage", 0) - pre.get("system_cpu_usage", 0)
    num_cpus = len(cpu.get("cpu_usage", {}).get("percpu_usage") or []) or cpu.get(
        "online_cpus", 1
    )

    if sys_delta > 0 and cpu_delta > 0:
        return round((cpu_delta / sys_delta) * num_cpus * 100.0, 2)
    return 0.0


def _parse_mem(stats: dict) -> tuple[float, float, float]:
    """Return (mem_used_mb, mem_limit_mb, mem_pct) from Docker memory stats.

    cgroup v2 places the page-cache bytes under memory_stats.stats.cache
    instead of at the top level.  We handle both layouts so the monitor
    works on Linux hosts regardless of cgroup version.
    """
    mem = stats.get("memory_stats", {})
    usage_raw = mem.get("usage", 0)

    # Subtract page cache so we report RSS, matching docker stats behaviour.
    inner_stats = mem.get("stats", {})
    cache = inner_stats.get("cache", 0)  # cgroup v1 key
    if cache == 0:
        cache = inner_stats.get("inactive_file", 0)  # cgroup v2 key

    rss = max(usage_raw - cache, 0)
    limit = mem.get("limit", 0)

    mem_used_mb = round(rss / 1_048_576, 1)
    mem_limit_mb = round(limit / 1_048_576, 1)
    mem_pct = round(rss / limit * 100.0, 2) if limit > 0 else 0.0
    return mem_used_mb, mem_limit_mb, mem_pct


def _parse_net_io(stats: dict) -> tuple[float, float]:
    """Return (rx_mb, tx_mb) aggregated across all network interfaces."""
    nets = stats.get("networks", {})
    rx = sum(v.get("rx_bytes", 0) for v in nets.values())
    tx = sum(v.get("tx_bytes", 0) for v in nets.values())
    return round(rx / 1_048_576, 1), round(tx / 1_048_576, 1)


def _parse_blk_io(stats: dict) -> tuple[float, float]:
    """Return (read_mb, write_mb) from blkio_stats."""
    entries = stats.get("blkio_stats", {}).get("io_service_bytes_recursive") or []
    read_ = sum(e.get("value", 0) for e in entries if e.get("op", "").lower() == "read")
    write = sum(
        e.get("value", 0) for e in entries if e.get("op", "").lower() == "write"
    )
    return round(read_ / 1_048_576, 1), round(write / 1_048_576, 1)


# =========================================================================
# ResourceMonitor
# =========================================================================


class ResourceMonitor:
    """Background daemon thread that samples Docker container stats.

    Usage:
        mon = ResourceMonitor(output_path, name_prefix="bench-", interval_ms=350)
        mon.start()
        # ... run benchmark ...
        mon.stop()
    """

    def __init__(
        self,
        output: Path,
        name_prefix: str = "bench-",
        interval_ms: int = 350,
    ) -> None:
        self._output = output
        self._prefix = name_prefix
        self._interval = interval_ms / 1000.0
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._client = docker.from_env()

    def start(self) -> None:
        """Spawn the background sampling thread."""
        self._output.parent.mkdir(parents=True, exist_ok=True)
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()
        print(f"Resource monitor started → {self._output}")

    def stop(self) -> None:
        """Signal the thread to stop and wait for it to exit."""
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=self._interval * 3 + 2)
        self._client.close()
        print("Resource monitor stopped.")

    # ------------------------------------------------------------------
    # Internal sampling loop
    # ------------------------------------------------------------------

    def _loop(self) -> None:
        with open(self._output, "a") as fh:
            while not self._stop_event.is_set():
                self._sample(fh)
                # Sleep in small increments so stop() is responsive even
                # with a long interval.
                deadline = time.monotonic() + self._interval
                while time.monotonic() < deadline and not self._stop_event.is_set():
                    time.sleep(0.05)

    def _sample(self, fh) -> None:
        """Collect one stats snapshot from every matching container."""
        ts = (
            datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.")
            + f"{datetime.now(timezone.utc).microsecond // 1000:03d}Z"
        )

        try:
            containers = self._client.containers.list()
        except docker.errors.DockerException:
            return

        for container in containers:
            name: str = container.name or ""
            if not name.startswith(self._prefix):
                continue

            try:
                # stream=False: single snapshot, returns a plain dict (not a generator).
                # The Docker SDK stubs type this as Iterator | dict, so we cast explicitly.
                raw = container.stats(stream=False)
                stats: dict = raw if isinstance(raw, dict) else {}
            except docker.errors.DockerException:
                continue

            if not stats:
                continue

            cpu_pct = _parse_cpu_pct(stats)
            mem_used_mb, mem_limit_mb, mem_pct = _parse_mem(stats)
            net_rx_mb, net_tx_mb = _parse_net_io(stats)
            blk_read_mb, blk_write_mb = _parse_blk_io(stats)

            record = {
                "ts": ts,
                "name": name,
                "cpu_pct": cpu_pct,
                "mem_used_mb": mem_used_mb,
                "mem_limit_mb": mem_limit_mb,
                "mem_pct": mem_pct,
                "net_rx_mb": net_rx_mb,
                "net_tx_mb": net_tx_mb,
                "blk_read_mb": blk_read_mb,
                "blk_write_mb": blk_write_mb,
            }
            fh.write(json.dumps(record) + "\n")
            fh.flush()
