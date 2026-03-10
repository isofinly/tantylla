"""services.py: Health checks for all benchmark stack services.

Replaces wait-for-services.sh. Uses cassandra-driver for ScyllaDB CQL and
requests for HTTP endpoints — no external tooling (cqlsh, curl) needed.
"""

import time

import requests
from cassandra.cluster import Cluster

_DEFAULT_RETRIES = 60
_DEFAULT_INTERVAL = 3.0


def wait_for_scylla(
    port: int,
    retries: int = _DEFAULT_RETRIES,
    interval: float = _DEFAULT_INTERVAL,
) -> None:
    """Block until ScyllaDB accepts a CQL connection on localhost:port."""
    print(f"Waiting for ScyllaDB CQL on port {port} ", end="", flush=True)
    for _ in range(retries):
        try:
            cluster = Cluster(["localhost"], port=port, connect_timeout=3)
            session = cluster.connect()
            session.execute("SELECT release_version FROM system.local")
            cluster.shutdown()
            print(" OK")
            return
        except Exception:
            print(".", end="", flush=True)
            time.sleep(interval)
    print()
    raise RuntimeError(f"ScyllaDB did not become healthy after {retries} attempts")


def wait_for_http(
    name: str,
    url: str,
    retries: int = _DEFAULT_RETRIES,
    interval: float = _DEFAULT_INTERVAL,
) -> None:
    """Block until an HTTP endpoint returns a 2xx response."""
    print(f"Waiting for {name} at {url} ", end="", flush=True)
    for _ in range(retries):
        try:
            if requests.get(url, timeout=5).ok:
                print(" OK")
                return
        except Exception:
            pass
        print(".", end="", flush=True)
        time.sleep(interval)
    print()
    raise RuntimeError(f"{name} did not become healthy after {retries} attempts")


def wait_for_all(
    scylla_port: int | None = None,
    gateway_port: int | None = None,
    es_port: int | None = None,
    retries: int = _DEFAULT_RETRIES,
    interval: float = _DEFAULT_INTERVAL,
) -> None:
    """Wait for all configured services. Pass None to skip a service."""
    if scylla_port:
        wait_for_scylla(scylla_port, retries, interval)
    if gateway_port:
        wait_for_http(
            "Tantylla Gateway",
            f"http://localhost:{gateway_port}/api/health",
            retries,
            interval,
        )
    if es_port:
        wait_for_http(
            "Elasticsearch",
            f"http://localhost:{es_port}/_cluster/health",
            retries,
            interval,
        )
    print("All services healthy.")
