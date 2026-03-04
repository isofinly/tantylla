#!/usr/bin/env python3
"""
run-benchmark.py: Search latency and throughput benchmark.

Runs identical search queries against Tantylla and/or Elasticsearch,
measures per-query latency, and reports p50 / p95 / p99 percentiles
plus aggregate throughput (QPS).

The benchmark consists of three phases:
  1. Warmup: A small burst of queries to prime caches and JIT.
  2. Latency: Sequential queries measuring per-request time.
  3. Throughput: Concurrent queries measuring sustained QPS.

Query workload:
  - Single-term queries  (e.g., "wireless")
  - Multi-term queries   (e.g., "premium bluetooth headphones")
  - Phrase queries        (e.g., '"noise cancellation"')
  - Field-scoped queries  (Tantivy syntax: 'brand:Zenith')

Usage:
    python scripts/run-benchmark.py \\
        --tantylla-url http://localhost:8080 \\
        --elasticsearch-url http://localhost:9200 \\
        --queries 1000 --concurrency 10

Dependencies:
    pip install requests
"""

import argparse
import json
import random
import statistics
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Optional

try:
    import requests
except ImportError:
    print(
        "ERROR: requests is required.\n"
        "Install it with: pip install requests",
        file=sys.stderr,
    )
    sys.exit(1)


# =========================================================================
# Query Generation
# =========================================================================
# Queries are drawn from vocabularies used by seed-data.py so they have
# a high probability of matching indexed documents.

SINGLE_TERMS = [
    "wireless", "premium", "portable", "ergonomic", "bluetooth",
    "waterproof", "rechargeable", "lightweight", "professional", "compact",
    "durable", "foldable", "adjustable", "insulated", "magnetic",
    "stainless", "organic", "headphones", "speakers", "keyboard",
    "monitor", "camping", "fitness", "cycling", "running",
]

MULTI_TERMS = [
    "premium wireless headphones",
    "portable bluetooth speaker",
    "ergonomic professional keyboard",
    "waterproof outdoor camping",
    "rechargeable battery charger",
    "noise cancelling earbuds",
    "lightweight carbon fiber",
    "durable stainless steel",
    "high performance monitor",
    "eco friendly organic material",
    "compact travel adapter",
    "advanced fitness tracker",
    "slim leather wallet",
    "heavy duty hiking boots",
    "smart home lighting",
]

PHRASE_QUERIES = [
    '"quick-charge technology"',
    '"active noise cancellation"',
    '"all-day comfort"',
    '"extended battery life"',
    '"precision engineering"',
    '"impact-resistant construction"',
    '"memory foam cushioning"',
    '"one-touch operation"',
    '"USB-C fast charging"',
    '"multi-device pairing"',
]

BRAND_QUERIES = [
    "Acme", "Zenith", "Apex", "Stellar", "Vortex",
    "Nexus", "Pinnacle", "Prism", "Quantum", "Aether",
]


def generate_query_mix(count: int) -> list[dict]:
    """
    Generate a balanced mix of query types.

    Distribution:
      40% single-term (simple lookup)
      30% multi-term  (boolean matching)
      15% phrase       (exact sequence)
      15% brand-scoped (field-specific)
    """
    queries = []
    for _ in range(count):
        r = random.random()
        if r < 0.40:
            q = random.choice(SINGLE_TERMS)
            qtype = "single-term"
        elif r < 0.70:
            q = random.choice(MULTI_TERMS)
            qtype = "multi-term"
        elif r < 0.85:
            q = random.choice(PHRASE_QUERIES)
            qtype = "phrase"
        else:
            q = random.choice(BRAND_QUERIES)
            qtype = "brand"
        queries.append({"query": q, "type": qtype})
    return queries


# =========================================================================
# Search Clients
# =========================================================================


def search_tantylla(url: str, query: str, limit: int = 10) -> dict:
    """Send a search request to the Tantylla gateway."""
    resp = requests.post(
        f"{url}/api/v1/search",
        json={"query": query, "limit": limit, "offset": 0, "consistency": 1},
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()
    return {
        "total_hits": data.get("total_hits", data.get("totalHits", 0)),
        "hit_count": len(data.get("hits", [])),
    }


def search_elasticsearch(url: str, query: str, limit: int = 10) -> dict:
    """Send a search request to Elasticsearch."""
    # Use multi_match to search across all text fields, similar to how
    # Tantivy's QueryParser searches the JSON document field.
    body = {
        "query": {
            "multi_match": {
                "query": query.strip('"'),
                "fields": ["name", "description", "brand"],
                "type": "best_fields",
            }
        },
        "size": limit,
    }

    # For phrase queries, use match_phrase instead.
    if query.startswith('"') and query.endswith('"'):
        phrase = query.strip('"')
        body = {
            "query": {
                "multi_match": {
                    "query": phrase,
                    "fields": ["name", "description", "brand"],
                    "type": "phrase",
                }
            },
            "size": limit,
        }

    resp = requests.post(
        f"{url}/benchmark.products/_search",
        json=body,
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()
    hits = data.get("hits", {})
    return {
        "total_hits": hits.get("total", {}).get("value", 0),
        "hit_count": len(hits.get("hits", [])),
    }


# =========================================================================
# Benchmark Runner
# =========================================================================


@dataclass
class BenchmarkResult:
    system: str
    query_count: int
    latencies_ms: list[float] = field(default_factory=list)
    errors: int = 0
    total_hits: int = 0

    @property
    def p50(self) -> float:
        return self._percentile(50)

    @property
    def p95(self) -> float:
        return self._percentile(95)

    @property
    def p99(self) -> float:
        return self._percentile(99)

    @property
    def mean(self) -> float:
        return statistics.mean(self.latencies_ms) if self.latencies_ms else 0.0

    @property
    def qps(self) -> float:
        total_s = sum(self.latencies_ms) / 1000.0
        return len(self.latencies_ms) / total_s if total_s > 0 else 0.0

    def _percentile(self, p: int) -> float:
        if not self.latencies_ms:
            return 0.0
        sorted_lat = sorted(self.latencies_ms)
        idx = int(len(sorted_lat) * p / 100)
        idx = min(idx, len(sorted_lat) - 1)
        return sorted_lat[idx]


def run_latency_benchmark(
    system_name: str,
    search_fn,
    queries: list[dict],
    warmup: int = 50,
) -> BenchmarkResult:
    """Run sequential queries and measure per-request latency."""
    result = BenchmarkResult(system=system_name, query_count=len(queries))

    # Warmup phase: prime caches and JIT, discard timings.
    warmup_queries = queries[:warmup] if len(queries) >= warmup else queries
    print(f"  [{system_name}] Warmup: {len(warmup_queries)} queries...")
    for q in warmup_queries:
        try:
            search_fn(q["query"])
        except Exception:
            pass

    # Measurement phase.
    print(f"  [{system_name}] Latency: {len(queries)} queries (sequential)...")
    for q in queries:
        t0 = time.perf_counter()
        try:
            res = search_fn(q["query"])
            elapsed_ms = (time.perf_counter() - t0) * 1000.0
            result.latencies_ms.append(elapsed_ms)
            result.total_hits += res["total_hits"]
        except Exception as e:
            elapsed_ms = (time.perf_counter() - t0) * 1000.0
            result.latencies_ms.append(elapsed_ms)
            result.errors += 1

    return result


def run_throughput_benchmark(
    system_name: str,
    search_fn,
    queries: list[dict],
    concurrency: int,
    duration_secs: int = 30,
) -> float:
    """Run concurrent queries for a fixed duration and return QPS."""
    print(f"  [{system_name}] Throughput: {concurrency} threads for {duration_secs}s...")

    # Thread-safe counters: plain int increment is not guaranteed atomic
    # across all Python implementations, so we guard with a lock.
    lock = threading.Lock()
    completed = 0
    errors = 0
    deadline = time.monotonic() + duration_secs

    def worker():
        nonlocal completed, errors
        local_completed = 0
        local_errors = 0
        while time.monotonic() < deadline:
            q = random.choice(queries)
            try:
                search_fn(q["query"])
                local_completed += 1
            except Exception:
                local_errors += 1
        with lock:
            completed += local_completed
            errors += local_errors

    t0 = time.monotonic()
    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        futures = [pool.submit(worker) for _ in range(concurrency)]
        for f in as_completed(futures):
            f.result()  # Propagate exceptions.

    elapsed = time.monotonic() - t0
    qps = completed / elapsed if elapsed > 0 else 0
    print(f"    Completed {completed:,} queries in {elapsed:.1f}s ({qps:,.0f} QPS, {errors} errors)")
    return qps


def print_report(results: list[BenchmarkResult], throughputs: dict[str, float]):
    """Print a formatted comparison table."""
    print("\n" + "=" * 72)
    print("BENCHMARK RESULTS")
    print("=" * 72)

    # Header.
    print(f"{'System':<20} {'p50':>8} {'p95':>8} {'p99':>8} {'Mean':>8} {'Errors':>8} {'QPS':>10}")
    print("-" * 72)

    for r in results:
        qps = throughputs.get(r.system, r.qps)
        print(
            f"{r.system:<20} "
            f"{r.p50:>7.1f}ms "
            f"{r.p95:>7.1f}ms "
            f"{r.p99:>7.1f}ms "
            f"{r.mean:>7.1f}ms "
            f"{r.errors:>8} "
            f"{qps:>9.0f}"
        )

    print("-" * 72)

    # Comparison.
    if len(results) == 2:
        a, b = results
        if a.p50 > 0 and b.p50 > 0:
            ratio = a.p50 / b.p50
            faster = b.system if ratio > 1 else a.system
            factor = max(ratio, 1 / ratio)
            print(f"\np50 comparison: {faster} is {factor:.1f}x faster")

        qps_a = throughputs.get(a.system, a.qps)
        qps_b = throughputs.get(b.system, b.qps)
        if qps_a > 0 and qps_b > 0:
            ratio = qps_a / qps_b
            higher = a.system if ratio > 1 else b.system
            factor = max(ratio, 1 / ratio)
            print(f"QPS comparison: {higher} has {factor:.1f}x higher throughput")

    print()


def main():
    parser = argparse.ArgumentParser(description="FTS benchmark runner")
    parser.add_argument(
        "--tantylla-url", type=str, default=None,
        help="Tantylla gateway URL (e.g., http://localhost:8080)"
    )
    parser.add_argument(
        "--elasticsearch-url", type=str, default=None,
        help="Elasticsearch URL (e.g., http://localhost:9200)"
    )
    parser.add_argument(
        "--queries", type=int, default=1000,
        help="Number of search queries for the latency benchmark"
    )
    parser.add_argument(
        "--concurrency", type=int, default=10,
        help="Number of concurrent threads for the throughput benchmark"
    )
    parser.add_argument(
        "--throughput-duration", type=int, default=30,
        help="Duration of the throughput benchmark in seconds"
    )
    parser.add_argument(
        "--limit", type=int, default=10,
        help="Number of results per query"
    )
    parser.add_argument(
        "--warmup", type=int, default=50,
        help="Number of warmup queries (not measured)"
    )
    parser.add_argument(
        "--seed", type=int, default=42,
        help="Random seed for reproducible query generation"
    )
    args = parser.parse_args()

    if not args.tantylla_url and not args.elasticsearch_url:
        print("ERROR: at least one of --tantylla-url or --elasticsearch-url is required", file=sys.stderr)
        sys.exit(1)

    random.seed(args.seed)
    queries = generate_query_mix(args.queries)

    print(f"Generated {len(queries)} queries (seed={args.seed})")
    print(f"  Single-term: {sum(1 for q in queries if q['type'] == 'single-term')}")
    print(f"  Multi-term:  {sum(1 for q in queries if q['type'] == 'multi-term')}")
    print(f"  Phrase:      {sum(1 for q in queries if q['type'] == 'phrase')}")
    print(f"  Brand:       {sum(1 for q in queries if q['type'] == 'brand')}")
    print()

    results = []
    throughputs = {}

    # --- Tantylla ---
    if args.tantylla_url:
        print(f"Benchmarking Tantylla at {args.tantylla_url}")
        search_fn = lambda q: search_tantylla(args.tantylla_url, q, args.limit)

        lat = run_latency_benchmark("Tantylla", search_fn, queries, warmup=args.warmup)
        results.append(lat)

        qps = run_throughput_benchmark(
            "Tantylla", search_fn, queries,
            concurrency=args.concurrency,
            duration_secs=args.throughput_duration,
        )
        throughputs["Tantylla"] = qps
        print()

    # --- Elasticsearch ---
    if args.elasticsearch_url:
        print(f"Benchmarking Elasticsearch at {args.elasticsearch_url}")
        search_fn = lambda q: search_elasticsearch(args.elasticsearch_url, q, args.limit)

        lat = run_latency_benchmark("Elasticsearch", search_fn, queries, warmup=args.warmup)
        results.append(lat)

        qps = run_throughput_benchmark(
            "Elasticsearch", search_fn, queries,
            concurrency=args.concurrency,
            duration_secs=args.throughput_duration,
        )
        throughputs["Elasticsearch"] = qps
        print()

    print_report(results, throughputs)

    # Dump raw latencies to JSON for further analysis.
    raw_path = "benchmark-results.json"
    raw = {
        r.system: {
            "latencies_ms": r.latencies_ms,
            "errors": r.errors,
            "total_hits": r.total_hits,
            "p50": r.p50,
            "p95": r.p95,
            "p99": r.p99,
            "mean": r.mean,
            "qps": throughputs.get(r.system, r.qps),
        }
        for r in results
    }
    with open(raw_path, "w") as f:
        json.dump(raw, f, indent=2)
    print(f"Raw results written to {raw_path}")


if __name__ == "__main__":
    main()
