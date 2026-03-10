"""search.py: Search latency and throughput benchmark.

Replaces scripts/run-benchmark.py.

Three phases per target system:
  1. Warmup  — small burst, timings discarded (primes caches and JIT).
  2. Latency — sequential queries, per-request latency captured.
  3. Throughput — concurrent threads for a fixed duration, QPS measured.

Query workload mix (40 / 30 / 15 / 15):
  single-term  — e.g. "wireless"
  multi-term   — e.g. "premium bluetooth headphones"
  phrase       — e.g. '"noise cancellation"'
  brand        — e.g. "Zenith"  (always has matching docs after seed.py)
"""

import json
import random
import statistics
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import requests

# =========================================================================
# Query corpus
# =========================================================================

SINGLE_TERMS: list[str] = [
    "wireless",
    "premium",
    "portable",
    "ergonomic",
    "bluetooth",
    "waterproof",
    "rechargeable",
    "lightweight",
    "professional",
    "compact",
    "durable",
    "foldable",
    "adjustable",
    "insulated",
    "magnetic",
    "stainless",
    "organic",
    "headphones",
    "speakers",
    "keyboard",
    "monitor",
    "camping",
    "fitness",
    "cycling",
    "running",
]

MULTI_TERMS: list[str] = [
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

PHRASE_QUERIES: list[str] = [
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

# Must match datagen.BRANDS so brand queries always find results.
BRAND_QUERIES: list[str] = [
    "Acme",
    "Zenith",
    "Apex",
    "Stellar",
    "Vortex",
    "Nexus",
    "Pinnacle",
    "Prism",
    "Quantum",
    "Aether",
]

# =========================================================================
# Query expansion helpers
# =========================================================================

# Tantivy stores document fields as nested JSON-path terms.
# A query against "document.name" targets the `name` field inside the
# stored JSON document.  All text fields must be listed to give Tantivy
# an equivalent search scope to Elasticsearch's multi_match.
_TANTIVY_TEXT_FIELDS: list[str] = [
    "document.name",
    "document.description",
    "document.brand",
    "document.category",
    "document.subcategory",
]

# ES CDC envelope: each column lives at after.<col>.value.
_ES_TEXT_FIELDS: list[str] = [
    "after.name.value",
    "after.description.value",
    "after.brand.value",
    "after.category.value",
    "after.subcategory.value",
]


def _to_tantivy_query(raw: str) -> str:
    """Expand a plain query string to explicit Tantivy JSON field-path syntax.

    Phrase queries (surrounded by double-quotes) are forwarded verbatim with
    the field prefix prepended.  Multi-word and single-term queries are
    expanded across all text fields with OR.
    """
    if raw.startswith('"') and raw.endswith('"'):
        return " OR ".join(f"{f}:{raw}" for f in _TANTIVY_TEXT_FIELDS)
    if " " in raw:
        return " OR ".join(f"{f}:({raw})" for f in _TANTIVY_TEXT_FIELDS)
    return " OR ".join(f"{f}:{raw}" for f in _TANTIVY_TEXT_FIELDS)


# =========================================================================
# Query mix generation
# =========================================================================


def generate_query_mix(count: int) -> list[dict]:
    """Return `count` queries sampled from the four query types.

    Distribution: 40% single-term / 30% multi-term / 15% phrase / 15% brand.
    """
    queries = []
    for _ in range(count):
        r = random.random()
        if r < 0.40:
            queries.append(
                {"query": random.choice(SINGLE_TERMS), "type": "single-term"}
            )
        elif r < 0.70:
            queries.append({"query": random.choice(MULTI_TERMS), "type": "multi-term"})
        elif r < 0.85:
            queries.append({"query": random.choice(PHRASE_QUERIES), "type": "phrase"})
        else:
            queries.append({"query": random.choice(BRAND_QUERIES), "type": "brand"})
    return queries


# =========================================================================
# Per-system search functions
# =========================================================================


def _search_tantylla(
    url: str, query: str, limit: int = 10, session: Optional[requests.Session] = None
) -> dict:
    req = session or requests
    resp = req.post(
        f"{url}/api/v1/search",
        json={
            "query": _to_tantivy_query(query),
            "limit": limit,
            "offset": 0,
            "consistency": 1,
        },
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()
    return {
        "total_hits": data.get("total_hits", 0),
        "hit_count": len(data.get("hits", [])),
    }


def _search_elasticsearch(
    url: str, query: str, limit: int = 10, session: Optional[requests.Session] = None
) -> dict:
    req = session or requests

    # NOTE: track_total_hits=True forces an exact count.  Without it ES may
    # cap the count at 10,000 or return a "gte" approximation.
    if query.startswith('"') and query.endswith('"'):
        body = {
            "query": {
                "multi_match": {
                    "query": query.strip('"'),
                    "fields": _ES_TEXT_FIELDS,
                    "type": "phrase",
                }
            },
            "size": limit,
            "track_total_hits": True,
        }
    else:
        body = {
            "query": {
                "multi_match": {
                    "query": query.strip('"'),
                    "fields": _ES_TEXT_FIELDS,
                    "type": "best_fields",
                }
            },
            "size": limit,
            "track_total_hits": True,
        }

    resp = req.post(f"{url}/scylla.benchmark.products/_search", json=body, timeout=30)
    resp.raise_for_status()
    hits = resp.json().get("hits", {})
    return {
        "total_hits": hits.get("total", {}).get("value", 0),
        "hit_count": len(hits.get("hits", [])),
    }


# =========================================================================
# Result dataclass
# =========================================================================


@dataclass
class SearchResult:
    system: str
    stack: str
    query_count: int
    latencies_ms: list[float] = field(default_factory=list)
    latency_errors: int = 0
    # throughput_errors is populated by run() after the throughput phase.
    throughput_errors: int = 0
    total_hits: int = 0
    # Concurrent QPS measured during the throughput phase.
    throughput_qps: float = 0.0

    @property
    def p50(self) -> float:
        return self._pct(50)

    @property
    def p95(self) -> float:
        return self._pct(95)

    @property
    def p99(self) -> float:
        return self._pct(99)

    @property
    def mean(self) -> float:
        return statistics.mean(self.latencies_ms) if self.latencies_ms else 0.0

    def _pct(self, p: int) -> float:
        if not self.latencies_ms:
            return 0.0
        s = sorted(self.latencies_ms)
        return s[min(int(len(s) * p / 100), len(s) - 1)]


# =========================================================================
# Benchmark phases
# =========================================================================


def _run_latency(
    system_name: str,
    stack: str,
    search_fn,
    queries: list[dict],
    warmup: int,
    limit: int,
) -> SearchResult:
    """Sequential latency phase.  Uses a single persistent HTTP session."""
    result = SearchResult(system=system_name, stack=stack, query_count=len(queries))

    with requests.Session() as session:
        bound = lambda q: search_fn(q, limit=limit, session=session)

        warmup_qs = queries[:warmup]
        print(f"  [{system_name}] Warmup: {len(warmup_qs)} queries...")
        for q in warmup_qs:
            try:
                bound(q["query"])
            except Exception:
                pass

        print(f"  [{system_name}] Latency: {len(queries)} queries (sequential)...")
        for q in queries:
            t0 = time.perf_counter()
            try:
                res = bound(q["query"])
                result.latencies_ms.append((time.perf_counter() - t0) * 1000.0)
                result.total_hits += res["total_hits"]
            except Exception:
                result.latencies_ms.append((time.perf_counter() - t0) * 1000.0)
                result.latency_errors += 1

    return result


def _run_throughput(
    system_name: str,
    search_fn,
    queries: list[dict],
    concurrency: int,
    duration_secs: int,
    limit: int,
) -> tuple[float, int]:
    """Concurrent throughput phase.  Returns (QPS, error_count)."""
    print(
        f"  [{system_name}] Throughput: {concurrency} threads for {duration_secs}s..."
    )

    lock = threading.Lock()
    completed = 0
    errors = 0
    deadline = time.monotonic() + duration_secs

    def worker():
        nonlocal completed, errors
        local_ok = 0
        local_err = 0
        # Each thread keeps its own session for connection reuse (keep-alive)
        # without cross-thread socket sharing.
        with requests.Session() as session:
            bound = lambda q: search_fn(q, limit=limit, session=session)
            while time.monotonic() < deadline:
                q = random.choice(queries)
                try:
                    bound(q["query"])
                    local_ok += 1
                except Exception:
                    local_err += 1
        with lock:
            completed += local_ok
            errors += local_err

    t0 = time.monotonic()
    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        for f in as_completed([pool.submit(worker) for _ in range(concurrency)]):
            f.result()

    elapsed = time.monotonic() - t0
    qps = completed / elapsed if elapsed > 0 else 0.0
    print(
        f"    {completed:,} queries in {elapsed:.1f}s ({qps:,.0f} QPS, {errors} errors)"
    )
    return qps, errors


# =========================================================================
# Reporting
# =========================================================================


def _print_report(results: list[SearchResult]) -> None:
    print("\n" + "=" * 72)
    print("BENCHMARK RESULTS")
    print("=" * 72)
    print(
        f"{'System':<20} {'p50':>8} {'p95':>8} {'p99':>8} "
        f"{'Mean':>8} {'Lat.Err':>8} {'Tput.Err':>9} {'QPS':>10}"
    )
    print("-" * 80)
    for r in results:
        print(
            f"{r.system:<20} "
            f"{r.p50:>7.1f}ms "
            f"{r.p95:>7.1f}ms "
            f"{r.p99:>7.1f}ms "
            f"{r.mean:>7.1f}ms "
            f"{r.latency_errors:>8} "
            f"{r.throughput_errors:>9} "
            f"{r.throughput_qps:>9.0f}"
        )
    print("-" * 80)

    if len(results) == 2:
        a, b = results
        if a.p50 > 0 and b.p50 > 0:
            ratio = a.p50 / b.p50
            faster = b.system if ratio > 1 else a.system
            print(f"\np50 comparison: {faster} is {max(ratio, 1 / ratio):.1f}x faster")
        if a.throughput_qps > 0 and b.throughput_qps > 0:
            ratio = a.throughput_qps / b.throughput_qps
            higher = a.system if ratio > 1 else b.system
            print(
                f"QPS comparison: {higher} has {max(ratio, 1 / ratio):.1f}x higher throughput"
            )
    print()


def _dump(results: list[SearchResult], output: Path) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    # One file may contain multiple systems (e.g. when both es_url and
    # tantylla_url are given).  We write a list of flat objects so DuckDB
    # can unnest them with a simple read_json + unnest rather than needing
    # to know the dynamic top-level key.
    data = [
        {
            "stack": r.stack,
            "system": r.system,
            "latencies_ms": r.latencies_ms,
            "latency_errors": r.latency_errors,
            "throughput_errors": r.throughput_errors,
            "total_hits": r.total_hits,
            "p50": r.p50,
            "p95": r.p95,
            "p99": r.p99,
            "mean": r.mean,
            "qps": r.throughput_qps,
        }
        for r in results
    ]
    with open(output, "w") as fh:
        json.dump(data, fh, indent=2)
    print(f"Search results written to {output}")


# =========================================================================
# Public entry point
# =========================================================================


def run(
    tantylla_url: Optional[str] = None,
    es_url: Optional[str] = None,
    output: Path = Path("data/output/benchmark-results.json"),
    query_count: int = 1000,
    concurrency: int = 10,
    throughput_duration: int = 30,
    limit: int = 10,
    warmup: int = 50,
    seed: int = 42,
    # Stack labels written into the output file so SQL queries can group
    # results without needing to parse the filename.
    tantylla_stack: str = "tantylla-stack",
    es_stack: str = "elasticsearch-stack",
) -> None:
    """Run the search latency + throughput benchmark and write results to `output`.

    At least one of `tantylla_url` or `es_url` must be provided.
    """
    if not tantylla_url and not es_url:
        raise ValueError("At least one of tantylla_url or es_url must be provided.")

    random.seed(seed)
    queries = generate_query_mix(query_count)

    print(f"Generated {len(queries)} queries (seed={seed})")
    print(f"  Single-term: {sum(1 for q in queries if q['type'] == 'single-term')}")
    print(f"  Multi-term:  {sum(1 for q in queries if q['type'] == 'multi-term')}")
    print(f"  Phrase:      {sum(1 for q in queries if q['type'] == 'phrase')}")
    print(f"  Brand:       {sum(1 for q in queries if q['type'] == 'brand')}")
    print()

    results: list[SearchResult] = []

    if tantylla_url:
        print(f"Benchmarking Tantylla at {tantylla_url}")
        search_fn = lambda q, limit=limit, session=None: _search_tantylla(
            tantylla_url, q, limit, session
        )
        lat = _run_latency("Tantylla", tantylla_stack, search_fn, queries, warmup, limit)
        qps, tput_err = _run_throughput(
            "Tantylla", search_fn, queries, concurrency, throughput_duration, limit
        )
        lat.throughput_qps = qps
        lat.throughput_errors = tput_err
        results.append(lat)
        print()

    if es_url:
        print(f"Benchmarking Elasticsearch at {es_url}")
        search_fn = lambda q, limit=limit, session=None: _search_elasticsearch(
            es_url, q, limit, session
        )
        lat = _run_latency("Elasticsearch", es_stack, search_fn, queries, warmup, limit)
        qps, tput_err = _run_throughput(
            "Elasticsearch", search_fn, queries, concurrency, throughput_duration, limit
        )
        lat.throughput_qps = qps
        lat.throughput_errors = tput_err
        results.append(lat)
        print()

    _print_report(results)
    _dump(results, output)
