"""ingest.py: Peak CDC ingestion throughput benchmark.

Replaces scripts/measure-ingest-speed.py.

The benchmark works in two concurrent phases:
  1. Inserter — blasts `count` rows into ScyllaDB as fast as possible using
     execute_concurrent_with_args (async in-flight requests without
     cross-partition batching, which Cassandra/ScyllaDB penalises heavily).
  2. Pollers — one per target system (Tantylla / Elasticsearch), each
     polling the search API in a tight loop until the sentinel brand is
     fully visible, capturing (elapsed, visible_count) samples along the way.

Because CDC events are strictly ordered, once the sentinel batch is fully
visible in the search system every preceding document (i.e., from seed.py)
is also guaranteed to be indexed.
"""

import json
import random
import string
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import requests
from cassandra.cluster import Cluster
from cassandra.concurrent import execute_concurrent_with_args
from cassandra.query import ConsistencyLevel

from bencher.datagen import generate_product

# =========================================================================
# Result dataclass
# =========================================================================


@dataclass
class IngestResult:
    """Collected metrics for one search system's indexing pipeline drain."""

    system: str
    stack: str
    doc_count: int
    # Wall-clock seconds from t0 (start of ScyllaDB insertion) until the
    # system reported doc_count visible documents.
    total_secs: float = 0.0
    # (elapsed_secs, visible_count) samples collected during polling.
    samples: list[tuple[float, int]] = field(default_factory=list)

    @property
    def end_to_end_rate(self) -> float:
        """Average docs/sec over the full observation window."""
        return self.doc_count / self.total_secs if self.total_secs > 0 else 0.0

    def peak_rate(self, window_secs: float = 15.0) -> float:
        """Maximum sustained docs/sec over any `window_secs` sliding window.

        If the total active indexing time is shorter than `window_secs`, the
        rate over the entire active window is returned instead.
        """
        if not self.samples or self.samples[-1][1] == 0:
            return 0.0

        first_visible_idx = next(
            (i for i, s in enumerate(self.samples) if s[1] > 0), -1
        )
        if first_visible_idx == -1:
            return 0.0

        t_start = self.samples[first_visible_idx][0]
        t_finish = self.samples[-1][0]
        active_duration = t_finish - t_start

        if 0 < active_duration <= window_secs:
            delta = self.samples[-1][1] - self.samples[first_visible_idx][1]
            return delta / active_duration

        max_rate = 0.0
        for i in range(first_visible_idx, len(self.samples)):
            t_i, c_i = self.samples[i]
            for j in range(i + 1, len(self.samples)):
                t_j, c_j = self.samples[j]
                dt = t_j - t_i
                if dt >= window_secs:
                    max_rate = max(max_rate, (c_j - c_i) / dt)
                    break
        return max_rate


# =========================================================================
# Search-engine polling helpers
# =========================================================================


def _count_tantylla(url: str, batch_marker: str, session: requests.Session) -> int:
    # Consistency 0 (ANY) is the fastest read path — matches standard ES
    # behaviour so the comparison is fair.
    resp = session.post(
        f"{url}/api/v1/search",
        json={
            "query": f'document.brand:"{batch_marker}"',
            "limit": 1,
            "offset": 0,
            "consistency": 0,
        },
        timeout=10,
    )
    resp.raise_for_status()
    return int(resp.json().get("total_hits", 0))


def _count_elasticsearch(url: str, batch_marker: str, session: requests.Session) -> int:
    # The CDC connector wraps column values in an `after.<col>.value` envelope.
    # We use the `.keyword` sub-field for an exact-term count so the English
    # analyser does not tokenise the sentinel brand name.
    resp = session.post(
        f"{url}/scylla.benchmark.products/_count",
        json={"query": {"term": {"after.brand.value.keyword": batch_marker}}},
        timeout=10,
    )
    resp.raise_for_status()
    return int(resp.json().get("count", 0))


# =========================================================================
# Core benchmark logic
# =========================================================================


def _insert_to_scylla(
    host: str,
    port: int,
    count: int,
    batch_marker: str,
    abort: threading.Event,
    concurrency: int = 100,
) -> float:
    """Blast `count` rows with sentinel brand into ScyllaDB asynchronously.

    Returns the wall-clock seconds taken to complete all inserts.
    Raises RuntimeError if any insert fails or `abort` is set mid-run.
    """
    cluster = Cluster([host], port=port)
    session = cluster.connect("benchmark")
    insert_cql = session.prepare(
        """
        INSERT INTO products (
            product_id, name, description, brand, category, subcategory,
            tags, attributes, price, stock_quantity, rating_avg,
            review_count, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
    )
    insert_cql.consistency_level = ConsistencyLevel.ONE

    def _params():
        for _ in range(count):
            p = generate_product(brand=batch_marker)
            yield (
                p["product_id"],
                p["name"],
                p["description"],
                p["brand"],
                p["category"],
                p["subcategory"],
                p["tags"],
                p["attributes"],
                p["price"],
                p["stock_quantity"],
                p["rating_avg"],
                p["review_count"],
                p["created_at"],
                p["updated_at"],
            )

    t0 = time.monotonic()
    results_gen = execute_concurrent_with_args(
        session,
        insert_cql,
        _params(),
        concurrency=concurrency,
        raise_on_first_error=False,
        results_generator=True,
    )

    completed = 0
    last_report = t0

    for success, exc in results_gen:
        if abort.is_set():
            break
        if success:
            completed += 1
        else:
            print(f"\n[ScyllaDB] Write error: {exc}", file=sys.stderr)
            abort.set()
            break

        now = time.monotonic()
        if now - last_report >= 5.0:
            elapsed = now - t0
            rate = completed / elapsed if elapsed > 0 else 0
            print(
                f"  [{100.0 * completed / count:5.1f}%] "
                f"{completed:>8,} / {count:,}  ({rate:,.0f} rows/sec)"
            )
            last_report = now

    elapsed = time.monotonic() - t0
    cluster.shutdown()

    if abort.is_set() or completed < count:
        raise RuntimeError("ScyllaDB insertion aborted or incomplete.")

    rate = completed / elapsed if elapsed > 0 else 0
    print(f"  [{100.0:5.1f}%] {completed:>8,} / {count:,}  ({rate:,.0f} rows/sec)")
    return elapsed


def _poll_until_visible(
    system_name: str,
    stack: str,
    count_fn,
    target: int,
    t0: float,
    abort: threading.Event,
    poll_interval: float = 0.5,
    timeout_secs: float = 600.0,
) -> IngestResult:
    """Poll `count_fn` until it returns >= `target`, collecting time-series samples."""
    result = IngestResult(system=system_name, stack=stack, doc_count=target)
    deadline = time.monotonic() + timeout_secs
    last_print = time.monotonic()

    while time.monotonic() < deadline and not abort.is_set():
        try:
            visible = count_fn()
        except Exception as exc:
            print(f"    [{system_name}] poll error: {exc}")
            time.sleep(poll_interval)
            continue

        elapsed = time.monotonic() - t0
        result.samples.append((elapsed, visible))

        if time.monotonic() - last_print >= 5.0:
            print(f"    [{system_name}] {visible:,} / {target:,} docs visible")
            last_print = time.monotonic()

        if visible >= target:
            print(
                f"    [{system_name}] COMPLETE: {visible:,} / {target:,} docs visible"
            )
            result.total_secs = elapsed
            return result

        # Slow the poll rate slightly once we're well within 5% of target to
        # avoid hammering the API right at the finish line.
        time.sleep(poll_interval * 0.5 if visible / target > 0.95 else poll_interval)

    if abort.is_set():
        print(f"[{system_name}] Polling aborted.")
        return result

    result.total_secs = time.monotonic() - t0
    last_visible = result.samples[-1][1] if result.samples else 0
    print(
        f"    [{system_name}] TIMEOUT after {timeout_secs:.0f}s: "
        f"{last_visible}/{target} docs visible"
    )
    return result


# =========================================================================
# Public entry point
# =========================================================================


def run(
    host: str = "localhost",
    port: int = 9043,
    count: int = 100_000,
    tantylla_url: Optional[str] = None,
    es_url: Optional[str] = None,
    output: Path = Path("data/output/ingest-results.json"),
    concurrency: int = 100,
    poll_interval: float = 0.5,
    window_secs: float = 15.0,
    timeout_secs: float = 600.0,
    seed: int = 42,
    # Stack labels written into output so SQL can GROUP BY without filename parsing.
    tantylla_stack: str = "tantylla-stack",
    es_stack: str = "elasticsearch-stack",
) -> None:
    """Run the peak ingestion benchmark and write results to `output`.

    At least one of `tantylla_url` or `es_url` must be provided.
    """
    if not tantylla_url and not es_url:
        raise ValueError("At least one of tantylla_url or es_url must be provided.")

    random.seed(seed)
    # The sentinel brand is unique per run so multiple concurrent benchmark
    # processes on the same cluster do not interfere with each other.
    batch_marker = "ingest" + "".join(random.choices(string.ascii_lowercase, k=8))
    print(f"Peak Ingestion Benchmark (batch_marker={batch_marker})")

    abort = threading.Event()
    t0 = time.monotonic()

    pool = ThreadPoolExecutor(max_workers=2)
    poll_futures = []

    try:
        if tantylla_url:
            sess = requests.Session()
            poll_futures.append(
                pool.submit(
                    _poll_until_visible,
                    "Tantylla",
                    tantylla_stack,
                    lambda s=sess: _count_tantylla(tantylla_url, batch_marker, s),
                    count,
                    t0,
                    abort,
                    poll_interval,
                    timeout_secs,
                )
            )

        if es_url:
            sess = requests.Session()
            poll_futures.append(
                pool.submit(
                    _poll_until_visible,
                    "Elasticsearch",
                    es_stack,
                    lambda s=sess: _count_elasticsearch(es_url, batch_marker, s),
                    count,
                    t0,
                    abort,
                    poll_interval,
                    timeout_secs,
                )
            )

        print(f"  Blasting {count:,} docs into ScyllaDB (marker={batch_marker})...")
        scylla_write_secs = _insert_to_scylla(
            host, port, count, batch_marker, abort, concurrency=concurrency
        )
        print(
            f"  ScyllaDB backlog built in {scylla_write_secs:.1f}s. "
            "Waiting for pipelines to drain..."
        )

        bulk_results: list[IngestResult] = [
            f.result() for f in as_completed(poll_futures)
        ]

    except Exception as exc:
        abort.set()
        print(f"\nFATAL: {exc}", file=sys.stderr)
        pool.shutdown(wait=False)
        raise
    finally:
        pool.shutdown(wait=True)

    _print_report(scylla_write_secs, bulk_results, count, window_secs)
    _dump(scylla_write_secs, bulk_results, count, window_secs, output)


# =========================================================================
# Reporting
# =========================================================================


def _print_report(
    scylla_write_secs: float,
    results: list[IngestResult],
    doc_count: int,
    window_secs: float,
) -> None:
    print("\n" + "=" * 80)
    print("PEAK INGESTION THROUGHPUT RESULTS")
    print("=" * 80)
    print(
        f"\nScyllaDB write: {doc_count:,} docs in {scylla_write_secs:.1f}s "
        f"({doc_count / scylla_write_secs:,.0f} docs/sec)"
    )

    if results:
        print(
            f"\n{'System':<16} {'Time (s)':>10} {'Peak Docs/sec':>16} "
            f"{'Avg Docs/sec':>14} {'Status':>10}"
        )
        print("-" * 70)
        for r in results:
            last_visible = r.samples[-1][1] if r.samples else 0
            status = "OK" if last_visible >= r.doc_count else "PARTIAL"
            print(
                f"{r.system:<16} {r.total_secs:>9.1f}s "
                f"{r.peak_rate(window_secs):>16,.0f} "
                f"{r.end_to_end_rate:>14,.0f} {status:>10}"
            )
            if r.total_secs > scylla_write_secs:
                lag = r.total_secs - scylla_write_secs
                print(
                    f"  {'':16} "
                    f"(ScyllaDB write: {scylla_write_secs:.1f}s | "
                    f"Indexing tail: {lag:.1f}s)"
                )

    if len(results) == 2:
        a, b = results
        pa, pb = a.peak_rate(window_secs), b.peak_rate(window_secs)
        if pa > 0 and pb > 0:
            ratio = pa / pb
            faster = a.system if ratio > 1 else b.system
            print(
                f"\nPeak throughput: {faster} sustained "
                f"{max(ratio, 1 / ratio):.1f}x higher docs/sec"
            )
    print()


def _dump(
    scylla_write_secs: float,
    results: list[IngestResult],
    doc_count: int,
    window_secs: float,
    output: Path,
) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    # `results` is an array of objects (not a dict keyed by system name) so
    # DuckDB can unnest it generically without knowing the system names in advance.
    data: dict = {
        "benchmark": "peak_ingestion",
        "doc_count": doc_count,
        "scylla_write_secs": round(scylla_write_secs, 2),
        "scylla_write_docs_per_sec": (
            round(doc_count / scylla_write_secs, 1) if scylla_write_secs > 0 else 0
        ),
        "sliding_window_secs": window_secs,
        "results": [
            {
                "system": r.system,
                "stack": r.stack,
                "total_secs": round(r.total_secs, 2),
                "peak_docs_per_sec": round(r.peak_rate(window_secs), 1),
                "avg_docs_per_sec": round(r.end_to_end_rate, 1),
                "samples": [
                    {"elapsed_secs": round(t, 2), "visible_count": c}
                    for t, c in r.samples
                ],
            }
            for r in results
        ],
    }
    with open(output, "w") as fh:
        json.dump(data, fh, indent=2)
    print(f"Ingest results written to {output}")
