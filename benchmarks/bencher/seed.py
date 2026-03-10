"""seed.py: Insert synthetic product data into ScyllaDB benchmark.products.

Replaces scripts/seed-data.py. All data-generation logic lives in datagen.py;
this module only owns the CQL insertion loop.
"""

import time

from cassandra.cluster import Cluster
from cassandra.query import BatchStatement, ConsistencyLevel

from bencher.datagen import generate_product

# Upper bound on CQL batch size.  Cassandra/ScyllaDB will raise a server-side
# error for batches that exceed the configured `batch_size_warn_threshold_in_kb`
# (default 5 KB) or `batch_size_fail_threshold_in_kb` (default 50 KB).  50
# rows per batch is well within the safe zone for our product schema.
_DEFAULT_BATCH_SIZE = 50


def run(
    host: str = "localhost",
    port: int = 9043,
    count: int = 100_000,
    batch_size: int = _DEFAULT_BATCH_SIZE,
) -> None:
    """Insert `count` products into benchmark.products using CQL batch writes.

    Progress is reported to stdout every 5 seconds so the user can see that
    the seeding step is making forward progress during long runs.
    """
    cluster = Cluster([host], port=port)
    session = cluster.connect("benchmark")

    # Prepare once — reused for every row in every batch.
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

    inserted = 0
    t0 = time.monotonic()
    last_report = t0

    print(f"Seeding {count:,} products into benchmark.products ...")

    while inserted < count:
        batch = BatchStatement(consistency_level=ConsistencyLevel.ONE)
        batch_count = min(batch_size, count - inserted)

        for _ in range(batch_count):
            p = generate_product()
            batch.add(
                insert_cql,
                (
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
                ),
            )

        session.execute(batch)
        inserted += batch_count

        # Report every 5 seconds, and once at completion.
        now = time.monotonic()
        if now - last_report >= 5.0 or inserted >= count:
            elapsed = now - t0
            rate = inserted / elapsed if elapsed > 0 else 0
            pct = 100.0 * inserted / count
            print(f"  [{pct:5.1f}%] {inserted:>8,} / {count:,}  ({rate:,.0f} docs/sec)")
            last_report = now

    elapsed = time.monotonic() - t0
    rate = count / elapsed if elapsed > 0 else 0
    print(
        f"\nDone. Inserted {count:,} products in {elapsed:.1f}s ({rate:,.0f} docs/sec)"
    )
    cluster.shutdown()
