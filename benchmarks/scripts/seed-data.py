#!/usr/bin/env python3
"""
seed-data.py: Insert realistic e-commerce product data into ScyllaDB.

This script generates synthetic product catalog entries with realistic
text fields (names, descriptions, brands) suitable for full-text search
benchmarking. The data is inserted into the benchmark.products table
which has CDC enabled, so both tantylla and the competitor pipeline
will receive change events.

Usage:
    python scripts/seed-data.py --host localhost --port 9043 --count 100000

Dependencies:
    pip install cassandra-driver
"""

import argparse
import random
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone

try:
    from cassandra.cluster import Cluster
    from cassandra.query import BatchStatement, ConsistencyLevel, SimpleStatement
except ImportError:
    print(
        "ERROR: cassandra-driver is required.\n"
        "Install it with: pip install cassandra-driver",
        file=sys.stderr,
    )
    sys.exit(1)

BRANDS = [
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
    "Cascade",
    "Meridian",
    "Solaris",
    "Titanium",
    "Vertex",
    "Aurora",
    "Helios",
    "Nimbus",
    "Orion",
    "Phoenix",
]

CATEGORIES = {
    "Electronics": [
        "Headphones",
        "Speakers",
        "Chargers",
        "Cables",
        "Adapters",
        "Keyboards",
        "Mice",
        "Monitors",
        "Webcams",
        "Microphones",
    ],
    "Home & Kitchen": [
        "Cookware",
        "Utensils",
        "Storage",
        "Lighting",
        "Cleaning",
        "Organizers",
        "Appliances",
        "Textiles",
        "Decor",
        "Furniture",
    ],
    "Sports & Outdoors": [
        "Fitness",
        "Camping",
        "Cycling",
        "Running",
        "Swimming",
        "Hiking",
        "Climbing",
        "Yoga",
        "Training",
        "Recovery",
    ],
    "Books & Media": [
        "Fiction",
        "Non-Fiction",
        "Technical",
        "Reference",
        "Audio",
        "Educational",
        "Science",
        "History",
        "Biography",
        "Philosophy",
    ],
    "Clothing & Accessories": [
        "Shirts",
        "Pants",
        "Jackets",
        "Shoes",
        "Hats",
        "Bags",
        "Watches",
        "Belts",
        "Scarves",
        "Gloves",
    ],
}

ADJECTIVES = [
    "premium",
    "ultra",
    "wireless",
    "portable",
    "compact",
    "ergonomic",
    "lightweight",
    "durable",
    "professional",
    "advanced",
    "high-performance",
    "noise-cancelling",
    "waterproof",
    "rechargeable",
    "foldable",
    "adjustable",
    "breathable",
    "insulated",
    "magnetic",
    "bluetooth",
    "stainless",
    "organic",
    "eco-friendly",
    "heavy-duty",
    "slim",
]

FEATURES = [
    "with quick-charge technology",
    "featuring active noise cancellation",
    "built for all-day comfort",
    "designed for professionals",
    "with extended battery life",
    "featuring anti-slip grip",
    "with temperature control",
    "optimized for daily use",
    "with smart connectivity",
    "featuring precision engineering",
    "with impact-resistant construction",
    "designed for outdoor adventures",
    "with memory foam cushioning",
    "featuring LED indicators",
    "with one-touch operation",
    "with USB-C fast charging",
    "with multi-device pairing",
    "featuring titanium frame",
    "with ambient awareness mode",
    "built with recycled materials",
]

MATERIALS = [
    "aluminum",
    "carbon fiber",
    "bamboo",
    "silicone",
    "leather",
    "nylon",
    "polyester",
    "stainless steel",
    "copper",
    "ceramic",
    "titanium",
    "polycarbonate",
    "rubber",
    "cotton",
    "mesh",
]

TAG_POOL = [
    "bestseller",
    "new-arrival",
    "sale",
    "limited-edition",
    "eco-friendly",
    "award-winning",
    "trending",
    "staff-pick",
    "value-pack",
    "clearance",
    "premium",
    "budget-friendly",
    "handcrafted",
    "imported",
    "refurbished",
    "gift-idea",
    "seasonal",
    "exclusive",
    "bundle",
    "subscription",
]

ATTRIBUTE_KEYS = [
    "color",
    "weight_grams",
    "dimensions_cm",
    "warranty_months",
    "country_of_origin",
    "material",
    "power_source",
    "connectivity",
]


def generate_product_name(category: str, subcategory: str) -> str:
    """Generate a realistic product name."""
    adj = random.choice(ADJECTIVES)
    brand = random.choice(BRANDS)
    return f"{brand} {adj.title()} {subcategory}"


def generate_description(name: str, category: str) -> str:
    """Generate a multi-sentence product description with good FTS content."""
    material = random.choice(MATERIALS)
    feat1 = random.choice(FEATURES)
    feat2 = random.choice(FEATURES)
    adj = random.choice(ADJECTIVES)

    sentences = [
        f"The {name} is a {adj} product in our {category} collection.",
        f"Crafted from high-quality {material}, it delivers exceptional performance.",
        f"This product comes {feat1}.",
        f"Additionally, it is {feat2}.",
        f"Perfect for everyday use, it combines style with functionality.",
        f"Backed by our satisfaction guarantee and responsive customer support.",
    ]
    # Use 3-5 sentences for varied description lengths.
    return " ".join(random.sample(sentences, k=random.randint(3, 5)))


def generate_product() -> dict:
    """Generate a single product record."""
    category = random.choice(list(CATEGORIES.keys()))
    subcategory = random.choice(CATEGORIES[category])
    name = generate_product_name(category, subcategory)

    now = datetime.now(timezone.utc)
    created = now - timedelta(days=random.randint(1, 365))

    tags = set(random.sample(TAG_POOL, k=random.randint(1, 5)))
    attributes = {
        k: random.choice(MATERIALS)
        if k == "material"
        else str(random.randint(100, 5000))
        if k == "weight_grams"
        else str(random.randint(1, 36))
        if k == "warranty_months"
        else random.choice(["US", "DE", "JP", "CN", "KR", "GB"])
        for k in random.sample(ATTRIBUTE_KEYS, k=random.randint(2, 5))
    }

    return {
        "product_id": uuid.uuid4(),
        "name": name,
        "description": generate_description(name, category),
        "brand": random.choice(BRANDS),
        "category": category,
        "subcategory": subcategory,
        "tags": tags,
        "attributes": attributes,
        "price": round(random.uniform(4.99, 999.99), 2),
        "stock_quantity": random.randint(0, 10000),
        "rating_avg": round(random.uniform(1.0, 5.0), 1),
        "review_count": random.randint(0, 5000),
        "created_at": created,
        "updated_at": now,
    }


def seed(host: str, port: int, count: int, batch_size: int = 50) -> None:
    """Insert `count` products into ScyllaDB in batches."""
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

        # Progress report every 5 seconds.
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

    # Also export the search query terms for the benchmark runner.
    # We sample product names from what we inserted so queries have
    # guaranteed matches.
    cluster.shutdown()


def main():
    parser = argparse.ArgumentParser(
        description="Seed benchmark data into ScyllaDB benchmark.products"
    )
    parser.add_argument("--host", default="localhost", help="ScyllaDB host")
    parser.add_argument("--port", type=int, default=9043, help="ScyllaDB CQL port")
    parser.add_argument(
        "--count",
        type=int,
        default=100_000,
        help="Number of product documents to insert",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=50,
        help="CQL batch size (too large may cause timeouts)",
    )
    args = parser.parse_args()

    seed(args.host, args.port, args.count, args.batch_size)


if __name__ == "__main__":
    main()
