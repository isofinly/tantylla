"""datagen.py: Synthetic product generation shared across seed and ingest modules.

A single source of truth for the data model so both modules produce identical
schemas and the search benchmark's brand/term queries always find matches.
"""

import random
import uuid
from datetime import datetime, timedelta, timezone

# =========================================================================
# Corpus constants
# =========================================================================

# 20 real brand names used by run-benchmark.py's brand-scoped queries.
# Any change here must be mirrored in bench/search.py BRAND_QUERIES.
BRANDS: list[str] = [
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

CATEGORIES: dict[str, list[str]] = {
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

ADJECTIVES: list[str] = [
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

FEATURES: list[str] = [
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

MATERIALS: list[str] = [
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

TAG_POOL: list[str] = [
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

_ATTRIBUTE_KEYS: list[str] = [
    "color",
    "weight_grams",
    "dimensions_cm",
    "warranty_months",
    "country_of_origin",
    "material",
    "power_source",
    "connectivity",
]


# =========================================================================
# Generation
# =========================================================================


def generate_product(brand: str | None = None) -> dict:
    """Generate one synthetic product record.

    When `brand` is supplied it overrides the random brand and is embedded
    in the product name — used by the ingest benchmark to mark a batch with
    a unique sentinel value it can poll for in the search engine.
    """
    category = random.choice(list(CATEGORIES.keys()))
    subcategory = random.choice(CATEGORIES[category])
    adj = random.choice(ADJECTIVES)
    effective_brand = brand if brand is not None else random.choice(BRANDS)
    name = f"{effective_brand} {adj.title()} {subcategory}"

    now = datetime.now(timezone.utc)
    created = now - timedelta(days=random.randint(1, 365))

    # Build a 3–5 sentence description by sampling from a pool of sentence
    # templates; this gives varied lengths while keeping FTS content rich.
    sentence_pool = [
        f"The {name} is a {adj} product in our {category} collection.",
        f"Crafted from high-quality {random.choice(MATERIALS)}, it delivers exceptional performance.",
        f"This product comes {random.choice(FEATURES)}.",
        f"Additionally, it is {random.choice(FEATURES)}.",
        f"Perfect for everyday use, it combines style with functionality.",
        f"Backed by our satisfaction guarantee and responsive customer support.",
    ]
    description = " ".join(random.sample(sentence_pool, k=random.randint(3, 5)))

    attributes = {
        k: (
            random.choice(MATERIALS)
            if k == "material"
            else str(random.randint(100, 5000))
            if k == "weight_grams"
            else str(random.randint(1, 36))
            if k == "warranty_months"
            else random.choice(["US", "DE", "JP", "CN", "KR", "GB"])
        )
        for k in random.sample(_ATTRIBUTE_KEYS, k=random.randint(2, 5))
    }

    return {
        "product_id": uuid.uuid4(),
        "name": name,
        "description": description,
        "brand": effective_brand,
        "category": category,
        "subcategory": subcategory,
        "tags": set(random.sample(TAG_POOL, k=random.randint(1, 5))),
        "attributes": attributes,
        "price": round(random.uniform(4.99, 999.99), 2),
        "stock_quantity": random.randint(0, 10000),
        "rating_avg": round(random.uniform(1.0, 5.0), 1),
        "review_count": random.randint(0, 5000),
        "created_at": created,
        "updated_at": now,
    }
