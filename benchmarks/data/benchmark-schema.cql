-- =========================================================================
-- Benchmark Schema: E-Commerce Product Catalog
-- =========================================================================
--
-- This schema provides a realistic FTS benchmark target with rich text
-- fields (name, description, brand) and collection columns (tags).
--
-- CDC is enabled on all tables so both tantylla and the competitor
-- pipeline can consume change events. TABLETS must be disabled because
-- ScyllaDB CDC requires vnode-based replication to generate log entries.
--
-- SimpleStrategy with RF=1 is appropriate for single-node local
-- benchmarks. Production would use NetworkTopologyStrategy.
--
-- No default TTL is set — benchmark data persists until explicit
-- deletion or `tofu destroy` cleans the volumes.

CREATE KEYSPACE IF NOT EXISTS benchmark
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
AND TABLETS = {'enabled': false};

USE benchmark;

-- Primary search target: product catalog.
--
-- The name, description, brand, and category fields are the main
-- targets for full-text search queries. In tantylla, all columns are
-- serialised into a single JSON document field indexed with Tantivy's
-- en_stem tokenizer. In Elasticsearch, each field gets its own mapping
-- with the English analyzer (configured by setup-connectors.sh).
--
-- The tags set demonstrates collection CDC delta handling, which is
-- one of tantylla's more complex features.
CREATE TABLE IF NOT EXISTS products (
    product_id      uuid,
    name            text,
    description     text,
    brand           text,
    category        text,
    subcategory     text,
    tags            set<text>,
    attributes      map<text, text>,
    price           decimal,
    stock_quantity  int,
    rating_avg      float,
    review_count    int,
    created_at      timestamp,
    updated_at      timestamp,
    PRIMARY KEY (product_id)
) WITH cdc = {'enabled': true};

-- Secondary search target: product reviews.
--
-- Composite primary key (product_id, review_id) tests tantylla's
-- partition-key-aware indexing and multi-row partition handling.
-- Clustering by review_id DESC gives recent-first ordering in
-- ScyllaDB range scans.
--
-- TODO: seed-data.py and run-benchmark.py do not yet use this table.
-- It is defined here for future benchmarks of composite-PK indexing
-- and multi-row partition search.
CREATE TABLE IF NOT EXISTS reviews (
    product_id      uuid,
    review_id       timeuuid,
    reviewer_name   text,
    title           text,
    body            text,
    rating          int,
    helpful_count   int,
    created_at      timestamp,
    PRIMARY KEY (product_id, review_id)
) WITH CLUSTERING ORDER BY (review_id DESC)
AND cdc = {'enabled': true};
