## Native Tantivy FTS Integration into ScyllaDB — Updated Plan (v2)

### 1. Goals and Constraints

**Goal:** Embed Tantivy full-text search as a native custom secondary index inside ScyllaDB, with schema-inferred typed fields instead of a single JSON catch-all field.

**Constraints:**

- No external services, no sidecar, no HTTP calls
- No visible side-effects beyond index availability
- CQL-native query syntax (`MATCH` operator)
- Must work with Seastar's thread-per-core cooperative model
- Must preserve all FTS features from Tantylla (keyword, phrase, prefix, fuzzy, numeric range, facets, boosted multi-field, TTL, collection indexing)

**Key change from v1:** The single `document` JSON field is replaced by **per-column typed Tantivy fields** inferred from the ScyllaDB table schema at `CREATE INDEX` time. This eliminates 14 of 16 known pain points in the current implementation.

**Reference implementations in ScyllaDB:**

- `wasmtime_bindings` — Rust FFI via cxx bridge (`scylladb/rust/wasmtime_bindings/`)
- `vector_index` — Only existing custom index (`scylladb/index/vector_index.cc`)
- `vector_indexed_table_select_statement` — Custom query path (`scylladb/cql3/statements/select_statement.cc:363`)

---

### 2. Architecture Options (Write Path)

#### In-Process CDC Consumer (Asynchronous)

```
CQL INSERT/UPDATE/DELETE
  → database::apply(mutation)
  → commitlog + CDC log entry (CDC implicitly enabled)
  → ack

BACKGROUND (per-shard):
  fts_cdc_consumer polls CDC log
  → extracts typed column values from CDC rows
  → calls fts::upsert_document() on alien thread
  → commits periodically
```

**Pros:** Follows `vector_index` pattern, no write latency impact, CDC provides durability.
**Cons:** Eventual consistency (~5s window), CDC write amplification, CDC log storage.

Both options share identical Rust bindings, C++ custom index class, read path, and lifecycle management.

---

### 3. Phase 1: Rust FTS Bindings with Schema-Inferred Fields

**New crate:** `scylladb/rust/fts_bindings/`

#### 3.1 Crate Structure

```
scylladb/rust/fts_bindings/
├── Cargo.toml
└── src/
    ├── lib.rs           # cxx::bridge definition
    ├── schema.rs        # CQL→Tantivy field mapping, schema construction
    ├── writer.rs        # Typed document upsert/delete/commit
    ├── reader.rs        # Search, facets, result extraction
    └── types.rs         # FieldMapping, TantivyFieldKind, etc.
```

#### 3.2 Dependencies

```toml
[package]
name = "fts_bindings"
version = "0.1.0"
edition = "2021"

[dependencies]
cxx = { version = "1.0.83", features = ["c++20"] }
tantivy = { version = "0.25.0", features = ["mmap"] }
serde = { version = "1.0", features = ["derive"] }
serde_json = "1.0"
anyhow = "1.0"
```

#### 3.3 Schema Inference: CQL Type → Tantivy Field Mapping

The C++ side (`fts_index::validate()`) has access to `schema.all_columns()` and each `column_definition.type->get_kind()`. It builds a JSON field mapping descriptor and passes it to Rust at index creation time.

**Type mapping table:**

| CQL type          | `abstract_type::kind` | Tantivy field       | Options                     | Notes                                 |
| ----------------- | --------------------- | ------------------- | --------------------------- | ------------------------------------- |
| `text`, `varchar` | `utf8`                | `add_text_field`    | `TEXT \| STORED`            | Tokenized, BM25, per-column tokenizer |
| `ascii`           | `ascii`               | `add_text_field`    | `TEXT \| STORED`            | Same as text                          |
| `int`             | `int32`               | `add_i64_field`     | `INDEXED \| STORED \| FAST` | Widened to i64                        |
| `bigint`          | `long_kind`           | `add_i64_field`     | `INDEXED \| STORED \| FAST` | Native i64                            |
| `smallint`        | `short_kind`          | `add_i64_field`     | `INDEXED \| STORED \| FAST` | Widened to i64                        |
| `tinyint`         | `byte`                | `add_i64_field`     | `INDEXED \| STORED \| FAST` | Widened to i64                        |
| `float`           | `float_kind`          | `add_f64_field`     | `INDEXED \| STORED \| FAST` | Widened to f64                        |
| `double`          | `double_kind`         | `add_f64_field`     | `INDEXED \| STORED \| FAST` | Native f64                            |
| `boolean`         | `boolean`             | `add_bool_field`    | `INDEXED \| STORED \| FAST` |                                       |
| `timestamp`       | `timestamp`           | `add_date_field`    | `INDEXED \| STORED \| FAST` | ms → `DateTime`                       |
| `date`            | `simple_date`         | `add_date_field`    | `INDEXED \| STORED \| FAST` | days → `DateTime`                     |
| `time`            | `time`                | `add_i64_field`     | `INDEXED \| STORED \| FAST` | Nanos since midnight                  |
| `uuid`            | `uuid`                | `add_text_field`    | `STRING \| STORED`          | Exact match, untokenized              |
| `timeuuid`        | `timeuuid`            | `add_text_field`    | `STRING \| STORED`          | Exact match                           |
| `inet`            | `inet`                | `add_ip_addr_field` | `INDEXED \| STORED \| FAST` | Native Tantivy IP                     |
| `blob`            | `bytes`               | `add_bytes_field`   | `STORED`                    | Not searchable                        |
| `decimal`         | `decimal`             | `add_text_field`    | `STRING \| STORED`          | Stored as string repr                 |
| `varint`          | `varint`              | `add_text_field`    | `STRING \| STORED`          | Stored as string repr                 |
| `duration`        | `duration`            | **Skip**            | —                           | ScyllaDB forbids indexing             |
| `counter`         | `counter`             | **Skip**            | —                           | ScyllaDB forbids indexing             |

**Collections:**

| CQL type                   | Strategy                | Tantivy field                                    |
| -------------------------- | ----------------------- | ------------------------------------------------ |
| `set<text>`                | Multi-valued text field | `add_text_field` — add one term per element      |
| `list<text>`               | Multi-valued text field | Same as `set<text>`                              |
| `set<int>` / `list<int>`   | Multi-valued numeric    | `add_i64_field` — add multiple values per doc    |
| `map<text, text>`          | JSON fallback           | `add_json_field` — preserves key-value structure |
| `map<K,V>` (non-text keys) | JSON fallback           | `add_json_field` — keys stringified              |

**UDTs (User-Defined Types):**

Decomposed recursively using `user_type_impl::field_names()` and `field_types()`:

```
UDT contact_info { email: text, phone: text }
→ add_text_field("contact_info.email", TEXT | STORED)
→ add_text_field("contact_info.phone", TEXT | STORED)

UDT address { street: text, city: text, zip: int }
→ add_text_field("address.street", TEXT | STORED)
→ add_text_field("address.city", TEXT | STORED)
→ add_i64_field("address.zip", INDEXED | STORED | FAST)
```

For deeply-nested UDTs (UDT containing UDT), dotted paths continue: `contact.address.city`. A configurable depth limit (default 3) prevents unbounded recursion.

#### 3.4 Field Mapping Descriptor

The C++ side serializes the mapping as a JSON array stored in the index `OPTIONS` map and passed to Rust at index creation:

```json
[
  { "name": "name", "kind": "text", "tokenizer": "default" },
  { "name": "description", "kind": "text", "tokenizer": "en_stem" },
  { "name": "brand", "kind": "text", "tokenizer": "keyword" },
  { "name": "price", "kind": "f64" },
  { "name": "in_stock", "kind": "bool" },
  { "name": "created_at", "kind": "date" },
  { "name": "tags", "kind": "text", "multi_valued": true },
  { "name": "ip_address", "kind": "ip_addr" },
  { "name": "attributes", "kind": "json" },
  { "name": "address.street", "kind": "text", "tokenizer": "default" },
  { "name": "address.city", "kind": "text", "tokenizer": "default" },
  { "name": "address.zip", "kind": "i64" }
]
```

#### 3.5 CXX Bridge Definition

```rust
#[cxx::bridge(namespace = "fts")]
mod ffi {
    // =========================================================================
    // Field mapping descriptor passed from C++ at index creation time.
    // Describes one CQL column → one Tantivy field.
    // =========================================================================
    struct FieldMapping {
        name: String,         // CQL column name (dotted for UDT subfields)
        kind: String,         // "text" | "i64" | "f64" | "bool" | "date"
                              // | "ip_addr" | "bytes" | "json" | "string"
        tokenizer: String,    // For text fields: "default" | "en_stem"
                              // | "keyword" | "whitespace" | ""
        multi_valued: bool,   // true for set<T> / list<T> columns
    }

    // =========================================================================
    // Typed field value passed from C++ for each column in a mutation.
    // Discriminated by `kind` to avoid boxing/dynamic dispatch across FFI.
    // =========================================================================
    struct FieldValue {
        field_name: String,   // Must match a FieldMapping.name
        kind: String,         // "text" | "i64" | "f64" | "bool" | "date_us"
                              // | "ip" | "bytes" | "json" | "null"
        str_val: String,      // Used for text, string, ip, json, date (ISO)
        i64_val: i64,         // Used for i64, bool (0/1), date_us (microseconds)
        f64_val: f64,         // Used for f64
    }

    // =========================================================================
    // Search result types
    // =========================================================================
    struct FtsSearchHit {
        id: String,
        partition_key: String,
        score: f32,
        // No payload_json — the native integration fetches full rows from
        // the base table by PK, so Tantivy only needs to return IDs + scores.
    }

    struct FtsFacetBucket {
        value: String,
        count: u64,
    }

    struct FtsFacetResult {
        field: String,
        buckets: Vec<FtsFacetBucket>,
    }

    struct FtsSearchResponse {
        hits: Vec<FtsSearchHit>,
        total_hits: u64,
        duration_us: u64,
        facets: Vec<FtsFacetResult>,
    }

    // =========================================================================
    // Opaque Rust types
    // =========================================================================
    extern "Rust" {
        type ShardIndex;

        // Lifecycle
        fn create_shard_index(
            path: &str,
            shard_id: u32,
            field_mappings: &[FieldMapping],
        ) -> Result<Box<ShardIndex>>;

        fn open_shard_index(
            path: &str,
            shard_id: u32,
        ) -> Result<Box<ShardIndex>>;

        // Write operations (called from alien thread)
        fn upsert_document(
            index: &mut ShardIndex,
            doc_id: &str,
            partition_key: &str,
            fields: &[FieldValue],
            writetime_us: u64,
            expires_at_us: i64,
        ) -> Result<()>;

        fn delete_document(
            index: &mut ShardIndex,
            doc_id: &str,
        ) -> Result<()>;

        fn delete_by_partition_key(
            index: &mut ShardIndex,
            partition_key: &str,
        ) -> Result<()>;

        fn commit(index: &mut ShardIndex) -> Result<u64>;

        fn prune_expired(index: &mut ShardIndex) -> Result<u64>;

        // Read operations (called from alien thread)
        fn search(
            index: &ShardIndex,
            query: &str,
            limit: u32,
            offset: u32,
            facet_fields: &[String],
            group_by_partition: bool,
        ) -> Result<Box<FtsSearchResponse>>;

        fn list_ids_by_partition_key(
            index: &ShardIndex,
            partition_key: &str,
        ) -> Result<Vec<String>>;

        // Maintenance
        fn doc_count(index: &ShardIndex) -> Result<u64>;
        fn drop_index(index: &mut ShardIndex) -> Result<()>;
    }
}
```

**Key differences from v1 (JSON-based) bridge:**

| Aspect              | v1 (JSON)                            | v2 (Schema-inferred)                             |
| ------------------- | ------------------------------------ | ------------------------------------------------ |
| Schema construction | Hardcoded 5 fields                   | `field_mappings: &[FieldMapping]` from C++       |
| Document ingestion  | `payload_json: &str` (one JSON blob) | `fields: &[FieldValue]` (typed per-column)       |
| Search results      | `payload_json` in each hit           | No payload — base table fetch by PK              |
| Query parsing       | Manual expansion + workarounds       | Native `QueryParser` with correct default fields |
| Boost fields        | Explicit parameter                   | Tantivy query syntax: `name:wireless^2.0`        |

#### 3.6 Rust `ShardIndex` Implementation

```rust
pub struct ShardIndex {
    index: tantivy::Index,
    writer: tantivy::IndexWriter,
    reader: tantivy::IndexReader,
    schema: tantivy::schema::Schema,

    // System fields (always present)
    field_id: Field,
    field_partition_key: Field,
    field_expires_at: Field,
    field_writetime: Field,

    // User fields (from CQL schema inference)
    // Maps CQL column name → (Tantivy Field handle, FieldKind)
    user_fields: HashMap<String, (Field, FieldKind)>,

    // Default text fields for QueryParser (all TEXT-type user fields)
    default_text_fields: Vec<Field>,

    // Uncommitted doc cache for writetime conflict resolution
    uncommitted: HashMap<String, CachedDoc>,
    generation: u64,
}

enum FieldKind {
    Text,     // Tokenized full-text (TEXT | STORED)
    String,   // Untokenized exact match (STRING | STORED)
    I64,      // Integer (INDEXED | STORED | FAST)
    F64,      // Float (INDEXED | STORED | FAST)
    Bool,     // Boolean (INDEXED | STORED | FAST)
    Date,     // DateTime (INDEXED | STORED | FAST)
    IpAddr,   // IP address (INDEXED | STORED | FAST)
    Bytes,    // Binary (STORED)
    Json,     // Fallback for maps/complex (JSON field)
}
```

#### 3.7 Schema Construction (replaces hardcoded JSON field)

```rust
fn create_shard_index(
    path: &str,
    shard_id: u32,
    field_mappings: &[FieldMapping],
) -> Result<Box<ShardIndex>> {
    let mut builder = Schema::builder();

    // ── System fields (always present) ──────────────────────────────
    let field_id = builder.add_text_field("_id", STRING | STORED);
    let field_pk = builder.add_text_field("_partition_key", STRING | STORED);
    let field_expires = builder.add_i64_field("_expires_at", FAST | STORED);
    let field_writetime = builder.add_i64_field("_writetime", FAST | STORED);

    // ── User fields (from CQL schema) ───────────────────────────────
    let mut user_fields = HashMap::new();
    let mut default_text_fields = Vec::new();

    for mapping in field_mappings {
        let (field, kind) = match mapping.kind.as_str() {
            "text" => {
                let tokenizer = if mapping.tokenizer.is_empty() {
                    "default"
                } else {
                    &mapping.tokenizer
                };
                let opts = TextOptions::default()
                    .set_stored()
                    .set_indexing_options(
                        TextFieldIndexing::default()
                            .set_tokenizer(tokenizer)
                            .set_index_option(IndexRecordOption::WithFreqsAndPositions),
                    );
                (builder.add_text_field(&mapping.name, opts), FieldKind::Text)
            }
            "string" => {
                (builder.add_text_field(&mapping.name, STRING | STORED), FieldKind::String)
            }
            "i64" => {
                (builder.add_i64_field(&mapping.name, INDEXED | STORED | FAST), FieldKind::I64)
            }
            "f64" => {
                (builder.add_f64_field(&mapping.name, INDEXED | STORED | FAST), FieldKind::F64)
            }
            "bool" => {
                (builder.add_bool_field(&mapping.name, INDEXED | STORED | FAST), FieldKind::Bool)
            }
            "date" => {
                (builder.add_date_field(&mapping.name, INDEXED | STORED | FAST), FieldKind::Date)
            }
            "ip_addr" => {
                (builder.add_ip_addr_field(&mapping.name, INDEXED | STORED | FAST), FieldKind::IpAddr)
            }
            "bytes" => {
                (builder.add_bytes_field(&mapping.name, STORED), FieldKind::Bytes)
            }
            "json" => {
                let opts = JsonObjectOptions::default()
                    .set_stored()
                    .set_fast(None)
                    .set_indexing_options(
                        TextFieldIndexing::default()
                            .set_tokenizer("default")
                            .set_index_option(IndexRecordOption::WithFreqsAndPositions),
                    );
                (builder.add_json_field(&mapping.name, opts), FieldKind::Json)
            }
            other => anyhow::bail!("Unknown field kind: {}", other),
        };

        if kind == FieldKind::Text {
            default_text_fields.push(field);
        }
        user_fields.insert(mapping.name.clone(), (field, kind));
    }

    let schema = builder.build();
    // ... open/create index, writer, reader ...
}
```

**What this fixes:**

- `QueryParser::for_index(&index, default_text_fields)` — bare queries like `wireless` now search across all text columns correctly (SESSION.md bug fixed)
- Per-column tokenizer: `description` can use `en_stem` for stemming while `brand` uses `keyword` for exact match
- Prefix queries (`wire*`) and fuzzy queries (`wireles~1`) work natively on typed text fields — no `try_build_json_special_query` workaround needed
- Phrase queries (`"noise cancelling"`) expand across default fields automatically
- No array-unwrapping — `doc.get_first::<OwnedValue>(field)` returns a direct value

#### 3.8 Document Upsert (typed fields)

```rust
fn upsert_document(
    index: &mut ShardIndex,
    doc_id: &str,
    partition_key: &str,
    fields: &[FieldValue],
    writetime_us: u64,
    expires_at_us: i64,
) -> Result<()> {
    // Writetime conflict resolution (same as current Tantylla)
    if let Some(existing) = index.find_by_id(doc_id) {
        if existing.writetime >= writetime_us {
            return Ok(()); // skip stale write
        }
    }

    let mut doc = TantivyDocument::new();

    // System fields
    doc.add_text(index.field_id, doc_id);
    doc.add_text(index.field_partition_key, partition_key);
    doc.add_i64(index.field_expires_at, expires_at_us);
    doc.add_i64(index.field_writetime, writetime_us as i64);

    // User fields — typed dispatch, no JSON serialization roundtrip
    for fv in fields {
        let (field, kind) = match index.user_fields.get(&fv.field_name) {
            Some(entry) => entry,
            None => continue, // Column not in index (e.g., added after index creation)
        };

        if fv.kind == "null" {
            continue; // NULL columns are not indexed
        }

        match kind {
            FieldKind::Text | FieldKind::String => {
                doc.add_text(*field, &fv.str_val);
            }
            FieldKind::I64 => {
                doc.add_i64(*field, fv.i64_val);
            }
            FieldKind::F64 => {
                doc.add_f64(*field, fv.f64_val);
            }
            FieldKind::Bool => {
                doc.add_bool(*field, fv.i64_val != 0);
            }
            FieldKind::Date => {
                // i64_val is microseconds since epoch
                let dt = tantivy::DateTime::from_timestamp_micros(fv.i64_val);
                doc.add_date(*field, dt);
            }
            FieldKind::IpAddr => {
                if let Ok(ip) = fv.str_val.parse::<std::net::Ipv6Addr>() {
                    doc.add_ip_addr(*field, ip);
                } else if let Ok(ip) = fv.str_val.parse::<std::net::Ipv4Addr>() {
                    doc.add_ip_addr(*field, ip.to_ipv6_mapped());
                }
            }
            FieldKind::Bytes => {
                doc.add_bytes(*field, fv.str_val.as_bytes());
            }
            FieldKind::Json => {
                // JSON fallback for maps/complex types
                if let Ok(json_val) = serde_json::from_str::<serde_json::Value>(&fv.str_val) {
                    doc.add_json_object(*field, json_val.as_object()
                        .cloned().unwrap_or_default());
                }
            }
        }
    }

    // Delete-then-add (same as current Tantylla)
    let term = Term::from_field_text(index.field_id, doc_id);
    index.writer.delete_term(term);
    index.writer.add_document(doc)?;

    // Cache for writetime resolution before commit
    index.uncommitted.insert(doc_id.to_string(), CachedDoc {
        writetime: writetime_us,
        generation: index.generation,
    });

    Ok(())
}
```

**What this fixes vs. the current approach:**

- No `serde_json::to_string()` → `TantivyDocument::parse_json()` roundtrip
- No `merge_json()` / `apply_collection_deltas()` needed — the C++ side provides the full current state
- Type-safe field values — `SmallInt(42)` becomes `i64_val: 42`, not `Debug` string `"SmallInt(42)"`
- `NaN`/`Infinity` for floats can be handled explicitly instead of silently becoming `null`

#### 3.9 Search (simplified, no workarounds)

```rust
fn search(
    index: &ShardIndex,
    query: &str,
    limit: u32,
    offset: u32,
    facet_fields: &[String],
    group_by_partition: bool,
) -> Result<Box<FtsSearchResponse>> {
    let reader = index.reader.searcher();

    // ── Query parsing ───────────────────────────────────────────────
    // QueryParser receives all text fields as defaults.
    // Bare queries like "wireless" search across all text columns.
    // Field-prefixed queries like "brand:Sony" target a specific column.
    // Prefix queries like "wire*" work natively on text fields.
    // Fuzzy queries like "wireles~1" work natively on text fields.
    // Boosted queries like "name:wireless^2.0" work natively.
    // Phrase queries like '"noise cancelling"' work natively.
    let qp = QueryParser::for_index(&index.index, index.default_text_fields.clone());
    let user_query = qp.parse_query(query)?;

    // ── TTL filter ──────────────────────────────────────────────────
    let now_us = now_micros();
    let ttl_filter = RangeQuery::new_i64_bounds(
        index.field_expires_at,
        Bound::Excluded(now_us),
        Bound::Unbounded,
    );
    let combined = BooleanQuery::new(vec![
        (Occur::Must, Box::new(user_query)),
        (Occur::Must, Box::new(ttl_filter)),
    ]);

    // ── Hit collection ──────────────────────────────────────────────
    // Only returns _id, _partition_key, and score.
    // No payload extraction — the C++ side fetches full rows from the base table.
    let hits = if group_by_partition {
        collect_grouped_by_partition(&reader, &combined, &index, limit, offset)?
    } else {
        let top_docs = TopDocs::with_limit(limit as usize)
            .and_offset(offset as usize);
        let (count_collector, top_collector) = (Count, top_docs);
        let (total, top) = reader.search(&combined, &(count_collector, top_collector))?;
        // ... extract _id, _partition_key from each hit ...
    };

    // ── Facet aggregation (uses FAST fields where possible) ─────────
    let facets = if !facet_fields.is_empty() {
        collect_facets(&reader, &combined, &index, facet_fields)?
    } else {
        vec![]
    };

    Ok(Box::new(FtsSearchResponse { hits, total_hits, duration_us, facets }))
}
```

**What was eliminated:**

- `try_build_json_special_query()` — 80-line workaround, entirely gone
- Query expansion logic (60 lines of `default_fields`/`boost_fields` string manipulation) — replaced by `QueryParser::for_index` with correct default fields
- `document.` prefix injection — fields are top-level
- Array unwrapping in result extraction — typed fields return direct values
- `no_colon` / `no_quote` heuristics — QueryParser handles all syntax natively

#### 3.10 Facet Aggregation (using FAST fields)

With typed fields, facets on numeric/boolean/date columns can use Tantivy's columnar fast fields instead of deserializing stored documents:

```rust
fn collect_facets(
    searcher: &Searcher,
    query: &dyn Query,
    index: &ShardIndex,
    facet_fields: &[String],
) -> Result<Vec<FtsFacetResult>> {
    let doc_set = searcher.search(query, &DocSetCollector)?;
    let mut results = Vec::new();

    for field_name in facet_fields {
        let (field, kind) = match index.user_fields.get(field_name.as_str()) {
            Some(f) => f,
            None => continue,
        };

        let mut buckets: HashMap<String, u64> = HashMap::new();

        match kind {
            // FAST field path — columnar access, no deserialization
            FieldKind::I64 => {
                let fast_reader = searcher.segment_readers().iter()
                    .map(|seg| seg.fast_fields().i64(*field))
                    .collect::<Result<Vec<_>, _>>()?;
                for doc_addr in &doc_set {
                    if let Some(val) = fast_reader[doc_addr.segment_ord as usize]
                        .first(doc_addr.doc_id) {
                        *buckets.entry(val.to_string()).or_default() += 1;
                    }
                }
            }
            FieldKind::Bool => { /* similar fast field access */ }
            // Text fields — must use stored values (no fast field for tokenized text)
            FieldKind::Text | FieldKind::String => {
                for doc_addr in &doc_set {
                    let doc = searcher.doc::<TantivyDocument>(doc_addr)?;
                    if let Some(val) = doc.get_first(*field) {
                        // Direct value — no array unwrapping needed
                        if let Some(s) = val.as_str() {
                            *buckets.entry(s.to_string()).or_default() += 1;
                        }
                    }
                }
            }
            _ => { /* skip non-facetable types */ }
        }

        results.push(FtsFacetResult {
            field: field_name.clone(),
            buckets: buckets.into_iter()
                .map(|(v, c)| FtsFacetBucket { value: v, count: c })
                .sorted_by(|a, b| b.count.cmp(&a.count))
                .collect(),
        });
    }

    Ok(results)
}
```

**Improvement:** Numeric/boolean facets now use O(1)-per-doc columnar access instead of O(N) JSON deserialization.

#### 3.11 Thread Safety Contract

Same as v1:

- Each `ShardIndex` is exclusive to one Seastar shard (or its alien thread).
- `unsafe impl Send for ShardIndex {}` — justified by single-threaded per-shard access (same as `ScyllaLinearMemory` in `wasmtime_bindings/src/memory_creator.rs:73`).
- Heavy operations (commit, merge, search) offloaded to alien threads.

---

### 4. Phase 2: C++ Custom Index Class

**New files:** `scylladb/index/fts_index.hh`, `scylladb/index/fts_index.cc`

#### 4.1 Class Definition

```cpp
// index/fts_index.hh
#pragma once

#include "index/secondary_index_manager.hh"
#include "schema/schema.hh"

namespace db::index {

class fts_index : public secondary_index::custom_index {
public:
    fts_index() = default;
    ~fts_index() override = default;

    // custom_index interface
    std::optional<cql3::description> describe(
        const index_metadata& im, const schema& base_schema) const override;
    bool view_should_exist() const override;  // returns false
    void validate(
        const schema& schema,
        const cql3::statements::index_specific_prop_defs& properties,
        const std::vector<::shared_ptr<cql3::statements::index_target>>& targets,
        const gms::feature_service& fs,
        const data_dictionary::database& db) const override;
    table_schema_version index_version(const schema& schema) override;

    // Static helpers
    static bool has_fts_index(const schema& s);
    static bool has_fts_index_on_column(const schema& s, const sstring& col);

    // Schema inference — builds the field mapping JSON from CQL schema
    static sstring build_field_mapping_json(
        const schema& s,
        const std::vector<::shared_ptr<cql3::statements::index_target>>& targets,
        const index_options_map& options);

private:
    void check_target(const schema& s,
        const std::vector<::shared_ptr<cql3::statements::index_target>>& targets) const;
    void check_index_options(
        const cql3::statements::index_specific_prop_defs& properties) const;

    // CQL type → Tantivy field kind mapping
    static sstring map_cql_type_to_field_kind(const abstract_type& type);
    static bool is_fts_indexable(const abstract_type& type);

    // Recursive UDT decomposition
    static void decompose_udt(
        const user_type_impl& udt,
        const sstring& prefix,
        const index_options_map& options,
        std::vector</* FieldMapping JSON entries */>&  out,
        int depth = 0);
};

std::unique_ptr<secondary_index::custom_index> fts_index_factory();

} // namespace db::index
```

#### 4.2 Validation with Schema Inference

```cpp
void fts_index::validate(
    const schema& schema,
    const cql3::statements::index_specific_prop_defs& properties,
    const std::vector<::shared_ptr<cql3::statements::index_target>>& targets,
    const gms::feature_service& fs,
    const data_dictionary::database& db) const
{
    check_target(schema, targets);
    check_index_options(properties);

    // Option B only: CDC must not be explicitly disabled
    // check_cdc_not_explicitly_disabled(schema);

    // Build and validate the field mapping
    auto mapping = build_field_mapping_json(schema, targets, properties.get_raw_options());
    // mapping is stored in OPTIONS["field_mapping"] for runtime use
}
```

`build_field_mapping_json()` iterates the target columns (or all regular columns if no explicit targets), inspects each `column_definition.type->get_kind()`, and builds the JSON mapping descriptor. Per-column tokenizer overrides come from the OPTIONS map (e.g., `'description.tokenizer': 'en_stem'`).

```cpp
sstring fts_index::map_cql_type_to_field_kind(const abstract_type& type) {
    switch (type.get_kind()) {
        case abstract_type::kind::utf8:
        case abstract_type::kind::ascii:
            return "text";
        case abstract_type::kind::int32:
        case abstract_type::kind::long_kind:
        case abstract_type::kind::short_kind:
        case abstract_type::kind::byte:
            return "i64";
        case abstract_type::kind::float_kind:
        case abstract_type::kind::double_kind:
            return "f64";
        case abstract_type::kind::boolean:
            return "bool";
        case abstract_type::kind::timestamp:
        case abstract_type::kind::simple_date:
            return "date";
        case abstract_type::kind::time:
            return "i64";  // nanos since midnight
        case abstract_type::kind::uuid:
        case abstract_type::kind::timeuuid:
            return "string";  // exact match, untokenized
        case abstract_type::kind::inet:
            return "ip_addr";
        case abstract_type::kind::bytes:
            return "bytes";
        case abstract_type::kind::decimal:
        case abstract_type::kind::varint:
            return "string";  // stored as string representation
        case abstract_type::kind::user:
            return "udt";  // triggers recursive decomposition
        case abstract_type::kind::map:
            return "json";  // JSON fallback
        case abstract_type::kind::list:
        case abstract_type::kind::set: {
            // Infer element type and use multi-valued typed field
            auto& elem_type = dynamic_cast<const listlike_collection_type_impl&>(type)
                .get_elements_type();
            return map_cql_type_to_field_kind(*elem_type);
            // Caller sets multi_valued = true
        }
        default:
            throw exceptions::invalid_request_exception(
                format("CQL type '{}' is not supported for full-text indexing",
                       type.name()));
    }
}
```

#### 4.3 CQL Syntax

```sql
-- Index specific columns with per-column tokenizers
CREATE CUSTOM INDEX products_fts ON ecommerce.products (name, description, brand, price, tags)
  USING 'fts_index'
  WITH OPTIONS = {
    'description.tokenizer': 'en_stem',
    'brand.tokenizer': 'keyword',
    'commit_interval_ms': '5000',
    'prune_interval_ms': '50000'
  };

-- Index all regular columns (no explicit targets)
CREATE CUSTOM INDEX users_fts ON ecommerce.users ()
  USING 'fts_index';

-- Query
SELECT * FROM products WHERE description MATCH 'wireless headphones' LIMIT 10;
SELECT * FROM products WHERE name MATCH 'wire*' LIMIT 10;     -- prefix
SELECT * FROM products WHERE name MATCH 'wireles~1' LIMIT 10; -- fuzzy
SELECT * FROM products WHERE description MATCH '"noise cancelling"' LIMIT 10; -- phrase
SELECT * FROM products WHERE price MATCH '[40 TO 100]' LIMIT 10; -- range
SELECT * FROM products WHERE name MATCH 'wireless^2.0 OR bluetooth' LIMIT 10; -- boosted
```

#### 4.4 Registration

```cpp
// secondary_index_manager.cc — add to the static classes map
const static std::unordered_map<std::string_view,
    std::function<std::unique_ptr<custom_index>()>> classes = {
    {"vector_index", vector_index_factory},
    {"fts_index", db::index::fts_index_factory},
};
```

---

### 6. Phase 3B: Write Path — In-Process CDC Consumer (Option B)

#### 6.1 Implicit CDC Enablement

```cpp
// cdc/log.cc
bool cdc_enabled(const schema& s) {
    return s.cdc_options().enabled()
        || secondary_index::vector_index::has_vector_index(s)
        || db::index::fts_index::has_fts_index(s);  // NEW
}
```

#### 6.2 Per-Shard CDC Consumer

**New files:** `scylladb/fts/fts_cdc_consumer.hh`, `scylladb/fts/fts_cdc_consumer.cc`

Same structure as Phase 3A's `fts_index_manager`, but instead of hooking into the write path, it polls the CDC log table:

```cpp
class fts_cdc_consumer : public seastar::peering_sharded_service<fts_cdc_consumer> {
    // Same index storage as 3A
    std::unordered_map<table_id,
        std::unordered_map<sstring, rust::Box<fts::ShardIndex>>> _indexes;
    std::unique_ptr<alien_thread_runner> _alien;

    // CDC polling
    seastar::timer<> _poll_timer;
    std::unordered_map<table_id, db_clock::time_point> _checkpoints;

public:
    future<> start();
    future<> stop();
    future<> on_schema_change(const schema& s);

    // Called from the query path (same interface as 3A)
    future<rust::Box<fts::FtsSearchResponse>> search(...);

private:
    future<> poll_cdc();
    future<> process_cdc_row(const schema& s, const cql3::untyped_result_set_row& row);
};
```

`process_cdc_row()` extracts typed `FieldValue`s from the CDC row's columns (same type-switch logic as `extract_cell_value` in 3A but reading from CDC row format).

#### 6.3 Type Extraction from CDC Rows

The CDC log stores column values in the same binary format as the base table. Extraction uses the same `abstract_type::deserialize()` path, but reads from CDC row columns instead of mutation cells. This preserves full type fidelity — no JSON intermediate.

---

### 7. Phase 4: Read Path — CQL Query Integration (Shared)

#### 7.1 New CQL Operator: `MATCH`

**Grammar** (`cql3/Cql.g`):

```antlr
K_MATCH: M A T C H;
```

Add to relation rules so `WHERE col MATCH 'query'` is parsed.

**Operator enum** (`cql3/expr/expression.hh`):

```cpp
enum class oper_t { EQ, NEQ, LT, LTE, GTE, GT, IN, NOT_IN,
                    CONTAINS, CONTAINS_KEY, IS_NOT, LIKE, MATCH };
```

**Index support** (`secondary_index_manager.cc`):

```cpp
case oper_t::MATCH:
    return from_bool(
        _target_type == target_type::regular_values
        && is_fts_custom_class());
```

#### 7.2 Custom Select Statement

```cpp
class fts_indexed_table_select_statement : public select_statement {
    secondary_index::index _fts_index;
    sstring _match_query;

public:
    static ::shared_ptr<select_statement> prepare(
        data_dictionary::database db, schema_ptr schema, ...);

    future<::shared_ptr<cql3::result_set>> do_execute(
        query_processor& qp, service::query_state& qs,
        const query_options& options) const override;
};
```

**Execution flow:**

```
do_execute():
  1. Extract query string from MATCH bind value
  2. Determine limit from CQL LIMIT clause
  3. Call fts_index_manager.search() (or fts_cdc_consumer.search())
     → alien thread: fts::search(index, query, limit, offset, ...)
     → returns FtsSearchResponse with hits (id, pk, score)
  4. Parse primary keys from hit IDs (split on ":")
  5. Fetch full rows from base table by PK (same two-path pattern
     as vector_indexed_table_select_statement:
     partition-only vs. partition+clustering)
  6. Return result set ordered by BM25 score descending
```

**No payload in search results:** Unlike the current Tantylla architecture where the gateway returns `payload_json` from Tantivy, the native integration returns only `(id, partition_key, score)` from the Tantivy search. Full row data is fetched from the base table. This avoids storing and extracting full documents from Tantivy stored fields.

#### 7.3 Score Exposure

```sql
SELECT *, fts_score() AS relevance
FROM products WHERE description MATCH 'wireless' LIMIT 10;
```

Implemented as a hidden column injected during `prepare()` (same pattern as vector*index's `similarity*\*` function), sorted descending.

---

### 8. Phase 5: Index Lifecycle Management

#### 8.1 Storage Layout

```
<scylla_data_dir>/fts_indexes/<keyspace>/<table>/<index_name>/shard-<N>/
  ├── meta.json, *.managed.idx, *.pos, *.term, *.store, *.fast
```

One independent Tantivy index per shard. No cross-shard synchronization.

#### 8.2 Index Building (Initial Data Population)

On `CREATE INDEX` over an existing table:

1. `on_schema_change()` detects new FTS index.
2. Full table scan on local shard's data.
3. For each row: extract typed `FieldValue`s, call `fts::upsert_document()`.
4. `fts::commit()`.
5. Mark as `BUILT`.

#### 8.3 Schema Change Detection

`index_version()` returns `schema.version()`. When `ALTER TABLE ADD column` changes the schema version, the index version mismatches, triggering a rebuild. The rebuild re-runs `build_field_mapping_json()` with the new schema and reconstructs the Tantivy index with the updated field set.

#### 8.4 DROP INDEX

Calls `fts::drop_index()` (deletes segment files) and removes the directory.

#### 8.5 Streaming / Repair

- CDC captures streaming writes automatically. The consumer picks them up.

---

### 9. Phase 6: Build System Integration

Same as v1:

```cmake
# rust/CMakeLists.txt
generate_cxxbridge(fts_bindings
  INPUT fts_bindings/src/lib.rs
  INCLUDE rust/cxx.h ...)

add_library(fts_bindings STATIC)
target_sources(fts_bindings PRIVATE ${cxx_header} ${fts_bindings_sources})
target_link_libraries(fts_bindings INTERFACE Rust::rust_combined)
```

```toml
# rust/Cargo.toml
[dependencies]
fts_bindings = { path = "fts_bindings", version = "0.1.0" }
```

---

### 10. Phase 7: Testing

#### 10.1 Rust Unit Tests

```
rust/fts_bindings/tests/
├── test_schema_inference.rs    # FieldMapping → Tantivy schema construction
├── test_typed_upsert.rs        # Typed FieldValue ingestion (text, i64, f64, bool, date, ip)
├── test_multi_valued.rs        # Set/list multi-valued field ingestion
├── test_search_keyword.rs      # Bare keyword search across default text fields
├── test_search_prefix.rs       # Prefix queries on typed text fields (no workaround)
├── test_search_fuzzy.rs        # Fuzzy queries on typed text fields (no workaround)
├── test_search_phrase.rs       # Phrase queries
├── test_search_numeric.rs      # Range queries on i64/f64 FAST fields
├── test_search_bool.rs         # Boolean filter queries
├── test_search_date.rs         # Date range queries
├── test_facets_fast.rs         # Facet aggregation using FAST field columnar access
├── test_facets_text.rs         # Facet aggregation on text stored fields
├── test_ttl.rs                 # Expiration and pruning
├── test_writetime.rs           # Last-writer-wins conflict resolution
├── test_partition_ops.rs       # Partition delete, list-by-partition
└── test_tokenizers.rs          # Per-field tokenizer (default, en_stem, keyword)
```

#### 10.2 CQL Integration Tests

```python
# Ported from the 15 Tantylla e2e tests

def test_create_fts_index_with_schema_inference(cql, table):
    """Verify field mapping is built from CQL schema."""
    cql.execute("""
        CREATE TABLE t (id int PRIMARY KEY, name text, price double, active boolean)
    """)
    cql.execute("CREATE CUSTOM INDEX ON t (name, price, active) USING 'fts_index'")

def test_per_column_tokenizer(cql, table):
    """Verify en_stem tokenizer stems 'running' to 'run'."""
    cql.execute("""
        CREATE CUSTOM INDEX ON t (description) USING 'fts_index'
        WITH OPTIONS = {'description.tokenizer': 'en_stem'}
    """)
    cql.execute("INSERT INTO t (id, description) VALUES (1, 'I was running fast')")
    rs = cql.execute("SELECT * FROM t WHERE description MATCH 'run' LIMIT 10")
    assert len(rs) == 1  # Stemming matches "running" → "run"

def test_bare_query_searches_all_text_columns(cql, table):
    """Verify SESSION.md bug is fixed: bare 'wireless' finds values, not keys."""
    # Insert a product with "wireless" in description
    # SELECT WHERE description MATCH 'wireless' → 1 result

def test_prefix_query_native(cql, table):
    """Verify prefix queries work without workaround."""
    # SELECT WHERE name MATCH 'wire*' → finds "wireless"

def test_fuzzy_query_native(cql, table):
    """Verify fuzzy queries work without workaround."""
    # SELECT WHERE name MATCH 'wireles~1' → finds "wireless"

def test_numeric_range_fast_field(cql, table):
    """Verify range queries on typed numeric fields."""
    # SELECT WHERE price MATCH '[40 TO 100]' → correct results

def test_collection_indexing(cql, table):
    """Verify set<text> is indexed as multi-valued text field."""

def test_udt_decomposition(cql, table):
    """Verify UDT fields are decomposed into dotted-path typed fields."""

# ... plus all 15 existing test scenarios ported from Tantylla e2e
```

---

### 11. Feature Parity Matrix (Updated)

| Feature             | Tantylla (JSON)                        | Native (typed fields)                          | Improvement                       |
| ------------------- | -------------------------------------- | ---------------------------------------------- | --------------------------------- |
| Keyword search      | Manual expansion                       | Native `QueryParser`                           | No workaround needed              |
| Phrase search       | Blocked by expansion guard             | Native                                         | Works across multi-field          |
| Prefix search       | 80-line workaround, single-clause only | Native                                         | Compound prefix queries work      |
| Fuzzy search        | 80-line workaround, single-clause only | Native                                         | Compound fuzzy queries work       |
| Numeric range       | JSON `.set_fast(None)` hack            | Typed `FAST` i64/f64                           | Direct fast field access          |
| Boolean filter      | Via JSON                               | Typed `BoolField`                              | Direct fast field access          |
| Date range          | Via JSON number                        | Typed `DateField`                              | Native Tantivy date support       |
| IP address filter   | Not supported                          | Native `IpAddrField`                           | New capability                    |
| BM25 scoring        | ✓                                      | ✓                                              | Same                              |
| Facet aggregation   | O(N) JSON deser                        | FAST field columnar (numeric); stored (text)   | Major perf improvement            |
| Per-field tokenizer | Impossible                             | Per-column `TextOptions`                       | stemming + keyword coexist        |
| Boosted multi-field | String-level rewriting                 | Native Tantivy syntax (`field:term^N`)         | No workaround needed              |
| TTL pruning         | Background task                        | Background task                                | Same                              |
| Writetime conflict  | Same                                   | Same                                           | Same                              |
| Collection indexing | CDC deltas (tombstone/add/remove)      | Multi-valued typed fields                      | Simpler                           |
| UDT indexing        | Flat JSON (loses type name)            | Recursive decomposition to dotted typed fields | Type-safe                         |
| SmallInt/TinyInt    | Broken (`Debug` string)                | Correct (widened to i64)                       | Bug fix                           |
| Float precision     | Widened to f64, artifacts              | Native f64                                     | Same precision, no JSON roundtrip |
| Decimal             | String (indistinguishable from text)   | Stored as STRING field                         | Explicit type                     |

---

### 12. Files Created / Modified

**New files:**

| File                              | Purpose                                            |
| --------------------------------- | -------------------------------------------------- |
| `rust/fts_bindings/Cargo.toml`    | Crate manifest                                     |
| `rust/fts_bindings/src/lib.rs`    | CXX bridge with typed `FieldMapping`/`FieldValue`  |
| `rust/fts_bindings/src/schema.rs` | Schema construction from `FieldMapping`            |
| `rust/fts_bindings/src/writer.rs` | Typed document upsert/delete/commit                |
| `rust/fts_bindings/src/reader.rs` | Search with native `QueryParser`, typed facets     |
| `rust/fts_bindings/src/types.rs`  | `ShardIndex`, `FieldKind`, `CachedDoc`             |
| `index/fts_index.hh`              | Custom index class + `build_field_mapping_json()`  |
| `index/fts_index.cc`              | Validation, type mapping, UDT decomposition        |
| `fts/fts_cdc_consumer.hh`         | Per-shard CDC consumer (Option B)                  |
| `fts/fts_cdc_consumer.cc`         | CDC polling + row→FieldValue extraction (Option B) |

**Modified files:**

| File                                          | Change                                                   |
| --------------------------------------------- | -------------------------------------------------------- |
| `rust/Cargo.toml`                             | Add `fts_bindings` dependency                            |
| `rust/src/lib.rs`                             | Re-export `fts_bindings`                                 |
| `rust/CMakeLists.txt`                         | Add cxxbridge + library targets                          |
| `index/secondary_index_manager.cc`            | Register `fts_index` factory (line 206)                  |
| `cql3/Cql.g`                                  | Add `K_MATCH` token + relation grammar rule              |
| `cql3/expr/expression.hh`                     | Add `MATCH` to `oper_t` enum                             |
| `cql3/statements/select_statement.hh`         | Add `fts_indexed_table_select_statement`                 |
| `cql3/statements/select_statement.cc`         | FTS query execution + statement selection                |
| `cql3/restrictions/statement_restrictions.cc` | Handle MATCH in index selection                          |
| `cdc/log.cc`                                  | Add `has_fts_index()` to `cdc_enabled()` (Option B only) |

---

### 13. Estimated Effort

| Phase   | Description                             | Effort         |
| ------- | --------------------------------------- | -------------- |
| Phase 1 | Rust FTS bindings with schema inference | 5-7 days       |
| Phase 2 | C++ custom index class + type mapping   | 3-4 days       |
| Phase 6 | Build system (CMake, Cargo)             | 1-2 days       |
| Phase 3 | Write path (A or B)                     | 4-6 days       |
| Phase 4 | Read path (MATCH, select statement)     | 3-5 days       |
| Phase 5 | Index lifecycle (build, recovery, drop) | 2-3 days       |
| Phase 7 | Testing                                 | 3-5 days       |
|         | **Total**                               | **21-32 days** |

The increase over the original PLAN.md estimate (8-13 days) reflects the schema inference complexity, CQL grammar changes, and the custom select statement — none of which were accounted for in the original plan.

---

### 14. Risks

| Risk                                                               | Impact                                                   | Mitigation                                                          |
| ------------------------------------------------------------------ | -------------------------------------------------------- | ------------------------------------------------------------------- |
| Schema changes (ALTER TABLE) invalidate index                      | Index returns stale/incorrect results                    | `index_version()` triggers automatic rebuild                        |
| Tantivy schema is static (can't add fields dynamically)            | New columns require full rebuild                         | Rebuild is O(data_size), can run in background                      |
| CXX bridge complexity with `Vec<FieldMapping>` / `Vec<FieldValue>` | Compilation/runtime errors at FFI boundary               | Extensive unit tests on the Rust side with mock C++ callers         |
| Per-field tokenizer interaction with `QueryParser`                 | Multi-field queries may mix stemmed and unstemmed tokens | Document tokenizer selection rules; test thoroughly                 |
| CDC log growth (Option B)                                          | Disk space pressure                                      | Configure CDC TTL; implicit CDC uses same TTL as vector_index (24h) |
