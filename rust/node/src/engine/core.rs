use std::ops::Bound;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use tantivy::collector::{Count, TopDocs};
use tantivy::query::{BooleanQuery, Occur, QueryParser, RangeQuery};
use tantivy::schema::{
    FAST, Field, IndexRecordOption, JsonObjectOptions, STORED, STRING, Schema, TextFieldIndexing,
    Value,
};
use tantivy::{Document, Index, IndexReader, IndexWriter, ReloadPolicy, TantivyDocument, Term};
use tantylla_common::indexer::index_operation::OpType;
use tantylla_common::indexer::{
    CollectionDelta, IndexBatchResponse, IndexOperation, SearchHit, SearchResponse,
};
use tantylla_common::tracing::events::{TestEvent, TestEventSource};
use tracing::{debug, error, info, trace, warn};

const WRITER_BUFFER_SIZE: usize = 50_000_000; // 50MB buffer
const PRUNE_INTERVAL: Duration = Duration::from_secs(50);

/// Time-based commit configuration.
/// Commits happen at fixed intervals to make documents visible for search.
#[derive(Clone, Copy, Debug)]
pub struct AdaptiveConfig {
    /// Time interval between commits (in seconds)
    pub commit_interval_secs: u64,
}

impl Default for AdaptiveConfig {
    fn default() -> Self {
        Self {
            commit_interval_secs: 5,
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct Doc {
    id: String,
    doc: serde_json::Value,
    expires_at: i64,
    writetime: u64,
    generation: u64,
}

#[derive(Clone)]
pub(crate) struct Engine {
    index: Index,
    reader: IndexReader,
    writer: Arc<RwLock<IndexWriter>>,
    schema: Schema,
    // TODO: Move it somewhere else
    field_id: Field,
    field_doc: Field,
    field_expires_at: Field,
    config: AdaptiveConfig,
    // TODO: Handle it somehow
    /// Map from `String` (_The unique Primary Key of the row_) to the `UncommittedDoc`
    uncommitted_docs: Arc<DashMap<String, Doc>>,
    current_generation: Arc<AtomicU64>,
}

impl Engine {
    pub(crate) fn new(path: impl AsRef<Path>, config: AdaptiveConfig) -> Result<Self> {
        let path = path.as_ref();
        std::fs::create_dir_all(path).context("Failed to create index directory")?;

        let mut schema_builder = Schema::builder();
        let field_id = schema_builder.add_text_field("id", STRING | STORED);
        let field_expires_at = schema_builder.add_i64_field("expires_at", FAST | STORED);
        // No need to interact with writetime field. We can safely ignore its handle.
        let _ = schema_builder.add_i64_field("writetime", FAST | STORED);
        let json_options = JsonObjectOptions::default()
            .set_stored()
            .set_indexing_options(
                TextFieldIndexing::default()
                    .set_tokenizer("en_stem")
                    .set_index_option(IndexRecordOption::WithFreqsAndPositions),
            );
        let field_doc = schema_builder.add_json_field("document", json_options);
        let schema = schema_builder.build();

        let index = Index::open_or_create(
            tantivy::directory::MmapDirectory::open(path)?,
            schema.clone(),
        )?;

        let writer = index.writer(WRITER_BUFFER_SIZE)?;

        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::OnCommitWithDelay)
            .try_into()?;

        let engine = Self {
            index,
            reader,
            writer: Arc::new(RwLock::new(writer)),
            schema,
            field_id,
            field_doc,
            field_expires_at,
            config,
            uncommitted_docs: Arc::new(DashMap::new()),
            current_generation: Arc::new(AtomicU64::new(0)),
        };

        engine.spawn_committer();
        engine.spawn_pruner();

        Ok(engine)
    }

    pub(crate) fn process_batch(
        &self,
        operations: Vec<IndexOperation>,
    ) -> Result<IndexBatchResponse> {
        debug!(
            target: "test_event",
            source = %TestEventSource::Node,
            event = %TestEvent::EngineProcessBatchEnter
        );

        let writer = self
            .writer
            .write()
            .map_err(|_| anyhow::anyhow!("Lock poisoned"))?;
        let uncommitted = self.uncommitted_docs.clone();

        let mut processed = 0;
        let mut skipped = 0;

        for op in operations {
            let op_type = OpType::try_from(op.op_type).unwrap_or(OpType::Unspecified);

            match op_type {
                OpType::Upsert => {
                    let existing = self.find_cached_or_indexed(&op.id);
                    if let Some(writetime) = existing.as_ref().map(|d| d.writetime)
                        && writetime >= op.writetime
                    {
                        warn!("Upsert operation skipped due to newer write time");
                        skipped += 1;
                        continue;
                    }
                    let mut current_doc_json = existing
                        .map(|d| d.doc)
                        .unwrap_or_else(|| serde_json::json!({}));

                    let patch_json: serde_json::Value =
                        serde_json::from_str(&op.payload_json).unwrap_or(serde_json::Value::Null);

                    merge_json(&mut current_doc_json, patch_json);

                    if !op.collection_deltas.is_empty() {
                        apply_collection_deltas(&mut current_doc_json, &op.collection_deltas);
                    }

                    let expires_at = if let Some(ttl) = op.cdc_ttl {
                        let ttl_micros = ttl.saturating_mul(1_000_000).max(0) as u64;
                        let expires_at = op.writetime.saturating_add(ttl_micros);
                        i64::try_from(expires_at).unwrap_or(i64::MAX)
                    } else {
                        i64::MAX
                    };

                    let generation = self.current_generation.load(Ordering::Acquire);
                    let uncommitted_doc = Doc {
                        id: op.id.clone(),
                        doc: current_doc_json.clone(),
                        writetime: op.writetime,
                        expires_at,
                        generation,
                    };

                    uncommitted.insert(op.id.clone(), uncommitted_doc);

                    let full_doc_wrapper = serde_json::json!({
                        "id": op.id,
                        "expires_at": expires_at,
                        "document": current_doc_json,
                        "writetime": op.writetime,
                    });

                    let term = Term::from_field_text(self.field_id, &op.id);
                    writer.delete_term(term);

                    match TantivyDocument::parse_json(&self.schema, &full_doc_wrapper.to_string()) {
                        Ok(doc) => {
                            writer.add_document(doc)?;
                            processed += 1;
                        }
                        Err(e) => {
                            error!("Skipping doc {}: {}", op.id, e);
                            skipped += 1;
                        }
                    };
                }
                OpType::Delete => {
                    let existing = self.find_cached_or_indexed(&op.id);
                    if let Some(writetime) = existing.as_ref().map(|d| d.writetime)
                        && writetime >= op.writetime
                    {
                        warn!("Delete operation skipped due to newer write time");
                        skipped += 1;
                        continue;
                    }
                    let term = Term::from_field_text(self.field_id, &op.id);
                    writer.delete_term(term);
                    uncommitted.remove(&op.id);
                    processed += 1;
                }
                _ => skipped += 1,
            }
        }

        Ok(IndexBatchResponse {
            processed_count: processed,
            skipped_count: skipped,
            success: true,
        })
    }

    pub(crate) fn search(
        &self,
        query_str: &str,
        limit: usize,
        offset: usize,
    ) -> Result<SearchResponse> {
        debug!(
            target: "test_event",
            source = %TestEventSource::Node,
            event = %TestEvent::EngineSearchEnter
        );

        let searcher = self.reader.searcher();

        let start_micros = now_micros();

        let query_parser = QueryParser::for_index(&self.index, vec![self.field_doc]);
        let query = query_parser.parse_query(query_str)?;

        let now_term = Term::from_field_i64(self.field_expires_at, start_micros);

        let expiration_query = RangeQuery::new(Bound::Excluded(now_term), Bound::Unbounded);
        let combined_query = BooleanQuery::new(vec![
            (Occur::Must, query),
            (Occur::Must, Box::new(expiration_query)),
        ]);

        let top_docs = searcher.search(
            &combined_query,
            &TopDocs::with_limit(limit).and_offset(offset),
        )?;
        let total_hits = searcher.search(&combined_query, &Count)?;

        let mut hits = Vec::new();
        for (score, doc_address) in top_docs {
            let retrieved_doc: TantivyDocument = searcher.doc(doc_address)?;

            let id = retrieved_doc
                .get_first(self.field_id)
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();

            let doc_json_str = retrieved_doc.to_json(&self.schema);

            let payload_json = serde_json::from_str::<serde_json::Value>(&doc_json_str)
                .ok()
                .and_then(|mut v| match v.get_mut("document") {
                    Some(serde_json::Value::Array(arr)) => {
                        if !arr.is_empty() {
                            Some(std::mem::take(&mut arr[0]))
                        } else {
                            None
                        }
                    }
                    Some(other) => Some(other.clone()),
                    None => None,
                })
                .and_then(|v| serde_json::to_string(&v).ok())
                .unwrap_or_else(|| "{}".to_string());

            hits.push(SearchHit {
                id,
                score,
                payload_json,
            });
        }

        let time_delta_micros = now_micros() - start_micros;

        Ok(SearchResponse {
            hits,
            total_hits: total_hits as u64,
            duration_ms: time_delta_micros as u64,
        })
    }

    fn spawn_committer(&self) {
        let writer_lock = self.writer.clone();
        let uncommitted = self.uncommitted_docs.clone();
        let interval_duration = Duration::from_secs(self.config.commit_interval_secs);
        let gen_counter = self.current_generation.clone();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(interval_duration);

            loop {
                interval.tick().await;
                if let Ok(mut writer) = writer_lock.try_write() {
                    match writer.commit() {
                        Ok(opstamp) => {
                            let committed_gen = gen_counter.fetch_add(1, Ordering::SeqCst);
                            info!("Commit successful. Opstamp: {}", opstamp);
                            uncommitted.retain(|_, doc| doc.generation > committed_gen);
                            trace!("Evicted cache entries from generation <= {}", committed_gen);
                        }
                        Err(e) => {
                            gen_counter.fetch_sub(1, Ordering::SeqCst);
                            error!("Commit failed: {}", e)
                        }
                    }
                }
                // Contended, skip commit
            }
        });
    }

    fn spawn_pruner(&self) {
        let writer_lock = self.writer.clone();
        let field_expires_at = self.field_expires_at;

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(PRUNE_INTERVAL);
            loop {
                interval.tick().await;
                let now = now_micros();
                trace!("Pruning started");
                if let Ok(mut writer) = writer_lock.try_write() {
                    let now_term = Term::from_field_i64(field_expires_at, now);
                    let prune_query = RangeQuery::new(Bound::Unbounded, Bound::Included(now_term));
                    match writer.delete_query(Box::new(prune_query)) {
                        Ok(opstamp) => {
                            info!("Pruned expired docs. Opstamp: {}", opstamp);
                            match writer.commit() {
                                Ok(_) => info!("Prune commit successful"),
                                Err(e) => error!("Prune commit failed: {}", e),
                            }
                        }
                        Err(e) => error!("Pruning failed: {}", e),
                    }
                }
                // Contended, skip pruning
            }
        });
    }

    fn find_cached_or_indexed(&self, id: &str) -> Option<Doc> {
        let searcher = self.reader.searcher();

        // 1. Check the uncommitted cache first
        if let Some(cached_doc) = self.uncommitted_docs.get(id) {
            return Some(cached_doc.clone());
        }

        // 2. If not in cache, search the Tantivy index
        let term = Term::from_field_text(self.field_id, id);
        let term_query = tantivy::query::TermQuery::new(term, IndexRecordOption::Basic);

        let top_docs = searcher
            .search(&term_query, &TopDocs::with_limit(1))
            .unwrap_or_default();

        if let Some((_, doc_addr)) = top_docs.first() {
            let retrieved: TantivyDocument = searcher.doc(*doc_addr).unwrap_or_default();
            let doc_str = retrieved.to_json(&self.schema);

            if let Ok(mut full_doc_val) = serde_json::from_str::<serde_json::Value>(&doc_str) {
                let writetime = full_doc_val
                    .get("writetime")
                    .and_then(|v| v.as_array())
                    .and_then(|a| a.first())
                    .and_then(|v| v.as_u64())
                    .unwrap_or(0);

                let expires_at = full_doc_val
                    .get("expires_at")
                    .and_then(|v| v.as_array())
                    .and_then(|a| a.first())
                    .and_then(|v| v.as_i64())
                    .unwrap_or(i64::MAX);

                let doc_body = match full_doc_val.get_mut("document") {
                    Some(serde_json::Value::Array(arr)) if !arr.is_empty() => {
                        std::mem::take(&mut arr[0])
                    }
                    Some(other) => other.clone(),
                    None => serde_json::json!({}),
                };

                return Some(Doc {
                    id: id.to_string(),
                    doc: doc_body,
                    writetime,
                    expires_at,
                    generation: 0,
                });
            }
        }

        None
    }
}

fn now_micros() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros() as i64
}

fn merge_json(a: &mut serde_json::Value, b: serde_json::Value) {
    match (a, b) {
        (a @ &mut serde_json::Value::Object(_), serde_json::Value::Object(b)) => {
            let a = a.as_object_mut().unwrap();
            for (k, v) in b {
                merge_json(a.entry(k).or_insert(serde_json::Value::Null), v);
            }
        }
        (a, b) => *a = b,
    }
}

/// Applies collection delta operations to the document's collection fields.
///
/// Each `CollectionDelta` targets a single column and carries three signals:
/// - tombstone (clear all)
/// - deleted elements (remove specific)
/// - added elements (union in new).
///
/// The function modifies `doc` in place.
fn apply_collection_deltas(doc: &mut serde_json::Value, deltas: &[CollectionDelta]) {
    let obj = match doc.as_object_mut() {
        Some(obj) => obj,
        // Intentionally omitted: handling non-object documents.
        // Collection deltas only make sense when the document is a JSON object.
        None => return,
    };

    for delta in deltas {
        let existing = obj
            .entry(&delta.column)
            .or_insert_with(|| serde_json::Value::Array(Vec::new()));

        let mut elements: Vec<serde_json::Value> = match existing.as_array() {
            Some(arr) => arr.clone(),
            // If the existing value is not an array (e.g., first time seeing
            // this column, or schema changed), start fresh.
            None => Vec::new(),
        };

        if delta.tombstoned {
            elements.clear();
        }

        if !delta.deleted_elements_json.is_empty()
            && let Ok(deleted) =
                serde_json::from_str::<Vec<serde_json::Value>>(&delta.deleted_elements_json)
        {
            elements.retain(|elem| !deleted.contains(elem));
        }

        if !delta.added_elements_json.is_empty()
            && let Ok(added) =
                serde_json::from_str::<Vec<serde_json::Value>>(&delta.added_elements_json)
        {
            for elem in added {
                if !elements.contains(&elem) {
                    elements.push(elem);
                }
            }
        }

        *existing = serde_json::Value::Array(elements);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn delta(column: &str, tombstoned: bool, added: &str, deleted: &str) -> CollectionDelta {
        CollectionDelta {
            column: column.to_string(),
            tombstoned,
            added_elements_json: added.to_string(),
            deleted_elements_json: deleted.to_string(),
        }
    }

    #[test]
    fn add_elements_to_empty_collection() {
        let mut doc = serde_json::json!({});
        let deltas = vec![delta("tags", false, r#"["a","b"]"#, "")];

        apply_collection_deltas(&mut doc, &deltas);

        assert_eq!(doc["tags"], serde_json::json!(["a", "b"]));
    }

    #[test]
    fn add_elements_to_existing_collection() {
        let mut doc = serde_json::json!({ "tags": ["existing"] });
        let deltas = vec![delta("tags", false, r#"["new"]"#, "")];

        apply_collection_deltas(&mut doc, &deltas);

        let tags = doc["tags"].as_array().expect("tags should be an array");
        assert_eq!(tags.len(), 2);
        assert!(tags.contains(&serde_json::json!("existing")));
        assert!(tags.contains(&serde_json::json!("new")));
    }

    #[test]
    fn add_duplicate_element_is_deduplicated() {
        let mut doc = serde_json::json!({ "tags": ["a", "b"] });
        let deltas = vec![delta("tags", false, r#"["b","c"]"#, "")];

        apply_collection_deltas(&mut doc, &deltas);

        let tags = doc["tags"].as_array().expect("tags should be an array");
        assert_eq!(tags.len(), 3, "duplicate 'b' should not be added twice");
        assert!(tags.contains(&serde_json::json!("a")));
        assert!(tags.contains(&serde_json::json!("b")));
        assert!(tags.contains(&serde_json::json!("c")));
    }

    #[test]
    fn remove_elements_from_collection() {
        let mut doc = serde_json::json!({ "tags": ["a", "b", "c"] });
        let deltas = vec![delta("tags", false, "", r#"["b"]"#)];

        apply_collection_deltas(&mut doc, &deltas);

        assert_eq!(doc["tags"], serde_json::json!(["a", "c"]));
    }

    #[test]
    fn remove_nonexistent_element_is_noop() {
        let mut doc = serde_json::json!({ "tags": ["a", "b"] });
        let deltas = vec![delta("tags", false, "", r#"["z"]"#)];

        apply_collection_deltas(&mut doc, &deltas);

        assert_eq!(doc["tags"], serde_json::json!(["a", "b"]));
    }

    #[test]
    fn tombstone_clears_collection() {
        let mut doc = serde_json::json!({ "tags": ["a", "b", "c"] });
        let deltas = vec![delta("tags", true, "", "")];

        apply_collection_deltas(&mut doc, &deltas);

        assert_eq!(doc["tags"], serde_json::json!([]));
    }

    #[test]
    fn tombstone_then_add_is_overwrite() {
        // This is the CDC pattern for `SET tags = {'new'}`:
        // tombstoned=true + added=["new"]
        let mut doc = serde_json::json!({ "tags": ["old1", "old2"] });
        let deltas = vec![delta("tags", true, r#"["new"]"#, "")];

        apply_collection_deltas(&mut doc, &deltas);

        assert_eq!(doc["tags"], serde_json::json!(["new"]));
    }

    #[test]
    fn insert_tombstones_then_adds() {
        let mut doc = serde_json::json!({});
        let deltas = vec![delta("tags", true, r#"["premium"]"#, "")];

        apply_collection_deltas(&mut doc, &deltas);

        assert_eq!(doc["tags"], serde_json::json!(["premium"]));
    }

    #[test]
    fn multiple_deltas_on_different_columns() {
        let mut doc = serde_json::json!({ "tags": ["a"], "categories": ["x"] });
        let deltas = vec![
            delta("tags", false, r#"["b"]"#, ""),
            delta("categories", false, "", r#"["x"]"#),
        ];

        apply_collection_deltas(&mut doc, &deltas);

        assert_eq!(doc["tags"], serde_json::json!(["a", "b"]));
        assert_eq!(doc["categories"], serde_json::json!([]));
    }

    #[test]
    fn multiple_deltas_on_same_column_applied_sequentially() {
        let mut doc = serde_json::json!({ "tags": ["a", "b"] });
        let deltas = vec![
            delta("tags", false, "", r#"["a"]"#),
            delta("tags", false, r#"["c"]"#, ""),
        ];

        apply_collection_deltas(&mut doc, &deltas);

        let tags = doc["tags"].as_array().expect("tags should be an array");
        assert_eq!(tags.len(), 2);
        assert!(tags.contains(&serde_json::json!("b")));
        assert!(tags.contains(&serde_json::json!("c")));
        assert!(!tags.contains(&serde_json::json!("a")));
    }

    #[test]
    fn non_object_document_is_noop() {
        // apply_collection_deltas intentionally skips non-object docs
        let mut doc = serde_json::json!("just a string");
        let deltas = vec![delta("tags", false, r#"["a"]"#, "")];

        apply_collection_deltas(&mut doc, &deltas);

        assert_eq!(doc, serde_json::json!("just a string"));
    }

    #[test]
    fn empty_deltas_is_noop() {
        let mut doc = serde_json::json!({ "tags": ["a"] });
        apply_collection_deltas(&mut doc, &[]);
        assert_eq!(doc["tags"], serde_json::json!(["a"]));
    }

    #[test]
    fn malformed_json_in_added_elements_is_ignored() {
        let mut doc = serde_json::json!({ "tags": ["a"] });
        let deltas = vec![delta("tags", false, "not valid json", "")];

        apply_collection_deltas(&mut doc, &deltas);

        // Existing data should be unchanged
        assert_eq!(doc["tags"], serde_json::json!(["a"]));
    }

    #[test]
    fn malformed_json_in_deleted_elements_is_ignored() {
        let mut doc = serde_json::json!({ "tags": ["a", "b"] });
        let deltas = vec![delta("tags", false, "", "not valid json")];

        apply_collection_deltas(&mut doc, &deltas);

        assert_eq!(doc["tags"], serde_json::json!(["a", "b"]));
    }

    #[test]
    fn existing_non_array_value_starts_fresh() {
        // If a column was previously a scalar and now gets collection deltas,
        // we start from an empty vec rather than crashing.
        let mut doc = serde_json::json!({ "tags": "was_a_string" });
        let deltas = vec![delta("tags", false, r#"["new"]"#, "")];

        apply_collection_deltas(&mut doc, &deltas);

        assert_eq!(doc["tags"], serde_json::json!(["new"]));
    }

    #[test]
    fn merge_json_adds_new_keys() {
        let mut a = serde_json::json!({ "name": "alice" });
        let b = serde_json::json!({ "age": 30 });
        merge_json(&mut a, b);
        assert_eq!(a, serde_json::json!({ "name": "alice", "age": 30 }));
    }

    #[test]
    fn merge_json_overwrites_existing_keys() {
        let mut a = serde_json::json!({ "name": "alice" });
        let b = serde_json::json!({ "name": "bob" });
        merge_json(&mut a, b);
        assert_eq!(a, serde_json::json!({ "name": "bob" }));
    }

    #[test]
    fn merge_json_nested_objects() {
        let mut a = serde_json::json!({ "meta": { "x": 1 } });
        let b = serde_json::json!({ "meta": { "y": 2 } });
        merge_json(&mut a, b);
        assert_eq!(a, serde_json::json!({ "meta": { "x": 1, "y": 2 } }));
    }
}
