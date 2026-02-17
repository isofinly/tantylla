use std::ops::Bound;
use std::path::Path;
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
use tantylla_common::indexer::{IndexBatchResponse, IndexOperation, SearchHit, SearchResponse};
use tracing::{error, info, trace, warn};

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
    expires_at: u64,
    writetime: u64,
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
        };

        engine.spawn_committer();
        engine.spawn_pruner();

        Ok(engine)
    }

    pub(crate) fn process_batch(
        &self,
        operations: Vec<IndexOperation>,
    ) -> Result<IndexBatchResponse> {
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
                    if let Some(writetime) = existing.as_ref().map(|d| d.writetime) {
                        if writetime >= op.writetime {
                            warn!("Upsert operation skipped due to newer write time");
                            skipped += 1;
                            continue;
                        }
                    }
                    let mut current_doc_json = existing
                        .map(|d| d.doc)
                        .unwrap_or_else(|| serde_json::json!({}));

                    let patch_json: serde_json::Value =
                        serde_json::from_str(&op.payload_json).unwrap_or(serde_json::Value::Null);

                    merge_json(&mut current_doc_json, patch_json);

                    let expires_at = if let Some(ttl) = op.cdc_ttl {
                        op.writetime + (ttl * 1_000_000) as u64
                    } else {
                        u64::MAX
                    };

                    let uncommitted_doc = Doc {
                        id: op.id.clone(),
                        doc: current_doc_json.clone(),
                        writetime: op.writetime,
                        expires_at,
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
                    if let Some(writetime) = existing.as_ref().map(|d| d.writetime) {
                        if writetime >= op.writetime {
                            warn!("Upsert operation skipped due to newer write time");
                            skipped += 1;
                            continue;
                        }
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
        let searcher = self.reader.searcher();

        let now = now_micros();

        let query_parser = QueryParser::for_index(&self.index, vec![self.field_doc]);
        let query = query_parser.parse_query(query_str)?;

        let now_term = Term::from_field_i64(self.field_expires_at, now);

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

        Ok(SearchResponse {
            hits,
            total_hits: total_hits as u64,
            duration_ms: 0,
        })
    }

    fn spawn_committer(&self) {
        let writer_lock = self.writer.clone();
        let uncommitted = self.uncommitted_docs.clone();
        let interval_duration = Duration::from_secs(self.config.commit_interval_secs);

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(interval_duration);

            loop {
                interval.tick().await;
                if let Ok(mut writer) = writer_lock.try_write() {
                    match writer.commit() {
                        Ok(opstamp) => {
                            info!("Commit successful. Opstamp: {}", opstamp);
                            uncommitted.clear();
                            trace!("Uncommitted cache cleared");
                        }
                        Err(e) => error!("Commit failed: {}", e),
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
                if let Ok(mut writer) = writer_lock.write() {
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
                    .and_then(|v| v.as_u64())
                    .unwrap_or(u64::MAX);

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
