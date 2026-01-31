use std::ops::Bound;
use std::path::Path;
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use tantivy::collector::{Count, TopDocs};
use tantivy::query::{BooleanQuery, Occur, QueryParser, RangeQuery};
use tantivy::schema::{
    FAST, Field, IndexRecordOption, JsonObjectOptions, STORED, STRING, Schema, TextFieldIndexing,
    Value,
};
use tantivy::{Document, Index, IndexReader, IndexWriter, ReloadPolicy, TantivyDocument, Term};
use tantylla_common::indexer::index_operation::OpType;
use tantylla_common::indexer::{IndexBatchResponse, IndexOperation, SearchHit, SearchResponse};
use tracing::{error, info, trace};

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

#[derive(Clone)]
pub(crate) struct Engine {
    index: Index,
    reader: IndexReader,
    writer: Arc<RwLock<IndexWriter>>,
    schema: Schema,
    field_id: Field,
    field_doc: Field,
    field_expires_at: Field,
    config: AdaptiveConfig,
}

impl Engine {
    pub(crate) fn new(path: impl AsRef<Path>, config: AdaptiveConfig) -> Result<Self> {
        let path = path.as_ref();
        std::fs::create_dir_all(path).context("Failed to create index directory")?;

        let mut schema_builder = Schema::builder();
        let field_id = schema_builder.add_text_field("id", STRING | STORED);
        let field_expires_at = schema_builder.add_i64_field("expires_at", FAST | STORED);
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
        };

        engine.spawn_committer();
        engine.spawn_pruner();

        info!(
            "Engine initialized with commit interval: {}s",
            config.commit_interval_secs
        );

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

        let mut processed = 0;
        let mut skipped = 0;

        for op in operations {
            let op_type = OpType::try_from(op.op_type).unwrap_or(OpType::Unspecified);

            match op_type {
                OpType::Upsert => {
                    let term = Term::from_field_text(self.field_id, &op.id);

                    let patch_json: serde_json::Value =
                        serde_json::from_str(&op.payload_json).unwrap_or(serde_json::Value::Null);

                    let full_doc = if patch_json.is_object() {
                        patch_json
                    } else {
                        serde_json::json!({})
                    };

                    let expires_at = if let Some(ttl) = op.cdc_ttl {
                        op.writetime + (ttl * 1_000_000)
                    } else {
                        i64::MAX
                    };

                    let full_doc_wrapper = serde_json::json!({
                        "id": op.id,
                        "expires_at": expires_at,
                        "document": full_doc
                    });

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
                    let term = Term::from_field_text(self.field_id, &op.id);
                    writer.delete_term(term);
                    processed += 1;
                }
                _ => skipped += 1,
            }
        }

        Ok(IndexBatchResponse {
            processed_count: processed as u32,
            skipped_count: skipped as u32,
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
        let interval_duration = Duration::from_secs(self.config.commit_interval_secs);

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(interval_duration);

            loop {
                interval.tick().await;

                if let Ok(mut writer) = writer_lock.write() {
                    match writer.commit() {
                        Ok(opstamp) => {
                            info!("Commit. Opstamp: {}", opstamp);
                        }
                        Err(e) => error!("Commit failed: {}", e),
                    }
                }
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
}

fn now_micros() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros() as i64
}
