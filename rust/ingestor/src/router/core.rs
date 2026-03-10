use std::sync::Arc;

use ahash::AHashMap;
use anyhow::{Context, Result};
use scylla::client::session::Session;
use scylla_cdc::consumer::OperationType;

use tantylla_common::{
    indexer::{
        ListDocIdsRequest, index_operation::OpType, index_service_client::IndexServiceClient,
    },
    tracing::events::{TestEvent, TestEventSource},
};
use tracing::{debug, warn};

use crate::{
    batch::service::{BatchItem, Service},
    router::{
        common::PendingRangeStart,
        utils::{self, ClusteringKeyColumn},
    },
};

const FLUSH_RETRIES: usize = 3;

#[derive(Clone, Debug)]
pub(crate) struct Router {
    node_info: AHashMap<usize, String>,
    batch_service: Arc<Service>,
    partition_key_columns: Vec<String>,
    full_primary_key_columns: Vec<String>,
    clustering_key_columns: Vec<ClusteringKeyColumn>,
    table_name: String,
}

impl Router {
    pub async fn new(
        node_info: AHashMap<usize, String>,
        keyspace: &String,
        table: &String,
        session: Arc<Session>,
        batch_service: Arc<Service>,
    ) -> Result<Self> {
        let key_info = utils::get_table_key_info(session.clone(), keyspace, table).await?;

        Ok(Router {
            node_info,
            batch_service,
            partition_key_columns: key_info.partition_key_columns,
            full_primary_key_columns: key_info.full_primary_key_columns,
            clustering_key_columns: key_info.clustering_key_columns,
            table_name: format!("{}.{}", keyspace, table),
        })
    }

    /// Routes a forward CDC operation (insert / update / delete / partition
    /// delete) to the appropriate search node.
    ///
    /// Checks backpressure first; if active, attempts a flush before
    /// proceeding.
    pub(crate) async fn route_forward(
        &self,
        row: &scylla_cdc::consumer::CDCRow<'_>,
        op_type: OpType,
    ) -> anyhow::Result<()> {
        if self.batch_service.is_backpressure_active() {
            debug!("Backpressure active, attempting to flush before routing");
            if let Err(e) = self.batch_service.flush().await {
                return Err(anyhow::anyhow!(
                    "Backpressure active and flush failed: {}",
                    e
                ));
            }
        }

        let target_node_id =
            utils::get_target_node_id(row, &self.partition_key_columns, self.node_info.len());
        let target_node = self.node_info.get(&target_node_id).expect(
            "target_node_id is derived from hash % node_count, so it must exist in node_info",
        );

        // Build partition key string (for the `partition_key` field on the
        // IndexOperation and for partition-level deletes).
        let pk_values: Vec<String> = self
            .partition_key_columns
            .iter()
            .map(|col_name| match row.get_value(col_name) {
                Some(val) => format!("{}", val),
                None => "null".to_string(),
            })
            .collect();
        let partition_key = pk_values.join(":");

        // Build the full primary key ID. For PartitionDelete,
        // CK columns will be null — the node uses `partition_key`
        // (not `id`) to locate documents for this op type.
        let full_pk_values: Vec<String> = self
            .full_primary_key_columns
            .iter()
            .map(|col_name| match row.get_value(col_name) {
                Some(val) => format!("{}", val),
                None => "null".to_string(),
            })
            .collect();
        let id = full_pk_values.join(":");

        tracing::debug!(
            target: "test_event",
            source = %TestEventSource::Ingestor,
            event = %TestEvent::CdcRowRouted,
            node_count = self.node_info.len(),
            table = self.table_name,
            id,
            op = format!("{:?}", op_type),
            target_node = target_node
        );

        let writetime = utils::extract_writetime_from_timeuuid(row.time)?;
        let cdc_ttl = row.ttl;

        let (payload_json, collection_deltas) = match op_type {
            OpType::Upsert if matches!(row.operation, OperationType::PostImage) => {
                let json = utils::serialize_postimage_to_json(row)?;
                (json, Vec::new())
            }
            OpType::Upsert => {
                let serialized = utils::serialize_cdc_row(row)?;
                (serialized.payload_json, serialized.collection_deltas)
            }
            _ => (String::new(), Vec::new()),
        };

        let batch_item = BatchItem {
            target_node: target_node.clone(),
            id: id.clone(),
            op_type: op_type as i32,
            writetime,
            cdc_ttl,
            payload_json: payload_json.clone(),
            collection_deltas,
            partition_key: Some(partition_key),
        };

        self.enqueue_batch_item(batch_item).await
    }

    /// Extracts a [`PendingRangeStart`] from a start-bound CDC row.
    ///
    /// The caller ([`super::super::cdc::consumer::Consumer`]) stores the
    /// returned value in its own `Option<PendingRangeStart>` field.  This
    /// keeps the pending state per-stream rather than shared across all
    /// streams via a `Mutex`.
    pub(crate) fn extract_range_delete_start(
        &self,
        row: &scylla_cdc::consumer::CDCRow<'_>,
    ) -> anyhow::Result<PendingRangeStart> {
        let start_inclusive = row.operation == OperationType::RowRangeDelInclLeft;

        let pk_values: Vec<String> = self
            .partition_key_columns
            .iter()
            .map(|col_name| match row.get_value(col_name) {
                Some(val) => format!("{}", val),
                None => "null".to_string(),
            })
            .collect();
        let partition_key = pk_values.join(":");

        let target_node_id =
            utils::get_target_node_id(row, &self.partition_key_columns, self.node_info.len());
        let target_node = self.node_info.get(&target_node_id).expect(
            "target_node_id is derived from hash % node_count, so it must exist in node_info",
        );

        // Extract clustering key values from the start-bound row.
        // Null means unbounded on that column.
        let ck_values: Vec<Option<String>> = self
            .clustering_key_columns
            .iter()
            .map(|ck_col| {
                row.get_value(&ck_col.name)
                    .as_ref()
                    .map(|v| format!("{}", v))
            })
            .collect();

        let writetime = utils::extract_writetime_from_timeuuid(row.time)?;

        Ok(PendingRangeStart {
            partition_key,
            target_node: target_node.clone(),
            ck_values,
            start_inclusive,
            writetime,
        })
    }

    /// Resolves a range-delete pair using the caller-supplied `start` bound
    /// and the current end-bound `row`.
    ///
    /// Flushes the batch service before querying the target node for document
    /// IDs, then enqueues individual DELETE operations for each matching
    /// document.
    pub(crate) async fn commit_range_delete(
        &self,
        start: PendingRangeStart,
        row: &scylla_cdc::consumer::CDCRow<'_>,
    ) -> anyhow::Result<()> {
        let end_inclusive = row.operation == OperationType::RowRangeDelInclRight;

        let end_ck_values: Vec<Option<String>> = self
            .clustering_key_columns
            .iter()
            .map(|ck_col| {
                row.get_value(&ck_col.name)
                    .as_ref()
                    .map(|v| format!("{}", v))
            })
            .collect();

        debug!(
            "Resolving range delete for partition_key={} on table {}",
            start.partition_key, self.table_name
        );

        for i in 0..FLUSH_RETRIES {
            if let Err(e) = self.batch_service.flush().await {
                warn!(
                    "Pre-range-delete flush failed (retries left: {}): {}",
                    FLUSH_RETRIES - i - 1,
                    e
                );
            } else {
                break;
            }
        }

        let all_doc_ids = self
            .fetch_doc_ids_for_partition(&start.target_node, &start.partition_key)
            .await?;

        let matching_ids =
            self.filter_doc_ids_by_ck_range(all_doc_ids, &start, &end_ck_values, end_inclusive);

        debug!(
            "Range delete resolved: {} docs match the CK range",
            matching_ids.len(),
        );

        for doc_id in matching_ids {
            let batch_item = BatchItem {
                target_node: start.target_node.clone(),
                id: doc_id,
                op_type: OpType::Delete as i32,
                writetime: start.writetime,
                cdc_ttl: None,
                payload_json: String::new(),
                collection_deltas: Vec::new(),
                partition_key: Some(start.partition_key.clone()),
            };

            self.enqueue_batch_item(batch_item).await?;
        }

        Ok(())
    }

    /// Connects to the target node and retrieves all document IDs for the given partition key.
    async fn fetch_doc_ids_for_partition(
        &self,
        target_node: &str,
        partition_key: &str,
    ) -> anyhow::Result<Vec<String>> {
        let address = if !target_node.starts_with("http") {
            format!("http://{}", target_node)
        } else {
            target_node.to_owned()
        };

        let mut client = IndexServiceClient::connect(address)
            .await
            .context("connect to node for range delete resolution")?;

        let response = client
            .list_document_ids_by_partition_key(tonic::Request::new(ListDocIdsRequest {
                partition_key: partition_key.to_owned(),
            }))
            .await
            .context("gRPC request to list document IDs by partition key")?;

        Ok(response.into_inner().document_ids)
    }

    /// Filters a list of document IDs to those whose clustering key falls within
    /// the given range bounds.
    ///
    /// Doc ID format: `pk1:pk2:...:ck1:ck2:...`
    fn filter_doc_ids_by_ck_range(
        &self,
        doc_ids: Vec<String>,
        start: &PendingRangeStart,
        end_ck_values: &[Option<String>],
        end_inclusive: bool,
    ) -> Vec<String> {
        let pk_col_count = self.partition_key_columns.len();
        let ck_col_count = self.clustering_key_columns.len();

        doc_ids
            .into_iter()
            .filter(|doc_id| {
                let parts: Vec<&str> = doc_id.split(':').collect();
                if parts.len() != pk_col_count + ck_col_count {
                    return false;
                }

                let ck_parts = &parts[pk_col_count..];

                for (i, ck_col) in self.clustering_key_columns.iter().enumerate() {
                    let doc_ck_val = ck_parts[i];

                    if let Some(ref start_val) = start.ck_values[i] {
                        let cmp =
                            utils::compare_cql_values(doc_ck_val, start_val, &ck_col.cql_type);
                        if start.start_inclusive {
                            if cmp == std::cmp::Ordering::Less {
                                return false;
                            }
                        } else if cmp != std::cmp::Ordering::Greater {
                            return false;
                        }
                    }

                    if let Some(ref end_val) = end_ck_values[i] {
                        let cmp = utils::compare_cql_values(doc_ck_val, end_val, &ck_col.cql_type);
                        if end_inclusive {
                            if cmp == std::cmp::Ordering::Greater {
                                return false;
                            }
                        } else if cmp != std::cmp::Ordering::Less {
                            return false;
                        }
                    }
                }

                true
            })
            .collect()
    }

    /// Serializes and enqueues a `BatchItem` into the batch service.
    async fn enqueue_batch_item(&self, batch_item: BatchItem) -> anyhow::Result<()> {
        let batch_item_json =
            serde_json::to_string(&batch_item).context("serialize batch item to JSON")?;
        debug!("Batch item JSON: {}", batch_item_json);

        match self.batch_service.add(batch_item_json).await {
            Ok(_) => Ok(()),
            Err(e) => {
                tracing::debug!(
                    target: "test_event",
                    source = %TestEventSource::Ingestor,
                    event = %TestEvent::BatchAddFailure,
                    table = self.table_name,
                    id = batch_item.id,
                    target_node = batch_item.target_node,
                    error = e.to_string()
                );
                Err(anyhow::anyhow!("Failed to add batch item: {}", e))
            }
        }
    }
}
