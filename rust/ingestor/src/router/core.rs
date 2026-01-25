use std::sync::Arc;

use ahash::AHashMap;
use anyhow::Result;
use scylla::client::session::Session;
use scylla_cdc::consumer::OperationType;

use tantylla_common::indexer::index_operation::OpType;
use tracing::debug;

use crate::{
    batch::service::{BatchItem, Service},
    router::utils,
};

#[derive(Debug)]
pub(crate) struct Router {
    node_info: AHashMap<usize, String>,
    batch_service: Service,
    pk_columns: Vec<String>,
}

impl Router {
    pub async fn new(
        node_info: AHashMap<usize, String>,
        keyspace: String,
        table: String,
        session: Arc<Session>,
    ) -> Result<Self> {
        let pk_columns =
            utils::get_partition_key_columns(session.clone(), &keyspace, &table).await?;

        Ok(Router {
            node_info,
            batch_service: Service::default(),
            pk_columns,
        })
    }

    pub async fn with_ticker_and_limit(
        flush_interval: tokio::time::Duration,
        batch_limit: usize,
        node_info: AHashMap<usize, String>,
        session: Arc<Session>,
        keyspace: String,
        table: String,
    ) -> Result<Self> {
        let pk_columns =
            utils::get_partition_key_columns(session.clone(), &keyspace, &table).await?;

        Ok(Router {
            node_info,
            pk_columns,
            batch_service: Service::with_ticker_and_limit(flush_interval, batch_limit),
        })
    }

    pub(crate) async fn route(&self, row: &scylla_cdc::consumer::CDCRow<'_>) -> anyhow::Result<()> {
        // TODO: This is not a partition key
        let target_node_id = utils::get_target_node_id(row, &self.pk_columns, self.node_info.len());
        let target_node = self.node_info.get(&target_node_id).unwrap();

        let op_type = match row.operation {
            OperationType::RowInsert | OperationType::RowUpdate => OpType::Upsert,
            OperationType::RowDelete | OperationType::PartitionDelete => OpType::Delete,
            // TODO: Not all operations are supported yet
            _ => return Ok(()),
        };

        let id = format!("{}:{}:{}", target_node_id, row.time, row.batch_seq_no);
        let writetime = utils::extract_writetime_from_timeuuid(row.time)?;
        let cdc_ttl = row.ttl;
        let payload_json = if matches!(op_type, OpType::Upsert) {
            utils::serialize_row_to_json(row)?
        } else {
            String::new()
        };

        let batch_item = BatchItem {
            // TODO
            target_node: target_node.clone(),
            id,
            op_type: op_type as i32,
            writetime,
            cdc_ttl,
            payload_json,
        };

        let batch_item_json = serde_json::to_string(&batch_item)?;
        debug!("Batch item JSON: {}", batch_item_json);

        self.batch_service.add(batch_item_json);

        Ok(())
    }
}
