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
    table_name: String,
}

impl Router {
    pub async fn new(
        node_info: AHashMap<usize, String>,
        keyspace: &String,
        table: &String,
        session: Arc<Session>,
        batch_service: Service,
    ) -> Result<Self> {
        let pk_columns = utils::get_partition_key_columns(session.clone(), keyspace, table).await?;

        Ok(Router {
            node_info,
            batch_service,
            pk_columns,
            table_name: format!("{}.{}", keyspace, table),
        })
    }

    pub(crate) async fn route(&self, row: &scylla_cdc::consumer::CDCRow<'_>) -> anyhow::Result<()> {
        // TODO: Is this really a router's concern?
        if self.batch_service.is_backpressure_active() {
            debug!("Backpressure active, attempting to flush before routing");
            if let Err(e) = self.batch_service.flush().await {
                return Err(anyhow::anyhow!(
                    "Backpressure active and flush failed: {}",
                    e
                ));
            }
        }

        let target_node_id = utils::get_target_node_id(row, &self.pk_columns, self.node_info.len());
        let target_node = self.node_info.get(&target_node_id).unwrap();

        let op_type = match row.operation {
            OperationType::RowInsert | OperationType::RowUpdate | OperationType::PostImage => {
                OpType::Upsert
            }
            OperationType::RowDelete | OperationType::PartitionDelete => OpType::Delete,
            // TODO: Not all operations are supported yet
            _ => return Ok(()),
        };

        let pk_values: Vec<String> = self
            .pk_columns
            .iter()
            .map(|col_name| match row.get_value(col_name) {
                Some(val) => format!("{}", val),
                None => "null".to_string(),
            })
            .collect();

        let id = pk_values.join(":");

        tracing::info!(
            target: "test_event",
            source = "ingestor",
            event = "cdc_row_routed",
            table = self.table_name,
            id,
            op = format!("{:?}", op_type),
            target_node = target_node
        );

        let writetime = utils::extract_writetime_from_timeuuid(row.time)?;
        let cdc_ttl = row.ttl;
        let payload_json = if matches!(op_type, OpType::Upsert) {
            utils::serialize_row_to_json(row)?
        } else {
            String::new()
        };

        let batch_item = BatchItem {
            target_node: target_node.clone(),
            id: id.clone(),
            op_type: op_type as i32,
            writetime,
            cdc_ttl,
            payload_json: payload_json.clone(),
        };

        let batch_item_json = serde_json::to_string(&batch_item)?;
        debug!("Batch item JSON: {}", batch_item_json);

        // Add to batch service - this may trigger backpressure if buffer is full
        // TODO: Maybe implement a retry mechanism to try again?
        match self.batch_service.add(batch_item_json).await {
            Ok(_) => Ok(()),
            Err(e) => {
                tracing::info!(
                    target: "test_event",
                    source = "ingestor",
                    event = "batch_enqueue_failed",
                    table = self.table_name,
                    id,
                    target_node,
                    error = e.to_string()
                );
                Err(anyhow::anyhow!("Failed to add batch item: {}", e))
            }
        }
    }
}
