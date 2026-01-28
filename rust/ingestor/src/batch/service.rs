use std::sync::{Arc, Mutex};

use ahash::AHashMap;
use serde::{Deserialize, Serialize};
use tantylla_common::indexer::{
    IndexBatchRequest, IndexOperation, index_service_client::IndexServiceClient,
};
use tokio::time::{self, Duration};
use tracing::{error, info};

#[derive(Debug, Clone)]
pub(crate) struct Service {
    batch_limit: usize,
    buffer: Arc<Mutex<Vec<String>>>,
    flush_interval: Duration,
}

#[derive(Serialize, Deserialize)]
pub(crate) struct BatchItem {
    pub target_node: String,
    pub id: String,
    pub op_type: i32,
    pub writetime: i64,
    pub cdc_ttl: Option<i64>,
    pub payload_json: String,
}

const DEFAULT_FLUSH_INTERVAL: Duration = Duration::from_millis(500);
const DEFAULT_BATCH_LIMIT: usize = 250;

impl Default for Service {
    fn default() -> Self {
        Self::new()
    }
}

impl Service {
    pub(crate) fn new() -> Self {
        let svc = Self {
            flush_interval: DEFAULT_FLUSH_INTERVAL,
            batch_limit: DEFAULT_BATCH_LIMIT,
            buffer: Arc::new(Mutex::new(Vec::new())),
        };
        svc.start_ticker();
        svc
    }

    pub(crate) fn with_ticker_and_limit(flush_interval: Duration, batch_limit: usize) -> Self {
        let svc = Service {
            flush_interval,
            batch_limit,
            buffer: Arc::new(Mutex::new(Vec::new())),
        };
        svc.start_ticker();
        svc
    }

    /// Spawns the background ticker task.
    fn start_ticker(&self) {
        let service = self.clone();

        tokio::spawn(async move {
            let mut interval = time::interval(service.flush_interval);
            // Set the first tick to complete immediately or after the duration.
            interval.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
            loop {
                interval.tick().await;
                service.flush();
            }
        });
    }

    pub(crate) fn add(&self, item: String) {
        let should_flush = {
            let mut buffer = self.buffer.lock().unwrap();
            buffer.push(item);
            buffer.len() >= self.batch_limit
        };

        if should_flush {
            self.flush();
        }
    }

    pub(crate) fn flush(&self) {
        let mut locked_buffer = self.buffer.lock().unwrap();
        if locked_buffer.is_empty() {
            return;
        }

        let items_to_process: Vec<String> = std::mem::take(&mut *locked_buffer);
        drop(locked_buffer);

        info!("Flushing batch of {} items", items_to_process.len());

        let mut node_batches: AHashMap<String, Vec<IndexOperation>> = AHashMap::new();

        for item in items_to_process {
            match serde_json::from_str::<BatchItem>(&item) {
                Ok(batch_item) => {
                    let operation = IndexOperation {
                        id: batch_item.id,
                        op_type: batch_item.op_type,
                        writetime: batch_item.writetime,
                        cdc_ttl: batch_item.cdc_ttl,
                        payload_json: batch_item.payload_json,
                    };

                    node_batches
                        .entry(batch_item.target_node)
                        .or_insert_with(Vec::new)
                        .push(operation);
                }
                Err(e) => {
                    error!("Failed to deserialize batch item: {}", e);
                    continue;
                }
            }
        }

        for (target_node, operations) in node_batches {
            tokio::spawn(async move {
                // TODO: Assumes all communication is http.
                let address = if !target_node.starts_with("http") {
                    format!("http://{}", target_node)
                } else {
                    target_node.clone()
                };
                match IndexServiceClient::connect(address.clone()).await {
                    Ok(mut client) => {
                        let request = tonic::Request::new(IndexBatchRequest { operations });

                        match client.index_batch(request).await {
                            Ok(response) => {
                                let resp = response.get_ref();
                                info!(
                                    "Node {} ({}): processed {} ops, skipped {} ops, success: {}",
                                    target_node,
                                    address,
                                    resp.processed_count,
                                    resp.skipped_count,
                                    resp.success
                                );
                            }
                            Err(e) => {
                                error!(
                                    "Failed to send batch to node {} ({}): {}",
                                    target_node, address, e
                                );
                                // TODO: Implement retry logic or dead letter queue
                            }
                        }
                    }
                    Err(e) => {
                        error!(
                            "Failed to connect to node {} ({}): {}",
                            target_node, address, e
                        );
                        // TODO: Implement retry logic or dead letter queue
                    }
                }
            });
        }
    }
}
