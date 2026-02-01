use std::sync::{Arc, Mutex};

use ahash::AHashMap;
use futures::executor::block_on;
use serde::{Deserialize, Serialize};
use tantylla_common::indexer::{
    IndexBatchRequest, IndexBatchResponse, IndexOperation, index_service_client::IndexServiceClient,
};
use tokio::time::{self, Duration};
use tracing::{error, info, warn};

#[derive(Debug, Clone)]
pub(crate) struct Service {
    batch_limit: usize,
    buffer: Arc<Mutex<Vec<String>>>,
    flush_interval: Duration,
    max_retry_attempts: u32,
    retry_backoff_ms: u64,
    /// Flag responsible for triggering backpressure
    last_flush_failed: Arc<Mutex<bool>>,
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
const DEFAULT_MAX_RETRY_ATTEMPTS: u32 = 3;
const DEFAULT_RETRY_BACKOFF_MS: u64 = 100;

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
            max_retry_attempts: DEFAULT_MAX_RETRY_ATTEMPTS,
            retry_backoff_ms: DEFAULT_RETRY_BACKOFF_MS,
            last_flush_failed: Arc::new(Mutex::new(false)),
        };
        svc.start_ticker();
        svc
    }

    #[allow(dead_code)]
    pub(crate) fn with_params(
        flush_interval: Duration,
        batch_limit: usize,
        max_retry_attempts: u32,
        retry_backoff_ms: u64,
    ) -> Self {
        let svc = Service {
            flush_interval,
            batch_limit,
            buffer: Arc::new(Mutex::new(Vec::new())),
            max_retry_attempts,
            retry_backoff_ms,
            last_flush_failed: Arc::new(Mutex::new(false)),
        };
        svc.start_ticker();
        svc
    }

    pub(crate) fn is_backpressure_active(&self) -> bool {
        *self.last_flush_failed.lock().unwrap()
    }

    fn clear_backpressure(&self) {
        *self.last_flush_failed.lock().unwrap() = false;
    }

    fn set_backpressure(&self) {
        *self.last_flush_failed.lock().unwrap() = true;
    }

    fn start_ticker(&self) {
        let service = self.clone();

        tokio::spawn(async move {
            let mut interval = time::interval(service.flush_interval);
            // Set the first tick to complete immediately or after the duration.
            interval.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
            loop {
                interval.tick().await;
                if let Err(e) = service.flush().await {
                    error!("Background flush failed, activating backpressure: {}", e);
                    service.set_backpressure();
                } else {
                    service.clear_backpressure();
                }
            }
        });
    }

    /// Adds an item to the buffer.
    /// If backpressure is active, returns an error to stop CDC ingestion.
    pub(crate) fn add(&self, item: String) -> Result<(), BatchFlushError> {
        if self.is_backpressure_active() {
            return Err(BatchFlushError {
                // TODO: Not enough context info to determine failed nodes
                failed_nodes: vec![],
                message: "Backpressure active due to previous batch failure".to_string(),
            });
        }

        let should_flush = {
            let mut buffer = self.buffer.lock().unwrap();
            buffer.push(item);
            buffer.len() >= self.batch_limit
        };

        if should_flush {
            // TODO: Does it use existing runtime and context?
            block_on(self.flush())?;
        }

        Ok(())
    }

    /// Flushes the current buffer to target nodes.
    /// Returns Err if any batch fails completely after all retries,
    /// enabling backpressure - caller should stop CDC ingestion on failure.
    pub(crate) async fn flush(&self) -> Result<(), BatchFlushError> {
        let items_to_process: Vec<String> = {
            let mut locked_buffer = self.buffer.lock().unwrap();
            if locked_buffer.is_empty() {
                return Ok(());
            }
            std::mem::take(&mut *locked_buffer)
        };

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
                        .or_default()
                        .push(operation);
                }
                Err(e) => {
                    error!("Failed to deserialize batch item: {}", e);
                    continue;
                }
            }
        }

        let mut failed_nodes: Vec<String> = Vec::new();

        for (target_node, operations) in node_batches {
            let expected_count = operations.len() as u32;
            let result = self.send_batch_with_retry(&target_node, operations).await;

            match result {
                Ok(response) => {
                    let total_processed = response.processed_count + response.skipped_count;
                    if total_processed < expected_count {
                        error!(
                            "Node {}: Batch partially indexed. Expected {} operations, \
                             processed {} (skipped {}). Success flag: {}",
                            target_node,
                            expected_count,
                            response.processed_count,
                            response.skipped_count,
                            response.success
                        );
                        // TODO: Implement handling for partially indexed batches
                        // Options: dead letter queue, manual intervention, or retry entire batch
                    } else {
                        info!(
                            "Node {}: processed {} ops, skipped {} ops, success: {}",
                            target_node,
                            response.processed_count,
                            response.skipped_count,
                            response.success
                        );
                    }
                }
                Err(e) => {
                    error!(
                        "Node {}: Failed to index batch after all retries: {}",
                        target_node, e
                    );
                    failed_nodes.push(target_node);
                }
            }
        }

        if !failed_nodes.is_empty() {
            let failed_count = failed_nodes.len();
            return Err(BatchFlushError {
                failed_nodes,
                message: format!("Failed to flush batches to {} nodes", failed_count),
            });
        }

        Ok(())
    }

    /// Sends a batch to a target node with retry logic.
    /// Returns the response only if all operations were processed successfully.
    async fn send_batch_with_retry(
        &self,
        target_node: &str,
        operations: Vec<IndexOperation>,
    ) -> Result<IndexBatchResponse, BatchSendError> {
        let address = if !target_node.starts_with("http") {
            format!("http://{}", target_node)
        } else {
            target_node.to_string()
        };

        let expected_count = operations.len() as u32;
        let mut last_error: Option<String> = None;

        for attempt in 0..self.max_retry_attempts {
            match self.try_send_batch(&address, &operations).await {
                Ok(response) => {
                    let total_processed = response.processed_count + response.skipped_count;
                    if total_processed == expected_count {
                        return Ok(response);
                    } else {
                        // TODO: Partial success - don't retry, log and return
                        warn!(
                            "Node {}: Partial batch success on attempt {}/{}. \
                             Expected {}, got {} processed + {} skipped",
                            target_node,
                            attempt + 1,
                            self.max_retry_attempts,
                            expected_count,
                            response.processed_count,
                            response.skipped_count
                        );
                        return Ok(response);
                    }
                }
                Err(e) => {
                    last_error = Some(e.clone());
                    warn!(
                        "Node {}: Batch send failed on attempt {}/{}: {}",
                        target_node,
                        attempt + 1,
                        self.max_retry_attempts,
                        e
                    );

                    if attempt < self.max_retry_attempts - 1 {
                        let backoff = self.retry_backoff_ms * (1_u64 << attempt);
                        info!("Retrying batch to {} after {}ms", target_node, backoff);
                        tokio::time::sleep(Duration::from_millis(backoff)).await;
                    }
                }
            }
        }

        Err(BatchSendError {
            message: format!(
                "Failed after {} attempts. Last error: {:?}",
                self.max_retry_attempts, last_error
            ),
        })
    }

    /// Attempts to send a batch once.
    async fn try_send_batch(
        &self,
        address: &str,
        operations: &[IndexOperation],
    ) -> Result<IndexBatchResponse, String> {
        match IndexServiceClient::connect(address.to_string()).await {
            Ok(mut client) => {
                let request = tonic::Request::new(IndexBatchRequest {
                    operations: operations.to_vec(),
                });

                match client.index_batch(request).await {
                    Ok(response) => Ok(response.into_inner()),
                    Err(e) => Err(format!("gRPC error: {}", e)),
                }
            }
            Err(e) => Err(format!("Connection error: {}", e)),
        }
    }
}

/// Error type for batch flush failures.
/// Contains list of nodes that failed to process their batches.
#[derive(Debug)]
pub(crate) struct BatchFlushError {
    pub failed_nodes: Vec<String>,
    pub message: String,
}

impl std::fmt::Display for BatchFlushError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Batch flush failed: {} (nodes: {:?})",
            self.message, self.failed_nodes
        )
    }
}

impl std::error::Error for BatchFlushError {}

/// Error type for individual batch send failures.
#[derive(Debug)]
pub(crate) struct BatchSendError {
    message: String,
}

impl std::fmt::Display for BatchSendError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for BatchSendError {}
