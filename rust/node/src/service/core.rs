use crate::engine::core::{AdaptiveConfig, Engine};
use anyhow::Result;
use tantylla_common::indexer::{
    HealthCheckRequest, HealthCheckResponse, IndexBatchRequest, IndexBatchResponse, SearchRequest,
    SearchResponse, index_service_server::IndexService,
};
use tonic::{Request, Response, Status};
use tracing::debug;

#[derive(Clone)]
pub struct IndexServiceService {
    engine: Engine,
}

impl IndexServiceService {
    pub fn new(engine_path: impl AsRef<std::path::Path>, config: AdaptiveConfig) -> Result<Self> {
        let engine = Engine::new(engine_path, config)?;
        Ok(IndexServiceService { engine })
    }
}

#[tonic::async_trait]
impl IndexService for IndexServiceService {
    async fn index_batch(
        &self,
        request: Request<IndexBatchRequest>,
    ) -> Result<Response<IndexBatchResponse>, Status> {
        debug!("Indexing batch with {:?}", request);
        let req = request.into_inner();
        let operation_count = req.operations.len();

        tracing::info!(
            target: "test_event",
            source = "node",
            event = "index_batch_request",
            operation_count
        );

        match self.engine.process_batch(req.operations) {
            Ok(response) => {
                tracing::info!(
                    target: "test_event",
                    source = "node",
                    event = "index_batch_response",
                    processed_count = response.processed_count,
                    skipped_count = response.skipped_count,
                    success = response.success
                );
                Ok(Response::new(response))
            }
            Err(err) => {
                tracing::info!(
                    target: "test_event",
                    source = "node",
                    event = "index_batch_failure",
                    error = err.to_string()
                );
                Err(Status::internal(err.to_string()))
            }
        }
    }

    async fn search(
        &self,
        request: Request<SearchRequest>,
    ) -> Result<Response<SearchResponse>, Status> {
        debug!("Searching {:?}", request);
        let req = request.into_inner();
        let query = req.query.clone();
        let limit = req.limit;
        let offset = req.offset;

        tracing::info!(
            target: "test_event",
            source = "node",
            event = "search_request",
            query,
            limit,
            offset
        );

        match self
            .engine
            .search(&req.query, req.limit as usize, req.offset as usize)
        {
            Ok(response) => {
                tracing::info!(
                    target: "test_event",
                    source = "node",
                    event = "search_response",
                    total_hits = response.total_hits,
                    duration_ms = response.duration_ms
                );
                Ok(Response::new(response))
            }
            Err(err) => {
                tracing::info!(
                    target: "test_event",
                    source = "node",
                    event = "search_failure",
                    error = err.to_string()
                );
                Err(Status::internal(err.to_string()))
            }
        }
    }

    async fn health_check(
        &self,
        _request: Request<HealthCheckRequest>,
    ) -> Result<Response<HealthCheckResponse>, Status> {
        debug!("Health check");
        Ok(Response::new(HealthCheckResponse { serving: true }))
    }
}
