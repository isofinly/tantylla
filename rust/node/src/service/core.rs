use crate::engine::core::{AdaptiveConfig, Engine};
use anyhow::Result;
use tantylla_common::{
    self,
    indexer::{
        HealthCheckRequest, HealthCheckResponse, IndexBatchRequest, IndexBatchResponse,
        SearchRequest, SearchResponse, index_service_server::IndexService,
    },
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
        match self.engine.process_batch(req.operations) {
            Ok(response) => Ok(Response::new(response)),
            Err(err) => Err(Status::internal(err.to_string())),
        }
    }

    async fn search(
        &self,
        request: Request<SearchRequest>,
    ) -> Result<Response<SearchResponse>, Status> {
        debug!("Searching {:?}", request);
        let req = request.into_inner();
        match self
            .engine
            .search(&req.query, req.limit as usize, req.offset as usize)
        {
            Ok(response) => Ok(Response::new(response)),
            Err(err) => Err(Status::internal(err.to_string())),
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
