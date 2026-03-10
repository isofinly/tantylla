use crate::engine::core::{AdaptiveConfig, Engine, SearchParams};
use anyhow::Result;
use tantylla_common::{
    indexer::{
        HealthCheckRequest, HealthCheckResponse, IndexBatchRequest, IndexBatchResponse,
        ListDocIdsRequest, ListDocIdsResponse, SearchRequest, SearchResponse,
        index_service_server::IndexService,
    },
    tracing::events::{TestEvent, TestEventSource},
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

        tracing::debug!(
            target: "test_event",
            source = %TestEventSource::Node,
            event = %TestEvent::IndexBatchRequest,
            operation_count
        );

        match self.engine.process_batch(req.operations) {
            Ok(response) => {
                tracing::debug!(
                    target: "test_event",
                    source = %TestEventSource::Node,
                    event = %TestEvent::IndexBatchResponse,
                    processed_count = response.processed_count,
                    skipped_count = response.skipped_count,
                    success = response.success
                );
                Ok(Response::new(response))
            }
            Err(err) => {
                tracing::debug!(
                    target: "test_event",
                    source = %TestEventSource::Node,
                    event = %TestEvent::IndexBatchFailure,
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

        if limit == 0 {
            return Err(Status::invalid_argument(
                "Limit must be strictly greater than 0",
            ));
        }

        tracing::debug!(
            target: "test_event",
            source = %TestEventSource::Node,
            event = %TestEvent::SearchRequest,
            query,
            limit,
            offset
        );

        match self.engine.search(SearchParams {
            query_str: &req.query,
            limit: req.limit as usize,
            offset: req.offset as usize,
            default_fields: &req.default_fields,
            facet_fields: &req.facet_fields,
            boost_fields: &req.boost_fields,
            group_by_partition: req.group_by_partition,
        }) {
            Ok(response) => {
                tracing::debug!(
                    target: "test_event",
                    source = %TestEventSource::Node,
                    event = %TestEvent::SearchResponse,
                    total_hits = response.total_hits,
                    duration_ms = response.duration_ms
                );
                Ok(Response::new(response))
            }
            Err(err) => {
                tracing::debug!(
                    target: "test_event",
                    source = %TestEventSource::Node,
                    event = %TestEvent::SearchFailure,
                    error = err.to_string()
                );
                Err(Status::internal(err.to_string()))
            }
        }
    }

    async fn list_document_ids_by_partition_key(
        &self,
        request: Request<ListDocIdsRequest>,
    ) -> Result<Response<ListDocIdsResponse>, Status> {
        let req = request.into_inner();
        debug!(
            "Listing document IDs for partition_key={}",
            req.partition_key
        );

        let document_ids = self
            .engine
            .list_document_ids_by_partition_key(&req.partition_key);

        Ok(Response::new(ListDocIdsResponse { document_ids }))
    }

    async fn health_check(
        &self,
        _request: Request<HealthCheckRequest>,
    ) -> Result<Response<HealthCheckResponse>, Status> {
        debug!("Health check");
        Ok(Response::new(HealthCheckResponse { serving: true }))
    }
}
