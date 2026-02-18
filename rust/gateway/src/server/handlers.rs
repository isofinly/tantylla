use axum::{
    extract::{Json, State},
    http::StatusCode,
    response::IntoResponse,
};
use std::sync::Arc;
use tantylla_common::{
    indexer::SearchRequest,
    tracing::events::{TestEvent, TestEventSource},
};

use crate::{querier::core::scatter_gather, server::core::AppState};

/// POST /api/v1/search
pub async fn search_handler(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<SearchRequest>,
) -> impl IntoResponse {
    let query = payload.query.clone();
    let limit = payload.limit;
    let offset = payload.offset;
    let consistency = payload.consistency;

    tracing::debug!(
        target: "test_event",
        source = %TestEventSource::Gateway,
        event = %TestEvent::SearchRequest,
        query,
        limit,
        offset,
        consistency
    );

    match scatter_gather(state.clone(), payload).await {
        Ok(response) => {
            tracing::debug!(
                target: "test_event",
                source = %TestEventSource::Gateway,
                event = %TestEvent::SearchResponse,
                total_hits = response.total_hits,
                hit_count = response.hits.len(),
                duration_ms = response.duration_ms
            );
            (StatusCode::OK, Json(response)).into_response()
        }
        Err(e) => {
            tracing::debug!(
                target: "test_event",
                source = %TestEventSource::Gateway,
                event = %TestEvent::SearchFailure,
                error = e.to_string()
            );
            (StatusCode::INTERNAL_SERVER_ERROR, Json(e)).into_response()
        }
    }
}
