use axum::{
    extract::{Json, State},
    http::StatusCode,
    response::IntoResponse,
};
use std::sync::Arc;
use tantylla_common::indexer::SearchRequest;

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

    tracing::info!(
        target: "test_event",
        source = "gateway",
        event = "search_request",
        query,
        limit,
        offset,
        consistency
    );

    match scatter_gather(state.clone(), payload).await {
        Ok(response) => {
            tracing::info!(
                target: "test_event",
                source = "gateway",
                event = "search_response",
                total_hits = response.total_hits,
                hit_count = response.hits.len(),
                duration_ms = response.duration_ms
            );
            (StatusCode::OK, Json(response)).into_response()
        }
        Err(e) => {
            tracing::info!(
                target: "test_event",
                source = "gateway",
                event = "search_failure",
                error = e.to_string()
            );
            (StatusCode::INTERNAL_SERVER_ERROR, Json(e)).into_response()
        }
    }
}
