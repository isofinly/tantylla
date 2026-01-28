use crate::server::{core::AppState, handlers::search_handler};
use axum::{
    Json, Router,
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
};
use std::sync::Arc;
use tower_http::trace::{self, TraceLayer};
use tracing::Level;

pub fn create_router(state: Arc<AppState>) -> Router {
    Router::new()
        .route(
            "/api/health",
            get(|| async { (StatusCode::OK, Json("OK")).into_response() }),
        )
        .route("/api/v1/search", post(search_handler))
        .with_state(state)
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(trace::DefaultMakeSpan::new().level(Level::INFO))
                .on_request(trace::DefaultOnRequest::new().level(Level::INFO))
                .on_response(
                    trace::DefaultOnResponse::new()
                        .level(Level::INFO)
                        .latency_unit(tower_http::LatencyUnit::Millis),
                )
                .on_failure(trace::DefaultOnFailure::new().level(Level::ERROR)),
        )
}
