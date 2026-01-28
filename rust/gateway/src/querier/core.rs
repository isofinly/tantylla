use crate::server::core::AppState;
use futures::future::join_all;
use std::sync::Arc;
use tantylla_common::indexer::{SearchHit, SearchRequest, SearchResponse};
use tonic::Request;

/// Broadcasts the query to all connected nodes and aggregates results.
//
// TODO: Never fails. Always returns a valid response.
// 2026-01-28 12:32:56.829 ERROR request{method=POST uri=/api/v1/search version=HTTP/1.1}: tantylla_gateway::querier::core: gateway/src/querier/core.rs:46: Node search failed: status: 'The service is currently unavailable', self: "tcp connect error"
// 2026-01-28 12:32:56.829  INFO request{method=POST uri=/api/v1/search version=HTTP/1.1}: tower_http::trace::on_response: /Users/isofinly/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/tower-http-0.6.8/src/trace/on_response.rs:114: finished processing request latency=3 ms status=200
pub async fn scatter_gather(
    state: Arc<AppState>,
    req: SearchRequest,
) -> Result<SearchResponse, String> {
    let clients = &state.clients;

    let futures: Vec<_> = clients
        .iter()
        .map(|client| {
            let mut client = client.clone();
            let req = req.clone();
            async move {
                // We request 'limit + offset' from every node to ensure global sorting accuracy
                // then we slice it locally.
                let mut node_req = req;
                node_req.limit = node_req.limit + node_req.offset;
                node_req.offset = 0; // We handle offset globally after merging

                client.search(Request::new(node_req)).await
            }
        })
        .collect();

    let results = join_all(futures).await;

    let mut all_hits: Vec<SearchHit> = Vec::new();
    let mut total_hits: u64 = 0;
    let mut max_duration: u64 = 0;

    for res in results {
        match res {
            Ok(response) => {
                let inner = response.into_inner();
                all_hits.extend(inner.hits);
                total_hits += inner.total_hits;
                max_duration = std::cmp::max(max_duration, inner.duration_ms);
            }
            Err(e) => {
                tracing::error!("Node search failed: {}", e);
            }
        }
    }

    all_hits.sort_by(|a, b| {
        b.score
            .partial_cmp(&a.score)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    let offset = req.offset as usize;
    let limit = req.limit as usize;

    let paged_hits = if offset >= all_hits.len() {
        Vec::new()
    } else {
        all_hits.into_iter().skip(offset).take(limit).collect()
    };

    Ok(SearchResponse {
        hits: paged_hits,
        total_hits,
        duration_ms: max_duration,
    })
}
