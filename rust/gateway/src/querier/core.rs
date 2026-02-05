use crate::server::core::AppState;
use futures::future::join_all;
use serde::{Deserialize, Serialize};
use std::{error::Error, sync::Arc};
use tantylla_common::indexer::{
    SearchHit, SearchRequest, SearchResponse, search_request::Consistency,
};
use tonic::Request;

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct ScatterFail {
    message: String,
    errors: Vec<String>,
}

impl std::fmt::Display for ScatterFail {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.errors.is_empty() {
            write!(f, "{}", self.message)
        } else {
            write!(f, "{}: {}", self.message, self.errors.join("; "))
        }
    }
}

impl Error for ScatterFail {}

/// Broadcasts the query to all connected nodes and aggregates results.
///
/// Consistency levels:
/// - ANY (default): Query succeeds if at least one node responds. Failed nodes are logged but ignored.
/// - ALL: Query must succeed on all nodes. If any node fails, the entire query fails.
pub async fn scatter_gather(
    state: Arc<AppState>,
    req: SearchRequest,
) -> Result<SearchResponse, ScatterFail> {
    let clients = &state.clients;
    let consistency = req.consistency();
    let total_nodes = clients.len();

    let futures: Vec<_> = clients
        .iter()
        .map(|client| {
            let mut client = client.clone();
            let req = req.clone();
            async move {
                // We request 'limit + offset' from every node to ensure global sorting accuracy
                // then we slice it locally.
                let mut node_req = req;
                node_req.limit += node_req.offset;
                node_req.offset = 0; // We handle offset globally after merging

                client.search(Request::new(node_req)).await
            }
        })
        .collect();

    let results = join_all(futures).await;

    let mut all_hits: Vec<SearchHit> = Vec::new();
    let mut total_hits: u64 = 0;
    let mut max_duration: u64 = 0;
    let mut failed_nodes: Vec<String> = Vec::new();
    let mut successful_nodes = 0;

    for (idx, res) in results.into_iter().enumerate() {
        match res {
            Ok(response) => {
                let inner = response.into_inner();
                all_hits.extend(inner.hits);
                total_hits += inner.total_hits;
                max_duration = std::cmp::max(max_duration, inner.duration_ms);
                successful_nodes += 1;
            }
            Err(e) => {
                let code = e.code();
                let msg = e.message();
                let error_msg = format!("Node {} search failed: {} ({:?})", idx, code, msg);
                tracing::error!("{}", error_msg);
                failed_nodes.push(error_msg);
            }
        }
    }

    match consistency {
        Consistency::All => {
            // ALL consistency: every node must succeed
            if successful_nodes < total_nodes {
                let error_msg = format!(
                    "Consistency ALL failed: {}/{} nodes succeeded",
                    successful_nodes, total_nodes,
                );
                tracing::error!("{}", error_msg);
                return Err(ScatterFail {
                    message: error_msg,
                    errors: failed_nodes,
                });
            }
        }
        _ => {
            // ANY or UNSPECIFIED (default): at least one node must succeed
            if successful_nodes == 0 {
                let error_msg =
                    format!("Consistency ANY failed: all {} nodes failed.", total_nodes);
                tracing::error!("{}", error_msg);
                return Err(ScatterFail {
                    message: error_msg,
                    errors: failed_nodes,
                });
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
