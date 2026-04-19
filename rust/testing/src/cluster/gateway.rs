use anyhow::{Context, Result, bail};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::time::Duration;
use tokio::time::sleep;

#[derive(Debug, Clone, Serialize)]
pub struct BoostField {
    pub field: String,
    pub boost: f32,
}

#[derive(Debug, Serialize)]
pub struct SearchRequest {
    pub query: String,
    pub limit: usize,
    pub offset: usize,
    pub consistency: i32,
    /// Sub-field names (without the `document.` prefix) to use as default
    /// search fields when the query does not contain an explicit field prefix.
    /// Maps to the `default_fields` field in the proto `SearchRequest`.
    /// An empty vec disables the feature (legacy behaviour).
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub default_fields: Vec<String>,
    /// Sub-field names to aggregate bucket counts over in the response.
    /// Empty means no facets are requested.
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub facet_fields: Vec<String>,
    /// Per-field boost weights for bare-keyword query expansion.
    /// Empty means no boosted expansion; falls back to `default_fields` or legacy.
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub boost_fields: Vec<BoostField>,
    /// When true, deduplicate hits by partition key before applying limit/offset.
    #[serde(default)]
    pub group_by_partition: bool,
}

#[derive(Debug, Deserialize)]
pub struct FacetBucket {
    pub value: String,
    pub count: u64,
}

#[derive(Debug, Deserialize)]
pub struct FacetResult {
    pub field: String,
    pub buckets: Vec<FacetBucket>,
}

#[derive(Debug, Deserialize)]
pub struct SearchResponse {
    pub total_hits: u64,
    pub hits: Vec<Value>,
    /// Facet aggregation results; empty when `facet_fields` was not requested.
    #[serde(default)]
    pub facets: Vec<FacetResult>,
}

pub struct GatewayClient {
    base_url: String,
    client: Client,
}

impl GatewayClient {
    pub fn new(addr: &str) -> Self {
        Self {
            base_url: format!("http://{}", addr),
            client: Client::new(),
        }
    }

    pub async fn search(&self, req: &SearchRequest) -> Result<SearchResponse> {
        let response = self
            .client
            .post(format!("{}/api/v1/search", self.base_url))
            .json(req)
            .send()
            .await
            .context("sending gateway search request")?;

        let status = response.status();
        let body = response
            .text()
            .await
            .context("reading gateway response body")?;

        if !status.is_success() {
            bail!("gateway returned error status {}: {}", status, body);
        }

        serde_json::from_str(&body).with_context(|| format!("parsing gateway response: {}", body))
    }

    pub async fn search_until_hits(
        &self,
        queries: &[&str],
        retries_per_query: usize,
    ) -> Result<(SearchResponse, String)> {
        for &query in queries {
            for _ in 0..retries_per_query {
                let req = SearchRequest {
                    query: query.to_string(),
                    limit: 10,
                    offset: 0,
                    consistency: 1,
                    default_fields: vec![],
                    facet_fields: vec![],
                    boost_fields: vec![],
                    group_by_partition: false,
                };

                match self.search(&req).await {
                    Ok(resp) if resp.total_hits > 0 => {
                        return Ok((resp, query.to_string()));
                    }
                    Ok(_resp) => {
                        tracing::debug!(query, "no hits yet, retrying...");
                    }
                    Err(e) => {
                        tracing::warn!(query, error = %e, "search failed, retrying...");
                    }
                }
                sleep(Duration::from_millis(500)).await;
            }
        }
        bail!("failed to get hits for any of the queries after retries");
    }
}
