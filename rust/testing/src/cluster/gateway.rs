use anyhow::{Context, Result, bail};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::time::Duration;
use tokio::time::sleep;

#[derive(Debug, Serialize)]
pub struct SearchRequest {
    pub query: String,
    pub limit: usize,
    pub offset: usize,
    pub consistency: i32,
}

#[derive(Debug, Deserialize)]
pub struct SearchResponse {
    pub total_hits: u64,
    pub hits: Vec<Value>,
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
        let response = self.client
            .post(format!("{}/api/v1/search", self.base_url))
            .json(req)
            .send()
            .await
            .context("sending gateway search request")?;

        let status = response.status();
        let body = response.text().await.context("reading gateway response body")?;

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
