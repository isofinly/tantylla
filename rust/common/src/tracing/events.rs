use serde::{Deserialize, Serialize};
use strum::EnumDiscriminants;
use strum_macros::{Display, EnumString, IntoStaticStr};

#[derive(
    Debug,
    Display,
    Clone,
    Default,
    PartialEq,
    Eq,
    Hash,
    Copy,
    Serialize,
    Deserialize,
    IntoStaticStr,
    EnumString,
)]
#[serde(tag = "source")]
pub enum TestEventSource {
    Gateway,
    Ingestor,
    Node,
    #[default]
    Unspecified,
}

#[derive(Debug, Clone, Serialize, Deserialize, EnumDiscriminants)]
#[serde(tag = "event")]
#[strum_discriminants(
    name(TestEvent),
    derive(Hash, EnumString, Display, IntoStaticStr, Serialize, Deserialize)
)]
pub enum TracePayload {
    Startup {},
    GatewayScatterGatherEnter {},
    SearchRequest {
        query: String,
        #[serde(default)]
        limit: u32,
        #[serde(default)]
        offset: u32,
        #[serde(default)]
        consistency: i32,
    },
    SearchResponse {
        #[serde(default)]
        hit_count: usize,
        #[serde(default)]
        total_hits: u64,
        #[serde(default)]
        duration_ms: u64,
    },
    SearchFailure {
        message: Option<String>,
    },
    EngineSearchEnter {},
    BatchAddEnter {},
    BatchAddFailure {},
    EngineProcessBatchEnter {},
    IndexBatchRequest {
        #[serde(default)]
        operation_count: usize,
    },
    IndexBatchFailure {
        #[serde(default)]
        error: Option<String>,
    },
    BatchFlushStart {
        table: String,
        item_count: usize,
    },
    BatchFlushNodeSuccess {
        table: String,
        target_node: String,
        processed_count: u32,
        skipped_count: u32,
        success: bool,
    },
    BatchFlushNodeFailure {
        table: String,
        target_node: String,
        error: String,
    },
    BatchFlushFailed {
        table: String,
        failed_nodes: Vec<String>,
    },
    BatchFlushSuccess {
        table: String,
    },
    CdcRowReceived {
        operation: String,
    },
    CdcRowRouted {
        node_count: usize,
    },
    IndexBatchResponse {
        processed_count: u32,
        skipped_count: u32,
        success: bool,
    },
    #[serde(other)]
    Unknown,
}
