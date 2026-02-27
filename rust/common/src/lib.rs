pub mod logger;
pub mod tracing;
pub mod indexer {
    tonic::include_proto!("indexer.v1");
}
