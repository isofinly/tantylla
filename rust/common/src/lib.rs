pub mod logger;
pub mod test_tracing;
pub mod indexer {
    tonic::include_proto!("indexer.v1");
}
