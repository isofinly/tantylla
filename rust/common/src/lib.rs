pub mod logger;
pub mod tracing;
pub mod indexer {
    tonic::include_proto!("indexer.v1");
}
pub use tracing_appender::non_blocking::WorkerGuard;
