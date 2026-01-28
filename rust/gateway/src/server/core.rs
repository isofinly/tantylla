use tantylla_common::indexer::index_service_client::IndexServiceClient;
use tonic::transport::Channel;

pub struct AppState {
    /// A list of gRPC clients. Tonic clients are cheap to clone.
    pub clients: Vec<IndexServiceClient<Channel>>,
}
