use clap::Parser;
use std::sync::Arc;
use tantylla_common::indexer::index_service_client::IndexServiceClient;
use tantylla_common::logger;
use tonic::transport::{Channel, Endpoint};

mod querier;
mod server;

use server::core::AppState;
use server::router::create_router;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(long, default_value = "8080")]
    port: u16,
    #[arg(long, default_value = "0.0.0.0")]
    address: String,
    #[arg(
        long,
        value_delimiter = ',',
        help = "List of node addresses, e.g. http://127.0.0.1:10000,http://127.0.0.1:10001",
        required = true
    )]
    search_nodes: Vec<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    logger::init_logger_with_config(logger::LoggerConfig {
        level: "debug".to_string(),
        file_path: None,
        file_level: "info".to_string(),
    });

    tracing::info!("Initializing Gateway...");

    let mut clients: Vec<IndexServiceClient<Channel>> = Vec::new();
    let mut nodes = args.search_nodes.clone();
    nodes.sort();
    for addr in nodes {
        let addr = if !addr.starts_with("http") {
            format!("http://{}", addr)
        } else {
            addr
        };

        tracing::info!("Connecting to search node: {}", addr);

        let channel = Endpoint::from_shared(addr.clone())?.connect_lazy();

        let client = IndexServiceClient::new(channel);
        clients.push(client);
    }

    let state = Arc::new(AppState { clients });

    let app = create_router(state);
    let bind_addr = format!("{}:{}", args.address, args.port);
    let listener = tokio::net::TcpListener::bind(&bind_addr).await?;

    tracing::info!("Gateway listening on {}", bind_addr);

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}
