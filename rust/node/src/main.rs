use anyhow::Context;
use clap::Parser;
use tantylla_common::test_tracing::TestEventLayer;
use tantylla_common::{indexer::index_service_server::IndexServiceServer, logger};
use tonic::transport::Server;
use tracing::info;

use crate::engine::core::AdaptiveConfig;
use crate::service::core::IndexServiceService;

mod engine;
mod service;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(long, default_value = "10000")]
    port: u16,
    #[arg(long, default_value = "[::1]")]
    address: String,

    /// Commit interval in seconds (documents become visible after this time)
    #[arg(long, default_value = "5")]
    commit_interval_secs: u64,

    #[cfg(debug_assertions)]
    #[arg(long, help = "UDP port for debug test events")]
    test_event_port: Option<u16>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    #[cfg(debug_assertions)]
    let test_event_port = args.test_event_port;
    #[cfg(not(debug_assertions))]
    let test_event_port: Option<u16> = None;

    let test_layer = if let Some(port) = test_event_port {
        Some(
            Box::new(TestEventLayer::connect(port).context("connecting test event layer")?)
                as Box<_>,
        )
    } else {
        None
    };

    let _guard = logger::init_logger_with_config_and_layer(
        logger::LoggerConfig {
            level: "debug".to_string(),
            file_path: None,
            file_level: "info".to_string(),
        },
        test_layer,
    );

    let addr = format!("{}:{}", args.address, args.port).parse().unwrap();

    let config = AdaptiveConfig {
        commit_interval_secs: args.commit_interval_secs,
    };

    tracing::info!(target: "test_event", source = "node", event = "startup", port = args.port);

    let svc = IndexServiceService::new(format!("./index-{}", args.port), config)?;

    let server = IndexServiceServer::new(svc);

    tracing::info!("Server started at {}", addr);

    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            info!("Shutdown signal received, stopping...");
        }
        res = Server::builder().add_service(server).serve(addr) => {
            match res {
                Ok(_) => info!("Server stopped"),
                Err(e) => info!("Server stopped with error: {}", e),
            }
        }
    }

    anyhow::Ok(())
}
