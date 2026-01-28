use clap::Parser;
use tantylla_common::{indexer::index_service_server::IndexServiceServer, logger};
use tonic::transport::Server;
use tracing::info;

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
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    let _guard = logger::init_logger_with_config(logger::LoggerConfig {
        level: "debug".to_string(),
        file_path: None,
        file_level: "info".to_string(),
    });

    let addr = format!("{}:{}", args.address, args.port).parse().unwrap();

    let svc = IndexServiceService::new(format!("./index-{}", args.port))?;

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
