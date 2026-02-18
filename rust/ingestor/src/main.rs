use anyhow::Context;
use clap::Parser;
use scylla::client::session_builder::SessionBuilder;
use std::{
    net::SocketAddr,
    sync::{Arc, OnceLock},
    time::{Duration, SystemTime},
};
use tantylla_common::logger;
use tantylla_common::test_tracing::TestEventLayer;
use tokio::time;
use tracing::{error, info};

use crate::{batch::service::Service, checkpointer::core::Checkpointer};

mod batch;
mod cdc;
mod checkpointer;
mod router;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(
        long,
        value_delimiter = ',',
        help = "One or more Scylla contact points, e.g. 127.0.0.1:9042",
        required = true
    )]
    scylla_uri: Vec<String>,

    #[arg(
        long,
        help = "Keyspace and table name separated by a dot, e.g. keyspace.table",
        required = true
    )]
    table_name: String,

    #[arg(
        long,
        value_delimiter = ',',
        help = "List of node addresses, e.g. 127.0.0.1:10000,127.0.0.1:10001",
        required = true
    )]
    search_nodes: Vec<String>,

    // In debug builds we use a short safety interval (500 ms);
    // in release builds the default is 30 seconds.
    #[cfg(debug_assertions)]
    #[arg(
        long,
        default_value = "500",
        help = "Interval between safety checks in milliseconds"
    )]
    safety_interval: u64,

    #[cfg(not(debug_assertions))]
    #[arg(
        long,
        default_value = "30_000",
        help = "Interval between safety checks in milliseconds"
    )]
    safety_interval: u64,

    // In debug builds we poll every 500 ms;
    // in release builds we sleep 10 seconds between polls.
    #[cfg(debug_assertions)]
    #[arg(
        long,
        default_value = "500",
        help = "Interval between polls in milliseconds"
    )]
    sleep_interval: u64,

    #[cfg(not(debug_assertions))]
    #[arg(
        long,
        default_value = "10_000",
        help = "Interval between polls in milliseconds"
    )]
    sleep_interval: u64,

    #[cfg(debug_assertions)]
    #[arg(long, help = "UDP port for debug test events")]
    test_event_port: Option<u16>,
}

pub(crate) struct AccessPair {
    pub(crate) keyspace: String,
    pub(crate) table: String,
}

static GLOBAL_CONFIG: OnceLock<AccessPair> = OnceLock::new();

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

    logger::init_logger_with_config_and_layer(
        logger::LoggerConfig {
            level: "debug".to_string(),
            file_path: None,
            file_level: "info".to_string(),
        },
        test_layer,
    );

    info!("Connecting to ScyllaDB at {:?}...", &args.scylla_uri);
    let scylla_uris: Vec<SocketAddr> = args
        .scylla_uri
        .into_iter()
        .map(|s| {
            s.parse::<SocketAddr>()
                .expect("Failed to parse ScyllaDB URIs")
        })
        .collect();

    let session = SessionBuilder::new()
        .known_nodes_addr(&scylla_uris)
        .build()
        .await?;
    let session = Arc::new(session);

    info!("Connected to ScyllaDB at {:?}", &scylla_uris);

    let mut node_info = ahash::AHashMap::new();
    let mut nodes = args.search_nodes.clone();
    nodes.sort();
    for (i, addr) in nodes.iter().enumerate() {
        node_info.insert(i, addr.to_string());
    }

    let (keyspace, table) = match args.table_name.split_once('.') {
        Some((k, t)) => (k.to_string(), t.to_string()),
        None => {
            return Err(anyhow::anyhow!(
                "Table name must be in format 'keyspace.table'"
            ));
        }
    };

    tracing::info!(
        target: "test_event",
        source = "ingestor",
        event = "startup",
        table = format!("{}.{}", keyspace, table)
    );

    if GLOBAL_CONFIG
        .set(AccessPair {
            keyspace: keyspace.to_string(),
            table: table.to_string(),
        })
        .is_err()
    {
        return Err(anyhow::anyhow!("Failed to set global configuration"));
    }

    let batch_service = Service::new(format!("{}.{}", keyspace, table));

    let router = Arc::new(
        router::core::Router::new(node_info, &keyspace, &table, session.clone(), batch_service)
            .await?,
    );
    let factory = Arc::new(cdc::consumer::ConsumerFactory::new(router));

    // TODO: Handle 2026-01-20 17:25:34.874  WARN scylla::cluster::metadata: /Users/isofinly/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/scylla-1.4.1/src/cluster/metadata.rs:641: Failed to fetch metadata using current control connection control_connection_address=127.0.0.1:9043 error=Control connection pool error: The pool is broken; Last connection failed with: Connection refused (os error 61)
    // TODO: Update builder params for persistent CDC consumption
    // TODO: Create unique builder for each keyspace and table instead of a single instance

    let last_checkpoint_offset = match Checkpointer::get_last_read_offset()? {
        Some(offset) => offset,
        None => {
            let mut offset = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .context("reading system time")?;
            // In test mode, start slightly earlier to avoid missing early writes.
            if test_event_port.is_some() {
                offset = offset.saturating_sub(Duration::from_secs(2));
            }
            offset
        }
    };

    let (mut reader, handle) = scylla_cdc::log_reader::CDCLogReaderBuilder::new()
        .session(session.clone())
        .keyspace(keyspace.as_str())
        .table_name(table.as_str())
        .consumer_factory(factory)
        .safety_interval(time::Duration::from_millis(args.safety_interval))
        .sleep_interval(time::Duration::from_millis(args.sleep_interval))
        .start_timestamp(
            chrono::Duration::from_std(last_checkpoint_offset)
                .context("converting checkpoint offset")?,
        )
        .build()
        .await?;

    info!("Starting CDC Log Reader for {}.{}", keyspace, table);
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            info!("Shutdown signal received, stopping...");
            reader.stop();
        }
        result = handle => {
            match result {
                Ok(_) => info!("CDC Reader finished successfully."),
                Err(e) => {
                    error!("CDC Reader crashed: {:?}", e);
                    return Err(e);
                }
            }
        }
    }

    info!("Shutting down...");

    anyhow::Ok(())
}
