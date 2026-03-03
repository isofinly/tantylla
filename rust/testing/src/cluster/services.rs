use crate::cluster::config::{InstrumentationConfig, ScyllaConfig};
use crate::cluster::core::SERVICE_READY_TIMEOUT_SECS;
use crate::cluster::utils::wait_for_tcp;
use crate::process::ServiceProcess;
use anyhow::Context;

pub(super) async fn spawn_search_nodes(
    count: usize,
    instrumentation: &InstrumentationConfig,
) -> anyhow::Result<(Vec<String>, Vec<ServiceProcess>)> {
    let mut addrs = Vec::new();
    let mut processes = Vec::new();
    for _ in 0..count {
        let port = portpicker::pick_unused_port().context("finding free port")?;
        let addr = format!("127.0.0.1:{}", port);
        let process = spawn_single_search_node(port, instrumentation).await?;
        wait_for_tcp(
            addr.parse().context("parsing node address")?,
            SERVICE_READY_TIMEOUT_SECS,
        )
        .await?;
        addrs.push(addr);
        processes.push(process);
    }
    Ok((addrs, processes))
}

/// Spawns a single search node on a pre-assigned `port` and returns the
/// process handle without waiting for TCP readiness.
///
/// The caller is responsible for calling [`wait_for_tcp`] before using the node.
pub(super) async fn spawn_single_search_node(
    port: u16,
    instrumentation: &InstrumentationConfig,
) -> anyhow::Result<ServiceProcess> {
    let mut args = vec![
        "--port".to_string(),
        port.to_string(),
        "--address".to_string(),
        "127.0.0.1".to_string(),
        "--commit-interval-secs".to_string(),
        "1".to_string(),
    ];

    if instrumentation.enabled
        && let Some(event_port) = instrumentation.event_port
    {
        args.push("--test-event-port".to_string());
        args.push(event_port.to_string());
    }

    ServiceProcess::spawn("tantylla-node", &args, &[])
        .await
        .context("spawning search node")
}

pub(super) async fn spawn_gateways(
    count: usize,
    node_addrs: &[String],
    instrumentation: &InstrumentationConfig,
) -> anyhow::Result<(Vec<String>, Vec<ServiceProcess>)> {
    let mut addrs = Vec::new();
    let mut processes = Vec::new();
    let nodes_arg = node_addrs.join(",");
    for _ in 0..count {
        let port = portpicker::pick_unused_port().context("finding free port")?;
        let addr = format!("127.0.0.1:{}", port);

        let mut args = vec![
            "--port".to_string(),
            port.to_string(),
            "--address".to_string(),
            "127.0.0.1".to_string(),
            "--search-nodes".to_string(),
            nodes_arg.clone(),
        ];

        if instrumentation.enabled
            && let Some(event_port) = instrumentation.event_port
        {
            args.push("--test-event-port".to_string());
            args.push(event_port.to_string());
        }

        let process = ServiceProcess::spawn("tantylla-gateway", &args, &[])
            .await
            .context("spawning gateway")?;
        wait_for_tcp(
            addr.parse().context("parsing gateway address")?,
            SERVICE_READY_TIMEOUT_SECS,
        )
        .await?;
        addrs.push(addr);
        processes.push(process);
    }
    Ok((addrs, processes))
}

pub(super) async fn spawn_ingestors(
    count: usize,
    scylla: &ScyllaConfig,
    keyspace: &str,
    table_name: &str,
    node_addrs: &[String],
    instrumentation: &InstrumentationConfig,
) -> anyhow::Result<Vec<ServiceProcess>> {
    let mut processes = Vec::new();
    for _ in 0..count {
        let process =
            spawn_single_ingestor(scylla, keyspace, table_name, node_addrs, instrumentation)
                .await?;
        processes.push(process);
    }
    Ok(processes)
}

/// Spawns a single ingestor process and returns the process handle.
///
/// The ingestor has no TCP listener to wait on — it connects outward to
/// ScyllaDB and the search nodes
///
/// Caller does not need to call `wait_for_tcp` after this function returns.
pub(super) async fn spawn_single_ingestor(
    scylla: &ScyllaConfig,
    keyspace: &str,
    table_name: &str,
    node_addrs: &[String],
    instrumentation: &InstrumentationConfig,
) -> anyhow::Result<ServiceProcess> {
    let nodes_arg = node_addrs.join(",");
    let scylla_arg = scylla.contact_points.join(",");
    let table_arg = format!("{}.{}", keyspace, table_name);

    let mut args = vec![
        "--scylla-uri".to_string(),
        scylla_arg,
        "--table-name".to_string(),
        table_arg,
        "--search-nodes".to_string(),
        nodes_arg,
        "--safety-interval".to_string(),
        "500".to_string(),
        "--sleep-interval".to_string(),
        "500".to_string(),
    ];

    if instrumentation.enabled
        && let Some(event_port) = instrumentation.event_port
    {
        args.push("--test-event-port".to_string());
        args.push(event_port.to_string());
    }

    ServiceProcess::spawn("tantylla-ingestor", &args, &[])
        .await
        .context("spawning ingestor")
}
