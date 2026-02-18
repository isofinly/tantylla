use crate::cluster::config::{InstrumentationConfig, ScyllaConfig};
use crate::cluster::core::SERVICE_READY_TIMEOUT_SECS;
use crate::cluster::utils::wait_for_tcp;
use crate::process::ServiceProcess;
use anyhow::Context;

// =========================================================================
// Service Orchestration
// =========================================================================

pub(super) async fn spawn_search_nodes(
    count: usize,
    instrumentation: &InstrumentationConfig,
) -> anyhow::Result<(Vec<String>, Vec<ServiceProcess>)> {
    let mut addrs = Vec::new();
    let mut processes = Vec::new();
    for _ in 0..count {
        let port = portpicker::pick_unused_port().context("finding free port")?;
        let addr = format!("127.0.0.1:{}", port);

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

        let process = ServiceProcess::spawn("tantylla-node", &args, &[])
            .await
            .context("spawning search node")?;
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
    let nodes_arg = node_addrs.join(",");
    let scylla_arg = scylla.contact_points.join(",");
    let table_arg = format!("{}.{}", keyspace, table_name);

    for _ in 0..count {
        let mut args = vec![
            "--scylla-uri".to_string(),
            scylla_arg.clone(),
            "--table-name".to_string(),
            table_arg.clone(),
            "--search-nodes".to_string(),
            nodes_arg.clone(),
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

        let process = ServiceProcess::spawn("tantylla-ingestor", &args, &[])
            .await
            .context("spawning ingestor")?;
        processes.push(process);
    }

    Ok(processes)
}
