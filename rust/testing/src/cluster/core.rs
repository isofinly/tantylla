use crate::cluster::config::{InstrumentationConfig, SchemaConfig, ScyllaConfig, TopologyConfig};
use crate::cluster::gateway::GatewayClient;
use crate::cluster::scylladb::{
    apply_schema, connect_scylla, create_keyspace, ensure_binaries, wait_for_cdc_log_table,
};
use crate::cluster::services::{
    spawn_gateways, spawn_ingestors, spawn_search_nodes, spawn_single_ingestor,
    spawn_single_search_node,
};
use crate::cluster::utils::{remove_dir_if_exists, wait_for_tcp};
use crate::process::{ServiceProcess, workspace_root};
use crate::trace::TraceCollector;
use anyhow::Context;
use futures::FutureExt;
use futures::future::BoxFuture;
use scylla::client::session::Session;
use scylla::value::CqlTimestamp;
use std::net::SocketAddr;
use tokio::task::JoinHandle;
use tokio::time::{Duration, Instant, sleep};
use uuid::Uuid;

pub(super) const SERVICE_READY_TIMEOUT_SECS: u64 = 20;

// =========================================================================
// Test Cluster
// =========================================================================

pub struct TestClusterBuilder {
    topology: TopologyConfig,
    scylla: ScyllaConfig,
    schema: SchemaConfig,
    table_name: String,
    instrumentation: InstrumentationConfig,
}

impl TestClusterBuilder {
    pub fn new() -> Self {
        Self {
            topology: TopologyConfig::default(),
            scylla: ScyllaConfig::default(),
            schema: SchemaConfig::default_schema(),
            table_name: "documents".to_string(),
            instrumentation: InstrumentationConfig::default(),
        }
    }

    pub fn with_topology(mut self, topology: TopologyConfig) -> Self {
        self.topology = topology;
        self
    }

    pub fn with_search_nodes(mut self, count: usize) -> Self {
        self.topology.search_nodes = count;
        self
    }

    pub fn with_ingestors(mut self, count: usize) -> Self {
        self.topology.ingestors = count;
        self
    }

    pub fn with_gateways(mut self, count: usize) -> Self {
        self.topology.gateways = count;
        self
    }

    pub fn with_scylla_contact_points(mut self, points: Vec<String>) -> Self {
        self.scylla.contact_points = points;
        self
    }

    pub fn with_schema(mut self, schema: SchemaConfig) -> Self {
        self.schema = schema;
        self
    }

    pub fn with_table_name(mut self, table_name: impl Into<String>) -> Self {
        self.table_name = table_name.into();
        self
    }

    pub fn enable_instrumentation(mut self, enabled: bool) -> Self {
        self.instrumentation.enabled = enabled;
        self
    }

    pub async fn build(self) -> anyhow::Result<TestCluster> {
        ensure_binaries().await?;
        let session = connect_scylla(&self.scylla).await?;

        let instrumentation =
            if self.instrumentation.enabled && self.instrumentation.event_port.is_none() {
                InstrumentationConfig {
                    event_port: Some(portpicker::pick_unused_port().context("finding event port")?),
                    ..self.instrumentation
                }
            } else {
                self.instrumentation
            };

        let (trace_collector, trace_task) = if instrumentation.enabled {
            let event_port = instrumentation
                .event_port
                .context("missing instrumentation event port")?;
            let bind_addr = format!("127.0.0.1:{}", event_port);
            let (collector, socket) = TraceCollector::bind(&bind_addr)
                .await
                .context("binding trace collector")?;
            let collector_clone = collector.clone();
            let task = tokio::spawn(async move {
                collector_clone.listen(socket).await;
            });
            (Some(collector), Some(task))
        } else {
            (None, None)
        };

        let keyspace = format!("test_{}", Uuid::new_v4().simple());
        create_keyspace(&session, &keyspace).await?;
        apply_schema(&session, &self.schema, &keyspace).await?;
        wait_for_cdc_log_table(&session, &keyspace, &self.table_name, 15).await?;

        let (node_addrs, node_processes) =
            spawn_search_nodes(self.topology.search_nodes, &instrumentation).await?;
        let (gateway_addrs, gateway_processes) =
            spawn_gateways(self.topology.gateways, &node_addrs, &instrumentation).await?;
        let ingestor_processes = spawn_ingestors(
            self.topology.ingestors,
            &self.scylla,
            &keyspace,
            &self.table_name,
            &node_addrs,
            &instrumentation,
        )
        .await?;

        Ok(TestCluster {
            session,
            keyspace,
            table_name: self.table_name,
            topology: self.topology,
            scylla: self.scylla,
            node_addrs,
            gateway_addrs,
            node_processes: node_processes.into_iter().map(Some).collect(),
            gateway_processes: gateway_processes.into_iter().map(Some).collect(),
            ingestor_processes: ingestor_processes.into_iter().map(Some).collect(),
            instrumentation,
            trace_collector,
            trace_task,
        })
    }
}

impl Default for TestClusterBuilder {
    fn default() -> Self {
        Self::new()
    }
}

pub struct TestCluster {
    session: Session,
    keyspace: String,
    table_name: String,
    topology: TopologyConfig,
    scylla: ScyllaConfig,
    node_addrs: Vec<String>,
    gateway_addrs: Vec<String>,
    node_processes: Vec<Option<ServiceProcess>>,
    gateway_processes: Vec<Option<ServiceProcess>>,
    ingestor_processes: Vec<Option<ServiceProcess>>,
    instrumentation: InstrumentationConfig,
    trace_collector: Option<TraceCollector>,
    trace_task: Option<JoinHandle<()>>,
}

impl TestCluster {
    pub fn builder() -> TestClusterBuilder {
        TestClusterBuilder::new()
    }

    pub fn keyspace(&self) -> &str {
        &self.keyspace
    }

    pub fn topology(&self) -> &TopologyConfig {
        &self.topology
    }

    pub fn table_name(&self) -> &str {
        &self.table_name
    }

    pub fn node_addrs(&self) -> &[String] {
        &self.node_addrs
    }

    pub fn gateway_addrs(&self) -> &[String] {
        &self.gateway_addrs
    }

    pub fn session(&self) -> &Session {
        &self.session
    }

    pub fn trace_collector(&self) -> Option<TraceCollector> {
        self.trace_collector.clone()
    }

    pub fn gateway(&self) -> anyhow::Result<GatewayClient> {
        let addr = self
            .gateway_addrs()
            .first()
            .context("no gateway addresses available")?;
        Ok(GatewayClient::new(addr))
    }

    pub async fn insert_document(
        &self,
        doc_id: &str,
        title: &str,
        body: &str,
    ) -> anyhow::Result<()> {
        let insert_cql = format!(
            "INSERT INTO {}.{} (doc_id, title, body) VALUES (?, ?, ?)",
            self.keyspace(),
            self.table_name(),
        );
        self.session()
            .query_unpaged(insert_cql, (doc_id, title, body))
            .await
            .context("inserting document")?;
        Ok(())
    }

    pub async fn scoped<F>(mut self, f: F) -> anyhow::Result<()>
    where
        F: for<'a> FnOnce(&'a mut TestCluster) -> BoxFuture<'a, anyhow::Result<()>>,
    {
        let result = std::panic::AssertUnwindSafe(f(&mut self))
            .catch_unwind()
            .await;

        self.shutdown()
            .await
            .context("shutting down cluster in scoped scope")?;

        match result {
            Ok(res) => res,
            Err(panic) => std::panic::resume_unwind(panic),
        }
    }

    pub fn instrumentation(&self) -> &InstrumentationConfig {
        &self.instrumentation
    }

    pub async fn shutdown(mut self) -> anyhow::Result<()> {
        for mut process in self.gateway_processes.drain(..).flatten() {
            process.terminate(3).await.context("terminating gateway")?;
        }

        for mut process in self.node_processes.drain(..).flatten() {
            process
                .terminate(3)
                .await
                .context("terminating search node")?;
        }

        for mut process in self.ingestor_processes.drain(..).flatten() {
            process.terminate(3).await.context("terminating ingestor")?;
        }

        if let Some(handle) = self.trace_task.take() {
            handle.abort();
        }

        let drop_keyspace = format!("DROP KEYSPACE IF EXISTS {}", self.keyspace);
        self.session
            .query_unpaged(drop_keyspace, ())
            .await
            .context("dropping keyspace")?;

        self.cleanup_indexes()
            .context("cleaning index directories")?;

        Ok(())
    }

    pub async fn terminate_node(&mut self, index: usize) -> anyhow::Result<()> {
        let process = self
            .node_processes
            .get_mut(index)
            .context("locating search node")?;
        if let Some(mut process) = process.take() {
            process
                .terminate(3)
                .await
                .context("terminating search node")?;
        }
        Ok(())
    }

    pub async fn terminate_ingestor(&mut self, index: usize) -> anyhow::Result<()> {
        let process = self
            .ingestor_processes
            .get_mut(index)
            .context("locating ingestor")?;
        if let Some(mut process) = process.take() {
            process.terminate(3).await.context("terminating ingestor")?;
        }
        Ok(())
    }

    pub async fn terminate_gateway(&mut self, index: usize) -> anyhow::Result<()> {
        let process = self
            .gateway_processes
            .get_mut(index)
            .context("locating gateway")?;
        if let Some(mut process) = process.take() {
            process.terminate(3).await.context("terminating gateway")?;
        }
        Ok(())
    }

    /// Restarts search node `index` by terminating the old process and spawning
    /// a fresh one on the **same port**.
    ///
    /// The method blocks until TCP is accepting connections again
    /// so the caller can immediately use the node.
    pub async fn restart_node(&mut self, index: usize) -> anyhow::Result<()> {
        if let Some(slot) = self.node_processes.get_mut(index) {
            if let Some(mut old) = slot.take() {
                old.terminate(3)
                    .await
                    .context("terminating old search node")?;
            }
        } else {
            anyhow::bail!("search node index {} out of range", index);
        }

        let addr = self
            .node_addrs
            .get(index)
            .context("locating node address")?
            .clone();
        let socket_addr: std::net::SocketAddr = addr.parse().context("parsing node address")?;
        let port = socket_addr.port();

        let process = spawn_single_search_node(port, &self.instrumentation)
            .await
            .context("spawning replacement search node")?;

        wait_for_tcp(socket_addr, SERVICE_READY_TIMEOUT_SECS)
            .await
            .context("waiting for replacement search node")?;

        self.node_processes[index] = Some(process);
        Ok(())
    }

    /// Restarts ingestor `index` by terminating the old process and spawning a
    /// fresh one.
    pub async fn restart_ingestor(&mut self, index: usize) -> anyhow::Result<()> {
        if let Some(slot) = self.ingestor_processes.get_mut(index) {
            if let Some(mut old) = slot.take() {
                old.terminate(3).await.context("terminating old ingestor")?;
            }
        } else {
            anyhow::bail!("ingestor index {} out of range", index);
        }

        let process = spawn_single_ingestor(
            &self.scylla,
            &self.keyspace,
            &self.table_name,
            &self.node_addrs,
            &self.instrumentation,
            Some(0i64),
        )
        .await
        .context("spawning replacement ingestor")?;

        self.ingestor_processes[index] = Some(process);
        Ok(())
    }

    pub async fn wait_for_checkpoint(
        &self,
        timeout_secs_max: u64,
    ) -> anyhow::Result<std::time::Duration> {
        let checkpoint_table =
            format!("{}.{}_tantylla_checkpoints", self.keyspace, self.table_name);

        let query = format!("SELECT stream_id, time FROM {checkpoint_table}");

        let deadline = Instant::now() + Duration::from_secs(timeout_secs_max);
        loop {
            let query_result = self
                .session
                .query_unpaged(query.as_str(), ())
                .await
                .context("querying checkpoint table")?;
            let rows_result = query_result
                .into_rows_result()
                .context("reading checkpoint rows")?;
            let rows = rows_result
                .rows::<(Vec<u8>, Option<CqlTimestamp>)>()
                .context("deserialising checkpoint rows")?;

            for row in rows {
                let (stream_id, time_opt) = row.context("reading checkpoint row")?;
                // Skip the generation sentinel (stream_id = [0x00]).
                if stream_id == [0u8] {
                    continue;
                }
                if let Some(CqlTimestamp(millis)) = time_opt {
                    // i64::MAX is written as a dummy value for the generation
                    // sentinel; skip it if it somehow appears on a real row.
                    if millis >= 0 && millis != i64::MAX {
                        return Ok(std::time::Duration::from_millis(millis as u64));
                    }
                }
            }

            if Instant::now() > deadline {
                anyhow::bail!(
                    "Timed out waiting for a checkpoint in {checkpoint_table} \
                     after {timeout_secs_max}s"
                );
            }

            sleep(Duration::from_millis(100)).await;
        }
    }

    fn cleanup_indexes(&self) -> anyhow::Result<()> {
        let root = workspace_root().context("finding workspace root")?;
        for addr in &self.node_addrs {
            let socket_addr: SocketAddr = addr.parse().context("parsing node address")?;
            let index_dir = root.join(format!("index-{}", socket_addr.port()));
            remove_dir_if_exists(&index_dir)
                .with_context(|| format!("removing index directory {:?}", index_dir))?;
        }
        Ok(())
    }
}
