use crate::cluster::{SchemaConfig, ScyllaConfig};
use crate::process::{resolve_binary, workspace_root};
use anyhow::Context;
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use tokio::process::Command;
use tokio::time::{Duration, Instant, sleep};
use tracing::{debug, info};

// =========================================================================
// Scylla Setup
// =========================================================================

pub(super) async fn connect_scylla(config: &ScyllaConfig) -> anyhow::Result<Session> {
    let mut builder = SessionBuilder::new();
    for node in &config.contact_points {
        builder = builder.known_node(node);
    }

    builder.build().await.context("connecting to ScyllaDB")
}

pub(super) async fn ensure_binaries() -> anyhow::Result<()> {
    if std::env::var("TANTYLLA_SKIP_BUILD").ok().as_deref() == Some("1") {
        return Ok(());
    }

    let binaries = ["tantylla-node", "tantylla-ingestor", "tantylla-gateway"];
    for name in binaries {
        if let Err(err) = resolve_binary(name) {
            debug!(binary = name, error = ?err, "Binary not found before build");
        }
    }

    let root = workspace_root().context("finding workspace root")?;
    let status = Command::new("cargo")
        .arg("build")
        .arg("-p")
        .arg("tantylla-node")
        .arg("-p")
        .arg("tantylla-ingestor")
        .arg("-p")
        .arg("tantylla-gateway")
        .current_dir(root)
        .status()
        .await
        .context("building service binaries")?;

    if !status.success() {
        anyhow::bail!("cargo build failed with status {}", status);
    }

    Ok(())
}

pub(super) async fn create_keyspace(session: &Session, keyspace: &str) -> anyhow::Result<()> {
    let cql = format!(
        "CREATE KEYSPACE IF NOT EXISTS {} WITH replication = \
        {{'class': 'SimpleStrategy', 'replication_factor': 1}}",
        keyspace
    );
    info!("Creating keyspace {}", keyspace);
    session
        .query_unpaged(cql, ())
        .await
        .context("creating keyspace")?;
    Ok(())
}

pub(super) async fn apply_schema(
    session: &Session,
    schema: &SchemaConfig,
    keyspace: &str,
) -> anyhow::Result<()> {
    for statement in schema.render(keyspace) {
        session
            .query_unpaged(statement, ())
            .await
            .context("applying schema")?;
    }
    Ok(())
}

pub(super) async fn wait_for_cdc_log_table(
    session: &Session,
    keyspace: &str,
    table_name: &str,
    timeout_secs_max: u64,
) -> anyhow::Result<()> {
    let cdc_table = format!("{}_scylla_cdc_log", table_name);
    let deadline = Instant::now() + Duration::from_secs(timeout_secs_max);
    loop {
        let result = session
            .query_unpaged(
                "SELECT table_name FROM system_schema.tables WHERE keyspace_name = ? AND table_name = ?",
                (keyspace, cdc_table.as_str()),
            )
            .await
            .context("checking cdc log table")?;

        let rows = result
            .into_rows_result()
            .context("reading cdc log table rows")?;
        let mut rows_iter = rows
            .rows::<(String,)>()
            .context("parsing cdc log table rows")?;
        if rows_iter.next().is_some() {
            return Ok(());
        }

        if Instant::now() > deadline {
            anyhow::bail!(
                "Timed out waiting for CDC log table {}.{}",
                keyspace,
                cdc_table
            );
        }

        sleep(Duration::from_millis(200)).await;
    }
}
