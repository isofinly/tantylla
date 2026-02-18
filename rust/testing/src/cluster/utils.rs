use anyhow::Context;
use std::net::SocketAddr;
use std::{fs, io::ErrorKind, path::PathBuf};
use tokio::net::TcpStream;
use tokio::time::{Duration, Instant, sleep};
use tracing::debug;

pub(super) async fn wait_for_tcp(addr: SocketAddr, timeout_secs_max: u64) -> anyhow::Result<()> {
    let deadline = Instant::now() + Duration::from_secs(timeout_secs_max);
    loop {
        match TcpStream::connect(addr).await {
            Ok(_) => {
                debug!("TCP ready on {}", addr);
                return Ok(());
            }
            Err(_) => {
                if Instant::now() > deadline {
                    anyhow::bail!("Timed out waiting for TCP readiness on {}", addr);
                }
                sleep(Duration::from_millis(100)).await;
            }
        }
    }
}

pub(super) fn parse_checkpoint(contents: &str) -> anyhow::Result<std::time::Duration> {
    let parts: Vec<&str> = contents.split('\t').map(|s| s.trim()).collect();
    if parts.len() != 3 {
        anyhow::bail!("Invalid checkpoint file format");
    }

    let timestamp_micro: u128 = parts[2].parse().context("parsing checkpoint timestamp")?;
    Ok(std::time::Duration::from_micros(timestamp_micro as u64))
}

pub(super) fn remove_file_if_exists(path: &PathBuf) -> anyhow::Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == ErrorKind::NotFound => Ok(()),
        Err(err) => Err(anyhow::anyhow!("Failed to remove file {:?}: {}", path, err)),
    }
}

pub(super) fn remove_dir_if_exists(path: &PathBuf) -> anyhow::Result<()> {
    match fs::remove_dir_all(path) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == ErrorKind::NotFound => Ok(()),
        Err(err) => Err(anyhow::anyhow!(
            "Failed to remove directory {:?}: {}",
            path,
            err
        )),
    }
}
