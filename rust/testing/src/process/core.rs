use anyhow::Context;
use nix::sys::signal::{self, Signal};
use nix::unistd::Pid;
use std::path::PathBuf;
use std::process::Stdio;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, Command};
use tokio::time::{Duration, timeout};
use tracing::{debug, error, info};

// =========================================================================
// Service Process Lifecycle
// =========================================================================

/// Represents a running service process (node, ingestor, gateway).
pub struct ServiceProcess {
    name: String,
    child: Option<Child>,
}

impl ServiceProcess {
    /// Spawn a new service process with captured stdout/stderr for debugging.
    pub async fn spawn(
        binary_name: &str,
        args: &[String],
        env: &[(&str, &str)],
    ) -> anyhow::Result<Self> {
        let binary_path = find_binary(binary_name).context("finding binary path")?;
        let workspace_root = workspace_root().context("finding workspace root")?;

        info!(
            "Spawning {} with args: {:?} env: {:?}",
            binary_name, args, env
        );

        let mut cmd = Command::new(&binary_path);
        cmd.args(args);
        cmd.current_dir(&workspace_root);
        for (key, val) in env {
            cmd.env(key, val);
        }

        // Capture stdout/stderr so tests can assert failure traces later.
        cmd.stdout(Stdio::piped());
        cmd.stderr(Stdio::piped());

        let mut child = cmd.spawn().context("spawning child process")?;

        let stdout = child
            .stdout
            .take()
            .expect("stdout is piped when spawning a service process");
        let stderr = child
            .stderr
            .take()
            .expect("stderr is piped when spawning a service process");

        let name_clone = binary_name.to_string();
        tokio::spawn(async move {
            let reader = BufReader::new(stdout);
            let mut lines = reader.lines();
            while let Ok(Some(line)) = lines.next_line().await {
                debug!("[{}] STDOUT: {}", name_clone, line);
            }
        });

        let name_clone = binary_name.to_string();
        tokio::spawn(async move {
            let reader = BufReader::new(stderr);
            let mut lines = reader.lines();
            while let Ok(Some(line)) = lines.next_line().await {
                error!("[{}] STDERR: {}", name_clone, line);
            }
        });

        Ok(Self {
            name: binary_name.to_string(),
            child: Some(child),
        })
    }

    /// Send SIGTERM, then SIGKILL on timeout.
    pub async fn terminate(&mut self, timeout_secs_max: u64) -> anyhow::Result<()> {
        let Some(mut child) = self.child.take() else {
            return Ok(());
        };

        let Some(pid) = child.id() else {
            return Ok(());
        };

        info!("Terminating process: {}", self.name);
        let pid = Pid::from_raw(pid as i32);
        signal::kill(pid, Signal::SIGTERM).context("sending SIGTERM")?;

        match timeout(Duration::from_secs(timeout_secs_max), child.wait()).await {
            Ok(status) => {
                status.context("waiting for process exit")?;
                return Ok(());
            }
            Err(_) => {
                info!(
                    "Process {} did not exit in time, sending SIGKILL",
                    self.name
                );
                signal::kill(pid, Signal::SIGKILL).context("sending SIGKILL")?;
                child.wait().await.context("waiting for SIGKILL exit")?;
            }
        }

        Ok(())
    }
}

pub fn resolve_binary(name: &str) -> anyhow::Result<PathBuf> {
    find_binary(name).context("resolving binary path")
}

pub fn workspace_root() -> anyhow::Result<PathBuf> {
    get_workspace_root().context("finding workspace root")
}

impl Drop for ServiceProcess {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            // Best-effort cleanup during Drop; tests should call `terminate` explicitly.
            let _ = child.start_kill();
        }
    }
}

// =========================================================================
// Binary Resolution
// =========================================================================

fn find_binary(name: &str) -> anyhow::Result<PathBuf> {
    let mut path = std::env::current_exe().context("reading current executable path")?;
    path.pop();
    if path.ends_with("deps") {
        path.pop();
    }

    let candidate = path.join(name);
    if candidate.exists() {
        return Ok(candidate);
    }

    let root = get_workspace_root().context("finding workspace root")?;
    let debug_path = root.join("target/debug").join(name);
    if debug_path.exists() {
        return Ok(debug_path);
    }

    let release_path = root.join("target/release").join(name);
    if release_path.exists() {
        return Ok(release_path);
    }

    anyhow::bail!("Binary '{}' not found in standard target locations", name);
}

fn get_workspace_root() -> anyhow::Result<PathBuf> {
    let mut path = std::env::current_dir().context("reading current directory")?;
    loop {
        let manifest_path = path.join("Cargo.toml");
        if manifest_path.exists() {
            let contents = std::fs::read_to_string(&manifest_path).context("reading Cargo.toml")?;
            if contents.contains("[workspace]") {
                return Ok(path);
            }
        }
        if !path.pop() {
            anyhow::bail!("Could not find workspace root");
        }
    }
}
