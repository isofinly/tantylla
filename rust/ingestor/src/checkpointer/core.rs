//! Module that commits atomically last read offset(interval)

use std::{
    fs::{self, File},
    io::{ErrorKind, Write},
    path::Path,
    time::Duration,
};

use anyhow::Result;

use crate::GLOBAL_CONFIG;

#[derive(Debug, Clone)]
pub(crate) struct Checkpointer {}

impl Checkpointer {
    /// Duration must be microseconds
    pub(crate) fn commit_last_read_offset(timestamp: std::time::Duration) -> Result<()> {
        let config = GLOBAL_CONFIG
            .get()
            .expect("Global config for keyspace and table is empty");
        let keyspace = config.keyspace.clone();
        let table = config.table.clone();
        let timestamp_micro = timestamp.as_micros();

        let path_name = format!("{}-{}.checkpoint", keyspace, table);
        let path = Path::new(&path_name);

        let dir = path.parent().unwrap_or_else(|| Path::new("."));
        let temp_path = dir.join(format!(
            ".tmp.{}",
            path.file_name().unwrap().to_string_lossy()
        ));

        let old_content = match fs::read_to_string(&path) {
            Ok(content) => {
                let parts: Vec<String> = content
                    .split('\t')
                    .map(|s| s.to_string().trim().to_string())
                    .collect();

                if parts.len() != 3 {
                    return Err(anyhow::anyhow!("Invalid checkpoint file format"));
                }
                if timestamp_micro < parts[2].parse::<u128>()? {
                    return Err(anyhow::anyhow!(
                        "Timestamp is older than the last checkpoint"
                    ));
                }
                parts
            }
            Err(e) => {
                if e.kind() == ErrorKind::NotFound {
                    Vec::new()
                } else {
                    return Err(anyhow::anyhow!("Failed to read checkpoint file: {}", e));
                }
            }
        };

        if old_content.len() == 0 || (old_content[0] == keyspace && old_content[1] == table) {
            let new_content = format!("{}\t{}\t{}\n", keyspace, table, timestamp_micro);
            let mut file = File::create(&temp_path)?;
            file.write_all(new_content.as_bytes())?;
            file.sync_all()?;
            fs::rename(&temp_path, path)?;
        } else {
            return Err(anyhow::anyhow!(
                "Wrong keyspace or table in checkpoint file",
            ));
        }

        Ok(())
    }

    /// Duration returned is microseconds
    pub(crate) fn get_last_read_offset() -> Result<Option<Duration>> {
        let config = GLOBAL_CONFIG
            .get()
            .expect("Global config for keyspace and table is empty");
        let keyspace = config.keyspace.clone();
        let table = config.table.clone();

        let path_name = format!(".{}-{}", keyspace, table);
        let path = Path::new(&path_name);
        let content = match fs::read_to_string(path) {
            Ok(content) => content,
            Err(e) => {
                if e.kind() == ErrorKind::NotFound {
                    return Ok(None);
                } else {
                    return Err(anyhow::anyhow!("Failed to read checkpoint file: {}", e));
                }
            }
        };

        let parts = content.split('\t').map(|s| s.trim()).collect::<Vec<_>>();
        if parts.len() != 3 {
            return Err(anyhow::anyhow!("Invalid checkpoint file format"));
        }
        let timestamp_micro = parts[2].parse::<u128>()?;
        let duration = Duration::from_micros(timestamp_micro as u64);
        Ok(Some(duration))
    }
}
