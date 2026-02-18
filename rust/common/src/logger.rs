use std::path::PathBuf;
use tracing::Level;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::{EnvFilter, Layer, Registry, fmt, prelude::*};

/// Configuration options for the logger
#[derive(Clone, Debug)]
pub struct LoggerConfig {
    /// Log level filter for stdout. Can be any valid `EnvFilter` string.
    pub level: String,
    /// Optional file path to write logs to. If None, logs go to stdout only
    pub file_path: Option<PathBuf>,
    /// Minimum level for file logging (if file is enabled). Can be any valid `EnvFilter` string.
    pub file_level: String,
}

impl Default for LoggerConfig {
    fn default() -> Self {
        Self {
            level: "info".to_string(),
            file_path: None,
            file_level: "info".to_string(),
        }
    }
}

/// Initialize the logger with stdout only (default configuration)
pub fn init_logger(log_level: Level) {
    let config = LoggerConfig {
        level: log_level.to_string().to_lowercase(),
        ..Default::default()
    };
    init_logger_with_config(config);
}

/// Initialize the logger with custom configuration
pub fn init_logger_with_config(config: LoggerConfig) -> Option<WorkerGuard> {
    init_logger_with_config_and_layer(config, None)
}

/// Initialize the logger with custom configuration and an optional layer
pub fn init_logger_with_config_and_layer(
    config: LoggerConfig,
    extra_layer: Option<Box<dyn Layer<Registry> + Send + Sync>>,
) -> Option<WorkerGuard> {
    let stdout_filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(&config.level));

    let stdout_layer = fmt::layer()
        .with_timer(fmt::time::ChronoLocal::new(
            "%Y-%m-%d %H:%M:%S%.3f".to_string(),
        ))
        .with_target(true)
        .with_file(true)
        .with_line_number(true)
        .with_ansi(true)
        .with_filter(stdout_filter);

    let (file_layer, guard) = if let Some(ref file_path) = config.file_path {
        let file = std::fs::File::create(file_path).expect("Failed to create log file");
        let (non_blocking, guard) = tracing_appender::non_blocking(file);

        let file_filter = EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| EnvFilter::new(&config.file_level));

        let layer = fmt::layer()
            .with_ansi(false)
            .with_writer(non_blocking)
            .with_filter(file_filter);

        (Some(layer), Some(guard))
    } else {
        (None, None)
    };

    tracing_subscriber::registry()
        .with(extra_layer)
        .with(stdout_layer)
        .with(file_layer)
        .init();

    tracing::info!("Logger initialized with level: {} (stdout)", config.level);
    if let Some(path) = &config.file_path {
        tracing::info!(
            "File logging enabled for level {} to {}",
            config.file_level,
            path.display()
        );
    }

    guard
}
