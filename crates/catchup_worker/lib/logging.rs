use crate::build_info;
use std::backtrace::Backtrace;
use std::error::Error as StdError;
use std::process;
use std::time::{SystemTime, UNIX_EPOCH};

use tracing_log::LogTracer;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

/// Output format for runtime logs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogFormat {
    Json,
    Text,
}

impl LogFormat {
    /// Resolves log format from `LOG_FORMAT`.
    ///
    /// Accepted values:
    /// - `json` (default)
    /// - `text`
    fn from_env() -> Self {
        let raw = std::env::var("LOG_FORMAT").unwrap_or_else(|_| "json".to_string());
        match raw.trim().to_ascii_lowercase().as_str() {
            "json" => Self::Json,
            "text" => Self::Text,
            _ => Self::Json,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Json => "json",
            Self::Text => "text",
        }
    }
}

/// Runtime logging metadata used as common context fields.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoggingContext {
    pub service: String,
    pub mode: String,
    pub environment: String,
    pub run_id: String,
    pub build_version: String,
    pub build_commit: String,
    pub format: LogFormat,
}

/// Initializes process-wide logging and emits one bootstrap event.
///
/// Design notes:
/// - We bridge `log` records into `tracing`, so existing `log::info!` calls still work.
/// - JSON output is newline-delimited and ready for log collectors.
/// - `RUST_LOG` remains the canonical per-target filter knob.
pub fn init_logging(service: &str, mode: &str, default_level: &str) -> LoggingContext {
    let context = LoggingContext {
        service: service.to_string(),
        mode: mode.to_string(),
        environment: std::env::var("APP_ENV")
            .or_else(|_| std::env::var("ENVIRONMENT"))
            .unwrap_or_else(|_| "dev".to_string()),
        run_id: build_run_id(service),
        build_version: build_info::VERSION.to_string(),
        build_commit: build_info::short_commit_hash().to_string(),
        format: LogFormat::from_env(),
    };

    install_subscriber(context.format, default_level);

    tracing::info!(
        event = "logging_initialized",
        service = %context.service,
        environment = %context.environment,
        mode = %context.mode,
        run_id = %context.run_id,
        build_version = %context.build_version,
        build_commit = %context.build_commit,
        log_format = context.format.as_str(),
        "initialized logging"
    );

    context
}

fn install_subscriber(format: LogFormat, default_level: &str) {
    let _ = LogTracer::init();

    let result = match format {
        LogFormat::Json => tracing_subscriber::registry()
            .with(default_env_filter(default_level))
            .with(
                tracing_subscriber::fmt::layer()
                    .json()
                    .with_target(true)
                    .with_current_span(true)
                    .with_span_list(false)
                    .flatten_event(true),
            )
            .try_init(),
        LogFormat::Text => tracing_subscriber::registry()
            .with(default_env_filter(default_level))
            .with(tracing_subscriber::fmt::layer().with_target(true))
            .try_init(),
    };

    let _ = result;
}

fn default_env_filter(default_level: &str) -> EnvFilter {
    EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(default_level))
}

fn build_run_id(service: &str) -> String {
    let epoch_millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or_default();
    format!("{service}-{}-{epoch_millis}", process::id())
}

/// Builds a multi-line error report containing:
/// - the top-level error message
/// - the full source chain (if present)
/// - a captured backtrace at the logging boundary
///
/// This is intended for terminal and structured logs at process boundaries where
/// failing with only `%err` would lose nested causes such as DNS/socket failures
/// wrapped by pool or query layers.
pub fn format_error_report(err: &(dyn StdError + 'static)) -> String {
    let mut report = String::new();
    report.push_str("error: ");
    report.push_str(&err.to_string());

    let mut current_source = err.source();
    let mut source_index = 1usize;
    while let Some(source) = current_source {
        report.push_str("\ncaused by (");
        report.push_str(&source_index.to_string());
        report.push_str("): ");
        report.push_str(&source.to_string());
        current_source = source.source();
        source_index = source_index.saturating_add(1);
    }

    report.push_str("\nbacktrace:\n");
    report.push_str(&Backtrace::force_capture().to_string());
    report
}
