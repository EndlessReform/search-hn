//! Updater TOML uses the same typed defaults as the legacy CLI.
use catchup_worker_lib::embeddings::EmbeddingArgs;
use clap::Parser;
const DEFAULT_DB_POOL_MAX_SIZE: usize = 64;

impl Default for UpdaterArgs {
    fn default() -> Self {
        Self::parse_from(["updater"])
    }
}

impl UpdaterArgs {
    /// TOML owns operational settings; DATABASE_URL comes from the service environment.
    /// Do not mix TOML with legacy setting flags or implicitly load a local .env file.
    pub fn resolve(self) -> Result<Self, String> {
        if let Some(path) = &self.config {
            let args: Vec<_> = std::env::args().skip(2).collect();
            if !(args.len() == 2 && args[0] == "--config"
                || args.len() == 1 && args[0].starts_with("--config="))
            {
                return Err("--config cannot be combined with updater setting flags".into());
            }
            Self::load(path)
        } else {
            Ok(self)
        }
    }

    pub fn load(path: &std::path::Path) -> Result<Self, String> {
        let input =
            std::fs::read_to_string(path).map_err(|e| format!("{}: {e}", path.display()))?;
        Self::parse_toml(&input)?.with_database_url(std::env::var("DATABASE_URL").ok())
    }

    /// Keep credentials outside versioned configuration; accept only an explicit environment value.
    fn with_database_url(mut self, value: Option<String>) -> Result<Self, String> {
        let value = value
            .filter(|v| !v.trim().is_empty())
            .ok_or("DATABASE_URL is required in the service environment")?;
        self.database_url = Some(value);
        Ok(self)
    }

    fn parse_toml(input: &str) -> Result<Self, String> {
        // Do not echo source spans: an invalid file could still contain a pasted secret.
        let mut args: Self = toml::from_str(&input).map_err(|e| e.message().to_string())?;
        args.hn_api_url
            .get_or_insert_with(|| super::DEFAULT_HN_API_URL.into());
        args.embeddings.enabled = Some(args.embeddings.enabled.unwrap_or(false));
        if args.embeddings.enabled == Some(true) && args.embeddings.embedding_base_url.is_none() {
            return Err("embedding.base_url is required when embedding.enabled=true".into());
        }
        super::validate_updater_args(&args)?;
        args.embeddings.validate().map_err(|e| e.to_string())?;
        Ok(args)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn operational_toml_preserves_defaults_and_explicit_embedding_switch() {
        let args = UpdaterArgs::parse_toml("[embedding]\nenabled=false\nbase_url='http://unused'")
            .unwrap();
        assert_eq!(args.embeddings.enabled, Some(false));
        assert_eq!(args.startup_rescan_days, 3);
        assert_eq!(
            args.hn_api_url.as_deref(),
            Some(super::super::DEFAULT_HN_API_URL)
        );
    }

    #[test]
    fn credentials_are_required_from_environment_and_rejected_in_toml() {
        assert!(UpdaterArgs::parse_toml("database_url='postgres://user:secret@host/db'").is_err());
        assert!(UpdaterArgs::default()
            .with_database_url(None)
            .unwrap_err()
            .contains("DATABASE_URL"));
        assert!(UpdaterArgs::default()
            .with_database_url(Some(" ".into()))
            .is_err());
        let args = UpdaterArgs::default()
            .with_database_url(Some("postgres://fixture/db".into()))
            .unwrap();
        assert_eq!(args.database_url.as_deref(), Some("postgres://fixture/db"));
    }

    #[test]
    fn rejects_unknown_settings_and_incomplete_embedding() {
        assert!(UpdaterArgs::parse_toml("realtme_workers=2").is_err());
        assert!(UpdaterArgs::parse_toml("[embedding]\nenabled=true")
            .unwrap_err()
            .contains("base_url"));
    }
}
#[derive(Debug, Parser, Clone, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct UpdaterArgs {
    #[arg(long)]
    #[serde(skip)]
    pub config: Option<std::path::PathBuf>,
    #[serde(rename = "embedding")]
    #[command(flatten)]
    pub embeddings: EmbeddingArgs,
    #[serde(skip)]
    #[arg(long = "database-url")]
    pub database_url: Option<String>,
    #[arg(long = "hn-api-url")]
    pub hn_api_url: Option<String>,

    #[arg(long = "log-level", default_value = "info")]
    pub log_level: String,
    #[arg(long = "metrics-bind", default_value = "0.0.0.0:3000")]
    pub metrics_bind: String,

    #[arg(long = "db-pool-size", default_value_t = DEFAULT_DB_POOL_MAX_SIZE)]
    pub db_pool_size: usize,
    #[arg(long = "realtime-workers", default_value_t = 8)]
    pub realtime_workers: usize,
    #[arg(long = "channel-capacity", default_value_t = 4096)]
    pub channel_capacity: usize,

    #[arg(long = "startup-rescan-days", default_value_t = 3)]
    pub startup_rescan_days: i64,
    #[arg(long = "persist-interval-seconds", default_value_t = 60)]
    pub persist_interval_seconds: u64,
    /// Reconnect when the Firebase SSE stream produces no frame for this long.
    #[arg(long = "sse-inactivity-timeout-seconds", default_value_t = 180)]
    pub sse_inactivity_timeout_seconds: u64,
    /// Independent forced-replay window used only after an SSE inactivity timeout.
    #[arg(long = "stale-replay-days", default_value_t = 2)]
    pub stale_replay_days: i64,

    #[arg(long = "catchup-workers", default_value_t = 24)]
    pub catchup_workers: usize,
    #[arg(long = "catchup-segment-width", default_value_t = 1000)]
    pub catchup_segment_width: i64,
    #[arg(long = "catchup-queue-capacity")]
    pub catchup_queue_capacity: Option<usize>,
    #[arg(long = "catchup-global-rps", default_value_t = 250)]
    pub catchup_global_rps: u32,
    #[arg(long = "catchup-batch-size", default_value_t = 500)]
    pub catchup_batch_size: usize,

    #[arg(long = "retry-attempts", default_value_t = 5)]
    pub retry_attempts: u32,
    #[arg(long = "retry-initial-ms", default_value_t = 100)]
    pub retry_initial_ms: u64,
    #[arg(long = "retry-max-ms", default_value_t = 5000)]
    pub retry_max_ms: u64,
    #[arg(long = "retry-jitter-ms", default_value_t = 25)]
    pub retry_jitter_ms: u64,
}
