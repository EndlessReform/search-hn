//! Runtime settings are independent of any client application or database.
use clap::Parser;
use std::net::SocketAddr;

/// Connection and admission settings. The output recipe itself is deliberately pinned.
#[derive(Debug, Clone, Parser)]
pub struct Config {
    #[arg(long, env = "EMBED_LISTEN", default_value = "0.0.0.0:8081")]
    pub listen: SocketAddr,
    /// Stock vLLM origin (without /v1), configured with the recipe documented in README.
    #[arg(long, env = "EMBED_BACKEND", default_value = "http://127.0.0.1:8080")]
    pub backend: String,
    /// Entire upstream request deadline; includes time waiting in vLLM's scheduler.
    #[arg(long, env = "EMBED_TIMEOUT_SECONDS", default_value = "30", value_parser = clap::value_parser!(u64).range(1..=300))]
    pub timeout_seconds: u64,
    /// Accepted interactive HTTP calls, each containing at most eight inputs.
    #[arg(long, env = "EMBED_INTERACTIVE_LIMIT", default_value = "16", value_parser = clap::value_parser!(u16).range(1..=128))]
    pub interactive_limit: u16,
}
