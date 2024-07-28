use std::env;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ConfigError {
    #[error("Missing environment variable: {0}")]
    MissingEnvVar(String),

    #[error("Environment variable error: {0}")]
    EnvVarError(#[from] env::VarError),

    #[error("Invalid value for N_WORKERS: {0}")]
    InvalidNWorkers(String),
}

pub struct Config {
    pub hn_api_url: String,
    pub db_url: String,
    pub redis_url: String,
    /// Default: 200
    pub n_workers: usize,
}

impl Config {
    pub fn from_env() -> Result<Self, ConfigError> {
        let hn_api_url = env::var("HN_API_URL")
            .unwrap_or_else(|_| "https://hacker-news.firebaseio.com/v0".to_string());

        let db_url = env::var("DATABASE_URL")
            .map_err(|_| ConfigError::MissingEnvVar("DATABASE_URL".to_string()))?;

        let redis_url = env::var("REDIS_URL")
            .map_err(|_| ConfigError::MissingEnvVar("REDIS_URL".to_string()))?;

        let n_workers = match env::var("N_WORKERS") {
            Ok(val) => val
                .parse::<usize>()
                .map_err(|_| ConfigError::InvalidNWorkers(val))?,
            Err(_) => 200,
        };

        Ok(Self {
            hn_api_url,
            db_url,
            redis_url,
            n_workers,
        })
    }
}
