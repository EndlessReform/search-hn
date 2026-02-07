use std::env;

pub struct Config {
    /// HN API url. required.
    pub hn_api_url: String,
    pub db_url: String,
}

impl Config {
    pub fn from_env() -> Result<Self, env::VarError> {
        let hn_api_url = env::var("HN_API_URL")
            .unwrap_or_else(|_| "https://hacker-news.firebaseio.com/v0".to_string());
        let db_url = env::var("DATABASE_URL")?;
        Ok(Self { hn_api_url, db_url })
    }
}
