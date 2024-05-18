use std::env;

pub struct Config {
    /// HN API url. required.
    pub hn_api_url: String,
    pub triton_server_addr: String,
    pub db_url: String,
}

impl Config {
    pub fn from_env() -> Result<Self, env::VarError> {
        let hn_api_url = env::var("HN_API_URL")?;
        let triton_server_addr = env::var("TRITON_SERVER_ADDR")?;
        let db_url = env::var("DB_URL")?;
        Ok(Self {
            hn_api_url,
            triton_server_addr,
            db_url,
        })
    }
}
