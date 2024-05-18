use crate::config::Config;

pub struct HnProcessor {
    hn_api_url: String,
    triton_server_addr: String,
}

impl HnProcessor {
    pub fn new(config: &Config) -> Self {
        Self {
            hn_api_url: config.hn_api_url.clone(),
            triton_server_addr: config.triton_server_addr.clone(),
        }
    }

    pub async fn start(&self) {
        let embedder = self.embed_and_store();
    }

    async fn embed_and_store(&self) {
        // Boilerplate
    }
}
