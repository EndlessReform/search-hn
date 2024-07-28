pub mod error;
pub mod items;

use crate::queue::{Job, RangesQueue};
use crate::server::monitoring::REALTIME_METRICS;
use error::FirebaseClientError;
use eventsource_client::{Client, ClientBuilder, SSE};
use futures_util::StreamExt;
use items::{Item, Update};
use log::{debug, error, info};
use reqwest;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

pub struct FirebaseListener {
    /// TODO: Make this a connection pool if it becomes a bottleneck!
    client: reqwest::Client,
    base_url: String,
}

impl FirebaseListener {
    pub fn new(url: String) -> Result<Self, FirebaseClientError> {
        let client = reqwest::Client::new();
        Ok(Self {
            client,
            base_url: url.to_string(),
        })
    }

    pub async fn get_item(&self, item_id: i64) -> Result<Item, FirebaseClientError> {
        let url = format!("{}/item/{}.json", self.base_url, item_id);
        let response = self.client.get(&url).send().await?;

        if !response.status().is_success() {
            return Err(FirebaseClientError::ConnectError(format!(
                "Received unexpected status code for item {}: {}",
                item_id,
                response.status()
            )));
        }

        match response.json::<Item>().await {
            Ok(item) => Ok(item.clone()),
            Err(e) => Err(FirebaseClientError::RequestError(e)),
        }
    }

    pub async fn get_max_id(&self) -> Result<i64, FirebaseClientError> {
        let url = format!("{}/maxitem.json", self.base_url);
        let response = self.client.get(&url).send().await?;
        if !response.status().is_success() {
            return Err(FirebaseClientError::ConnectError(format!(
                "Received unexpected status code for maxid: {}",
                response.status()
            )));
        }

        let response_text = response.text().await?;
        let max_id: i64 = response_text.parse().map_err(|_| {
            FirebaseClientError::ParseError(format!("Could not parse ID {}", response_text))
        })?;
        Ok(max_id)
    }

    pub async fn listen_to_updates(
        &self,
        queue: Arc<RangesQueue>,
        cancel_token: CancellationToken,
    ) -> Result<(), FirebaseClientError> {
        let url = format!("{}/updates.json", self.base_url);
        let client = ClientBuilder::for_url(&url).map_err(|_| {
            FirebaseClientError::ConnectError("Could not connect to SSE client!".into())
        })?;

        let mut stream = client.build().stream();
        loop {
            tokio::select! {
                event_option = stream.next() => {
                    if let Some(event) = event_option {
                        match event {
                            Ok(SSE::Event(ev)) => {
                                match serde_json::from_str::<Update>(&ev.data) {
                                    Ok(update) => {
                                        if let Some(ids) = update.data.items {
                                            //info!("{:?}; {:?} new items", ev.event_type, ids.len());
                                            debug!("{:?}", ids);
                                            if let Some(metrics) = REALTIME_METRICS.get() {
                                                metrics.batch_size.set(ids.len() as i64);
                                            }
                                            for id in ids {
                                               queue.push(&Job::realtime((id, id))).await?;
                                            }
                                        }
                                    }
                                    Err(err) => {
                                        if ev.event_type == "keep-alive" {
                                            // This is normal, just a keep-alive event
                                            debug!("keep-alive")
                                        } else {
                                            error!("Error parsing JSON for event {:?}: {:?}", ev.event_type, err);
                                        }
                                    }
                                }
                            },
                            Err(err) => {
                                error!("Error {:?}", err);
                            },
                            Ok(SSE::Comment(_)) => {},
                        }
                    } else {
                        // Stream ended
                        break;
                    }
                }
                _ = cancel_token.cancelled() => {
                    info!("Shutting down Firebase listener");
                    break;
                }
            }
        }
        Ok(())
    }
}
