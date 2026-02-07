use crate::server::monitoring::REALTIME_METRICS;
use eventsource_client::{Client, ClientBuilder, SSE};
use flume::{SendError, Sender};
use futures_util::StreamExt;
use log::{debug, error, info};
use reqwest;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio_util::sync::CancellationToken;

pub struct FirebaseListener {
    /// TODO: Make this a connection pool if it becomes a bottleneck!
    client: reqwest::Client,
    base_url: String,
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq, Eq)]
pub struct Item {
    pub id: i64,
    pub deleted: Option<bool>,
    pub type_: Option<String>,
    pub by: Option<String>,
    pub time: Option<i64>,
    pub text: Option<String>,
    pub dead: Option<bool>,
    pub parent: Option<i64>,
    pub poll: Option<i64>,
    pub url: Option<String>,
    pub score: Option<i64>,
    pub title: Option<String>,
    pub parts: Option<Vec<i64>>,
    pub descendants: Option<i64>,
    pub kids: Option<Vec<i64>>,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct UpdateData {
    pub items: Option<Vec<i64>>,
    pub profiles: Option<Vec<String>>,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct Update {
    pub path: String,
    pub data: UpdateData,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct User {
    pub id: String,
    pub created: Option<i64>,
    pub karma: Option<i64>,
    pub about: Option<String>,
    pub submitted: Option<Vec<i64>>,
}

#[derive(Error, Debug)]
pub enum FirebaseListenerErr {
    #[error("connection error: {0}")]
    ConnectError(String),
    #[error("parse error: {0}")]
    ParseError(String),
    #[error("unexpected HTTP status while fetching {resource}: {status}")]
    UnexpectedStatus { resource: String, status: u16 },
    #[error(transparent)]
    JsonParseError(#[from] serde_json::Error),
    #[error(transparent)]
    ChannelError(#[from] SendError<i64>),
    #[error(transparent)]
    RequestError(#[from] reqwest::Error),
}

impl FirebaseListener {
    pub fn new(url: String) -> Result<Self, FirebaseListenerErr> {
        let client = reqwest::Client::new();
        Ok(Self {
            client,
            base_url: url.to_string(),
        })
    }

    pub async fn get_item(&self, item_id: i64) -> Result<Option<Item>, FirebaseListenerErr> {
        let url = format!("{}/item/{}.json", self.base_url, item_id);
        let response = self.client.get(&url).send().await?;

        if !response.status().is_success() {
            return Err(FirebaseListenerErr::UnexpectedStatus {
                resource: format!("item {item_id}"),
                status: response.status().as_u16(),
            });
        }

        response.json::<Option<Item>>().await.map_err(Into::into)
    }

    pub async fn get_max_id(&self) -> Result<i64, FirebaseListenerErr> {
        let url = format!("{}/maxitem.json", self.base_url);
        let response = self.client.get(&url).send().await?;
        if !response.status().is_success() {
            return Err(FirebaseListenerErr::UnexpectedStatus {
                resource: "maxitem".to_string(),
                status: response.status().as_u16(),
            });
        }

        let response_text = response.text().await?;
        let max_id: i64 = response_text.parse().map_err(|_| {
            FirebaseListenerErr::ParseError(format!("Could not parse ID {}", response_text))
        })?;
        Ok(max_id)
    }

    pub async fn listen_to_updates(
        &self,
        tx: Sender<i64>,
        cancel_token: CancellationToken,
    ) -> Result<(), FirebaseListenerErr> {
        let url = format!("{}/updates.json", self.base_url);
        let client = ClientBuilder::for_url(&url).map_err(|_| {
            FirebaseListenerErr::ConnectError("Could not connect to SSE client!".into())
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
                                                tx.send_async(id).await?;
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
                            Ok(SSE::Connected(_)) => {
                                debug!("connected");
                            },
                        }
                    } else {
                        // Stream ended
                        break;
                    }
                }
                _ = cancel_token.cancelled() => {
                    info!("Cancellation token triggered, exiting listen_to_updates.");
                    break;
                }
            }
        }
        Ok(())
    }
}
