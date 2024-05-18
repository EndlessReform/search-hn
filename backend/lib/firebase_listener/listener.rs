use eventsource_client::{Client, ClientBuilder, SSE};
use flume::{SendError, Sender};
use futures_util::StreamExt;
use log::{debug, error, info};
use reqwest;
use serde::{Deserialize, Serialize};
use std::fmt::{self};
use thiserror::Error;
use tokio_util::sync::CancellationToken;

pub struct FirebaseListener {
    /// TODO: Make this a connection pool if it becomes a bottleneck!
    client: reqwest::Client,
    base_url: String,
}

#[derive(Deserialize, Serialize, Debug)]
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
    pub parts: Option<String>,
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
    ConnectError(String),
    ParseError(String),
    JsonParseError(#[from] serde_json::Error), // Added for JSON parsing errors
    ChannelError(#[from] SendError<i64>),
    RequestError(#[from] reqwest::Error),
}

impl fmt::Display for FirebaseListenerErr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            FirebaseListenerErr::ConnectError(e) => write!(f, "ConnectError: {}", e),
            FirebaseListenerErr::ParseError(e) => write!(f, "ParseError: {}", e),
            FirebaseListenerErr::JsonParseError(e) => write!(f, "ParseError: {}", e),
            FirebaseListenerErr::ChannelError(e) => write!(f, "ChannelError: {}", e),
            FirebaseListenerErr::RequestError(e) => write!(f, "RequestError: {}", e),
        }
    }
}

impl FirebaseListener {
    pub fn new(url: String) -> Result<Self, FirebaseListenerErr> {
        let client = reqwest::Client::new();
        Ok(Self {
            client,
            base_url: url.to_string(),
        })
    }

    pub async fn get_item(&self, item_id: i64) -> Result<Item, FirebaseListenerErr> {
        let url = format!("{}/item/{}.json", self.base_url, item_id);
        let response = self.client.get(&url).send().await?;

        if !response.status().is_success() {
            return Err(FirebaseListenerErr::ConnectError(format!(
                "Received unexpected status code for item {}: {}",
                item_id,
                response.status()
            )));
        }

        response
            .json::<Item>()
            .await
            .map_err(|_| FirebaseListenerErr::ParseError(format!("Item {} is not valid!", item_id)))
    }

    pub async fn get_max_id(&self) -> Result<i64, FirebaseListenerErr> {
        let url = format!("{}/maxitem.json", self.base_url);
        let response = self.client.get(&url).send().await?;
        if !response.status().is_success() {
            return Err(FirebaseListenerErr::ConnectError(format!(
                "Received unexpected status code for maxid: {}",
                response.status()
            )));
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
                                            info!("{:?}; {:?} new items", ev.event_type, ids.len());
                                            debug!("{:?}", ids);
                                            for id in ids {
                                                tx.send_async(id).await?;
                                            }
                                        }
                                    }
                                    Err(err) => {
                                        if ev.event_type == "keep-alive" {
                                            debug!("keep-alive")
                                        }
                                        error!("Error parsing JSON for event {:?}: {:?}", ev.event_type, err);
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
                    info!("Cancellation token triggered, exiting listen_to_updates.");
                    break;
                }
            }
        }
        Ok(())
    }
}
