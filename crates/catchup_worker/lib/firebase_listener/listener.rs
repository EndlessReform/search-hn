use crate::server::monitoring::REALTIME_METRICS;
use eventsource_client::{Client, ClientBuilder, SSE};
use flume::{SendError, Sender};
use futures_util::StreamExt;
use hn_core::HnItem;
use reqwest;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use thiserror::Error;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

pub struct FirebaseListener {
    /// TODO: Make this a connection pool if it becomes a bottleneck!
    client: reqwest::Client,
    base_url: String,
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

    pub async fn get_item(&self, item_id: i64) -> Result<Option<HnItem>, FirebaseListenerErr> {
        let url = format!("{}/item/{}.json", self.base_url, item_id);
        let response = self.client.get(&url).send().await?;

        if !response.status().is_success() {
            return Err(FirebaseListenerErr::UnexpectedStatus {
                resource: format!("item {item_id}"),
                status: response.status().as_u16(),
            });
        }

        response.json::<Option<HnItem>>().await.map_err(Into::into)
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

    /// Listens to `updates.json` with reconnect/backoff and reconnect gap-fill.
    ///
    /// `last_event_epoch` is updated on every successfully received stream event (including
    /// keep-alives) so the caller can persist heartbeat state without writing on each event.
    pub async fn listen_to_updates_resilient(
        &self,
        tx: Sender<i64>,
        cancel_token: CancellationToken,
        last_event_epoch: Arc<AtomicI64>,
    ) -> Result<(), FirebaseListenerErr> {
        let url = format!("{}/updates.json", self.base_url);
        let mut reconnect_backoff = Duration::from_millis(500);
        let mut max_id_before = self.get_max_id().await?;

        loop {
            if cancel_token.is_cancelled() {
                info!(
                    event = "realtime_updates_cancelled",
                    "cancellation token triggered; exiting listener"
                );
                if let Some(metrics) = REALTIME_METRICS.get() {
                    metrics.listener_connected.set(0);
                }
                return Ok(());
            }

            let client = ClientBuilder::for_url(&url).map_err(|_| {
                FirebaseListenerErr::ConnectError("Could not connect to SSE client!".into())
            })?;
            let mut stream = client.build().stream();
            let mut saw_stream_event = false;

            loop {
                tokio::select! {
                    _ = cancel_token.cancelled() => {
                        info!(
                            event = "realtime_updates_cancelled",
                            "cancellation token triggered; exiting listener"
                        );
                        if let Some(metrics) = REALTIME_METRICS.get() {
                            metrics.listener_connected.set(0);
                        }
                        return Ok(());
                    }
                    event_option = stream.next() => {
                        match event_option {
                            Some(Ok(SSE::Connected(_))) => {
                                info!(
                                    event = "realtime_updates_connected",
                                    "connected to realtime updates stream"
                                );
                                if let Some(metrics) = REALTIME_METRICS.get() {
                                    metrics.listener_connected.set(1);
                                }
                            }
                            Some(Ok(SSE::Comment(_))) => {}
                            Some(Ok(SSE::Event(ev))) => {
                                saw_stream_event = true;
                                let now_epoch = current_unix_epoch_seconds();
                                last_event_epoch.store(now_epoch, Ordering::Relaxed);

                                if let Some(metrics) = REALTIME_METRICS.get() {
                                    metrics.last_event_age_seconds.set(0);
                                }

                                match serde_json::from_str::<Update>(&ev.data) {
                                    Ok(update) => {
                                        if let Some(ids) = update.data.items {
                                            if let Some(metrics) = REALTIME_METRICS.get() {
                                                metrics.batch_size.set(ids.len() as i64);
                                            }
                                            for id in ids {
                                                send_update_id(&tx, id).await?;
                                            }
                                        }
                                    }
                                    Err(err) => {
                                        if ev.event_type == "keep-alive" {
                                            debug!(
                                                event = "realtime_updates_keepalive",
                                                "received realtime keep-alive event"
                                            );
                                        } else {
                                            error!(
                                                event = "realtime_update_parse_failed",
                                                update_event_type = %ev.event_type,
                                                error = %err,
                                                "failed to parse realtime update payload"
                                            );
                                        }
                                    }
                                }
                            }
                            Some(Err(err)) => {
                                error!(
                                    event = "realtime_update_stream_error",
                                    error = %err,
                                    "realtime update stream error; reconnecting"
                                );
                                break;
                            }
                            None => {
                                info!(
                                    event = "realtime_updates_stream_ended",
                                    "realtime updates stream ended; reconnecting"
                                );
                                break;
                            }
                        }
                    }
                }
            }

            if let Some(metrics) = REALTIME_METRICS.get() {
                metrics.listener_connected.set(0);
                metrics.reconnects_total.inc();
            }

            match self.get_max_id().await {
                Ok(max_id_now) => {
                    if max_id_now > max_id_before {
                        let gap_start = max_id_before + 1;
                        info!(
                            event = "realtime_gap_fill_enqueued",
                            from_id = gap_start,
                            to_id = max_id_now,
                            count = (max_id_now - max_id_before),
                            "enqueuing maxitem reconnect gap"
                        );
                        for id in gap_start..=max_id_now {
                            send_update_id(&tx, id).await?;
                        }
                    }
                    max_id_before = max_id_now;
                }
                Err(err) => {
                    warn!(
                        event = "realtime_gap_fill_maxitem_failed",
                        error = %err,
                        "failed to fetch maxitem during reconnect; retrying after backoff"
                    );
                }
            }

            if saw_stream_event {
                reconnect_backoff = Duration::from_millis(500);
            }

            info!(
                event = "realtime_reconnect_wait",
                backoff_ms = reconnect_backoff.as_millis(),
                "waiting before realtime reconnect"
            );
            tokio::select! {
                _ = cancel_token.cancelled() => return Ok(()),
                _ = tokio::time::sleep(reconnect_backoff) => {}
            }
            reconnect_backoff = (reconnect_backoff * 2).min(Duration::from_secs(30));
        }
    }

    pub async fn listen_to_updates(
        &self,
        tx: Sender<i64>,
        cancel_token: CancellationToken,
    ) -> Result<(), FirebaseListenerErr> {
        self.listen_to_updates_resilient(
            tx,
            cancel_token,
            Arc::new(AtomicI64::new(current_unix_epoch_seconds())),
        )
        .await
    }
}

async fn send_update_id(tx: &Sender<i64>, id: i64) -> Result<(), FirebaseListenerErr> {
    let was_full = tx.is_full();
    let send_started = Instant::now();
    tx.send_async(id).await?;

    if let Some(metrics) = REALTIME_METRICS.get() {
        metrics.queue_depth.set(tx.len() as i64);
        if was_full && send_started.elapsed() > Duration::from_millis(500) {
            metrics.queue_overflow_total.inc();
        }
    }

    Ok(())
}

fn current_unix_epoch_seconds() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock should be after unix epoch")
        .as_secs() as i64
}

#[cfg(test)]
mod tests {
    use hn_core::HnItem;

    /// Guards against silently dropping the reserved JSON field `type`.
    #[test]
    fn item_deserialization_maps_type_field() {
        let raw = r#"{
            "id": 8863,
            "type": "story",
            "by": "dhouston",
            "title": "My YC app: Dropbox - Throw away your USB drive"
        }"#;

        let item: HnItem = serde_json::from_str(raw).expect("valid item JSON should deserialize");

        assert_eq!(item.id, 8863);
        assert_eq!(item.type_.as_deref(), Some("story"));
        assert_eq!(item.by.as_deref(), Some("dhouston"));
    }

    /// Guards against decode failures when boolean fields are unexpectedly returned as arrays.
    #[test]
    fn item_deserialization_tolerates_array_boolean_shape() {
        let raw = r#"{
            "id": 41550939,
            "deleted": [true],
            "dead": [],
            "type": "story"
        }"#;

        let item: HnItem =
            serde_json::from_str(raw).expect("array boolean shape should deserialize");

        assert_eq!(item.id, 41550939);
        assert_eq!(item.deleted, Some(true));
        assert_eq!(item.dead, None);
        assert_eq!(item.type_.as_deref(), Some("story"));
    }
}
