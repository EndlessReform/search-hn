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
    use super::{FirebaseListener, FirebaseListenerErr};
    use crate::server::monitoring::{RealtimeMetrics, REALTIME_METRICS};
    use axum::{
        extract::State,
        response::sse::{Event, Sse},
        routing::get,
        Router,
    };
    use futures_util::{
        stream::{self, BoxStream},
        StreamExt,
    };
    use hn_core::HnItem;
    use prometheus_client::registry::Registry;
    use std::convert::Infallible;
    use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::net::TcpListener;
    use tokio::task::JoinHandle;
    use tokio::time::timeout;
    use tokio_util::sync::CancellationToken;

    #[derive(Clone, Copy)]
    enum FirstConnectionBehavior {
        CloseAfterBatch,
        KeepOpenAfterBatch,
    }

    /// Shared state for a deterministic in-process SSE server used by listener tests.
    struct MockSseState {
        maxitem: AtomicI64,
        first_batch_payload: String,
        reconnect_maxitem: Option<i64>,
        first_connection_behavior: FirstConnectionBehavior,
        connection_count: AtomicUsize,
    }

    struct MockSseServer {
        base_url: String,
        state: Arc<MockSseState>,
        task_handle: JoinHandle<()>,
    }

    impl MockSseServer {
        async fn start(
            initial_maxitem: i64,
            first_batch_ids: Vec<i64>,
            reconnect_maxitem: Option<i64>,
            first_connection_behavior: FirstConnectionBehavior,
        ) -> Self {
            let state = Arc::new(MockSseState {
                maxitem: AtomicI64::new(initial_maxitem),
                first_batch_payload: serde_json::json!({
                    "path": "/",
                    "data": { "items": first_batch_ids }
                })
                .to_string(),
                reconnect_maxitem,
                first_connection_behavior,
                connection_count: AtomicUsize::new(0),
            });

            let app = Router::new()
                .route("/v0/maxitem.json", get(maxitem_handler))
                .route("/v0/updates.json", get(updates_handler))
                .with_state(state.clone());

            let listener = TcpListener::bind("127.0.0.1:0")
                .await
                .expect("failed to bind mock SSE listener");
            let addr = listener
                .local_addr()
                .expect("mock SSE listener should have a local address");

            let task_handle = tokio::spawn(async move {
                axum::serve(listener, app)
                    .await
                    .expect("mock SSE axum server failed");
            });

            Self {
                base_url: format!("http://{addr}/v0"),
                state,
                task_handle,
            }
        }

        fn connection_count(&self) -> usize {
            self.state.connection_count.load(Ordering::Relaxed)
        }
    }

    impl Drop for MockSseServer {
        fn drop(&mut self) {
            self.task_handle.abort();
        }
    }

    async fn maxitem_handler(State(state): State<Arc<MockSseState>>) -> String {
        state.maxitem.load(Ordering::Relaxed).to_string()
    }

    async fn updates_handler(
        State(state): State<Arc<MockSseState>>,
    ) -> Sse<BoxStream<'static, Result<Event, Infallible>>> {
        let connection_idx = state.connection_count.fetch_add(1, Ordering::Relaxed);

        if connection_idx == 0 {
            if let Some(next_maxitem) = state.reconnect_maxitem {
                // Simulate new IDs appearing while the first stream is active and then drops.
                state.maxitem.store(next_maxitem, Ordering::Relaxed);
            }

            let event = Event::default()
                .event("put")
                .data(state.first_batch_payload.clone());
            let first_batch = stream::iter(vec![Ok(event)]);
            let stream = match state.first_connection_behavior {
                FirstConnectionBehavior::CloseAfterBatch => first_batch.boxed(),
                FirstConnectionBehavior::KeepOpenAfterBatch => {
                    first_batch.chain(stream::pending()).boxed()
                }
            };
            return Sse::new(stream);
        }

        Sse::new(stream::pending().boxed())
    }

    async fn recv_exact_ids(
        rx: &flume::Receiver<i64>,
        count: usize,
        per_item_timeout: Duration,
    ) -> Vec<i64> {
        let mut out = Vec::with_capacity(count);
        for _ in 0..count {
            let recv = timeout(per_item_timeout, rx.recv_async())
                .await
                .expect("timed out receiving ID from update queue");
            out.push(recv.expect("update queue unexpectedly disconnected"));
        }
        out
    }

    async fn ensure_realtime_metrics_for_tests() -> &'static RealtimeMetrics {
        REALTIME_METRICS
            .get_or_init(|| async {
                let mut registry = Registry::default();
                RealtimeMetrics::register(&mut registry, "realtime")
            })
            .await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn resilient_listener_gap_fills_missing_ids_after_disconnect() {
        let server = MockSseServer::start(
            10,
            vec![10],
            Some(13),
            FirstConnectionBehavior::CloseAfterBatch,
        )
        .await;
        let listener = FirebaseListener::new(server.base_url.clone())
            .expect("failed to construct firebase listener");

        let (tx, rx) = flume::bounded(64);
        let cancel = CancellationToken::new();
        let last_event_epoch = Arc::new(AtomicI64::new(0));
        let listener_last_event = Arc::clone(&last_event_epoch);
        let listener_cancel = cancel.clone();

        let listener_task: JoinHandle<Result<(), FirebaseListenerErr>> = tokio::spawn(async move {
            listener
                .listen_to_updates_resilient(tx, listener_cancel, listener_last_event)
                .await
        });

        let received = recv_exact_ids(&rx, 4, Duration::from_secs(5)).await;
        assert_eq!(received, vec![10, 11, 12, 13]);
        assert!(
            last_event_epoch.load(Ordering::Relaxed) > 0,
            "expected listener to record a last-event timestamp"
        );
        assert!(
            server.connection_count() >= 1,
            "expected at least one SSE connection"
        );

        cancel.cancel();
        let joined = timeout(Duration::from_secs(2), listener_task)
            .await
            .expect("listener task did not shut down after cancellation")
            .expect("listener task panicked");
        assert!(
            joined.is_ok(),
            "listener should exit cleanly after cancellation"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn resilient_listener_backpressure_does_not_drop_moderate_bursts() {
        let burst_ids: Vec<i64> = (1..=256).collect();
        let server = MockSseServer::start(
            256,
            burst_ids.clone(),
            None,
            FirstConnectionBehavior::KeepOpenAfterBatch,
        )
        .await;
        let listener = FirebaseListener::new(server.base_url.clone())
            .expect("failed to construct firebase listener");

        let (tx, rx) = flume::bounded(16);
        let cancel = CancellationToken::new();
        let listener_task: JoinHandle<Result<(), FirebaseListenerErr>> = tokio::spawn({
            let cancel = cancel.clone();
            async move {
                listener
                    .listen_to_updates_resilient(tx, cancel, Arc::new(AtomicI64::new(0)))
                    .await
            }
        });

        // Let the producer hit bounded-channel pressure before draining.
        tokio::time::sleep(Duration::from_millis(300)).await;

        let received = recv_exact_ids(&rx, burst_ids.len(), Duration::from_secs(10)).await;
        assert_eq!(
            received, burst_ids,
            "bounded queue should apply backpressure, not drop IDs"
        );

        cancel.cancel();
        let joined = timeout(Duration::from_secs(2), listener_task)
            .await
            .expect("listener task did not shut down after cancellation")
            .expect("listener task panicked");
        assert!(
            joined.is_ok(),
            "listener should exit cleanly after cancellation"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn queue_overflow_metric_increments_after_blocked_send() {
        let metrics = ensure_realtime_metrics_for_tests().await;
        let overflow_before = metrics.queue_overflow_total.get();

        let server = MockSseServer::start(
            2,
            vec![1, 2],
            None,
            FirstConnectionBehavior::KeepOpenAfterBatch,
        )
        .await;
        let listener = FirebaseListener::new(server.base_url.clone())
            .expect("failed to construct firebase listener");

        let (tx, rx) = flume::bounded(1);
        let cancel = CancellationToken::new();
        let listener_task: JoinHandle<Result<(), FirebaseListenerErr>> = tokio::spawn({
            let cancel = cancel.clone();
            async move {
                listener
                    .listen_to_updates_resilient(tx, cancel, Arc::new(AtomicI64::new(0)))
                    .await
            }
        });

        // Wait until the first ID is queued so we know the second send is in (or near) blocked state.
        let wait_deadline = tokio::time::Instant::now() + Duration::from_secs(3);
        while rx.len() < 1 {
            assert!(
                tokio::time::Instant::now() < wait_deadline,
                "timed out waiting for queue to become full"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        // First send fills capacity=1; second send should remain blocked past the overflow threshold.
        tokio::time::sleep(Duration::from_millis(650)).await;
        let received = recv_exact_ids(&rx, 2, Duration::from_secs(5)).await;
        assert_eq!(received, vec![1, 2]);

        // Give metrics update a short scheduling window.
        tokio::time::sleep(Duration::from_millis(50)).await;
        let overflow_after = metrics.queue_overflow_total.get();
        assert!(
            overflow_after > overflow_before,
            "expected queue_overflow_total to increase after blocked send"
        );

        cancel.cancel();
        let joined = timeout(Duration::from_secs(2), listener_task)
            .await
            .expect("listener task did not shut down after cancellation")
            .expect("listener task panicked");
        assert!(
            joined.is_ok(),
            "listener should exit cleanly after cancellation"
        );
    }

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
