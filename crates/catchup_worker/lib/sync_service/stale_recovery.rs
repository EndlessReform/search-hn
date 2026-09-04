use diesel_async::pooled_connection::deadpool::Pool;
use std::time::Instant;
use tracing::{info, warn};

use super::updater_state::find_rescan_start_id_from_time;
use super::{CatchupOrchestratorConfig, SyncService};
use crate::firebase_listener::StaleStreamIncident;

const SECONDS_PER_DAY: i64 = 24 * 60 * 60;

/// Force-replays a recent item window after an SSE inactivity timeout.
///
/// The reconnect `maxitem` gap-fill recovers newly allocated IDs, but cannot recover mutations
/// to existing stories and comments. This replay deliberately covers every known item in an
/// independently configured recent time window.
pub async fn replay_after_stale_stream(
    service: &SyncService,
    pool: &Pool<diesel_async::AsyncPgConnection>,
    incident: StaleStreamIncident,
    replay_days: i64,
    orchestrator_config: CatchupOrchestratorConfig,
) {
    assert!(
        replay_days > 0,
        "stale replay days must be greater than zero"
    );

    let replay_from_epoch = incident
        .detected_at_epoch
        .saturating_sub(replay_days.saturating_mul(SECONDS_PER_DAY));
    let start_id = match find_rescan_start_id_from_time(pool, replay_from_epoch).await {
        Ok(id) => id,
        Err(err) => {
            warn!(
                event = "stale_stream_replay_window_resolve_failed",
                error = %err,
                replay_days,
                replay_from_epoch,
                "failed to resolve pessimistic replay start after a stuck Firebase stream"
            );
            return;
        }
    };

    let started_at = Instant::now();
    info!(
        event = "stale_stream_replay_started",
        replay_days,
        replay_from_epoch,
        start_id,
        incident_detected_at_epoch = incident.detected_at_epoch,
        last_sse_event_epoch = incident.last_event_epoch,
        "starting forced recent-item replay after a stuck Firebase stream"
    );

    match service
        .catchup_with_summary(
            None,
            Some(start_id),
            None,
            false,
            CatchupOrchestratorConfig {
                force_replay_window: true,
                ..orchestrator_config
            },
        )
        .await
    {
        Ok(summary) => {
            info!(
                event = "stale_stream_replay_complete",
                elapsed_ms = started_at.elapsed().as_millis(),
                replay_days,
                start_id,
                frontier_id = summary.frontier_id,
                target_max_id = summary.target_max_id,
                "completed forced recent-item replay after a stuck Firebase stream"
            );
        }
        Err(err) => {
            warn!(
                event = "stale_stream_replay_failed",
                error = %err,
                elapsed_ms = started_at.elapsed().as_millis(),
                replay_days,
                start_id,
                "forced recent-item replay failed after a stuck Firebase stream"
            );
        }
    }
}
