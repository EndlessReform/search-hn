use diesel::sql_query;
use diesel::sql_types::{BigInt, Integer, Nullable, Text};
use diesel_async::pooled_connection::deadpool::Pool;
use diesel_async::RunQueryDsl;

use super::error::Error;
use super::types::{ItemOutcome, ItemOutcomeKind};

/// Source runtime that emitted the failed item outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DlqSource {
    Catchup,
    Realtime,
}

impl DlqSource {
    fn as_db_str(self) -> &'static str {
        match self {
            Self::Catchup => "catchup",
            Self::Realtime => "realtime",
        }
    }
}

/// Durable item-failure state for triage/replay.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DlqState {
    RetryWait,
    DeadLetter,
    TerminalMissing,
}

impl DlqState {
    fn as_db_str(self) -> &'static str {
        match self {
            Self::RetryWait => "retry_wait",
            Self::DeadLetter => "dead_letter",
            Self::TerminalMissing => "terminal_missing",
        }
    }
}

/// Canonical persisted failure record shared by catchup and realtime runtimes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DlqRecord {
    pub source: DlqSource,
    pub run_id: String,
    pub segment_id: Option<i64>,
    pub item_id: i64,
    pub state: DlqState,
    pub attempts: u32,
    pub failure_class: Option<String>,
    pub last_error: Option<String>,
    pub diagnostics_json: Option<String>,
}

impl DlqRecord {
    /// Maps a worker outcome into a persisted DLQ record when that outcome is non-success.
    pub fn from_item_outcome(
        source: DlqSource,
        run_id: impl Into<String>,
        segment_id: Option<i64>,
        outcome: &ItemOutcome,
    ) -> Option<Self> {
        let state = match outcome.kind {
            ItemOutcomeKind::Succeeded => return None,
            ItemOutcomeKind::TerminalMissing => DlqState::TerminalMissing,
            ItemOutcomeKind::RetryableFailure => DlqState::RetryWait,
            ItemOutcomeKind::FatalFailure => DlqState::DeadLetter,
        };

        Some(Self {
            source,
            run_id: run_id.into(),
            segment_id,
            item_id: outcome.item_id,
            state,
            attempts: outcome.attempts,
            failure_class: outcome.failure_class.clone(),
            last_error: outcome.message.clone(),
            diagnostics_json: outcome.diagnostics.as_ref().map(|diag| format!("{diag:?}")),
        })
    }

    /// Builds a terminal-missing record from a raw missing item ID.
    pub fn terminal_missing(
        source: DlqSource,
        run_id: impl Into<String>,
        segment_id: Option<i64>,
        item_id: i64,
    ) -> Self {
        Self {
            source,
            run_id: run_id.into(),
            segment_id,
            item_id,
            state: DlqState::TerminalMissing,
            attempts: 1,
            failure_class: None,
            last_error: None,
            diagnostics_json: None,
        }
    }
}

/// Persists one DLQ item record.
pub async fn insert_dlq_record(
    pool: &Pool<diesel_async::AsyncPgConnection>,
    record: &DlqRecord,
) -> Result<(), Error> {
    let mut conn = pool.get().await?;
    let attempts = i32::try_from(record.attempts).map_err(|_| {
        Error::Orchestration(format!(
            "dlq attempts overflowed i32 for item {}: {}",
            record.item_id, record.attempts
        ))
    })?;

    sql_query(
        "INSERT INTO ingest_dlq_items \
         (source, run_id, segment_id, item_id, state, attempts, failure_class, last_error, diagnostics_json, updated_at) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NOW())",
    )
    .bind::<Text, _>(record.source.as_db_str())
    .bind::<Text, _>(record.run_id.as_str())
    .bind::<Nullable<BigInt>, _>(record.segment_id)
    .bind::<BigInt, _>(record.item_id)
    .bind::<Text, _>(record.state.as_db_str())
    .bind::<Integer, _>(attempts)
    .bind::<Nullable<Text>, _>(record.failure_class.as_deref())
    .bind::<Nullable<Text>, _>(record.last_error.as_deref())
    .bind::<Nullable<Text>, _>(record.diagnostics_json.as_deref())
    .execute(&mut conn)
    .await?;

    Ok(())
}
