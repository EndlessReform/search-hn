use super::store::{map_exception_row, map_segment_row, quote, quote_opt, SegmentDb};
use super::types::{
    CatchupPreparation, ExceptionState, IngestException, IngestSegment, SegmentStateError,
    SegmentStatus,
};

/// Creates one `pending` segment covering `[start_id, end_id]` inclusive.
///
/// `unresolved_count` is initialized to the segment width (`end_id - start_id + 1`).
pub fn enqueue_segment<C>(
    conn: &mut C,
    start_id: i64,
    end_id: i64,
) -> Result<i64, SegmentStateError>
where
    C: SegmentDb,
{
    if start_id > end_id {
        return Err(SegmentStateError::InvalidInput(format!(
            "segment start_id ({start_id}) must be <= end_id ({end_id})"
        )));
    }

    let unresolved_count_i64 = end_id - start_id + 1;
    let unresolved_count = i32::try_from(unresolved_count_i64).map_err(|_| {
        SegmentStateError::InvalidInput(format!(
            "segment unresolved_count overflowed i32 for range [{start_id}, {end_id}]"
        ))
    })?;

    let insert_sql = format!(
        "INSERT INTO ingest_segments (start_id, end_id, status, attempts, scan_cursor_id, unresolved_count, last_error) \
         VALUES ({start_id}, {end_id}, {}, 0, NULL, {unresolved_count}, NULL)",
        quote(SegmentStatus::Pending.as_db_str()),
    );
    conn.execute_sql(&insert_sql)?;

    let mut rows = conn.load_segment_ids(
        "SELECT segment_id FROM ingest_segments ORDER BY segment_id DESC LIMIT 1",
    )?;
    let inserted = rows
        .pop()
        .expect("segment insert succeeded but no segment_id was returned");

    Ok(inserted.segment_id)
}

/// Splits `[start_id, end_id]` into fixed-width segments and enqueues each one.
///
/// Returns created `segment_id`s in creation order.
pub fn enqueue_range<C>(
    conn: &mut C,
    start_id: i64,
    end_id: i64,
    segment_width: i64,
) -> Result<Vec<i64>, SegmentStateError>
where
    C: SegmentDb,
{
    if segment_width <= 0 {
        return Err(SegmentStateError::InvalidInput(format!(
            "segment_width must be > 0, got {segment_width}"
        )));
    }
    if start_id > end_id {
        return Ok(Vec::new());
    }

    let mut out = Vec::new();
    let mut cursor = start_id;
    while cursor <= end_id {
        let segment_end = (cursor + segment_width - 1).min(end_id);
        out.push(enqueue_segment(conn, cursor, segment_end)?);
        cursor = segment_end + 1;
    }

    Ok(out)
}

/// Reads one segment by `segment_id`, if present.
pub fn get_segment<C>(
    conn: &mut C,
    segment_id: i64,
) -> Result<Option<IngestSegment>, SegmentStateError>
where
    C: SegmentDb,
{
    let sql = format!(
        "SELECT segment_id, start_id, end_id, status, attempts, scan_cursor_id, unresolved_count, last_error \
         FROM ingest_segments WHERE segment_id = {segment_id} LIMIT 1"
    );

    let mut rows = conn.load_segments(&sql)?;
    match rows.pop() {
        Some(row) => Ok(Some(map_segment_row(row)?)),
        None => Ok(None),
    }
}

/// Lists segments by status, ordered by `(start_id, segment_id)`.
pub fn list_segments_by_status<C>(
    conn: &mut C,
    status: SegmentStatus,
    limit: i64,
) -> Result<Vec<IngestSegment>, SegmentStateError>
where
    C: SegmentDb,
{
    if limit <= 0 {
        return Err(SegmentStateError::InvalidInput(format!(
            "limit must be > 0, got {limit}"
        )));
    }

    let sql = format!(
        "SELECT segment_id, start_id, end_id, status, attempts, scan_cursor_id, unresolved_count, last_error \
         FROM ingest_segments \
         WHERE status = {} \
         ORDER BY start_id ASC, segment_id ASC \
         LIMIT {limit}",
        quote(status.as_db_str()),
    );

    conn.load_segments(&sql)?
        .into_iter()
        .map(map_segment_row)
        .collect()
}

/// Claims the next `pending` segment by moving it to `in_progress` and incrementing `attempts`.
///
/// Returns `None` when no pending segment exists.
///
/// Assumes one catchup process for v1.
pub fn claim_next_pending_segment<C>(
    conn: &mut C,
) -> Result<Option<IngestSegment>, SegmentStateError>
where
    C: SegmentDb,
{
    let mut rows = conn.load_segment_ids(&format!(
        "SELECT segment_id FROM ingest_segments \
         WHERE status = {} \
         ORDER BY start_id ASC, segment_id ASC \
         LIMIT 1",
        quote(SegmentStatus::Pending.as_db_str())
    ))?;

    let Some(next) = rows.pop() else {
        return Ok(None);
    };

    let update_sql = format!(
        "UPDATE ingest_segments \
         SET status = {}, attempts = attempts + 1, last_error = NULL \
         WHERE segment_id = {}",
        quote(SegmentStatus::InProgress.as_db_str()),
        next.segment_id
    );
    conn.execute_sql(&update_sql)?;

    get_segment(conn, next.segment_id)
}

/// Marks every `in_progress` segment as `pending`.
///
/// Intended for process restart recovery before resuming work.
pub fn requeue_in_progress_segments<C>(conn: &mut C) -> Result<usize, SegmentStateError>
where
    C: SegmentDb,
{
    let sql = format!(
        "UPDATE ingest_segments \
         SET status = {} \
         WHERE status = {}",
        quote(SegmentStatus::Pending.as_db_str()),
        quote(SegmentStatus::InProgress.as_db_str())
    );
    Ok(conn.execute_sql(&sql)?)
}

/// Marks every `retry_wait` segment as `pending`.
///
/// This is the startup-side "reactivate retryables" step for v1 catchup planning.
pub fn requeue_retry_wait_segments<C>(conn: &mut C) -> Result<usize, SegmentStateError>
where
    C: SegmentDb,
{
    let sql = format!(
        "UPDATE ingest_segments \
         SET status = {} \
         WHERE status = {}",
        quote(SegmentStatus::Pending.as_db_str()),
        quote(SegmentStatus::RetryWait.as_db_str())
    );
    Ok(conn.execute_sql(&sql)?)
}

/// Prepares durable catchup state before any worker fanout begins.
///
/// Steps:
/// 1. Recover crashed work by requeuing `in_progress` segments.
/// 2. Reactivate retryable segments by moving `retry_wait` -> `pending`.
/// 3. Resolve planning window start from either explicit cold start or computed frontier.
/// 4. Optionally force replay for the window by resetting overlapping segments back to `pending`.
/// 5. Compute uncovered holes in `[planning_start_id, target_max_id]` and enqueue new segments.
/// 6. Return all pending segments overlapping the planning window.
///
/// This function is intentionally deterministic and idempotent for a single-process v1 topology.
pub fn prepare_catchup_segments<C>(
    conn: &mut C,
    target_max_id: i64,
    cold_start_id: Option<i64>,
    segment_width: i64,
    force_replay_window: bool,
) -> Result<CatchupPreparation, SegmentStateError>
where
    C: SegmentDb,
{
    if target_max_id <= 0 {
        return Err(SegmentStateError::InvalidInput(format!(
            "target_max_id must be > 0, got {target_max_id}"
        )));
    }
    if segment_width <= 0 {
        return Err(SegmentStateError::InvalidInput(format!(
            "segment_width must be > 0, got {segment_width}"
        )));
    }
    if let Some(start_id) = cold_start_id {
        if start_id <= 0 {
            return Err(SegmentStateError::InvalidInput(format!(
                "cold_start_id must be > 0, got {start_id}"
            )));
        }
    }

    let requeued_in_progress = requeue_in_progress_segments(conn)?;
    let reactivated_retry_wait = requeue_retry_wait_segments(conn)?;
    let frontier_id = compute_frontier_id(conn)?;
    let planning_start_id = cold_start_id.unwrap_or_else(|| frontier_id.saturating_add(1));

    let mut created_segment_ids = Vec::new();
    if planning_start_id <= target_max_id {
        if force_replay_window {
            reset_segments_for_window(conn, planning_start_id, target_max_id)?;
        }

        let existing_ranges =
            list_existing_ranges_for_window(conn, planning_start_id, target_max_id)?;
        let missing_ranges =
            compute_uncovered_ranges(planning_start_id, target_max_id, &existing_ranges);

        for (start_id, end_id) in missing_ranges {
            created_segment_ids.extend(enqueue_range(conn, start_id, end_id, segment_width)?);
        }
    }

    let pending_segments =
        list_pending_segments_for_window(conn, planning_start_id, target_max_id)?;

    Ok(CatchupPreparation {
        frontier_id,
        planning_start_id,
        target_max_id,
        requeued_in_progress,
        reactivated_retry_wait,
        created_segment_ids,
        pending_segments,
    })
}

/// Resets every segment overlapping the replay window back to a fresh `pending` attempt.
///
/// Why this exists:
/// - For debug/perf runs we sometimes intentionally re-scan a bounded historical window
///   (for example IDs 1..100000) even when those IDs are already covered by done segments.
/// - Setting `status = pending` alone is not sufficient: done segments also have
///   `scan_cursor_id = end_id` and `unresolved_count = 0`, which would cause immediate no-op.
///
/// This helper clears those progress markers so workers will actually refetch and upsert.
fn reset_segments_for_window<C>(
    conn: &mut C,
    start_id: i64,
    end_id: i64,
) -> Result<usize, SegmentStateError>
where
    C: SegmentDb,
{
    let reset_segments_sql = format!(
        "UPDATE ingest_segments \
         SET status = {}, \
             scan_cursor_id = NULL, \
             unresolved_count = (end_id - start_id + 1), \
             last_error = NULL \
         WHERE end_id >= {start_id} \
           AND start_id <= {end_id}",
        quote(SegmentStatus::Pending.as_db_str()),
    );
    let affected_segments = conn.execute_sql(&reset_segments_sql)?;

    let reset_exceptions_sql = format!(
        "UPDATE ingest_exceptions \
         SET state = {}, \
             attempts = 0, \
             next_retry_at = NULL, \
             last_error = NULL, \
             updated_at = CURRENT_TIMESTAMP \
         WHERE segment_id IN ( \
             SELECT segment_id FROM ingest_segments \
             WHERE end_id >= {start_id} \
               AND start_id <= {end_id} \
         )",
        quote(ExceptionState::Pending.as_db_str()),
    );
    let _ = conn.execute_sql(&reset_exceptions_sql)?;

    Ok(affected_segments)
}

/// Sets a segment back to `pending`.
///
/// Useful for replaying retry-wait or dead-letter segments.
pub fn set_segment_pending<C>(conn: &mut C, segment_id: i64) -> Result<usize, SegmentStateError>
where
    C: SegmentDb,
{
    let sql = format!(
        "UPDATE ingest_segments \
         SET status = {}, last_error = NULL \
         WHERE segment_id = {segment_id}",
        quote(SegmentStatus::Pending.as_db_str())
    );
    Ok(conn.execute_sql(&sql)?)
}

/// Updates per-segment scan progress while work is running.
pub fn update_segment_progress<C>(
    conn: &mut C,
    segment_id: i64,
    scan_cursor_id: Option<i64>,
    unresolved_count: i32,
) -> Result<usize, SegmentStateError>
where
    C: SegmentDb,
{
    if unresolved_count < 0 {
        return Err(SegmentStateError::InvalidInput(format!(
            "unresolved_count must be >= 0, got {unresolved_count}"
        )));
    }

    let sql = format!(
        "UPDATE ingest_segments \
         SET scan_cursor_id = {}, unresolved_count = {unresolved_count} \
         WHERE segment_id = {segment_id}",
        scan_cursor_id
            .map(|v| v.to_string())
            .unwrap_or_else(|| "NULL".to_string())
    );

    Ok(conn.execute_sql(&sql)?)
}

/// Seals a segment as `done` with `unresolved_count = 0`.
pub fn mark_segment_done<C>(
    conn: &mut C,
    segment_id: i64,
    scan_cursor_id: Option<i64>,
) -> Result<usize, SegmentStateError>
where
    C: SegmentDb,
{
    let sql = format!(
        "UPDATE ingest_segments \
         SET status = {}, scan_cursor_id = {}, unresolved_count = 0, last_error = NULL \
         WHERE segment_id = {segment_id}",
        quote(SegmentStatus::Done.as_db_str()),
        scan_cursor_id
            .map(|v| v.to_string())
            .unwrap_or_else(|| "NULL".to_string())
    );

    Ok(conn.execute_sql(&sql)?)
}

/// Marks a segment as `retry_wait` after retryable failure.
pub fn mark_segment_retry_wait<C>(
    conn: &mut C,
    segment_id: i64,
    last_error: String,
) -> Result<usize, SegmentStateError>
where
    C: SegmentDb,
{
    let sql = format!(
        "UPDATE ingest_segments \
         SET status = {}, last_error = {} \
         WHERE segment_id = {segment_id}",
        quote(SegmentStatus::RetryWait.as_db_str()),
        quote(&last_error)
    );

    Ok(conn.execute_sql(&sql)?)
}

/// Marks a segment as `dead_letter` after retry exhaustion.
pub fn mark_segment_dead_letter<C>(
    conn: &mut C,
    segment_id: i64,
    last_error: String,
) -> Result<usize, SegmentStateError>
where
    C: SegmentDb,
{
    let sql = format!(
        "UPDATE ingest_segments \
         SET status = {}, last_error = {} \
         WHERE segment_id = {segment_id}",
        quote(SegmentStatus::DeadLetter.as_db_str()),
        quote(&last_error)
    );

    Ok(conn.execute_sql(&sql)?)
}

/// Upserts one per-item exception row for a segment.
pub fn upsert_exception<C>(
    conn: &mut C,
    segment_id: i64,
    item_id: i64,
    state: ExceptionState,
    attempts: i32,
    last_error: Option<String>,
) -> Result<usize, SegmentStateError>
where
    C: SegmentDb,
{
    if attempts < 0 {
        return Err(SegmentStateError::InvalidInput(format!(
            "attempts must be >= 0, got {attempts}"
        )));
    }

    let sql = format!(
        "INSERT INTO ingest_exceptions (segment_id, item_id, state, attempts, last_error) \
         VALUES ({segment_id}, {item_id}, {}, {attempts}, {}) \
         ON CONFLICT(segment_id, item_id) DO UPDATE SET \
             state = EXCLUDED.state, \
             attempts = EXCLUDED.attempts, \
             last_error = EXCLUDED.last_error, \
             updated_at = CURRENT_TIMESTAMP",
        quote(state.as_db_str()),
        quote_opt(last_error.as_deref()),
    );

    Ok(conn.execute_sql(&sql)?)
}

/// Lists exception rows by state.
pub fn list_exceptions_by_state<C>(
    conn: &mut C,
    state: ExceptionState,
    limit: i64,
) -> Result<Vec<IngestException>, SegmentStateError>
where
    C: SegmentDb,
{
    if limit <= 0 {
        return Err(SegmentStateError::InvalidInput(format!(
            "limit must be > 0, got {limit}"
        )));
    }

    let sql = format!(
        "SELECT segment_id, item_id, state, attempts, last_error \
         FROM ingest_exceptions \
         WHERE state = {} \
         ORDER BY segment_id ASC, item_id ASC \
         LIMIT {limit}",
        quote(state.as_db_str())
    );

    conn.load_exceptions(&sql)?
        .into_iter()
        .map(map_exception_row)
        .collect()
}

/// Computes the contiguous sealed frontier from completed segments.
///
/// Frontier definition:
/// largest `id` such that every ID in `[1, id]` is covered by at least one `done` segment.
///
/// Overlapping or nested done segments are handled by interval-merge logic.
pub fn compute_frontier_id<C>(conn: &mut C) -> Result<i64, SegmentStateError>
where
    C: SegmentDb,
{
    let sql = format!(
        "SELECT start_id, end_id FROM ingest_segments \
         WHERE status = {} \
         ORDER BY start_id ASC, end_id ASC",
        quote(SegmentStatus::Done.as_db_str())
    );

    let ranges = conn.load_ranges(&sql)?;
    let mut frontier = 0i64;
    for row in ranges {
        if row.start_id > frontier + 1 {
            break;
        }
        if row.end_id > frontier {
            frontier = row.end_id;
        }
    }

    Ok(frontier)
}

/// Lists segment ranges that already exist for a planning window.
///
/// We treat existing rows in any durable segment status as coverage to avoid duplicate segment
/// materialization. Dead letters remain auditable and replayable, so they should not be silently
/// overwritten by new rows.
fn list_existing_ranges_for_window<C>(
    conn: &mut C,
    start_id: i64,
    end_id: i64,
) -> Result<Vec<(i64, i64)>, SegmentStateError>
where
    C: SegmentDb,
{
    let sql = format!(
        "SELECT start_id, end_id FROM ingest_segments \
         WHERE end_id >= {start_id} \
           AND start_id <= {end_id} \
           AND status IN ({}, {}, {}, {}, {}) \
         ORDER BY start_id ASC, end_id ASC",
        quote(SegmentStatus::Pending.as_db_str()),
        quote(SegmentStatus::InProgress.as_db_str()),
        quote(SegmentStatus::Done.as_db_str()),
        quote(SegmentStatus::RetryWait.as_db_str()),
        quote(SegmentStatus::DeadLetter.as_db_str()),
    );

    Ok(conn
        .load_ranges(&sql)?
        .into_iter()
        .map(|row| (row.start_id, row.end_id))
        .collect())
}

/// Lists pending segments overlapping the planning window.
fn list_pending_segments_for_window<C>(
    conn: &mut C,
    start_id: i64,
    end_id: i64,
) -> Result<Vec<IngestSegment>, SegmentStateError>
where
    C: SegmentDb,
{
    if start_id > end_id {
        return Ok(Vec::new());
    }

    let sql = format!(
        "SELECT segment_id, start_id, end_id, status, attempts, scan_cursor_id, unresolved_count, last_error \
         FROM ingest_segments \
         WHERE status = {} \
           AND end_id >= {start_id} \
           AND start_id <= {end_id} \
         ORDER BY start_id ASC, segment_id ASC",
        quote(SegmentStatus::Pending.as_db_str()),
    );

    conn.load_segments(&sql)?
        .into_iter()
        .map(map_segment_row)
        .collect()
}

/// Computes uncovered ranges in `[start_id, end_id]` after interval merge over existing coverage.
fn compute_uncovered_ranges(
    start_id: i64,
    end_id: i64,
    existing_ranges: &[(i64, i64)],
) -> Vec<(i64, i64)> {
    if start_id > end_id {
        return Vec::new();
    }

    let mut cursor = start_id;
    let mut uncovered = Vec::new();

    for (range_start, range_end) in existing_ranges {
        if *range_end < cursor {
            continue;
        }
        if *range_start > end_id {
            break;
        }
        if *range_start > cursor {
            uncovered.push((cursor, range_start.saturating_sub(1)));
        }
        if *range_end >= cursor {
            cursor = range_end.saturating_add(1);
        }
        if cursor > end_id {
            break;
        }
    }

    if cursor <= end_id {
        uncovered.push((cursor, end_id));
    }

    uncovered
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::sqlite_test::setup_in_memory_sqlite;

    #[test]
    fn happy_path_advances_frontier_across_range() {
        let mut conn = setup_in_memory_sqlite();

        let segment_ids =
            enqueue_range(&mut conn, 1, 2500, 1000).expect("failed to enqueue catchup segments");
        assert_eq!(segment_ids.len(), 3);

        let first = claim_next_pending_segment(&mut conn)
            .expect("failed to claim first segment")
            .expect("expected a pending segment");
        assert_eq!(first.start_id, 1);
        assert_eq!(first.end_id, 1000);
        assert_eq!(first.status, SegmentStatus::InProgress);
        assert_eq!(first.attempts, 1);

        update_segment_progress(&mut conn, first.segment_id, Some(500), 500)
            .expect("failed to update first segment progress");
        mark_segment_done(&mut conn, first.segment_id, Some(1000))
            .expect("failed to mark first segment done");

        let second = claim_next_pending_segment(&mut conn)
            .expect("failed to claim second segment")
            .expect("expected second pending segment");
        mark_segment_done(&mut conn, second.segment_id, Some(2000))
            .expect("failed to mark second segment done");

        let third = claim_next_pending_segment(&mut conn)
            .expect("failed to claim third segment")
            .expect("expected third pending segment");
        mark_segment_done(&mut conn, third.segment_id, Some(2500))
            .expect("failed to mark third segment done");

        let frontier = compute_frontier_id(&mut conn).expect("failed to compute frontier");
        assert_eq!(frontier, 2500);
    }

    #[test]
    fn restore_requeues_crashed_in_progress_segments() {
        let mut conn = setup_in_memory_sqlite();

        enqueue_range(&mut conn, 1, 1500, 1000).expect("failed to enqueue restore range");

        let first_claim = claim_next_pending_segment(&mut conn)
            .expect("failed to claim segment before simulated crash")
            .expect("expected a segment to claim");
        assert_eq!(first_claim.attempts, 1);
        assert_eq!(first_claim.status, SegmentStatus::InProgress);

        // Simulate a process crash where in-progress work is left behind.
        let requeued = requeue_in_progress_segments(&mut conn)
            .expect("failed to requeue in-progress segments");
        assert_eq!(requeued, 1);

        let second_claim = claim_next_pending_segment(&mut conn)
            .expect("failed to claim segment after simulated restore")
            .expect("expected segment to be reclaimable after restore");
        assert_eq!(second_claim.segment_id, first_claim.segment_id);
        assert_eq!(second_claim.attempts, 2);

        mark_segment_done(&mut conn, second_claim.segment_id, Some(1000))
            .expect("failed to complete restored segment");

        let frontier =
            compute_frontier_id(&mut conn).expect("failed to compute frontier after restore");
        assert_eq!(frontier, 1000);
    }

    #[test]
    fn dead_letter_is_auditable_and_replayable() {
        let mut conn = setup_in_memory_sqlite();

        enqueue_range(&mut conn, 1, 2000, 1000).expect("failed to enqueue range");

        let first = claim_next_pending_segment(&mut conn)
            .expect("failed to claim first")
            .expect("expected first segment");
        mark_segment_done(&mut conn, first.segment_id, Some(1000))
            .expect("failed to mark first done");

        let second = claim_next_pending_segment(&mut conn)
            .expect("failed to claim second")
            .expect("expected second segment");
        mark_segment_retry_wait(
            &mut conn,
            second.segment_id,
            "upstream timeout while fetching id 1500".to_string(),
        )
        .expect("failed to move segment to retry_wait");
        mark_segment_dead_letter(
            &mut conn,
            second.segment_id,
            "retry budget exhausted after 8 attempts".to_string(),
        )
        .expect("failed to dead-letter segment");

        upsert_exception(
            &mut conn,
            second.segment_id,
            1500,
            ExceptionState::DeadLetter,
            8,
            Some("HTTP 503 from Firebase".to_string()),
        )
        .expect("failed to record dead-letter exception");

        let frontier =
            compute_frontier_id(&mut conn).expect("failed to compute frontier with dead letter");
        assert_eq!(frontier, 1000);

        let dead_letters = list_exceptions_by_state(&mut conn, ExceptionState::DeadLetter, 100)
            .expect("failed to list dead-letter exceptions");
        assert_eq!(dead_letters.len(), 1);
        assert_eq!(dead_letters[0].item_id, 1500);

        // Replay flow: move segment and exception back to pending and process successfully.
        set_segment_pending(&mut conn, second.segment_id)
            .expect("failed to set dead-letter segment back to pending");
        upsert_exception(
            &mut conn,
            second.segment_id,
            1500,
            ExceptionState::Pending,
            0,
            None,
        )
        .expect("failed to set exception back to pending");

        let replay_claim = claim_next_pending_segment(&mut conn)
            .expect("failed to claim replayed segment")
            .expect("expected replayed pending segment");
        assert_eq!(replay_claim.segment_id, second.segment_id);

        mark_segment_done(&mut conn, replay_claim.segment_id, Some(2000))
            .expect("failed to complete replayed segment");

        let replayed = get_segment(&mut conn, replay_claim.segment_id)
            .expect("failed to read replayed segment")
            .expect("expected replayed segment row");
        assert_eq!(replayed.status, SegmentStatus::Done);
        assert_eq!(replayed.unresolved_count, 0);

        let final_frontier =
            compute_frontier_id(&mut conn).expect("failed to compute final frontier");
        assert_eq!(final_frontier, 2000);
    }

    #[test]
    fn prepare_catchup_handles_fresh_db_bootstrap() {
        let mut conn = setup_in_memory_sqlite();

        let preparation = prepare_catchup_segments(&mut conn, 2500, None, 1000, false)
            .expect("failed to prepare catchup for fresh db");

        assert_eq!(preparation.frontier_id, 0);
        assert_eq!(preparation.planning_start_id, 1);
        assert_eq!(preparation.requeued_in_progress, 0);
        assert_eq!(preparation.reactivated_retry_wait, 0);
        assert_eq!(preparation.created_segment_ids.len(), 3);
        assert_eq!(preparation.pending_segments.len(), 3);

        let ranges: Vec<(i64, i64)> = preparation
            .pending_segments
            .iter()
            .map(|segment| (segment.start_id, segment.end_id))
            .collect();
        assert_eq!(ranges, vec![(1, 1000), (1001, 2000), (2001, 2500)]);
    }

    #[test]
    fn prepare_catchup_honors_cold_start_without_duplicate_ranges() {
        let mut conn = setup_in_memory_sqlite();

        let seeded = enqueue_range(&mut conn, 1, 2000, 1000).expect("failed to seed done ranges");
        for segment_id in seeded {
            mark_segment_done(&mut conn, segment_id, None).expect("failed to mark seed as done");
        }

        let preparation = prepare_catchup_segments(&mut conn, 3200, Some(500), 1000, false)
            .expect("failed to prepare catchup with cold start");

        assert_eq!(preparation.frontier_id, 2000);
        assert_eq!(preparation.planning_start_id, 500);
        assert_eq!(preparation.created_segment_ids.len(), 2);

        let ranges: Vec<(i64, i64)> = preparation
            .pending_segments
            .iter()
            .map(|segment| (segment.start_id, segment.end_id))
            .collect();
        assert_eq!(ranges, vec![(2001, 3000), (3001, 3200)]);
    }

    #[test]
    fn prepare_catchup_recovers_crash_reactivates_retryables_and_fills_holes() {
        let mut conn = setup_in_memory_sqlite();

        let done_segment = enqueue_segment(&mut conn, 1, 1000).expect("failed to seed done row");
        mark_segment_done(&mut conn, done_segment, Some(1000))
            .expect("failed to mark first segment done");

        let in_progress_segment =
            enqueue_segment(&mut conn, 1001, 2000).expect("failed to seed in-progress row");
        let claimed = claim_next_pending_segment(&mut conn)
            .expect("failed to claim seed segment")
            .expect("expected one pending segment");
        assert_eq!(claimed.segment_id, in_progress_segment);

        let retry_segment =
            enqueue_segment(&mut conn, 3001, 4000).expect("failed to seed retry-wait row");
        mark_segment_retry_wait(
            &mut conn,
            retry_segment,
            "transient upstream failure while processing segment".to_string(),
        )
        .expect("failed to mark retry-wait segment");

        let preparation = prepare_catchup_segments(&mut conn, 4000, None, 1000, false)
            .expect("failed to prepare catchup after simulated crash");

        assert_eq!(preparation.frontier_id, 1000);
        assert_eq!(preparation.planning_start_id, 1001);
        assert_eq!(preparation.requeued_in_progress, 1);
        assert_eq!(preparation.reactivated_retry_wait, 1);
        assert_eq!(preparation.created_segment_ids.len(), 1);

        let ranges: Vec<(i64, i64)> = preparation
            .pending_segments
            .iter()
            .map(|segment| (segment.start_id, segment.end_id))
            .collect();
        assert_eq!(ranges, vec![(1001, 2000), (2001, 3000), (3001, 4000)]);
    }

    #[test]
    fn prepare_catchup_rejects_invalid_inputs() {
        let mut conn = setup_in_memory_sqlite();

        let err = prepare_catchup_segments(&mut conn, 0, None, 1000, false)
            .expect_err("expected invalid target_max_id error");
        assert!(matches!(err, SegmentStateError::InvalidInput(_)));

        let err = prepare_catchup_segments(&mut conn, 10, Some(0), 1000, false)
            .expect_err("expected invalid cold_start_id error");
        assert!(matches!(err, SegmentStateError::InvalidInput(_)));

        let err = prepare_catchup_segments(&mut conn, 10, None, 0, false)
            .expect_err("expected invalid segment_width error");
        assert!(matches!(err, SegmentStateError::InvalidInput(_)));
    }

    #[test]
    fn prepare_catchup_force_replay_resets_done_window_to_pending() {
        let mut conn = setup_in_memory_sqlite();

        let seeded = enqueue_range(&mut conn, 1, 3000, 1000).expect("failed to seed done ranges");
        for segment_id in seeded {
            mark_segment_done(&mut conn, segment_id, Some(3000))
                .expect("failed to mark segment done");
        }

        let preparation = prepare_catchup_segments(&mut conn, 2000, Some(1), 1000, true)
            .expect("failed to prepare forced replay");

        let ranges: Vec<(i64, i64)> = preparation
            .pending_segments
            .iter()
            .map(|segment| (segment.start_id, segment.end_id))
            .collect();
        assert_eq!(ranges, vec![(1, 1000), (1001, 2000)]);
    }
}
