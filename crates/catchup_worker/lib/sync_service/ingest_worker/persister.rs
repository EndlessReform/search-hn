use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use diesel::insert_into;
use diesel::pg::upsert::excluded;
use diesel::prelude::*;
use diesel::sql_types::{BigInt, Nullable};
use diesel_async::pooled_connection::deadpool::Pool;
use diesel_async::RunQueryDsl;
use futures::future::BoxFuture;

use crate::db::models;
use crate::db::schema::{items, kids};
use crate::server::monitoring::INGEST_METRICS;

use super::super::types::PersistError;
use super::error_mapping::map_diesel_error;

/// Persists a batch of item rows and item->kid edges.
///
/// This is intentionally abstracted so we can test transient/fatal persistence behavior without
/// requiring a Postgres instance.
pub trait BatchPersister: Send + Sync {
    fn persist_batch<'a>(
        &'a self,
        items_batch: &'a [models::Item],
        kids_batch: &'a [models::Kid],
        source: IngestSource,
    ) -> BoxFuture<'a, Result<(), PersistError>>;
}

/// Runtime pipeline that initiated one persistence batch.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IngestSource {
    Catchup,
    Realtime,
}

impl IngestSource {
    /// Returns the stable source label used in metrics dimensions.
    pub fn as_label(self) -> &'static str {
        match self {
            Self::Catchup => "catchup",
            Self::Realtime => "realtime",
        }
    }
}

impl<T> BatchPersister for Arc<T>
where
    T: BatchPersister + ?Sized,
{
    fn persist_batch<'a>(
        &'a self,
        items_batch: &'a [models::Item],
        kids_batch: &'a [models::Kid],
        source: IngestSource,
    ) -> BoxFuture<'a, Result<(), PersistError>> {
        (**self).persist_batch(items_batch, kids_batch, source)
    }
}

/// Postgres-backed batch persister used by production runtime.
pub struct PgBatchPersister {
    pool: Pool<diesel_async::AsyncPgConnection>,
}

impl PgBatchPersister {
    pub fn new(pool: Pool<diesel_async::AsyncPgConnection>) -> Self {
        Self { pool }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ParentStoryRef {
    pub(crate) type_: Option<String>,
    pub(crate) story_id: Option<i64>,
}

/// Loads parent rows needed to resolve `story_id` for unresolved comment writes.
///
/// The lookup intentionally pulls only parent metadata (`id`, `type`, `story_id`), so we avoid
/// loading full `items` rows when all we need is root-story lineage.
async fn load_parent_story_refs(
    conn: &mut diesel_async::AsyncPgConnection,
    items_batch: &[models::Item],
) -> Result<HashMap<i64, ParentStoryRef>, PersistError> {
    let parent_ids: Vec<i64> = items_batch
        .iter()
        .filter(|item| item.type_.as_deref() == Some("comment") && item.story_id.is_none())
        .filter_map(|item| item.parent)
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();

    if parent_ids.is_empty() {
        return Ok(HashMap::new());
    }

    let parent_rows: Vec<(i64, Option<String>, Option<i64>)> = items::dsl::items
        .select((items::id, items::type_, items::story_id))
        .filter(items::id.eq_any(parent_ids))
        .load(conn)
        .await
        .map_err(map_diesel_error)?;

    Ok(parent_rows
        .into_iter()
        .map(|(id, type_, story_id)| (id, ParentStoryRef { type_, story_id }))
        .collect())
}

/// Resolves denormalized `story_id` values in-place for one write batch.
///
/// Rules:
/// - Story rows always write `story_id = id`.
/// - Comment rows inherit from their parent: parent story -> `parent.id`, parent comment ->
///   `parent.story_id`.
/// - If the parent chain is unavailable during this batch, `story_id` remains `NULL` and is
///   expected to be repaired by later replays/backfill.
///
/// The algorithm performs bounded fixed-point passes so comment chains already present in the
/// same batch can resolve without extra database round-trips.
pub(crate) fn resolve_story_ids_from_known_parents(
    items_batch: &mut [models::Item],
    known_parents: &HashMap<i64, ParentStoryRef>,
) {
    let mut lineage: HashMap<i64, ParentStoryRef> = known_parents.clone();

    for item in items_batch.iter_mut() {
        match item.type_.as_deref() {
            Some("story") => {
                item.story_id = Some(item.id);
            }
            Some("comment") => {}
            _ => {
                item.story_id = None;
            }
        }

        lineage.insert(
            item.id,
            ParentStoryRef {
                type_: item.type_.clone(),
                story_id: item.story_id,
            },
        );
    }

    let mut changed = true;
    while changed {
        changed = false;

        for item in items_batch.iter_mut() {
            if item.type_.as_deref() != Some("comment") || item.story_id.is_some() {
                continue;
            }

            let Some(parent_id) = item.parent else {
                continue;
            };
            let Some(parent_ref) = lineage.get(&parent_id) else {
                continue;
            };

            let resolved = if parent_ref.type_.as_deref() == Some("story") {
                Some(parent_id)
            } else {
                parent_ref.story_id
            };

            if let Some(story_id) = resolved {
                item.story_id = Some(story_id);
                lineage.insert(
                    item.id,
                    ParentStoryRef {
                        type_: item.type_.clone(),
                        story_id: item.story_id,
                    },
                );
                changed = true;
            }
        }
    }
}

impl BatchPersister for PgBatchPersister {
    fn persist_batch<'a>(
        &'a self,
        items_batch: &'a [models::Item],
        kids_batch: &'a [models::Kid],
        source: IngestSource,
    ) -> BoxFuture<'a, Result<(), PersistError>> {
        Box::pin(async move {
            if items_batch.is_empty() {
                if kids_batch.is_empty() {
                    return Ok(());
                }
                return Err(PersistError::fatal(
                    "internal invariant violated: non-empty kids batch with empty items batch",
                ));
            }

            let mut conn = self.pool.get().await.map_err(|err| {
                PersistError::retryable(format!("failed to acquire DB pool connection: {err}"))
            })?;
            let mut resolved_items_batch = items_batch.to_vec();
            let known_parents = load_parent_story_refs(&mut conn, &resolved_items_batch).await?;
            resolve_story_ids_from_known_parents(&mut resolved_items_batch, &known_parents);
            let existing_item_ids =
                load_existing_item_ids(&mut conn, &resolved_items_batch).await?;

            insert_into(items::dsl::items)
                .values(&resolved_items_batch)
                .on_conflict(items::id)
                .do_update()
                .set((
                    items::deleted.eq(excluded(items::deleted)),
                    items::type_.eq(excluded(items::type_)),
                    items::by.eq(excluded(items::by)),
                    items::time.eq(excluded(items::time)),
                    items::text.eq(excluded(items::text)),
                    items::dead.eq(excluded(items::dead)),
                    items::parent.eq(excluded(items::parent)),
                    items::poll.eq(excluded(items::poll)),
                    items::url.eq(excluded(items::url)),
                    items::score.eq(excluded(items::score)),
                    items::title.eq(excluded(items::title)),
                    items::parts.eq(excluded(items::parts)),
                    items::descendants.eq(excluded(items::descendants)),
                    items::story_id.eq(diesel::dsl::sql::<Nullable<BigInt>>(
                        "COALESCE(EXCLUDED.story_id, items.story_id)",
                    )),
                ))
                .execute(&mut conn)
                .await
                .map_err(map_diesel_error)?;

            if !kids_batch.is_empty() {
                insert_into(kids::dsl::kids)
                    .values(kids_batch)
                    .on_conflict((kids::item, kids::kid))
                    .do_update()
                    .set(kids::display_order.eq(excluded(kids::display_order)))
                    .execute(&mut conn)
                    .await
                    .map_err(map_diesel_error)?;
            }

            emit_item_write_metrics(source, &resolved_items_batch, &existing_item_ids);
            Ok(())
        })
    }
}

/// Loads IDs that already exist in `items` for the incoming batch.
///
/// This powers operation-level write telemetry (`insert` vs `update`) without adding new schema.
async fn load_existing_item_ids(
    conn: &mut diesel_async::AsyncPgConnection,
    items_batch: &[models::Item],
) -> Result<HashSet<i64>, PersistError> {
    if items_batch.is_empty() {
        return Ok(HashSet::new());
    }

    let ids: Vec<i64> = items_batch.iter().map(|item| item.id).collect();
    let existing_ids: Vec<i64> = items::dsl::items
        .select(items::id)
        .filter(items::id.eq_any(ids))
        .load(conn)
        .await
        .map_err(map_diesel_error)?;

    Ok(existing_ids.into_iter().collect())
}

/// Emits one labeled write counter per persisted item row.
fn emit_item_write_metrics(
    source: IngestSource,
    items_batch: &[models::Item],
    existing_item_ids: &HashSet<i64>,
) {
    let Some(metrics) = INGEST_METRICS.get() else {
        return;
    };

    for item in items_batch {
        let operation = if existing_item_ids.contains(&item.id) {
            "update"
        } else {
            "insert"
        };
        let item_kind = classify_item_kind_label(item.type_.as_deref());
        metrics.inc_item_write(source.as_label(), operation, item_kind);
    }
}

fn classify_item_kind_label(type_: Option<&str>) -> &'static str {
    match type_ {
        Some("story") => "story",
        Some("comment") => "comment",
        _ => "other",
    }
}

/// Deduplicates one batch of item rows by `items.id`.
///
/// Why this exists:
/// - Postgres rejects `INSERT ... ON CONFLICT ... DO UPDATE` batches that contain the same
///   conflict target more than once (`cannot affect row a second time`).
/// - Segment overlap/replay edge-cases should not normally produce duplicate item IDs, but this
///   guard keeps the ingest data plane fault-tolerant if they ever do.
///
/// Keeps the last occurrence for each ID so later rows in the batch win.
pub(crate) fn dedupe_items_by_id(items_batch: &mut Vec<models::Item>) -> usize {
    if items_batch.len() < 2 {
        return 0;
    }

    let mut seen_ids: HashSet<i64> = HashSet::with_capacity(items_batch.len());
    let mut unique_reversed: Vec<models::Item> = Vec::with_capacity(items_batch.len());
    let mut duplicates = 0usize;

    for item in items_batch.drain(..).rev() {
        if seen_ids.insert(item.id) {
            unique_reversed.push(item);
        } else {
            duplicates += 1;
        }
    }

    unique_reversed.reverse();
    *items_batch = unique_reversed;
    duplicates
}

/// Deduplicates one batch of parent-child edge rows by `(kids.item, kids.kid)`.
///
/// The raw HN `kids` list is expected to be unique, but occasional upstream or replay anomalies
/// can still duplicate edges inside one SQL insert batch. Keeping this dedupe here prevents
/// conflict-target duplication failures from aborting long-running catchup jobs.
///
/// Keeps the last occurrence for each edge so latest display order wins.
pub(crate) fn dedupe_kids_by_edge(kids_batch: &mut Vec<models::Kid>) -> usize {
    if kids_batch.len() < 2 {
        return 0;
    }

    let mut seen_edges: HashSet<(i64, i64)> = HashSet::with_capacity(kids_batch.len());
    let mut unique_reversed: Vec<models::Kid> = Vec::with_capacity(kids_batch.len());
    let mut duplicates = 0usize;

    for edge in kids_batch.drain(..).rev() {
        let key = (edge.item, edge.kid);
        if seen_edges.insert(key) {
            unique_reversed.push(edge);
        } else {
            duplicates += 1;
        }
    }

    unique_reversed.reverse();
    *kids_batch = unique_reversed;
    duplicates
}

#[cfg(test)]
mod tests {
    use super::{classify_item_kind_label, IngestSource};

    #[test]
    fn item_kind_label_maps_story_comment_and_other() {
        assert_eq!(classify_item_kind_label(Some("story")), "story");
        assert_eq!(classify_item_kind_label(Some("comment")), "comment");
        assert_eq!(classify_item_kind_label(Some("poll")), "other");
        assert_eq!(classify_item_kind_label(None), "other");
    }

    #[test]
    fn ingest_source_label_is_stable() {
        assert_eq!(IngestSource::Catchup.as_label(), "catchup");
        assert_eq!(IngestSource::Realtime.as_label(), "realtime");
    }
}
