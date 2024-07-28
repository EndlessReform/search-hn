use diesel::insert_into;
use diesel::pg::upsert::excluded;
use diesel::prelude::*;
use diesel_async::pooled_connection::deadpool::Pool;
use diesel_async::RunQueryDsl;
use governor::clock::DefaultClock;
use governor::state::InMemoryState;
use governor::state::NotKeyed;
use governor::RateLimiter;
use log::{error, info};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

use crate::catchup::error::Error;
use crate::db::models;
use crate::db::schema::items;
use crate::db::schema::kids;
use crate::firebase_client::FirebaseListener;
use crate::queue::RangesQueue;
use crate::server::monitoring::CATCHUP_METRICS;

async fn download_item(
    fb: &FirebaseListener,
    id: i64,
    items_batch: &mut Vec<models::Item>,
    kids_batch: &mut Vec<models::Kid>,
) -> Result<(), Error> {
    let raw_item = fb.get_item(id).await?;
    if let Some(kids) = &raw_item.kids {
        for (idx, kid) in kids.iter().enumerate() {
            kids_batch.push(models::Kid {
                item: *&raw_item.id,
                kid: *kid,
                display_order: Some(idx as i64),
            })
        }
    }
    let item = Into::<models::Item>::into(raw_item);
    items_batch.push(item);
    Ok(())
}

async fn upload_items(
    pool: &Pool<diesel_async::AsyncPgConnection>,
    items_batch: &mut Vec<models::Item>,
    kids_batch: &mut Vec<models::Kid>,
) -> Result<(), Error> {
    let mut conn = pool.get().await?;
    insert_into(items::dsl::items)
        .values(&*items_batch)
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
        ))
        .execute(&mut conn)
        .await?;
    items_batch.clear();

    insert_into(kids::dsl::kids)
        .values(&*kids_batch)
        .on_conflict((kids::item, kids::kid))
        .do_update()
        .set(kids::display_order.eq(excluded(kids::display_order)))
        .execute(&mut conn)
        .await?;
    kids_batch.clear();
    Ok(())
}

const FLUSH_INTERVAL: usize = 1000;

pub async fn worker(
    firebase_url: &str,
    pool: Pool<diesel_async::AsyncPgConnection>,
    queue: Arc<RangesQueue>,
    rate_limiter: Arc<RateLimiter<NotKeyed, InMemoryState, DefaultClock>>,
    cancel_token: CancellationToken,
) -> Result<(), Error> {
    let fb = FirebaseListener::new(firebase_url.to_string())
        .map_err(|_| Error::ConnectError("Failed to create FirebaseListener".into()))?;

    let mut items_batch: Vec<models::Item> = Vec::new();
    let mut kids_batch: Vec<models::Kid> = Vec::new();

    while !cancel_token.is_cancelled() {
        tokio::select! {
            Ok(Some((start_id, end_id))) = queue.pop() => {
                for id in start_id..=end_id {
                    if id != 0 {
                        rate_limiter.until_ready().await;
                        match download_item(&fb, id, &mut items_batch, &mut kids_batch).await {
                            Ok(_) => {
                                if let Some(metrics) = CATCHUP_METRICS.get() {
                                    metrics.records_pulled.inc();
                                }
                            }
                            Err(e) => {
                                error!("Error downloading item {}: {:?}", id, e);
                                if let Some(metrics) = CATCHUP_METRICS.get() {
                                    metrics.error_count.inc();
                                }
                            }
                        }
                    }

                    if items_batch.len() >= FLUSH_INTERVAL {
                        info!("Flushing batch: {} to {}", (id - items_batch.len() as i64 + 1), id);
                        if let Err(e) = upload_items(&pool, &mut items_batch, &mut kids_batch).await {
                            error!("Error uploading batch: {:?}", e);
                        }
                    }
                }

                // Flush any remaining items in the batch
                if !items_batch.is_empty() {
                    info!("Flushing remaining items: {} to {}", (end_id - items_batch.len() as i64 + 1), end_id);
                    if let Err(e) = upload_items(&pool, &mut items_batch, &mut kids_batch).await {
                        error!("Error uploading final batch: {:?}", e);
                    }
                }
            }
            _ = cancel_token.cancelled() => {
                info!("Worker received cancellation signal");
                break;
            }
        }
    }

    // Final flush of any remaining items
    if !items_batch.is_empty() {
        info!("Performing final flush before shutdown");
        if let Err(e) = upload_items(&pool, &mut items_batch, &mut kids_batch).await {
            error!("Error uploading final batch during shutdown: {:?}", e);
        }
    }

    Ok(())
}
