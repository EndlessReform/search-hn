use diesel::insert_into;
use diesel::pg::upsert::excluded;
use diesel::prelude::*;
use diesel_async::pooled_connection::deadpool::Pool;
use diesel_async::RunQueryDsl;
use log::info;

use super::error::Error;
use crate::db::models;
use crate::db::schema::items;
use crate::db::schema::kids;
use crate::firebase_listener::FirebaseListener;

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

pub enum WorkerMode {
    Catchup,
    Updater,
}

pub async fn worker(
    firebase_url: &str,
    min_id: Option<i64>,
    max_id: Option<i64>,
    pool: Pool<diesel_async::AsyncPgConnection>,
    mode: WorkerMode,
    receiver: Option<flume::Receiver<i64>>,
) -> Result<(), Error> {
    // TODO: Magic number, fix this
    const FLUSH_INTERVAL: usize = 1000;
    let fb = FirebaseListener::new(firebase_url.to_string())
        .map_err(|_| Error::ConnectError("HALP".into()))?;

    let mut items_batch: Vec<models::Item> = Vec::new();
    let mut kids_batch: Vec<models::Kid> = Vec::new();

    match mode {
        WorkerMode::Catchup => {
            if let (Some(min_id), Some(max_id)) = (min_id, max_id) {
                info!("Beginning catchup for ({:?}, {:?})", min_id, max_id);
                for i in min_id..=max_id {
                    if i != 0 {
                        download_item(&fb, i, &mut items_batch, &mut kids_batch).await?;
                        // info!("Downloaded {}, batch length: {}", i, items_batch.len());
                    }
                    if items_batch.len() == FLUSH_INTERVAL {
                        info!("Pushing {} to {}", (i - items_batch.len() as i64 + 1), i);
                        upload_items(&pool, &mut items_batch, &mut kids_batch).await?;
                    }
                }
                info!("Length of items_batch: {}", items_batch.len());
                // Flush any remaining items in the batch after the loop ends
                if !items_batch.is_empty() {
                    info!(
                        "Pushing {} to {}",
                        (max_id - items_batch.len() as i64 + 1),
                        max_id
                    );
                    upload_items(&pool, &mut items_batch, &mut kids_batch).await?;
                } else {
                    panic!("Perverse situation");
                }
            }
        }
        WorkerMode::Updater => {
            let receiver = receiver.ok_or(Error::ConnectError("No channel provided!".into()))?;
            while let Ok(id) = receiver.recv_async().await {
                download_item(&fb, id, &mut items_batch, &mut kids_batch).await?;
                info!("Pushing {}", id);
                upload_items(&pool, &mut items_batch, &mut kids_batch).await?;
            }
        }
    }
    Ok(())
}
