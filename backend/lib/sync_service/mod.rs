use diesel::dsl::max;
use diesel::insert_into;
use diesel::pg::upsert::excluded;
use diesel::prelude::*;
use diesel::result::Error as DieselError;
use diesel_async::pooled_connection::deadpool::{Pool, PoolError};
use diesel_async::RunQueryDsl;
use futures::future::join_all;
use log::{debug, info};
use std::vec;
use thiserror::Error;
use tokio::task::{spawn, JoinError};

use crate::db::models;
use crate::db::schema::items;
use crate::db::schema::kids;
use crate::firebase_listener::{FirebaseListener, FirebaseListenerErr};

#[derive(Error, Debug)]
pub enum Error {
    #[error("Connection error: {0}")]
    ConnectError(String),

    #[error(transparent)]
    FirebaseError(#[from] FirebaseListenerErr),

    #[error(transparent)]
    DieselError(#[from] DieselError),

    #[error(transparent)]
    DBPoolError(#[from] PoolError),

    #[error("Task join error: {0}")]
    TaskJoinError(#[from] JoinError),
}

pub struct SyncService {
    /// Pool for Postgres DB backing up HN data
    db_pool: Pool<diesel_async::AsyncPgConnection>,
    firebase_url: String,
    num_workers: usize,
}
impl SyncService {
    pub fn new(
        firebase_url: String,
        db_pool: Pool<diesel_async::AsyncPgConnection>,
        num_workers: usize,
    ) -> Self {
        Self {
            db_pool,
            num_workers,
            firebase_url,
        }
    }

    /// `divide_ranges` somewhat fairly distributes the catchup range among workers
    fn divide_ranges(&self, min_id: i64, max_id: i64) -> Vec<(i64, i64)> {
        let coerced_nworkers: i64 = self.num_workers as i64;
        if min_id >= max_id {
            vec![]
        } else if max_id - min_id <= coerced_nworkers {
            // Potentially can't saturate workers, so give as many as possible an ID
            return (min_id..=max_id).map(|i| (i, i)).collect();
        } else {
            let num_ids_per_worker = (max_id - min_id) / coerced_nworkers;

            let mut res: Vec<(i64, i64)> = (0..coerced_nworkers)
                .map(|i| {
                    (
                        min_id + i * num_ids_per_worker,
                        min_id + (i + 1) * num_ids_per_worker - 1,
                    )
                })
                .collect();

            // for convenience, give the remainder to the last: it's only on the order of ~20, which compared to thousands of items is nothing
            if let Some(i) = res.last_mut() {
                *i = (i.0, max_id);
            }

            return res;
        }
    }

    /**
    `catchup` pulls all items from HN after the latest in the DB.

    Assumes no gaps in DB before its max ID
    */
    pub async fn catchup(
        &self,
        n_additional: Option<i64>,
        n_start: Option<i64>,
    ) -> Result<(), Error> {
        let fb = FirebaseListener::new(self.firebase_url.clone())?;
        let max_fb_id = fb.get_max_id().await?;
        info!("Current max item on HN: {}", max_fb_id);

        let mut conn = self
            .db_pool
            .get()
            .await
            .map_err(|_| Error::ConnectError("Listener could not access db pool!".into()))?;

        let max_db_item: Option<i64> = items::dsl::items
            .select(max(items::dsl::id))
            .first(&mut conn)
            .await?;
        let max_db_item = max_db_item.ok_or(Error::ConnectError(
            "Cannot find max DB item in Postgres!".into(),
        ))?;
        let min_id = match n_start {
            Some(n) => n,
            // TODO: Make this constant less arbitrary
            // About a day and a half of data
            None => max_db_item - 25_000,
        };
        let max_id = match n_additional {
            Some(n) => min_id + n,
            None => max_fb_id,
        };
        info!("Current max item in db: {:?}", max_db_item);
        let id_ranges = self.divide_ranges(min_id, max_id);
        info!("Items to download: {}", max_fb_id - max_db_item);
        info!("Ranges: {:?}", &id_ranges);

        let mut handles = Vec::new();
        for range in id_ranges.into_iter() {
            let db_pool = self.db_pool.clone();
            let fb_url = self.firebase_url.clone();
            let handle = spawn(async move {
                worker(
                    &fb_url,
                    Some(range.0),
                    Some(range.1),
                    db_pool,
                    WorkerMode::Catchup,
                    None,
                )
                .await
            });
            handles.push(handle);
        }

        let results = join_all(handles).await;
        for result in results {
            match result {
                Ok(_) => {
                    // Handle success case
                    log::debug!("Worker handled successfully!");
                }
                Err(err) => {
                    // Handle error case
                    log::error!("An error occurred in a worker: {:?}", err);
                }
            }
        }
        Ok(())
    }

    /// Realtime subscription to HN item updates
    pub async fn realtime_update(
        &self,
        num_workers: usize,
        receiver: flume::Receiver<i64>,
    ) -> Result<(), Error> {
        info!("Spawning {} realtime update workers...", num_workers);
        let mut update_worker_handles = Vec::new();
        for _ in 0..num_workers {
            let worker_receiver = receiver.clone();
            let firebase_url = self.firebase_url.clone();
            let db_pool = self.db_pool.clone();
            let handle = tokio::spawn(async move {
                worker(
                    &firebase_url,
                    None,
                    None,
                    db_pool,
                    WorkerMode::Updater,
                    Some(worker_receiver),
                )
                .await
            });
            update_worker_handles.push(handle);
        }
        info!("Successfully spawned all realtime update workers.");
        Ok(())
    }
}

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

enum WorkerMode {
    Catchup,
    Updater,
}

async fn worker(
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
                for i in min_id..=max_id {
                    download_item(&fb, i, &mut items_batch, &mut kids_batch).await?;
                    if items_batch.len() == FLUSH_INTERVAL || i == max_id {
                        info!("Pushing {} to {}", (i - items_batch.len() as i64), i);
                        upload_items(&pool, &mut items_batch, &mut kids_batch).await?;
                    }
                }
            }
        }
        WorkerMode::Updater => {
            let receiver = receiver.ok_or(Error::ConnectError("No channel provided!".into()))?;
            while let Ok(id) = receiver.recv_async().await {
                download_item(&fb, id, &mut items_batch, &mut kids_batch).await?;
                debug!("Pushing {}", id);
                upload_items(&pool, &mut items_batch, &mut kids_batch).await?;
            }
        }
    }
    Ok(())
}
