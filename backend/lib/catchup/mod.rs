pub mod error;
mod ranges;

use diesel::dsl::max;
use diesel::prelude::*;
use diesel_async::pooled_connection::deadpool::Pool;
use diesel_async::RunQueryDsl;
use log::info;
use std::sync::Arc;
use std::vec;

use crate::db::schema::items;
use crate::firebase_client::FirebaseListener;
use crate::queue::{Job, RangesQueue};
use error::Error;
use ranges::get_missing_ranges;

pub struct CatchupService {
    /// Pool for Postgres DB backing up HN data
    db_pool: Pool<diesel_async::AsyncPgConnection>,
    firebase_url: String,
    queue: Arc<RangesQueue>,
    num_workers: usize,
    min_ids_per_worker: usize,
}

impl CatchupService {
    pub fn new(
        firebase_url: String,
        db_pool: Pool<diesel_async::AsyncPgConnection>,
        queue: Arc<RangesQueue>,
        num_workers: usize,
    ) -> Self {
        Self {
            db_pool,
            num_workers,
            firebase_url,
            queue,
            // TODO: Make this an option
            min_ids_per_worker: 100,
        }
    }

    async fn heal(&self, max_id: i64) -> Result<Vec<(i64, i64)>, Error> {
        let mut conn = self
            .db_pool
            .get()
            .await
            .map_err(|_| Error::ConnectError("Listener could not access db pool!".into()))?;

        Ok(get_missing_ranges(&mut conn, 0, max_id).await?)
    }

    /// `divide_ranges` somewhat fairly distributes the catchup range among workers
    fn divide_ranges(&self, min_id: i64, max_id: i64) -> Vec<(i64, i64)> {
        let coerced_nworkers: i64 = self.num_workers as i64;
        let min_ids_per_worker: i64 = self.min_ids_per_worker as i64;

        if min_id >= max_id {
            return vec![];
        }
        let total_ids = max_id - min_id + 1;
        // Ensure at least 1 ID per worker
        let max_workers = (total_ids / min_ids_per_worker).max(1);

        let actual_nworkers = coerced_nworkers.min(max_workers);
        // Ensure at least 1 ID per worker
        let num_ids_per_worker = (total_ids / actual_nworkers).max(1);

        let mut res: Vec<(i64, i64)> = (0..actual_nworkers)
            .map(|i| {
                let start_id = min_id + i * num_ids_per_worker;
                let end_id = if i == actual_nworkers - 1 {
                    max_id
                } else {
                    min_id + (i + 1) * num_ids_per_worker - 1
                };
                (start_id, end_id)
            })
            .collect();

        // Ensure last range ends at max_id
        if let Some(last) = res.last_mut() {
            last.1 = max_id;
        }
        res
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
        let max_db_item = max_db_item.unwrap_or(0);

        let min_id = match n_start {
            Some(n) => n,
            None => max_db_item,
        };
        let max_id = match n_additional {
            Some(n) => min_id + n,
            None => max_fb_id,
        };
        info!("Healing mode: Checking if DB has missing ranges (this may take a while)");
        let mut id_ranges = self.heal(max_id).await?;
        if !id_ranges.is_empty() {
            info!("Missing ranges on DB: {:?}", id_ranges);
        } else {
            info!("No missing ranges on DB");
        }
        info!("Current max item in db: {:?}", max_db_item);
        info!("Items to download: {}", max_id - max_db_item);
        // id_ranges = [id_ranges, self.divide_ranges(min_id, max_id)].concat();
        // TODO turn healing back on
        id_ranges = self.divide_ranges(min_id, max_id);
        info!("Ranges: {:?}", &id_ranges);

        for range in id_ranges {
            self.queue.push(&Job::catchup(range)).await?;
        }
        Ok(())
    }
}
