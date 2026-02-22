use std::sync::Arc;

use futures::future::BoxFuture;

use crate::firebase_listener::{FirebaseListener, FirebaseListenerErr};

use super::super::types::{FetchError, FetchItemResponse, GlobalRateLimiter};
use super::error_mapping::map_firebase_error;

/// Fetches one HN item by ID.
///
/// This trait exists so worker logic can be unit-tested against deterministic scripted failures
/// without requiring live network access.
pub trait ItemFetcher: Send + Sync {
    fn fetch_item<'a>(
        &'a self,
        item_id: i64,
    ) -> BoxFuture<'a, Result<FetchItemResponse, FetchError>>;
}

impl<T> ItemFetcher for Arc<T>
where
    T: ItemFetcher + ?Sized,
{
    fn fetch_item<'a>(
        &'a self,
        item_id: i64,
    ) -> BoxFuture<'a, Result<FetchItemResponse, FetchError>> {
        (**self).fetch_item(item_id)
    }
}

/// Firebase-backed fetcher implementation used by production runtime.
pub struct FirebaseItemFetcher {
    listener: FirebaseListener,
    global_rate_limiter: GlobalRateLimiter,
}

impl FirebaseItemFetcher {
    /// Builds a fetcher that shares a single global request budget with all ingest workers.
    ///
    /// The limiter is intentionally injected here (rather than around worker loops) so retries
    /// are also constrained by the same RPS budget.
    pub fn new(
        firebase_url: String,
        global_rate_limiter: GlobalRateLimiter,
    ) -> Result<Self, FirebaseListenerErr> {
        Ok(Self {
            listener: FirebaseListener::new(firebase_url)?,
            global_rate_limiter,
        })
    }
}

impl ItemFetcher for FirebaseItemFetcher {
    fn fetch_item<'a>(
        &'a self,
        item_id: i64,
    ) -> BoxFuture<'a, Result<FetchItemResponse, FetchError>> {
        Box::pin(async move {
            self.global_rate_limiter.until_ready().await;
            match self.listener.get_item(item_id).await {
                Ok(Some(item)) => Ok(FetchItemResponse::Found(item)),
                Ok(None) => Ok(FetchItemResponse::Missing),
                Err(err) => Err(map_firebase_error(err)),
            }
        })
    }
}
