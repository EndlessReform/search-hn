use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;
use std::time::Duration;

use futures::future::BoxFuture;
use hn_core::HnItem;

use super::super::types::{
    BatchPolicy, FetchError, FetchErrorKind, FetchItemResponse, IngestWorkerConfig, RetryPolicy,
};
use super::{BatchPersister, ItemFetcher};
use crate::db::models;
use crate::sync_service::types::PersistError;

pub(super) fn test_worker_config(max_attempts: u32, batch_size: usize) -> IngestWorkerConfig {
    IngestWorkerConfig {
        retry_policy: RetryPolicy {
            max_attempts,
            initial_backoff: Duration::ZERO,
            max_backoff: Duration::ZERO,
            jitter: Duration::ZERO,
        },
        batch_policy: BatchPolicy {
            max_items: batch_size,
        },
    }
}

pub(super) fn sample_item(id: i64, deleted: bool, kids: Option<Vec<i64>>) -> HnItem {
    HnItem {
        id,
        deleted: Some(deleted),
        type_: Some("story".to_string()),
        by: Some("alice".to_string()),
        time: Some(1_700_000_000),
        text: Some("hello".to_string()),
        dead: Some(false),
        parent: None,
        poll: None,
        url: Some("https://example.com".to_string()),
        score: Some(42),
        title: Some(format!("item-{id}")),
        parts: None,
        descendants: Some(0),
        kids,
    }
}

#[derive(Default)]
pub(super) struct MockFetcher {
    plans: Mutex<HashMap<i64, VecDeque<Result<FetchItemResponse, FetchError>>>>,
    call_counts: Mutex<HashMap<i64, u32>>,
}

impl MockFetcher {
    pub(super) fn with_plan(plan: Vec<(i64, Vec<Result<FetchItemResponse, FetchError>>)>) -> Self {
        let mut plans = HashMap::new();
        for (item_id, entries) in plan {
            plans.insert(item_id, entries.into_iter().collect());
        }
        Self {
            plans: Mutex::new(plans),
            call_counts: Mutex::new(HashMap::new()),
        }
    }

    pub(super) fn calls_for(&self, item_id: i64) -> u32 {
        *self
            .call_counts
            .lock()
            .expect("call_count mutex poisoned")
            .get(&item_id)
            .unwrap_or(&0)
    }
}

impl ItemFetcher for MockFetcher {
    fn fetch_item<'a>(
        &'a self,
        item_id: i64,
    ) -> BoxFuture<'a, Result<FetchItemResponse, FetchError>> {
        Box::pin(async move {
            {
                let mut counts = self.call_counts.lock().expect("call_count mutex poisoned");
                *counts.entry(item_id).or_insert(0) += 1;
            }

            let mut plans = self.plans.lock().expect("plans mutex poisoned");
            let responses = plans.get_mut(&item_id).ok_or_else(|| {
                FetchError::new(
                    FetchErrorKind::Other,
                    format!("no scripted fetch response for item {item_id}"),
                )
            })?;

            responses.pop_front().ok_or_else(|| {
                FetchError::new(
                    FetchErrorKind::Other,
                    format!("scripted responses exhausted for item {item_id}"),
                )
            })?
        })
    }
}

#[derive(Default)]
pub(super) struct MockPersister {
    outcomes: Mutex<VecDeque<Result<(), PersistError>>>,
    calls: Mutex<u32>,
    persisted_ids: Mutex<Vec<i64>>,
}

impl MockPersister {
    pub(super) fn with_outcomes(outcomes: Vec<Result<(), PersistError>>) -> Self {
        Self {
            outcomes: Mutex::new(outcomes.into_iter().collect()),
            calls: Mutex::new(0),
            persisted_ids: Mutex::new(Vec::new()),
        }
    }

    pub(super) fn calls(&self) -> u32 {
        *self.calls.lock().expect("calls mutex poisoned")
    }

    pub(super) fn persisted_ids(&self) -> Vec<i64> {
        self.persisted_ids
            .lock()
            .expect("persisted_ids mutex poisoned")
            .clone()
    }
}

impl BatchPersister for MockPersister {
    fn persist_batch<'a>(
        &'a self,
        items_batch: &'a [models::Item],
        _kids_batch: &'a [models::Kid],
    ) -> BoxFuture<'a, Result<(), PersistError>> {
        Box::pin(async move {
            *self.calls.lock().expect("calls mutex poisoned") += 1;

            let next = self
                .outcomes
                .lock()
                .expect("outcomes mutex poisoned")
                .pop_front()
                .unwrap_or(Ok(()));

            if next.is_ok() {
                let mut persisted = self
                    .persisted_ids
                    .lock()
                    .expect("persisted_ids mutex poisoned");
                persisted.extend(items_batch.iter().map(|item| item.id));
            }

            next
        })
    }
}
