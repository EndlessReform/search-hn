use std::collections::HashMap;
use std::sync::Arc;

use super::super::types::{
    FetchError, FetchErrorKind, FetchItemResponse, ItemOutcomeKind, SegmentAttemptStatus,
    SegmentWork, FAILURE_CLASS_DECODE, FAILURE_CLASS_SCHEMA,
};
use super::error_mapping::map_firebase_error;
use super::persister::{
    dedupe_items_by_id, dedupe_kids_by_edge, resolve_story_ids_from_known_parents, ParentStoryRef,
};
use super::test_support::{sample_item, test_worker_config, MockFetcher, MockPersister};
use super::SegmentIngestExecutor;
use crate::db::models;
use crate::firebase_listener::FirebaseListenerErr;
use crate::sync_service::types::PersistError;

#[tokio::test]
async fn malformed_segment_returns_fatal_failure() {
    let fetcher = MockFetcher::default();
    let persister = MockPersister::default();
    let worker = SegmentIngestExecutor::new(fetcher, persister, test_worker_config(3, 2));

    let result = worker
        .process_segment_once(&SegmentWork {
            segment_id: 1,
            start_id: 10,
            end_id: 5,
            resume_cursor_id: None,
        })
        .await;

    assert_eq!(result.status, SegmentAttemptStatus::FatalFailure);
    assert!(result
        .failure
        .as_ref()
        .expect("expected failure")
        .message
        .as_ref()
        .expect("expected message")
        .contains("invalid segment assignment"));
    assert_eq!(
        result
            .failure
            .as_ref()
            .and_then(|failure| failure.failure_class.as_deref()),
        Some("schema")
    );
}

#[test]
fn json_parse_error_is_retryable_decode_failure() {
    let parse_err = serde_json::from_str::<serde_json::Value>("{")
        .expect_err("fixture should produce a json parse failure");
    let mapped = map_firebase_error(FirebaseListenerErr::JsonParseError(parse_err));

    assert!(mapped.is_retryable());
    assert_eq!(mapped.failure_class, FAILURE_CLASS_DECODE);
}

#[test]
fn parse_error_is_schema_failure() {
    let mapped = map_firebase_error(FirebaseListenerErr::ParseError(
        "invalid payload shape".to_string(),
    ));

    assert!(!mapped.is_retryable());
    assert_eq!(mapped.failure_class, FAILURE_CLASS_SCHEMA);
}

#[test]
fn dedupe_items_by_id_keeps_last_row_for_each_id() {
    let mut first = models::Item::from(sample_item(100, false, None));
    first.title = Some("first".to_string());
    let mut second = models::Item::from(sample_item(100, false, None));
    second.title = Some("second".to_string());
    let mut third = models::Item::from(sample_item(101, false, None));
    third.title = Some("third".to_string());

    let mut rows = vec![first, second, third];
    let duplicates = dedupe_items_by_id(&mut rows);

    assert_eq!(duplicates, 1);
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].id, 100);
    assert_eq!(rows[0].title.as_deref(), Some("second"));
    assert_eq!(rows[1].id, 101);
}

#[test]
fn dedupe_kids_by_edge_keeps_last_display_order() {
    let mut edges = vec![
        models::Kid {
            item: 42,
            kid: 7,
            display_order: Some(0),
        },
        models::Kid {
            item: 42,
            kid: 7,
            display_order: Some(3),
        },
        models::Kid {
            item: 42,
            kid: 9,
            display_order: Some(1),
        },
    ];

    let duplicates = dedupe_kids_by_edge(&mut edges);

    assert_eq!(duplicates, 1);
    assert_eq!(edges.len(), 2);
    assert_eq!(edges[0].item, 42);
    assert_eq!(edges[0].kid, 7);
    assert_eq!(edges[0].display_order, Some(3));
    assert_eq!(edges[1].kid, 9);
}

#[test]
fn resolve_story_ids_sets_story_rows_to_self_id() {
    let mut rows = vec![models::Item {
        id: 10,
        deleted: None,
        type_: Some("story".to_string()),
        by: None,
        time: None,
        text: None,
        dead: None,
        parent: None,
        poll: None,
        url: None,
        score: None,
        title: None,
        parts: None,
        descendants: None,
        story_id: None,
    }];
    let known_parents = HashMap::new();

    resolve_story_ids_from_known_parents(&mut rows, &known_parents);

    assert_eq!(rows[0].story_id, Some(10));
}

#[test]
fn resolve_story_ids_fills_comment_from_story_parent() {
    let mut rows = vec![models::Item {
        id: 20,
        deleted: None,
        type_: Some("comment".to_string()),
        by: None,
        time: None,
        text: None,
        dead: None,
        parent: Some(10),
        poll: None,
        url: None,
        score: None,
        title: None,
        parts: None,
        descendants: None,
        story_id: None,
    }];
    let known_parents = HashMap::from([(
        10,
        ParentStoryRef {
            type_: Some("story".to_string()),
            story_id: Some(10),
        },
    )]);

    resolve_story_ids_from_known_parents(&mut rows, &known_parents);

    assert_eq!(rows[0].story_id, Some(10));
}

#[test]
fn resolve_story_ids_walks_comment_chain_within_batch() {
    let mut rows = vec![
        models::Item {
            id: 21,
            deleted: None,
            type_: Some("comment".to_string()),
            by: None,
            time: None,
            text: None,
            dead: None,
            parent: Some(10),
            poll: None,
            url: None,
            score: None,
            title: None,
            parts: None,
            descendants: None,
            story_id: None,
        },
        models::Item {
            id: 22,
            deleted: None,
            type_: Some("comment".to_string()),
            by: None,
            time: None,
            text: None,
            dead: None,
            parent: Some(21),
            poll: None,
            url: None,
            score: None,
            title: None,
            parts: None,
            descendants: None,
            story_id: None,
        },
    ];
    let known_parents = HashMap::from([(
        10,
        ParentStoryRef {
            type_: Some("story".to_string()),
            story_id: Some(10),
        },
    )]);

    resolve_story_ids_from_known_parents(&mut rows, &known_parents);

    assert_eq!(rows[0].story_id, Some(10));
    assert_eq!(rows[1].story_id, Some(10));
}

#[test]
fn resolve_story_ids_leaves_unresolved_comment_as_null() {
    let mut rows = vec![models::Item {
        id: 30,
        deleted: None,
        type_: Some("comment".to_string()),
        by: None,
        time: None,
        text: None,
        dead: None,
        parent: Some(999_999),
        poll: None,
        url: None,
        score: None,
        title: None,
        parts: None,
        descendants: None,
        story_id: None,
    }];
    let known_parents = HashMap::new();

    resolve_story_ids_from_known_parents(&mut rows, &known_parents);

    assert_eq!(rows[0].story_id, None);
}

#[tokio::test]
async fn missing_and_deleted_items_are_treated_as_terminal() {
    let fetcher = MockFetcher::with_plan(vec![
        (
            100,
            vec![Ok(FetchItemResponse::Found(sample_item(
                100,
                true,
                Some(vec![200, 201]),
            )))],
        ),
        (101, vec![Ok(FetchItemResponse::Missing)]),
        (
            102,
            vec![Ok(FetchItemResponse::Found(sample_item(102, false, None)))],
        ),
    ]);
    let persister = MockPersister::default();
    let worker = SegmentIngestExecutor::new(fetcher, persister, test_worker_config(3, 2));

    let result = worker
        .process_segment_once(&SegmentWork {
            segment_id: 42,
            start_id: 100,
            end_id: 102,
            resume_cursor_id: None,
        })
        .await;

    assert_eq!(result.status, SegmentAttemptStatus::Completed);
    assert_eq!(result.unresolved_count, 0);
    assert_eq!(result.last_durable_id, Some(102));
    assert_eq!(result.terminal_missing_ids, vec![101]);
    assert!(result.failure.is_none());
}

#[tokio::test]
async fn retryable_fetch_errors_retry_and_then_succeed() {
    let fetcher = MockFetcher::with_plan(vec![(
        500,
        vec![
            Err(FetchError::new(FetchErrorKind::RateLimited, "429")),
            Err(FetchError::new(FetchErrorKind::Network, "timeout")),
            Ok(FetchItemResponse::Found(sample_item(500, false, None))),
        ],
    )]);
    let fetcher = Arc::new(fetcher);
    let persister = MockPersister::default();
    let worker = SegmentIngestExecutor::new(fetcher.clone(), persister, test_worker_config(5, 10));

    let result = worker
        .process_segment_once(&SegmentWork {
            segment_id: 2,
            start_id: 500,
            end_id: 500,
            resume_cursor_id: None,
        })
        .await;

    assert_eq!(result.status, SegmentAttemptStatus::Completed);
    assert_eq!(fetcher.calls_for(500), 3);
}

#[tokio::test]
async fn retryable_fetch_exhaustion_returns_retryable_failure() {
    let fetcher = MockFetcher::with_plan(vec![(
        900,
        vec![
            Err(FetchError::new(FetchErrorKind::Network, "timeout-1")),
            Err(FetchError::new(FetchErrorKind::Network, "timeout-2")),
            Err(FetchError::new(FetchErrorKind::Network, "timeout-3")),
        ],
    )]);
    let worker =
        SegmentIngestExecutor::new(fetcher, MockPersister::default(), test_worker_config(3, 5));

    let result = worker
        .process_segment_once(&SegmentWork {
            segment_id: 3,
            start_id: 900,
            end_id: 902,
            resume_cursor_id: None,
        })
        .await;

    assert_eq!(result.status, SegmentAttemptStatus::RetryableFailure);
    assert_eq!(result.last_durable_id, None);
    assert_eq!(result.unresolved_count, 3);
    let failure = result.failure.expect("expected retryable failure");
    assert_eq!(failure.item_id, 900);
    assert_eq!(failure.kind, ItemOutcomeKind::RetryableFailure);
    assert_eq!(failure.attempts, 3);
    assert_eq!(failure.failure_class.as_deref(), Some("network_transient"));
}

#[tokio::test]
async fn fatal_fetch_error_does_not_retry() {
    let fetcher = MockFetcher::with_plan(vec![(
        1200,
        vec![Err(FetchError::new(
            FetchErrorKind::Unauthorized,
            "401 unauthorized",
        ))],
    )]);
    let fetcher = Arc::new(fetcher);
    let worker = SegmentIngestExecutor::new(
        fetcher.clone(),
        MockPersister::default(),
        test_worker_config(6, 5),
    );

    let result = worker
        .process_segment_once(&SegmentWork {
            segment_id: 4,
            start_id: 1200,
            end_id: 1200,
            resume_cursor_id: None,
        })
        .await;

    assert_eq!(result.status, SegmentAttemptStatus::FatalFailure);
    assert_eq!(fetcher.calls_for(1200), 1);
    let failure = result.failure.expect("expected fatal failure");
    assert_eq!(failure.kind, ItemOutcomeKind::FatalFailure);
    assert_eq!(failure.failure_class.as_deref(), Some("http_4xx"));
}

#[tokio::test]
async fn retryable_persistence_error_retries_and_recovers() {
    let fetcher = MockFetcher::with_plan(vec![
        (
            700,
            vec![Ok(FetchItemResponse::Found(sample_item(700, false, None)))],
        ),
        (
            701,
            vec![Ok(FetchItemResponse::Found(sample_item(701, false, None)))],
        ),
    ]);
    let persister = Arc::new(MockPersister::with_outcomes(vec![
        Err(PersistError::retryable("temporary DB connectivity")),
        Ok(()),
    ]));

    let worker = SegmentIngestExecutor::new(fetcher, persister.clone(), test_worker_config(3, 2));

    let result = worker
        .process_segment_once(&SegmentWork {
            segment_id: 5,
            start_id: 700,
            end_id: 701,
            resume_cursor_id: None,
        })
        .await;

    assert_eq!(result.status, SegmentAttemptStatus::Completed);
    assert_eq!(persister.calls(), 2);
    assert_eq!(persister.persisted_ids(), vec![700, 701]);
}

#[tokio::test]
async fn fatal_persistence_error_reports_fatal_failure() {
    let fetcher = MockFetcher::with_plan(vec![(
        800,
        vec![Ok(FetchItemResponse::Found(sample_item(800, false, None)))],
    )]);
    let persister = MockPersister::with_outcomes(vec![Err(PersistError::fatal(
        "column \"parts\" does not exist",
    ))]);

    let worker = SegmentIngestExecutor::new(fetcher, persister, test_worker_config(2, 10));

    let result = worker
        .process_segment_once(&SegmentWork {
            segment_id: 6,
            start_id: 800,
            end_id: 800,
            resume_cursor_id: None,
        })
        .await;

    assert_eq!(result.status, SegmentAttemptStatus::FatalFailure);
    assert_eq!(result.unresolved_count, 1);
    assert_eq!(result.last_durable_id, None);
    let failure = result.failure.expect("expected fatal persistence failure");
    assert_eq!(failure.item_id, 800);
    assert_eq!(failure.kind, ItemOutcomeKind::FatalFailure);
    assert_eq!(failure.failure_class.as_deref(), Some("schema"));
    assert!(failure
        .message
        .expect("expected error message")
        .contains("column \"parts\" does not exist"));
}
