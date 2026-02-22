use super::super::types::{FetchItemResponse, ItemOutcomeKind};
use super::test_support::{test_worker_config, MockFetcher, MockPersister};
use super::RealtimeIngestExecutor;

#[tokio::test]
async fn realtime_item_missing_is_reported_as_terminal() {
    let fetcher = MockFetcher::with_plan(vec![(1500, vec![Ok(FetchItemResponse::Missing)])]);
    let worker =
        RealtimeIngestExecutor::new(fetcher, MockPersister::default(), test_worker_config(3, 1));

    let result = worker.process_item_once(1500).await;
    assert_eq!(result.outcome.kind, ItemOutcomeKind::TerminalMissing);
    assert_eq!(result.outcome.item_id, 1500);
}
