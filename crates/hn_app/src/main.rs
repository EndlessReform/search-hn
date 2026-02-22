mod assets;
mod home_page;
mod item_page;

use std::collections::HashMap;
use std::env;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::RwLock;
use std::time::{Duration, Instant};

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::Html;
use axum::routing::get;
use axum::{Json, Router};
use clap::Parser;
use diesel::sql_types::{Array, BigInt, Nullable, Text};
use diesel::{sql_query, QueryableByName};
use diesel_async::RunQueryDsl;
use hn_core::db::build_db_pool;
use hn_core::db::story_tree::{
    retrieve_story_comments_as_tree_async_pg, BackendFailureClass, RetrieveStoryTreeError,
    StoryCommentTree, StoryTreeOptions,
};
use serde::{Deserialize, Serialize};
use tracing::{error, info};

const DEFAULT_PORT: u16 = 3001;
const DB_POOL_MAX_SIZE: usize = 16;
const HOME_ROUTE: &str = "/";
const STORY_TREE_ROUTE: &str = "/api/stories/{story_id}/tree";
const ITEM_ROUTE: &str = "/item";
const ITEM_THREAD_ROUTE: &str = "/item/thread";
const HOME_PAGE_GRAVITY: f64 = 1.8;
const HOME_PAGE_CACHE_TTL_SECONDS: u64 = 30;
/// Recent global item-ID window used to bound homepage ranking work.
///
/// Calibrated from story-only checks against the mirror: `75k` IDs covered roughly
/// ~5.5 days of stories and `100k` IDs covered ~9 days on 2026-02-22, so `90k`
/// provides about a week plus margin while keeping the candidate set bounded.
const HOME_PAGE_MAX_CANDIDATE_ITEM_WINDOW: i64 = 90_000;

#[derive(Parser, Debug)]
#[command(about = "Read-oriented API for HN story/thread retrieval")]
struct Cli {
    #[arg(long, default_value_t = DEFAULT_PORT)]
    /// HTTP port for the JSON API server.
    port: u16,
}

struct Config {
    db_url: String,
}

impl Config {
    fn from_env() -> Result<Self, env::VarError> {
        let db_url = env::var("DATABASE_URL")?;
        Ok(Self { db_url })
    }
}

#[derive(Clone)]
struct AppState {
    pool: diesel_async::pooled_connection::deadpool::Pool<diesel_async::AsyncPgConnection>,
    home_page_cache: Arc<RwLock<HomePageCache>>,
}

#[derive(Debug, Deserialize)]
struct ItemQuery {
    id: i64,
}

#[derive(Debug, Deserialize, Default)]
struct HomePageQuery {
    p: Option<usize>,
}

#[derive(Debug, Serialize)]
struct ErrorResponse {
    error: String,
}

#[derive(Debug, QueryableByName)]
struct HomePageStoryRow {
    #[diesel(sql_type = BigInt)]
    id: i64,
    #[diesel(sql_type = Nullable<Text>)]
    title: Option<String>,
    #[diesel(sql_type = Nullable<Text>)]
    url: Option<String>,
    #[diesel(sql_type = Nullable<Text>)]
    domain: Option<String>,
    #[diesel(sql_type = Nullable<Text>)]
    by: Option<String>,
    #[diesel(sql_type = Nullable<BigInt>)]
    time: Option<i64>,
    #[diesel(sql_type = Nullable<BigInt>)]
    score: Option<i64>,
    #[diesel(sql_type = Nullable<BigInt>)]
    descendants: Option<i64>,
}

#[derive(Debug, QueryableByName)]
struct HomePageStoryCandidateRow {
    #[diesel(sql_type = BigInt)]
    id: i64,
    #[diesel(sql_type = BigInt)]
    time: i64,
    #[diesel(sql_type = Nullable<BigInt>)]
    score: Option<i64>,
}

#[derive(Debug, QueryableByName)]
struct IdRow {
    #[diesel(sql_type = BigInt)]
    id: i64,
}

/// In-memory ranked story candidate used between the two homepage queries.
///
/// Query 1 returns only the fields needed to compute ranking; we sort these candidates
/// in Rust and then fetch display fields for the selected page IDs in query 2.
#[derive(Debug, Clone)]
struct RankedHomePageCandidate {
    id: i64,
    time: i64,
    rank_score: f64,
}

/// In-memory cache entry for one rendered homepage page.
///
/// We cache rendered HTML rather than row structs so repeated requests avoid both the DB
/// query and string rendering work.
#[derive(Debug, Clone)]
struct HomePageCacheEntry {
    rendered_html: String,
    cached_at: Instant,
}

/// Very small process-local cache keyed by page number.
///
/// This is intentionally simple for the MVP: best-effort, no persistence, and safe to miss.
#[derive(Debug, Default)]
struct HomePageCache {
    pages: HashMap<usize, HomePageCacheEntry>,
}

enum StoryTreeRequestError {
    PoolUnavailable,
    Retrieval(RetrieveStoryTreeError),
}

enum HomePageRequestError {
    PoolUnavailable,
    QueryFailed,
}

#[tokio::main]
async fn main() {
    dotenv::dotenv().ok();
    init_logging();

    let cli = Cli::parse();
    let config =
        Config::from_env().expect("Config incorrectly specified: DATABASE_URL is required");

    let pool = build_db_pool(&config.db_url, DB_POOL_MAX_SIZE)
        .await
        .expect("could not initialize DB pool");
    let state = Arc::new(AppState {
        pool,
        home_page_cache: Arc::new(RwLock::new(HomePageCache::default())),
    });

    let app = build_router(state);

    let addr = SocketAddr::from(([0, 0, 0, 0], cli.port));
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .expect("failed to bind API server port");
    info!(event = "hn_app_listening", %addr, "HN app server listening");

    axum::serve(listener, app)
        .await
        .expect("HN app server failed");
}

fn init_logging() {
    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| "hn_app=info,info".into());
    tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_target(true)
        .compact()
        .init();
}

async fn health_handler() -> &'static str {
    "ok"
}

/// Serves the `/` homepage using a simple Hacker News-style hotness score.
///
/// Ranking formula (MVP):
/// `score = (P - 1) / (T + 2)^G`, with `G = 1.8`
/// where `P` is points and `T` is age in hours.
async fn get_home_page(
    State(state): State<Arc<AppState>>,
    Query(query): Query<HomePageQuery>,
) -> Result<Html<String>, (StatusCode, Html<String>)> {
    let page_number = query.p.unwrap_or(1).max(1);

    if let Some(cached_html) = read_cached_home_page(&state, page_number) {
        return Ok(Html(cached_html));
    }

    fetch_home_page_stories(&state, page_number)
        .await
        .map(|(stories, has_more)| {
            let rendered = home_page::render_home_page(&home_page::HomePageView {
                page_number,
                has_more,
                stories,
            });
            write_cached_home_page(&state, page_number, rendered.clone());
            Html(rendered)
        })
        .map_err(|err| {
            let (status, message) = map_home_page_error(err);
            (
                status,
                Html(home_page::render_home_page_error(page_number, &message)),
            )
        })
}

async fn get_story_tree(
    State(state): State<Arc<AppState>>,
    Path(story_id): Path<i64>,
) -> Result<Json<StoryCommentTree>, (StatusCode, Json<ErrorResponse>)> {
    fetch_story_tree(&state, story_id)
        .await
        .map(Json)
        .map_err(map_story_tree_json_error)
}

async fn get_item_page(Query(query): Query<ItemQuery>) -> Html<String> {
    Html(item_page::render_item_page_shell(query.id))
}

async fn get_item_thread(
    State(state): State<Arc<AppState>>,
    Query(query): Query<ItemQuery>,
) -> Result<Html<String>, (StatusCode, Html<String>)> {
    fetch_story_tree(&state, query.id)
        .await
        .map(|tree| Html(item_page::render_story_thread_fragment(&tree)))
        .map_err(|err| {
            let (status, message) = map_story_tree_html_error(err);
            (
                status,
                Html(item_page::render_thread_error_fragment(query.id, &message)),
            )
        })
}

async fn fetch_story_tree(
    state: &Arc<AppState>,
    story_id: i64,
) -> Result<StoryCommentTree, StoryTreeRequestError> {
    let mut conn = state.pool.get().await.map_err(|err| {
        error!(
            event = "hn_app_pool_get_failed",
            error = %err,
            "failed to fetch database connection from pool"
        );
        StoryTreeRequestError::PoolUnavailable
    })?;

    retrieve_story_comments_as_tree_async_pg(&mut conn, story_id, StoryTreeOptions::default())
        .await
        .map_err(|err| {
            if let RetrieveStoryTreeError::Backend(backend) = &err {
                error!(
                    event = "hn_app_story_tree_backend_error",
                    dependency = ?backend.dependency,
                    class = ?backend.class,
                    message = %backend.message,
                    story_id,
                    "story tree retrieval failed"
                );
            }
            StoryTreeRequestError::Retrieval(err)
        })
}

/// Loads one homepage page from Postgres and computes whether another page exists.
///
/// Strategy:
/// 1. Read `MAX(id)` and derive a bounded recent-ID cutoff.
/// 2. Fetch only candidate fields (`id`, `time`, `score`) for homepage-eligible stories.
/// 3. Rank and paginate in Rust.
/// 4. Fetch display fields for only the selected page IDs.
///
/// This keeps the expensive ranking math and ordering in-process while allowing Postgres
/// to focus on indexed filtering and small point lookups.
async fn fetch_home_page_stories(
    state: &Arc<AppState>,
    page_number: usize,
) -> Result<(Vec<home_page::HomePageStory>, bool), HomePageRequestError> {
    let offset_rows = page_number
        .saturating_sub(1)
        .saturating_mul(home_page::PAGE_SIZE);

    let mut conn = state.pool.get().await.map_err(|err| {
        error!(
            event = "hn_app_pool_get_failed",
            error = %err,
            "failed to fetch database connection from pool"
        );
        HomePageRequestError::PoolUnavailable
    })?;

    let mut max_id_rows = sql_query("SELECT COALESCE(MAX(id), 0) AS id FROM items")
        .load::<IdRow>(&mut conn)
        .await
        .map_err(|err| {
            error!(
                event = "hn_app_home_page_max_id_query_failed",
                error = %err,
                "homepage max(id) query failed"
            );
            HomePageRequestError::QueryFailed
        })?;
    let max_id = max_id_rows.pop().map_or(0, |row| row.id);
    let min_candidate_id = max_id.saturating_sub(HOME_PAGE_MAX_CANDIDATE_ITEM_WINDOW);

    let candidate_rows = fetch_home_page_story_candidates(&mut conn, min_candidate_id)
        .await
        .map_err(|err| {
            error!(
                event = "hn_app_home_page_candidate_query_failed",
                error = %err,
                page_number,
                max_id,
                min_candidate_id,
                "homepage candidate query failed"
            );
            HomePageRequestError::QueryFailed
        })?;

    let now_epoch_seconds = unix_now_seconds();
    let mut ranked_candidates = candidate_rows
        .into_iter()
        .map(|row| RankedHomePageCandidate {
            id: row.id,
            time: row.time,
            rank_score: compute_home_page_rank_score(row.score, row.time, now_epoch_seconds),
        })
        .collect::<Vec<_>>();
    sort_ranked_home_page_candidates(&mut ranked_candidates);

    let page_end_exclusive = offset_rows.saturating_add(home_page::PAGE_SIZE);
    let has_more = ranked_candidates.len() > page_end_exclusive;

    let page_ids = ranked_candidates
        .iter()
        .skip(offset_rows)
        .take(home_page::PAGE_SIZE)
        .map(|candidate| candidate.id)
        .collect::<Vec<_>>();

    if page_ids.is_empty() {
        return Ok((Vec::new(), false));
    }

    let rows = fetch_home_page_story_details_for_ids(&mut conn, &page_ids)
        .await
        .map_err(|err| {
            error!(
                event = "hn_app_home_page_detail_query_failed",
                error = %err,
                page_number,
                max_id,
                min_candidate_id,
                ids_requested = page_ids.len(),
                "homepage detail query failed"
            );
            HomePageRequestError::QueryFailed
        })?;

    let stories = rows
        .into_iter()
        .map(home_page_story_from_row)
        .collect::<Vec<_>>();

    Ok((stories, has_more))
}

/// Loads the narrow candidate set used for homepage ranking.
///
/// This query is intentionally constrained to the fields needed for the ranking formula so a
/// dedicated partial covering index can satisfy it efficiently.
async fn fetch_home_page_story_candidates(
    conn: &mut diesel_async::AsyncPgConnection,
    min_candidate_id: i64,
) -> diesel::QueryResult<Vec<HomePageStoryCandidateRow>> {
    sql_query(
        r#"
        SELECT id, time, score
        FROM items
        WHERE id >= $1
          AND type = 'story'
          AND COALESCE(deleted, FALSE) = FALSE
          AND COALESCE(dead, FALSE) = FALSE
          AND title IS NOT NULL
          AND time IS NOT NULL
        ORDER BY id DESC
        "#,
    )
    .bind::<BigInt, _>(min_candidate_id)
    .load::<HomePageStoryCandidateRow>(conn)
    .await
}

/// Fetches display fields for the selected page IDs while preserving input order.
///
/// `UNNEST(..) WITH ORDINALITY` lets us bind a single `BIGINT[]` parameter and then return
/// rows in the exact ranked order computed in Rust.
async fn fetch_home_page_story_details_for_ids(
    conn: &mut diesel_async::AsyncPgConnection,
    story_ids: &[i64],
) -> diesel::QueryResult<Vec<HomePageStoryRow>> {
    debug_assert!(
        !story_ids.is_empty(),
        "detail query should not run with empty id list"
    );

    sql_query(
        r#"
        WITH requested_ids AS (
            SELECT req_id, ord
            FROM UNNEST($1::BIGINT[]) WITH ORDINALITY AS t(req_id, ord)
        )
        SELECT
            i.id,
            i.title,
            i.url,
            i.domain,
            i.by,
            i.time,
            i.score,
            i.descendants
        FROM requested_ids r
        JOIN items i ON i.id = r.req_id
        ORDER BY r.ord
        "#,
    )
    .bind::<Array<BigInt>, _>(story_ids.to_vec())
    .load::<HomePageStoryRow>(conn)
    .await
}

/// Converts a DB row into the homepage renderer's story view model.
fn home_page_story_from_row(row: HomePageStoryRow) -> home_page::HomePageStory {
    home_page::HomePageStory {
        id: row.id,
        title: row.title.unwrap_or_else(|| "Untitled story".to_string()),
        url: row.url,
        domain: row.domain,
        by: row.by,
        time: row.time,
        score: row.score,
        descendants: row.descendants,
    }
}

/// Computes the homepage hotness score used for Rust-side ranking.
///
/// Formula: `(P - 1) / (T + 2)^G`
/// - `P`: points (`score`, coerced to non-negative)
/// - `T`: age in hours from the current request time
/// - `G`: gravity constant (`HOME_PAGE_GRAVITY`)
fn compute_home_page_rank_score(
    score: Option<i64>,
    story_time_epoch: i64,
    now_epoch_seconds: i64,
) -> f64 {
    let points = score.unwrap_or(0).max(0) as f64;
    let age_seconds = (now_epoch_seconds - story_time_epoch).max(0) as f64;
    let age_hours = age_seconds / 3600.0;
    (points - 1.0).max(0.0) / (age_hours + 2.0).powf(HOME_PAGE_GRAVITY)
}

/// Applies a deterministic ordering for homepage candidates.
///
/// Tie-breaks mirror the SQL implementation so page ordering stays stable:
/// 1. Higher rank score
/// 2. Newer `time`
/// 3. Larger `id`
fn sort_ranked_home_page_candidates(candidates: &mut [RankedHomePageCandidate]) {
    candidates.sort_by(|left, right| {
        right
            .rank_score
            .partial_cmp(&left.rank_score)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| right.time.cmp(&left.time))
            .then_with(|| right.id.cmp(&left.id))
    });
}

fn unix_now_seconds() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time should be after unix epoch")
        .as_secs() as i64
}

/// Returns a fresh cached homepage HTML response, if available.
///
/// Cache lock failures (for example, poisoning after a panic) degrade gracefully to a miss.
fn read_cached_home_page(state: &Arc<AppState>, page_number: usize) -> Option<String> {
    let ttl = Duration::from_secs(HOME_PAGE_CACHE_TTL_SECONDS);
    let now = Instant::now();
    let cache = state.home_page_cache.read().ok()?;
    let entry = cache.pages.get(&page_number)?;
    if now.duration_since(entry.cached_at) > ttl {
        return None;
    }
    Some(entry.rendered_html.clone())
}

/// Stores a rendered homepage page in the best-effort in-memory cache.
///
/// We opportunistically prune expired entries on writes so stale pages do not accumulate.
fn write_cached_home_page(state: &Arc<AppState>, page_number: usize, rendered_html: String) {
    let ttl = Duration::from_secs(HOME_PAGE_CACHE_TTL_SECONDS);
    let now = Instant::now();
    let Ok(mut cache) = state.home_page_cache.write() else {
        return;
    };

    cache
        .pages
        .retain(|_, entry| now.duration_since(entry.cached_at) <= ttl);
    cache.pages.insert(
        page_number,
        HomePageCacheEntry {
            rendered_html,
            cached_at: now,
        },
    );
}

fn map_home_page_error(err: HomePageRequestError) -> (StatusCode, String) {
    match err {
        HomePageRequestError::PoolUnavailable => (
            StatusCode::SERVICE_UNAVAILABLE,
            "database connection unavailable".to_string(),
        ),
        HomePageRequestError::QueryFailed => (
            StatusCode::INTERNAL_SERVER_ERROR,
            "homepage query failed".to_string(),
        ),
    }
}

fn map_story_tree_json_error(err: StoryTreeRequestError) -> (StatusCode, Json<ErrorResponse>) {
    let (status, message) = map_story_tree_html_error(err);
    (status, Json(ErrorResponse { error: message }))
}

fn map_story_tree_html_error(err: StoryTreeRequestError) -> (StatusCode, String) {
    match err {
        StoryTreeRequestError::PoolUnavailable => (
            StatusCode::SERVICE_UNAVAILABLE,
            "database connection unavailable".to_string(),
        ),
        StoryTreeRequestError::Retrieval(retrieval) => map_retrieve_story_tree_error(retrieval),
    }
}

fn map_retrieve_story_tree_error(err: RetrieveStoryTreeError) -> (StatusCode, String) {
    match err {
        RetrieveStoryTreeError::StoryNotFound { story_id } => (
            StatusCode::NOT_FOUND,
            format!("story {story_id} was not found"),
        ),
        RetrieveStoryTreeError::NotAStory {
            requested_story_id,
            actual_type,
        } => (
            StatusCode::NOT_FOUND,
            format!(
                "item {requested_story_id} is not a story (type: {})",
                actual_type.as_deref().unwrap_or("<null>")
            ),
        ),
        RetrieveStoryTreeError::Backend(backend) => {
            let status = match backend.class {
                BackendFailureClass::Transient => StatusCode::SERVICE_UNAVAILABLE,
                BackendFailureClass::Permanent => StatusCode::INTERNAL_SERVER_ERROR,
            };
            (status, backend.message)
        }
    }
}

fn build_router(state: Arc<AppState>) -> Router {
    Router::new()
        .route(HOME_ROUTE, get(get_home_page))
        .route("/health", get(health_handler))
        .route(assets::HTMX_ASSET_ROUTE, get(assets::serve_htmx_min_js))
        .route(ITEM_ROUTE, get(get_item_page))
        .route(ITEM_THREAD_ROUTE, get(get_item_thread))
        .route(STORY_TREE_ROUTE, get(get_story_tree))
        .with_state(state)
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn dummy_story_route(Path(_story_id): Path<i64>) -> &'static str {
        "ok"
    }

    async fn dummy_item_route() -> &'static str {
        "ok"
    }

    async fn dummy_home_route() -> &'static str {
        "ok"
    }

    #[test]
    fn story_tree_route_uses_axum_v08_syntax() {
        let _router: Router<()> = Router::new().route(STORY_TREE_ROUTE, get(dummy_story_route));
        assert!(
            !STORY_TREE_ROUTE.contains(':'),
            "route should use Axum v0.8 capture syntax"
        );
        assert!(
            STORY_TREE_ROUTE.contains('{') && STORY_TREE_ROUTE.contains('}'),
            "route should include a named path capture"
        );
    }

    #[test]
    fn item_routes_are_registered() {
        let _router: Router<()> = Router::new()
            .route(HOME_ROUTE, get(dummy_home_route))
            .route(ITEM_ROUTE, get(dummy_item_route))
            .route(ITEM_THREAD_ROUTE, get(dummy_item_route));
        assert_eq!(HOME_ROUTE, "/");
        assert_eq!(ITEM_ROUTE, "/item");
        assert_eq!(ITEM_THREAD_ROUTE, "/item/thread");
    }
}
