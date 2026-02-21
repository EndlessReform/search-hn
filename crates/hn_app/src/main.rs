mod item_page;

use std::env;
use std::net::SocketAddr;
use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::Html;
use axum::routing::get;
use axum::{Json, Router};
use clap::Parser;
use hn_core::db::build_db_pool;
use hn_core::db::story_tree::{
    retrieve_story_comments_as_tree_async_pg, BackendFailureClass, RetrieveStoryTreeError,
    StoryCommentTree, StoryTreeOptions,
};
use serde::{Deserialize, Serialize};
use tracing::{error, info};

const DEFAULT_PORT: u16 = 3001;
const DB_POOL_MAX_SIZE: usize = 16;
const STORY_TREE_ROUTE: &str = "/api/stories/{story_id}/tree";
const ITEM_ROUTE: &str = "/item";
const ITEM_THREAD_ROUTE: &str = "/item/thread";

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
}

#[derive(Debug, Deserialize)]
struct ItemQuery {
    id: i64,
}

#[derive(Debug, Serialize)]
struct ErrorResponse {
    error: String,
}

enum StoryTreeRequestError {
    PoolUnavailable,
    Retrieval(RetrieveStoryTreeError),
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
    let state = Arc::new(AppState { pool });

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
        .route("/health", get(health_handler))
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
            .route(ITEM_ROUTE, get(dummy_item_route))
            .route(ITEM_THREAD_ROUTE, get(dummy_item_route));
        assert_eq!(ITEM_ROUTE, "/item");
        assert_eq!(ITEM_THREAD_ROUTE, "/item/thread");
    }
}
