#![allow(dead_code)] // Helpers are shared by independent integration-test executables.
use axum::{
    extract::State,
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::post,
    Json, Router,
};
use catchup_worker_lib::embeddings::EmbeddingArgs;
use diesel::{prelude::*, sql_query, sql_types::BigInt};
use hn_core::{
    db::{build_db_pool, migrations::run_postgres_migrations, story_search::SearchPool},
    embeddings::MODEL,
};
use serde_json::{json, Value};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use tokio::sync::Semaphore;

#[path = "postgres.rs"]
mod postgres;
pub use postgres::TempPostgres;

pub async fn database() -> (TempPostgres, SearchPool) {
    let db = TempPostgres::start();
    let mut conn = PgConnection::establish(&db.database_url()).unwrap();
    run_postgres_migrations(&mut conn);
    let pool = build_db_pool(&db.database_url(), 4).await.unwrap();
    (db, pool)
}

pub fn execute(db: &TempPostgres, sql: &str) {
    use diesel::connection::SimpleConnection;
    PgConnection::establish(&db.database_url())
        .unwrap()
        .batch_execute(sql)
        .unwrap();
}

pub fn count(db: &TempPostgres, sql: &str) -> i64 {
    #[derive(QueryableByName)]
    struct Row {
        #[diesel(sql_type = BigInt)]
        n: i64,
    }
    sql_query(sql)
        .get_result::<Row>(&mut PgConnection::establish(&db.database_url()).unwrap())
        .unwrap()
        .n
}

pub fn insert(db: &TempPostgres, n: i64) {
    execute(db, &format!("INSERT INTO items(id,type,title,url,score,time) SELECT n,'story','database '||n,'https://example.org/'||n,25,1700000000 FROM generate_series(1,{n}) n"));
}

pub struct Mock {
    pub mode: AtomicUsize,
    pub calls: AtomicUsize,
    pub entered: Semaphore,
    pub release: Semaphore,
    recipe: String,
}

/// Modes: 0 success, 1 barrier, 2 outage, 3 poison document, 4 bad recipe,
/// 5 bad coordinates, 6 duplicate response indices, 7 timeout, 8 transient 429.
pub async fn server(recipe: String) -> (String, Arc<Mock>, tokio::task::JoinHandle<()>) {
    let state = Arc::new(Mock {
        mode: AtomicUsize::new(0),
        calls: AtomicUsize::new(0),
        entered: Semaphore::new(0),
        release: Semaphore::new(0),
        recipe,
    });
    let router = Router::new()
        .route("/embeddings", post(embed))
        .with_state(state.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    (url, state, task)
}

async fn embed(
    State(state): State<Arc<Mock>>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Response {
    assert_eq!(headers["X-Embedding-Workload"], "bulk");
    state.calls.fetch_add(1, Ordering::SeqCst);
    let mode = state.mode.load(Ordering::SeqCst);
    if mode == 1 {
        state.entered.add_permits(1);
        state.release.acquire().await.unwrap().forget();
    }
    if mode == 7 {
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    }
    if mode == 2 {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error":{"code":"backend_error","message":"offline"}})),
        )
            .into_response();
    }
    if mode == 8 {
        return (
            StatusCode::TOO_MANY_REQUESTS,
            [("Retry-After", "2")],
            Json(json!({"error":{"code":"overloaded","message":"busy"}})),
        )
            .into_response();
    }
    let inputs = body["input"].as_array().unwrap();
    if mode == 3
        && inputs
            .iter()
            .any(|s| s.as_str().unwrap().contains("poison"))
    {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error":{"code":"backend_error","message":"input exceeds token limit"}})),
        )
            .into_response();
    }
    let recipe = if mode == 4 {
        "different-recipe"
    } else {
        &state.recipe
    };
    let data: Vec<_> = inputs
        .iter()
        .enumerate()
        .rev()
        .map(|(i, input)| {
            let vector = if mode == 9 {
                let mut seed = input
                    .as_str()
                    .unwrap()
                    .bytes()
                    .fold(1u64, |a, b| a.wrapping_mul(31).wrapping_add(u64::from(b)));
                (0..1024)
                    .map(|_| {
                        seed ^= seed << 13;
                        seed ^= seed >> 7;
                        seed ^= seed << 17;
                        (seed % 255) as i32 - 127
                    })
                    .collect()
            } else {
                vec![if mode == 5 { 0 } else { i as i32 + 1 }; 1024]
            };
            json!({"index":if mode==6 {0} else {i},"embedding":vector})
        })
        .collect();
    (
        [("X-Embedding-Recipe", recipe)],
        Json(json!({"model":MODEL,"embedding_recipe":recipe,"data":data})),
    )
        .into_response()
}

pub fn args(url: String) -> EmbeddingArgs {
    EmbeddingArgs {
        enabled: None,
        embedding_base_url: Some(url),
        embedding_batch_size: 4,
        embedding_poll_seconds: 1,
        embedding_retry_seconds: 1,
        embedding_invalid_retry_seconds: 60,
        embedding_timeout_seconds: 1,
    }
}
