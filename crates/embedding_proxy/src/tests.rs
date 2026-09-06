//! HTTP-level checks with a controllable upstream: failures must release admission.
use super::*;
use axum::{
    body::{Body, to_bytes},
    http::{Request as HttpRequest, header},
};
use clap::Parser;
use tower::ServiceExt;

fn config(backend: String) -> Config {
    Config {
        backend,
        timeout_seconds: 1,
        interactive_limit: 1,
        ..Config::parse_from(["test"])
    }
}

#[tokio::test]
async fn client_docs_follow_caddy_origin_without_configuration() {
    let router = app(config("http://127.0.0.1:1".into())).unwrap();
    for (scheme, host) in [
        ("https", "inference.example"),
        ("https", "other.example:8443"),
    ] {
        for path in ["/usage.md", "/llms.txt"] {
            let response = router
                .clone()
                .oneshot(
                    HttpRequest::builder()
                        .uri(path)
                        .header("host", "127.0.0.1:8081")
                        .header("x-forwarded-proto", scheme)
                        .header("x-forwarded-host", host)
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            assert_eq!(
                response.headers()[header::CONTENT_TYPE],
                "text/markdown; charset=utf-8"
            );
            assert_eq!(response.headers()[header::CACHE_CONTROL], "no-store");
            let text = String::from_utf8(
                to_bytes(response.into_body(), 20000)
                    .await
                    .unwrap()
                    .to_vec(),
            )
            .unwrap();
            assert!(!text.contains("{{PUBLIC_BASE_URL}}"));
            assert!(!text.contains("127.0.0.1"));
            if path == "/usage.md" {
                assert!(text.contains(&format!("{scheme}://{host}/embeddings/v1")));
                assert!(text.contains(&format!("{scheme}://{host}/vllm/embeddings/v1/embeddings")));
            } else {
                assert!(text.contains(&format!("{scheme}://{host}/embeddings/usage.md")));
            }
        }
    }
}

#[tokio::test]
async fn client_docs_reject_malformed_origins_and_allow_local_inspection() {
    let router = app(config("http://127.0.0.1:1".into())).unwrap();
    for (scheme, host) in [
        ("ftp", "inference.example"),
        ("https", "user@inference.example"),
        ("https", "inference.example/path"),
        ("https", "inference.example?x=1"),
        ("https", "inference.example#x"),
        ("https", "$(whoami).example"),
        ("https", "inference.example,other.example"),
    ] {
        let response = router
            .clone()
            .oneshot(
                HttpRequest::builder()
                    .uri("/usage.md")
                    .header("x-forwarded-proto", scheme)
                    .header("x-forwarded-host", host)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }
    let response = router
        .oneshot(
            HttpRequest::builder()
                .uri("/llms.txt")
                .header("host", "localhost:8081")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let text = String::from_utf8(
        to_bytes(response.into_body(), 20000)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap();
    assert!(text.contains("http://localhost:8081/embeddings/usage.md"));
}

fn request(workload: &str, body: serde_json::Value) -> HttpRequest<Body> {
    HttpRequest::builder()
        .method("POST")
        .uri("/v1/embeddings")
        .header("content-type", "application/json")
        .header("x-embedding-workload", workload)
        .body(Body::from(body.to_string()))
        .unwrap()
}
fn valid() -> serde_json::Value {
    json!({"model":MODEL,"input":["hello"]})
}

async fn backend(router: Router) -> (String, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let handle = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    (format!("http://{addr}"), handle)
}

#[tokio::test]
async fn validates_before_inference() {
    let router = app(config("http://127.0.0.1:1".into())).unwrap();
    for (workload, body) in [
        ("wrong", valid()),
        ("bulk", json!({"model":"wrong","input":"hi"})),
        ("bulk", json!({"model":MODEL,"input":[]})),
        ("bulk", json!({"model":MODEL,"input":" "})),
        (
            "bulk",
            json!({"model":MODEL,"input":"hi","encoding_format":"base64"}),
        ),
        ("bulk", json!({"model":MODEL,"input":"hi","priority":-1})),
    ] {
        assert!(
            router
                .clone()
                .oneshot(request(workload, body))
                .await
                .unwrap()
                .status()
                .is_client_error()
        );
    }
}

#[tokio::test]
async fn forwards_workload_priority_and_returns_recipe() {
    let upstream = Router::new().route("/v1/embeddings", post(|Json(value): Json<serde_json::Value>| async move {
        assert_eq!(value["priority"], 1);
        Json(json!({"model":MODEL,"data":[{"index":0,"embedding":vec![1.;1024]}],"usage":{"prompt_tokens":1}}))
    }));
    let (url, server) = backend(upstream).await;
    let response = app(config(url))
        .unwrap()
        .oneshot(request("bulk", valid()))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["x-embedding-recipe"], RECIPE);
    let body: contract::Response =
        serde_json::from_slice(&to_bytes(response.into_body(), 100000).await.unwrap()).unwrap();
    assert_eq!(body.data[0].embedding, vec![97; 1024]);
    server.abort();
}

#[tokio::test]
async fn bulk_overload_reserves_interactive_capacity_and_timeout_releases_slot() {
    let started = Arc::new(tokio::sync::Notify::new());
    let notify = started.clone();
    let upstream = Router::new().route(
        "/v1/embeddings",
        post(move || {
            let notify = notify.clone();
            async move {
                notify.notify_one();
                tokio::time::sleep(Duration::from_secs(5)).await;
                "{}"
            }
        }),
    );
    let (url, server) = backend(upstream).await;
    let router = app(config(url)).unwrap();
    let task = tokio::spawn(router.clone().oneshot(request("bulk", valid())));
    started.notified().await;
    let overloaded = router
        .clone()
        .oneshot(request("bulk", valid()))
        .await
        .unwrap();
    assert_eq!(overloaded.status(), StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(overloaded.headers()["retry-after"], "1");
    let interactive = tokio::spawn(router.clone().oneshot(request("interactive", valid())));
    started.notified().await; // Interactive reached upstream while the bulk slot was occupied.
    assert_eq!(
        task.await.unwrap().unwrap().status(),
        StatusCode::GATEWAY_TIMEOUT
    );
    assert_eq!(
        interactive.await.unwrap().unwrap().status(),
        StatusCode::GATEWAY_TIMEOUT
    );
    let retry = router.oneshot(request("bulk", valid())).await.unwrap();
    assert_eq!(retry.status(), StatusCode::GATEWAY_TIMEOUT); // Not stuck at 429.
    server.abort();
}

#[tokio::test]
async fn cancelled_handler_releases_bulk_permit() {
    let started = Arc::new(tokio::sync::Notify::new());
    let notify = started.clone();
    let upstream = Router::new().route(
        "/v1/embeddings",
        post(move || {
            let notify = notify.clone();
            async move {
                notify.notify_one();
                tokio::time::sleep(Duration::from_secs(5)).await;
                "{}"
            }
        }),
    );
    let (url, server) = backend(upstream).await;
    let router = app(config(url)).unwrap();
    let task = tokio::spawn(router.clone().oneshot(request("bulk", valid())));
    started.notified().await;
    task.abort();
    let _ = task.await;
    let retry = tokio::spawn(router.oneshot(request("bulk", valid())));
    // A real cancelled handler must release admission before its upstream deadline.
    tokio::time::timeout(Duration::from_millis(500), started.notified())
        .await
        .unwrap();
    retry.abort();
    server.abort();
}

#[tokio::test]
async fn backend_failure_never_returns_raw_floats_or_leaks_permits() {
    let router = app(config("http://127.0.0.1:1".into())).unwrap();
    for _ in 0..2 {
        let response = router
            .clone()
            .oneshot(request("bulk", valid()))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    }
}
