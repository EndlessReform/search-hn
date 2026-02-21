use axum::http::header;
use axum::response::IntoResponse;

/// Current vendored HTMX version served by this application.
///
/// Keeping the version and route in one place makes it easy to bump HTMX
/// without chasing hardcoded URLs through templates and router setup.
pub const HTMX_VERSION: &str = "2.0.8";

/// Immutable, versioned path for the vendored HTMX payload.
///
/// A versioned filename allows us to apply long-lived caching safely.
pub const HTMX_ASSET_ROUTE: &str = "/assets/vendor/htmx-2.0.8.min.js";

const HTMX_MIN_JS: &str = include_str!("../assets/vendor/htmx-2.0.8.min.js");

/// Serves vendored HTMX as a local static asset.
///
/// We deliberately set an aggressive immutable cache policy because the route
/// is versioned. A version bump changes the URL, which naturally invalidates
/// existing browser caches.
pub async fn serve_htmx_min_js() -> impl IntoResponse {
    (
        [
            (
                header::CONTENT_TYPE,
                "application/javascript; charset=utf-8",
            ),
            (header::CACHE_CONTROL, "public, max-age=31536000, immutable"),
        ],
        HTMX_MIN_JS,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::response::IntoResponse;

    #[tokio::test]
    async fn htmx_asset_response_sets_immutable_cache_headers() {
        let response = serve_htmx_min_js().await.into_response();
        let headers = response.headers();

        assert_eq!(
            headers
                .get(header::CACHE_CONTROL)
                .and_then(|v| v.to_str().ok()),
            Some("public, max-age=31536000, immutable")
        );
        assert_eq!(
            headers
                .get(header::CONTENT_TYPE)
                .and_then(|v| v.to_str().ok()),
            Some("application/javascript; charset=utf-8")
        );
    }

    #[test]
    fn htmx_asset_route_is_versioned() {
        assert!(
            HTMX_ASSET_ROUTE.contains(HTMX_VERSION),
            "htmx asset route should include the version string for cache busting"
        );
    }
}
