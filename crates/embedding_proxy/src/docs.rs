//! Caddy supplies the external origin through its standard forwarding headers.
//! The proxy is private to the host/Compose network: Caddy is the public entrypoint
//! and replaces client-supplied forwarding headers. No second hostname setting is needed.
use crate::error::ApiError;
use axum::{
    http::{HeaderMap, header},
    response::{IntoResponse, Response},
};

pub(crate) async fn usage(headers: HeaderMap) -> Result<Response, ApiError> {
    render(include_str!("../docs/usage.md"), &headers)
}

pub(crate) async fn llms(headers: HeaderMap) -> Result<Response, ApiError> {
    render(include_str!("../docs/llms.txt"), &headers)
}

/// Render per request so links follow the hostname used to reach Caddy. Local
/// HTTP inspection can use Host instead; the advertised paths still describe Caddy.
/// Validate the origin before inserting it into Markdown and shell examples.
fn render(template: &str, headers: &HeaderMap) -> Result<Response, ApiError> {
    let invalid = || ApiError::bad_input("documentation requires a valid HTTP(S) request origin");
    let (scheme, host) = match (
        headers.get("x-forwarded-proto"),
        headers.get("x-forwarded-host"),
    ) {
        (Some(scheme), Some(host)) => (
            scheme.to_str().map_err(|_| invalid())?,
            host.to_str().map_err(|_| invalid())?,
        ),
        (None, None) => (
            "http",
            headers
                .get(header::HOST)
                .ok_or_else(invalid)?
                .to_str()
                .map_err(|_| invalid())?,
        ),
        _ => return Err(invalid()),
    };
    if !matches!(scheme, "http" | "https")
        || host.is_empty()
        || !host
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || b".-:[]".contains(&c))
    {
        return Err(invalid());
    }
    let url = reqwest::Url::parse(&format!("{scheme}://{host}")).map_err(|_| invalid())?;
    if url.host_str().is_none() {
        return Err(invalid());
    }
    Ok((
        [
            (header::CONTENT_TYPE, "text/markdown; charset=utf-8"),
            (header::CACHE_CONTROL, "no-store"),
        ],
        template.replace("{{PUBLIC_BASE_URL}}", &url.origin().ascii_serialization()),
    )
        .into_response())
}
