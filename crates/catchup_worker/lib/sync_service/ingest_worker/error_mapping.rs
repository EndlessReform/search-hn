use std::error::Error as StdError;

use diesel::result::{DatabaseErrorKind, Error as DieselError};

use crate::firebase_listener::FirebaseListenerErr;

use super::super::types::{
    FetchDiagnostics, FetchError, FetchErrorKind, PersistError, FAILURE_CLASS_DECODE,
    FAILURE_CLASS_HTTP_4XX, FAILURE_CLASS_NETWORK_TRANSIENT, FAILURE_CLASS_SCHEMA,
};

pub fn map_diesel_error(error: DieselError) -> PersistError {
    match error {
        DieselError::DatabaseError(kind, info) => match kind {
            DatabaseErrorKind::SerializationFailure
            | DatabaseErrorKind::ClosedConnection
            | DatabaseErrorKind::UnableToSendCommand => PersistError::retryable(format!(
                "transient database error ({kind:?}): {}",
                info.message()
            )),
            _ => PersistError::fatal(format!(
                "fatal database error ({kind:?}): {}",
                info.message()
            )),
        },
        DieselError::RollbackTransaction => {
            PersistError::retryable("transaction rollback requested by database".to_string())
        }
        other => PersistError::fatal(format!("fatal diesel error: {other}")),
    }
}

pub fn map_firebase_error(error: FirebaseListenerErr) -> FetchError {
    match error {
        FirebaseListenerErr::UnexpectedStatus { resource, status } => {
            map_status_to_fetch_error(resource, status)
        }
        FirebaseListenerErr::RequestError(req_err) => {
            let diagnostics = collect_reqwest_diagnostics(&req_err);
            log_reqwest_diagnostics(&diagnostics);

            if let Some(status) = req_err.status() {
                return map_status_to_fetch_error("item".to_string(), status.as_u16())
                    .with_diagnostics(diagnostics);
            }

            if req_err.is_timeout()
                || req_err.is_connect()
                || req_err.is_request()
                || req_err.is_body()
            {
                return FetchError::new(
                    FetchErrorKind::Network,
                    format!("network/transport error while fetching item: {req_err}"),
                )
                .with_failure_class(FAILURE_CLASS_NETWORK_TRANSIENT)
                .with_diagnostics(diagnostics);
            }

            if req_err.is_decode() {
                return FetchError::new(
                    FetchErrorKind::Network,
                    format!("response decode error while fetching item (retryable): {req_err}"),
                )
                .with_failure_class(FAILURE_CLASS_DECODE)
                .with_diagnostics(diagnostics);
            }

            FetchError::new(FetchErrorKind::MalformedResponse, format!("{req_err:#}"))
                .with_failure_class(FAILURE_CLASS_SCHEMA)
                .with_diagnostics(diagnostics)
        }
        FirebaseListenerErr::JsonParseError(err) => FetchError::new(
            FetchErrorKind::Network,
            format!("invalid JSON payload from Firebase item endpoint (retryable): {err}"),
        )
        .with_failure_class(FAILURE_CLASS_DECODE),
        FirebaseListenerErr::ParseError(message) => FetchError::new(
            FetchErrorKind::MalformedResponse,
            format!("unparseable Firebase payload shape: {message}"),
        )
        .with_failure_class(FAILURE_CLASS_SCHEMA),
        FirebaseListenerErr::ConnectError(message) => FetchError::new(
            FetchErrorKind::Network,
            format!("connection error while fetching item: {message}"),
        )
        .with_failure_class(FAILURE_CLASS_NETWORK_TRANSIENT),
        FirebaseListenerErr::ChannelError(err) => FetchError::new(
            FetchErrorKind::Other,
            format!("internal channel error while fetching item: {err}"),
        )
        .with_failure_class(FAILURE_CLASS_SCHEMA),
    }
}

fn map_status_to_fetch_error(resource: String, status: u16) -> FetchError {
    match status {
        401 => FetchError::new(
            FetchErrorKind::Unauthorized,
            format!("unauthorized while fetching {resource}"),
        )
        .with_failure_class(FAILURE_CLASS_HTTP_4XX),
        403 => FetchError::new(
            FetchErrorKind::Forbidden,
            format!("forbidden while fetching {resource}"),
        )
        .with_failure_class(FAILURE_CLASS_HTTP_4XX),
        404 => FetchError::new(
            FetchErrorKind::Other,
            format!("not found while fetching {resource}"),
        )
        .with_failure_class(FAILURE_CLASS_HTTP_4XX),
        429 => FetchError::new(
            FetchErrorKind::RateLimited,
            format!("rate limited while fetching {resource}"),
        )
        .with_failure_class(FAILURE_CLASS_NETWORK_TRANSIENT),
        400..=499 => FetchError::new(
            FetchErrorKind::Other,
            format!("upstream client error {status} while fetching {resource}"),
        )
        .with_failure_class(FAILURE_CLASS_HTTP_4XX),
        500..=599 => FetchError::new(
            FetchErrorKind::UpstreamUnavailable,
            format!("upstream server error {status} while fetching {resource}"),
        )
        .with_failure_class(FAILURE_CLASS_NETWORK_TRANSIENT),
        _ => FetchError::new(
            FetchErrorKind::Other,
            format!("unexpected HTTP status {status} while fetching {resource}"),
        )
        .with_failure_class(FAILURE_CLASS_SCHEMA),
    }
}

/// Builds a transport-focused diagnostics payload from reqwest internals.
///
/// We persist and log this metadata so dead-letter rows and journal entries contain enough detail
/// to distinguish DNS/TLS/socket churn from true upstream payload issues.
fn collect_reqwest_diagnostics(req_err: &reqwest::Error) -> FetchDiagnostics {
    FetchDiagnostics {
        status: req_err.status().map(|status| status.as_u16()),
        url: req_err.url().map(|url| url.to_string()),
        is_timeout: req_err.is_timeout(),
        is_connect: req_err.is_connect(),
        is_decode: req_err.is_decode(),
        is_body: req_err.is_body(),
        is_request: req_err.is_request(),
        error_chain: render_error_chain(req_err),
        error_debug: format!("{req_err:#?}"),
    }
}

fn render_error_chain(error: &reqwest::Error) -> String {
    let mut parts = vec![error.to_string()];
    let mut source = error.source();
    while let Some(next) = source {
        parts.push(next.to_string());
        source = next.source();
    }
    parts.join(" | caused_by: ")
}

fn log_reqwest_diagnostics(diag: &FetchDiagnostics) {
    tracing::debug!(
        event = "firebase_request_error_diagnostics",
        reqwest_status = ?diag.status,
        reqwest_url = ?diag.url.as_deref(),
        reqwest_is_timeout = diag.is_timeout,
        reqwest_is_connect = diag.is_connect,
        reqwest_is_decode = diag.is_decode,
        reqwest_is_body = diag.is_body,
        reqwest_is_request = diag.is_request,
        reqwest_error_chain = %diag.error_chain,
        reqwest_error_debug = %diag.error_debug,
        "captured reqwest diagnostics while fetching item"
    );
}
