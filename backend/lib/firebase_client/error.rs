use crate::queue::QueueError;
use std::fmt::{self};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum FirebaseClientError {
    ConnectError(String),
    ParseError(String),
    JsonParseError(#[from] serde_json::Error), // Added for JSON parsing errors
    RequestError(#[from] reqwest::Error),
    QueueError(#[from] QueueError),
}

impl fmt::Display for FirebaseClientError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            FirebaseClientError::ConnectError(e) => write!(f, "ConnectError: {}", e),
            FirebaseClientError::ParseError(e) => write!(f, "ParseError: {}", e),
            FirebaseClientError::JsonParseError(e) => write!(f, "ParseError: {}", e),
            FirebaseClientError::RequestError(e) => write!(f, "RequestError: {}", e),
            FirebaseClientError::QueueError(e) => write!(f, "QueueError: {}", e),
        }
    }
}
