use crate::firebase_listener::FirebaseListenerErr;
use crate::segment_manager::SegmentStateError;
use diesel::result::Error as DieselError;
use diesel_async::pooled_connection::deadpool::PoolError;
use thiserror::Error;
use tokio::task::JoinError;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Connection error: {0}")]
    ConnectError(String),

    #[error(transparent)]
    FirebaseError(#[from] FirebaseListenerErr),

    #[error(transparent)]
    DieselError(#[from] DieselError),

    #[error(transparent)]
    DBPoolError(#[from] PoolError),

    #[error("Task join error: {0}")]
    TaskJoinError(#[from] JoinError),

    #[error(transparent)]
    SegmentState(#[from] SegmentStateError),

    #[error("Orchestration error: {0}")]
    Orchestration(String),
}
