use diesel::pg::PgConnection;
use diesel::prelude::*;
use diesel::result::Error as DieselError;
use diesel::sql_query;
use diesel::sql_types::{BigInt, Integer, Nullable, Text};
use diesel::sqlite::SqliteConnection;

use super::types::{
    ExceptionState, IngestException, IngestSegment, SegmentStateError, SegmentStatus,
};

#[doc(hidden)]
#[derive(Debug, QueryableByName)]
#[diesel(check_for_backend(diesel::pg::Pg, diesel::sqlite::Sqlite))]
pub struct SegmentRow {
    #[diesel(sql_type = BigInt)]
    pub segment_id: i64,
    #[diesel(sql_type = BigInt)]
    pub start_id: i64,
    #[diesel(sql_type = BigInt)]
    pub end_id: i64,
    #[diesel(sql_type = Text)]
    pub status: String,
    #[diesel(sql_type = Integer)]
    pub attempts: i32,
    #[diesel(sql_type = Nullable<BigInt>)]
    pub scan_cursor_id: Option<i64>,
    #[diesel(sql_type = Integer)]
    pub unresolved_count: i32,
    #[diesel(sql_type = Nullable<Text>)]
    pub last_error: Option<String>,
}

#[doc(hidden)]
#[derive(Debug, QueryableByName)]
#[diesel(check_for_backend(diesel::pg::Pg, diesel::sqlite::Sqlite))]
pub struct ExceptionRow {
    #[diesel(sql_type = BigInt)]
    pub segment_id: i64,
    #[diesel(sql_type = BigInt)]
    pub item_id: i64,
    #[diesel(sql_type = Text)]
    pub state: String,
    #[diesel(sql_type = Integer)]
    pub attempts: i32,
    #[diesel(sql_type = Nullable<Text>)]
    pub last_error: Option<String>,
}

#[doc(hidden)]
#[derive(Debug, QueryableByName)]
#[diesel(check_for_backend(diesel::pg::Pg, diesel::sqlite::Sqlite))]
pub struct SegmentIdRow {
    #[diesel(sql_type = BigInt)]
    pub segment_id: i64,
}

#[doc(hidden)]
#[derive(Debug, QueryableByName)]
#[diesel(check_for_backend(diesel::pg::Pg, diesel::sqlite::Sqlite))]
pub struct RangeRow {
    #[diesel(sql_type = BigInt)]
    pub start_id: i64,
    #[diesel(sql_type = BigInt)]
    pub end_id: i64,
}

#[doc(hidden)]
pub trait SegmentDb {
    fn execute_sql(&mut self, sql: &str) -> Result<usize, DieselError>;
    fn load_segment_ids(&mut self, sql: &str) -> Result<Vec<SegmentIdRow>, DieselError>;
    fn load_segments(&mut self, sql: &str) -> Result<Vec<SegmentRow>, DieselError>;
    fn load_exceptions(&mut self, sql: &str) -> Result<Vec<ExceptionRow>, DieselError>;
    fn load_ranges(&mut self, sql: &str) -> Result<Vec<RangeRow>, DieselError>;
}

impl SegmentDb for PgConnection {
    fn execute_sql(&mut self, sql: &str) -> Result<usize, DieselError> {
        sql_query(sql).execute(self)
    }

    fn load_segment_ids(&mut self, sql: &str) -> Result<Vec<SegmentIdRow>, DieselError> {
        sql_query(sql).load::<SegmentIdRow>(self)
    }

    fn load_segments(&mut self, sql: &str) -> Result<Vec<SegmentRow>, DieselError> {
        sql_query(sql).load::<SegmentRow>(self)
    }

    fn load_exceptions(&mut self, sql: &str) -> Result<Vec<ExceptionRow>, DieselError> {
        sql_query(sql).load::<ExceptionRow>(self)
    }

    fn load_ranges(&mut self, sql: &str) -> Result<Vec<RangeRow>, DieselError> {
        sql_query(sql).load::<RangeRow>(self)
    }
}

impl SegmentDb for SqliteConnection {
    fn execute_sql(&mut self, sql: &str) -> Result<usize, DieselError> {
        sql_query(sql).execute(self)
    }

    fn load_segment_ids(&mut self, sql: &str) -> Result<Vec<SegmentIdRow>, DieselError> {
        sql_query(sql).load::<SegmentIdRow>(self)
    }

    fn load_segments(&mut self, sql: &str) -> Result<Vec<SegmentRow>, DieselError> {
        sql_query(sql).load::<SegmentRow>(self)
    }

    fn load_exceptions(&mut self, sql: &str) -> Result<Vec<ExceptionRow>, DieselError> {
        sql_query(sql).load::<ExceptionRow>(self)
    }

    fn load_ranges(&mut self, sql: &str) -> Result<Vec<RangeRow>, DieselError> {
        sql_query(sql).load::<RangeRow>(self)
    }
}

pub(crate) fn map_segment_row(row: SegmentRow) -> Result<IngestSegment, SegmentStateError> {
    Ok(IngestSegment {
        segment_id: row.segment_id,
        start_id: row.start_id,
        end_id: row.end_id,
        status: SegmentStatus::from_db_str(&row.status)?,
        attempts: row.attempts,
        scan_cursor_id: row.scan_cursor_id,
        unresolved_count: row.unresolved_count,
        last_error: row.last_error,
    })
}

pub(crate) fn map_exception_row(row: ExceptionRow) -> Result<IngestException, SegmentStateError> {
    Ok(IngestException {
        segment_id: row.segment_id,
        item_id: row.item_id,
        state: ExceptionState::from_db_str(&row.state)?,
        attempts: row.attempts,
        last_error: row.last_error,
    })
}

pub(crate) fn quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

pub(crate) fn quote_opt(value: Option<&str>) -> String {
    value.map(quote).unwrap_or_else(|| "NULL".to_string())
}
