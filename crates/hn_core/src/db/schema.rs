// @generated automatically by Diesel CLI.

pub mod sql_types {
    #[derive(diesel::query_builder::QueryId, diesel::sql_types::SqlType)]
    #[diesel(postgres_type(name = "tsvector", schema = "pg_catalog"))]
    pub struct Tsvector;
}

diesel::table! {
    ingest_exceptions (segment_id, item_id) {
        segment_id -> Int8,
        item_id -> Int8,
        state -> Text,
        attempts -> Int4,
        next_retry_at -> Nullable<Timestamptz>,
        last_error -> Nullable<Text>,
        updated_at -> Timestamptz,
        failure_class -> Nullable<Text>,
    }
}

diesel::table! {
    ingest_segments (segment_id) {
        segment_id -> Int8,
        start_id -> Int8,
        end_id -> Int8,
        status -> Text,
        attempts -> Int4,
        scan_cursor_id -> Nullable<Int8>,
        unresolved_count -> Int4,
        heartbeat_at -> Nullable<Timestamptz>,
        started_at -> Timestamptz,
        last_error -> Nullable<Text>,
        failure_class -> Nullable<Text>,
    }
}

diesel::table! {
    use diesel::sql_types::*;
    use super::sql_types::Tsvector;

    items (id) {
        id -> Int8,
        deleted -> Nullable<Bool>,
        #[sql_name = "type"]
        type_ -> Nullable<Text>,
        by -> Nullable<Text>,
        time -> Nullable<Int8>,
        text -> Nullable<Text>,
        dead -> Nullable<Bool>,
        parent -> Nullable<Int8>,
        poll -> Nullable<Int8>,
        url -> Nullable<Text>,
        score -> Nullable<Int8>,
        title -> Nullable<Text>,
        parts -> Nullable<Array<Nullable<Int8>>>,
        descendants -> Nullable<Int8>,
        domain -> Nullable<Text>,
        day -> Nullable<Date>,
        search_tsv -> Nullable<Tsvector>,
        story_id -> Nullable<Int8>,
    }
}

diesel::table! {
    kids (item, kid) {
        item -> Int8,
        kid -> Int8,
        display_order -> Nullable<Int8>,
    }
}

diesel::table! {
    users (id) {
        id -> Text,
        created -> Nullable<Int8>,
        karma -> Nullable<Int8>,
        about -> Nullable<Text>,
        submitted -> Nullable<Text>,
    }
}

diesel::joinable!(ingest_exceptions -> ingest_segments (segment_id));

diesel::allow_tables_to_appear_in_same_query!(
    ingest_exceptions,
    ingest_segments,
    items,
    kids,
    users,
);
