// @generated automatically by Diesel CLI.

diesel::table! {
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
        parts -> Nullable<Text>,
        descendants -> Nullable<Int8>,
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

diesel::joinable!(kids -> items (item));

diesel::allow_tables_to_appear_in_same_query!(
    items,
    kids,
    users,
);
