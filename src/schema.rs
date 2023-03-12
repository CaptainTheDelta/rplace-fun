// @generated automatically by Diesel CLI.

diesel::table! {
    pixel (pixel_id) {
        pixel_id -> Int4,
        ts -> Timestamp,
        user_id -> Int4,
        color -> Int4,
        x1 -> Int4,
        y1 -> Int4,
        x2 -> Nullable<Int4>,
        y2 -> Nullable<Int4>,
    }
}

diesel::table! {
    users (user_id) {
        user_id -> Int4,
        hash -> Varchar,
    }
}

diesel::joinable!(pixel -> users (user_id));

diesel::allow_tables_to_appear_in_same_query!(
    pixel,
    users,
);
