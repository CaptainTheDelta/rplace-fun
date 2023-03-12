use chrono::NaiveDateTime;
use diesel::prelude::*;

use crate::schema::{pixel, users};

#[derive(Insertable, Queryable)]
#[diesel(table_name = users)]
pub struct User {
    pub user_id: i32,
    pub hash: String,
}

#[derive(Insertable, Queryable)]
#[diesel(table_name = pixel)]
pub struct Pixel {
    pub pixel_id: i32,
    pub ts: NaiveDateTime,
    pub user_id: i32,
    pub color: i32,
    pub x1: i32,
    pub y1: i32,
    pub x2: Option<i32>,
    pub y2: Option<i32>,
}
