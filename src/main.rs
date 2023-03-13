use std::{collections::HashMap, env, error::Error, io};

use chrono::NaiveDateTime;
use diesel::dsl::count;
use diesel::pg::PgConnection;
use diesel::prelude::*;
use dotenvy::dotenv;
use itertools::Itertools;

use rplace_fun::models::{Pixel, User};
use rplace_fun::schema::pixel::pixel_id;
use rplace_fun::schema::{pixel, users};

#[derive(Debug, serde::Deserialize)]
struct Record {
    timestamp: String,
    user_id: String,
    pixel_color: String,
    coordinate: String,
}

struct UserManager {
    users: HashMap<String, i32>,
    next_user_id: i32,
    users_to_insert: Vec<User>,
}

impl UserManager {
    fn create(connection: &mut Option<PgConnection>) -> UserManager {
        let mut mgr = UserManager {
            users: HashMap::new(),
            next_user_id: 0,
            users_to_insert: Vec::new(),
        };

        match connection.iter_mut().next() {
            None => (),
            Some(c) => {
                let users = users::table.load::<User>(c).expect("Failed to query users");
                for user in users.into_iter() {
                    mgr.users.insert(user.hash, user.user_id);
                }
            }
        }

        mgr
    }

    fn get_from_hash(&mut self, hash: &str) -> i32 {
        *self.users.entry(hash.to_string()).or_insert_with(|| {
            let current_user_id = self.next_user_id;
            self.next_user_id += 1;

            let user = User {
                user_id: current_user_id,
                hash: hash.to_string(),
            };
            self.users_to_insert.push(user);
            current_user_id
        })
    }

    fn insert_users(&mut self, c: &mut PgConnection) {
        diesel::insert_into(users::table)
            .values(&self.users_to_insert)
            .execute(c)
            .expect(&format!(
                "Failed to insert {} users in the table",
                self.users_to_insert.len()
            ));
        self.users_to_insert.clear();
    }
}

struct PixelManager {
    pixels_to_insert: Vec<Pixel>,
    pixels_in_db: usize,
}

impl PixelManager {
    fn create(connection: &mut Option<PgConnection>) -> PixelManager {
        let pixels_in_db = match connection.iter_mut().next() {
            None => 0,
            Some(c) => pixel::table
                .select(count(pixel_id))
                .first::<i64>(c)
                .expect("Failed to count pixels") as usize,
        };

        PixelManager {
            pixels_to_insert: Vec::new(),
            pixels_in_db,
        }
    }

    fn push(&mut self, p: Pixel) {
        self.pixels_to_insert.push(p)
    }

    fn insert_pixels(&mut self, c: &mut PgConnection) {
        diesel::insert_into(pixel::table)
            .values(&self.pixels_to_insert)
            .execute(c)
            .expect("Failed to insert pixels in the table");
        self.pixels_in_db += self.pixels_to_insert.len();
        self.pixels_to_insert.clear();
    }
}

pub fn establish_connection() -> PgConnection {
    dotenv().ok();

    let database_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    PgConnection::establish(&database_url)
        .unwrap_or_else(|_| panic!("Error connecting to {}", database_url))
}

fn main() -> Result<(), Box<dyn Error>> {
    let no_db = env::args().any(|arg| arg == "--nodb");
    if no_db {
        eprintln!("nodb flag enabled. There will be no interaction with the database.")
    }

    // Connect to the database
    let mut connection = if no_db {
        None
    } else {
        Some(establish_connection())
    };
    // Read the CSV
    let mut csv_reader = csv::Reader::from_reader(io::stdin());

    let mut user_manager = UserManager::create(&mut connection);
    let mut pixel_manager = PixelManager::create(&mut connection);

    // Iterating over records to insert the data in the database
    let headers = csv_reader
        .headers()
        .expect("Failed to get CSV headers")
        .to_owned();

    let mut record_consumed = pixel_manager.pixels_in_db;
    let records_iter = csv_reader
        .records()
        .skip(record_consumed)
        .enumerate()
        .chunks(10_000);
    for chunk in records_iter.into_iter() {
        for (record_id, line) in chunk {
            let pid = record_id as i32;
            let line = line.unwrap();
            let record: Record = line
                .deserialize(Some(&headers))
                .expect(&format!("Failed to parse a CSV record : {:?}", line));
            let user_hash = record.user_id.clone();

            // Retrieving (or generating) the user of the pixel
            let user_id = user_manager.get_from_hash(&user_hash);

            // Timestamp conversion
            let ts = NaiveDateTime::parse_from_str(&record.timestamp, "%Y-%m-%d %H:%M:%S%.f UTC")
                .expect(&format!(
                    "Failed to parse this timestamp: {}",
                    record.timestamp
                ));
            // Color conversion to integer
            let color = i32::from_str_radix(&record.pixel_color[1..], 16).unwrap();

            // Pixel coordinates
            let coords: Vec<i32> = record
                .coordinate
                .split(',')
                .map(|x| {
                    x.parse().expect(&format!(
                        "Error while parsing coordinates: {}",
                        record.coordinate
                    ))
                })
                .collect();
            let (x1, y1, x2, y2) = {
                if coords.len() == 2 {
                    (coords[0], coords[1], None, None)
                } else if coords.len() == 4 {
                    (coords[0], coords[1], Some(coords[2]), Some(coords[3]))
                } else {
                    panic!("Coordinates are not by two or four at record {pid}")
                }
            };

            let pixel = Pixel {
                pixel_id: pid,
                user_id,
                ts,
                color,
                x1,
                y1,
                x2,
                y2,
            };
            pixel_manager.push(pixel);
            record_consumed += 1;
        }

        match connection.iter_mut().next() {
            None => (),
            Some(c) => {
                user_manager.insert_users(c);
                // Pixel insertion into the database
                pixel_manager.insert_pixels(c);
            }
        }

        eprint!("{record_consumed} records processed\r");
    }

    Ok(())
}
