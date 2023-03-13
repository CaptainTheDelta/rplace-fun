use std::fs::File;
use std::{collections::HashMap, env, error::Error};

use chrono::NaiveDateTime;
use csv::ByteRecord;
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
        let mut users = HashMap::new();

        match connection.iter_mut().next() {
            None => (),
            Some(c) => {
                for user in users::table
                    .load::<User>(c)
                    .expect("Failed to query users")
                    .into_iter()
                {
                    users.insert(user.hash, user.user_id);
                }
            }
        }

        let next_user_id = users.len() as i32;
        UserManager {
            users,
            next_user_id,
            users_to_insert: Vec::new(),
        }
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

    let filepath = env::args()
        .last()
        .expect("Expected a file argument at the last position");
    let file = File::open(filepath).expect("Failed to load the file");

    // Connect to the database
    let mut connection = if no_db {
        None
    } else {
        Some(establish_connection())
    };
    // Read the CSV
    let mut csv_reader = csv::ReaderBuilder::new()
        .buffer_capacity(1_073_741_824)
        .from_reader(file);

    let mut user_manager = UserManager::create(&mut connection);
    let mut pixel_manager = PixelManager::create(&mut connection);

    println!(
        "Restarting with {} users and {} pixel records already in the database",
        user_manager.next_user_id, pixel_manager.pixels_in_db
    );

    // Iterating over records to insert the data in the database
    let headers = csv_reader
        .byte_headers()
        .expect("Failed to get CSV headers")
        .to_owned();

    let mut record_consumed = 0;
    {
        println!("Skipping already inserted records."); // In an optimized way
        let mut byte_record = ByteRecord::new();
        while record_consumed < pixel_manager.pixels_in_db
            && csv_reader
                .read_byte_record(&mut byte_record)
                .expect("Failed to read a byte_record while skipping already inserted records.")
        {
            record_consumed += 1;
        }
    }

    println!(
        "Skipped records. Now at position {:?}",
        csv_reader.position()
    );

    // Starting enumeration from the number of records consumed, to give the right id to the next records to insert
    let records_iter = (record_consumed..)
        .zip(csv_reader.into_byte_records())
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
