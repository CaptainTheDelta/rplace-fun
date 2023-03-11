use std::{collections::HashMap, env, error::Error, io};

use chrono::{TimeZone, Utc};
use postgres::{types::Type, Client, NoTls};

const DB_CONFIG: &str = "
	host=10.89.0.8
	port=5432
	user=postgres
	password=kikou
	connect_timeout=10
";

#[derive(Debug, serde::Deserialize)]
struct Record {
    timestamp: String,
    user_id: String,
    pixel_color: String,
    coordinate: String,
}

fn main() -> Result<(), Box<dyn Error>> {
    let no_db = env::args().any(|arg| arg == "--nodb");
    if no_db {
        eprintln!("nodb flag enabled. There will be no interaction with the database.")
    }

    // Connect to the database
    let mut client = if no_db {
        None
    } else {
        Some(Client::connect(DB_CONFIG, NoTls).unwrap())
    };

    let (insert_new_user, insert_pixel) = match client.iter_mut().next() {
        None => (None, None),
        Some(c) => {
            // Ensure the tables exist
            c.execute(
                "
		CREATE TABLE IF NOT EXISTS users (
			user_id     INTEGER NOT NULL PRIMARY KEY,
			hash		VARCHAR NOT NULL
		)",
                &[],
            )
            .expect("Table creation for 'users' failed");
            c.execute(
                "
			CREATE TABLE IF NOT EXISTS pixel (
				pixel_id    INTEGER NOT NULL PRIMARY KEY,
				ts			TIMESTAMP NOT NULL,
				user_id		INTEGER NOT NULL REFERENCES users,
				color		INTEGER NOT NULL,
				x1			INTEGER NOT NULL,
				y1			INTEGER NOT NULL,
				x2			INTEGER,
				y2			INTEGER
			)",
                &[],
            )
            .expect("Table cration for 'pixel' failed");

            // Queries used in this program
            let insert_new_user = c
                .prepare_typed(
                    "INSERT INTO users (user_id, hash) VALUES ($1, $2)",
                    &[Type::INT4, Type::VARCHAR],
                )
                .expect("Query preparation for inserting a new user failed");
            let insert_pixel = c.prepare_typed(
			"INSERT INTO pixel (pixel_id, ts, user_id, color, x1, y1, x2, y2) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
			&[Type::INT4, Type::TIMESTAMP, Type::INT4, Type::INT4, Type::INT4, Type::INT4, Type::INT4, Type::INT4]
		).expect("Query preparation for inserting a new pixel failed");

            (Some(insert_new_user), Some(insert_pixel))
        }
    };

    // Read the CSV
    let mut csv_reader = csv::Reader::from_reader(io::stdin());

    let mut users = HashMap::new();
    let mut next_user_id = 0i32;

    // Iterating over records to insert the data in the database
    let headers = csv_reader
        .headers()
        .expect("Failed to get CSV headers")
        .to_owned();
    for (pixel_id, line) in csv_reader.records().enumerate() {
        let pixel_id = pixel_id as i32;
        let line = line.unwrap();
        let record: Record = line
            .deserialize(Some(&headers))
            .expect(&format!("Failed to parse a CSV record : {:?}", line));
        let user_hash = record.user_id.clone();

        // Retrieving (or generating) the user of the pixel
        let user_id: i32 = *users.entry(record.user_id).or_insert_with(|| {
            let current_user_id = next_user_id;
            next_user_id += 1;
            // Adding client
            match client.iter_mut().next() {
                None => (),
                Some(c) => {
                    c.execute(
                        insert_new_user.as_ref().unwrap(),
                        &[&current_user_id, &user_hash],
                    )
                    .expect(&format!(
                        "Failed to insert a new user in the table, with id {} and hash '{}'",
                        current_user_id, user_hash
                    ));
                }
            };
            current_user_id
        });

        // Timestamp conversion
        let ts = Utc
            .datetime_from_str(&record.timestamp, "%Y-%m-%d %H:%M:%S%.f UTC")
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
                panic!("Coordinates are not by two or four at record {pixel_id}")
            }
        };

        // Pixel insertion into the database
        match client.iter_mut().next() {
            None => (),
            Some(c) => {
                c.execute(
                    insert_pixel.as_ref().unwrap(),
                    &[&pixel_id, &ts, &user_id, &color, &x1, &y1, &x2, &y2],
                )
                .expect(&format!(
                    "Failed to insert a new pixel in the table, with id {} and timestamp '{:?}'",
                    pixel_id, ts
                ));
            }
        };

		if pixel_id % 100_000 == 0 {
			eprint!("{pixel_id} records processed\r");
		}
    }

    Ok(())
}
