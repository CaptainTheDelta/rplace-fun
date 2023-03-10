use std::{collections::HashMap, io};

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

fn main() {
    // Connect to the database
    let mut client = Client::connect(DB_CONFIG, NoTls).unwrap();
    // Ensure the tables exist
    client
        .execute(
            "
		CREATE TABLE IF NOT EXISTS users (
			user_id     INTEGER NOT NULL PRIMARY KEY,
			hash		VARCHAR NOT NULL
		)",
            &[],
        )
        .unwrap();
    client
        .execute(
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
        .unwrap();

    // Queries used in this program
    let insert_new_user = client
        .prepare_typed(
            "INSERT INTO users (user_id, hash) VALUES ($1, $2)",
            &[Type::INT4, Type::VARCHAR],
        )
        .unwrap();
    let insert_pixel = client.prepare_typed(
		"INSERT INTO pixel (pixel_id, ts, user_id, color, x1, y1, x2, y2) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
		&[Type::INT4, Type::TIMESTAMP, Type::INT4, Type::INT4, Type::INT4, Type::INT4, Type::INT4, Type::INT4]
	).unwrap();

    // Read the CSV
    let mut csv_reader = csv::Reader::from_reader(io::stdin());

    let mut users = HashMap::new();
    let mut next_user_id = 0i32;

    // Iterating over records to insert the data in the database
    let headers = csv_reader.headers().unwrap().to_owned();
    for (pixel_id, line) in csv_reader.records().enumerate() {
        let record: Record = line.unwrap().deserialize(Some(&headers)).unwrap();
		let user_hash = record.user_id.clone();

        // Retrieving (or generating) the user of the pixel
        let user_id: i32 = *users.entry(record.user_id).or_insert_with(|| {
            let current_id = next_user_id;
            next_user_id += 1;
            // Adding client
            client.execute(&insert_new_user, &[&current_id, &user_hash]).unwrap();
            current_id
        });

		// Timestamp conversion
		let ts = DateTime::parse_from_str(record.timestamp, "%Y-%m-%d %H:%M:%S%.f %z").unwrap();

        // Color conversion to integer
        let color = i32::from_str_radix(&record.pixel_color[1..], 16).unwrap();

        // Pixel coordinates
        let coords: Vec<i32> = record.coordinate[1..record.coordinate.len() - 1]
            .split(',')
            .map(|x| x.parse().unwrap())
            .collect();
        let (x1, y1, x2, y2) = {
            if coords.len() == 2 {
                (coords[0], coords[1], None, None)
            } else if coords.len() == 4 {
                (coords[0], coords[1], Some(coords[3]), Some(coords[4]))
            } else {
                panic!("Coordinates are not by two or four at record {}", pixel_id)
            }
        };

        // Pixel insertion into the database
        client
            .execute(
                &insert_pixel,
                &[
                    &(pixel_id as i32),
                    &record.timestamp,
                    &user_id,
                    &color,
                    &x1,
                    &y1,
                    &x2,
                    &y2,
                ],
            )
            .unwrap();
    }
}
