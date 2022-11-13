use mongodb::sync::Client;
use crate::mongodb::models::{BookSideEntry,  OpenInterestEntry, };

pub struct MongoClient {
    pub database: mongodb::sync::Database,
    pub open_interest: mongodb::sync::Collection<OpenInterestEntry>,
    pub book_side: mongodb::sync::Collection<BookSideEntry>,
}

impl MongoClient {
    pub fn new() -> Self {
        let mut mongodb_url = "mongodb://localhost:27017/binance-studies".to_string();
        match std::env::var("MONGODB_URL") {
            Ok(url) => { mongodb_url = url }
            Err(_) => {}
        }

        let client = Client::with_uri_str(&mongodb_url).unwrap();
        let database = client.database("arb-swap");
        let open_interest = database.collection::<OpenInterestEntry>("open_interest");
        let book_side = database.collection::<BookSideEntry>("book_side");

        MongoClient {
            database,
            open_interest,
            book_side,
        }
    }
}