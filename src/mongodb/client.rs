use mongodb::sync::Client;
use crate::mongodb::models::{BookSideEntry, OpenInterestEntry, TradeEntry};

pub struct MongoClient {
    pub database: mongodb::sync::Database,
    pub open_interest: mongodb::sync::Collection<OpenInterestEntry>,
    pub book_side: mongodb::sync::Collection<BookSideEntry>,
    pub trades: mongodb::sync::Collection<TradeEntry>,
}

impl MongoClient {
    pub fn new() -> Self {
        let mut mongodb_url = "mongodb://localhost:27017/binance-studies".to_string();
        match std::env::var("MONGODB_URL") {
            Ok(url) => { mongodb_url = url }
            Err(_) => {}
        }

        let client = Client::with_uri_str(&mongodb_url).unwrap();
        let database = client.database("binance-studies");
        let open_interest = database.collection::<OpenInterestEntry>("open_interest");
        let book_side = database.collection::<BookSideEntry>("book_side");
        let trades = database.collection::<TradeEntry>("trade");

        MongoClient {
            database,
            open_interest,
            book_side,
            trades
        }
    }
    pub fn reset_db(&self) {
        self.database.drop(None).unwrap();
    }
}