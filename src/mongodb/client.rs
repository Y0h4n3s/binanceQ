use mongodb::Client;

use crate::mongodb::models::{ATREntry, BookSideEntry, ChoppinessIndexEntry, OpenInterestEntry, TradeEntry};

pub struct MongoClient {
    pub database: mongodb::Database,
    pub open_interest: mongodb::Collection<OpenInterestEntry>,
    pub book_side: mongodb::Collection<BookSideEntry>,
    pub trades: mongodb::Collection<TradeEntry>,
    pub atr: mongodb::Collection<ATREntry>,
    pub choppiness: mongodb::Collection<ChoppinessIndexEntry>,
}

impl MongoClient {
    pub async fn new() -> Self {
        let mut mongodb_url = "mongodb://localhost:27017/binance-studies".to_string();
        match std::env::var("MONGODB_URL") {
            Ok(url) => { mongodb_url = url }
            Err(_) => {}
        }

        let client = Client::with_uri_str(&mongodb_url).await.unwrap();
        let database = client.database("binance-studies");
        let open_interest = database.collection::<OpenInterestEntry>("open_interest");
        let book_side = database.collection::<BookSideEntry>("book_side");
        let trades = database.collection::<TradeEntry>("trade");
        let atr = database.collection::<ATREntry>("atr");
        let choppiness = database.collection::<ChoppinessIndexEntry>("choppiness");
        MongoClient {
            database,
            open_interest,
            book_side,
            trades,
            atr,
            choppiness
        }
    }
    pub async fn reset_db(&self) {
        self.database.drop(None).await.unwrap();
    }
}