use mongodb::Client;

use binance_q_types::{ATREntry, Kline,AverageDirectionalIndexEntry, BookSideEntry, ChoppinessIndexEntry, OpenInterestEntry, Order, TfTrade, Trade, TradeEntry};

pub struct MongoClient {
    pub database: mongodb::Database,
    pub open_interest: mongodb::Collection<OpenInterestEntry>,
    pub book_side: mongodb::Collection<BookSideEntry>,
    pub trades: mongodb::Collection<TradeEntry>,
    pub tf_trades: mongodb::Collection<TfTrade>,
    pub atr: mongodb::Collection<ATREntry>,
    pub adi: mongodb::Collection<AverageDirectionalIndexEntry>,
    pub kline: mongodb::Collection<Kline>,
    pub orders: mongodb::Collection<Order>,
    pub choppiness: mongodb::Collection<ChoppinessIndexEntry>,
    pub past_trades: mongodb::Collection<Trade>,
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
        let tf_trades = database.collection::<TfTrade>("tf_trade");
        let atr = database.collection::<ATREntry>("atr");
        let adi = database.collection::<AverageDirectionalIndexEntry>("adi");
        let orders = database.collection::<Order>("orders");
        let choppiness = database.collection::<ChoppinessIndexEntry>("choppiness");
        let past_trades = database.collection::<Trade>("past_trades");
        let kline = database.collection::<Kline>("kline");
        MongoClient {
            database,
            open_interest,
            orders,
            book_side,
            trades,
            tf_trades,
            atr,
            adi,
            kline,
            choppiness,
            past_trades
            
        }
    }
    pub async fn reset_db(&self) {
        self.database.drop(None).await.unwrap();
    }
    pub async fn reset_studies(&self) {
        self.atr.drop(None).await.unwrap();
        self.choppiness.drop(None).await.unwrap();
        self.adi.drop(None).await.unwrap();
    }
    pub async fn reset_trades(&self) {
        self.past_trades.drop(None).await.unwrap();
    }
    pub async fn reset_orders(&self) {
        self.orders.drop(None).await.unwrap();
    }
}