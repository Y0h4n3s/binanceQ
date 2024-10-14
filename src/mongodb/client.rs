use crate::types::{Kline, Order, Symbol, TfTrade, Trade, TradeEntry};
use mongodb::bson;
use mongodb::bson::doc;
use mongodb::Client;

pub struct MongoClient {
    pub database: mongodb::Database,
    pub trades: mongodb::Collection<TradeEntry>,
    pub tf_trades: mongodb::Collection<TfTrade>,
    pub kline: mongodb::Collection<Kline>,
    pub orders: mongodb::Collection<Order>,
    pub past_trades: mongodb::Collection<Trade>,
}

impl MongoClient {
    pub async fn new() -> Self {
        let mut mongodb_url = "mongodb://localhost:27017/binance-studies".to_string();
        if let Ok(url) = std::env::var("MONGODB_URL") {
            mongodb_url = url
        }

        let client = Client::with_uri_str(&mongodb_url).await.unwrap();
        let database = client.database("binance-studies");
        let trades = database.collection::<TradeEntry>("trade");
        let tf_trades = database.collection::<TfTrade>("tf_trade");
        let orders = database.collection::<Order>("orders");
        let past_trades = database.collection::<Trade>("past_trades");
        let kline = database.collection::<Kline>("kline");
        MongoClient {
            database,
            orders,
            trades,
            tf_trades,
            kline,
            past_trades,
        }
    }
    pub async fn reset_db(&self, symbol: &Symbol) {
        self.kline
            .delete_many(doc! {"symbol": bson::to_bson(symbol).unwrap()}, None)
            .await
            .unwrap();
    }

    pub async fn reset_trades(&self, symbol: &Symbol) {
        self.past_trades
            .delete_many(doc! {"symbol": bson::to_bson(symbol).unwrap()}, None)
            .await
            .unwrap();
    }
    pub async fn reset_orders(&self, symbol: &Symbol) {
        self.orders
            .delete_many(doc! {"symbol": bson::to_bson(symbol).unwrap()}, None)
            .await
            .unwrap();
    }
}
