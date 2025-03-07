use crate::types::{Kline, Order, OrderType, Side, Symbol, TfTrade, TfTrades, Trade, TradeEntry};
use futures::stream::BoxStream;
use rusqlite::{params, ToSql};
use rust_decimal::prelude::FromPrimitive;
use rust_decimal::Decimal;
use serde::{Deserialize, Deserializer, Serialize};
use serde_with::__private__::DeError;
use sqlx::pool::PoolOptions;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqliteRow, SqliteSynchronous};
use sqlx::types::Text;
use sqlx::{
    Acquire, Connection, Decode, Error, Executor, FromRow, Row, Sqlite, SqliteConnection,
    SqlitePool, Type,
};
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use rayon::prelude::*;
use tracing::info;
use uuid::Uuid;
#[derive(Clone)]
pub struct SQLiteClient {
    pub pool: SqlitePool,
    pub conn: Arc<Mutex<rusqlite::Connection>>,
}


#[cfg(feature = "trades")]
#[derive(Serialize, Deserialize, Debug, Clone, FromRow, Decode, Type)]
pub struct SQLiteTradeEntry {
    pub price: f64,
    pub qty: f64,
    pub timestamp: u64,
    pub delta: f64,
    pub symbol: String,
}

#[cfg(feature = "trades")]
#[derive(Serialize, Deserialize, Debug, Clone, FromRow, Decode)]
pub struct SQLiteTfTrade {
    pub symbol: String,
    pub tf: i64,
    pub timestamp: String,
    pub min_trade_time: String,
    pub max_trade_time: String,
    pub trades: serde_json::Value,
}


#[cfg(feature = "trades")]
#[derive(Serialize, Deserialize, Debug, Clone, FromRow, Decode)]
pub struct TfTradeToTradeEntry {
    pub symbol: String,
    pub tf: i64,
    pub timestamp: String,
    pub min_trade_time: String,
    pub max_trade_time: String,
    #[serde(deserialize_with = "deserialize_trade_entry")]
    pub trades: Vec<SQLiteTradeEntry>,
}

#[cfg(feature = "trades")]
#[derive(Serialize, Deserialize, Debug, Clone, FromRow, Decode)]
pub struct SQliteTfTradeToTradeEntry {
    pub tf: i64,
    pub symbol: String,
    pub tf_trade_id: u64,
    pub trade_entry_id: u64,
}

#[cfg(feature = "trades")]
fn deserialize_trade_entry<'de, D>(deserializer: D) -> Result<Vec<SQLiteTradeEntry>, D::Error>
where
    D: Deserializer<'de>,
{
    let s: Option<&str> = Option::deserialize(deserializer)?;
    match s {
        Some(s) => serde_json::from_str(s).map_err(DeError::custom),
        None => Ok(Vec::new()),
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, FromRow, Decode)]
pub struct SQLiteKline {
    pub symbol: String,
    pub open_time: String,
    pub open: String,
    pub high: String,
    pub low: String,
    pub close: String,
    pub volume: String,
    pub close_time: String,
    pub quote_volume: String,
    pub count: i64,
    pub taker_buy_volume: String,
    pub taker_buy_quote_volume: String,
    pub ignore: i64,
}
#[cfg(feature = "candles")]
#[derive(Serialize, Deserialize, Debug, Clone, FromRow, Decode)]
pub struct SQLiteTfTrade {
    pub symbol: String,
    pub open_time: String,
    pub open: String,
    pub high: String,
    pub low: String,
    pub close: String,
    pub volume: String,
    pub close_time: String,
    pub quote_volume: String,
    pub count: i64,
    pub taker_buy_volume: String,
    pub taker_buy_quote_volume: String,
    pub tf: String,
    pub ignore: i64,
}

#[derive(Serialize, Deserialize, Debug, Clone, FromRow, Decode)]
pub struct SQLiteTrade {
    pub id: u64,
    pub order_id: Uuid,
    pub symbol: String,

    pub maker: bool,
    pub price: String,
    pub commission: String,
    pub position_side: String,
    pub side: String,
    pub realized_pnl: String,
    pub exit_order_type: serde_json::Value,
    pub qty: String,
    pub quote_qty: String,
    pub time: u64,
}

#[derive(Serialize, Deserialize, Debug, Clone, FromRow, Decode)]
pub struct SQLiteOrder {
    pub order_id: serde_json::Value,
    pub symbol: String,
    pub side: String,
    pub price: String,
    pub quantity: String,
    pub time: u64,
    pub order_type: serde_json::Value,
    pub lifetime: u64,
    pub close_policy: serde_json::Value,
}


#[cfg(feature = "trades")]
impl FromRow<'_, SqliteRow> for TradeEntry {
    fn from_row(row: &SqliteRow) -> Result<TradeEntry, sqlx::Error> {
        let price: f64 = row.try_get("price")?;
        let qty: f64 = row.try_get("qty")?;
        let delta: f64 = row.try_get("delta")?;
        Ok(TradeEntry {
            id: row.try_get("id")?,
            price: Decimal::from_f64(price).unwrap(),
            qty: Decimal::from_f64(qty).unwrap(),
            timestamp: row.try_get("timestamp")?,
            delta: Decimal::from_f64(delta).unwrap(),
            symbol: row.try_get("symbol")?,
        })
    }
}

#[cfg(feature = "trades")]
impl From<TfTrade> for SQLiteTfTrade {
    fn from(tf_trade: TfTrade) -> Self {
        SQLiteTfTrade {
            symbol: tf_trade.symbol.symbol,
            tf: tf_trade.tf as i64,
            timestamp: tf_trade.timestamp.to_string(),
            max_trade_time: tf_trade.max_trade_time.to_string(),
            min_trade_time: tf_trade.min_trade_time.to_string(),
            trades: serde_json::to_value(&tf_trade.trades).unwrap(),
        }
    }
}
#[cfg(feature = "trades")]
impl FromRow<'_, SqliteRow> for TfTrade {
    fn from_row(row: &'_ SqliteRow) -> Result<Self, Error> {
        let mut symbol = Symbol::default();
        symbol.symbol = row.try_get("symbol")?;
        Ok(Self {
            symbol,
            tf: row.try_get("tf")?,
            id: row.try_get("id")?,
            timestamp: row.try_get("timestamp")?,
            min_trade_time: row.try_get("min_trade_time")?,
            max_trade_time: row.try_get("max_trade_time")?,
            trades: serde_json::from_value(row.try_get("trades")?).unwrap(),
        })
    }
}

#[cfg(feature = "candles")]
impl FromRow<'_, SqliteRow> for TfTrade {
    fn from_row(row: &'_ SqliteRow) -> Result<Self, Error> {
        let open: f64 = row.try_get("open")?;
        let high: f64 = row.try_get("high")?;
        let low: f64 = row.try_get("low")?;
        let close: f64 = row.try_get("close")?;
        let volume: f64 = row.try_get("volume")?;
        let quote_volume: f64 = row.try_get("quote_volume")?;
        let taker_buy_volume: f64 = row.try_get("taker_buy_volume")?;
        let taker_buy_quote_volume: f64 = row.try_get("taker_buy_quote_volume")?;

        let mut symbol = Symbol::default();
        symbol.symbol = row.try_get("symbol")?;
        Ok(Self {
            symbol,
            open_time: row.try_get("open_time")?,
            open: Decimal::from_f64(open).unwrap(),
            high: Decimal::from_f64(high).unwrap(),
            low: Decimal::from_f64(low).unwrap(),
            close: Decimal::from_f64(close).unwrap(),
            volume: Decimal::from_f64(volume).unwrap(),
            close_time: row.try_get("close_time")?,
            quote_volume: Decimal::from_f64(quote_volume).unwrap(),
            count: row.try_get("count")?,
            taker_buy_volume: Decimal::from_f64(taker_buy_volume).unwrap(),
            taker_buy_quote_volume: Decimal::from_f64(taker_buy_quote_volume).unwrap(),
            ignore: row.try_get("ignore")?,
            tf: row.try_get("tf")?
        })
    }
}

impl From<Kline> for SQLiteKline {
    fn from(kline: Kline) -> Self {
        SQLiteKline {
            symbol: kline.symbol.symbol,
            open_time: kline.open_time.to_string(),
            open: kline.open.to_string(),
            high: kline.high.to_string(),
            low: kline.low.to_string(),
            close: kline.close.to_string(),
            volume: kline.volume.to_string(),
            close_time: kline.close_time.to_string(),
            quote_volume: kline.quote_volume.to_string(),
            count: kline.count as i64,
            taker_buy_volume: kline.taker_buy_volume.to_string(),
            taker_buy_quote_volume: kline.taker_buy_quote_volume.to_string(),
            ignore: kline.ignore as i64,
        }
    }
}
#[cfg(feature = "candles")]
impl From<TfTrade> for SQLiteTfTrade {
    fn from(tf_trade: TfTrade) -> Self {
        SQLiteTfTrade {
            symbol: tf_trade.symbol.symbol,
            open_time: tf_trade.open_time.to_string(),
            open: tf_trade.open.to_string(),
            high: tf_trade.high.to_string(),
            low: tf_trade.low.to_string(),
            close: tf_trade.close.to_string(),
            volume: tf_trade.volume.to_string(),
            close_time: tf_trade.close_time.to_string(),
            quote_volume: tf_trade.quote_volume.to_string(),
            count: tf_trade.count as i64,
            taker_buy_volume: tf_trade.taker_buy_volume.to_string(),
            taker_buy_quote_volume: tf_trade.taker_buy_quote_volume.to_string(),
            tf: tf_trade.tf.to_string(),
            ignore: tf_trade.ignore as i64,
        }
    }
}

impl FromRow<'_, SqliteRow> for Trade {
    fn from_row(row: &SqliteRow) -> Result<Self, Error> {
        let price: f64 = row.try_get("price")?;
        let commission: f64 = row.try_get("commission")?;
        let realized_pnl: f64 = row.try_get("realized_pnl")?;
        let qty: f64 = row.try_get("qty")?;
        let quote_qty: f64 = row.try_get("quote_qty")?;
        let time: u64 = row.try_get("time")?;
        let side: String = row.try_get("side")?;
        let position_side: String = row.try_get("position_side")?;

        let mut symbol = Symbol::default();
        symbol.symbol = row.try_get("symbol")?;
        Ok(Self {
            id: row.try_get("id")?,
            order_id: row.try_get("order_id")?,
            symbol,
            maker: row.try_get("maker")?,
            price: Decimal::from_f64(price).unwrap(),
            commission: Decimal::from_f64(commission).unwrap(),
            position_side: match position_side.as_str() {
                "Bid" => Side::Bid,
                _ => Side::Ask,
            },
            side: match side.as_str() {
                "Bid" => Side::Bid,
                _ => Side::Ask,
            },
            realized_pnl: Decimal::from_f64(realized_pnl).unwrap(),
            exit_order_type: serde_json::from_value(row.try_get("exit_order_type")?).unwrap(),
            qty: Decimal::from_f64(qty).unwrap(),
            quote_qty: Decimal::from_f64(quote_qty).unwrap(),
            time,
        })
    }
}

impl FromRow<'_, SqliteRow> for Kline {
    fn from_row(row: &'_ SqliteRow) -> Result<Self, Error> {
        let open: f64 = row.try_get("open")?;
        let high: f64 = row.try_get("high")?;
        let low: f64 = row.try_get("low")?;
        let close: f64 = row.try_get("close")?;
        let volume: f64 = row.try_get("volume")?;
        let quote_volume: f64 = row.try_get("quote_volume")?;
        let taker_buy_volume: f64 = row.try_get("taker_buy_volume")?;
        let taker_buy_quote_volume: f64 = row.try_get("taker_buy_quote_volume")?;

        let mut symbol = Symbol::default();
        symbol.symbol = row.try_get("symbol")?;
        Ok(Self {
            symbol,
            open_time: row.try_get("open_time")?,
            open: Decimal::from_f64(open).unwrap(),
            high: Decimal::from_f64(high).unwrap(),
            low: Decimal::from_f64(low).unwrap(),
            close: Decimal::from_f64(close).unwrap(),
            volume: Decimal::from_f64(volume).unwrap(),
            close_time: row.try_get("close_time")?,
            quote_volume: Decimal::from_f64(quote_volume).unwrap(),
            count: row.try_get("count")?,
            taker_buy_volume: Decimal::from_f64(taker_buy_volume).unwrap(),
            taker_buy_quote_volume: Decimal::from_f64(taker_buy_quote_volume).unwrap(),
            ignore: row.try_get("ignore")?,
        })
    }
}
impl From<Trade> for SQLiteTrade {
    fn from(trade: Trade) -> Self {
        SQLiteTrade {
            id: trade.id,
            order_id: trade.order_id,
            symbol: trade.symbol.symbol,
            maker: trade.maker,
            price: trade.price.to_string(),
            commission: trade.commission.to_string(),
            position_side: format!("{:?}", trade.position_side), // Store enum as TEXT
            side: format!("{:?}", trade.side),                   // Store enum as TEXT
            realized_pnl: trade.realized_pnl.to_string(),
            exit_order_type: serde_json::to_value(trade.exit_order_type).unwrap(), // Store enum as TEXT
            qty: trade.qty.to_string(),
            quote_qty: trade.quote_qty.to_string(),
            time: trade.time,
        }
    }
}
impl From<Order> for SQLiteOrder {
    fn from(order: Order) -> Self {
        SQLiteOrder {
            order_id: serde_json::to_value(order.id).unwrap(),
            symbol: order.symbol.symbol,
            side: format!("{:?}", order.side),
            price: order.price.to_string(),
            quantity: order.quantity.to_string(),
            time: order.time,
            order_type: serde_json::to_value(order.order_type).unwrap(),
            lifetime: order.lifetime,
            close_policy: serde_json::to_value(order.close_policy).unwrap(),
        }
    }
}

impl FromRow<'_, SqliteRow> for Order {
    fn from_row(row: &'_ SqliteRow) -> Result<Self, Error> {
        let side: String = row.try_get("side")?;
        let price: f64 = row.try_get("price")?;
        let quantity: f64 = row.try_get("quantity")?;
        let time: u64 = row.try_get("time")?;

        let mut symbol = Symbol::default();
        symbol.symbol = row.try_get("symbol")?;
        Ok(Order {
            id: serde_json::from_value(row.try_get("order_id")?).unwrap(),
            symbol,

            side: match side.as_str() {
                "Bid" => Side::Bid,
                _ => Side::Ask,
            },
            price: Decimal::from_f64(price).unwrap(),
            quantity: Decimal::from_f64(quantity).unwrap(),
            time,
            order_type: serde_json::from_value(row.try_get("order_type")?).unwrap(),
            lifetime: row.try_get("lifetime")?,
            close_policy: serde_json::from_value(row.try_get("close_policy")?).unwrap(),
        })
    }
}

impl SQLiteClient {
    pub async fn new() -> Self {
        let options: <<Sqlite as sqlx::Database>::Connection as Connection>::Options =
            SqliteConnectOptions::from_str("sqlite://binance_studies.db")
                .unwrap()
                .create_if_missing(true)
                .journal_mode(SqliteJournalMode::Wal);
        let mut pool_options = PoolOptions::<Sqlite>::new();
        pool_options = pool_options.max_connections(20);
        let pool = pool_options.connect_with(options).await.unwrap();
        SQLiteClient::create_tables(&pool).await;
        let connection = rusqlite::Connection::open("./binance_studies.db").unwrap();
        connection
            .execute("PRAGMA temp_store = MEMORY;", ())
            .unwrap();
        connection.execute("PRAGMA shrink_memory;", ()).unwrap();
        connection.execute("PRAGMA synchronous = OFF;", ()).unwrap();
        SQLiteClient {
            pool,
            conn: Arc::new(Mutex::new(connection)),
        }
    }
    pub async fn create_tables(pool: &SqlitePool) {
        // Create the TradeEntry table
        #[cfg(feature = "trades")]
        sqlx::query(
            r#"
        CREATE TABLE IF NOT EXISTS trade_entries (
            id INTEGER PRIMARY KEY,
            price REAL,
            qty REAL,
            timestamp INTEGER,
            delta REAL,
            symbol TEXT
        );
        "#,
        )
        .execute(pool)
        .await
        .unwrap();

        // Create the TfTrade table
        #[cfg(feature = "trades")]
        sqlx::query(
            r#"
        CREATE TABLE IF NOT EXISTS tf_trades (
            id INTEGER PRIMARY KEY,
            symbol TEXT,
            tf INTEGER,
            timestamp INTEGER,
            min_trade_time INTEGER,
            max_trade_time INTEGER,
            trades JSON
        );
        "#,
        )
        .execute(pool)
        .await
        .unwrap();

        // Create the Order table
        sqlx::query(
            r#"
        CREATE TABLE IF NOT EXISTS orders (
            id TEXT PRIMARY KEY,
            order_id JSON,
            symbol TEXT,
            side TEXT,
            price REAL,
            quantity REAL,
            time INTEGER,
            order_type JSON,
            lifetime INTEGER,
            close_policy JSON
        );
        "#,
        )
        .execute(pool)
        .await
        .unwrap();

        // Create the Kline table
        sqlx::query(
            r#"
        CREATE TABLE IF NOT EXISTS klines (
            symbol TEXT,
            open_time INTEGER,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume REAL,
            close_time INTEGER,
            quote_volume REAL,
            count INTEGER,
            taker_buy_volume REAL,
            taker_buy_quote_volume REAL,
            ignore INTEGER
        );
        "#,
        )
        .execute(pool)
        .await
        .unwrap();

        // Create the TfTrade table
        #[cfg(feature = "candles")]
        sqlx::query(
            r#"
        CREATE TABLE IF NOT EXISTS tf_trades (
            symbol TEXT,
            open_time INTEGER,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume REAL,
            close_time INTEGER,
            quote_volume REAL,
            count INTEGER,
            taker_buy_volume REAL,
            taker_buy_quote_volume REAL,
            tf TEXT,
            ignore INTEGER
        );
        "#,
        )
        .execute(pool)
        .await
        .unwrap();

        // Create the Trade table
        sqlx::query(
            r#"
        CREATE TABLE IF NOT EXISTS trades (
            id INTEGER PRIMARY KEY,
            order_id TEXT,
            symbol TEXT,
            maker BOOLEAN,
            price REAL,
            commission TEXT,
            position_side TEXT,
            side TEXT,
            realized_pnl REAL,
            exit_order_type JSON,
            qty REAL,
            quote_qty REAL,
            time INTEGER
        );
        "#,
        )
        .execute(pool)
        .await
        .unwrap();

        #[cfg(feature = "trades")]
        sqlx::query(
            r#"
        CREATE TABLE IF NOT EXISTS tf_trades_to_entries (
            tf_trade_id INTEGER,
            trade_entry_id INTEGER,
            symbol TEXT,
            tf INTEGER,
            PRIMARY KEY (tf_trade_id, trade_entry_id),
            FOREIGN KEY (tf_trade_id) REFERENCES tf_trades(id) ON DELETE CASCADE,
            FOREIGN KEY (trade_entry_id) REFERENCES trade_entries(id) ON DELETE CASCADE
        );
        "#,
        )
        .execute(pool)
        .await
        .unwrap();
    }

    pub fn create_download_indices(pool: &Arc<Mutex<rusqlite::Connection>>) {
        let conn = pool.lock().unwrap();
        // initial delete
        #[cfg(feature = "trades")]
        conn.execute("CREATE INDEX IF NOT EXISTS trade_entries_symbols ON trade_entries(symbol);", ())
            .unwrap();

    }
    pub fn drop_download_indices(pool: &Arc<Mutex<rusqlite::Connection>>) {
        let conn = pool.lock().unwrap();
        #[cfg(feature = "trades")]
        conn.execute_batch("DROP INDEX trade_entries_symbols;")
            .unwrap();

    }

    #[cfg(feature = "trades")]
    pub fn create_compile_indices(pool: &Arc<Mutex<rusqlite::Connection>>) {
        let conn = pool.lock().unwrap();
        // compiler initial delete
        conn.execute("CREATE INDEX IF NOT EXISTS tf_symbols_tf ON tf_trades(symbol, tf);", ())
            .unwrap();
        // compiler min max select
        conn.execute("CREATE INDEX IF NOT EXISTS trade_entries_symbols_timestamps ON trade_entries(symbol, timestamp);", ())
            .unwrap();

    }

    #[cfg(feature = "trades")]
    pub fn drop_compile_indices(pool: &Arc<Mutex<rusqlite::Connection>>) {
        let conn = pool.lock().unwrap();
        conn.execute_batch("DROP INDEX tf_symbols_tf;\
                                DROP INDEX trade_entries_symbols_timestamps;")
            .unwrap();

    }
  pub fn create_backtest_indices(pool: &Arc<Mutex<rusqlite::Connection>>) {
        let conn = pool.lock().unwrap();
      #[cfg(feature = "trades")]
      {
          // initial count and main iterator
          conn.execute("CREATE INDEX IF NOT EXISTS kline_symbols_close_times ON klines(symbol, close_time);", ())
              .unwrap();
          // join select statement
          conn.execute("CREATE INDEX IF NOT EXISTS tf_trades_to_trade_entries_tf_trades_id ON tf_trades_to_entries(tf_trade_id);", ())
              .unwrap();
      }
      #[cfg(feature = "candles")]
      {
          conn.execute("CREATE INDEX IF NOT EXISTS kline_symbols_close_times_tf ON tf_trades(symbol, close_time, tf);", ())
          .unwrap();
      }

    }
    pub fn drop_backtest_indices(pool: &Arc<Mutex<rusqlite::Connection>>) {
        let conn = pool.lock().unwrap();
        #[cfg(feature = "trades")]
        conn.execute_batch("DROP INDEX kline_symbols_close_times;\
                                DROP INDEX tf_trades_to_trade_entries_tf_trades_id;")
            .unwrap();
        #[cfg(feature = "candles")]
        conn.execute_batch("DROP INDEX kline_symbols_close_times_tf;")
            .unwrap();
    }

    pub async fn insert_order(pool: &SqlitePool, order: Order) {
        let s_trade = SQLiteOrder::from(order);
        sqlx::query(
            r#"
                INSERT INTO orders (order_id, symbol, side, price, quantity, time, order_type, lifetime, close_policy) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
            "#
        ).bind(&s_trade.order_id)
            .bind(s_trade.symbol)
            .bind(s_trade.side)
            .bind(s_trade.price)
            .bind(s_trade.quantity)
            .bind(s_trade.time.to_string())
            .bind(s_trade.order_type)
            .bind(s_trade.lifetime.to_string())
            .bind(s_trade.close_policy)
            .execute(pool)
            .await
            .unwrap();
    }
    pub async fn insert_trade(pool: &SqlitePool, trade: Trade) {
        let s_trade = SQLiteTrade::from(trade);
        sqlx::query(
            r#"
                INSERT INTO trades (order_id, symbol, maker, price, commission, position_side, side, realized_pnl, exit_order_type, qty, quote_qty, time) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            "#
        ).bind(&s_trade.order_id)
            .bind(s_trade.symbol)
            .bind(s_trade.maker)
            .bind(s_trade.price)
            .bind(s_trade.commission)
            .bind(s_trade.position_side)
            .bind(s_trade.side)
            .bind(s_trade.realized_pnl)
            .bind(s_trade.exit_order_type)
            .bind(s_trade.qty)
            .bind(s_trade.quote_qty)
            .bind(s_trade.time.to_string())
            .execute(pool)
            .await
            .unwrap();
    }

    #[cfg(feature = "trades")]
    pub async fn get_trades_count_by_symbol<'a>(pool: &'a SqlitePool, symbol: &'a Symbol) -> u64 {
        sqlx::query_scalar(
            r#"
                SELECT COUNT(*) FROM trade_entries WHERE symbol = ?;
            "#,
        )
        .bind(&symbol.symbol)
        .fetch_one(pool)
        .await
        .unwrap()
    }
    #[cfg(feature = "trades")]
    pub async fn get_oldest_trade<'a>(pool: &'a SqlitePool, symbol: &'a Symbol) -> TradeEntry {
        sqlx::query_as(
            r#"
                SELECT * FROM trade_entries WHERE symbol = ? ORDER BY timestamp ASC LIMIT 1;
            "#,
        )
        .bind(&symbol.symbol)
        .fetch_one(pool)
        .await
        .unwrap()
    }
    #[cfg(feature = "trades")]
    pub async fn get_latest_trade<'a>(pool: &'a SqlitePool, symbol: &'a Symbol) -> TradeEntry {
        sqlx::query_as(
            r#"
                SELECT * FROM trade_entries WHERE symbol = ? ORDER BY timestamp DESC LIMIT 1;
            "#,
        )
        .bind(&symbol.symbol)
        .fetch_one(pool)
        .await
        .unwrap()
    }

    pub fn get_kline_stream<'a>(
        pool: &'a SqlitePool,
        symbol: Symbol,
        until: String,
    ) -> BoxStream<'a, Result<Kline, Error>> {
        sqlx::query_as(
            r#"
                SELECT * FROM klines WHERE symbol = ? AND close_time > ? ORDER BY close_time ASC
            "#,
        )
        .bind(symbol.symbol)
        .bind(until)
        .fetch(pool)
    }


    #[cfg(feature = "candles")]
    pub fn get_kline_stream_with_tf<'a>(
        pool: &'a SqlitePool,
        symbol: Symbol,
        until: String,
        tf: String
    ) -> BoxStream<'a, Result<Kline, Error>> {
        sqlx::query_as(
            r#"
                SELECT * FROM tf_trades WHERE symbol = ? AND close_time > ? AND tf = ? ORDER BY close_time ASC
            "#,
        )
        .bind(symbol.symbol)
        .bind(until)
            .bind(tf)
        .fetch(pool)
    }

    #[cfg(feature = "candles")]
    pub fn get_tf_trade_stream_with_tf<'a>(
        pool: &'a SqlitePool,
        symbol: Symbol,
        until: String,
        tf: String,
    ) -> BoxStream<'a, Result<TfTrade, Error>> {
        sqlx::query_as(
            r#"
                SELECT * FROM tf_trades WHERE symbol = ? AND close_time > ? AND tf = ? ORDER BY close_time ASC
            "#,
        )
        .bind(symbol.symbol)
        .bind(until)
            .bind(tf)
        .fetch(pool)
    }
    #[cfg(feature = "trades")]
    pub fn _get_tf_trades_stream<'a>(
        pool: &'a SqlitePool,
        symbol: &'a Symbol,
        until: String,
    ) -> BoxStream<'a, Result<TfTrade, Error>> {
        sqlx::query_as(
            r#"
                SELECT * FROM tf_trades WHERE symbol = ? AND timestamp > ? ORDER BY timestamp ASC
            "#,
        )
        .bind(&symbol.symbol)
        .bind(until)
        .fetch(pool)
    }
    #[cfg(feature = "trades")]
    pub fn get_tf_trades_stream_new<'a>(
        pool: &'a SqlitePool,
        symbol: &'a Symbol,
        until: String,
    ) -> BoxStream<'a, Result<TfTrade, Error>> {
        sqlx::query_as(
            r#"
                SELECT
            A.id as id,
            A.symbol AS symbol,
            A.tf AS tf,
            A.timestamp AS timestamp,
            A.min_trade_time AS min_trade_time,
            A.max_trade_time AS max_trade_time,
            json_group_array(
                json_object(
                    'id', B.id,
                    'price', B.price,
                    'qty', B.qty,
                    'timestamp', B.timestamp,
                    'delta', B.delta,
                    'symbol', B.symbol
                )
            ) AS trades
        FROM tf_trades A
        JOIN tf_trades_to_entries AB ON A.id = AB.tf_trade_id
        JOIN trade_entries B ON B.id = AB.trade_entry_id
        WHERE A.symbol = ? AND A.timestamp > ?
        GROUP BY A.id
        ORDER BY A.timestamp ASC
            "#,
        )
        .bind(&symbol.symbol)
        .bind(until)
        .fetch(pool)
    }

    #[cfg(feature = "trades")]
    pub async fn select_values_between_min_max(
        pool: &SqlitePool,
        symbol: String,
        min: String,
        max: String,
    ) -> Vec<TradeEntry> {
        let values = sqlx::query_as(
            r#"
        SELECT * FROM trade_entries
        WHERE symbol = ? AND timestamp BETWEEN ? AND ? ORDER BY timestamp ASC;
        "#,
        )
        .bind(symbol)
        .bind(min)
        .bind(max)
        .fetch_all(pool)
        .await
        .unwrap();

        values
    }

    #[cfg(feature = "trades")]
    pub fn select_values_between_min_max_sync(
        pool: &Arc<Mutex<rusqlite::Connection>>,
        symbol: &String,
        min: u64,
        max: u64,
    ) -> Vec<(u64, u64)> {
        let s = format!(
            "SELECT id, timestamp FROM trade_entries
        WHERE symbol = '{}' AND timestamp BETWEEN {} AND {} ORDER BY timestamp ASC;",
            symbol, min, max
        );

        let pool = pool.lock().unwrap();

        let mut stmt = pool.prepare(&s).unwrap();

        let rows = stmt
            .query_and_then((), |row| {
                Ok::<(u64, u64), rusqlite::Error>((row.get(0).unwrap(), row.get(1).unwrap()))
            })
            .unwrap()
            .map(|r: Result<(u64, u64), _>| r.unwrap())
            .collect::<Vec<(u64, u64)>>();
        rows
    }
    #[cfg(feature = "trades")]
    pub fn insert_klines(pool: &Arc<Mutex<rusqlite::Connection>>, klines: Vec<Kline>) {
        let statement = "INSERT INTO klines (symbol, open_time, open, high, low, close, volume, close_time, quote_volume, count, taker_buy_volume, taker_buy_quote_volume, ignore) VALUES ".to_string();
        for klines in klines.chunks(10000) {
            let mut rows = "".to_string();
            for kline in klines.to_owned() {
                let s_kline = SQLiteKline::from(kline);

                rows += &format!(
                    "('{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}'),\n",
                    s_kline.symbol,
                    s_kline.open_time,
                    s_kline.open,
                    s_kline.high,
                    s_kline.low,
                    s_kline.close,
                    s_kline.volume,
                    s_kline.close_time,
                    s_kline.quote_volume,
                    s_kline.count,
                    s_kline.taker_buy_volume,
                    s_kline.taker_buy_quote_volume,
                    s_kline.ignore,
                )
            }
            let s = statement.clone() + &rows[..rows.len() - 2];
            let pool = pool.lock().unwrap();
            pool.execute_batch(&s).unwrap();
            drop(s)
        }
    }
    #[cfg(feature = "candles")]
    pub fn insert_tf_trades(pool: &Arc<Mutex<rusqlite::Connection>>, trades: Vec<TfTrade>) {
        let statement = "INSERT INTO tf_trades (symbol, open_time, open, high, low, close, volume, close_time, quote_volume, count, taker_buy_volume, taker_buy_quote_volume, ignore, tf) VALUES ".to_string();
        for trades in trades.chunks(10000) {
            let mut rows = "".to_string();
            for trade in trades.to_owned() {
                let s_trade = SQLiteTfTrade::from(trade);

                rows += &format!(
                    "('{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}', '{}'),\n",
                    s_trade.symbol,
                    s_trade.open_time,
                    s_trade.open,
                    s_trade.high,
                    s_trade.low,
                    s_trade.close,
                    s_trade.volume,
                    s_trade.close_time,
                    s_trade.quote_volume,
                    s_trade.count,
                    s_trade.taker_buy_volume,
                    s_trade.taker_buy_quote_volume,
                    s_trade.ignore,
                    s_trade.tf
                )
            }
            let s = statement.clone() + &rows[..rows.len() - 2];
            let pool = pool.lock().unwrap();
            pool.execute_batch(&s).unwrap();
            drop(s)
        }
    }

    #[cfg(feature = "trades")]
        pub fn insert_trade_entries(
        pool: &Arc<Mutex<rusqlite::Connection>>,
        trade_entries: &Vec<SQLiteTradeEntry>,
    ) {
        let start = Instant::now();
                let mut conn = pool.lock().unwrap();
                let tx = conn.transaction().unwrap();
                {
                    let mut stmt = tx.prepare_cached(
                        "INSERT INTO trade_entries (price, qty, timestamp, delta, symbol) VALUES (?1, ?2, ?3, ?4, ?5)",
                    ).unwrap();
                    for s_entry in trade_entries {
                        stmt.execute(params![
                            s_entry.price,
                            s_entry.qty,
                            s_entry.timestamp,
                            s_entry.delta,
                            s_entry.symbol
                        ]).unwrap();
                    }
                }
                tx.commit().unwrap();

        info!("inserted {} entries in {:?}", trade_entries.len(), start.elapsed());
    }


    #[cfg(feature = "trades")]
    pub fn insert_tf_trades(pool: &Arc<Mutex<rusqlite::Connection>>, trades: TfTrades) -> Vec<u64>{
        let mut conn = pool.lock().unwrap();
        let tx = conn.transaction().unwrap();
        let mut row_ids = vec![];
        {
            let mut stmt = tx.prepare_cached(
                "INSERT INTO tf_trades (symbol, tf, timestamp, min_trade_time, max_trade_time, trades) VALUES (?1, ?2, ?3, ?4, ?5, ?6) RETURNING id",
            ).unwrap();
            for trade in trades {
                let row = stmt.query_row::<u64, _, _>(params![
                    trade.symbol.symbol,
                    trade.tf,
                    trade.timestamp,
                    trade.min_trade_time,
                    trade.max_trade_time,
                    serde_json::to_string(&trade.trades).unwrap()
                ], |row| row.get(0)).unwrap();
                row_ids.push(row);
            }
        }
        tx.commit().unwrap();
        row_ids
    }

    #[cfg(feature = "trades")]
    pub fn insert_tf_trade(pool: &Arc<Mutex<rusqlite::Connection>>, trade: TfTrade) -> u64{
        let mut conn = pool.lock().unwrap();
            let mut stmt = conn.prepare_cached(
                "INSERT INTO tf_trades (symbol, tf, timestamp, min_trade_time, max_trade_time, trades) VALUES (?1, ?2, ?3, ?4, ?5, ?6) RETURNING id",
            ).unwrap();
                let row = stmt.query_row::<u64, _, _>(params![
                    trade.symbol.symbol,
                    trade.tf,
                    trade.timestamp,
                    trade.min_trade_time,
                    trade.max_trade_time,
                    serde_json::to_string(&trade.trades).unwrap()
                ], |row| row.get(0)).unwrap();
        row
    }


    #[cfg(feature = "trades")]
    pub fn insert_tf_trades_to_entries(
        pool: &Arc<Mutex<rusqlite::Connection>>,
        trades: Vec<SQliteTfTradeToTradeEntry>,
    ) {
        let statement =
            "INSERT INTO tf_trades_to_entries (tf_trade_id, trade_entry_id, symbol, tf) VALUES "
                .to_string();
        for trades in trades.chunks(10000) {
            let mut rows = "".to_string();
            for trade in trades.to_owned() {
                rows += &format!(
                    "('{}','{}','{}','{}'),\n",
                    trade.tf_trade_id.to_string(),
                    trade.trade_entry_id.to_string(),
                    trade.symbol,
                    trade.tf
                )
            }
            let s = statement.clone() + &rows[..rows.len() - 2];
            let pool = pool.lock().unwrap();
            pool.execute_batch(&s).unwrap();
            // sqlx::raw_sql(&s).execute(pool).await.unwrap();
            drop(s)
        }
    }

    // Reset kline data by symbol
    #[cfg(feature = "trades")]
    pub async fn reset_kline(&self, symbol: &Symbol) {
        sqlx::query("DELETE FROM klines WHERE symbol = ?")
            .bind(&symbol.symbol)
            .execute(&self.pool)
            .await
            .unwrap();
    }
    // Reset trades data by symbol
    pub async fn reset_trades(&self, symbol: &Symbol) {
        sqlx::query("DELETE FROM trades WHERE symbol = ?")
            .bind(&symbol.symbol)
            .execute(&self.pool)
            .await
            .unwrap();
    }

    // Reset orders by symbol
    pub async fn reset_orders(&self, symbol: &Symbol) {
        sqlx::query("DELETE FROM orders WHERE symbol = ?")
            .bind(&symbol.symbol)
            .execute(&self.pool)
            .await
            .unwrap();
    }

    // Reset trades by symbol
    #[cfg(feature = "trades")]
    pub fn reset_trade_entries(conn: &Arc<Mutex<rusqlite::Connection>>, symbol: &Symbol) {
        let conn = conn.lock().unwrap();
        conn.execute("DELETE FROM trade_entries WHERE symbol = ?1", params![symbol.symbol]).unwrap();
    }
    // Reset trades by symbol
    pub async fn reset_tf_trades(&self, symbol: &Symbol) {
        sqlx::query("DELETE FROM tf_trades WHERE symbol = ?")
            .bind(&symbol.symbol)
            .execute(&self.pool)
            .await
            .unwrap();
    } // Reset trades by symbol
    #[cfg(feature = "trades")]
    pub async fn reset_tf_trades_to_entries_by_tf(&self, symbol: &Symbol, tf: u64) {
        sqlx::query("DELETE FROM tf_trades_to_entries WHERE symbol = ? AND tf = ?")
            .bind(&symbol.symbol)
            .bind(tf.to_string())
            .execute(&self.pool)
            .await
            .unwrap();
    }
    #[cfg(feature = "candles")]
    pub async fn reset_tf_trades_by_tf(&self, symbol: &Symbol, tf: &String) {
        sqlx::query("DELETE FROM tf_trades WHERE symbol = ? AND tf = ?")
            .bind(&symbol.symbol)
            .bind(tf)
            .execute(&self.pool)
            .await
            .unwrap();
    }
    #[cfg(feature = "trades")]
    pub async fn reset_tf_trades_by_tf(&self, symbol: &Symbol, tf: u64) {
        sqlx::query("DELETE FROM tf_trades WHERE symbol = ? AND tf = ?")
            .bind(&symbol.symbol)
            .bind(tf.to_string())
            .execute(&self.pool)
            .await
            .unwrap();
    }

    // vacuum
    pub async fn vacuum(&self) {
        sqlx::query("VACUUM;").execute(&self.pool).await.unwrap();
    }
}
