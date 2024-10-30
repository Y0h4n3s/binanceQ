use std::str::FromStr;
use std::sync::{Arc, Mutex};
use futures::stream::BoxStream;
use rusqlite::ToSql;
use rust_decimal::Decimal;
use rust_decimal::prelude::FromPrimitive;
use sqlx::{FromRow, Decode, SqlitePool, Executor, Type, Row, Error, Acquire, SqliteConnection};
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqliteRow, SqliteSynchronous};
use crate::types::{Kline, Order, OrderType, Side, Symbol, TfTrade, TfTrades, Trade, TradeEntry};
use serde::{Serialize, Deserialize};
use sqlx::types::Text;
use uuid::Uuid;

#[derive(Clone)]
pub struct SQLiteClient {
    pub pool: SqlitePool,
    pub conn: Arc<Mutex<rusqlite::Connection>>,
}


#[derive(Serialize, Deserialize, Debug, Clone, FromRow, Decode, Type)]
pub struct SQLiteTradeEntry {
    pub price: String,
    pub qty: String,
    pub timestamp: String,
    pub delta: String,
    pub symbol: String,
    pub symbol_data: serde_json::Value,
}
#[derive(Serialize, Deserialize, Debug, Clone, FromRow, Decode)]
pub struct SQLiteTfTrade {
    pub symbol: String,
    pub symbol_data: serde_json::Value,
    pub tf: i64,
    pub timestamp: String,
    pub min_trade_time: String,
    pub max_trade_time: String,
    pub trades: serde_json::Value
}

#[derive(Serialize, Deserialize, Debug, Clone, FromRow, Decode)]
pub struct SQLiteKline {
    pub symbol: String,
    pub symbol_data: serde_json::Value,
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

#[derive(Serialize, Deserialize, Debug, Clone, FromRow, Decode)]
pub struct SQLiteTrade {
    pub id: u64,
    pub order_id: Uuid,
    pub symbol: String,
    pub symbol_data: serde_json::Value,

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
    pub symbol_data: serde_json::Value,
    pub side: String,
    pub price: String,
    pub quantity: String,
    pub time: u64,
    pub order_type: serde_json::Value,
    pub lifetime: u64,
    pub close_policy: serde_json::Value,
}

impl From<TradeEntry> for SQLiteTradeEntry {
    fn from(trade: TradeEntry) -> Self {
        SQLiteTradeEntry {
            price: trade.price.to_string(),
            qty: trade.qty.to_string(),
            timestamp: trade.timestamp.to_string(),
            delta: trade.delta.to_string(),
            symbol_data: serde_json::to_value(&trade.symbol).unwrap(),
            symbol: trade.symbol.symbol,
        }
    }
}

impl FromRow<'_, SqliteRow> for TradeEntry {
    fn from_row(row: &SqliteRow) -> Result<TradeEntry, sqlx::Error> {
        let price: f64 = row.try_get("price")?;
        let qty: f64 = row.try_get("qty")?;
        let delta: f64 = row.try_get("delta")?;
        Ok(TradeEntry {
            trade_id: row.try_get("id")?,
            price: Decimal::from_f64(price).unwrap(),
            qty: Decimal::from_f64(qty).unwrap(),
            timestamp: row.try_get("timestamp")?,
            delta: Decimal::from_f64(delta).unwrap(),
            symbol:serde_json::from_value(row.try_get("symbol_data")?).unwrap(),
        })
    }
}
impl From<TfTrade> for SQLiteTfTrade {
    fn from(tf_trade: TfTrade) -> Self {
        SQLiteTfTrade {
            symbol_data: serde_json::to_value(&tf_trade.symbol).unwrap(),
            symbol: tf_trade.symbol.symbol,
            tf: tf_trade.tf as i64,
            timestamp: tf_trade.timestamp.to_string(),
            max_trade_time: tf_trade.max_trade_time.to_string(),
            min_trade_time: tf_trade.min_trade_time.to_string(),
            trades: serde_json::to_value(&tf_trade.trades).unwrap(),
        }
    }
}

impl FromRow<'_, SqliteRow> for TfTrade {
    fn from_row(row: &'_ SqliteRow) -> Result<Self, Error> {
        Ok(Self {
            symbol:serde_json::from_value(row.try_get("symbol_data")?).unwrap(),
            tf: row.try_get("tf")?,
            id: row.try_get("id")?,
            timestamp: row.try_get("timestamp")?,
            min_trade_time: row.try_get("min_trade_time")?,
            max_trade_time: row.try_get("max_trade_time")?,
            trades: serde_json::from_value(row.try_get("trades")?).unwrap(),
        })
    }
}


impl From<Kline> for SQLiteKline {
    fn from(kline: Kline) -> Self {
        SQLiteKline {
            symbol_data: serde_json::to_value(&kline.symbol).unwrap(),
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

        Ok(Self {
            id: row.try_get("id")?,
            order_id: row.try_get("order_id")?,
            symbol:serde_json::from_value(row.try_get("symbol_data")?).unwrap(),
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
            realized_pnl:  Decimal::from_f64(realized_pnl).unwrap(),
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
        Ok(Self {
            symbol:serde_json::from_value(row.try_get("symbol_data")?).unwrap(),
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
            symbol_data: serde_json::to_value(&trade.symbol).unwrap(),
            symbol: trade.symbol.symbol,
            maker: trade.maker,
            price: trade.price.to_string(),
            commission: trade.commission.to_string(),
            position_side: format!("{:?}", trade.position_side),  // Store enum as TEXT
            side: format!("{:?}", trade.side),  // Store enum as TEXT
            realized_pnl: trade.realized_pnl.to_string(),
            exit_order_type: serde_json::to_value(trade.exit_order_type).unwrap(),  // Store enum as TEXT
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
            symbol_data: serde_json::to_value(&order.symbol).unwrap(),
            symbol: order.symbol.symbol,
            side: format!("{:?}", order.side),
            price: order.price.to_string(),
            quantity: order.quantity.to_string(),
            time: order.time,
            order_type:serde_json::to_value(order.order_type).unwrap(),
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
        Ok( Order {
            id: serde_json::from_value( row.try_get("order_id")?).unwrap(),
            symbol:serde_json::from_value(row.try_get("symbol_data")?).unwrap(),

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
        let options = SqliteConnectOptions::from_str("sqlite://binance_studies.db").unwrap()
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal);
        let pool = SqlitePool::connect_with(options).await.unwrap();

        SQLiteClient::create_tables(&pool).await;
        let connection = rusqlite::Connection::open("./binance_studies.db").unwrap();
        connection.execute("PRAGMA temp_store = MEMORY;", ()).unwrap();
        connection.execute("PRAGMA shrink_memory;", ()).unwrap();
        connection.execute("PRAGMA synchronous = OFF;", ()).unwrap();
        SQLiteClient {
            pool,
            conn: Arc::new(Mutex::new(connection)),
        }
    }
    pub async fn create_tables(pool: &SqlitePool) {
        // Create the TradeEntry table
        sqlx::query(
            r#"
        CREATE TABLE IF NOT EXISTS trade_entries (
            id INTEGER PRIMARY KEY,
            price REAL,
            qty REAL,
            timestamp INTEGER,
            delta REAL,
            symbol TEXT,
            symbol_data JSON
        );
        "#
        ).execute(pool).await.unwrap();

        // Create the TfTrade table
        sqlx::query(
            r#"
        CREATE TABLE IF NOT EXISTS tf_trades (
            id INTEGER PRIMARY KEY,
            symbol TEXT,
            tf INTEGER,
            timestamp INTEGER,
            min_trade_time INTEGER,
            max_trade_time INTEGER,
            symbol_data JSON,
            trades JSON
        );
        "#
        ).execute(pool).await.unwrap();




        // Create the Order table
        sqlx::query(
            r#"
        CREATE TABLE IF NOT EXISTS orders (
            id TEXT PRIMARY KEY,
            order_id JSON,
            symbol TEXT,
            symbol_data JSON,
            side TEXT,
            price REAL,
            quantity REAL,
            time INTEGER,
            order_type JSON,
            lifetime INTEGER,
            close_policy JSON
        );
        "#
        ).execute(pool).await.unwrap();

        // Create the Kline table
        sqlx::query(
            r#"
        CREATE TABLE IF NOT EXISTS klines (
            symbol TEXT,
            symbol_data JSON,
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
        "#
        ).execute(pool).await.unwrap();

        // Create the Trade table
        sqlx::query(
            r#"
        CREATE TABLE IF NOT EXISTS trades (
            id INTEGER PRIMARY KEY,
            order_id TEXT,
            symbol TEXT,
            symbol_data JSON,
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
        "#
        ).execute(pool).await.unwrap();
        sqlx::query(
            r#"CREATE INDEX IF NOT EXISTS timestamps ON trade_entries(timestamp);"#
        ).execute(pool).await.unwrap();
        sqlx::query(
            r#"CREATE INDEX IF NOT EXISTS tf_timestamps ON tf_trades(timestamp);"#
        ).execute(pool).await.unwrap();
        sqlx::query(
            r#"CREATE INDEX IF NOT EXISTS tf_symbols ON tf_trades(symbol);"#
        ).execute(pool).await.unwrap();
        sqlx::query(
            r#"CREATE INDEX IF NOT EXISTS tf_symbols_timestamps ON tf_trades(symbol, timestamp);"#
        ).execute(pool).await.unwrap();
        sqlx::query(
            r#"CREATE INDEX IF NOT EXISTS kline_symbols ON klines(symbol);"#
        ).execute(pool).await.unwrap();
        sqlx::query(
            r#"CREATE INDEX IF NOT EXISTS kline_close_times ON klines(close_time);"#
        ).execute(pool).await.unwrap();

    }

    pub async fn insert_order(pool: &SqlitePool, order: Order) {
        let s_trade = SQLiteOrder::from(order);
        sqlx::query(
            r#"
                INSERT INTO orders (order_id, symbol, symbol_data, side, price, quantity, time, order_type, lifetime, close_policy) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            "#
        ).bind(&s_trade.order_id)
            .bind(s_trade.symbol)
            .bind(s_trade.symbol_data)
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
                INSERT INTO trades (order_id, symbol, symbol_data, maker, price, commission, position_side, side, realized_pnl, exit_order_type, qty, quote_qty, time) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            "#
        ).bind(&s_trade.order_id)
            .bind(s_trade.symbol)
            .bind(s_trade.symbol_data)
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
    pub async fn get_trades_count_by_symbol<'a>(pool: &'a SqlitePool, symbol: &'a Symbol) -> u64 {
        sqlx::query_scalar(
            r#"
                SELECT COUNT(*) FROM trade_entries WHERE symbol = ?;
            "#
        ).bind(&symbol.symbol)
            .fetch_one(pool)
            .await
            .unwrap()
    }

    pub async fn get_oldest_trade<'a>(pool: &'a SqlitePool, symbol: &'a Symbol) -> TradeEntry {
        sqlx::query_as(
            r#"
                SELECT * FROM trade_entries WHERE symbol = ? ORDER BY timestamp ASC LIMIT 1;
            "#
        ).bind(&symbol.symbol)
            .fetch_one(pool)
            .await
            .unwrap()
    }

    pub async fn get_latest_trade<'a>(pool: &'a SqlitePool, symbol: &'a Symbol) -> TradeEntry {
        sqlx::query_as(
            r#"
                SELECT * FROM trade_entries WHERE symbol = ? ORDER BY timestamp DESC LIMIT 1;
            "#
        ).bind(&symbol.symbol)
            .fetch_one(pool)
            .await.unwrap()

    }

    pub fn get_kline_stream<'a>(pool: &'a SqlitePool, symbol: Symbol, until: String) -> BoxStream<'a, Result<Kline, Error>> {
        sqlx::query_as(
            r#"
                SELECT * FROM klines WHERE symbol = ? AND close_time > ? ORDER BY close_time ASC
            "#
        ).bind(symbol.symbol)
            .bind(until)
            .fetch(pool)

    }

    pub fn get_tf_trades_stream<'a>(pool: &'a SqlitePool, symbol: &'a Symbol, until: String) -> BoxStream<'a, Result<TfTrade, Error>> {
        sqlx::query_as(
            r#"
                SELECT * FROM tf_trades WHERE symbol = ? AND timestamp > ? ORDER BY timestamp ASC
            "#
        ).bind(&symbol.symbol)
            .bind(until)
            .fetch(pool)

    }
    pub async fn select_values_between_min_max(
        pool: &SqlitePool,
        min: String,
        max: String,
        
    ) -> Vec<TradeEntry>
    {
        let values = sqlx::query_as(
        r#"
        SELECT * FROM trade_entries
        WHERE timestamp BETWEEN ? AND ? ORDER BY timestamp ASC;
        "#,
    ).bind(min)
            .bind(max)
            .fetch_all(pool)
            .await
            .unwrap();

        values
    }

    // Insert a trade entry
    pub fn insert_klines(pool: &Arc<Mutex<rusqlite::Connection>>, klines: Vec<Kline>) {
        let statement = "INSERT INTO klines (symbol, symbol_data, open_time, open, high, low, close, volume, close_time, quote_volume, count, taker_buy_volume, taker_buy_quote_volume, ignore) VALUES ".to_string();
        for klines in klines.chunks(10000) {
            let mut rows = "".to_string();
            for kline in klines.to_owned() {
                let s_kline = SQLiteKline::from(kline);

                rows += &format!("('{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}'),\n",
                    s_kline.symbol,
                    s_kline.symbol_data,
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
            let s = statement.clone() + &rows[..rows.len()-2];
            let pool = pool.lock().unwrap();
            pool.execute(&s, ()).unwrap();
            drop(s)
        }
    }
    pub fn insert_trade_entries(pool: &Arc<Mutex<rusqlite::Connection>>, trade_entries: &Vec<SQLiteTradeEntry>) {
        let statement = "INSERT INTO trade_entries (price, qty, timestamp, delta, symbol, symbol_data) VALUES ".to_string();
        for trade_entries in trade_entries.chunks(10000) {
            let mut rows = "".to_string();
            for s_entry in trade_entries {
                rows += &format!("('{}','{}','{}','{}','{}','{}'),\n", s_entry.price, s_entry.qty, s_entry.timestamp, s_entry.delta, s_entry.symbol, s_entry.symbol_data)
            }
            let s = statement.clone() + &rows[..rows.len()-2];
            let pool = pool.lock().unwrap();
            pool.execute(&s, ()).unwrap();
            drop(s)
        }
    }
    
    pub async fn insert_trade_entry(pool: &SqlitePool, trade_entry: TradeEntry) {

        let s_entry: SQLiteTradeEntry = trade_entry.into();
        sqlx::query(
            r#"
        INSERT INTO trade_entries (price, qty, timestamp, delta, symbol, symbol_data)
        VALUES (?, ?, ?, ?, ?, ?);
        "#
        )
            .bind(s_entry.price)
            .bind(s_entry.qty)
            .bind(s_entry.timestamp)
            .bind(s_entry.delta)
            .bind(s_entry.symbol)
            .bind(s_entry.symbol_data)
            .execute(pool)
            .await
            .unwrap();
    }
    pub async fn get_trade_entry(pool: &SqlitePool, trade_entry: TradeEntry) {

        let s_entry: SQLiteTradeEntry = trade_entry.into();
        sqlx::query_as::<_, TradeEntry>(
            r#"
        SELECT * FROM trade_entries LIMIT 1;
        "#
        )
            .fetch_one(pool)
            .await
            .unwrap();
    }
    pub fn insert_tf_trades(pool: &Arc<Mutex<rusqlite::Connection>>, trades: TfTrades) {

        let statement = "INSERT INTO tf_trades (symbol, tf, timestamp, min_trade_time, max_trade_time, symbol_data, trades) VALUES ".to_string();
        for trades  in trades .chunks(10000) {
            let mut rows = "".to_string();
            for trade in trades.to_owned() {
                let s_trade: SQLiteTfTrade = trade.into();
                rows += &format!("('{}',{},'{}','{}','{}','{}', '{}'),\n", s_trade.symbol, s_trade.tf, s_trade.timestamp, s_trade.min_trade_time, s_trade.max_trade_time, s_trade.symbol_data, s_trade.trades)
            }
            let s = statement.clone() + &rows[..rows.len()-2];
            let pool = pool.lock().unwrap();
            pool.execute(&s, ()).unwrap();
            // sqlx::raw_sql(&s).execute(pool).await.unwrap();
            drop(s)
        }




    }



    // Reset kline data by symbol
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
    pub async fn reset_trade_entries(&self, symbol: &Symbol) {
        sqlx::query("DELETE FROM trade_entries WHERE symbol = ?")
            .bind(&symbol.symbol)
            .execute(&self.pool)
            .await
            .unwrap();
    }
    // Reset trades by symbol
    pub async fn reset_tf_trades(&self, symbol: &Symbol) {
        sqlx::query("DELETE FROM tf_trades WHERE symbol = ?")
            .bind(&symbol.symbol)
            .execute(&self.pool)
            .await
            .unwrap();
    }    // Reset trades by symbol
    pub async fn reset_tf_trades_by_tf(&self, symbol: &Symbol, tf: u64) {
        sqlx::query("DELETE FROM tf_trades WHERE symbol = ? AND tf = ?")
            .bind(&symbol.symbol)
            .bind(tf.to_string())
            .execute(&self.pool)
            .await
            .unwrap();
    }// vacuum
    pub async fn vacuum(&self) {
        sqlx::query("VACUUM;")
            .execute(&self.pool)
            .await
            .unwrap();
    }
}
