use crate::client::MongoClient;
use async_broadcast::Sender;
use async_std::sync::Arc;
use async_trait::async_trait;
use binance::api::Binance;
use binance::futures::market::FuturesMarket;
use binance::futures::model::AggTrade;
use binance::futures::model::AggTrades::AllAggTrades;
use binance_q_events::EventEmitter;
use binance_q_types::{AccessKey, GlobalConfig, Kline, TfTrade, TfTrades};
use binance_q_types::{Symbol, TradeEntry};
use chrono::prelude::*;
use csv::{Reader, ReaderBuilder, StringRecord};
use futures::TryStreamExt;
use mongodb::bson;
use mongodb::bson::doc;
use mongodb::options::{FindOneOptions, FindOptions};
use mongodb::results::InsertManyResult;
use reqwest::Client;
use rust_decimal::prelude::*;
use serde::Deserialize;
use std::fs::{read, read_to_string, File};
use std::io::{Read, Write};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::{Mutex, RwLock, Semaphore};
use tokio::task::JoinHandle;

pub fn to_tf_chunks(tf: u64, mut data: Vec<TradeEntry>) -> Vec<Vec<TradeEntry>> {
    if data.len() <= 0 {
        return vec![];
    }
    data.sort_by(|a, b| a.timestamp.cmp(&b.timestamp));

    let mut chunks = vec![];
    let mut timestamp = data[0].timestamp;
    let mut last_i = 0;
    for (i, trade) in data.clone().iter().enumerate() {
        if trade.timestamp - timestamp < tf * 1000 {
            continue;
        }
        timestamp = trade.timestamp;
        chunks.push(data[last_i..i].to_vec());
        last_i = i;
    }
    if chunks.len() <= 0 {
        chunks.push(data);
    }
    chunks
}

pub async fn insert_trade_entries(
    trades: &Vec<AggTrade>,
    symbol: Symbol,
    client: Arc<Mutex<MongoClient>>
) -> anyhow::Result<()> {
    let mut entries = vec![];
    for (i, t) in trades.iter().enumerate() {
        if i == 0 {
            continue;
        }
        let delta = ((trades[i].price - trades[i - 1].price) * 100.0) / trades[i - 1].price;
    
        let entry = TradeEntry {
            id: t.agg_id,
            price: t.price,
            qty: t.qty,
            timestamp: t.time,
            symbol: symbol.clone(),
            delta,
        };
        entries.push(entry);
    }
    let c = client.lock().await;
    c.trades.insert_many(entries, None).await;
    drop(c);
    Ok(())
}

#[derive(Deserialize)]
struct ArchiveKline {
    pub open_time: u64,
    pub open: Decimal,
    pub high: Decimal,
    pub low: Decimal,
    pub close: Decimal,
    pub volume: Decimal,
    pub close_time: u64,
    pub quote_volume: Decimal,
    pub count: u64,
    pub taker_buy_volume: Decimal,
    pub taker_buy_quote_volume: Decimal,
    pub ignore: u64,
}

pub async fn load_klines_from_archive(symbol: Symbol, tf: String, fetch_history_span: i64,) {
    let client = MongoClient::new().await;
    client
        .kline
        .delete_many(
            doc! {"symbol": bson::to_bson(&symbol.clone()).unwrap(), "tf": tf.clone()},
            None,
        )
        .await
        .unwrap();
    let mongo_client = Arc::new(Mutex::new(client));
    let today = chrono::DateTime::<Utc>::from(SystemTime::now());
    let tf = Arc::new(tf);
    let symbol = Arc::new(symbol);
    let permits = Arc::new(Semaphore::new(100));
    let dates = Arc::new(RwLock::new(vec![]));
    let inserts = Arc::new(RwLock::new(0));
    // initialize dates with 20 years of days if span is -1
    if fetch_history_span == -1 {
        let mut dates = dates.write().await;
        for i in 0..365 * 20 {
            let date = today - chrono::Duration::days(i);
            dates.push(date);
        }
    } else {
        let mut dates = dates.write().await;
        let today = SystemTime::now();
        let farthest = today - Duration::from_millis(fetch_history_span as u64);
        let farthest_date = DateTime::<Utc>::from(farthest);
        let todays_date = DateTime::<Utc>::from(today);
        let span = todays_date - farthest_date;
        let today = chrono::DateTime::<Utc>::from(SystemTime::now());
        for i in 0..span.num_days() {
            let date = today - chrono::Duration::days(i);
            dates.push(date);
        }
    }

    dates.write().await.reverse();
    let mut futures = vec![];
    'loader: loop {
        let permit = permits.clone().acquire_owned().await.unwrap();
        let date = dates.write().await.pop();
        if date.is_none() {
            break 'loader;
        }
        let tf = tf.clone();
        let symbol = symbol.clone();
        let date = date.unwrap();
        let mongo_client = mongo_client.clone();
        futures.push(tokio::spawn(async move {
            let mut dir = std::env::temp_dir();
            let date_str = date.format("%Y-%m-%d").to_string();
            let url = format!(
                "https://data.binance.vision/data/futures/um/daily/klines/{}/{}/{}-{}-{}.zip",
                symbol.symbol.clone(),
                tf,
                symbol.symbol.clone(),
                tf,
                date_str.clone()
            );
            let client = Client::new();
            let exists = client.head(&url).send().await.unwrap();
    
            if exists.status() != 200 {
                println!("[-] loader > {} trades for {} does not exist", symbol.symbol, url);
                drop(permit);
                return;
            }
            let res = client.get(&url).send().await;
            if let Ok(res) = res {
                let file_name = format!("{}-{}-{}.zip", symbol.symbol.clone(), tf, date_str);
                let file_path = dir.join(file_name.clone());
                let mut file = File::create(file_path.clone()).unwrap();
                let content = res.bytes().await;
                file.write_all(&content.unwrap()).unwrap();
                let mut archive = zip::ZipArchive::new(File::open(file_path).unwrap()).unwrap();
                archive.extract(std::env::temp_dir()).unwrap();
                let file_contents =
                    read_to_string(std::env::temp_dir().join(archive.by_index(0).unwrap().name()))
                        .unwrap();
                let mut sers: Vec<StringRecord> = vec![];
                let mut reader = csv::Reader::from_reader(file_contents.as_bytes());
                let mut trades = vec![];
                let header = reader.headers().unwrap().clone();
                if &header[0] == "open_time" {
                    let mut modified = reader
                        .records()
                        .map(|e| e.unwrap())
                        .collect::<Vec<StringRecord>>();
                    let f: Vec<StringRecord> = modified;
                    sers = f;
                } else {
                    let mut left = reader
                        .into_records()
                        .map(|e| e.unwrap())
                        .collect::<Vec<StringRecord>>();
                    left.splice(0..0, vec![header.clone()]);
                    sers = left;
                }
                for ser in sers {
                    let record: ArchiveKline = ser.deserialize(None).unwrap();
                    let symbol = Symbol {
                        symbol: symbol.symbol.clone(),
                        exchange: symbol.exchange.clone(),
                        base_asset_precision: symbol.base_asset_precision,
                        quote_asset_precision: symbol.quote_asset_precision,
                    };
                    let r = Kline {
                        open_time: record.open_time,
                        close_time: record.close_time,
                        quote_volume: record.quote_volume,
                        count: record.count,
                        taker_buy_volume: record.taker_buy_volume,
                        taker_buy_quote_volume: record.taker_buy_quote_volume,
                        symbol: symbol,
                        open: record.open,
                        close: record.close,
                        high: record.high,
                        low: record.low,
                        volume: record.volume,
                        ignore: record.ignore,
                    };
                    trades.push(r);
                }
               
                let c = mongo_client.lock().await;
                c.kline.insert_many(&trades, None).await.unwrap();
                drop(permit);
                drop(c);
    
            }
        }));
    }
    let futures = futures
        .into_iter()
        .filter(|f| !f.is_finished())
        .collect::<Vec<_>>();
    futures::future::join_all(futures).await;
}

pub async fn load_history_from_archive(symbol: Symbol, fetch_history_span: u64)  -> u64 {
    let client = MongoClient::new().await;
    client
          .trades
          .delete_many(
              doc! {"symbol": bson::to_bson(&symbol.clone()).unwrap()},
              None,
          )
          .await
          .unwrap();
    let today = SystemTime::now();
    let farthest = today - Duration::from_millis(fetch_history_span);
    let farthest_date = DateTime::<Utc>::from(farthest);
    let todays_date = DateTime::<Utc>::from(today);
    let span = todays_date - farthest_date;
    
    let mut dir = std::env::temp_dir();
    let mut start_time = (SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis()
        - 60 * 60 * 1000 * 24 * 2) as u64;
    
    let today = chrono::DateTime::<Utc>::from(SystemTime::now());
    let symbol = Arc::new(symbol);
    let permits = Arc::new(Semaphore::new(100));
    let dates = Arc::new(RwLock::new(vec![]));
    for i in 0..span.num_days() {
        let date = today - chrono::Duration::days(i);
        dates.write().await.push(date);
    }
    let mongo_client = Arc::new(Mutex::new(client));
    
    dates.write().await.reverse();
    let mut futures = vec![];
    
    'loader: loop {
        let permit = permits.clone().acquire_owned().await.unwrap();
        let date = dates.write().await.pop();
        if date.is_none() {
            break 'loader;
        }
        let symbol = symbol.clone();
        let date = date.unwrap();
        let mongo_client = mongo_client.clone();
        futures.push(tokio::spawn(async move {
            let mut dir = std::env::temp_dir();
            let date_str = date.format("%Y-%m-%d").to_string();
            let url = format!(
                "https://data.binance.vision/data/futures/um/daily/aggTrades/{}/{}-aggTrades-{}.zip",
                symbol.symbol.clone(),
                symbol.symbol.clone(),
                date_str.clone()
            );
            let client = Client::new();
            let exists = client.head(&url).send().await.unwrap();
            if exists.status() != 200 {
                println!("[-] loader > {} trades for {} does not exist", symbol.symbol, url);
                drop(permit);
                return;
            }
            let res = client.get(&url).send().await;
            if let Ok(res) = res {
                let file_name = format!("{}-aggTrades-{}.zip", symbol.symbol.clone(), date_str);
                let file_path = dir.join(file_name.clone());
                let mut file = File::create(file_path.clone()).unwrap();
                let content = res.bytes().await;
                file.write_all(&content.unwrap()).unwrap();
                let mut archive = zip::ZipArchive::new(File::open(file_path).unwrap()).unwrap();
                archive.extract(std::env::temp_dir()).unwrap();
                let file_contents =
                      read_to_string(std::env::temp_dir().join(archive.by_index(0).unwrap().name()))
                            .unwrap();
                let mut sers: Vec<StringRecord> = vec![];
                let mut reader = csv::Reader::from_reader(file_contents.as_bytes());
                let mut trades = vec![];
                let header = reader.headers().unwrap().clone();
                if &header[0] == "agg_trade_id" {
                    let mut modified = reader
                          .records()
                          .map(|e| e.unwrap())
                          .collect::<Vec<StringRecord>>();
                    let f: Vec<StringRecord> = modified;
                    sers = f;
                } else {
                    let mut left = reader
                          .into_records()
                          .map(|e| e.unwrap())
                          .collect::<Vec<StringRecord>>();
                    left.splice(0..0, vec![header.clone()]);
                    sers = left;
                }
                for ser in sers {
                    let record: ArchiveAggTrade = ser.deserialize(None).unwrap();
                    trades.push(AggTrade::from(record));
                }
                
                let symbol = Symbol {
                    symbol: symbol.symbol.clone(),
                    exchange: symbol.exchange.clone(),
                    base_asset_precision: symbol.base_asset_precision,
                    quote_asset_precision: symbol.quote_asset_precision,
                };
                
                insert_trade_entries(&trades, symbol, mongo_client.clone()).await;
                drop(permit);
            }
        }));
    }
    let futures = futures
          .into_iter()
          .filter(|f| !f.is_finished())
          .collect::<Vec<_>>();
    futures::future::join_all(futures).await;
    
    start_time
}
#[derive(Deserialize)]
struct ArchiveAggTrade {
    agg_trade_id: u64,
    price: f64,
    quantity: f64,
    first_trade_id: u64,
    last_trade_id: u64,
    transact_time: u64,
    is_buyer_maker: bool,
}

impl From<ArchiveAggTrade> for AggTrade {
    fn from(a: ArchiveAggTrade) -> Self {
        AggTrade {
            agg_id: a.agg_trade_id,
            price: a.price,
            qty: a.quantity,
            first_id: a.first_trade_id,
            last_id: a.last_trade_id,
            time: a.transact_time,
            maker: a.is_buyer_maker,
        }
    }
}

// TODO: need to get recent history also for when we were loading
pub async fn load_history(key: AccessKey, symbol: Symbol, fetch_history_span: u64, zip_only: bool) {
    let market = FuturesMarket::new(Some(key.api_key.clone()), Some(key.secret_key.clone()));
    let mut span = fetch_history_span;
    if fetch_history_span > 24 * 60 * 60 * 2 * 1000 {
        println!("[?] loader > Using zip download for history longer than 48h {}", symbol.symbol);
        let last_time = load_history_from_archive(symbol.clone(), fetch_history_span).await;
        span = (SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
            - last_time);
    }
    if zip_only {
        return;
    }

    let starting_time = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();
    let mut start_time = starting_time as u64 - span;
    let mongo_client = Arc::new(Mutex::new(MongoClient::new().await));
    
    loop {
        
        println!(
            "Fetching history from {} to {}",
            chrono::prelude::Local
                .timestamp_millis_opt(start_time as i64)
                .unwrap()
                .time()
                .to_string(),
            chrono::prelude::Local
                .timestamp_millis_opt(starting_time as i64)
                .unwrap()
                .time()
                .to_string()
        );
        if start_time > starting_time as u64 {
            break;
        }
        let trades_result = market
            .get_agg_trades(
                symbol.symbol.clone(),
                None,
                Some(start_time),
                None,
                Some(1000),
            )
            .await;
        if let Ok(t) = trades_result {
            match t {
                AllAggTrades(trades) => {
                    if trades.len() <= 2 {
                        continue;
                    }
                    if let Ok(_) = insert_trade_entries(&trades, symbol.clone(), mongo_client.clone()).await {
                        start_time = trades.last().unwrap().time + 1;
                    }
                }
            }
        }
    }
}

// TODO: use websockets for this
pub async fn start_loader(key: AccessKey, symbol: Symbol, tf1: u64) -> JoinHandle<()> {
    let market = FuturesMarket::new(Some(key.api_key.clone()), Some(key.secret_key.clone()));
    tokio::spawn(async move {
        let mut last_id = None;
        let mut start_time = Some(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64
                - tf1,
        );
        let mongo_client = Arc::new(Mutex::new(MongoClient::new().await));
    
        loop {
            let trades_result = market
                .get_agg_trades(&symbol.symbol, last_id, start_time, None, Some(1000))
                .await;
            if let Ok(t) = trades_result {
                match t {
                    AllAggTrades(trades) => {
                        if trades.len() <= 2 {
                            continue;
                        }
                        if let Ok(_) = insert_trade_entries(&trades, symbol.clone(), mongo_client.clone()).await {
                            last_id = Some(trades.last().unwrap().agg_id + 1);
                            // println!("{}: {} {:?}", tf1, first_id.agg_id,  last_id.unwrap());
                        }
                    }
                }
            }
            if start_time.is_some() {
                start_time = None;
            }
            tokio::time::sleep(Duration::from_secs(tf1)).await;
        }
    })
}



pub struct TfTradeEmitter {
    subscribers: Arc<RwLock<Sender<TfTrades>>>,
    pub tf: u64,
    global_config: GlobalConfig,
}

pub struct KlineEmitter {
    subscribers: Arc<RwLock<Sender<TfTrades>>>,
    pub tf: String,
    global_config: GlobalConfig,
}

impl KlineEmitter {
    pub fn new(tf: String, global_config: GlobalConfig) -> Self {
        Self {
            subscribers: Arc::new(RwLock::new(async_broadcast::broadcast(1).0)),
            tf,
            global_config,
        }
    }
}


impl TfTradeEmitter {
    pub fn new(tf: u64, global_config: GlobalConfig) -> Self {
        Self {
            subscribers: Arc::new(RwLock::new(async_broadcast::broadcast(1).0)),
            tf,
            global_config,
        }
    }

    pub async fn get_tf_trades_until(
        &self,
        until: u64,
        limit: u64,
    ) -> anyhow::Result<Vec<TfTrade>> {
        let mongo_client = MongoClient::new().await;
        let trades = mongo_client
            .tf_trades
            .find(
                doc! {"tf": bson::to_bson(&self.tf)?, "id": {"$lte": bson::to_bson(&until)?}},
                FindOptions::builder()
                    .sort(doc! {"id": -1})
                    .limit(limit as i64)
                    .build(),
            )
            .await?;
        let trades = trades.try_collect::<TfTrades>().await?;
        Ok(trades)
    }

    pub async fn log_history(&self, symbol: Symbol) {
        let mut last_id = 1;
        let mongo_client = MongoClient::new().await;
        if let Ok(t) = mongo_client
            .trades
            .find(
                doc! {
                    "timestamp": {
                        "$gt": mongodb::bson::to_bson(&0).unwrap(),
                    },
                    "symbol": bson::to_bson(&symbol.clone()).unwrap()
                },
                None,
            )
            .await
        {
            let trades = t.try_collect().await.unwrap_or_else(|_| vec![]);
            let tf_trades = to_tf_chunks(self.tf, trades)
                .iter()
                .map(|t| {
                    let trades = t.clone();
                    let trade = TfTrade {
                        id: last_id,
                        tf: self.tf,
                        symbol: self.global_config.symbol.clone(),
                        timestamp: trades
                            .iter()
                            .max_by(|a, b| a.timestamp.cmp(&b.timestamp))
                            .unwrap()
                            .timestamp,
                        trades,
                    };
                    last_id += 1;
                    trade
                })
                .collect::<Vec<_>>();
            if !tf_trades.is_empty() {
                mongo_client
                    .tf_trades
                    .insert_many(tf_trades.clone(), None)
                    .await
                    .unwrap();
            }
        }
    }
}


#[async_trait]
impl EventEmitter<TfTrades> for TfTradeEmitter {
    fn get_subscribers(&self) -> Arc<RwLock<Sender<TfTrades>>> {
        self.subscribers.clone()
    }

    async fn emit(&self) -> anyhow::Result<JoinHandle<()>> {
        let mongo_client = MongoClient::new().await;
        let last_entry = mongo_client
            .tf_trades
            .find_one(
                doc! {"tf": bson::to_bson(&self.tf)?},
                FindOneOptions::builder().sort(doc! {"id": -1}).build(),
            )
            .await?;
        let mut last_timestamp = 0_u64;
        let mut last_id = 1_u64;
        if let Some(t) = last_entry {
            last_timestamp = t.timestamp;
            last_id = t.id + 1;
        }
        let tf = self.tf;
        let subscribers = self.subscribers.clone();
        let config = self.global_config.clone();
        Ok(tokio::spawn(async move {
            loop {
                if let Ok(t) = mongo_client
                    .trades
                    .find(
                        doc! {
                            "timestamp": {
                                "$gt": mongodb::bson::to_bson(&last_timestamp).unwrap()
                            }
                        },
                        None,
                    )
                    .await
                {
                    let trades = t.try_collect().await.unwrap_or_else(|_| vec![]);
                    let tf_trades = to_tf_chunks(tf, trades)
                        .iter()
                        .map(|t| {
                            let trades = t.clone();
                            let trade = TfTrade {
                                id: last_id,
                                tf: tf,
                                symbol: config.symbol.clone(),
                                timestamp: std::time::SystemTime::now()
                                    .duration_since(std::time::UNIX_EPOCH)
                                    .unwrap()
                                    .as_secs(),
                                trades,
                            };
                            last_id += 1;
                            trade
                        })
                        .collect::<Vec<_>>();
                    if !tf_trades.is_empty() {
                        mongo_client
                            .tf_trades
                            .insert_many(tf_trades.clone(), None)
                            .await
                            .unwrap();
                    }
                    if tf_trades.len() > 0 {
                        last_timestamp = tf_trades.last().unwrap().trades.last().unwrap().timestamp;
                        match subscribers.read().await.broadcast(tf_trades.clone()).await {
                            Ok(_) => {}
                            Err(e) => {
                                eprintln!("Error broadcasting tf trades {:?}", e)
                            }
                        }
                    }
                }
                tokio::time::sleep(std::time::Duration::from_secs(tf)).await;
            }
        }))
    }
}
