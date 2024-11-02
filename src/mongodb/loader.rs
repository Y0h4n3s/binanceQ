use crate::events::EventEmitter;
use crate::mongodb::client::MongoClientSync;
use crate::mongodb::MongoClient;
use crate::types::{AccessKey, GlobalConfig, Kline, TfTrade, TfTrades};
use crate::types::{Symbol, TradeEntry};
use async_broadcast::Sender;
use async_std::sync::Arc;
use async_trait::async_trait;
use binance::api::Binance;
use binance::futures::market::FuturesMarket;
use binance::futures::model::AggTrade;
use binance::futures::model::AggTrades::AllAggTrades;
use binance::model::KlineSummaries::AllKlineSummaries;
use chrono::prelude::*;
use csv::StringRecord;
use futures::stream::{self, StreamExt};
use futures::TryStreamExt;
use indicatif::ProgressBar;
use mongodb::bson;
use mongodb::bson::doc;
use mongodb::options::{FindOneOptions, FindOptions, InsertManyOptions};
use rayon::prelude::*;
use rust_decimal::prelude::*;
use serde::Deserialize;
use std::cmp::Ordering;
use std::io::Read;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tempdir::TempDir;
use tokio::sync::{Notify, RwLock};
use tokio::task::JoinHandle;
use tokio_retry::strategy::ExponentialBackoff;
use tokio_retry::RetryIf;
use tracing::{debug, error, info, warn};

pub fn to_tf_chunks(tf: u64, data: &Vec<TradeEntry>) -> Vec<&[TradeEntry]> {
    if data.len() <= 0 {
        return vec![];
    }

    let mut chunks = vec![];
    let mut timestamp = data[0].timestamp;
    if data[data.len() - 1].timestamp - timestamp < tf * 1000 {
        return vec![data];
    }
    let mut last_i = 0;
    for (i, trade) in data.iter().enumerate() {
        if trade.timestamp - timestamp < tf * 1000 {
            continue;
        }
        timestamp = trade.timestamp;
        chunks.push(&data[last_i..i]);
        last_i = i;
    }
    if chunks.len() <= 0 {
        chunks.push(data);
    }
    chunks
}

pub fn insert_trade_entries(
    trades: &Vec<AggTrade>,
    symbol: &Symbol,
    client: Arc<MongoClientSync>,
    tf: Arc<u64>,
) -> anyhow::Result<()> {
    if trades.is_empty() {
        return Ok(());
    }
    let mut entries = Vec::with_capacity(trades.len() - 1);
    for (i, t) in trades.iter().enumerate().skip(1) {
        let delta = ((trades[i].price - trades[i - 1].price) * 100.0) / trades[i - 1].price;

        let entry = TradeEntry {
            id: uuid::Uuid::new_v4().to_string(),
            price: Decimal::from_f64(t.price).unwrap(),
            qty: Decimal::from_f64(t.qty).unwrap(),
            timestamp: t.time,
            symbol: symbol.symbol.clone(),
            delta: Decimal::from_f64(delta).unwrap(),
        };
        entries.push(entry);
    }
    let chunks = to_tf_chunks(*tf, &entries);

    let mut tf_trades = Vec::with_capacity(chunks.len());
    for chunk in chunks {
        let min = chunk
            .iter()
            .max_by(|x, y| {
                if x.price > y.price {
                    Ordering::Less
                } else {
                    Ordering::Greater
                }
            })
            .unwrap()
            .clone();
        let max = chunk
            .iter()
            .max_by(|x, y| {
                if x.price > y.price {
                    Ordering::Greater
                } else {
                    Ordering::Less
                }
            })
            .unwrap()
            .clone();

        let tf_trade = TfTrade {
            symbol: symbol.clone(),
            tf: *tf,
            id: uuid::Uuid::new_v4().to_string(),
            timestamp: chunk[0].timestamp,
            trades: chunk.to_vec(),
            max_trade_time: max.timestamp,
            min_trade_time: min.timestamp,
        };
        tf_trades.push(tf_trade);
    }

    for chunk in tf_trades.chunks(4000) {
        client.tf_trades.insert_many(
            chunk,
            Some(
                InsertManyOptions::builder()
                    .ordered(false)
                    .bypass_document_validation(Some(true))
                    .build(),
            ),
        )?;
    }

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

fn is_leap_year(y: u32) -> bool {
    y % 4 == 0 && (y % 100 != 0 || y % 400 == 0)
}

/// Return the number of days in the month `m` in year `y` in the Gregorian calendar. Note that
/// The month number is zero based, i.e. `m=0` corresponds to January, `m=1` to February, etc.
fn days_per_month(y: u32, m: u32) -> u32 {
    const DAYS_PER_MONTH: [u32; 12] = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
    let nd = DAYS_PER_MONTH[m as usize];
    nd + (m == 1 && is_leap_year(y)) as u32
}
fn datediff<T>(date0: T, date1: T) -> (u32, u32, u32, bool)
where
    T: chrono::Datelike + PartialOrd,
{
    if date1 < date0 {
        let (ny, nm, nd, _) = datediff(date1, date0);
        (ny, nm, nd, true)
    } else {
        let (y0, m0, mut d0) = (date0.year() as u32, date0.month0(), date0.day0());
        let (mut y1, mut m1, mut d1) = (date1.year() as u32, date1.month0(), date1.day0());

        if d0 > d1 {
            let (py1, pm1) = if m1 == 0 { (y1 - 1, 11) } else { (y1, m1 - 1) };
            let pnd = days_per_month(py1, pm1);
            d0 = d0.min(pnd - 1);
            if d0 > d1 {
                y1 = py1;
                m1 = pm1;
                d1 += pnd;
            }
        }
        if m0 > m1 {
            y1 -= 1;
            m1 += 12;
        }

        (y1 - y0, m1 - m0, d1 - d0, false)
    }
}

pub async fn load_klines_from_archive(
    symbol: Symbol,
    tf: String,
    fetch_history_span: i64,
    pb: ProgressBar,
    verbose: bool,
) {
    let client = MongoClient::new().await;
    client
        .kline
        .delete_many(
            doc! {"symbol": bson::to_bson(&symbol.clone()).unwrap()},
            None,
        )
        .await
        .unwrap();
    let mongo_client = Arc::new(client);
    let today = chrono::DateTime::<Utc>::from(SystemTime::now());
    let tf = Arc::new(tf);
    let symbol = Arc::new(symbol);
    let pb = Arc::new(pb);
    let months = if fetch_history_span == -1 {
        (0..(12 * 20))
            .map(|i| today - chrono::Duration::weeks(4 * i))
            .collect::<Vec<_>>()
    } else {
        let span = ((fetch_history_span as f64) / (4.0 * 7.0 * 24.0 * 3600.0)).ceil() as i64;
        (0..span)
            .map(|i| today - chrono::Duration::weeks(4 * i))
            .collect::<Vec<_>>()
    };

    if !verbose {
        pb.inc_length(months.len() as u64);
    }
    let concurrency_limit = num_cpus::get_physical();

    stream::iter(months)
        .map(|month| {
            let tf = tf.clone();
            let symbol = symbol.clone();
            let mongo_client = mongo_client.clone();
            let pb = pb.clone();
            async move {
                let month_str = month.format("%Y-%m").to_string();
                let url = format!(
                    "https://data.binance.vision/data/spot/monthly/klines/{}/{}/{}-{}-{}.zip",
                    symbol.symbol, tf, symbol.symbol, tf, month_str
                );
                if verbose {
                    debug!("[+] fetching {}", url);
                }
                let retry_strategy = ExponentialBackoff::from_millis(100).take(3);

                let res = RetryIf::spawn(
                    retry_strategy,
                    || reqwest::get(&url),
                    |e: &reqwest::Error| e.is_connect() || e.is_timeout(),
                )
                .await;
                if let Ok(res) = res {
                    if !res.status().is_success() {
                        warn!("[-] Failed to fetch {}. Reason: {}", url, res.status());
                        return;
                    }
                    if let Ok(bytes) = res.bytes().await {
                        let buf = bytes.to_vec();
                        let reader = std::io::Cursor::new(buf);
                        let mut archive = zip::ZipArchive::new(reader).unwrap();
                        let mut file_contents = String::new();
                        archive
                            .by_index(0)
                            .unwrap()
                            .read_to_string(&mut file_contents)
                            .expect("failed to read archive contents");
                        let mut reader = csv::Reader::from_reader(file_contents.as_bytes());

                        reader.set_headers(StringRecord::from(vec![
                            "open_time",
                            "open",
                            "high",
                            "low",
                            "close",
                            "volume",
                            "close_time",
                            "quote_volume",
                            "count",
                            "taker_buy_volume",
                            "taker_buy_quote_volume",
                            "ignore",
                        ]));
                        let mut trades = vec![];

                        for record in reader
                            .deserialize::<ArchiveKline>()
                            .filter_map(|result| result.ok())
                        {
                            let symbol: Symbol = (*symbol).clone();
                            let r = Kline {
                                open_time: record.open_time,
                                close_time: record.close_time,
                                quote_volume: record.quote_volume,
                                count: record.count,
                                taker_buy_volume: record.taker_buy_volume,
                                taker_buy_quote_volume: record.taker_buy_quote_volume,
                                symbol,
                                open: record.open,
                                close: record.close,
                                high: record.high,
                                low: record.low,
                                volume: record.volume,
                                ignore: record.ignore,
                            };
                            trades.push(r);
                        }

                        if trades.is_empty() {
                            info!("[-] No Kline found");
                        } else {
                            mongo_client.kline.insert_many(&trades, None).await.unwrap();
                        }
                    } else if verbose {
                        warn!("[-] failed to deserialize {}", url);
                    }
                } else if verbose {
                    warn!("[-] failed to fetch {}", url);
                }
                if !verbose {
                    pb.inc(1);
                }
            }
        })
        .buffer_unordered(concurrency_limit)
        .collect::<Vec<_>>()
        .await;
}

pub fn load_history_from_archive(
    symbol: Symbol,
    fetch_history_span: i64,
    tf: u64,
    pb: ProgressBar,
    verbose: bool,
) {
    let client = MongoClientSync::new();
    client
        .trades
        .delete_many(
            doc! {"symbol": bson::to_bson(&symbol.clone()).unwrap()},
            None,
        )
        .unwrap();
    client
        .tf_trades
        .delete_many(
            doc! {"symbol": bson::to_bson(&symbol.clone()).unwrap()},
            None,
        )
        .unwrap();

    let mongo_client = Arc::new(client);
    let today = chrono::DateTime::<Utc>::from(SystemTime::now());
    let tf = Arc::new(tf);
    let symbol = Arc::new(symbol);
    let pb = Arc::new(pb);
    let months = if fetch_history_span == -1 {
        (0..(12 * 20))
            .map(|i| today - chrono::Duration::weeks(4 * i))
            .collect::<Vec<_>>()
    } else {
        let span = ((fetch_history_span as f64) / (4.0 * 7.0 * 24.0 * 3600.0)).ceil() as i64;
        (0..span)
            .map(|i| today - chrono::Duration::weeks(4 * i))
            .collect::<Vec<_>>()
    };

    if !verbose {
        pb.inc_length(months.len() as u64);
    }
    let concurrency_limit = num_cpus::get_physical() / 2;
    let tmp_dir = TempDir::new(&format!("agg-trades-{}", symbol.symbol)).unwrap();

    info!("[?] Downloading...");
    months.par_iter().for_each(|month| {
        let month_str = month.format("%Y-%m").to_string();
        let url = format!(
            "https://data.binance.vision/data/spot/monthly/aggTrades/{}/{}-aggTrades-{}.zip",
            symbol.symbol, symbol.symbol, month_str
        );
        if verbose {
            debug!("[+] fetching {}", url);
        }
        if let Ok(res) = ripunzip::UnzipEngine::for_uri(&url, None, || {}) {
            if let Err(e) = res.unzip(ripunzip::UnzipOptions {
                output_directory: Some(tmp_dir.path().to_path_buf()),
                password: None,
                single_threaded: false,
                filename_filter: None,
                progress_reporter: Box::new(ripunzip::NullProgressReporter),
            }) {
                warn!("[-] Failed to fetch {}. Reason: {}", url, e);
                return;
            }

            let tmp_dir = tmp_dir.path();
            let file_name = format!("{}-aggTrades-{}.csv", symbol.symbol, month_str);
            let file_path = tmp_dir.join(file_name);

            let reader = csv::Reader::from_path(file_path.clone());

            if let Ok(mut reader) = reader {
                reader.set_headers(StringRecord::from(vec![
                    "agg_trade_id",
                    "price",
                    "quantity",
                    "first_trade_id",
                    "last_trade_id",
                    "transact_time",
                    "is_buyer_maker",
                    "was_best_price",
                ]));
                let trades = reader
                    .deserialize::<ArchiveAggTrade>()
                    .filter_map(|result| result.ok())
                    .collect::<Vec<_>>()
                    .into_par_iter()
                    .map(AggTrade::from)
                    .collect::<Vec<_>>();

                drop(reader);
                let symbol = Symbol {
                    symbol: symbol.symbol.clone(),
                    exchange: symbol.exchange.clone(),
                    base_asset_precision: symbol.base_asset_precision,
                    quote_asset_precision: symbol.quote_asset_precision,
                };

                if trades.is_empty() {
                    warn!("[-] No agg trades found");
                } else {
                    insert_trade_entries(&trades, &symbol, mongo_client.clone(), tf.clone())
                        .expect("error inserting trade entries");
                }
            } else if verbose {
                warn!("[-] failed to deserialize {}", file_path.display());
            }
            if !verbose {
                pb.inc(1);
            }
        }
    });
}
#[derive(Deserialize)]
struct ArchiveAggTrade {
    agg_trade_id: u64,
    price: f64,
    quantity: f64,
    first_trade_id: u64,
    last_trade_id: u64,
    transact_time: u64,
    is_buyer_maker: String,
    was_best_price: String,
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
            maker: a.is_buyer_maker == "True",
        }
    }
}

// TODO: need to get recent history also for when we were loading
pub async fn load_history(
    key: AccessKey,
    symbol: Symbol,
    fetch_history_span: u64,
    _zip_only: bool,
) {
    let market = FuturesMarket::new(Some(key.api_key.clone()), Some(key.secret_key.clone()));
    let span = fetch_history_span;

    let starting_time = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();
    let start_time = starting_time as u64 - span;

    loop {
        info!(
            "Fetching history from {} to {}",
            Local
                .timestamp_millis_opt(start_time as i64)
                .unwrap()
                .time(),
            chrono::prelude::Local
                .timestamp_millis_opt(starting_time as i64)
                .unwrap()
                .time()
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
                    // if let Ok(_) = insert_trade_entries(&trades, symbol.clone(), mongo_client.clone()).await {
                    //     start_time = trades.last().unwrap().time + 1;
                    // }
                }
            }
        }
    }
}

// TODO: use websockets for this
pub async fn start_loader(symbol: Symbol, tf1: u64) -> JoinHandle<()> {
    let market = FuturesMarket::new(None, None);
    tokio::spawn(async move {
        let mut last_id = None;
        let mut start_time = Some(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64
                - tf1,
        );
        let mongo_client = Arc::new(MongoClientSync::new());

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
                        if let Ok(_) = insert_trade_entries(
                            &trades,
                            &symbol,
                            mongo_client.clone(),
                            Arc::new(tf1),
                        ) {
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

pub async fn start_kline_loader(symbol: Symbol, tf: String, _from: u64) -> JoinHandle<()> {
    let market = FuturesMarket::new(None, None);
    tokio::spawn(async move {
        let mongo_client = MongoClient::new().await;
        let mut last = 0;

        loop {
            let trades_result = market
                .get_klines(&symbol.symbol.clone(), tf.clone(), Some(1), None, None)
                .await;
            if let Ok(t) = trades_result {
                match t {
                    AllKlineSummaries(klines) => {
                        if klines.is_empty() {
                            continue;
                        }

                        for kline in klines {
                            if kline.close_time as u64 > last {
                                let k = Kline {
                                    symbol: symbol.clone(),
                                    open_time: kline.open_time as u64,
                                    open: Decimal::from_str(&kline.open).unwrap(),
                                    high: Decimal::from_str(&kline.high).unwrap(),
                                    low: Decimal::from_str(&kline.low).unwrap(),
                                    close: Decimal::from_str(&kline.close).unwrap(),
                                    volume: Decimal::from_str(&kline.volume).unwrap(),
                                    close_time: kline.close_time as u64,
                                    quote_volume: Decimal::from_str(&kline.quote_asset_volume)
                                        .unwrap(),
                                    count: kline.number_of_trades as u64,
                                    taker_buy_volume: Decimal::from_str(
                                        &kline.taker_buy_base_asset_volume,
                                    )
                                    .unwrap(),
                                    taker_buy_quote_volume: Decimal::from_str(
                                        &kline.taker_buy_quote_asset_volume,
                                    )
                                    .unwrap(),
                                    ignore: 0,
                                };

                                last = k.close_time;

                                mongo_client.kline.insert_one(k, None).await.unwrap();
                            }
                        }
                    }
                }
            }

            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    })
}

pub struct TfTradeEmitter {
    subscribers: Arc<RwLock<Sender<(TfTrades, Option<Arc<Notify>>)>>>,
    pub tf: u64,
    global_config: GlobalConfig,
}

pub struct KlineEmitter {
    subscribers: Arc<RwLock<Sender<(Kline, Option<Arc<Notify>>)>>>,
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
}

#[async_trait]
impl EventEmitter<Kline> for KlineEmitter {
    fn get_subscribers(&self) -> Arc<RwLock<Sender<(Kline, Option<Arc<Notify>>)>>> {
        self.subscribers.clone()
    }

    async fn emit(&self) -> anyhow::Result<JoinHandle<()>> {
        let mongo_client = MongoClient::new().await;
        let config = self.global_config.clone();

        let last_entry = mongo_client
            .kline
            .find_one(
                doc! {"symbol.symbol": bson::to_bson(&config.symbol.symbol)?},
                FindOneOptions::builder()
                    .sort(doc! {"close_time": -1})
                    .build(),
            )
            .await?;
        let mut last_timestamp = 0_u64;
        if let Some(t) = last_entry {
            last_timestamp = t.close_time;
        }
        let subscribers = self.subscribers.clone();
        Ok(tokio::spawn(async move {
            loop {
                let last_entry = mongo_client
                    .kline
                    .find_one(
                        doc! {"symbol.symbol": bson::to_bson(&config.symbol.symbol).unwrap()},
                        FindOneOptions::builder()
                            .sort(doc! {"close_time": -1})
                            .build(),
                    )
                    .await;

                if let Ok(Some(t)) = last_entry {
                    if t.close_time > last_timestamp {
                        last_timestamp = t.close_time;
                        subscribers.read().await.broadcast((t, None)).await.unwrap();
                    }
                }

                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }))
    }
}

#[async_trait]
impl EventEmitter<TfTrades> for TfTradeEmitter {
    fn get_subscribers(&self) -> Arc<RwLock<Sender<(TfTrades, Option<Arc<Notify>>)>>> {
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
        if let Some(t) = last_entry {
            last_timestamp = t.timestamp;
        }
        let tf = self.tf;
        let subscribers = self.subscribers.clone();
        Ok(tokio::spawn(async move {
            loop {
                if let Ok(t) = mongo_client
                    .tf_trades
                    .find(
                        doc! {"tf": bson::to_bson(&tf).unwrap(), "timestamp": {
                            "$gt": bson::to_bson(&last_timestamp).unwrap()
                        }},
                        Some(FindOptions::builder().sort(doc! {"id": -1}).build()),
                    )
                    .await
                {
                    let tf_trades = t.try_collect().await.unwrap_or_else(|_| vec![]);
                    debug!(
                        "last_timestamp: {} trades_len: {}",
                        last_timestamp,
                        tf_trades.len()
                    );
                    let notify = Arc::new(Notify::new());
                    let sm_notifer = notify.notified();
                    last_timestamp = tf_trades.last().unwrap().trades.last().unwrap().timestamp;

                    match subscribers
                        .read()
                        .await
                        .broadcast((tf_trades, Some(notify.clone())))
                        .await
                    {
                        Ok(_) => {
                            sm_notifer.await;
                        }
                        Err(e) => {
                            error!("Error broadcasting tf trades {:?}", e)
                        }
                    }
                } else {
                    debug!("no trades last_timestamp: {} trades_len: ", last_timestamp,);
                }
            }
        }))
    }
}
