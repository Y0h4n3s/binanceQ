use crate::db;
use crate::db::client::SQLiteTradeEntry;
use crate::types::{ Kline, TfTrade, TfTrades};
use crate::types::{Symbol};
use async_std::sync::Arc;
use chrono::prelude::*;
use csv::StringRecord;
use db::client::SQLiteClient;
use futures::stream::{self, StreamExt};
use indicatif::ProgressBar;
use rayon::prelude::*;
use rust_decimal::prelude::*;
use serde::Deserialize;
use std::cmp::Ordering;
use std::io::Read;
use std::time::{ SystemTime};
use binance::model::AggTrade;
use tempdir::TempDir;
use tokio::runtime::Handle;
use tokio_retry::strategy::ExponentialBackoff;
use tokio_retry::RetryIf;
use tracing::{debug, info, warn};


pub async fn insert_trade_entries(
    trades: Vec<AggTrade>,
    symbol: &Symbol,
    client: Arc<std::sync::Mutex<SQLiteClient>>,
    tf: Arc<u64>,
) -> anyhow::Result<()> {
    if trades.is_empty() {
        return Ok(())
    }

    let mut entries = Vec::with_capacity(trades.len() - 1);
    for (i, t) in trades.iter().enumerate().skip(1) {
        let delta = ((trades[i].price - trades[i - 1].price) * 100.0) / trades[i - 1].price;

        let entry = SQLiteTradeEntry {
            price: t.price.to_string(),
            qty: t.qty.to_string(),
            timestamp: t.time.to_string(),
            symbol_data: serde_json::to_value(symbol)?,
            symbol: symbol.symbol.clone(),
            delta: delta.to_string(),
        };
        entries.push(entry);
    }
    drop(trades);
    SQLiteClient::insert_trade_entries(&client.lock().unwrap().conn, &entries);

    


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

pub async fn compile_agg_trades_for(
    symbol: &Symbol,
    tf: u64,
    pb: ProgressBar,
    verbose: bool,
    sqlite_client: Arc<SQLiteClient>
) {

    sqlite_client.reset_tf_trades_by_tf(symbol, tf).await;
    let count = SQLiteClient::get_trades_count_by_symbol(&sqlite_client.pool, symbol).await;
    pb.inc_length(count);
    let last_entry = SQLiteClient::get_oldest_trade(&sqlite_client.pool, symbol).await;
    let latest_entry = SQLiteClient::get_latest_trade(&sqlite_client.pool, symbol).await;
    let mut start = last_entry.timestamp;
    let concurrency_limit = num_cpus::get_physical();

    stream::iter((start..latest_entry.timestamp).step_by(tf as usize * 1000))
        .chunks(concurrency_limit)
        .map(|batch| {
            let sqlite_client = sqlite_client.clone();
            let pb = pb.clone();
            async move {
                let results = stream::iter(batch)
                    .map(|start_time| {
                        let sqlite_client = sqlite_client.clone();
                            async move {
                                tokio::spawn(async move {
                                    SQLiteClient::select_values_between_min_max(
                                        &sqlite_client.pool,
                                        start_time.to_string(),
                                        (start_time + tf * 1000).to_string(),
                                    ).await

                                }).await
                                    .expect("Failed to get  range values")
                            }


                    })
                    .buffer_unordered(concurrency_limit)
                    .collect::<Vec<_>>()
                    .await;

                let tf_trades = results.into_par_iter()
                    .filter(|v| !v.is_empty())
                    .map(|entries| {
                        pb.inc(entries.len() as u64);

                        TfTrade {
                            symbol: symbol.clone(),
                            tf,
                            id: entries[0].trade_id,
                            timestamp: entries[0].timestamp,
                            max_trade_time: entries.last().unwrap().timestamp,
                            min_trade_time: entries.first().unwrap().timestamp,
                            trades: entries,
                        }
                    }).collect::<Vec<_>>();

                tokio::task::spawn_blocking(move || {
                    SQLiteClient::insert_tf_trades(&sqlite_client.conn, tf_trades)
                }).await.unwrap();


            }
        })
        .buffer_unordered(concurrency_limit)
        .collect::<Vec<_>>()
        .await;
}

pub async fn load_klines_from_archive(
    symbol: Symbol,
    tf: String,
    fetch_history_span: i64,
    pb: ProgressBar,
    verbose: bool,
    sqlite_client: Arc<std::sync::Mutex<SQLiteClient>>
)
{
    sqlite_client.lock().unwrap().reset_kline(&symbol).await;
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
            let sqlite_client = sqlite_client.clone();
            let pb = pb.clone();
            async move {
                let month_str = month.format("%Y-%m").to_string();
                let url = format!(
                    "https://data.binance.vision/data/spot/monthly/klines/{}/{}/{}-{}-{}.zip",
                    symbol.symbol,
                    tf,
                    symbol.symbol,
                    tf,
                    month_str
                );
                if verbose {
                    debug!("[+] fetching {}", url);
                }
                let retry_strategy = ExponentialBackoff::from_millis(100).take(3);

                let res = RetryIf::spawn(retry_strategy, || reqwest::get(&url), |e: &reqwest::Error| {
                    e.is_connect() || e.is_timeout()
                }).await;
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
                            .read_to_string(&mut file_contents).expect("failed to read archive contents");
                        let mut reader = csv::Reader::from_reader(file_contents.as_bytes());

                        reader.set_headers(
                            StringRecord::from(vec!["open_time", "open", "high", "low", "close", "volume", "close_time", "quote_volume", "count", "taker_buy_volume", "taker_buy_quote_volume", "ignore"])
                        );
                        let mut trades = vec![];

                        for record in  reader
                            .deserialize::<ArchiveKline>()
                            .filter_map(|result| result.ok()) {
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
                            SQLiteClient::insert_klines(&sqlite_client.lock().unwrap().conn, trades);
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
        }).buffer_unordered(concurrency_limit)
        .collect::<Vec<_>>()
        .await;
    info!("Cleaning up...");
    // sqlite_client.lock().unwrap().vacuum().await;
}

pub fn load_history_from_archive(
    symbol: Symbol,
    fetch_history_span: i64,
    tf: u64,
    pb: ProgressBar,
    verbose: bool,
    sqlite_client: Arc<std::sync::Mutex<SQLiteClient>>,
    rt: Arc<Handle>
    
)  {
    rt.block_on(async {
        let sqlite_client = sqlite_client.lock().unwrap();
        sqlite_client.reset_trade_entries(&symbol).await;
        sqlite_client.reset_tf_trades(&symbol).await;
    });
    let today = chrono::DateTime::<Utc>::from(SystemTime::now());
    let tf = Arc::new(tf);
    let symbol = Arc::new(symbol);
    let pb = Arc::new(pb);
    let months = if fetch_history_span == -1 {
        (0..(12 * 20))
            .map(|i| today - chrono::Duration::weeks(4 * i))
            .collect::<Vec<_>>()
    }
    else {
        let span = ((fetch_history_span as f64) / (4.0 * 7.0 * 24.0 * 3600.0)).ceil() as i64;
        (0..span)
            .map(|i| today - chrono::Duration::weeks(4 * i))
            .collect::<Vec<_>>()
    };


    if !verbose {
        pb.inc_length(months.len() as u64);
    }
    let tmp_dir = TempDir::new(&format!("agg-trades-{}", symbol.symbol)).unwrap();

    info!("[?] Downloading...");
    months.par_iter().for_each(|month| {
        let month_str = month.format("%Y-%m").to_string();
        let url = format!(
            "https://data.binance.vision/data/spot/monthly/aggTrades/{}/{}-aggTrades-{}.zip",
            symbol.symbol,
            symbol.symbol,
            month_str
        );
        if verbose {

            debug!("[+] fetching {}", url);
        }
        if let Ok(res) = ripunzip::UnzipEngine::for_uri(&url, None, || {

        }) {
            if let Err(e) = res.unzip(ripunzip::UnzipOptions{
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
            let file_name = format!("{}-aggTrades-{}.csv",
                                    symbol.symbol,
                                    month_str);
            let file_path = tmp_dir.join(file_name);


            let reader = csv::Reader::from_path(file_path.clone());

            if let Ok( mut reader) = reader {

                reader.set_headers(
                    StringRecord::from(vec!["agg_trade_id", "price", "quantity", "first_trade_id", "last_trade_id", "transact_time", "is_buyer_maker", "was_best_price"])
                );
                let trades =  reader
                    .deserialize::<ArchiveAggTrade>()
                    .filter_map(|result| result.ok()).collect::<Vec<_>>().into_par_iter().map(AggTrade::from).collect::<Vec<_>>();
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
                    rt.block_on(async {
                        insert_trade_entries(trades, &symbol, sqlite_client.clone(), tf.clone()).await.expect("error inserting trade entries");
                    })
                }

            } else if verbose {

                warn!("[-] failed to deserialize {}", file_path.display());
            }
            if !verbose {
                pb.inc(1);
            }
        }

    });

    info!("Cleaning up...");
    rt.block_on(async {
        // sqlite_client.lock().unwrap().vacuum().await;
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
            best_match: false,
        }
    }
}
