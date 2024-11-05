#![feature(iterator_try_collect)]
#![feature(async_closure)]
#[allow(unused)]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[allow(unused)]
mod back_tester;

#[allow(unused)]
mod events;
#[allow(unused)]
mod executors;
#[allow(unused)]
mod managers;
#[allow(unused)]
#[cfg(feature = "trades")]
mod mongodb;

#[allow(unused)]
mod db;
#[allow(unused)]
mod strategies;
#[allow(dead_code, unused)]
mod types;
#[allow(unused)]
mod utils;

use crate::back_tester::{BackTester, BackTesterConfig, BackTesterMulti};
#[cfg(feature = "trades")]
use crate::db::loader::compile_agg_trades_for;
use crate::types::OrderStatus;
use async_broadcast::{Receiver, Sender};
use async_std::sync::Arc;
use clap::{arg, command, value_parser, Command};
use events::EventSink;
#[cfg(feature = "trades")]
use crate::executors::simulated::SimulatedAccount;
#[cfg(feature = "candles")]
use crate::executors::simulated_candle::SimulatedCandleAccount as SimulatedAccount;

use indicatif::{ProgressBar, ProgressState, ProgressStyle};
use once_cell::sync::Lazy;
use std::env;
use std::fmt::Write;
use std::time::SystemTime;
use tokio::sync::Notify;
use tracing::info;
use tracing::level_filters::LevelFilter;
use tracing_subscriber::EnvFilter;
use types::{AccessKey, ExchangeId, GlobalConfig, Mode, Symbol, TfTrades, Trade};
use crate::db::client::SQLiteClient;

static KEY: Lazy<AccessKey> = Lazy::new(|| {
    let api_key = env::var("API_KEY")
        .unwrap_or("bk5UAaA4AuSTWD0myql8ZaXjOJJWaYcP3TY67lB6ptSRUNUdPHFnkE6R98OxMCEP".to_string());
    let secret_key = env::var("SECRET_KEY")
        .unwrap_or("qyZ0VfigHXfGshXysehAZN2iJhVw9L2Bq5pu8WkmMHob8A9hhwCZLtYoHVp73Bvt".to_string());
    AccessKey {
        api_key,
        secret_key,
    }
});

fn main() -> Result<(), anyhow::Error> {
    // console_subscriber::init();
    // or as an allow list (INFO, but drill into my crate's logs)

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(16)
        .max_blocking_threads(1024)
        .build()
        .unwrap();
    runtime.block_on(async_main())
}
async fn async_main() -> anyhow::Result<()> {
    #[cfg(feature = "trades")]
    let main_matches = {
        command!()
            .arg(arg!( -v --verbose "Verbose output"))
            .subcommand(
                Command::new("backtest")
                    .about("Backtest a strategy")
                    .arg(
                        arg!(-m --mode <MODE> "Single Instrument or Multiple Instrument")
                            .required(false)
                            .default_value("single")
                            .value_parser(clap::builder::PossibleValuesParser::new(vec![
                                "single", "multi",
                            ])),
                    )
                    .arg(
                        arg!(-l --length <SECONDS> "The span of the backtest in seconds")
                            .required(true)
                            .value_parser(value_parser!(u64)),
                    )
                    .arg(
                        arg!(--timeframe <SECONDS> "The compiled timeframe chunk to use")
                            .required(true)
                            .value_parser(value_parser!(u64)),
                    )
                    .arg(
                        arg!(-s --symbols <SYMBOLS> "The instruments to backtest")
                            .required(false)
                            .num_args(1..)
                            .default_value("BTCUSDT"),
                    )
                    .arg(
                        arg!(--ktf <KLINE_TF> "Kline timeframe")
                            .required(false)
                            .default_value("5m"),
                    )
                    .arg(arg!(--loghistory "Log tftrades history from agg trades").required(false)),
            )
            .subcommand(
                Command::new("download")
                    .arg(
                        arg!(--ktf <KLINE_TF> "Kline timeframe")
                            .required(false)
                            .default_value("5m"),
                    )
                    .arg(
                        arg!(-s --symbols <SYMBOLS> "The instruments to download")
                            .required(false)
                            .num_args(1..)
                            .default_value("BTCUSDT"),
                    )
                    .arg(
                        arg!(-l --length <SECONDS> "The span of the download in seconds")
                            .required(false)
                            .default_value("-1")
                            .value_parser(value_parser!(i64)),
                    )
                    .arg(arg!(--nokline "Don't download klines").required(false))
                    .arg(arg!(--noaggtrades "Don't download aggtrades").required(false))
                    .about("download candles"),
            )
            .subcommand(
                Command::new("compile")
                    .arg(
                        arg!(--tf <TF> "Chunk timeframe in seconds")
                            .required(false)
                            .default_value("30")
                            .value_parser(value_parser!(u64)),
                    )
                    .arg(
                        arg!(-s --symbols <SYMBOLS> "The instruments to compile")
                            .required(false)
                            .num_args(1..)
                            .default_value("BTCUSDT"),
                    ),
            )
            .subcommand(
                Command::new("live")
                    .about("Live mode")
                    .arg(
                        arg!(-l --length <SECONDS> "The span of the backtest in seconds")
                            .required(true)
                            .value_parser(value_parser!(u64)),
                    )
                    .arg(
                        arg!(--timeframe1 <SECONDS> "The first timeframe to use")
                            .required(true)
                            .value_parser(value_parser!(u64)),
                    )
                    .arg(
                        arg!(-s --symbols <SYMBOLS> "The instruments to backtest")
                            .required(false)
                            .num_args(1..)
                            .default_value("BTCUSDT"),
                    )
                    .arg(
                        arg!(--ktf <KLINE_TF> "Kline timeframe")
                            .required(false)
                            .default_value("15m"),
                    ),
            )
            .get_matches()
    };

    #[cfg(feature = "candles")]
    let main_matches = { command!()
            .arg(arg!( -v --verbose "Verbose output"))
            .subcommand(
                Command::new("backtest")
                    .about("Backtest a strategy")
                    .arg(
                        arg!(-m --mode <MODE> "Single Instrument or Multiple Instrument")
                            .required(false)
                            .default_value("single")
                            .value_parser(clap::builder::PossibleValuesParser::new(vec![
                                "single", "multi",
                            ])),
                    )
                    .arg(
                        arg!(-l --length <SECONDS> "The span of the backtest in seconds")
                            .required(true)
                            .value_parser(value_parser!(u64)),
                    )
                    .arg(
                        arg!(--timeframe <SECONDS> "The timeframe to use for validating orders")
                            .required(true)
                            .default_value("1m"),
                    )
                    .arg(
                        arg!(-s --symbols <SYMBOLS> "The instruments to backtest")
                            .required(false)
                            .num_args(1..)
                            .default_value("BTCUSDT"),
                    )
                    .arg(
                        arg!(--ktf <KLINE_TF> "Kline timeframe")
                            .required(false)
                            .default_value("5m"),
                    )
                    .arg(arg!(--loghistory "Log tftrades history from agg trades").required(false)),
            )
            .subcommand(
                Command::new("download")
                    .arg(
                        arg!(--ktf <KLINE_TF> "Kline timeframe")
                            .required(false)
                            .default_value("5m"),
                    )
                    .arg(
                        arg!(-s --symbols <SYMBOLS> "The instruments to download")
                            .required(false)
                            .num_args(1..)
                            .default_value("BTCUSDT"),
                    )
                    .arg(
                        arg!(-l --length <SECONDS> "The span of the download in seconds")
                            .required(false)
                            .default_value("-1")
                            .value_parser(value_parser!(i64)),
                    )
                    .about("download candles"),
            )
            .subcommand(
                Command::new("live")
                    .about("Live mode")
                    .arg(
                        arg!(-l --length <SECONDS> "The span of the backtest in seconds")
                            .required(true)
                            .value_parser(value_parser!(u64)),
                    )
                    .arg(
                        arg!(--timeframe1 <SECONDS> "The first timeframe to use")
                            .required(true)
                            .value_parser(value_parser!(u64)),
                    )
                    .arg(
                        arg!(-s --symbols <SYMBOLS> "The instruments to backtest")
                            .required(false)
                            .num_args(1..)
                            .default_value("BTCUSDT"),
                    )
                    .arg(
                        arg!(--ktf <KLINE_TF> "Kline timeframe")
                            .required(false)
                            .default_value("15m"),
                    ),
            )
            .get_matches()
    };
    let log_level = if main_matches.get_flag("verbose") {
        "binance_q=trace"
    } else {
        "binance_q=info"
    };
    let filter = EnvFilter::builder()
        .with_default_directive(LevelFilter::DEBUG.into())
        .parse(log_level)?;

    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .compact()
        .init();
    #[cfg(feature = "trades")]
    if let Some(matches) = main_matches.subcommand_matches("backtest") {
        let mode = matches.get_one::<String>("mode").unwrap();
        let ktf = matches.get_one::<String>("ktf").unwrap();
        let mut symbols = matches.get_many::<String>("symbols").unwrap().clone();
        let backtest_span = *matches
            .get_one::<u64>("length")
            .ok_or(anyhow::anyhow!("Invalid span"))?;
        let tf = *matches
            .get_one::<u64>("timeframe")
            .ok_or(anyhow::anyhow!("Invalid tf"))?;
        let pb = ProgressBar::new(0);
        pb.set_style(
            ProgressStyle::with_template(
                "[?] [{elapsed_precise}] [{wide_bar:.cyan/blue}] {percent}%|[ETA: {eta_precise}]",
            )
            .unwrap()
            .with_key("eta", |state: &ProgressState, w: &mut dyn Write| {
                write!(w, "{:.1}s", state.eta().as_secs_f64()).unwrap()
            })
            .progress_chars("#>-"),
        );

        let tf_trades_channel: (
            Sender<(TfTrades, Option<Arc<Notify>>)>,
            Receiver<(TfTrades, Option<Arc<Notify>>)>,
        ) = async_broadcast::broadcast(10000);
        let trades_channel: (
            Sender<(Trade, Option<Arc<Notify>>)>,
            Receiver<(Trade, Option<Arc<Notify>>)>,
        ) = async_broadcast::broadcast(1000);
        let order_statuses_channel: (
            Sender<(OrderStatus, Option<Arc<Notify>>)>,
            Receiver<(OrderStatus, Option<Arc<Notify>>)>,
        ) = async_broadcast::broadcast(100);
        let client = Arc::new(db::client::SQLiteClient::new().await);
        info!("Creating index...");
        let c = client.clone();
        SQLiteClient::create_backtest_indices(&client.conn);
        if mode == "single" {
            let symbol = Symbol {
                symbol: symbols.next().unwrap().clone(),
                exchange: ExchangeId::Simulated,
                base_asset_precision: 0,
                quote_asset_precision: 0,
            };
            let global_config = GlobalConfig {
                symbol: symbol.clone(),
                key: KEY.clone(),
                verbose: main_matches.get_flag("verbose"),
                mode: Mode::Backtest,
            };

            let back_tester_config = BackTesterConfig {
                symbol: symbol.clone(),
                length: backtest_span,
                _log_history: matches.get_flag("loghistory"),
                grpc_server_port: 50011.to_string(),
                _kline_tf: ktf.clone(),
            };
            let back_tester = BackTester::new(global_config, back_tester_config);

            let start = SystemTime::now();
            let account = SimulatedAccount::new(
                tf_trades_channel.1.deactivate(),
                order_statuses_channel.1.deactivate(),
                trades_channel.1.clone().deactivate(),
                vec![symbol],
                order_statuses_channel.0.clone(),
                trades_channel.0,
                client.clone(),
            )
            .await;
            let account = Arc::new(account);
            let ac = account.clone();
            tokio::spawn(async move {
                EventSink::<Trade>::listen(ac).unwrap();
            });
            let ac = account.clone();
            tokio::spawn(async move {
                EventSink::<TfTrades>::listen(ac).unwrap();
            });
            let ac = account.clone();
            tokio::spawn(async move {
                EventSink::<OrderStatus>::listen(ac).unwrap();
            });

            back_tester
                .run(
                    pb.clone(),
                    trades_channel.1.deactivate(),
                    tf_trades_channel.0,
                    order_statuses_channel.0,
                    account,
                    client,
                )
                .await?;

            info!("Back-test finished in {:?} seconds", start.elapsed()?);
        } else if mode == "multi" {
            let mut b_configs = vec![];
            let mut symbols_i: Vec<Symbol> = vec![];
            let mut grpc_server = 50011;
            for symbol in symbols {
                let symbol = Symbol {
                    symbol: symbol.clone(),
                    exchange: ExchangeId::Simulated,
                    base_asset_precision: 0,
                    quote_asset_precision: 0,
                };
                symbols_i.push(symbol.clone());
                let back_tester_config = BackTesterConfig {
                    symbol,
                    length: backtest_span,
                    _log_history: matches.get_flag("loghistory"),
                    grpc_server_port: grpc_server.to_string(),
                    _kline_tf: ktf.clone(),
                };
                grpc_server += 1;
                b_configs.push(back_tester_config);
            }

            let global_config = GlobalConfig {
                symbol: b_configs.first().unwrap().symbol.clone(),
                key: KEY.clone(),
                mode: Mode::Backtest,
                verbose: main_matches.get_flag("verbose"),
            };

            let account = SimulatedAccount::new(
                tf_trades_channel.1.deactivate(),
                order_statuses_channel.1.deactivate(),
                trades_channel.1.clone().deactivate(),
                symbols_i,
                order_statuses_channel.0.clone(),
                trades_channel.0,
                client.clone(),
            )
            .await;
            let account = Arc::new(account);
            let ac = account.clone();
            tokio::spawn(async move {
                EventSink::<Trade>::listen(ac).unwrap();
            });
            let ac = account.clone();
            tokio::spawn(async move {
                EventSink::<TfTrades>::listen(ac).unwrap();
            });
            let ac = account.clone();
            tokio::spawn(async move {
                EventSink::<OrderStatus>::listen(ac).unwrap();
            });
            let back_tester = BackTesterMulti::new(global_config, b_configs);
            let start = std::time::SystemTime::now();
            back_tester
                .run(
                    pb.clone(),
                    trades_channel.1.deactivate(),
                    tf_trades_channel.0,
                    order_statuses_channel.0,
                    account,
                    client,
                )
                .await?;

            info!("Back-test finished in {:?} seconds", start.elapsed()?);
        }
        info!("Dropping index...");
        SQLiteClient::drop_backtest_indices(&c.conn);
    }
    #[cfg(feature = "candles")]
    if let Some(matches) = main_matches.subcommand_matches("backtest") {
        let mode = matches.get_one::<String>("mode").unwrap();
        let ktf = matches.get_one::<String>("ktf").unwrap();
        let mut symbols = matches.get_many::<String>("symbols").unwrap().clone();
        let backtest_span = *matches
            .get_one::<u64>("length")
            .ok_or(anyhow::anyhow!("Invalid span"))?;
        let tf = matches
            .get_one::<String>("timeframe")
            .unwrap_or(ktf);
        let pb = ProgressBar::new(0);
        pb.set_style(
            ProgressStyle::with_template(
                "[?] [{elapsed_precise}] [{wide_bar:.cyan/blue}] {percent}%|[ETA: {eta_precise}]",
            )
            .unwrap()
            .with_key("eta", |state: &ProgressState, w: &mut dyn Write| {
                write!(w, "{:.1}s", state.eta().as_secs_f64()).unwrap()
            })
            .progress_chars("#>-"),
        );

        let tf_trades_channel: (
            Sender<(TfTrades, Option<Arc<Notify>>)>,
            Receiver<(TfTrades, Option<Arc<Notify>>)>,
        ) = async_broadcast::broadcast(10000);
        let trades_channel: (
            Sender<(Trade, Option<Arc<Notify>>)>,
            Receiver<(Trade, Option<Arc<Notify>>)>,
        ) = async_broadcast::broadcast(1000);
        let order_statuses_channel: (
            Sender<(OrderStatus, Option<Arc<Notify>>)>,
            Receiver<(OrderStatus, Option<Arc<Notify>>)>,
        ) = async_broadcast::broadcast(100);
        let client = Arc::new(db::client::SQLiteClient::new().await);
        info!("Creating index...");
        let c = client.clone();
        SQLiteClient::create_backtest_indices(&client.conn);
        if mode == "single" {
            let symbol = Symbol {
                symbol: symbols.next().unwrap().clone(),
                exchange: ExchangeId::Simulated,
                base_asset_precision: 0,
                quote_asset_precision: 0,
            };
            let global_config = GlobalConfig {
                symbol: symbol.clone(),
                key: KEY.clone(),
                verbose: main_matches.get_flag("verbose"),
                mode: Mode::Backtest,
            };

            let back_tester_config = BackTesterConfig {
                symbol: symbol.clone(),
                length: backtest_span,
                _log_history: matches.get_flag("loghistory"),
                grpc_server_port: 50011.to_string(),
                _kline_tf: ktf.clone(),
            };
            let back_tester = BackTester::new(global_config, back_tester_config);

            let start = SystemTime::now();
            let account = SimulatedAccount::new(
                tf_trades_channel.1.deactivate(),
                order_statuses_channel.1.deactivate(),
                trades_channel.1.clone().deactivate(),
                vec![symbol],
                order_statuses_channel.0.clone(),
                trades_channel.0,
                client.clone(),
            )
            .await;
            let account = Arc::new(account);
            let ac = account.clone();
            tokio::spawn(async move {
                EventSink::<Trade>::listen(ac).unwrap();
            });
            let ac = account.clone();
            tokio::spawn(async move {
                EventSink::<TfTrades>::listen(ac).unwrap();
            });
            let ac = account.clone();
            tokio::spawn(async move {
                EventSink::<OrderStatus>::listen(ac).unwrap();
            });

            back_tester
                .run(
                    pb.clone(),
                    trades_channel.1.deactivate(),
                    tf_trades_channel.0,
                    order_statuses_channel.0,
                    account,
                    client,
                    ktf.to_string(),
                    tf.to_string()
                )
                .await?;

            info!("Back-test finished in {:?} seconds", start.elapsed()?);
        } else if mode == "multi" {
            let mut b_configs = vec![];
            let mut symbols_i: Vec<Symbol> = vec![];
            let mut grpc_server = 50011;
            for symbol in symbols {
                let symbol = Symbol {
                    symbol: symbol.clone(),
                    exchange: ExchangeId::Simulated,
                    base_asset_precision: 0,
                    quote_asset_precision: 0,
                };
                symbols_i.push(symbol.clone());
                let back_tester_config = BackTesterConfig {
                    symbol,
                    length: backtest_span,
                    _log_history: matches.get_flag("loghistory"),
                    grpc_server_port: grpc_server.to_string(),
                    _kline_tf: ktf.clone(),
                };
                grpc_server += 1;
                b_configs.push(back_tester_config);
            }

            let global_config = GlobalConfig {
                symbol: b_configs.first().unwrap().symbol.clone(),
                key: KEY.clone(),
                mode: Mode::Backtest,
                verbose: main_matches.get_flag("verbose"),
            };

            let account = SimulatedAccount::new(
                tf_trades_channel.1.deactivate(),
                order_statuses_channel.1.deactivate(),
                trades_channel.1.clone().deactivate(),
                symbols_i,
                order_statuses_channel.0.clone(),
                trades_channel.0,
                client.clone(),
            )
            .await;
            let account = Arc::new(account);
            let ac = account.clone();
            tokio::spawn(async move {
                EventSink::<Trade>::listen(ac).unwrap();
            });
            let ac = account.clone();
            tokio::spawn(async move {
                EventSink::<TfTrades>::listen(ac).unwrap();
            });
            let ac = account.clone();
            tokio::spawn(async move {
                EventSink::<OrderStatus>::listen(ac).unwrap();
            });
            let back_tester = BackTesterMulti::new(global_config, b_configs);
            let start = std::time::SystemTime::now();
            back_tester
                .run(
                    pb.clone(),
                    trades_channel.1.deactivate(),
                    tf_trades_channel.0,
                    order_statuses_channel.0,
                    account,
                    client,
                    ktf.to_string(),
                    tf.to_string()
                )
                .await?;

            info!("Back-test finished in {:?} seconds", start.elapsed()?);
        }
        info!("Dropping index...");
        SQLiteClient::drop_backtest_indices(&c.conn);
    }
    #[cfg(feature = "trades")]
    if let Some(matches) = main_matches.subcommand_matches("download") {
        let cpus = num_cpus::get_physical();
        rayon::ThreadPoolBuilder::new()
            .num_threads(cpus)
            .build_global()?;
        let symbols = matches.get_many::<String>("symbols").unwrap().clone();
        let tf1 = matches
            .get_one::<String>("ktf")
            .ok_or(anyhow::anyhow!("Invalid timeframe"))?
            .clone();
        let l = *matches
            .get_one::<i64>("length")
            .ok_or(anyhow::anyhow!("Invalid timeframe"))?;
        let download_klines = !matches.get_flag("nokline");
        let download_trades = !matches.get_flag("noaggtrades");
        let pb = ProgressBar::new(0);
        let verbose = main_matches.get_flag("verbose");

        pb.set_style(
            ProgressStyle::with_template(
                "[?] Progress [{elapsed_precise}] [{wide_bar:.cyan/blue}] {percent}%}",
            )
            .unwrap()
            .with_key("eta", |state: &ProgressState, w: &mut dyn Write| {
                write!(w, "{:.1}s", state.eta().as_secs_f64()).unwrap()
            })
            .progress_chars("#>-"),
        );

        let mut threads = vec![];
        let client = Arc::new(std::sync::Mutex::new(db::client::SQLiteClient::new().await));
        let w = client.lock().unwrap();
        info!("Creating index...");
        // SQLiteClient::create_download_indices(&w.conn);
        drop(w);
        info!("[?] Downloading...");
        for s in symbols.into_iter() {
            let symbol = Symbol {
                symbol: s.clone(),
                exchange: ExchangeId::Simulated,
                base_asset_precision: 1,
                quote_asset_precision: 2,
            };
            let ktf = tf1.clone();
            let lpb = pb.clone();
            let lpb1 = pb.clone();
            let client = client.clone();
            let rt_handle = Arc::new(tokio::runtime::Handle::current());
            threads.push(std::thread::spawn(move || {
                let s = symbol.clone();
                let c = client.clone();
                if download_klines {
                    rt_handle.block_on(async move {
                        db::loader::load_klines_from_archive(s, ktf, l, lpb, verbose, c).await;
                    });
                }
                let client = client.clone();
                let s = symbol.clone();
                if download_trades {
                    rt_handle.block_on(async move {
                        db::loader::load_history_from_archive(s, l, lpb1, verbose, client).await;
                    });
                }
                info!("{} data downloaded", symbol.symbol.clone());
            }));
        }
        for t in threads.into_iter() {
            t.join().unwrap();
        }
        info!("Dropping index...");
        let w = client.lock().unwrap();
        SQLiteClient::drop_download_indices(&w.conn);
        info!("Cleaning up...");
        w.vacuum().await;

    }

    #[cfg(feature = "candles")]
    if let Some(matches) = main_matches.subcommand_matches("download") {
        let cpus = num_cpus::get_physical();
        rayon::ThreadPoolBuilder::new()
            .num_threads(cpus)
            .build_global()?;
        let symbols = matches.get_many::<String>("symbols").unwrap().clone();
        let tf1 = matches
            .get_one::<String>("ktf")
            .ok_or(anyhow::anyhow!("Invalid timeframe"))?
            .clone();
        let l = *matches
            .get_one::<i64>("length")
            .ok_or(anyhow::anyhow!("Invalid timeframe"))?;
        let pb = ProgressBar::new(0);
        let verbose = main_matches.get_flag("verbose");

        pb.set_style(
            ProgressStyle::with_template(
                "[?] Progress [{elapsed_precise}] [{wide_bar:.cyan/blue}] {percent}%}",
            )
            .unwrap()
            .with_key("eta", |state: &ProgressState, w: &mut dyn Write| {
                write!(w, "{:.1}s", state.eta().as_secs_f64()).unwrap()
            })
            .progress_chars("#>-"),
        );

        let mut threads = vec![];
        let client = Arc::new(std::sync::Mutex::new(db::client::SQLiteClient::new().await));
        let w = client.lock().unwrap();
        info!("Creating index...");
        // SQLiteClient::create_download_indices(&w.conn);
        drop(w);
        info!("[?] Downloading...");
        for s in symbols.into_iter() {
            let symbol = Symbol {
                symbol: s.clone(),
                exchange: ExchangeId::Simulated,
                base_asset_precision: 1,
                quote_asset_precision: 2,
            };
            let ktf = tf1.clone();
            let lpb = pb.clone();
            let lpb1 = pb.clone();
            let client = client.clone();
            let rt_handle = Arc::new(tokio::runtime::Handle::current());
            threads.push(std::thread::spawn(move || {
                let s = symbol.clone();
                let c = client.clone();
                    rt_handle.block_on(async move {
                        db::loader::load_klines_from_archive(s, ktf, l, lpb, verbose, c).await;
                    });
                info!("{} data downloaded", symbol.symbol.clone());
            }));
        }
        for t in threads.into_iter() {
            t.join().unwrap();
        }
        info!("Dropping index...");
        let w = client.lock().unwrap();
        SQLiteClient::drop_download_indices(&w.conn);
        info!("Cleaning up...");
        w.vacuum().await;

    }

    #[cfg(feature = "trades")]
        if let Some(matches) = main_matches.subcommand_matches("compile") {
        let cpus = num_cpus::get() * 2;
        rayon::ThreadPoolBuilder::new()
            .num_threads(cpus)
            .build_global()?;
        let symbols = matches.get_many::<String>("symbols").unwrap().clone();
        let tf = *matches
            .get_one::<u64>("tf")
            .ok_or(anyhow::anyhow!("Invalid timeframe"))?;
        let pb = ProgressBar::new(0);
        let verbose = main_matches.get_flag("verbose");
        pb.set_style(
            ProgressStyle::with_template(
                "[?] [{elapsed_precise}] [{wide_bar:.cyan/blue}] {percent}%|[ETA: {eta_precise}]",
            )
                .unwrap()
                .with_key("eta", |state: &ProgressState, w: &mut dyn Write| {
                    write!(w, "{:.1}s", state.eta().as_secs_f64()).unwrap()
                })
                .progress_chars("#>-"),
        );

        let client = Arc::new(db::client::SQLiteClient::new().await);
        info!("Creating index...");
        SQLiteClient::create_compile_indices(&client.conn);
        let mut threads = vec![];
        let permits = 5;
        let semaphore = Arc::new(tokio::sync::Semaphore::new(permits));
        for symbol in symbols {
            let symbol = Symbol {
                symbol: symbol.clone(),
                exchange: ExchangeId::Simulated,
                base_asset_precision: 0,
                quote_asset_precision: 0,
            };
            let pb = pb.clone();
            let client = client.clone();
            let semaphore = semaphore.clone();
            threads.push(tokio::spawn(async move {
                let _ = semaphore.acquire().await.unwrap();
                compile_agg_trades_for(&symbol, tf, pb, verbose, client.clone()).await;
            }));
        }
        for thread in threads {
            let _ = thread.await;
        }
        info!("Dropping index...");
        SQLiteClient::drop_compile_indices(&client.conn);
    }
    if let Some(_matches) = main_matches.subcommand_matches("live") {
        todo!()
    }

    Ok(())
}
