#![feature(iterator_try_collect)]
#![feature(async_closure)]

mod back_tester;
use futures::StreamExt;
use once_cell::sync::Lazy;
use std::env;
use std::sync::Mutex;
use async_std::sync::Arc;
use binance_q_mongodb::loader::{KlineEmitter, load_history_from_archive, load_klines_from_archive, start_kline_loader, start_loader, TfTradeEmitter};
use binance_q_types::{AccessKey, ExchangeId, Mode, ExecutionCommand, GlobalConfig, Kline, Order, Symbol, TfTrades, Trade};
use std::fmt::Write;
use std::time::{Duration, SystemTime};
use async_broadcast::{Receiver, Sender};
use binance::futures::general::FuturesGeneral;
use binance::futures::market::FuturesMarket;
use clap::{arg, command, Command, value_parser};
use clap::builder::TypedValueParser;
use futures::stream::FuturesUnordered;
use indicatif::{ProgressBar, ProgressState, ProgressStyle};
use subprocess::Exec;
use tokio::sync::RwLock;
use binance::api::Binance;
use binance_q_events::{EventEmitter, EventSink};
use binance_q_executors::live::BinanceLiveAccount;
use binance_q_executors::simulated::SimulatedAccount;
use binance_q_executors::{ExchangeAccount, TradeExecutor};
use binance_q_managers::risk_manager::{RiskManager, RiskManagerConfig};
use binance_q_managers::strategy_manager::{StrategyManager, StrategyManagerConfig};
use crate::back_tester::{BackTester, BackTesterConfig, BackTesterMulti};


static KEY: Lazy<AccessKey> = Lazy::new(|| {
    let api_key = env::var("API_KEY")
        .unwrap_or("ftcpi3OSjk26htxak54hqkZ6e9vdHq2Vd7oN83VZN39UcYmw1VwVkibug52oGIs4".to_string());
    let secret_key = env::var("SECRET_KEY")
        .unwrap_or("iXArQEDFfmFanIfz7RfJj6G034b76nDuNetqxdqSLiUwPqDtLGIaNYK4TgDxR9H4".to_string());
    AccessKey {
        api_key,
        secret_key,
    }
});

fn main() -> Result<(), anyhow::Error> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
          .enable_all()
          .worker_threads(32)
          .max_blocking_threads(1024)
          .build()
          .unwrap();
    runtime.block_on(async_main())
}
async fn async_main() -> anyhow::Result<()> {
    let main_matches = command!()
          .arg(arg!( -v --verbose "Verbose output"))
          .subcommand(
              Command::new("backtest")
                    .about("Backtest a strategy")
                    .arg(arg!(-m --mode <MODE> "Single Instrument or Multiple Instrument")
                          .required(false)
                          .default_value("single")
                            .value_parser(clap::builder::PossibleValuesParser::new(vec!["single", "multi"]))
                    )
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
                        arg!(--timeframe2 <SECONDS> "The second timeframe to use")
                            .required(true)
                              .value_parser(value_parser!(u64)),

                    )
                    .arg(
                        arg!(--timeframe3 <SECONDS> "The third timeframe to use")
                            .required(true)
                              .value_parser(value_parser!(u64)),

                    )
                    .arg(
                        arg!(-s --symbols <SYMBOLS> "The instruments to backtest")
                            .required(false)
                              .num_args(1..)
                            .default_value("BTCUSDT")

                    )
                    .arg(
                        arg!(--ktf <KLINE_TF> "Kline timeframe")
                            .required(false)
                            .default_value("5m")

                    )
                    .arg(
                        arg!(--loghistory "Log tftrades history from agg trades")
                            .required(false)
                            
                    )
          ).subcommand(Command::new("download")
          .arg(
              arg!(--ktf <KLINE_TF> "Kline timeframe")
                    .required(false)
                    .default_value("5m")
      
          ).arg(
              arg!(--tf <TF> "Chunk timeframe in seconds")
                    .required(false)
                    .default_value("30")
                    .value_parser(value_parser!(u64))
      
          ) .arg(
        arg!(-s --symbols <SYMBOLS> "The instruments to download")
              .required(false)
              .num_args(1..)
              .default_value("BTCUSDT")

    ).arg(
        arg!(-l --length <SECONDS> "The span of the download in seconds")
              .required(false)
              .default_value("-1")
              .value_parser(value_parser!(i64)),
    )
    .arg(
             arg!(--nokline "Don't download klines")
                              .required(false)
    ).arg(arg!(--noaggtrades "Don't download aggtrades")
          .required(false))
          .about("download candles"))
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
                              .default_value("BTCUSDT")
                
                    )
                    .arg(
                        arg!(--ktf <KLINE_TF> "Kline timeframe")
                              .required(false)
                              .default_value("15m")
                
                    ))
          .get_matches();
    
    if let Some(matches) = main_matches.subcommand_matches("backtest") {
        
        let mode = matches.get_one::<String>("mode").unwrap();
        let ktf = matches.get_one::<String>("ktf").unwrap();
        let mut symbols = matches.get_many::<String>("symbols").unwrap().clone();
        let backtest_span = *matches.get_one::<u64>("length").ok_or(anyhow::anyhow!("Invalid span"))?;
        let tf1 = *matches.get_one::<u64>("timeframe1").ok_or(anyhow::anyhow!("Invalid tf1"))?;
        let tf2 = *matches.get_one::<u64>("timeframe2").ok_or(anyhow::anyhow!("Invalid tf2"))?;
        let tf3 = *matches.get_one::<u64>("timeframe3").ok_or(anyhow::anyhow!("Invalid tf3"))?;
        let pb = ProgressBar::new(0);
        pb.set_style(ProgressStyle::with_template("[?] [{elapsed_precise}] [{wide_bar:.cyan/blue}] {percent}%}")
              .unwrap()
              .with_key("eta", |state: &ProgressState, w: &mut dyn Write| write!(w, "{:.1}s", state.eta().as_secs_f64()).unwrap())
              .progress_chars("#>-"));
    
        let orders_channel: (Sender<Order>, Receiver<Order>) = async_broadcast::broadcast(1000);
        let tf_trades_channel: (Sender<TfTrades>, Receiver<TfTrades>) = async_broadcast::broadcast(10000);
        let trades_channel: (Sender<Trade>, Receiver<Trade>) = async_broadcast::broadcast(1000);
        
        if mode == "single" {
            let symbol = Symbol {
                symbol: symbols.next().unwrap().clone(),
                exchange: ExchangeId::Simulated,
                base_asset_precision: 1,
                quote_asset_precision: 2
            };
            let global_config = GlobalConfig {
                symbol: symbol.clone(),
                tf1,
                tf2,
                tf3,
                key: KEY.clone(),
                verbose: main_matches.get_flag("verbose"),
                mode: Mode::Backtest,
            };
            
            let back_tester_config = BackTesterConfig {
                symbol: symbol.clone(),
                length: backtest_span,
                log_history: matches.get_flag("loghistory"),
                grpc_server_port: 50011.to_string(),
                kline_tf: ktf.clone()
            };
            let back_tester = BackTester::new(global_config, back_tester_config, vec![symbol.clone()]);

            let start = std::time::SystemTime::now();
    
            let executor:  Box<Arc<dyn TradeExecutor<Account = SimulatedAccount>>> = Box::new(Arc::new(binance_q_executors::simulated::SimulatedExecutor::new(
                orders_channel.1,
                tf_trades_channel.1,
                vec![symbol.clone()],
                trades_channel.0,
            )
                  .await));
            let s_e = executor.clone();
            std::thread::spawn(move || {
                s_e.listen().unwrap();
            });
            back_tester.run(pb.clone(), trades_channel.1, tf_trades_channel.0, orders_channel.0, executor).await?;
            pb.finish_with_message(format!(
                "[+] back_tester > Back-test finished in {:?} seconds",
                start.elapsed().unwrap()
            ));
        } else if mode == "multi" {
            let mut b_configs = vec![];
            let mut symbols_i: Vec<Symbol> = vec![];
            let mut grpc_server = 50011;
            for symbol in symbols {
                let symbol = Symbol {
                    symbol: symbol.clone(),
                    exchange: ExchangeId::Simulated,
                    base_asset_precision: 1,
                    quote_asset_precision: 2
                };
                symbols_i.push(symbol.clone());
                let back_tester_config = BackTesterConfig {
                    symbol,
                    length: backtest_span,
                    log_history: matches.get_flag("loghistory"),
                    grpc_server_port: grpc_server.to_string(),
                    kline_tf: ktf.clone(),
                    
                };
                grpc_server += 1;
                b_configs.push(back_tester_config);
            }
    
            let global_config = GlobalConfig {
                symbol: b_configs.first().unwrap().symbol.clone(),
                tf1,
                tf2,
                tf3,
                key: KEY.clone(),
                mode: Mode::Backtest,
                verbose: main_matches.get_flag("verbose")
            };
            
    
            let executor: Box<Arc<dyn TradeExecutor<Account = SimulatedAccount>>> = Box::new(Arc::new(binance_q_executors::simulated::SimulatedExecutor::new(
                orders_channel.1,
                tf_trades_channel.1,
                symbols_i.clone(),
                trades_channel.0,
            )
                  .await));
            let s_e = executor.clone();
            std::thread::spawn(move || {
                s_e.listen().unwrap();
            });
            
            let back_tester = BackTesterMulti::new(global_config, b_configs, symbols_i);
            let start = std::time::SystemTime::now();
            back_tester.run(pb.clone(), trades_channel.1, tf_trades_channel.0, orders_channel.0, executor).await?;
    
            pb.finish_with_message(format!(
                "[+] back_tester > Back-test finished in {:?} seconds",
                start.elapsed().unwrap()
            ));
        }
        
        
    
    }
    
    if let Some(matches) = main_matches.subcommand_matches("download") {
    
        let symbols = matches.get_many::<String>("symbols").unwrap().clone();
        let tf1 = matches.get_one::<String>("ktf").ok_or(anyhow::anyhow!("Invalid timeframe"))?.clone();
        let tf = matches.get_one::<u64>("tf").ok_or(anyhow::anyhow!("Invalid timeframe"))?.clone();
        let l = matches.get_one::<i64>("length").ok_or(anyhow::anyhow!("Invalid timeframe"))?.clone();
        let download_klines = !matches.get_flag("nokline");
        let download_trades = !matches.get_flag("noaggtrades");
        let mut threads = vec![];
        let pb = ProgressBar::new(0);
        let verbose = main_matches.get_flag("verbose");
    
        pb.set_style(ProgressStyle::with_template("[?] Progress [{elapsed_precise}] [{wide_bar:.cyan/blue}] {percent}%}")
              .unwrap()
              .with_key("eta", |state: &ProgressState, w: &mut dyn Write| write!(w, "{:.1}s", state.eta().as_secs_f64()).unwrap())
              .progress_chars("#>-"));
    
        for s in symbols.into_iter() {
            let symbol = Symbol {
                symbol: s.clone(),
                exchange: ExchangeId::Simulated,
                base_asset_precision: 1,
                quote_asset_precision: 2
            };
            let ktf = tf1.clone();
            let lpb = pb.clone();
            threads.push(tokio::spawn(async move {
                    if download_klines {
    
                        load_klines_from_archive(symbol.clone(), ktf, l, lpb.clone(), verbose).await;
                    }
                if download_trades {
    
                    load_history_from_archive(symbol.clone(),l, tf, lpb.clone(), verbose).await;
                }
                    println!("[+] download > {} data downloaded", symbol.symbol.clone());
            }))
        }
        futures::future::join_all(threads).await;
        
    
    
        
    }
    
    if let Some(matches) = main_matches.subcommand_matches("live") {
        let mut symbols = matches.get_many::<String>("symbols").unwrap().clone();
        let ktf = matches.get_one::<String>("ktf").unwrap().clone();
        let length = matches.get_one::<u64>("length").unwrap().clone();
        let tf1 = *matches.get_one::<u64>("timeframe1").ok_or(anyhow::anyhow!("Invalid tf1"))?;
        
        let orders_channel: (Sender<Order>, Receiver<Order>) = async_broadcast::broadcast(1000);
        let tf_trades_channel: (Sender<TfTrades>, Receiver<TfTrades>) = async_broadcast::broadcast(10000);
        let trades_channel: (Sender<Trade>, Receiver<Trade>) = async_broadcast::broadcast(1000);
        let from_time = SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as u64 - length;
        let mut syms = vec![];
        let general = FuturesGeneral::new(None, None);
        for s in symbols.clone() {
            let asset = general.get_symbol_info(s.clone()).await.unwrap();
            syms.push(Symbol {
                symbol: s.clone(),
                exchange: ExchangeId::Binance,
                base_asset_precision: asset.quantity_precision as u32,
                quote_asset_precision: asset.price_precision as u32
            });
        };
        let executor: Box<Arc<dyn TradeExecutor<Account=BinanceLiveAccount>>> = Box::new(Arc::new(binance_q_executors::live::BinanceLiveExecutor::new(
            KEY.clone(),
            orders_channel.1,
            tf_trades_channel.1,
            syms.clone(),
            trades_channel.0,
        )
              .await));
        let s_e = executor.clone();
        std::thread::spawn(move || {
            s_e.listen().unwrap();
        });
    
        let mut grpc_server = 50011;
        let mut workers = vec![];
        let verbose = main_matches.get_flag("verbose");
        for symbol in syms {
    
            let t_r = trades_channel.1.clone();
            let t_t = tf_trades_channel.0.clone();
            let o_t = orders_channel.0.clone();
            let e = executor.clone();
            let ktf = ktf.clone();
            let global_config = GlobalConfig {
                symbol: symbol.clone(),
                tf1,
                tf2: tf1,
                tf3: tf1,
                key: KEY.clone(),
                verbose,
                mode: Mode::Live
            };
            workers.push(std::thread::spawn(move || {
                let runtime = tokio::runtime::Builder::new_multi_thread()
                      .enable_all()
                      .build()
                      .unwrap();
                runtime.block_on(async move {
    
                    let done = Arc::new(std::sync::RwLock::new(false));
                    let bd = done.clone();
                    let s = symbol.symbol.clone();
                    std::thread::spawn( move || {
                        println!("[?] live > Launching Signal Generators for {}", s);
        
                        let mut process = Exec::cmd("python3")
                              .arg("./python/signals/signal_generator.py")
                              .arg("--symbol")
                              .arg(&s)
                              .arg("--grpc-port")
                              .arg(&grpc_server.to_string())
                              .arg("--mode")
                              .arg("Live")
                              .popen()
                              .unwrap();
                        loop {
                            if bd.read().unwrap().clone() {
                                process.kill().unwrap();
                                break;
                            }
                            std::thread::sleep(Duration::from_millis(2000));
                        }
                    });
    
                    tokio::time::sleep(Duration::from_secs(5)).await;
    
                    let klines_channel = async_broadcast::broadcast(10000);
                    let execution_commands_channel = async_broadcast::broadcast(100);
    
    
    
                    let inner_account: Box<Arc<dyn ExchangeAccount>> =
                          Box::new(e.get_account().clone());
                    println!("[?] live > Initializing Risk Manager for {}",  symbol.symbol);
                    // calculate position size based on risk tolerance and send orders to executors
                    let mut risk_manager = RiskManager::new(
                        global_config.clone(),
                        RiskManagerConfig {
                            max_daily_losses: 100,
                            max_risk_per_trade: 0.01,
                        },
                        klines_channel.1.clone(),
                        t_r,
                        execution_commands_channel.1,
                        inner_account,
                    );
    
    
                    let inner_account: Box<Arc<dyn ExchangeAccount>> =
                          Box::new(e.get_account().clone());
    
                    // receive strategy edges from multiple strategies and forward them to risk manager
                    println!("[?] live > Initializing Strategy Manager for {}",  symbol.symbol);
                    let mut strategy_manager =
                          StrategyManager::new(
                              StrategyManagerConfig{ symbol: global_config.symbol.clone() },
                              global_config.clone(),
                              klines_channel.1,
                              grpc_server.to_string(),
                              inner_account
                          ).await;
    
                    
                    let mut trade_emitter = TfTradeEmitter::new(tf1, global_config.clone());
                    
                    let mut kline_emitter = KlineEmitter::new(ktf.clone(), global_config.clone());
                    
                    trade_emitter.subscribe(t_t).await;
                    kline_emitter.subscribe(klines_channel.0).await;
                    
                    risk_manager.subscribe(o_t).await;
    
                    println!("[?] live > Starting Listeners {}",  symbol.symbol);
                    strategy_manager
                          .subscribe(execution_commands_channel.0)
                          .await;
                    let mut r_threads = vec![];
                    let rc = risk_manager.clone();
                    r_threads.push(std::thread::spawn(move || {
                        EventSink::<ExecutionCommand>::listen(&rc).unwrap();
                    }));
                    let rc = risk_manager.clone();
                    r_threads.push(std::thread::spawn(move || {
                        EventSink::<Trade>::listen(&rc).unwrap();
                    }));
                    let sm = strategy_manager.clone();
                    r_threads.push(std::thread::spawn(move || {
                        EventSink::<Kline>::listen(&sm).unwrap();
                    }));
                    let rc_1 = risk_manager.clone();
                    r_threads.push(std::thread::spawn(move || {
                        EventSink::<Kline>::listen(&rc_1).unwrap();
                    }));

                    
                    let mut futures = FuturesUnordered::new();
                    futures.push(start_loader(symbol.clone(), tf1).await);
                    futures.push(start_kline_loader(symbol.clone(), ktf.clone(),from_time).await);
                    futures.push(strategy_manager.emit().await.unwrap());
                    futures.push(risk_manager.emit().await.unwrap());
                    futures.push(e.emit().await.unwrap());
                    futures.push(trade_emitter.emit().await.unwrap());
                    futures.push(kline_emitter.emit().await.unwrap());

                    while let Some(done) = futures.next().await {
                        continue
                    }
                    for t in r_threads {
                        t.join().unwrap();
                    }
                });
            }));
            
            grpc_server += 1;
            
        }
        drop(trades_channel.1);
        for worker in workers {
            worker.join().unwrap();
        }
    }
    
    
    
    Ok(())
}
