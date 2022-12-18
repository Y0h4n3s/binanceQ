#![feature(iterator_try_collect)]
#![feature(async_closure)]

mod back_tester;

use once_cell::sync::Lazy;
use std::env;
use std::sync::Mutex;
use async_std::sync::Arc;
use binance_q_mongodb::loader::{load_history_from_archive, load_klines_from_archive};
use binance_q_types::{AccessKey, ExchangeId, GlobalConfig, Order, Symbol, TfTrades, Trade};
use std::fmt::Write;
use async_broadcast::{Receiver, Sender};
use clap::{arg, command, Command, value_parser};
use clap::builder::TypedValueParser;
use indicatif::{ProgressBar, ProgressState, ProgressStyle};
use binance_q_executors::simulated::SimulatedAccount;
use binance_q_executors::TradeExecutor;
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
                            
                    
          )).subcommand(Command::new("download")
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
                verbose: main_matches.get_flag("verbose")
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
                    kline_tf: ktf.clone()
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
                verbose: main_matches.get_flag("verbose")
            };
            
    
            let executor: Box<Arc<dyn TradeExecutor<Account = SimulatedAccount>>> = Box::new(Arc::new(binance_q_executors::simulated::SimulatedExecutor::new(
                orders_channel.1,
                tf_trades_channel.1,
                symbols_i.clone(),
                trades_channel.0,
            )
                  .await));
            
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
        pb.set_style(ProgressStyle::with_template("[?] [{elapsed_precise}] [{wide_bar:.cyan/blue}] {percent}%}")
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
            let lpb = pb.clone()
            threads.push(tokio::spawn(async move {
                    if download_klines {
    
                        load_klines_from_archive(symbol.clone(), ktf, l, lpb.clone()).await;
                    }
                if download_trades {
    
                    load_history_from_archive(symbol.clone(),l, tf, lpb.clone()).await;
                }
                    println!("[+] download > {} data downloaded", symbol.symbol.clone());
            }))
        }
        futures::future::join_all(threads).await;
        
    
    
        
    }
    

    Ok(())
}
