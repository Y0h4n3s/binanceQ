#![feature(iterator_try_collect)]
#![feature(async_closure)]

mod back_tester;

use once_cell::sync::Lazy;
use std::env;
use binance_q_mongodb::loader::load_klines_from_archive;
use binance_q_types::{AccessKey, ExchangeId, GlobalConfig, Symbol};

use clap::{arg, command, Command, value_parser};
use clap::builder::TypedValueParser;
use crate::back_tester::{BackTester, BackTesterConfig};


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
                        arg!(--loadhistory "Load history from binance")
                            .required(false)
                            
                    
          )).subcommand(Command::new("download")
          .arg(
              arg!(--timeframe <TIMEFRAME> "The first timeframe to use")
                    .required(true)
      
          )   .arg(
        arg!(-s --symbol <SYMBOL> "The symbol to backtest")
              .required(false)
              .default_value("BTCUSDT")
    )
          .about("download candles"))
          .get_matches();
    
    if let Some(matches) = main_matches.subcommand_matches("backtest") {
        
        let mode = matches.get_one::<String>("mode").unwrap();
        let mut symbols = matches.get_many::<String>("symbols").unwrap().clone();
        let backtest_span = *matches.get_one::<u64>("length").ok_or(anyhow::anyhow!("Invalid span"))?;
        let tf1 = *matches.get_one::<u64>("timeframe1").ok_or(anyhow::anyhow!("Invalid tf1"))?;
        let tf2 = *matches.get_one::<u64>("timeframe2").ok_or(anyhow::anyhow!("Invalid tf2"))?;
        let tf3 = *matches.get_one::<u64>("timeframe3").ok_or(anyhow::anyhow!("Invalid tf3"))?;
    
        
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
                symbol,
                length: backtest_span,
                load_history: matches.get_flag("loadhistory")
            };
            let back_tester = BackTester::new(global_config, back_tester_config);
            back_tester.run().await?;
        } else if mode == "multi" {
            let mut back_testers = vec![];
            for symbol in symbols {
                let symbol = Symbol {
                    symbol: symbol.clone(),
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
                    symbol,
                    length: backtest_span,
                    load_history: matches.get_flag("loadhistory")
                };
                let back_tester = BackTester::new(global_config, back_tester_config);
                back_testers.push(back_tester);
            }
        }
        
        
    
    }
    
    if let Some(matches) = main_matches.subcommand_matches("download") {
    
        let symbol = matches.get_one::<String>("symbol").unwrap().clone();
        let tf1 = matches.get_one::<String>("timeframe").ok_or(anyhow::anyhow!("Invalid tf1"))?.clone();
    
        let symbol = Symbol {
            symbol,
            exchange: ExchangeId::Simulated,
            base_asset_precision: 1,
            quote_asset_precision: 2
        };
        load_klines_from_archive(symbol, tf1, -1).await;
    
    
        
    }
    

    Ok(())
}
