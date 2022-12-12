#![feature(iterator_try_collect)]
#![feature(async_closure)]

mod back_tester;

use once_cell::sync::Lazy;
use std::env;
use binance_q_types::{AccessKey, ExchangeId, GlobalConfig, Symbol};

use clap::{arg, command, Command, value_parser};
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
    let matches = command!()
          .subcommand(
              Command::new("backtest")
                    .about("Backtest a strategy")
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
                        arg!(-s --symbol <SYMBOL> "The symbol to backtest")
                            .required(false)
                            .default_value("BTCUSDT")
                    )
          ).get_matches();
    
    if let Some(matches) = matches.subcommand_matches("backtest") {
        let symbol = matches.get_one::<String>("symbol").unwrap().clone();
        let backtest_span = *matches.get_one::<u64>("length").ok_or(anyhow::anyhow!("Invalid span"))?;
        let tf1 = *matches.get_one::<u64>("timeframe1").ok_or(anyhow::anyhow!("Invalid tf1"))?;
        let tf2 = *matches.get_one::<u64>("timeframe2").ok_or(anyhow::anyhow!("Invalid tf2"))?;
        let tf3 = *matches.get_one::<u64>("timeframe3").ok_or(anyhow::anyhow!("Invalid tf3"))?;
        let symbol = Symbol {
            symbol,
            exchange: ExchangeId::Simulated,
            base_asset_precision: 1,
            quote_asset_precision: 2
        };
        let global_config = GlobalConfig {
            symbol: symbol.clone(),
            tf1,
            tf2,
            tf3,
            key: KEY.clone()
        };
        let back_tester_config = BackTesterConfig {
            symbol,
            length: backtest_span,
        };
        let back_tester = BackTester::new(global_config, back_tester_config);
        back_tester.run().await?;
        
    
    } else {
        
    
    
        
    }
    

    Ok(())
}
