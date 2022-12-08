#![feature(iterator_try_collect)]
#![feature(async_closure)]
use crate::events::{EventEmitter, EventSink, TfTradeEmitter};
use once_cell::sync::Lazy;
use std::env;
use ::mongodb::bson::doc;
use ::mongodb::options::FindOptions;

use crate::managers::risk_manager::{ExecutionCommand, RiskManager, RiskManagerConfig};
use crate::managers::strategy_manager::StrategyManager;
use crate::strategies::chop_directional::ChopDirectionalStrategy;
use crate::strategies::random_strategy::RandomStrategy;
use crate::studies::choppiness_study::ChoppinessStudy;
use crate::studies::{atr_study::ATRStudy, Study, StudyConfig};
use crate::studies::directional_index_study::DirectionalIndexStudy;
use crate::types::{AccessKey, GlobalConfig, TfTrades};
use clap::{arg, command, value_parser, ArgAction, Command};
use crate::backtester::{BackTester, BackTesterConfig};

mod bracket_order;
mod cmds;
mod errors;
mod events;
mod helpers;
mod loader;
mod managers;
mod market_classifier;
mod mongodb;
mod strategies;
mod studies;
mod types;
mod executors;
mod backtester;

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
    let runtime = tokio::runtime::Runtime::new().unwrap();
    tracing_subscriber::fmt::try_init().unwrap();
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
                    )
                    .arg(
                        arg!(--timeframe1 <SECONDS> "The first timeframe to use")
                            .required(true)
                    )
                    .arg(
                        arg!(--timeframe2 <SECONDS> "The second timeframe to use")
                            .required(true)
                    )
                    .arg(
                        arg!(--timeframe3 <SECONDS> "The third timeframe to use")
                            .required(true)
                    )
                    .arg(
                        arg!(-s --symbol <SYMBOL> "The symbol to backtest")
                            .required(false)
                            .default_value("BTCUSDT")
                    )
          ).get_matches();
    
    if let Some(matches) = matches.subcommand_matches("backtest") {
        let backtest_span = *matches.get_one::<u64>("length").ok_or(anyhow::anyhow!("Invalid span"))?;
        let symbol = matches.get_one::<String>("symbol").unwrap().clone();
        let tf1 = *matches.get_one::<u64>("timeframe1").ok_or(anyhow::anyhow!("Invalid tf1"))?;
        let tf2 = *matches.get_one::<u64>("timeframe2").ok_or(anyhow::anyhow!("Invalid tf2"))?;
        let tf3 = *matches.get_one::<u64>("timeframe3").ok_or(anyhow::anyhow!("Invalid tf3"))?;
        
        let global_config = GlobalConfig {
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
        back_tester.run().await;
        
    
    } else {
        
    
        let global_config = GlobalConfig {
            tf1: 1,
            tf2: 60,
            tf3: 300,
            key: KEY.clone(),
        };

    
        let config = StudyConfig {
            symbol: "XRPUSTDT".to_string(),
            range: 10,
            tf1: 1,
            tf2: 60,
            tf3: 300,
        };
    
        
    }
    

    Ok(())
}
