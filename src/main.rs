#![feature(iterator_try_collect)]
#![feature(async_closure)]
use crate::events::{EventEmitter, EventSink, TfTradeEmitter};
use once_cell::sync::Lazy;
use std::env;

use crate::managers::money_manager::{MoneyManager, MoneyManagerConfig, PositionSizeF};
use crate::managers::risk_manager::{ExecutionCommand, RiskManager, RiskManagerConfig};
use crate::managers::strategy_manager::StrategyManager;
use crate::strategies::chop_directional::ChopDirectionalStrategy;
use crate::strategies::random_strategy::RandomStrategy;
use crate::studies::choppiness_study::ChoppinessStudy;
use crate::studies::{atr_study::ATRStudy, Study, StudyConfig};
use crate::studies::directional_index_study::DirectionalIndexStudy;
use crate::types::{AccessKey, GlobalConfig, TfTrades};
use clap::{arg, command, value_parser, ArgAction, Command};

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
        let mongo_client = mongodb::client::MongoClient::new().await;
        mongo_client.reset_db().await;
    
        loader::load_history(KEY.clone(), symbol.clone(), backtest_span * 1000).await;
    
    } else {
        
    
        let global_config = GlobalConfig {
            tf1: 1,
            tf2: 60,
            tf3: 300,
            key: KEY.clone(),
        };
    
        let trades_channel = kanal::bounded_async(1000);
        let execution_commands_channel = kanal::bounded_async(100);
    
        let risk_manager = RiskManager::new(
            global_config.clone(),
            RiskManagerConfig {
                max_daily_losses: 100,
                max_risk_per_trade: 0.01,
            },
            trades_channel.1.clone(),
            execution_commands_channel.1.clone(),
        )
              .await;
        let money_manager = MoneyManager::new(
            global_config.clone(),
            MoneyManagerConfig {
                position_size_f: PositionSizeF::Kelly,
                min_position_size: None,
                constant_size: None,
                max_position_size: None,
                leverage: 0,
                initial_quote_balance: 0.0,
            },
            trades_channel.1.clone(),
        )
              .await;
    
        let config = StudyConfig {
            symbol: "XRPUSTDT".to_string(),
            range: 10,
            tf1: 1,
            tf2: 60,
            tf3: 300,
        };
    
        let mut threads = vec![];
    
    
        loader::load_history(KEY.clone(), "DOGEUSDT".to_string(), 1 * 30 * 60 * 1000).await;
        println!("Historical data loaded");
        threads.push(loader::start_loader(KEY.clone(), "DOGEUSDT".to_string(), 1).await);
    
    
        // start tf trade emitters, they should be started before any study is initialized because studies depend on them
        for tf in vec![global_config.tf1, global_config.tf2, global_config.tf3] {
            let mut tf_trade = TfTradeEmitter::new(tf);
            tf_trade.subscribe(trades_channel.0.clone()).await;
            threads.push(tf_trade.emit().await?);
        }
        println!("Tf trade emitters started");
    
        // initialize studies
        let atr_study = ATRStudy::new(&config, trades_channel.1.clone());
        let choppiness_study = ChoppinessStudy::new(&config, trades_channel.1.clone());
        let adi_study = DirectionalIndexStudy::new(&config, trades_channel.1.clone());
        atr_study.log_history().await?;
        choppiness_study.log_history().await?;
        println!("Studies initialized");
    
        // initialize used strategies
        let chop_strategy = ChopDirectionalStrategy::new(
            global_config.clone(),
            atr_study.clone(),
            choppiness_study.clone(),
        );
        let rand_strategy = RandomStrategy::new();
    
        let mut strategy_manager = StrategyManager::new(global_config.clone())
              .with_strategy(chop_strategy)
              .await
              .with_exit_strategy(rand_strategy)
              .await;
    
        strategy_manager
              .subscribe(execution_commands_channel.0.clone())
              .await;
    
    
    
        threads.push(atr_study.listen().await?);
        threads.push(choppiness_study.listen().await?);
        threads.push(strategy_manager.emit().await?);
        threads.push(EventSink::<ExecutionCommand>::listen(&risk_manager).await?);
        threads.push(EventSink::<TfTrades>::listen(&risk_manager).await?);
        threads.push(money_manager.listen().await?);
    
        //Todo: start event emitters and sinks on different runtimes
        futures::future::join_all(threads).await;
    }
    

    Ok(())
}
