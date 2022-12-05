#![feature(iterator_try_collect)]
#![feature(async_closure)]
use std::{env};
use once_cell::sync::Lazy;
use crate::events::{EventEmitter, EventSink, TfTradeEmitter};

use crate::managers::money_manager::{MoneyManager, MoneyManagerConfig, PositionSizeF};
use crate::managers::risk_manager::{ExecutionCommand, RiskManager, RiskManagerConfig};
use crate::managers::strategy_manager::StrategyManager;
use crate::strategies::chop_directional::ChopDirectionalStrategy;
use crate::strategies::random_strategy::RandomStrategy;
use crate::studies::{atr_study::ATRStudy, Study, StudyConfig};
use crate::studies::choppiness_study::ChoppinessStudy;
use crate::types::{AccessKey, GlobalConfig, TfTrades};

mod cmds;
mod bracket_order;
mod mongodb;
mod studies;
mod managers;
mod helpers;
mod errors;
mod market_classifier;
mod loader;
mod events;
mod types;
mod strategies;




static KEY: Lazy<AccessKey> = Lazy::new(|| {
    let api_key = env::var("API_KEY").unwrap_or("ftcpi3OSjk26htxak54hqkZ6e9vdHq2Vd7oN83VZN39UcYmw1VwVkibug52oGIs4".to_string());
    let secret_key = env::var("SECRET_KEY").unwrap_or("iXArQEDFfmFanIfz7RfJj6G034b76nDuNetqxdqSLiUwPqDtLGIaNYK4TgDxR9H4".to_string());
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
    

    let mongo_client = mongodb::client::MongoClient::new().await;
    mongo_client.reset_db().await;
    
    let global_config = GlobalConfig {
        tf1: 1,
        tf2: 50,
        tf3: 300,
        key: KEY.clone()
    };
    
    let trades_channel = kanal::bounded_async(1000);
    let execution_commands_channel = kanal::bounded_async(100);
    
    let risk_manager = RiskManager::new(global_config.clone(), RiskManagerConfig {
        max_daily_losses: 100,
        max_risk_per_trade: 0.01,
    }, trades_channel.1.clone(), execution_commands_channel.1.clone()).await;
    let money_manager = MoneyManager::new(global_config.clone(), MoneyManagerConfig {
        position_size_f: PositionSizeF::Kelly,
        min_position_size: None,
        constant_size: None,
        max_position_size: None,
        leverage: 0,
        initial_quote_balance: 0.0
    }, trades_channel.1.clone()).await;
    
    
    
    
    
    
    
    let config = StudyConfig {
        symbol: "XRPUSTDT".to_string(),
        range: 10,
        tf1: 1,
        tf2: 60,
        tf3: 300
    };
    
    // initialize studies
    let atr_study = ATRStudy::new( &config, trades_channel.1.clone());
    let choppiness_study = ChoppinessStudy::new( &config, trades_channel.1.clone());
    
    
    // initialize used strategies
    let chop_strategy = ChopDirectionalStrategy::new(global_config.clone(), atr_study.clone(), choppiness_study.clone());
    let rand_strategy = RandomStrategy::new();
    
    
    let mut strategy_manager = StrategyManager::new(global_config.clone())
          .with_strategy(chop_strategy).await
          .with_exit_strategy(rand_strategy).await;
    

    strategy_manager.subscribe(execution_commands_channel.0.clone()).await;
    let mut threads = vec![];
    
    loader::load_history(KEY.clone(), "DOGEUSDT".to_string(),  12 * 60 * 60 * 1000).await;
    println!("Historical data loaded");
    
    // start tf trade emitters
    for tf in vec![global_config.tf1, global_config.tf2, global_config.tf3] {
        let mut tf_trade = TfTradeEmitter::new(tf);
        tf_trade.subscribe(trades_channel.0.clone()).await;
        threads.push(tf_trade.emit().await?);
    }
    
    
    threads.push(loader::start_loader(KEY.clone(), "DOGEUSDT".to_string(), 1).await);
    threads.push(atr_study.listen().await?);
    threads.push(choppiness_study.listen().await?);
    threads.push(strategy_manager.emit().await?);
    threads.push(EventSink::<ExecutionCommand>::listen(&risk_manager).await?);
    threads.push(EventSink::<TfTrades>::listen(&risk_manager).await?);
    threads.push(money_manager.listen().await?);
    
    //Todo: start event emitters and sinks on different runtimes
    futures::future::join_all(threads).await;

    Ok(())
    
}
