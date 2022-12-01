#![feature(iterator_try_collect)]
use std::{env};
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use once_cell::sync::Lazy;
use crate::events::{EventEmitter, EventSink, TfTradeEmitter};

use crate::managers::money_manager::{MoneyManager, MoneyManagerConfig, PositionSizeF};
use crate::managers::risk_manager::{RiskManager, RiskManagerConfig};
use crate::managers::strategy_manager::StrategyManager;
use crate::market_classifier::{MarketClassifer, MarketClassiferConfig};
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

const MARKET: [&str;4] = ["BTCUSDT","SOLUSDT","XRPUSDT","APTUSDT"];
const MARKET_QTY_PRECISION: [u32;4] = [3,0,1,1];
const MARKET_PRICE_PRECISION: [u32;4] = [1,2,4,3];
const BASE_SIZE: f64 = 0.05;






static KEY: Lazy<AccessKey> = Lazy::new(|| {
    let api_key = env::var("API_KEY").unwrap_or("ftcpi3OSjk26htxak54hqkZ6e9vdHq2Vd7oN83VZN39UcYmw1VwVkibug52oGIs4".to_string());
    let secret_key = env::var("SECRET_KEY").unwrap_or("iXArQEDFfmFanIfz7RfJj6G034b76nDuNetqxdqSLiUwPqDtLGIaNYK4TgDxR9H4".to_string());
    AccessKey {
        api_key,
        secret_key,
    }
});


fn main() {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async_main());
}
async fn async_main() {
    

    let mongo_client = mongodb::client::MongoClient::new().await;
    mongo_client.reset_db().await;
    let global_config = GlobalConfig {
        tf1: 1,
        tf2: 50,
        tf3: 300,
        key: KEY.clone()
    };
    
    let trades_channel = kanal::bounded_async(100);
    let execution_commands_channel = kanal::bounded_async(100);
    let risk_manager = RiskManager::new(global_config.clone(), RiskManagerConfig {
        max_daily_losses: 100,
        max_risk_per_trade: 0.01,
    }, trades_channel.1.clone(), execution_commands_channel.1.clone());
    
    let money_manager = MoneyManager::new(global_config.clone(), MoneyManagerConfig {
        position_size_f: PositionSizeF::Kelly,
        min_position_size: None,
        constant_size: None,
        max_position_size: None,
        leverage: 0,
        initial_quote_balance: 0.0
    }, trades_channel.1.clone());
    let mut tf_trades = vec![];
    for tf in vec![global_config.tf1, global_config.tf2, global_config.tf3] {
        let mut tf_trade = TfTradeEmitter::new(tf);
        tf_trade.subscribe(trades_channel.0.clone());
        tf_trades.push(tf_trade);
    }
    
    
    
    
    let config = StudyConfig {
        symbol: "XRPUSTDT".to_string(),
        tf1: 1,
        tf2: 60,
        tf3: 300
    };
    let atr_study = ATRStudy::new(global_config.clone(), &config, trades_channel.1.clone());
    let choppiness_study = ChoppinessStudy::new(global_config.clone(), &config, trades_channel.1.clone());
    let chop_strategy = ChopDirectionalStrategy::new(global_config.clone(), Box::new(atr_study.clone()), Box::new(choppiness_study));
    let rand_strategy = RandomStrategy::new(global_config.clone());
    let mut strategy_manager = StrategyManager::new(global_config.clone())
          .with_strategy(chop_strategy).await
          .with_exit_strategy(rand_strategy).await;
    

    strategy_manager.subscribe(execution_commands_channel.0.clone());
    let mut threads = vec![];
    loader::load_history(KEY.clone(), "DOGEUSDT".to_string(),  12 * 60 * 60 * 1000);
    println!("History loader finished");

    
    threads.push(loader::start_loader(KEY.clone(), "DOGEUSDT".to_string(), 1).await);
    threads.push(atr_study.listen().await);
    // futures.push( &choppiness_study.listen());
    
    
    futures::future::join_all(threads).await;

    
}
