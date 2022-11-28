#![feature(iterator_try_collect)]

use std::{env};
use once_cell::sync::Lazy;

use crate::managers::money_manager::{MoneyManager, MoneyManagerConfig, PositionSizeF};
use crate::managers::risk_manager::{RiskManager, RiskManagerConfig};
use crate::market_classifier::{MarketClassifer, MarketClassiferConfig};
use crate::studies::{atr_study::ATRStudy, Study, StudyConfig};
use crate::studies::choppiness_study::ChoppinessStudy;
use crate::types::AccessKey;

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
    let config = StudyConfig {
        symbol: "XRPUSTDT".to_string(),
        tf1: 1,
        tf2: 60,
        tf3: 300
    };
    let atr_study = ATRStudy::new(KEY.clone(), &config);
    let choppiness_study = ChoppinessStudy::new(KEY.clone(), &config);
    
    let mut threads = vec![];
    loader::load_history(KEY.clone(), "DOGEUSDT".to_string(),  12 * 60 * 60 * 1000);
    println!("History loader finished");
    
    threads.push(loader::start_loader(KEY.clone(), "DOGEUSDT".to_string(), 1).await);
    std::iter::Extend::extend(&mut threads, atr_study.start_log().await);
    std::iter::Extend::extend(&mut threads, choppiness_study.start_log().await);
    
    futures::future::join_all(threads).await;
    

    
}
