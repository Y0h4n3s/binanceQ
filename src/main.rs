mod cmds;
mod bracket_order;
mod mongodb;
mod studies;
mod managers;
mod helpers;
mod errors;
mod market_classifier;
mod loader;

use std::{env, io};
use std::cmp::Ordering;
use std::mem::take;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use async_std::stream::Extend;
use binance::account::{Account, OrderSide, TimeInForce};
use binance::api::Binance;
use binance::futures::account::{CustomOrderRequest, FuturesAccount, OrderType};
use binance::futures::market::FuturesMarket;
use binance::futures::model::AccountBalance;
use binance::market::Market;
use binance::model::SymbolPrice;
use console::Term;
use once_cell::sync::Lazy;
use rust_decimal::prelude::*;
use crate::managers::risk_manager::{RiskManager, RiskManagerConfig};
use crate::studies::{Study, StudyConfig, StudyTypes};
use futures::executor::block_on;
use crate::managers::Manager;
use crate::managers::money_manager::{MoneyManager, MoneyManagerConfig, PositionSizeF};
use crate::market_classifier::{MarketClassifer, MarketClassiferConfig};

const MARKET: [&str;4] = ["BTCUSDT","SOLUSDT","XRPUSDT","APTUSDT"];
const MARKET_QTY_PRECISION: [u32;4] = [3,0,1,1];
const MARKET_PRICE_PRECISION: [u32;4] = [1,2,4,3];
const BASE_SIZE: f64 = 0.05;
struct Config {
    stop_loss_percent: f64,
    target_percent: f64,
    stop_loss: f64,
    target: f64,
    qty_precision: usize,
    price_precision: usize,
    traded_market: String,
    margin: f64,
}

#[derive(Debug, Clone)]
pub struct AccessKey {
    api_key: String,
    secret_key: String,
}



static KEY: Lazy<AccessKey> = Lazy::new(|| {
    let api_key = env::var("API_KEY").unwrap_or("ftcpi3OSjk26htxak54hqkZ6e9vdHq2Vd7oN83VZN39UcYmw1VwVkibug52oGIs4".to_string());
    let secret_key = env::var("SECRET_KEY").unwrap_or("iXArQEDFfmFanIfz7RfJj6G034b76nDuNetqxdqSLiUwPqDtLGIaNYK4TgDxR9H4".to_string());
    AccessKey {
        api_key,
        secret_key,
    }
});



fn main() {
    

    let mongo_client = mongodb::client::MongoClient::new();
    mongo_client.reset_db();
    
    let rm = RiskManager::new(KEY.clone(), RiskManagerConfig {
        max_risk_per_trade: 0.05,
        max_daily_losses: 3
    });
    let mm = MoneyManager::new(KEY.clone(), MoneyManagerConfig {
        constant_size: None,
        max_position_size: None,
        min_position_size: None,
        position_size_f: PositionSizeF::Constant,
        leverage: 50,
        initial_quote_balance: 1.15
    });
    let mc = MarketClassifer::new(KEY.clone(), MarketClassiferConfig {
    });
    
    let mut threads = vec![];
    let loaders = loader::start_loader(KEY.clone(), "DOGEUSDT".to_string(), 1, 60, 300, true, 4 * 60 * 60 * 1000);
    std::iter::Extend::extend(&mut threads, loaders);
    
    
    for thread in threads {
        thread.join().unwrap();
    }
    
}
