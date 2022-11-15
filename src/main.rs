mod cmds;
mod bracket_order;
mod mongodb;
mod studies;
mod managers;
mod helpers;
mod errors;

use std::{env, io};
use std::cmp::Ordering;
use std::mem::take;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
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
use crate::studies::{Study, ob_study::ObStudy, vol_study::VolStudy, oi_study::OiStudy, StudyConfig, StudyTypes};
use futures::executor::block_on;
use crate::managers::Manager;
use crate::managers::money_manager::{MoneyManager, MoneyManagerConfig, PositionSizeF};

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
    
    let study_config = StudyConfig {
        symbol: "XRPUSDT".to_string(),
        tf1: 1,
        tf2: 5
    };
    
    // let oi_study: Arc<OiStudy> = Arc::new(OiStudy::new( KEY.clone(), &study_config));
    // let ob_study:  Arc<ObStudy>  =  Arc::new(ObStudy::new( KEY.clone(), &study_config));
    // let vol_study:  Arc<VolStudy>  = Arc::new(VolStudy::new(KEY.clone(), &study_config));
    // let join_handles = vec![
    //     std::thread::spawn(move || {
    //         ob_study.start_log();
    //     }),
    //     std::thread::spawn(move ||{
    //         oi_study.start_log();
    //     }),
    //     std::thread::spawn(move || {
    //         vol_study.start_log();
    //     })
    // ];
    // for handle in join_handles {
    //     handle.join().unwrap();
    // }
    
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
    block_on(mm.manage());
    
}
