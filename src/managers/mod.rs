#![allow(unused)]

use std::future::Future;
use async_std::task::JoinHandle;
use async_trait::async_trait;
use binance::futures::account::FuturesAccount;
use binance::futures::market::FuturesMarket;
use binance::futures::model::Symbol;
use binance::model::SpotFuturesTransferType;
use binance::savings::Savings;
use futures::future::BoxFuture;

pub mod risk_manager;
pub mod strategy_manager;

#[async_trait]
pub trait Manager {
	async fn manage(&self);
}