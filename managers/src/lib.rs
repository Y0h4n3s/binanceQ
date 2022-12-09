#![allow(unused)]


use async_std::task::JoinHandle;
use async_trait::async_trait;
use futures::future::BoxFuture;

pub mod risk_manager;
pub mod strategy_manager;

#[async_trait]
pub trait Manager {
	async fn manage(&self);
}