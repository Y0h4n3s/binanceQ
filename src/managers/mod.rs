#![allow(unused)]

use async_trait::async_trait;

pub mod risk_manager;
pub mod strategy_manager;
pub mod strategy_manager_python;

#[async_trait]
pub trait Manager {
    async fn manage(&self);
}
