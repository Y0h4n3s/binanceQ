use std::collections::VecDeque;
use std::sync::Arc;
use async_broadcast::{InactiveReceiver, Receiver, Sender};
use async_trait::async_trait;
use futures::SinkExt;
use rayon::prelude::IntoParallelRefIterator;
use tokio::sync::{Mutex, Notify, RwLock};
use tracing::debug;
use crate::events::EventSink;
use crate::executors::{ ExchangeAccountInfo};
use crate::managers::risk_manager::RiskManager;
use crate::types::{ExecutionCommand, GlobalConfig, Kline, Order};


#[async_trait]
pub trait SignalGenerator:  Send + Sync + 'static {
    async fn handle_kline(&mut self, kline: &Kline, account: &Box<Arc<dyn ExchangeAccountInfo>>) -> Option<Vec<Order>>;
}
#[derive(Clone)]
pub struct StrategyManager<RiskManager: Send + Sync + 'static> {
    global_config: GlobalConfig,
    pub command_subscribers: Arc<Mutex<Sender<(Order, Option<Arc<Notify>>)>>>,
    strategies: Arc<RwLock<Vec<Box<dyn SignalGenerator>>>>,
    klines: InactiveReceiver<(Kline, Option<Arc<Notify>>)>,
    risk_manager: RiskManager,
    pub account: Box<Arc<dyn ExchangeAccountInfo>>,
}

impl<RiskManager:  Send + Sync + 'static> StrategyManager<RiskManager> {
    pub fn new(global_config: GlobalConfig, command_subscribers: Sender<(Order, Option<Arc<Notify>>)>, account: Box<Arc<dyn ExchangeAccountInfo>>, klines: InactiveReceiver<(Kline, Option<Arc<Notify>>)>, risk_manager: RiskManager) -> Self {
        Self {
            global_config,
            command_subscribers: Arc::new(Mutex::new(command_subscribers)),
            strategies: Arc::new(Default::default()),
            klines,
            risk_manager,
            account
        }
    }
    async fn load_strategy(&mut self, strategy: Box<dyn SignalGenerator>) {
        self.strategies.write().await.push(strategy);
    }
}


#[async_trait]
impl<RiskManager:  Send + Sync + 'static> EventSink<Kline> for StrategyManager<RiskManager> {
    fn get_receiver(&self) -> Receiver<(Kline, Option<Arc<Notify>>)> {
        self.klines.activate_cloned()
    }

    async fn handle_event(&self, event_msg: Kline) -> anyhow::Result<()> {
        for mut strategy in self.strategies.read().await.iter() {
            if let Some(orders) = strategy.handle_kline(&event_msg, &self.account).await {
                let broadcast = self.command_subscribers.lock().await;
                for order in orders {
                    debug!("Sending order: {:?}", order);
                    // apply risk management and send off to executor
                    // should wait for in case risk management depends on open orders
                    let notify = Arc::new(Notify::new());
                    let notified = notify.notified();
                    broadcast.broadcast((order, Some(notify.clone()))).await?;
                    notified.await;
                }
            }
        }
        Ok(())
    }


}