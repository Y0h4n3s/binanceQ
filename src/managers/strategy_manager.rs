use crate::events::EventSink;
use crate::executors::ExchangeAccountInfo;
use crate::managers::risk_manager::RiskManager;
use crate::types::{ExecutionCommand, GlobalConfig, Kline, Order, OrderStatus, Trade};
use async_broadcast::{InactiveReceiver, Receiver, Sender};
use async_trait::async_trait;
use futures::SinkExt;
use rayon::prelude::IntoParallelRefIterator;
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::{Mutex, Notify, RwLock};
use tracing::debug;

#[async_trait]
pub trait SignalGenerator: Send + Sync + 'static {
    async fn handle_kline(
        &mut self,
        kline: &Kline,
        account: &Box<Arc<dyn ExchangeAccountInfo>>,
    ) -> Option<Vec<Order>>;
    async fn handle_trade(
        &mut self,
        trade: &Trade,
        account: &Box<Arc<dyn ExchangeAccountInfo>>,
    ) -> Option<Vec<Order>>;
}
#[derive(Clone)]
pub struct StrategyManager<RiskManager: Send + Sync + 'static> {
    global_config: GlobalConfig,
    pub command_subscribers: Arc<Mutex<Sender<(OrderStatus, Option<Arc<Notify>>)>>>,
    strategies: Arc<Mutex<Vec<Box<dyn SignalGenerator>>>>,
    klines: InactiveReceiver<(Kline, Option<Arc<Notify>>)>,
    trades: InactiveReceiver<(Trade, Option<Arc<Notify>>)>,
    risk_manager: RiskManager,
    pub account: Box<Arc<dyn ExchangeAccountInfo>>,
}

impl<RiskManager: Send + Sync + 'static> StrategyManager<RiskManager> {
    pub fn new(
        global_config: GlobalConfig,
        command_subscribers: Sender<(OrderStatus, Option<Arc<Notify>>)>,
        account: Box<Arc<dyn ExchangeAccountInfo>>,
        klines: InactiveReceiver<(Kline, Option<Arc<Notify>>)>,
        trades: InactiveReceiver<(Trade, Option<Arc<Notify>>)>,
        risk_manager: RiskManager,
    ) -> Self {
        Self {
            global_config,
            command_subscribers: Arc::new(Mutex::new(command_subscribers)),
            strategies: Arc::new(Default::default()),
            klines,
            trades,
            risk_manager,
            account,
        }
    }
    pub async fn load_strategy(&mut self, strategy: Box<dyn SignalGenerator>) {
        self.strategies.lock().await.push(strategy);
    }
}

#[async_trait]
impl<RiskManager: Send + Sync + 'static> EventSink<Kline> for StrategyManager<RiskManager> {
    fn get_receiver(&self) -> Receiver<(Kline, Option<Arc<Notify>>)> {
        self.klines.activate_cloned()
    }
    async fn name(&self) -> String {
        format!(
            "{}: StrategyManager Kline Sink",
            self.global_config.symbol.symbol
        )
    }
    async fn handle_event(&self, event_msg: Kline) -> anyhow::Result<()> {
        for mut strategy in self.strategies.lock().await.iter_mut() {
            if let Some(orders) = strategy.handle_kline(&event_msg, &self.account).await {
                let broadcast = self.command_subscribers.lock().await;
                for order in orders {
                    // apply risk management and send off to executor
                    // should wait for in case risk management depends on open orders
                    let notify = Arc::new(Notify::new());
                    let notified = notify.notified();
                    broadcast
                        .broadcast((OrderStatus::Pending(order), Some(notify.clone())))
                        .await?;
                    notified.await;
                }
            }
        }
        Ok(())
    }
}

#[async_trait]
impl<RiskManager: Send + Sync + 'static> EventSink<Trade> for StrategyManager<RiskManager> {
    fn get_receiver(&self) -> Receiver<(Trade, Option<Arc<Notify>>)> {
        self.trades.activate_cloned()
    }
    async fn name(&self) -> String {
        format!(
            "{}: StrategyManager Trade Sink",
            self.global_config.symbol.symbol
        )
    }
    async fn handle_event(&self, event_msg: Trade) -> anyhow::Result<()> {
        for mut strategy in self.strategies.lock().await.iter_mut() {
            if let Some(orders) = strategy.handle_trade(&event_msg, &self.account).await {
                let broadcast = self.command_subscribers.lock().await;
                for order in orders {
                    // apply risk management and send off to executor
                    // should wait for in case risk management depends on open orders
                    let notify = Arc::new(Notify::new());
                    let notified = notify.notified();
                    broadcast
                        .broadcast((OrderStatus::Pending(order), Some(notify.clone())))
                        .await?;
                    notified.await;
                }
            }
        }
        Ok(())
    }
}
