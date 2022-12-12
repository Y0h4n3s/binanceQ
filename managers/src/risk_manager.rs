use async_std::sync::Arc;
use std::collections::VecDeque;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::Manager;
use async_broadcast::{Receiver, Sender};
use async_trait::async_trait;
use binance_q_events::{EventEmitter, EventSink};
use binance_q_executors::ExchangeAccountInfo;
use binance_q_types::{ExecutionCommand, GlobalConfig, Order, OrderType, Side, TfTrades, Trade};
use rust_decimal::Decimal;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
#[derive(Clone)]
pub struct RiskManagerConfig {
    pub max_daily_losses: usize,
    pub max_risk_per_trade: f64,
}

#[derive(Clone)]
pub struct RiskManager {
    pub global_config: Arc<GlobalConfig>,
    pub config: RiskManagerConfig,
    pub account: Box<Arc<dyn ExchangeAccountInfo>>,
    tf_trades: Arc<RwLock<Receiver<TfTrades>>>,
    trades: Arc<RwLock<Receiver<Trade>>>,
    execution_commands: Arc<RwLock<Receiver<ExecutionCommand>>>,
    subscribers: Arc<RwLock<Sender<Order>>>,
    order_q: Arc<RwLock<VecDeque<Order>>>,
    execution_commands_working: Arc<std::sync::RwLock<bool>>,
    tf_trades_working: Arc<std::sync::RwLock<bool>>,
    trade_working: Arc<std::sync::RwLock<bool>>,
}

#[async_trait]
impl EventEmitter<Order> for RiskManager {
    fn get_subscribers(&self) -> Arc<RwLock<Sender<Order>>> {
        self.subscribers.clone()
    }

    async fn emit(&self) -> anyhow::Result<JoinHandle<()>> {
        let order_q = self.order_q.clone();
        let subscribers = self.subscribers.clone();
        Ok(tokio::spawn(async move {
            //send waiting orders to the executor
            loop {
                let mut oq = order_q.write().await;
                let order = oq.pop_front();
                std::mem::drop(oq);
                if let Some(order) = order {
                    // there is only one subscriber
                    let subs = subscribers.read().await;
                    subs.broadcast(order).await.unwrap();
                }
            }
        }))
    }
}

impl EventSink<Trade> for RiskManager {
    fn get_receiver(&self) -> Arc<RwLock<Receiver<Trade>>> {
        self.trades.clone()
    }
    fn working(&self) -> bool {
        self.trade_working.read().unwrap().clone()
    }
    fn set_working(&self, working: bool) -> anyhow::Result<()> {
        *self.trade_working.write().unwrap() = working;
        Ok(())
    }
    // Act on trade events for risk manager
    fn handle_event(&self, event: Trade) -> anyhow::Result<JoinHandle<anyhow::Result<()>>> {
        let global_config = self.global_config.clone();
        let account = self.account.clone();
        Ok(tokio::spawn(async move {
            
            Ok(())
        }))
    }
}

impl EventSink<ExecutionCommand> for RiskManager {
    fn get_receiver(&self) -> Arc<RwLock<Receiver<ExecutionCommand>>> {
        self.execution_commands.clone()
    }
    fn working(&self) -> bool {
        self.execution_commands_working.read().unwrap().clone()
    }
    fn set_working(&self, working: bool) -> anyhow::Result<()> {
        *self.execution_commands_working.write().unwrap() = working;
        Ok(())
    }
    fn handle_event(
        &self,
        event_msg: ExecutionCommand,
    ) -> anyhow::Result<JoinHandle<anyhow::Result<()>>> {
        let account = self.account.clone();
        let global_config = self.global_config.clone();
        let order_q = self.order_q.clone();
        /// decide on size and price and order_type and send to order_q
        Ok(tokio::spawn(async move {
            match event_msg {
                // try different configs here
                ExecutionCommand::OpenLongPosition(symbol, confidence) => {
                    let symbol_balance = account.get_symbol_account(&symbol).await;
                    let trade_history = account.get_past_trades(&symbol, None).await;
                    let position = account.get_position(&symbol).await;
                    // calculate size here
                    let size = symbol_balance.quote_asset_free;
                    // get the price based on confidence level
                    let price = 0.0;
                    let order = Order {
                        id: 0,
                        symbol,
                        side: Side::Bid,
                        price: Default::default(),
                        quantity: Decimal::new(1000, 0),
                        time: SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap()
                            .as_millis() as u64,
                        order_type: OrderType::Market,
                    };
                    let mut oq = order_q.write().await;
                    oq.push_back(order);
                    
                }
                ExecutionCommand::OpenShortPosition(symbol, confidence) => {
                    let order = Order {
                        id: 0,
                        symbol,
                        side: Side::Ask,
                        price: Default::default(),
                        quantity: Decimal::new(1000, 0),
                        time: SystemTime::now()
                              .duration_since(UNIX_EPOCH)
                              .unwrap()
                              .as_millis() as u64,
                        order_type: OrderType::Market,
                    };
                    let mut oq = order_q.write().await;
                    oq.push_back(order);
                }
                ExecutionCommand::CloseLongPosition(symbol, confidence) => {
                    let position = account.get_position(&symbol).await;
                    if position.is_long() {
                        let order = Order {
                            id: 0,
                            symbol,
                            side: Side::Ask,
                            price: Default::default(),
                            quantity: Decimal::new(1000, 0),
                            time: SystemTime::now()
                                  .duration_since(UNIX_EPOCH)
                                  .unwrap()
                                  .as_millis() as u64,
                            order_type: OrderType::Market,
                        };
                        let mut oq = order_q.write().await;
                        oq.push_back(order);
                    }
                }
                ExecutionCommand::CloseShortPosition(symbol, confidence) => {
                    let position = account.get_position(&symbol).await;
    
                    if position.is_short() {
                        let order = Order {
                            id: 0,
                            symbol,
                            side: Side::Bid,
                            price: Default::default(),
                            quantity: Decimal::new(1000, 0),
                            time: SystemTime::now()
                                  .duration_since(UNIX_EPOCH)
                                  .unwrap()
                                  .as_millis() as u64,
                            order_type: OrderType::Market,
                        };
                        let mut oq = order_q.write().await;
                        oq.push_back(order);
                    }
                }
            }
            Ok(())
        }))
    }
}

impl EventSink<TfTrades> for RiskManager {
    fn get_receiver(&self) -> Arc<RwLock<Receiver<TfTrades>>> {
        self.tf_trades.clone()
    }
    fn working(&self) -> bool {
        self.tf_trades_working.read().unwrap().clone()
    }
    fn set_working(&self, working: bool) -> anyhow::Result<()> {
        *self.tf_trades_working.write().unwrap() = working;
        Ok(())
    }
    // Act on trade events for risk manager
    fn handle_event(&self, event: TfTrades) -> anyhow::Result<JoinHandle<anyhow::Result<()>>> {
        let global_config = self.global_config.clone();
        Ok(tokio::spawn(async move {
            for trade in event {
                match trade.tf {
                    x if x == global_config.tf1 => {}
                    x if x == global_config.tf2 => {}
                    x if x == global_config.tf3 => {}
                    _ => {}
                }
            }
            Ok(())
        }))
    }
}

impl RiskManager {
    pub fn new(
        global_config: GlobalConfig,
        config: RiskManagerConfig,
        tf_trades: Receiver<TfTrades>,
        trades: Receiver<Trade>,
        execution_commands: Receiver<ExecutionCommand>,
        account: Box<Arc<dyn ExchangeAccountInfo>>,
    ) -> Self {
        let key = &global_config.key;

        RiskManager {
            global_config: Arc::new(global_config),
            config,
            account,
            tf_trades: Arc::new(RwLock::new(tf_trades)),
            trades: Arc::new(RwLock::new(trades)),
            execution_commands: Arc::new(RwLock::new(execution_commands)),
            subscribers: Arc::new(RwLock::new(async_broadcast::broadcast(1).0)),
            order_q: Arc::new(RwLock::new(VecDeque::new())),
            execution_commands_working: Arc::new(std::sync::RwLock::new(false)),
            tf_trades_working: Arc::new(std::sync::RwLock::new(false)),
            trade_working: Arc::new(std::sync::RwLock::new(false)),
        }
    }
}
#[async_trait]
impl Manager for RiskManager {
    async fn manage(&self) {
        let start_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();

        loop {
            std::thread::sleep(std::time::Duration::from_secs(45));
        }
    }
}
