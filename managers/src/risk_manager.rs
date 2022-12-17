use async_std::sync::Arc;
use std::collections::VecDeque;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::Manager;
use async_broadcast::{Receiver, Sender};
use async_trait::async_trait;
use binance_q_events::{EventEmitter, EventSink};
use binance_q_executors::ExchangeAccount;
use binance_q_types::{ClosePolicy, ExecutionCommand, GlobalConfig, Kline, Order, OrderStatus, OrderType, Side, TfTrades, Trade};
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
    pub account: Box<Arc<dyn ExchangeAccount>>,
    klines: Arc<RwLock<Receiver<Kline>>>,
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
        Ok(tokio::spawn(async move { Ok(()) }))
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
        let order_q = self.order_q.clone();/// decide on size and price and order_type and send to order_q
        Ok(tokio::spawn(async move {
            let position = account.get_position(&global_config.symbol).await;
            let lifetime = 80 * 60 * 1000;
            match event_msg {
                // try different configs here
                ExecutionCommand::OpenLongPosition(symbol, confidence) => {
                    let symbol_balance = account.get_symbol_account(&symbol).await;
                    let trade_history = account.get_past_trades(&symbol, None).await;
                    let position = account.get_position(&symbol).await;

                    let spread = account.get_spread(&symbol).await;
                    // calculate size here
                    let size = symbol_balance.quote_asset_free;
                    // get the price based on confidence level
                    let price = spread.spread;

                    
                    // Entry order
                    let order = Order {
                        id: uuid::Uuid::new_v4(),
                        symbol: symbol.clone(),
                        side: Side::Bid,
                        price: Default::default(),
                        quantity: Decimal::new(1000, 0),
                        time: spread.time,
                        order_type: OrderType::Market,
                        lifetime,
                        close_policy: ClosePolicy::ImmediateMarket,
                    };

                    let pr = Decimal::new(20, 3);

                    // target order
                    let target_price = price + (price * pr);
                    let target_order = Order {
                        id: uuid::Uuid::new_v4(),
                        symbol: symbol.clone(),
                        side: Side::Ask,
                        price: target_price,
                        quantity: Decimal::new(1000, 0),
                        time: spread.time,
                        order_type: OrderType::TakeProfit(order.id),
                        lifetime,
                        close_policy: ClosePolicy::ImmediateMarket,
                    };

                    // stop loss order
                    let stop_loss_price = price - (price  * pr / Decimal::new(2, 0));
                    let stop_loss_order = Order {
                        id: uuid::Uuid::new_v4(),
                        symbol: symbol.clone(),
                        side: Side::Ask,
                        price: stop_loss_price,
                        quantity: Decimal::new(1000, 0),
                        time: spread.time,
                        order_type: OrderType::StopLoss(order.id),
                        lifetime,
                        close_policy: ClosePolicy::ImmediateMarket,
                    };

                    if position.is_open() && position.is_long() {
                        println!("Adding long to position");
                        println!("position: {:?}", position);
                    }
                    let mut oq = order_q.write().await;
                    oq.push_back(order);
                    oq.push_back(target_order);
                    oq.push_back(stop_loss_order);
                    
                }
                ExecutionCommand::OpenShortPosition(symbol, confidence) => {
                    let spread = account.get_spread(&symbol).await;
                    let price = spread.spread;

                    let order = Order {
                        id: uuid::Uuid::new_v4(),
                        symbol: symbol.clone(),
                        side: Side::Ask,
                        price: Default::default(),
                        quantity: Decimal::new(1000, 0),
                        time: spread.time,
                        order_type: OrderType::Market,
                        lifetime,
                        close_policy: ClosePolicy::ImmediateMarket,
                    };

                    let pr = Decimal::new(20, 3);

                    // target order
                    let target_price = price - (price * pr);
                    let target_order = Order {
                        id: uuid::Uuid::new_v4(),
                        symbol: symbol.clone(),
                        side: Side::Bid,
                        price: target_price,
                        quantity: Decimal::new(1000, 0),
                        time: spread.time,
                        order_type: OrderType::TakeProfit(order.id),
                        lifetime,
                        close_policy: ClosePolicy::ImmediateMarket,
                    };

                    // stop loss order
                    let stop_loss_price = price + (price * pr / Decimal::new(2, 0));
                    let stop_loss_order = Order {
                        id: uuid::Uuid::new_v4(),
                        symbol: symbol.clone(),
                        side: Side::Bid,
                        price: stop_loss_price,
                        quantity: Decimal::new(1000, 0),
                        time: spread.time,
                        order_type: OrderType::StopLoss(order.id),
                        lifetime,
                        close_policy: ClosePolicy::ImmediateMarket,
                    };
                    if position.is_open() && position.is_short() {
                        println!("Adding short to position");
                        println!("position: {:?}", position);
                    }
                    let mut oq = order_q.write().await;
                    oq.push_back(order);
                    oq.push_back(target_order);
                    oq.push_back(stop_loss_order);
                    
                }
                ExecutionCommand::ExecuteOrder(order) => {
                    // possibly add some controls here
                    // like if there is too much risk, don't execute
                    // or if there is too much exposure, don't execute
                    // or if there is no enough balance, don't execute
                    // or apply portfolio management rules
                    let mut order = order.clone();
                    let mut oq = order_q.write().await;
                    oq.push_back(order);
                }
                _ => {}
            }
            Ok(())
        }))
    }
}

impl EventSink<Kline> for RiskManager {
    fn get_receiver(&self) -> Arc<RwLock<Receiver<Kline>>> {
        self.klines.clone()
    }
    fn working(&self) -> bool {
        self.tf_trades_working.read().unwrap().clone()
    }
    fn set_working(&self, working: bool) -> anyhow::Result<()> {
        *self.tf_trades_working.write().unwrap() = working;
        Ok(())
    }
    // Act on trade events for risk manager
    // deal with expired lifetime orders
    fn handle_event(&self, event: Kline) -> anyhow::Result<JoinHandle<anyhow::Result<()>>> {
        
        let global_config = self.global_config.clone();
        let account = self.account.clone();
        let order_q = self.order_q.clone();
        Ok(tokio::spawn(async move {
            let open_orders = account.get_open_orders(&global_config.symbol).await;
            
            let mut handled_orders = vec![];
            for o in open_orders.iter() {
                match o {
                    // skip partially filled orders as they are mostly going to be limit orders
                    OrderStatus::Pending(order) => {
                        let now = event.close_time;
                        if now < order.time {
                            return Ok(());
                        }
                        if order.lifetime > 0 && now - order.time > order.lifetime {
                            match order.order_type {
                                // handle case when market filled order's lifetime is expired
                                OrderType::TakeProfit(for_id) | OrderType::StopLoss(for_id)=> {
                                    if handled_orders.iter().find(|x| *x == &for_id).is_some() {
                                        continue;
                                    }
                                    match order.close_policy {
                                        ClosePolicy::ImmediateMarket => {
    
                                            let position = account.get_position(&global_config.symbol).await;
                                            if !position.is_open() {
                                                return Ok(());
                                            }
                                            println!("Target/Stop order expired for order {} Handling with policy {:?} {:?}", for_id, order.close_policy, order.side);

                                            let mut oq = order_q.write().await;
                                            // cancel the take profit or stop loss order
                                            oq.push_back(Order {
                                                id: for_id,
                                                symbol: order.symbol.clone(),
                                                side: order.side.clone(),
                                                price: Default::default(),
                                                quantity: order.quantity,
                                                time: 0,
                                                order_type: OrderType::Cancel(for_id),
                                                lifetime: 30 * 60 * 1000,
                                                close_policy: ClosePolicy::ImmediateMarket,
                                            });
                                            // then market close the order's position
                                            oq.push_back(Order {
                                                id: uuid::Uuid::new_v4(),
                                                symbol: order.symbol.clone(),
                                                side: order.side.clone(),
                                                price: Default::default(),
                                                quantity: order.quantity,
                                                time: 0,
                                                order_type: OrderType::Market,
                                                lifetime: 30 * 60 * 1000,
                                                close_policy: ClosePolicy::ImmediateMarket,
                                            });
                                            handled_orders.push(for_id);
                                        }
                                        _ => {
                                            todo!()
                                        }
                                    }
                                }

                                _ => {
                                
                                }
                            }
                            return Ok(());
                        }
                    }

                    _ => {
                    }

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
        klines: Receiver<Kline>,
        trades: Receiver<Trade>,
        execution_commands: Receiver<ExecutionCommand>,
        account: Box<Arc<dyn ExchangeAccount>>,
    ) -> Self {
        let key = &global_config.key;

        RiskManager {
            global_config: Arc::new(global_config),
            config,
            account,
            klines: Arc::new(RwLock::new(klines)),
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
