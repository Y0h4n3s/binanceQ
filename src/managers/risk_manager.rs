use async_std::sync::Arc;
use std::collections::VecDeque;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::events::{EventEmitter, EventSink};
use crate::executors::{ExchangeAccount, ExchangeAccountInfo};
use crate::managers::Manager;
use crate::types::{
    ClosePolicy, ExecutionCommand, GlobalConfig, Kline, Order, OrderStatus, OrderType, Side
    , Trade,
};
use async_broadcast::{InactiveReceiver, Receiver, Sender};
use async_trait::async_trait;
use rust_decimal::Decimal;
use std::time::Duration;
use tokio::sync::{Notify, RwLock};
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
    klines: InactiveReceiver<(Kline, Option<Arc<Notify>>)>,
    trades: InactiveReceiver<(Trade, Option<Arc<Notify>>)>,
    subscribers: Arc<RwLock<Sender<(Order, Option<Arc<Notify>>)>>>,
}




#[async_trait]
impl EventSink<Trade> for RiskManager {
    fn get_receiver(&self) -> Receiver<(Trade, Option<Arc<Notify>>)> {
        self.trades.clone().activate()
    }
    // Act on trade events for risk manager
    async fn handle_event(&self, event: Trade) -> anyhow::Result<()> {
        let global_config = self.global_config.clone();
        let account = self.account.clone();
        Ok(())
    }
}
#[async_trait]
impl EventSink<Kline> for RiskManager {
    fn get_receiver(&self) -> Receiver<(Kline, Option<Arc<Notify>>)> {
        self.klines.clone().activate()
    }
    // Act on trade events for risk manager
    // deal with expired lifetime orders
    async fn handle_event(&self, event: Kline) -> anyhow::Result<()> {
        // let global_config = self.global_config.clone();
        // let account = self.account.clone();
        // let order_q = self.order_q.clone();
        //     let open_orders = account.get_open_orders(&global_config.symbol).await;
        //     let o1 = open_orders.clone();
        //     let mut handled_orders = vec![];
        //     for o in open_orders.iter() {
        //         match &*o {
        //             // skip partially filled orders as they are mostly going to be limit orders
        //             OrderStatus::Pending(order) => {
        //                 let now = event.close_time;
        //                 if now < order.time {
        //                     return Ok(());
        //                 }
        //                 if order.lifetime > 0 && now - order.time > order.lifetime {
        //                     match order.order_type {
        //                         // handle case when market filled order's lifetime is expired
        //                         OrderType::StopLoss(for_id)
        //                         | OrderType::StopLossTrailing(for_id, _) => {
        //                             if handled_orders.iter().any(|x| x == &order.id) {
        //                                 continue;
        //                             }
        // 
        //                             let for_orders =
        //                                 account.get_order(&global_config.symbol, &for_id).await;
        //                             let position =
        //                                 account.get_position(&global_config.symbol).await;
        //                             let mut oq = order_q.write().await;
        // 
        //                             // if position is neutral and there are no limit orders with the same id
        //                             if !position.is_open()
        //                                 && o1
        //                                     .iter()
        //                                     .find(|o| match &**o {
        //                                         OrderStatus::Pending(order)
        //                                         | OrderStatus::PartiallyFilled(order, _) => {
        //                                             order.order_type == OrderType::Limit
        //                                                 && order.id == for_id
        //                                         }
        //                                         _ => false,
        //                                     })
        //                                     .is_none()
        //                             {
        //                                 oq.push_back(Order {
        //                                     id: for_id,
        //                                     symbol: order.symbol.clone(),
        //                                     side: order.side.clone(),
        //                                     price: Default::default(),
        //                                     quantity: order.quantity,
        //                                     time: 0,
        //                                     order_type: OrderType::CancelFor(for_id),
        //                                     lifetime: 30 * 60 * 1000,
        //                                     close_policy: ClosePolicy::ImmediateMarket,
        //                                 });
        //                                 return Ok(());
        //                             }
        //                             let spread = account.get_spread(&global_config.symbol).await;
        //                             let average_entry = for_orders
        //                                 .iter()
        //                                 .map(|o| o.price)
        //                                 .reduce(|a, b| a + b)
        //                                 .unwrap()
        //                                 / Decimal::new(for_orders.len() as i64, 0);
        //                             match order.close_policy {
        //                                 ClosePolicy::ImmediateMarket => {
        //                                     // println!("Target/Stop order expired for order {} Handling with policy {:?} {:?}", for_id, order.close_policy, order.side);
        // 
        //                                     // cancel the take profit or stop loss order
        //                                     oq.push_back(Order {
        //                                         id: for_id,
        //                                         symbol: order.symbol.clone(),
        //                                         side: order.side.clone(),
        //                                         price: Default::default(),
        //                                         quantity: order.quantity,
        //                                         time: 0,
        //                                         order_type: OrderType::CancelFor(for_id),
        //                                         lifetime: 30 * 60 * 1000,
        //                                         close_policy: ClosePolicy::ImmediateMarket,
        //                                     });
        //                                     // then market close the order's position
        //                                     oq.push_back(Order {
        //                                         id: uuid::Uuid::new_v4(),
        //                                         symbol: order.symbol.clone(),
        //                                         side: order.side.clone(),
        //                                         price: Default::default(),
        //                                         quantity: order.quantity,
        //                                         time: 0,
        //                                         order_type: OrderType::Market,
        //                                         lifetime: 30 * 60 * 1000,
        //                                         close_policy: ClosePolicy::ImmediateMarket,
        //                                     });
        //                                     handled_orders.push(order.id);
        //                                 }
        //                                 ClosePolicy::BreakEvenOrMarketClose => {
        //                                     match order.side {
        //                                         Side::Bid => {
        //                                             // breakeven stop order
        //                                             if spread.spread < average_entry {
        //                                                 oq.push_back(Order {
        //                                                     id: for_id,
        //                                                     symbol: order.symbol.clone(),
        //                                                     side: order.side.clone(),
        //                                                     price: Default::default(),
        //                                                     quantity: order.quantity,
        //                                                     time: 0,
        //                                                     order_type: OrderType::Cancel(order.id),
        //                                                     lifetime: 30 * 60 * 1000,
        //                                                     close_policy: ClosePolicy::None,
        //                                                 });
        //                                                 oq.push_back(Order {
        //                                                     id: uuid::Uuid::new_v4(),
        //                                                     symbol: order.symbol.clone(),
        //                                                     side: order.side.clone(),
        //                                                     price: average_entry,
        //                                                     quantity: order.quantity,
        //                                                     time: 0,
        //                                                     order_type: OrderType::StopLoss(for_id),
        //                                                     lifetime: u64::MAX,
        //                                                     close_policy: ClosePolicy::None,
        //                                                 });
        //                                             }
        //                                             // market close
        //                                             else {
        //                                                 oq.push_back(Order {
        //                                                     id: for_id,
        //                                                     symbol: order.symbol.clone(),
        //                                                     side: order.side.clone(),
        //                                                     price: Default::default(),
        //                                                     quantity: order.quantity,
        //                                                     time: 0,
        //                                                     order_type: OrderType::CancelFor(
        //                                                         for_id,
        //                                                     ),
        //                                                     lifetime: 30 * 60 * 1000,
        //                                                     close_policy:
        //                                                         ClosePolicy::ImmediateMarket,
        //                                                 });
        //                                                 // then market close the order's position
        //                                                 oq.push_back(Order {
        //                                                     id: uuid::Uuid::new_v4(),
        //                                                     symbol: order.symbol.clone(),
        //                                                     side: order.side.clone(),
        //                                                     price: Default::default(),
        //                                                     quantity: order.quantity,
        //                                                     time: 0,
        //                                                     order_type: OrderType::Market,
        //                                                     lifetime: 30 * 60 * 1000,
        //                                                     close_policy:
        //                                                         ClosePolicy::ImmediateMarket,
        //                                                 });
        //                                             }
        //                                             handled_orders.push(order.id);
        //                                         }
        //                                         Side::Ask => {
        //                                             if spread.spread > average_entry {
        //                                                 oq.push_back(Order {
        //                                                     id: for_id,
        //                                                     symbol: order.symbol.clone(),
        //                                                     side: order.side.clone(),
        //                                                     price: Default::default(),
        //                                                     quantity: order.quantity,
        //                                                     time: 0,
        //                                                     order_type: OrderType::CancelFor(
        //                                                         order.id,
        //                                                     ),
        //                                                     lifetime: 30 * 60 * 1000,
        //                                                     close_policy: ClosePolicy::None,
        //                                                 });
        //                                                 oq.push_back(Order {
        //                                                     id: uuid::Uuid::new_v4(),
        //                                                     symbol: order.symbol.clone(),
        //                                                     side: order.side.clone(),
        //                                                     price: average_entry,
        //                                                     quantity: order.quantity,
        //                                                     time: 0,
        //                                                     order_type: OrderType::StopLoss(for_id),
        //                                                     lifetime: u64::MAX,
        //                                                     close_policy: ClosePolicy::None,
        //                                                 });
        //                                             } else {
        //                                                 oq.push_back(Order {
        //                                                     id: for_id,
        //                                                     symbol: order.symbol.clone(),
        //                                                     side: order.side.clone(),
        //                                                     price: Default::default(),
        //                                                     quantity: order.quantity,
        //                                                     time: 0,
        //                                                     order_type: OrderType::CancelFor(
        //                                                         for_id,
        //                                                     ),
        //                                                     lifetime: 30 * 60 * 1000,
        //                                                     close_policy:
        //                                                         ClosePolicy::ImmediateMarket,
        //                                                 });
        //                                                 // then market close the order's position
        //                                                 oq.push_back(Order {
        //                                                     id: uuid::Uuid::new_v4(),
        //                                                     symbol: order.symbol.clone(),
        //                                                     side: order.side.clone(),
        //                                                     price: Default::default(),
        //                                                     quantity: order.quantity,
        //                                                     time: 0,
        //                                                     order_type: OrderType::Market,
        //                                                     lifetime: 30 * 60 * 1000,
        //                                                     close_policy:
        //                                                         ClosePolicy::ImmediateMarket,
        //                                                 });
        //                                             }
        //                                             handled_orders.push(order.id);
        //                                         }
        //                                     }
        //                                 }
        //                                 ClosePolicy::BreakEven => match order.side {
        //                                     Side::Bid => {
        //                                         if spread.spread < average_entry {
        //                                             oq.push_back(Order {
        //                                                 id: for_id,
        //                                                 symbol: order.symbol.clone(),
        //                                                 side: order.side.clone(),
        //                                                 price: Default::default(),
        //                                                 quantity: order.quantity,
        //                                                 time: 0,
        //                                                 order_type: OrderType::Cancel(order.id),
        //                                                 lifetime: 30 * 60 * 1000,
        //                                                 close_policy: ClosePolicy::None,
        //                                             });
        //                                             oq.push_back(Order {
        //                                                 id: uuid::Uuid::new_v4(),
        //                                                 symbol: order.symbol.clone(),
        //                                                 side: order.side.clone(),
        //                                                 price: average_entry,
        //                                                 quantity: order.quantity,
        //                                                 time: 0,
        //                                                 order_type: OrderType::StopLoss(for_id),
        //                                                 lifetime: u64::MAX,
        //                                                 close_policy: ClosePolicy::None,
        //                                             });
        //                                             handled_orders.push(order.id)
        //                                         }
        //                                     }
        //                                     Side::Ask => {
        //                                         if spread.spread > average_entry {
        //                                             oq.push_back(Order {
        //                                                 id: for_id,
        //                                                 symbol: order.symbol.clone(),
        //                                                 side: order.side.clone(),
        //                                                 price: Default::default(),
        //                                                 quantity: order.quantity,
        //                                                 time: 0,
        //                                                 order_type: OrderType::CancelFor(order.id),
        //                                                 lifetime: 30 * 60 * 1000,
        //                                                 close_policy: ClosePolicy::None,
        //                                             });
        //                                             oq.push_back(Order {
        //                                                 id: uuid::Uuid::new_v4(),
        //                                                 symbol: order.symbol.clone(),
        //                                                 side: order.side.clone(),
        //                                                 price: average_entry,
        //                                                 quantity: order.quantity,
        //                                                 time: 0,
        //                                                 order_type: OrderType::StopLoss(for_id),
        //                                                 lifetime: u64::MAX,
        //                                                 close_policy: ClosePolicy::None,
        //                                             });
        //                                         }
        //                                         handled_orders.push(order.id);
        //                                     }
        // 
        //                                 },
        //                                 _ => {
        //                                     todo!()
        //                                 }
        //                             }
        //                         }
        // 
        //                         _ => {}
        //                     }
        //                     return Ok(());
        //                 }
        //             }
        // 
        //             _ => {}
        // 
        //             _ => {}
        //         }
        //     }
            Ok(())
    }

}

impl RiskManager {
    pub fn new(
        global_config: GlobalConfig,
        config: RiskManagerConfig,
        klines: InactiveReceiver<(Kline, Option<Arc<Notify>>)>,
        trades: InactiveReceiver<(Trade, Option<Arc<Notify>>)>,
        execution_commands: InactiveReceiver<(ExecutionCommand, Option<Arc<Notify>>)>,
        account: Box<Arc<dyn ExchangeAccount>>,
    ) -> Self {
        let key = &global_config.key;

        RiskManager {
            global_config: Arc::new(global_config),
            config,
            account,
            klines,
            trades,
            subscribers: Arc::new(RwLock::new(async_broadcast::broadcast(1).0)),
        }
    }

    pub async fn neutralize(&self) {
        let position = self.account.get_position(&self.global_config.symbol).await;
        let oq = self.subscribers.clone();
        if position.is_open() {
            loop {
                // wait for all orders to be sent for execution
                if oq.read().await.len() > 0 {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                } else {
                    break;
                }
            }
            // updated position
            let position = self.account.get_position(&self.global_config.symbol).await;

            match position.side {
                Side::Bid => {
                    let mut oq = oq.write().await;
                    oq.broadcast((Order {
                        id: uuid::Uuid::new_v4(),
                        symbol: self.global_config.symbol.clone(),
                        side: Side::Ask,
                        price: Default::default(),
                        quantity: position.qty,
                        time: 0,
                        order_type: OrderType::Market,
                        lifetime: 30 * 60 * 1000,
                        close_policy: ClosePolicy::ImmediateMarket,
                    }, None)).await;
                }
                Side::Ask => {
                    let mut oq = oq.write().await;
                    oq.broadcast((Order {
                        id: uuid::Uuid::new_v4(),
                        symbol: self.global_config.symbol.clone(),
                        side: Side::Bid,
                        price: Default::default(),
                        quantity: position.qty,
                        time: 0,
                        order_type: OrderType::Market,
                        lifetime: 30 * 60 * 1000,
                        close_policy: ClosePolicy::ImmediateMarket,
                    }, None)).await;
                }
            }
        }
    }
}