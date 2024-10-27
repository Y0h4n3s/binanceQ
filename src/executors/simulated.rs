use crate::events::{EventEmitter, EventSink};
use crate::executors::{ExchangeAccount, ExchangeAccountInfo, Position, Spread, TradeExecutor};
use crate::mongodb::MongoClient;
use crate::types::{
    ExchangeId, Order, OrderStatus, OrderType, Side, Symbol, SymbolAccount, TfTrades, Trade,
};
use async_broadcast::{InactiveReceiver, Receiver, Sender};
use async_trait::async_trait;
use dashmap::{DashMap, DashSet};
use rust_decimal::prelude::FromPrimitive;
use rust_decimal::Decimal;
use std::collections::{HashMap, VecDeque};
use std::fmt::Debug;
use std::sync::Arc;
use rust_decimal_macros::dec;
use tokio::sync::{Mutex, Notify, RwLock};
use tokio::task::JoinHandle;
use tracing::trace;
use uuid::Uuid;

type ArcMap<K, V> = Arc<DashMap<K, V>>;
type ArcSet<T> = Arc<DashSet<T>>;

#[derive(Clone)]
pub struct SimulatedAccount {
    pub symbol_accounts: ArcMap<Symbol, SymbolAccount>,
    pub open_orders: ArcMap<Symbol, ArcSet<Order>>,
    pub order_history: ArcMap<Symbol, ArcSet<OrderStatus>>,
    pub trade_history: ArcMap<Symbol, ArcSet<Trade>>,
    trade_q: Arc<Mutex<(VecDeque<Trade>, Arc<Notify>)>>,
    pub trade_subscribers: Arc<RwLock<Sender<(Trade, Option<Arc<Notify>>)>>>,
    pub positions: ArcMap<Symbol, Position>,
    tf_trades: InactiveReceiver<(TfTrades, Option<Arc<Notify>>)>,
    order_statuses: InactiveReceiver<(OrderStatus, Option<Arc<Notify>>)>,
    spreads: ArcMap<Symbol, Arc<RwLock<Spread>>>,
    pub order_status_subscribers: Arc<RwLock<Sender<(OrderStatus, Option<Arc<Notify>>)>>>,
}

#[derive(Clone)]
pub struct SimulatedExecutor {
    pub account: Arc<SimulatedAccount>,
    pub orders: InactiveReceiver<(Order, Option<Arc<Notify>>)>,
    order_status_q: Arc<RwLock<VecDeque<OrderStatus>>>,
    pub order_status_subscribers: Arc<RwLock<Sender<(OrderStatus, Option<Arc<Notify>>)>>>,
}
impl SimulatedAccount {
    pub async fn new(
        tf_trades: InactiveReceiver<(TfTrades, Option<Arc<Notify>>)>,
        order_statuses: InactiveReceiver<(OrderStatus, Option<Arc<Notify>>)>,
        symbols: Vec<Symbol>,
        order_status_subscribers: Arc<RwLock<Sender<(OrderStatus, Option<Arc<Notify>>)>>>,
    ) -> Self {
        let symbol_accounts = Arc::new(DashMap::new());
        let open_orders = Arc::new(DashMap::new());
        let order_history = Arc::new(DashMap::new());
        let trade_history = Arc::new(DashMap::new());
        let positions = Arc::new(DashMap::new());
        let spreads = Arc::new(DashMap::new());

        for symbol in symbols {
            let symbol_account = SymbolAccount {
                symbol: symbol.clone(),
                base_asset_free: Default::default(),
                base_asset_locked: Default::default(),
                quote_asset_free: Decimal::new(100000, 0),
                quote_asset_locked: Default::default(),
            };
            let position = Position::new(Side::Ask, symbol.clone(), Decimal::ZERO, Decimal::ZERO);
            let spread = Spread::new(symbol.clone());
            symbol_accounts.insert(symbol.clone(), symbol_account);
            open_orders.insert(symbol.clone(), Arc::new(DashSet::new()));
            order_history.insert(symbol.clone(), Arc::new(DashSet::new()));
            trade_history.insert(symbol.clone(), Arc::new(DashSet::new()));
            positions.insert(symbol.clone(), position);
            spreads.insert(symbol.clone(), Arc::new(RwLock::new(spread)));
        }
        Self {
            symbol_accounts,
            open_orders,
            order_history,
            trade_history,
            positions,
            spreads,
            tf_trades,
            trade_q: Arc::new(Mutex::new((VecDeque::new(), Arc::new(Notify::new())))),
            trade_subscribers: Arc::new(RwLock::new(async_broadcast::broadcast(1).0)),
            order_statuses,
            order_status_subscribers,
        }
    }
}

impl SimulatedExecutor {
    pub async fn new(
        orders_rx: InactiveReceiver<(Order, Option<Arc<Notify>>)>,
        trades_rx: InactiveReceiver<(TfTrades, Option<Arc<Notify>>)>,
        symbols: Vec<Symbol>,
        trades: Sender<(Trade, Option<Arc<Notify>>)>,
    ) -> Self {
        let order_statuses_channel = async_broadcast::broadcast(100);

        let mut account = SimulatedAccount::new(
            trades_rx,
            order_statuses_channel.1.deactivate(),
            symbols,
            Arc::new(RwLock::new(order_statuses_channel.0.clone())),
        )
        .await;
        account.subscribe(trades).await;
        account.emit().await.expect("failed to emit trades");
        let ac = Arc::new(account.clone());
        tokio::spawn(async move {
            EventSink::<TfTrades>::listen(ac).unwrap();
        });
        let ac = Arc::new(account.clone());
        tokio::spawn(async move {
            EventSink::<OrderStatus>::listen(ac).unwrap();
        });
        Self {
            account: Arc::new(account),
            orders: orders_rx,
            order_status_q: Arc::new(RwLock::new(VecDeque::new())),
            order_status_subscribers: Arc::new(RwLock::new(order_statuses_channel.0)),
        }
    }
}

#[async_trait]
impl EventEmitter<Trade> for SimulatedAccount {
    fn get_subscribers(&self) -> Arc<RwLock<Sender<(Trade, Option<Arc<Notify>>)>>> {
        self.trade_subscribers.clone()
    }

    async fn emit(&self) -> anyhow::Result<JoinHandle<()>> {
        let trade_q = self.trade_q.clone();
        let subscribers = self.trade_subscribers.clone();
        let trade_history = self.trade_history.clone();
        Ok(tokio::spawn(async move {
            let mongo_client = MongoClient::new().await;
            let w = trade_q.lock().await;
            let new_trade = w.1.clone();
            drop(w);
            loop {
                let new_trade = new_trade.notified();
                new_trade.await;
                let mut w = trade_q.lock().await;
                let trade = w.0.pop_front();
                drop(w);
                if let Some(trade) = trade {
                    mongo_client
                        .past_trades
                        .insert_one(trade.clone(), None)
                        .await
                        .unwrap();
                    let th = trade_history.get_mut(&trade.symbol).unwrap();
                    th.insert(trade.clone());
                    drop(th);
                    let subs = subscribers.read().await;
                    subs.broadcast((trade.clone(), None)).await.unwrap();
                }
            }
        }))
    }
}

async fn broadcast_and_wait<T: Clone + Debug>(
    channel: &Arc<RwLock<Sender<(T, Option<Arc<Notify>>)>>>,
    value: T,
) {
    let notifier = Arc::new(Notify::new());
    let notified = notifier.notified();
    let channel = channel.read().await;
    if let Err(e) = channel.broadcast((value, Some(notifier.clone()))).await {
        trace!("Failed to broadcast notification: {}", e);
    }
    notified.await;
}

async fn broadcast<T: Clone + Debug>(
    channel: &Arc<RwLock<Sender<(T, Option<Arc<Notify>>)>>>,
    value: T,
) {
    let channel = channel.read().await;
    if let Err(e) = channel.broadcast((value, None)).await {
        trace!("Failed to broadcast notification: {}", e);
    }
}

#[async_trait]
impl EventSink<TfTrades> for SimulatedAccount {
    fn get_receiver(&self) -> Receiver<(TfTrades, Option<Arc<Notify>>)> {
        self.tf_trades.clone().activate()
    }
    async fn handle_event(&self, event_msg: TfTrades) -> anyhow::Result<()> {
        if event_msg.is_empty() {
            return Ok(());
        }
        let open_orders = self.open_orders.clone();

        let trade_q = self.trade_q.clone();
        let positions = self.positions.clone();
        let spreads = self.spreads.clone();
        // update spread first with the last trade
        if let Some(last_trade) = event_msg.last() {
            if let Some(last) = last_trade.trades.last() {
                if let Some(spread_lock) = spreads.get(&last_trade.symbol) {
                    let mut spread = spread_lock.write().await;
                        spread.update(last.price, last_trade.timestamp);
                } else {
                    // Handle missing spread for the symbol
                }
            }
        }

        // TODO: remove used orders to avoid trade reuse inconsistencies
        // TODO: wait for all order status wakes to be notified to increase reliability
        // if any open orders are fillable move them to filled orders and update position and push a trade event to trade queue if it is a order opposite to position
        for tf_trade in event_msg {
            let symbol = tf_trade.symbol.clone();
            let open_orders = open_orders.get(&symbol).unwrap().clone();
            if open_orders.is_empty() {
                continue;
            }
            let order_search = open_orders.iter().map(|o| (o.id, o.clone())).collect::<HashMap<Uuid, Order>>();
                // let mut remove_orders: Vec<OrderStatus> = vec![];
                // let mut remove_for_orders = vec![];
                // let mut re_add = vec![];


                for last in open_orders.iter() {
                    let order = &*last;

                    for trade in &tf_trade.trades {

                    match order.order_type {
                        // check if order is in pending openorders list and send a
                        // cancelled order status if it exists and a filled orderstatus
                        // for this order
                        OrderType::Cancel(order_id)=> {
                             if let Some(order1) = order_search.get(&order_id) {
                                 broadcast(
                                     &self.order_status_subscribers, 
                                     OrderStatus::Canceled(order.clone(), "Cancel Order".to_string())
                                 ).await;
                                 broadcast(
                                     &self.order_status_subscribers, 
                                     OrderStatus::Filled(order1.clone())
                                 ).await;
                                 break
                             }
                         }
                        OrderType::Market => {
                            // maybe should mutate position in order statuses sink?
                            let mut position = positions.get_mut(&symbol).unwrap();
                            if let Some(trade) =
                                    position.apply_order(order, trade.timestamp)
                                {
                                    let mut trade_q = trade_q.lock().await;
                                    trade_q.0.push_back(trade);
                                    trade_q.1.notify_one();
                                }
                            if trade.qty.lt(&order.quantity) {
                                broadcast(
                                    &self.order_status_subscribers,
                                    OrderStatus::PartiallyFilled(
                                        order.clone(),
                                        trade.qty,
                                    )).await;
                            } else {
                                broadcast(
                                    &self.order_status_subscribers,
                                    OrderStatus::Filled(order.clone()),
                                ).await;
                            }
                            break
                        }

                        OrderType::StopLossLimit => {
                            let mut position = positions.get_mut(&symbol).unwrap();
                            if !position.is_open() {
                                broadcast(
                                    &self.order_status_subscribers,
                                    OrderStatus::Canceled(order.clone(), "Stoploss on neutral position".to_string())
                                ).await;
                                break
                            }
                            // fill long stop if stop price is above trade price
                               if position.is_long() {
                                   if trade.price
                                       .le(&order.price) {
                                      
                                       if let Some(trade) =
                                           position.apply_order(order, trade.timestamp)
                                       {
                                           broadcast(&self.trade_subscribers, trade).await;

                                       }
                                       if trade.qty.lt(&order.quantity) {
                                               broadcast(
                                                   &self.order_status_subscribers,
                                                   OrderStatus::PartiallyFilled(
                                                   order.clone(),
                                                   trade.qty,
                                               )).await;
                                           } else {
                                           broadcast(
                                               &self.order_status_subscribers,
                                               OrderStatus::Filled(order.clone()),
                                           ).await;
                                       }
                                   }
                               } else {
                                   // fill short stop if stop price is below trade price
                                   if trade.price
                                       .ge(&order.price) {
                                      
                                       if let Some(trade) =
                                           position.apply_order(order, trade.timestamp)
                                       {
                                           broadcast(&self.trade_subscribers, trade).await;

                                       }
                                       if trade.qty.lt(&order.quantity) {
                                           broadcast(
                                               &self.order_status_subscribers,
                                               OrderStatus::PartiallyFilled(
                                                   order.clone(),
                                                   trade.qty,
                                               )).await;
                                       } else {
                                           broadcast(
                                               &self.order_status_subscribers,
                                               OrderStatus::Filled(order.clone()),
                                           ).await;
                                       }
                                   }

                               }
                                break
                        }

                        OrderType::TakeProfitLimit => {
                            let mut position = positions.get_mut(&symbol).unwrap();
                            if !position.is_open() {
                                broadcast(
                                    &self.order_status_subscribers,
                                    OrderStatus::Canceled(order.clone(), "Take profit on neutral position".to_string())
                                ).await;
                                break
                            }
                            // fill long if target price is above trade price
                               if position.is_long() {
                                   if trade.price
                                       .le(&order.price) {
                                       
                                       if let Some(trade) =
                                           position.apply_order(order, trade.timestamp)
                                       {
                                           broadcast(&self.trade_subscribers, trade).await;

                                       }
                                       if trade.qty.lt(&order.quantity) {
                                               broadcast(
                                                   &self.order_status_subscribers,
                                                   OrderStatus::PartiallyFilled(
                                                   order.clone(),
                                                   trade.qty,
                                               )).await;
                                           } else {
                                           broadcast(
                                               &self.order_status_subscribers,
                                               OrderStatus::Filled(order.clone()),
                                           ).await;
                                       }
                                   }
                               } else {
                                   // fill short if stop price is below trade price
                                   if trade.price
                                       .ge(&order.price) {
                                      
                                       if let Some(trade) =
                                           position.apply_order(order, trade.timestamp)
                                       {
                                           broadcast(&self.trade_subscribers, trade).await;
                                       }
                                       if trade.qty.lt(&order.quantity) {
                                           broadcast(
                                               &self.order_status_subscribers,
                                               OrderStatus::PartiallyFilled(
                                                   order.clone(),
                                                   trade.qty,
                                               )).await;
                                       } else {
                                           broadcast(
                                               &self.order_status_subscribers,
                                               OrderStatus::Filled(order.clone()),
                                           ).await;
                                       }
                                   }

                               }
                                break
                        }

                        OrderType::Limit => {
                            let mut position = positions.get_mut(&symbol).unwrap();
                            if order.side == Side::Bid {
                                if trade.price
                                    .le(&order.price) {

                                    if let Some(trade) =
                                        position.apply_order(order, trade.timestamp)
                                    {
                                        broadcast(&self.trade_subscribers, trade).await;

                                    }
                                    if trade.qty.lt(&order.quantity) {
                                        broadcast(
                                            &self.order_status_subscribers,
                                            OrderStatus::PartiallyFilled(
                                                order.clone(),
                                                trade.qty,
                                            )).await;
                                    } else {
                                        broadcast(
                                            &self.order_status_subscribers,
                                            OrderStatus::Filled(order.clone()),
                                        ).await;
                                    }
                                }
                            } else {
                                if trade.price
                                    .ge(&order.price) {

                                    if let Some(trade) =
                                        position.apply_order(order, trade.timestamp)
                                    {
                                        broadcast(&self.trade_subscribers, trade).await;
                                    }
                                    if trade.qty.lt(&order.quantity) {
                                        broadcast(
                                            &self.order_status_subscribers,
                                            OrderStatus::PartiallyFilled(
                                                order.clone(),
                                                trade.qty,
                                            )).await;
                                    } else {
                                        broadcast(
                                            &self.order_status_subscribers,
                                            OrderStatus::Filled(order.clone()),
                                        ).await;
                                    }
                                }
                            }
                            break;
                        }
                        OrderType::StopLoss(limit_order_id) => {
                            // first we check if the limit order is filled
                            // if it's not and is still an open order ignore and continue
                            // if it's filled we try to fill this order
                            // if it's not still an open order and doesn't exist in order history
                            // cancel this order
                        }
                        _ => {}
                    }
                    
                }

            }
        }
        Ok(())
    }
}

// TODO: receive statuses from the TFTrades sink and process them here instead of there
#[async_trait]
impl EventSink<OrderStatus> for SimulatedAccount {
    fn get_receiver(&self) -> Receiver<(OrderStatus, Option<Arc<Notify>>)> {
        self.order_statuses.clone().activate()
    }
    async fn handle_event(&self, event_msg: OrderStatus) -> anyhow::Result<()> {
        println!("orderStatus: {:?}", event_msg);
        let open_orders = self.open_orders.clone();
        match &event_msg {
            // add to order set
            OrderStatus::Pending(order) => {
                trace!("Adding pending order to order set: {:?}", order);
                if let Some(orders) = open_orders.get_mut(&order.symbol) {
                    orders.insert(order.clone());
                } else {
                    let mut new_set = Arc::new(DashSet::new());
                    new_set.insert(order.clone());
                    open_orders.insert(order.symbol.clone(), new_set);
                }
            }
            // valid types are limit
            OrderStatus::PartiallyFilled(order, delta) => {
                let mut new_order = order.clone();
                new_order.quantity = order.quantity - delta;
                trace!(
                    "adding new order {:?} for partially filled order: {:?}",
                    new_order,
                    order
                );
                if let Some(orders) = open_orders.get_mut(&order.symbol) {
                    orders.remove(order);
                    orders.insert(new_order);
                } else {
                    let mut new_set = Arc::new(DashSet::new());
                    new_set.insert(order.clone());
                    open_orders.insert(order.symbol.clone(), new_set);
                }
            }
            // remove from order set
            OrderStatus::Filled(order) => {
                trace!("Removing filled order from order set: {:?}", order);
                if let Some(orders) = open_orders.get_mut(&order.symbol) {
                    orders.remove(order);
                }
                println!("done: {}", "");
            }
            // log reason and remove
            OrderStatus::Canceled(order, reason) => {
                trace!(
                    "Removing canceled order with reason `{}` from order set: {:?}",
                    reason,
                    order
                );
                if let Some(orders) = open_orders.get_mut(&order.symbol) {
                    orders.remove(order);
                }
            }
        }
        Ok(())
    }
}

#[async_trait]
impl EventSink<Order> for SimulatedExecutor {
    fn get_receiver(&self) -> Receiver<(Order, Option<Arc<Notify>>)> {
        self.orders.clone().activate()
    }
    async fn handle_event(&self, event_msg: Order) -> anyhow::Result<()> {
        println!("order: {:?}", event_msg);
        let fut = self.process_order(event_msg);
        let order_status_q = self.order_status_q.clone();
        if let Ok(res) = fut {
            order_status_q.write().await.push_back(res);
        }
        Ok(())
    }
}

#[async_trait]
impl EventEmitter<OrderStatus> for SimulatedExecutor {
    fn get_subscribers(&self) -> Arc<RwLock<Sender<(OrderStatus, Option<Arc<Notify>>)>>> {
        self.order_status_subscribers.clone()
    }

    async fn emit(&self) -> anyhow::Result<JoinHandle<()>> {
        let q = self.order_status_q.clone();
        let subs = self.order_status_subscribers.clone();
        Ok(tokio::spawn(async move {
            loop {
                let mut w = q.write().await;
                let order_status = w.pop_front();
                std::mem::drop(w);
                if let Some(order_status) = order_status {
                    let subs = subs.read().await;
                    match subs.broadcast((order_status.clone(), None)).await {
                        Ok(_) => {}
                        Err(e) => {
                            eprintln!("error broadcasting order status: {:?}", e);
                        }
                    }
                }
            }
        }))
    }
}

#[async_trait]
impl ExchangeAccountInfo for SimulatedAccount {
    fn get_exchange_id(&self) -> ExchangeId {
        ExchangeId::Simulated
    }

    async fn get_open_orders(&self, symbol: &Symbol) -> Arc<DashSet<Order>> {
        self.open_orders.get(symbol).unwrap().clone()
    }

    async fn get_symbol_account(&self, symbol: &Symbol) -> SymbolAccount {
        self.symbol_accounts.get(symbol).unwrap().clone()
    }

    async fn get_past_trades(&self, symbol: &Symbol, length: Option<usize>) -> Arc<DashSet<Trade>> {
        if let Some(length) = length {
            let mut trades_vec = self
                .trade_history
                .get(symbol)
                .unwrap()
                .iter()
                .map(|v| v.clone())
                .collect::<Vec<_>>();
            trades_vec.sort_by(|a, b| b.time.cmp(&a.time));
            trades_vec.truncate(length);
            let trades = trades_vec.into_iter().collect::<DashSet<Trade>>();
            Arc::new(trades)
        } else {
            self.trade_history.get(symbol).unwrap().clone()
        }
    }

    async fn get_position(&self, symbol: &Symbol) -> Arc<Position> {
        let position = self.positions.get(symbol).unwrap();
        Arc::new(position.clone())
    }

    async fn get_spread(&self, symbol: &Symbol) -> Arc<Spread> {
        let spread_lock = self.spreads.get(symbol).unwrap();
        let spread = spread_lock.read().await.clone();
        Arc::new(spread)
    }

    async fn get_order(&self, symbol: &Symbol, id: &Uuid) -> Vec<Order> {
        let order_history = self.order_history.get(symbol).unwrap().clone();
        let res = order_history
            .iter()
            .filter_map(|o| match &*o {
                OrderStatus::Filled(or) => {
                    if &or.id == id {
                        Some(or.clone())
                    } else {
                        None
                    }
                }
                _ => None,
            })
            .collect();
        res
    }
}

#[async_trait]
impl ExchangeAccount for SimulatedAccount {
    async fn limit_long(&self, order: Order) -> anyhow::Result<OrderStatus> {
        Ok(OrderStatus::Pending(order))
    }

    async fn limit_short(&self, order: Order) -> anyhow::Result<OrderStatus> {
        Ok(OrderStatus::Pending(order))
    }

    async fn market_long(&self, order: Order) -> anyhow::Result<OrderStatus> {
        Ok(OrderStatus::Filled(order))
    }

    async fn market_short(&self, order: Order) -> anyhow::Result<OrderStatus> {
        Ok(OrderStatus::Filled(order))
    }

    async fn cancel_order(&self, order: Order) -> anyhow::Result<OrderStatus> {
        Ok(OrderStatus::Canceled(order, "canceled".to_string()))
    }
}

impl TradeExecutor for SimulatedExecutor {
    type Account = SimulatedAccount;
    fn get_account(&self) -> Arc<Self::Account> {
        self.account.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::events::{EventEmitter, EventSink};
    use crate::executors::ExchangeAccountInfo;
    use crate::types::{
        ClosePolicy, ExchangeId, Order, OrderStatus, OrderType, Side, Symbol, TfTrade, TfTrades,
        TradeEntry,
    };
    use rust_decimal::Decimal;

    #[tokio::test]
    async fn test_simulated_account_cancel_order() -> anyhow::Result<()> {
        let tf_trades_channel = async_broadcast::broadcast(100);
        let order_statuses_channel = async_broadcast::broadcast(100);

        let symbol = Symbol {
            symbol: "TST/USDT".to_string(),
            exchange: ExchangeId::Simulated,
            base_asset_precision: 1,
            quote_asset_precision: 2,
        };
        let simulated_account = Arc::new(
            SimulatedAccount::new(
                tf_trades_channel.1.deactivate(),
                order_statuses_channel.1.deactivate(),
                vec![symbol.clone()],
                Arc::new(RwLock::new(order_statuses_channel.0.clone())),
            )
            .await,
        );
        let open_orders = simulated_account.get_open_orders(&symbol).await;

        tokio::spawn(async move {
            EventSink::<OrderStatus>::listen(simulated_account.clone()).unwrap();
        });

        // Create a pending order
        let os = OrderStatus::Pending(Order {
            id: Uuid::new_v4(),
            symbol: symbol.clone(),
            side: Side::Bid,
            price: Decimal::new(100, 1),
            quantity: Decimal::new(100, 1),
            time: 123,
            order_type: OrderType::Limit,
            lifetime: 0,
            close_policy: ClosePolicy::None,
        });

        // Broadcast the pending order
        let notifier = Arc::new(Notify::new());
        let n = notifier.notified();
        order_statuses_channel
            .0
            .broadcast((os.clone(), Some(notifier.clone())))
            .await
            .unwrap();
        n.await;

        // Cancel the order
        let canceled_order = OrderStatus::Canceled(os.order(), "Test cancel".into());
        let n = notifier.notified();
        order_statuses_channel
            .0
            .broadcast((canceled_order.clone(), Some(notifier.clone())))
            .await
            .unwrap();
        n.await;

        assert!(open_orders.is_empty(), "Order was not properly canceled");
        Ok(())
    }

    #[tokio::test]
    async fn test_simulated_account_apply_stop_loss() -> anyhow::Result<()> {
        let tf_trades_channel = async_broadcast::broadcast(100);
        let order_statuses_channel = async_broadcast::broadcast(100);

        let symbol = Symbol {
            symbol: "TST/USDT".to_string(),
            exchange: ExchangeId::Simulated,
            base_asset_precision: 1,
            quote_asset_precision: 2,
        };
        let simulated_account = Arc::new(
            SimulatedAccount::new(
                tf_trades_channel.1.deactivate(),
                order_statuses_channel.1.deactivate(),
                vec![symbol.clone()],
                Arc::new(RwLock::new(order_statuses_channel.0.clone())),

            )
                .await,
        );

        let sa1 = simulated_account.clone();
        let sa2 = simulated_account.clone();
        tokio::spawn(async move {
            EventSink::<OrderStatus>::listen(sa1).unwrap();
        });
        tokio::spawn(async move {
            EventSink::<TfTrades>::listen(sa2).unwrap();
        });

        let order_id =  Uuid::new_v4();

        // Create an initial position
        let os = OrderStatus::Pending(Order {
            id: order_id,
            symbol: symbol.clone(),
            side: Side::Bid,
            price: Decimal::new(100, 1),
            quantity: Decimal::new(100, 1),
            time: 123,
            order_type: OrderType::Market,
            lifetime: u64::MAX,
            close_policy: ClosePolicy::None,
        });

        // Broadcast and fill the pending order
        let notifier = Arc::new(Notify::new());
        let n = notifier.notified();
        order_statuses_channel
            .0
            .broadcast((os.clone(), Some(notifier.clone())))
            .await
            .unwrap();
        n.await;

        let n = notifier.notified();
        let entry = TradeEntry {
            trade_id: 1,
            price: dec!(11.0),
            qty: dec!(100.0),
            timestamp: 0,
            delta: dec!(0.0),
            symbol: symbol.clone(),
        };
        tf_trades_channel
            .0
            .broadcast((
                vec![TfTrade {
                    symbol: symbol.clone(),
                    tf: 1,
                    id: 1,
                    timestamp: 124,
                    trades: vec![entry.clone()],
                    min_trade_time: entry.clone(),
                    max_trade_time: entry,
                }],
                Some(notifier.clone()),
            ))
            .await
            .unwrap();
        n.await;
        let open_orders = simulated_account.get_open_orders(&symbol).await;
        let position = simulated_account.get_position(&symbol).await;
        assert!(open_orders.is_empty(), "initial position order not properly executed");
        assert!(position.is_long(), "incorrect position");
        // Create a stop-loss order
        let os = OrderStatus::Pending(Order {
            id: Uuid::new_v4(),
            symbol: symbol.clone(),
            side: Side::Ask,
            price: Decimal::new(90, 1),
            quantity: Decimal::new(100, 1),
            time: 123,
            order_type: OrderType::StopLossLimit,
            lifetime: u64::MAX,
            close_policy: ClosePolicy::None,
        });

        // Broadcast the stop-loss order
        let notifier = Arc::new(Notify::new());
        let n = notifier.notified();
        order_statuses_channel
            .0
            .broadcast((os.clone(), Some(notifier.clone())))
            .await
            .unwrap();
        n.await;

        // Simulate a trade that triggers the stop-loss
        let n = notifier.notified();
        let entry = TradeEntry {
            trade_id: 1,
            price: dec!(8.0),
            qty: dec!(100.0),
            timestamp: 0,
            delta: dec!(0.0),
            symbol: symbol.clone(),
        };
        tf_trades_channel
            .0
            .broadcast((
                vec![TfTrade {
                    symbol: symbol.clone(),
                    tf: 1,
                    id: 1,
                    timestamp: 124,
                    trades: vec![entry.clone()],
                    min_trade_time: entry.clone(),
                    max_trade_time: entry,
                }],
                Some(notifier.clone()),
            ))
            .await
            .unwrap();
        n.await;

        // Check if stop-loss is executed
        assert!(open_orders.is_empty(), "Stop-loss order was not properly executed");
        let position = simulated_account.get_position(&symbol).await;
        assert!(!position.is_open(), "Position is still open");
        Ok(())
    }
    #[tokio::test]
    async fn test_simulated_account_apply_take_profit() -> anyhow::Result<()> {
        let tf_trades_channel = async_broadcast::broadcast(100);
        let order_statuses_channel = async_broadcast::broadcast(100);

        let symbol = Symbol {
            symbol: "TST/USDT".to_string(),
            exchange: ExchangeId::Simulated,
            base_asset_precision: 1,
            quote_asset_precision: 2,
        };
        let simulated_account = Arc::new(
            SimulatedAccount::new(
                tf_trades_channel.1.deactivate(),
                order_statuses_channel.1.deactivate(),
                vec![symbol.clone()],
                Arc::new(RwLock::new(order_statuses_channel.0.clone())),

            )
                .await,
        );

        let sa1 = simulated_account.clone();
        let sa2 = simulated_account.clone();
        tokio::spawn(async move {
            EventSink::<OrderStatus>::listen(sa1).unwrap();
        });
        tokio::spawn(async move {
            EventSink::<TfTrades>::listen(sa2).unwrap();
        });

        let order_id =  Uuid::new_v4();

        // Create an initial position
        let os = OrderStatus::Pending(Order {
            id: order_id,
            symbol: symbol.clone(),
            side: Side::Bid,
            price: Decimal::new(100, 1),
            quantity: Decimal::new(100, 1),
            time: 123,
            order_type: OrderType::Market,
            lifetime: u64::MAX,
            close_policy: ClosePolicy::None,
        });

        // Broadcast and fill the pending order
        let notifier = Arc::new(Notify::new());
        let n = notifier.notified();
        order_statuses_channel
            .0
            .broadcast((os.clone(), Some(notifier.clone())))
            .await
            .unwrap();
        n.await;

        let n = notifier.notified();
        let entry = TradeEntry {
            trade_id: 1,
            price: dec!(11.0),
            qty: dec!(100.0),
            timestamp: 0,
            delta: dec!(0.0),
            symbol: symbol.clone(),
        };
        tf_trades_channel
            .0
            .broadcast((
                vec![TfTrade {
                    symbol: symbol.clone(),
                    tf: 1,
                    id: 1,
                    timestamp: 124,
                    trades: vec![entry.clone()],
                    min_trade_time: entry.clone(),
                    max_trade_time: entry,
                }],
                Some(notifier.clone()),
            ))
            .await
            .unwrap();
        n.await;
        let open_orders = simulated_account.get_open_orders(&symbol).await;
        let position = simulated_account.get_position(&symbol).await;
        assert!(open_orders.is_empty(), "initial position order not properly executed");
        assert!(position.is_long(), "incorrect position");
        // Create a take-profit order
        let os = OrderStatus::Pending(Order {
            id: Uuid::new_v4(),
            symbol: symbol.clone(),
            side: Side::Ask,
            price: Decimal::new(190, 1),
            quantity: Decimal::new(100, 1),
            time: 123,
            order_type: OrderType::TakeProfitLimit,
            lifetime: u64::MAX,
            close_policy: ClosePolicy::None,
        });

        // Broadcast the stop-loss order
        let notifier = Arc::new(Notify::new());
        let n = notifier.notified();
        order_statuses_channel
            .0
            .broadcast((os.clone(), Some(notifier.clone())))
            .await
            .unwrap();
        n.await;

        // Simulate a trade that triggers the stop-loss
        let n = notifier.notified();
        let entry = TradeEntry {
            trade_id: 1,
            price: dec!(19.0),
            qty: dec!(100.0),
            timestamp: 0,
            delta: dec!(0.0),
            symbol: symbol.clone(),
        };
        tf_trades_channel
            .0
            .broadcast((
                vec![TfTrade {
                    symbol: symbol.clone(),
                    tf: 1,
                    id: 1,
                    timestamp: 124,
                    trades: vec![entry.clone()],
                    min_trade_time: entry.clone(),
                    max_trade_time: entry,
                }],
                Some(notifier.clone()),
            ))
            .await
            .unwrap();
        n.await;

        // Check if stop-loss is executed
        assert!(open_orders.is_empty(), "take-profit order was not properly executed");
        let position = simulated_account.get_position(&symbol).await;
        assert!(!position.is_open(), "Position is still open");
        Ok(())
    }
    #[tokio::test]
    async fn test_simulated_account_add_pending_order() -> anyhow::Result<()> {
        let tf_trades_channel = async_broadcast::broadcast(100);
        let order_statuses_channel = async_broadcast::broadcast(100);

        let symbol = Symbol {
            symbol: "TST/USDT".to_string(),
            exchange: ExchangeId::Simulated,
            base_asset_precision: 1,
            quote_asset_precision: 2,
        };
        let simulated_account = Arc::new(
            SimulatedAccount::new(
                tf_trades_channel.1.deactivate(),
                order_statuses_channel.1.deactivate(),
                vec![symbol.clone()],
                Arc::new(RwLock::new(order_statuses_channel.0.clone())),
            )
            .await,
        );
        let s_a = simulated_account.clone();
        tokio::spawn(async move {
            EventSink::<OrderStatus>::listen(simulated_account).unwrap();
        });
        let os = OrderStatus::Pending(Order {
            id: Uuid::new_v4(),
            symbol: symbol.clone(),
            side: Side::Bid,
            price: Decimal::new(100, 1),
            quantity: Decimal::new(100, 1),
            time: 123,
            order_type: OrderType::Limit,
            lifetime: 0,
            close_policy: ClosePolicy::None,
        });
        let notifier = Arc::new(Notify::new());
        let n = notifier.notified();
        order_statuses_channel
            .0
            .broadcast((os.clone(), Some(notifier.clone())))
            .await
            .unwrap();
        n.await;

        let open_orders = s_a.get_open_orders(&symbol).await;
        assert_eq!(open_orders.len(), 1);
        assert!(open_orders.contains(&os.order()));
        Ok(())
    }

    #[tokio::test]
    async fn test_simulated_account_fill_pending_order() -> anyhow::Result<()> {
        let tf_trades_channel = async_broadcast::broadcast(100);
        let order_statuses_channel = async_broadcast::broadcast(100);

        let symbol = Symbol {
            symbol: "TST/USDT".to_string(),
            exchange: ExchangeId::Simulated,
            base_asset_precision: 1,
            quote_asset_precision: 2,
        };
        let simulated_account = Arc::new(
            SimulatedAccount::new(
                tf_trades_channel.1.deactivate(),
                order_statuses_channel.1.deactivate(),
                vec![symbol.clone()],
                Arc::new(RwLock::new(order_statuses_channel.0.clone())),
            )
            .await,
        );
        let s_a1 = simulated_account.clone();
        let s_a = simulated_account.clone();
        tokio::spawn(async move {
            EventSink::<OrderStatus>::listen(simulated_account).unwrap();
        });
        tokio::spawn(async move {
            EventSink::<TfTrades>::listen(s_a1).unwrap();
        });
        let os = OrderStatus::Pending(Order {
            id: Uuid::new_v4(),
            symbol: symbol.clone(),
            side: Side::Bid,
            price: Decimal::new(100, 1),
            quantity: Decimal::new(100, 1),
            time: 123,
            order_type: OrderType::Limit,
            lifetime: 0,
            close_policy: ClosePolicy::None,
        });
        let notifier = Arc::new(Notify::new());
        let n = notifier.notified();

        order_statuses_channel
            .0
            .broadcast((os.clone(), Some(notifier.clone())))
            .await
            .unwrap();
        n.await;
        let n = notifier.notified();
        let entry = TradeEntry {
            trade_id: 1,
            price: dec!(9.0),
            qty: dec!(100.0),
            timestamp: 0,
            delta: dec!(0.0),
            symbol: symbol.clone(),
        };
        tf_trades_channel
            .0
            .broadcast((
                vec![TfTrade {
                    symbol: symbol.clone(),
                    tf: 1,
                    id: 1,
                    timestamp: 124,
                    trades: vec![entry.clone()],
                    min_trade_time: entry.clone(),
                    max_trade_time: entry,
                }],
                Some(notifier.clone()),
            ))
            .await
            .unwrap();
        n.await;

        let open_orders = s_a.get_open_orders(&symbol).await;
        assert_eq!(open_orders.len(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn test_simulated_account_partial_fill_pending_order() -> anyhow::Result<()> {
        let tf_trades_channel = async_broadcast::broadcast(100);
        let order_statuses_channel = async_broadcast::broadcast(100);

        let symbol = Symbol {
            symbol: "TST/USDT".to_string(),
            exchange: ExchangeId::Simulated,
            base_asset_precision: 1,
            quote_asset_precision: 2,
        };
        let simulated_account = Arc::new(
            SimulatedAccount::new(
                tf_trades_channel.1.deactivate(),
                order_statuses_channel.1.deactivate(),
                vec![symbol.clone()],
                Arc::new(RwLock::new(order_statuses_channel.0.clone())),
            )
            .await,
        );
        let s_a1 = simulated_account.clone();
        let s_a = simulated_account.clone();
        tokio::spawn(async move {
            EventSink::<OrderStatus>::listen(simulated_account).unwrap();
        });
        tokio::spawn(async move {
            EventSink::<TfTrades>::listen(s_a1).unwrap();
        });
        let os = OrderStatus::Pending(Order {
            id: Uuid::new_v4(),
            symbol: symbol.clone(),
            side: Side::Bid,
            price: Decimal::new(100, 1),
            quantity: Decimal::new(100, 0),
            time: 123,
            order_type: OrderType::Limit,
            lifetime: 0,
            close_policy: ClosePolicy::None,
        });
        order_statuses_channel
            .0
            .broadcast((os.clone(), None))
            .await
            .unwrap();
        while !order_statuses_channel.0.is_empty() {
            tokio::time::sleep(std::time::Duration::from_micros(100)).await;
        }
        let notifier = Arc::new(Notify::new());
        let n = notifier.notified();
        let entry = TradeEntry {
            trade_id: 1,
            price: dec!(10.0),
            qty: dec!(90.0),
            timestamp: 0,
            delta: dec!(0.0),
            symbol: symbol.clone(),
        };
        tf_trades_channel
            .0
            .broadcast((
                vec![TfTrade {
                    symbol: symbol.clone(),
                    tf: 1,
                    id: 1,
                    timestamp: 124,
                    trades: vec![entry.clone()],
                    min_trade_time: entry.clone(),
                    max_trade_time: entry,
                }],
                Some(notifier.clone()),
            ))
            .await
            .unwrap();

        n.await;

        let open_orders = s_a.get_open_orders(&symbol).await;
        assert_eq!(open_orders.len(), 1);

        Ok(())
    }

    #[tokio::test]
    async fn test_simulated_account_emits_trade() -> anyhow::Result<()> {
        let tf_trades_channel = async_broadcast::broadcast(100);
        let order_statuses_channel = async_broadcast::broadcast(100);
        let (trades_sender, mut trades_receiver) = async_broadcast::broadcast(100);
        let symbol = Symbol {
            symbol: "TST/USDT".to_string(),
            exchange: ExchangeId::Simulated,
            base_asset_precision: 1,
            quote_asset_precision: 2,
        };
        let mut simulated_account = SimulatedAccount::new(
            tf_trades_channel.1.deactivate(),
            order_statuses_channel.1.deactivate(),
            vec![symbol.clone()],
            Arc::new(RwLock::new(order_statuses_channel.0.clone())),
        )
        .await;
        simulated_account.subscribe(trades_sender).await;
        let simulated_account = Arc::new(simulated_account);
        let s_a1 = simulated_account.clone();
        let s_a = simulated_account.clone();
        tokio::spawn(async move {
            EventSink::<OrderStatus>::listen(simulated_account).unwrap();
        });
        tokio::spawn(async move {
            EventSink::<TfTrades>::listen(s_a1).unwrap();
        });
        let os = OrderStatus::Filled(Order {
            id: Uuid::new_v4(),
            symbol: symbol.clone(),
            side: Side::Bid,
            price: Decimal::new(100, 1),
            quantity: Decimal::new(100, 0),
            time: 123,
            order_type: OrderType::Market,
            lifetime: 0,
            close_policy: ClosePolicy::None,
        });

        let closing_order = OrderStatus::Filled(Order {
            id: Uuid::new_v4(),
            symbol: symbol.clone(),
            side: Side::Ask,
            price: Decimal::new(100, 1),
            quantity: Decimal::new(100, 0),
            time: 123,
            order_type: OrderType::Market,
            lifetime: 0,
            close_policy: ClosePolicy::None,
        });

        let notifier = Arc::new(Notify::new());
        let n = notifier.notified();
        let n2 = notifier.notified();
        order_statuses_channel
            .0
            .broadcast((os.clone(), Some(notifier.clone())))
            .await
            .unwrap();
        n.await;

        order_statuses_channel
            .0
            .broadcast((closing_order.clone(), Some(notifier.clone())))
            .await
            .unwrap();
        s_a.emit().await?;
        n2.await;
        let notifier = Arc::new(Notify::new());
        let n = notifier.notified();
        let entry = TradeEntry {
            trade_id: 1,
            price: dec!(10.0),
            qty: dec!(90.0),
            timestamp: 0,
            delta: dec!(0.0),
            symbol: symbol.clone(),
        };
        tf_trades_channel
            .0
            .broadcast((
                vec![TfTrade {
                    symbol: symbol.clone(),
                    tf: 1,
                    id: 1,
                    timestamp: 124,
                    trades: vec![entry.clone()],
                    min_trade_time: entry.clone(),
                    max_trade_time: entry,
                }],
                Some(notifier.clone()),
            ))
            .await
            .unwrap();

        n.await;
        // let (trade, _) = trades_receiver.recv().await.unwrap();
        // assert_eq!(trade.realized_pnl, Decimal::ZERO);
        // assert_eq!(trade.position_side, Side::Bid);
        // assert_eq!(trade.side, Side::Ask);
        //
        // let open_orders = s_a.get_open_orders(&symbol).await;
        // assert_eq!(open_orders.len(), 0);
        Ok(())
    }
}
