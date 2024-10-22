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
            order_status_subscribers
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
        
        let mut account =
            SimulatedAccount::new(trades_rx, order_statuses_channel.1.deactivate(), symbols, Arc::new(RwLock::new(order_statuses_channel.0.clone()))).await;
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

async fn broadcast_and_wait<T: Clone + Debug>(channel: &Arc<RwLock<Sender<(T, Option<Arc<Notify>>)>>>, value: T) {
    let notifier = Arc::new(Notify::new());
    let notified = notifier.notified();
    let channel = channel.read().await;
    if let Err(e) = channel.broadcast((value, Some(notifier.clone()))).await {
        trace!("Failed to broadcast notification: {}", e);
    }
    notified.await;
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
        let filled_orders = self.order_history.clone();
        let positions = self.positions.clone();
        let spreads = self.spreads.clone();
        // update spread first with the last trade
        if let Some(last_trade) = event_msg.last() {
            if let Some(last) = last_trade.trades.last() {
                if let Some(spread_lock) = spreads.get(&last_trade.symbol) {
                    let mut spread = spread_lock.write().await;
                    if let Some(price_decimal) = Decimal::from_f64(last.price) {
                        spread.update(price_decimal, last_trade.timestamp);
                    } else {
                        // Handle invalid price conversion
                    }
                } else {
                    // Handle missing spread for the symbol
                }
            }
        }

        // if any open orders are fillable move them to filled orders and update position and push a trade event to trade queue if it is a order opposite to position
        for tf_trade in event_msg {
            let symbol = tf_trade.symbol.clone();
            let open_orders = open_orders.get(&symbol).unwrap().clone();
            if open_orders.is_empty() {
                continue;
            }
            let order_search = open_orders.iter().map(|o| (o.id, o.clone())).collect::<HashMap<Uuid, Order>>();
            for trade in tf_trade.trades {
                // let mut remove_orders: Vec<OrderStatus> = vec![];
                // let mut remove_for_orders = vec![];
                // let mut re_add = vec![];


                let mut filled = false;
                for last in open_orders.iter() {
                    if filled {
                        break;
                    }
                    let order = &*last;
                    match order.order_type {
                        // check if order is in pending openorders list and send a 
                        // cancelled order status if it exists and a filled orderstatus
                        // for this order
                         OrderType::Cancel(order_id)=> {
                             if let Some(order1) = order_search.get(&order_id) {
                                 broadcast_and_wait(
                                     &self.order_status_subscribers, 
                                     OrderStatus::Canceled(order.clone(), "Cancel Order".to_string())
                                 ).await;
                                 broadcast_and_wait(
                                     &self.order_status_subscribers, 
                                     OrderStatus::Filled(order1.clone())
                                 ).await;
                             }
                         }
                        OrderType::Market | OrderType::Limit => {
                            if (order.side == Side::Bid
                                && (Decimal::from_f64(trade.price)
                                .unwrap()
                                .lt(&order.price)
                                || order
                                .price
                                .eq(&Decimal::from_f64(trade.price).unwrap())))
                                || (order.side == Side::Ask
                                && (order
                                .price
                                .lt(&Decimal::from_f64(trade.price).unwrap())
                                || order
                                .price
                                .eq(&Decimal::from_f64(trade.price).unwrap())))
                            {
                                // filled = true;
                                // remove_orders.push(last.clone());   
                                // 
                                // let mut position = positions.get_mut(&symbol).unwrap();
                                // let filled_qty = Decimal::from_f64(trade.qty).unwrap();
                                // let filled_price = Decimal::from_f64(trade.price).unwrap();
                                // let mut filled_order = order.clone();
                                // filled_order.quantity = filled_qty.max(order.quantity);
                                // filled_order.price = filled_price;
                                // filled_order.time = trade.timestamp;
                                // 
                                // if let Some(trade) =
                                //     position.apply_order(&filled_order, trade.timestamp)
                                // {
                                //     let mut trade_q = trade_q.lock().await;
                                //     trade_q.0.push_back(trade);
                                //     trade_q.1.notify_one();
                                // }
                                // if filled_qty < order.quantity {
                                //     re_add.push(OrderStatus::PartiallyFilled(
                                //         order.clone(),
                                //         Decimal::from_f64(trade.qty).unwrap(),
                                //     ));
                                // } else {
                                //     let filled_orders =
                                //         filled_orders.get_mut(&symbol).unwrap();
                                // 
                                //     filled_orders
                                //         .insert(OrderStatus::Filled(order.clone()));
                                // }
                            }
                        }
                        //     let trade_price = Decimal::from_f64(trade.price).unwrap();
                        //     if (order.side == Side::Bid
                        //         && (trade_price.gt(&order.price)
                        //         || order.price.eq(&trade_price)))
                        //         || (order.side == Side::Ask
                        //         && (order.price.gt(&trade_price)
                        //         || order.price.eq(&trade_price)))
                        //     {
                        //         remove_orders.push(last.clone());
                        // 
                        //         filled = true;
                        //         // println!("Filling order {:?} {}", order, trade.price);
                        //         let mut position = positions.get_mut(&symbol).unwrap();
                        //         let mut or = order.clone();
                        //         or.quantity = position.qty;
                        //         or.price = Decimal::from_f64(trade.price).unwrap();
                        //         println!("[?] SLT removing {:?} {:?}", or, trade);
                        //         if let Some(trade) =
                        //             position.apply_order(&or, trade.timestamp)
                        //         {
                        //             let mut trade_q = trade_q.lock().await;
                        //             trade_q.0.push_back(trade);
                        //             trade_q.1.notify_one();
                        //         }
                        // 
                        //         remove_for_orders.push(for_id);
                        // 
                        //         let filled_orders = filled_orders.get_mut(&symbol).unwrap();
                        // 
                        //         filled_orders.insert(OrderStatus::Filled(or.clone()));
                        //     } else {
                        //         match order.side {
                        //             Side::Bid => {
                        //                 if trade_price + delta < order.price {
                        //                     remove_orders.push(last.clone());
                        // 
                        //                     let mut or = order.clone();
                        //                     or.price = trade_price + delta;
                        //                     // println!("[?] executor > Adjusting price from {} to {} on side BID due to {}", order.price, or.price, trade_price);
                        //                     re_add.push(OrderStatus::Pending(or));
                        //                 }
                        //             }
                        //             Side::Ask => {
                        //                 if trade_price - delta > order.price {
                        //                     remove_orders.push(last.clone());
                        // 
                        //                     let mut or = order.clone();
                        //                     or.price = trade_price - delta;
                        //                     // println!("[?] executor > Adjusting price from {} to {} on side ASK due to {}", order.price, or.price, trade_price);
                        // 
                        //                     re_add.push(OrderStatus::Pending(or));
                        //                 }
                        //             }
                        //         }
                        //     }
                        // }
                        // OrderType::Market | OrderType::Limit => {
                        //     if (order.side == Side::Bid
                        //         && (Decimal::from_f64(trade.price)
                        //         .unwrap()
                        //         .lt(&order.price)
                        //         || order
                        //         .price
                        //         .eq(&Decimal::from_f64(trade.price).unwrap())))
                        //         || (order.side == Side::Ask
                        //         && (order
                        //         .price
                        //         .lt(&Decimal::from_f64(trade.price).unwrap())
                        //         || order
                        //         .price
                        //         .eq(&Decimal::from_f64(trade.price).unwrap())))
                        //     {
                        //         filled = true;
                        //         remove_orders.push(last.clone());
                        // 
                        //         let mut position = positions.get_mut(&symbol).unwrap();
                        //         let filled_qty = Decimal::from_f64(trade.qty).unwrap();
                        //         let filled_price = Decimal::from_f64(trade.price).unwrap();
                        //         let mut filled_order = order.clone();
                        //         filled_order.quantity = filled_qty.max(order.quantity);
                        //         filled_order.price = filled_price;
                        //         filled_order.time = trade.timestamp;
                        // 
                        //         if let Some(trade) =
                        //             position.apply_order(&filled_order, trade.timestamp)
                        //         {
                        //             let mut trade_q = trade_q.lock().await;
                        //             trade_q.0.push_back(trade);
                        //             trade_q.1.notify_one();
                        //         }
                        //         if filled_qty < order.quantity {
                        //             re_add.push(OrderStatus::PartiallyFilled(
                        //                 order.clone(),
                        //                 Decimal::from_f64(trade.qty).unwrap(),
                        //             ));
                        //         } else {
                        //             let filled_orders =
                        //                 filled_orders.get_mut(&symbol).unwrap();
                        // 
                        //             filled_orders
                        //                 .insert(OrderStatus::Filled(order.clone()));
                        //         }
                        //     }
                        // }
                        // 
                        // // skip partial fills for target and stop orders, if price reaches one of the targets or stop, the order will be immediately filled
                        // OrderType::TakeProfit(for_id) => {
                        //     if remove_for_orders.iter().any(|id| *id == for_id) {
                        //         continue;
                        //     }
                        //     if (order.side == Side::Bid
                        //         && (Decimal::from_f64(trade.price)
                        //         .unwrap()
                        //         .lt(&order.price)
                        //         || order
                        //         .price
                        //         .eq(&Decimal::from_f64(trade.price).unwrap())))
                        //         || (order.side == Side::Ask
                        //         && (order
                        //         .price
                        //         .lt(&Decimal::from_f64(trade.price).unwrap())
                        //         || order
                        //         .price
                        //         .eq(&Decimal::from_f64(trade.price).unwrap())))
                        //     {
                        //         filled = true;
                        //         remove_orders.push(last.clone());
                        // 
                        //         println!("Filling order {:?} {}", order, trade.price);
                        //         let mut position = positions.get_mut(&symbol).unwrap();
                        //         let mut or = order.clone();
                        //         or.price = Decimal::from_f64(trade.price).unwrap();
                        //         if let Some(trade) =
                        //             position.apply_order(&or, trade.timestamp)
                        //         {
                        //             let mut trade_q = trade_q.lock().await;
                        //             trade_q.0.push_back(trade);
                        //             trade_q.1.notify_one();
                        //         }
                        //         println!("[?] TP removing {:?} {:?}", order, trade);
                        //         remove_for_orders.push(for_id);
                        // 
                        //         let filled_orders = filled_orders.get_mut(&symbol).unwrap();
                        //         println!("[?] TP removing {:?} {:?}", order, trade);
                        //         filled_orders.insert(OrderStatus::Filled(or.clone()));
                        //     }
                        // }
                        // 
                        // OrderType::StopLoss(for_id) => {
                        //     if remove_for_orders.iter().any(|id| *id == for_id) {
                        //         continue;
                        //     }
                        //     if (order.side == Side::Bid
                        //         && (Decimal::from_f64(trade.price)
                        //         .unwrap()
                        //         .gt(&order.price)
                        //         || order
                        //         .price
                        //         .eq(&Decimal::from_f64(trade.price).unwrap())))
                        //         || (order.side == Side::Ask
                        //         && (order
                        //         .price
                        //         .gt(&Decimal::from_f64(trade.price).unwrap())
                        //         || order
                        //         .price
                        //         .eq(&Decimal::from_f64(trade.price).unwrap())))
                        //     {
                        //         remove_orders.push(last.clone());
                        //         filled = true;
                        //         println!("Filling order {:?} {}", order, trade.price);
                        //         let mut position = positions.get_mut(&symbol).unwrap();
                        //         let mut or = order.clone();
                        //         or.quantity = position.qty;
                        //         or.price = Decimal::from_f64(trade.price).unwrap();
                        // 
                        //         if let Some(trade) =
                        //             position.apply_order(&or, trade.timestamp)
                        //         {
                        //             let mut trade_q = trade_q.lock().await;
                        //             trade_q.0.push_back(trade);
                        //             trade_q.1.notify_one();
                        //         }
                        //         println!("[?] SL removing {:?} {:?}", order, trade);
                        // 
                        //         remove_for_orders.push(for_id);
                        // 
                        //         let filled_orders = filled_orders.get_mut(&symbol).unwrap();
                        // 
                        //         filled_orders.insert(OrderStatus::Filled(or.clone()));
                        //     }
                        // }
                        _ => {}
                    }
                    // match last {
                    //     OrderStatus::Pending(order) => {
                    //         match order.order_type {
                    //             OrderType::StopLossLimit
                    //             | OrderType::TakeProfitLimit
                    //             | OrderType::Cancel(_)
                    //             | OrderType::CancelFor(_)
                    //             | OrderType::Unknown => {}
                    //             OrderType::StopLossTrailing(for_id, delta) => {
                    //                 if remove_for_orders.iter().any(|id| *id == for_id) {
                    //                     continue;
                    //                 }
                    // 
                    //                 let trade_price = Decimal::from_f64(trade.price).unwrap();
                    //                 if (order.side == Side::Bid
                    //                     && (trade_price.gt(&order.price)
                    //                         || order.price.eq(&trade_price)))
                    //                     || (order.side == Side::Ask
                    //                         && (order.price.gt(&trade_price)
                    //                             || order.price.eq(&trade_price)))
                    //                 {
                    //                     remove_orders.push(last.clone());
                    // 
                    //                     filled = true;
                    //                     // println!("Filling order {:?} {}", order, trade.price);
                    //                     let mut position = positions.get_mut(&symbol).unwrap();
                    //                     let mut or = order.clone();
                    //                     or.quantity = position.qty;
                    //                     or.price = Decimal::from_f64(trade.price).unwrap();
                    //                     println!("[?] SLT removing {:?} {:?}", or, trade);
                    //                     if let Some(trade) =
                    //                         position.apply_order(&or, trade.timestamp)
                    //                     {
                    //                         let mut trade_q = trade_q.lock().await;
                    //                         trade_q.0.push_back(trade);
                    //                         trade_q.1.notify_one();
                    //                     }
                    // 
                    //                     remove_for_orders.push(for_id);
                    // 
                    //                     let filled_orders = filled_orders.get_mut(&symbol).unwrap();
                    // 
                    //                     filled_orders.insert(OrderStatus::Filled(or.clone()));
                    //                 } else {
                    //                     match order.side {
                    //                         Side::Bid => {
                    //                             if trade_price + delta < order.price {
                    //                                 remove_orders.push(last.clone());
                    // 
                    //                                 let mut or = order.clone();
                    //                                 or.price = trade_price + delta;
                    //                                 // println!("[?] executor > Adjusting price from {} to {} on side BID due to {}", order.price, or.price, trade_price);
                    //                                 re_add.push(OrderStatus::Pending(or));
                    //                             }
                    //                         }
                    //                         Side::Ask => {
                    //                             if trade_price - delta > order.price {
                    //                                 remove_orders.push(last.clone());
                    // 
                    //                                 let mut or = order.clone();
                    //                                 or.price = trade_price - delta;
                    //                                 // println!("[?] executor > Adjusting price from {} to {} on side ASK due to {}", order.price, or.price, trade_price);
                    // 
                    //                                 re_add.push(OrderStatus::Pending(or));
                    //                             }
                    //                         }
                    //                     }
                    //                 }
                    //             }
                    //             OrderType::Market | OrderType::Limit => {
                    //                 if (order.side == Side::Bid
                    //                     && (Decimal::from_f64(trade.price)
                    //                         .unwrap()
                    //                         .lt(&order.price)
                    //                         || order
                    //                             .price
                    //                             .eq(&Decimal::from_f64(trade.price).unwrap())))
                    //                     || (order.side == Side::Ask
                    //                         && (order
                    //                             .price
                    //                             .lt(&Decimal::from_f64(trade.price).unwrap())
                    //                             || order
                    //                                 .price
                    //                                 .eq(&Decimal::from_f64(trade.price).unwrap())))
                    //                 {
                    //                     filled = true;
                    //                     remove_orders.push(last.clone());
                    // 
                    //                     let mut position = positions.get_mut(&symbol).unwrap();
                    //                     let filled_qty = Decimal::from_f64(trade.qty).unwrap();
                    //                     let filled_price = Decimal::from_f64(trade.price).unwrap();
                    //                     let mut filled_order = order.clone();
                    //                     filled_order.quantity = filled_qty.max(order.quantity);
                    //                     filled_order.price = filled_price;
                    //                     filled_order.time = trade.timestamp;
                    // 
                    //                     if let Some(trade) =
                    //                         position.apply_order(&filled_order, trade.timestamp)
                    //                     {
                    //                         let mut trade_q = trade_q.lock().await;
                    //                         trade_q.0.push_back(trade);
                    //                         trade_q.1.notify_one();
                    //                     }
                    //                     if filled_qty < order.quantity {
                    //                         re_add.push(OrderStatus::PartiallyFilled(
                    //                             order.clone(),
                    //                             Decimal::from_f64(trade.qty).unwrap(),
                    //                         ));
                    //                     } else {
                    //                         let filled_orders =
                    //                             filled_orders.get_mut(&symbol).unwrap();
                    // 
                    //                         filled_orders
                    //                             .insert(OrderStatus::Filled(order.clone()));
                    //                     }
                    //                 }
                    //             }
                    // 
                    //             // skip partial fills for target and stop orders, if price reaches one of the targets or stop, the order will be immediately filled
                    //             OrderType::TakeProfit(for_id) => {
                    //                 if remove_for_orders.iter().any(|id| *id == for_id) {
                    //                     continue;
                    //                 }
                    //                 if (order.side == Side::Bid
                    //                     && (Decimal::from_f64(trade.price)
                    //                         .unwrap()
                    //                         .lt(&order.price)
                    //                         || order
                    //                             .price
                    //                             .eq(&Decimal::from_f64(trade.price).unwrap())))
                    //                     || (order.side == Side::Ask
                    //                         && (order
                    //                             .price
                    //                             .lt(&Decimal::from_f64(trade.price).unwrap())
                    //                             || order
                    //                                 .price
                    //                                 .eq(&Decimal::from_f64(trade.price).unwrap())))
                    //                 {
                    //                     filled = true;
                    //                     remove_orders.push(last.clone());
                    // 
                    //                     println!("Filling order {:?} {}", order, trade.price);
                    //                     let mut position = positions.get_mut(&symbol).unwrap();
                    //                     let mut or = order.clone();
                    //                     or.price = Decimal::from_f64(trade.price).unwrap();
                    //                     if let Some(trade) =
                    //                         position.apply_order(&or, trade.timestamp)
                    //                     {
                    //                         let mut trade_q = trade_q.lock().await;
                    //                         trade_q.0.push_back(trade);
                    //                         trade_q.1.notify_one();
                    //                     }
                    //                     println!("[?] TP removing {:?} {:?}", order, trade);
                    //                     remove_for_orders.push(for_id);
                    // 
                    //                     let filled_orders = filled_orders.get_mut(&symbol).unwrap();
                    //                     println!("[?] TP removing {:?} {:?}", order, trade);
                    //                     filled_orders.insert(OrderStatus::Filled(or.clone()));
                    //                 }
                    //             }
                    // 
                    //             OrderType::StopLoss(for_id) => {
                    //                 if remove_for_orders.iter().any(|id| *id == for_id) {
                    //                     continue;
                    //                 }
                    //                 if (order.side == Side::Bid
                    //                     && (Decimal::from_f64(trade.price)
                    //                         .unwrap()
                    //                         .gt(&order.price)
                    //                         || order
                    //                             .price
                    //                             .eq(&Decimal::from_f64(trade.price).unwrap())))
                    //                     || (order.side == Side::Ask
                    //                         && (order
                    //                             .price
                    //                             .gt(&Decimal::from_f64(trade.price).unwrap())
                    //                             || order
                    //                                 .price
                    //                                 .eq(&Decimal::from_f64(trade.price).unwrap())))
                    //                 {
                    //                     remove_orders.push(last.clone());
                    //                     filled = true;
                    //                     println!("Filling order {:?} {}", order, trade.price);
                    //                     let mut position = positions.get_mut(&symbol).unwrap();
                    //                     let mut or = order.clone();
                    //                     or.quantity = position.qty;
                    //                     or.price = Decimal::from_f64(trade.price).unwrap();
                    // 
                    //                     if let Some(trade) =
                    //                         position.apply_order(&or, trade.timestamp)
                    //                     {
                    //                         let mut trade_q = trade_q.lock().await;
                    //                         trade_q.0.push_back(trade);
                    //                         trade_q.1.notify_one();
                    //                     }
                    //                     println!("[?] SL removing {:?} {:?}", order, trade);
                    // 
                    //                     remove_for_orders.push(for_id);
                    // 
                    //                     let filled_orders = filled_orders.get_mut(&symbol).unwrap();
                    // 
                    //                     filled_orders.insert(OrderStatus::Filled(or.clone()));
                    //                 }
                    //             }
                    //         }
                    //     }
                    //     OrderStatus::PartiallyFilled(order, filled_qty_so_far) => {
                    //         if (order.side == Side::Bid
                    //             && order.price.gt(&Decimal::from_f64(trade.price).unwrap()))
                    //             || (order.side == Side::Ask
                    //                 && order.price.lt(&Decimal::from_f64(trade.price).unwrap()))
                    //         {
                    //             remove_orders.push(last.clone());
                    //             filled = true;
                    //             let mut position = positions.get_mut(&symbol).unwrap();
                    //             let filled_qty = Decimal::from_f64(trade.qty).unwrap();
                    //             let filled_price = Decimal::from_f64(trade.price).unwrap();
                    // 
                    //             let mut filled_order = order.clone();
                    //             filled_order.quantity =
                    //                 filled_qty.max(order.quantity - filled_qty_so_far);
                    //             filled_order.price = filled_price;
                    //             filled_order.time = trade.timestamp;
                    // 
                    //             if let Some(trade) =
                    //                 position.apply_order(&filled_order, trade.timestamp)
                    //             {
                    //                 let mut trade_q = trade_q.lock().await;
                    //                 trade_q.0.push_back(trade);
                    //                 trade_q.1.notify_one();
                    //             }
                    //             if filled_qty < order.quantity {
                    //                 re_add.push(OrderStatus::PartiallyFilled(
                    //                     order.clone(),
                    //                     filled_qty + filled_qty_so_far,
                    //                 ));
                    //             } else {
                    //                 let filled_orders = filled_orders.get_mut(&symbol).unwrap();
                    // 
                    //                 filled_orders.insert(OrderStatus::Filled(order.clone()));
                    //             }
                    //         }
                    //     }
                    //     OrderStatus::Filled(order) => {
                    //         remove_orders.push(last.clone());
                    //         let mut position = positions.get_mut(&symbol).unwrap();
                    //         let filled_price = Decimal::from_f64(trade.price).unwrap();
                    // 
                    //         let mut filled_order = order.clone();
                    // 
                    //         filled_order.price = filled_price;
                    //         filled_order.time = trade.timestamp;
                    //         if let Some(trade) =
                    //             position.apply_order(&filled_order, trade.timestamp)
                    //         {
                    //             let mut trade_q = trade_q.lock().await;
                    //             trade_q.0.push_back(trade);
                    //             trade_q.1.notify_one();
                    //         }
                    // 
                    //         // if position is neutral, remove all stop and take profit orders
                    //         if !position.is_open() {
                    //             let to_keep_ids: Vec<Uuid> = open_orders
                    //                 .iter()
                    //                 .filter_map(|o| match &*o {
                    //                     OrderStatus::Filled(or) | OrderStatus::Pending(or) => {
                    //                         match or.order_type {
                    //                             OrderType::Market | OrderType::Limit => {
                    //                                 if order.id != or.id {
                    //                                     Some(or.id)
                    //                                 } else {
                    //                                     None
                    //                                 }
                    //                             }
                    //                             _ => None,
                    //                         }
                    //                     }
                    //                     _ => None,
                    //                 })
                    //                 .collect();
                    //             open_orders.iter().for_each(|o| {
                    //                 // println!("to keep {:?} {:?}", to_keep_ids, open_orders);
                    //                 match &*o {
                    //                     OrderStatus::Pending(or)
                    //                     | OrderStatus::PartiallyFilled(or, _) => {
                    //                         match or.order_type {
                    //                             OrderType::TakeProfit(for_id)
                    //                             | OrderType::StopLoss(for_id)
                    //                             | OrderType::StopLossTrailing(for_id, _) => {
                    //                                 if !to_keep_ids.iter().any(|id| *id == for_id) {
                    //                                     remove_for_orders.push(for_id);
                    //                                     remove_orders.push(last.clone());
                    //                                 }
                    //                             }
                    //                             _ => {}
                    //                         }
                    //                     }
                    //                     _ => {}
                    //                 }
                    //             });
                    //         }
                    //         let filled_orders = filled_orders.get_mut(&symbol).unwrap();
                    //         let mongo_client = MongoClient::new().await;
                    //         let _ = mongo_client
                    //             .orders
                    //             .insert_one(filled_order.clone(), None)
                    //             .await;
                    //         filled_orders.insert(OrderStatus::Filled(filled_order.clone()));
                    //     }
                    // 
                    //     // removes all orders that are sent in Cancel(id)
                    //     // including target and stop order with for_id
                    //     // to only cancel target or stop orders, use their associated id
                    //     // canceling an order that has stop or target orders will cancel all of them
                    //     OrderStatus::Canceled(order, reason) => {
                    //         remove_orders.push(last.clone());
                    // 
                    //         match order.order_type {
                    //             OrderType::Cancel(_id) => {}
                    //             OrderType::CancelFor(for_id) => {
                    //                 remove_for_orders.push(for_id);
                    //             }
                    //             _ => re_add
                    //                 .push(OrderStatus::Canceled(order.clone(), reason.clone())),
                    //         }
                    //     }
                    // }
                }

                // for id in remove_orders.clone() {
                //     open_orders.remove(&id);
                // }
                // 
                // let for_orders = open_orders
                //     .iter()
                //     .filter_map(|or| match &*or {
                //         OrderStatus::Canceled(order, ..)
                //         | OrderStatus::PartiallyFilled(order, _)
                //         | OrderStatus::Filled(order)
                //         | OrderStatus::Pending(order) => {
                //             if remove_for_orders.contains(&order.id) {
                //                 Some(or.clone())
                //             } else {
                //                 None
                //             }
                //         }
                //     })
                //     .collect::<Vec<_>>();
                // for id in for_orders {
                //     open_orders.remove(&id);
                // }
                // for order in &re_add {
                //     open_orders.insert(order.clone());
                // }

                // println!("[?] Open orders: {}", open_orders_len);
                // println!("re_add: {}", re_add.len());
                // println!("remove_ids: {}", remove_ids.len());
                // assert_eq!(open_orders_len, ((re_add.len() as i64 - remove_ids.len() as i64).max(0) as usize));
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
                trace!("adding new order {:?} for partially filled order: {:?}", new_order, order);
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
            }
            // log reason and remove 
            OrderStatus::Canceled(order, reason) => {
                trace!("Removing canceled order with reason `{}` from order set: {:?}", reason, order);
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
                Arc::new(RwLock::new(order_statuses_channel.0.clone()))
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
                Arc::new(RwLock::new(order_statuses_channel.0.clone()))
                
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
                Arc::new(RwLock::new(order_statuses_channel.0.clone()))
                
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
            id: 1,
            price: 9.0,
            qty: 100.0,
            timestamp: 0,
            delta: 0.0,
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
                    min_price_trade: entry.clone(),
                    max_price_trade: entry
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
                Arc::new(RwLock::new(order_statuses_channel.0.clone()))
                
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
            id: 1,
            price: 10.0,
            qty: 90.0,
            timestamp: 0,
            delta: 0.0,
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
                    min_price_trade: entry.clone(),
                    max_price_trade: entry
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
            Arc::new(RwLock::new(order_statuses_channel.0.clone()))
            
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
            id: 1,
            price: 10.0,
            qty: 90.0,
            timestamp: 0,
            delta: 0.0,
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
                    min_price_trade: entry.clone(),
                    max_price_trade: entry
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
