use crate::events::{EventEmitter, EventSink};
use crate::executors::{ExchangeAccount, ExchangeAccountInfo, Position, Spread, TradeExecutor};
use crate::mongodb::MongoClient;
use crate::types::{
    ExchangeId, Order, OrderStatus, OrderType, Side, Symbol, SymbolAccount, TfTrades, Trade,
};
use async_broadcast::{Receiver, Sender};
use async_trait::async_trait;
use rust_decimal::prelude::FromPrimitive;
use rust_decimal::Decimal;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use tokio::sync::{Notify, RwLock};
use tokio::task::JoinHandle;
use uuid::Uuid;

type ArcMap<K, V> = Arc<RwLock<HashMap<K, V>>>;
type ArcSet<T> = Arc<RwLock<HashSet<T>>>;

#[derive(Clone)]
pub struct SimulatedAccount {
    pub symbol_accounts: ArcMap<Symbol, SymbolAccount>,
    pub open_orders: ArcMap<Symbol, ArcSet<OrderStatus>>,
    pub order_history: ArcMap<Symbol, ArcSet<OrderStatus>>,
    pub trade_history: ArcMap<Symbol, ArcSet<Trade>>,
    trade_q: Arc<RwLock<VecDeque<Trade>>>,
    pub trade_subscribers: Arc<RwLock<Sender<(Trade, Option<Arc<Notify>>)>>>,
    pub positions: ArcMap<Symbol, Arc<RwLock<Position>>>,
    tf_trades: Arc<RwLock<Receiver<(TfTrades, Option<Arc<Notify>>)>>>,
    order_statuses: Arc<RwLock<Receiver<(OrderStatus, Option<Arc<Notify>>)>>>,
    tf_trades_working: Arc<std::sync::RwLock<bool>>,
    order_statuses_working: Arc<std::sync::RwLock<bool>>,
    spreads: ArcMap<Symbol, Arc<RwLock<Spread>>>,
}

#[derive(Clone)]
pub struct SimulatedExecutor {
    pub account: Arc<SimulatedAccount>,
    pub orders: Arc<RwLock<Receiver<(Order, Option<Arc<Notify>>)>>>,
    order_status_q: Arc<RwLock<VecDeque<OrderStatus>>>,
    pub order_status_subscribers: Arc<RwLock<Sender<(OrderStatus, Option<Arc<Notify>>)>>>,
    order_working: Arc<std::sync::RwLock<bool>>,
}
impl SimulatedAccount {
    pub async fn new(
        tf_trades: Receiver<(TfTrades, Option<Arc<Notify>>)>,
        order_statuses: Receiver<(OrderStatus, Option<Arc<Notify>>)>,
        symbols: Vec<Symbol>,
    ) -> Self {
        let symbol_accounts = Arc::new(RwLock::new(HashMap::new()));
        let open_orders = Arc::new(RwLock::new(HashMap::new()));
        let order_history = Arc::new(RwLock::new(HashMap::new()));
        let trade_history = Arc::new(RwLock::new(HashMap::new()));
        let positions = Arc::new(RwLock::new(HashMap::new()));
        let spreads = Arc::new(RwLock::new(HashMap::new()));

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
            symbol_accounts
                .write()
                .await
                .insert(symbol.clone(), symbol_account);
            open_orders
                .write()
                .await
                .insert(symbol.clone(), Arc::new(RwLock::new(HashSet::new())));
            order_history
                .write()
                .await
                .insert(symbol.clone(), Arc::new(RwLock::new(HashSet::new())));
            trade_history
                .write()
                .await
                .insert(symbol.clone(), Arc::new(RwLock::new(HashSet::new())));
            positions
                .write()
                .await
                .insert(symbol.clone(), Arc::new(RwLock::new(position)));
            spreads
                .write()
                .await
                .insert(symbol.clone(), Arc::new(RwLock::new(spread)));
        }
        Self {
            symbol_accounts,
            open_orders,
            order_history,
            trade_history,
            positions,
            spreads,
            tf_trades: Arc::new(RwLock::new(tf_trades)),
            trade_q: Arc::new(RwLock::new(VecDeque::new())),
            trade_subscribers: Arc::new(RwLock::new(async_broadcast::broadcast(1).0)),
            order_statuses: Arc::new(RwLock::new(order_statuses)),
            tf_trades_working: Arc::new(std::sync::RwLock::new(false)),
            order_statuses_working: Arc::new(std::sync::RwLock::new(false)),
        }
    }
}

impl SimulatedExecutor {
    pub async fn new(
        orders_rx: Receiver<(Order, Option<Arc<Notify>>)>,
        trades_rx: Receiver<(TfTrades, Option<Arc<Notify>>)>,
        symbols: Vec<Symbol>,
        trades: Sender<(Trade, Option<Arc<Notify>>)>,
    ) -> Self {
        let order_statuses_channel = async_broadcast::broadcast(100);
        let mut account = SimulatedAccount::new(trades_rx, order_statuses_channel.1, symbols).await;
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
            orders: Arc::new(RwLock::new(orders_rx)),
            order_status_q: Arc::new(RwLock::new(VecDeque::new())),
            order_status_subscribers: Arc::new(RwLock::new(order_statuses_channel.0)),
            order_working: Arc::new(std::sync::RwLock::new(false)),
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
            loop {
                let mut w = trade_q.write().await;
                let trade = w.pop_front();
                std::mem::drop(w);
                if let Some(trade) = trade {
                    mongo_client
                        .past_trades
                        .insert_one(trade.clone(), None)
                        .await
                        .unwrap();
                    let thw = trade_history.read().await;
                    let mut th = thw.get(&trade.symbol).unwrap().write().await;
                    th.insert(trade.clone());
                    drop(th);
                    drop(thw);
                    let subs = subscribers.read().await;
                    subs.broadcast((trade.clone(), None)).await.unwrap();
                }
            }
        }))
    }
}

impl EventSink<TfTrades> for SimulatedAccount {
    fn get_receiver(&self) -> Arc<RwLock<Receiver<(TfTrades, Option<Arc<Notify>>)>>> {
        self.tf_trades.clone()
    }
    fn handle_event(&self, event_msg: TfTrades) -> anyhow::Result<JoinHandle<anyhow::Result<()>>> {
        if event_msg.is_empty() {
            return Ok(tokio::spawn(async move { Ok(()) }));
        }
        let open_orders = self.open_orders.clone();

        let trade_q = self.trade_q.clone();
        let filled_orders = self.order_history.clone();
        let positions = self.positions.clone();
        let spreads = self.spreads.clone();

        Ok(tokio::spawn(async move {
            // update spread first with the last trade
            let spreads = spreads.read().await;
            if let Some(last_trade) = event_msg.last() {
                if let Some(last) = last_trade.trades.last() {
                    let mut spread = spreads.get(&last_trade.symbol).unwrap().write().await;
                    spread.update(Decimal::from_f64(last.price).unwrap(), last_trade.timestamp);
                }
            }

            // if any open orders are fillable move them to filled orders and update position and push a trade event to trade queue if it is a order opposite to position
            for tf_trade in event_msg {
                let symbol = tf_trade.symbol.clone();
                let open_orders = open_orders.read().await.get(&symbol).unwrap().clone();
                let all_filled_orders = filled_orders.read().await;
                let all_positions = positions.read().await;
                for trade in tf_trade.trades {
                    let mut remove_ids = vec![];
                    let mut remove_for_ids = vec![];
                    let mut re_add = vec![];
                    let o = open_orders.read().await;
                    let i_open_orders = o.clone();
                    drop(o);
                    if i_open_orders.is_empty() {
                        continue;
                    }

                    let mut filled = false;
                    let iter_orders = i_open_orders.iter();
                    for last in iter_orders {
                        if filled {
                            break;
                        }
                        match last {
                            OrderStatus::Pending(order) => {
                                match order.order_type {
                                    OrderType::StopLossLimit
                                    | OrderType::TakeProfitLimit
                                    | OrderType::Cancel(_)
                                    | OrderType::CancelFor(_)
                                    | OrderType::Unknown => {}
                                    OrderType::StopLossTrailing(for_id, delta) => {
                                        if remove_for_ids.iter().any(|id| *id == for_id) {
                                            continue;
                                        }

                                        let trade_price = Decimal::from_f64(trade.price).unwrap();
                                        if (order.side == Side::Bid
                                            && (trade_price.gt(&order.price)
                                                || order.price.eq(&trade_price)))
                                            || (order.side == Side::Ask
                                                && (order.price.gt(&trade_price)
                                                    || order.price.eq(&trade_price)))
                                        {
                                            remove_ids.push(order.id);

                                            filled = true;
                                            // println!("Filling order {:?} {}", order, trade.price);
                                            let position = all_positions.get(&symbol).unwrap();
                                            let mut w = position.write().await;
                                            let mut or = order.clone();
                                            or.quantity = w.qty;
                                            or.price = Decimal::from_f64(trade.price).unwrap();
                                            if let Some(trade) = w.apply_order(&or, trade.timestamp)
                                            {
                                                let mut trade_q = trade_q.write().await;
                                                trade_q.push_back(trade);
                                            }
                                            println!(
                                                "[?] SLT removing {:?} {}",
                                                for_id,
                                                i_open_orders.len()
                                            );

                                            remove_for_ids.push(for_id);

                                            let mut filled_orders = all_filled_orders
                                                .get(&symbol)
                                                .unwrap()
                                                .write()
                                                .await;

                                            filled_orders.insert(OrderStatus::Filled(or.clone()));
                                        } else {
                                            match order.side {
                                                Side::Bid => {
                                                    if trade_price + delta < order.price {
                                                        remove_ids.push(order.id);

                                                        let mut or = order.clone();
                                                        or.price = trade_price + delta;
                                                        // println!("[?] executor > Adjusting price from {} to {} on side BID due to {}", order.price, or.price, trade_price);
                                                        re_add.push(OrderStatus::Pending(or));
                                                    }
                                                }
                                                Side::Ask => {
                                                    if trade_price - delta > order.price {
                                                        remove_ids.push(order.id);

                                                        let mut or = order.clone();
                                                        or.price = trade_price - delta;
                                                        // println!("[?] executor > Adjusting price from {} to {} on side ASK due to {}", order.price, or.price, trade_price);

                                                        re_add.push(OrderStatus::Pending(or));
                                                    }
                                                }
                                            }
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
                                                        .eq(&Decimal::from_f64(trade.price)
                                                            .unwrap())))
                                        {
                                            filled = true;
                                            remove_ids.push(order.id);

                                            let position = all_positions.get(&symbol).unwrap();
                                            let mut w = position.write().await;
                                            let filled_qty = Decimal::from_f64(trade.qty).unwrap();
                                            let filled_price =
                                                Decimal::from_f64(trade.price).unwrap();
                                            let mut filled_order = order.clone();
                                            filled_order.quantity = filled_qty.max(order.quantity);
                                            filled_order.price = filled_price;
                                            filled_order.time = trade.timestamp;

                                            if let Some(trade) =
                                                w.apply_order(&filled_order, trade.timestamp)
                                            {
                                                let mut trade_q = trade_q.write().await;
                                                trade_q.push_back(trade);
                                            }
                                            if filled_qty < order.quantity {
                                                re_add.push(OrderStatus::PartiallyFilled(
                                                    order.clone(),
                                                    Decimal::from_f64(trade.qty).unwrap(),
                                                ));
                                            } else {
                                                let mut filled_orders = all_filled_orders
                                                    .get(&symbol)
                                                    .unwrap()
                                                    .write()
                                                    .await;

                                                filled_orders
                                                    .insert(OrderStatus::Filled(order.clone()));
                                            }
                                        }
                                    }

                                    // skip partial fills for target and stop orders, if price reaches one of the targets or stop, the order will be immediately filled
                                    OrderType::TakeProfit(for_id) => {
                                        if remove_for_ids.iter().any(|id| *id == for_id) {
                                            continue;
                                        }
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
                                                        .eq(&Decimal::from_f64(trade.price)
                                                            .unwrap())))
                                        {
                                            filled = true;
                                            remove_ids.push(order.id);

                                            // println!("Filling order {:?} {}", order, trade.price);
                                            let position = all_positions.get(&symbol).unwrap();
                                            let mut w = position.write().await;
                                            let mut or = order.clone();
                                            or.price = Decimal::from_f64(trade.price).unwrap();
                                            if let Some(trade) = w.apply_order(&or, trade.timestamp)
                                            {
                                                let mut trade_q = trade_q.write().await;
                                                trade_q.push_back(trade);
                                            }
                                            println!(
                                                "[?] TP removing {:?} {}",
                                                for_id,
                                                i_open_orders.len()
                                            );
                                            remove_for_ids.push(for_id);

                                            let mut filled_orders = all_filled_orders
                                                .get(&symbol)
                                                .unwrap()
                                                .write()
                                                .await;

                                            filled_orders.insert(OrderStatus::Filled(or.clone()));
                                        }
                                    }

                                    OrderType::StopLoss(for_id) => {
                                        if remove_for_ids.iter().any(|id| *id == for_id) {
                                            continue;
                                        }
                                        if (order.side == Side::Bid
                                            && (Decimal::from_f64(trade.price)
                                                .unwrap()
                                                .gt(&order.price)
                                                || order
                                                    .price
                                                    .eq(&Decimal::from_f64(trade.price).unwrap())))
                                            || (order.side == Side::Ask
                                                && (order
                                                    .price
                                                    .gt(&Decimal::from_f64(trade.price).unwrap())
                                                    || order
                                                        .price
                                                        .eq(&Decimal::from_f64(trade.price)
                                                            .unwrap())))
                                        {
                                            remove_ids.push(order.id);
                                            filled = true;
                                            // println!("Filling order {:?} {}", order, trade.price);
                                            let position = all_positions.get(&symbol).unwrap();
                                            let mut w = position.write().await;
                                            let mut or = order.clone();
                                            or.quantity = w.qty;
                                            or.price = Decimal::from_f64(trade.price).unwrap();

                                            if let Some(trade) = w.apply_order(&or, trade.timestamp)
                                            {
                                                let mut trade_q = trade_q.write().await;
                                                trade_q.push_back(trade);
                                            }
                                            println!(
                                                "[?] SL removing {:?} {}",
                                                for_id,
                                                i_open_orders.len()
                                            );

                                            remove_for_ids.push(for_id);

                                            let mut filled_orders = all_filled_orders
                                                .get(&symbol)
                                                .unwrap()
                                                .write()
                                                .await;

                                            filled_orders.insert(OrderStatus::Filled(or.clone()));
                                        }
                                    }
                                }
                            }
                            OrderStatus::PartiallyFilled(order, filled_qty_so_far) => {
                                if (order.side == Side::Bid
                                    && order.price.gt(&Decimal::from_f64(trade.price).unwrap()))
                                    || (order.side == Side::Ask
                                        && order.price.lt(&Decimal::from_f64(trade.price).unwrap()))
                                {
                                    remove_ids.push(order.id);
                                    filled = true;
                                    let position = all_positions.get(&symbol).unwrap();
                                    let mut w = position.write().await;
                                    let filled_qty = Decimal::from_f64(trade.qty).unwrap();
                                    let filled_price = Decimal::from_f64(trade.price).unwrap();

                                    let mut filled_order = order.clone();
                                    filled_order.quantity =
                                        filled_qty.max(order.quantity - filled_qty_so_far);
                                    filled_order.price = filled_price;
                                    filled_order.time = trade.timestamp;

                                    if let Some(trade) =
                                        w.apply_order(&filled_order, trade.timestamp)
                                    {
                                        let mut trade_q = trade_q.write().await;
                                        trade_q.push_back(trade);
                                    }
                                    if filled_qty < order.quantity {
                                        re_add.push(OrderStatus::PartiallyFilled(
                                            order.clone(),
                                            filled_qty + filled_qty_so_far,
                                        ));
                                    } else {
                                        let mut filled_orders =
                                            all_filled_orders.get(&symbol).unwrap().write().await;

                                        filled_orders.insert(OrderStatus::Filled(order.clone()));
                                    }
                                }
                            }
                            OrderStatus::Filled(order) => {
                                remove_ids.push(order.id);
                                let position = all_positions.get(&symbol).unwrap();
                                let mut w = position.write().await;
                                let filled_price = Decimal::from_f64(trade.price).unwrap();

                                let mut filled_order = order.clone();

                                filled_order.price = filled_price;
                                filled_order.time = trade.timestamp;
                                if let Some(trade) = w.apply_order(&filled_order, trade.timestamp) {
                                    let mut trade_q = trade_q.write().await;
                                    trade_q.push_back(trade);
                                }

                                // if position is neutral, remove all stop and take profit orders
                                if !w.is_open() {
                                    let open_orders = open_orders.read().await;
                                    let to_keep_ids: Vec<Uuid> = open_orders
                                        .iter()
                                        .filter_map(|o| match o {
                                            OrderStatus::Filled(or) | OrderStatus::Pending(or) => {
                                                match or.order_type {
                                                    OrderType::Market | OrderType::Limit => {
                                                        if order.id != or.id {
                                                            Some(or.id)
                                                        } else {
                                                            None
                                                        }
                                                    }
                                                    _ => None,
                                                }
                                            }
                                            _ => None,
                                        })
                                        .collect();
                                    open_orders.iter().for_each(|o| {
                                        println!("to keep {:?} {:?}", to_keep_ids, open_orders);
                                        match o {
                                            OrderStatus::Pending(or)
                                            | OrderStatus::PartiallyFilled(or, _) => {
                                                match or.order_type {
                                                    OrderType::TakeProfit(for_id)
                                                    | OrderType::StopLoss(for_id)
                                                    | OrderType::StopLossTrailing(for_id, _) => {
                                                        if !to_keep_ids
                                                            .iter()
                                                            .any(|id| *id == for_id)
                                                        {
                                                            remove_for_ids.push(for_id);
                                                            remove_ids.push(or.id);
                                                        }
                                                    }
                                                    _ => {}
                                                }
                                            }
                                            _ => {}
                                        }
                                    });
                                }
                                let mut filled_orders =
                                    all_filled_orders.get(&symbol).unwrap().write().await;
                                let mongo_client = MongoClient::new().await;
                                let _ = mongo_client
                                    .orders
                                    .insert_one(filled_order.clone(), None)
                                    .await;
                                filled_orders.insert(OrderStatus::Filled(filled_order.clone()));
                            }

                            // removes all orders that are sent in Cancel(id)
                            // including target and stop order with for_id
                            // to only cancel target or stop orders, use their associated id
                            // canceling an order that has stop or target orders will cancel all of them
                            OrderStatus::Canceled(order, reason) => {
                                remove_ids.push(order.id);

                                match order.order_type {
                                    OrderType::Cancel(id) => {
                                        remove_ids.push(id);
                                    }
                                    OrderType::CancelFor(for_id) => {
                                        remove_for_ids.push(for_id);
                                    }
                                    _ => re_add
                                        .push(OrderStatus::Canceled(order.clone(), reason.clone())),
                                }
                            }
                        }
                    }

                    for id in remove_ids.clone() {
                        let r = open_orders.read().await.clone();
                        let iter = r.into_iter();
                        for o in iter {
                            let or = o.clone();

                            match o {
                                OrderStatus::Pending(order)
                                | OrderStatus::Filled(order)
                                | OrderStatus::PartiallyFilled(order, _) => {
                                    if order.id == id {
                                        println!(
                                            "[?] Removing {} order {:?} {}",
                                            order.symbol.symbol, order.order_type, order.id
                                        );

                                        let mut open_orders = open_orders.write().await;
                                        open_orders.remove(&or.clone());
                                    }
                                }
                                OrderStatus::Canceled(order, _) => {
                                    if let OrderType::Cancel(i) = order.order_type {
                                        if i == id {
                                            // println!("[?] Removing {} order {:?}", order.symbol.symbol, order.order_type );

                                            let mut open_orders = open_orders.write().await;
                                            open_orders.remove(&or.clone());
                                        }
                                    }
                                }
                            }
                        }
                    }
                    for id in remove_for_ids.clone() {
                        let r = open_orders.read().await.clone();
                        println!("[?] Removing for id {} for {}", id, r.len());
                        let iter = r.into_iter();
                        for o in iter {
                            let or = o.clone();
                            match o {
                                OrderStatus::Pending(order) => match order.order_type {
                                    OrderType::TakeProfit(for_id)
                                    | OrderType::StopLoss(for_id)
                                    | OrderType::StopLossTrailing(for_id, _) => {
                                        if for_id == id {
                                            println!(
                                                "[?] Removing {} for order {:?}",
                                                order.symbol.symbol, order.order_type
                                            );
                                            let mut open_orders = open_orders.write().await;
                                            open_orders.remove(&or.clone());
                                        }
                                    }

                                    _ => {}
                                },
                                OrderStatus::Canceled(order, _) => {
                                    if let OrderType::CancelFor(i) = order.order_type {
                                        if i == id {
                                            println!(
                                                "[?] Removing {} for order {:?}",
                                                order.symbol.symbol, order.order_type
                                            );
                                            let mut open_orders = open_orders.write().await;
                                            open_orders.remove(&or.clone());
                                        }
                                    }
                                }
                                _ => {}
                            }
                        }
                    }
                    for order in &re_add {
                        open_orders.write().await.insert(order.clone());
                    }

                    // println!("[?] Open orders: {}", open_orders_len);
                    // println!("re_add: {}", re_add.len());
                    // println!("remove_ids: {}", remove_ids.len());
                    // assert_eq!(open_orders_len, ((re_add.len() as i64 - remove_ids.len() as i64).max(0) as usize));
                }
            }
            Ok(())
        }))
    }
    fn working(&self) -> bool {
        *self.tf_trades_working.read().unwrap()
    }
    fn set_working(&self, working: bool) -> anyhow::Result<()> {
        *self.tf_trades_working.write().unwrap() = working;
        Ok(())
    }
}

impl EventSink<OrderStatus> for SimulatedAccount {
    fn get_receiver(&self) -> Arc<RwLock<Receiver<(OrderStatus, Option<Arc<Notify>>)>>> {
        self.order_statuses.clone()
    }
    fn handle_event(
        &self,
        event_msg: OrderStatus,
    ) -> anyhow::Result<JoinHandle<anyhow::Result<()>>> {
        let open_orders = self.open_orders.clone();
        Ok(tokio::spawn(async move {
            match &event_msg {
                OrderStatus::Pending(order)
                | OrderStatus::PartiallyFilled(order, _)
                | OrderStatus::Filled(order)
                | OrderStatus::Canceled(order, _) => {
                    if open_orders.read().await.get(&order.symbol).is_none() {
                        open_orders
                            .write()
                            .await
                            .insert(order.symbol.clone(), Arc::new(RwLock::new(HashSet::new())));
                    }

                    open_orders
                        .read()
                        .await
                        .get(&order.symbol)
                        .unwrap()
                        .write()
                        .await
                        .insert(event_msg);
                }
            }
            Ok(())
        }))
    }
    fn working(&self) -> bool {
        *self.order_statuses_working.read().unwrap()
    }
    fn set_working(&self, working: bool) -> anyhow::Result<()> {
        *self.order_statuses_working.write().unwrap() = working;
        Ok(())
    }
}

impl EventSink<Order> for SimulatedExecutor {
    fn get_receiver(&self) -> Arc<RwLock<Receiver<(Order, Option<Arc<Notify>>)>>> {
        self.orders.clone()
    }
    fn handle_event(&self, event_msg: Order) -> anyhow::Result<JoinHandle<anyhow::Result<()>>> {
        let fut = self.process_order(event_msg);
        let order_status_q = self.order_status_q.clone();
        Ok(tokio::spawn(async move {
            if let Ok(res) = fut {
                order_status_q.write().await.push_back(res);
            }
            Ok(())
        }))
    }
    fn working(&self) -> bool {
        *self.order_working.read().unwrap()
    }
    fn set_working(&self, working: bool) -> anyhow::Result<()> {
        *self.order_working.write().unwrap() = working;
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

    async fn get_open_orders(&self, symbol: &Symbol) -> Arc<HashSet<OrderStatus>> {
        Arc::new(
            self.open_orders
                .read()
                .await
                .get(symbol)
                .unwrap()
                .read()
                .await
                .clone(),
        )
    }

    async fn get_symbol_account(&self, symbol: &Symbol) -> SymbolAccount {
        self.symbol_accounts
            .read()
            .await
            .get(symbol)
            .unwrap()
            .clone()
    }

    async fn get_past_trades(&self, symbol: &Symbol, length: Option<usize>) -> Arc<HashSet<Trade>> {
        if let Some(length) = length {
            let all_trades = self.trade_history.read().await;
            let guard = all_trades.get(symbol).unwrap().read().await;
            let mut trades_vec = guard.iter().collect::<Vec<&Trade>>();
            trades_vec.sort_by(|a, b| b.time.cmp(&a.time));
            trades_vec.truncate(length);
            let trades = trades_vec.into_iter().cloned().collect::<HashSet<Trade>>();
            Arc::new(trades)
        } else {
            Arc::new(
                self.trade_history
                    .read()
                    .await
                    .get(symbol)
                    .unwrap()
                    .read()
                    .await
                    .clone(),
            )
        }
    }

    async fn get_position(&self, symbol: &Symbol) -> Arc<Position> {
        let positions = self.positions.read().await;
        let position_lock = positions.get(symbol).unwrap();
        let position = position_lock.read().await.clone();
        Arc::new(position)
    }

    async fn get_spread(&self, symbol: &Symbol) -> Arc<Spread> {
        let spreads = self.spreads.read().await;
        let spread_lock = spreads.get(symbol).unwrap();
        let spread = spread_lock.read().await.clone();
        Arc::new(spread)
    }

    async fn get_order(&self, symbol: &Symbol, id: &Uuid) -> Vec<Order> {
        let order_history = self.order_history.read().await.get(symbol).unwrap().clone();
        let res = order_history
            .read()
            .await
            .iter()
            .filter_map(|o| match o {
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
    async fn test_simulated_account_add_pending_order() -> anyhow::Result<()> {
        let tf_trades_channel = async_broadcast::broadcast(100);
        let order_statuses_channel = async_broadcast::broadcast(100);

        let symbol = Symbol {
            symbol: "TST/USDT".to_string(),
            exchange: ExchangeId::Simulated,
            base_asset_precision: 1,
            quote_asset_precision: 2,
        };
        let simulated_account = SimulatedAccount::new(
            tf_trades_channel.1,
            order_statuses_channel.1,
            vec![symbol.clone()],
        )
        .await;
        let s_a = simulated_account.clone();
        std::thread::spawn(move || {
            EventSink::<OrderStatus>::listen(&simulated_account).unwrap();
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
        order_statuses_channel
            .0
            .broadcast(os.clone())
            .await
            .unwrap();
        while !order_statuses_channel.0.is_empty() {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }

        let open_orders = s_a.get_open_orders(&symbol).await;
        assert_eq!(open_orders.len(), 1);
        assert!(open_orders.contains(&os));
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
        let simulated_account = SimulatedAccount::new(
            tf_trades_channel.1,
            order_statuses_channel.1,
            vec![symbol.clone()],
        )
        .await;
        let s_a1 = simulated_account.clone();
        let s_a = simulated_account.clone();
        std::thread::spawn(move || {
            EventSink::<OrderStatus>::listen(&simulated_account).unwrap();
        });
        std::thread::spawn(move || {
            EventSink::<TfTrades>::listen(&s_a1).unwrap();
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
        order_statuses_channel
            .0
            .broadcast(os.clone())
            .await
            .unwrap();
        while !order_statuses_channel.0.is_empty() {
            tokio::time::sleep(std::time::Duration::from_micros(100)).await;
        }

        tf_trades_channel
            .0
            .broadcast(vec![TfTrade {
                symbol: symbol.clone(),
                tf: 1,
                id: 1,
                timestamp: 124,
                trades: vec![TradeEntry {
                    id: 1,
                    price: 9.0,
                    qty: 100.0,
                    timestamp: 0,
                    delta: 0.0,
                    symbol: symbol.clone(),
                }],
            }])
            .await
            .unwrap();
        while !tf_trades_channel.0.is_empty() {
            tokio::time::sleep(std::time::Duration::from_micros(100)).await;
        }

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
        let simulated_account = SimulatedAccount::new(
            tf_trades_channel.1,
            order_statuses_channel.1,
            vec![symbol.clone()],
        )
        .await;
        let s_a1 = simulated_account.clone();
        let s_a = simulated_account.clone();
        std::thread::spawn(move || {
            EventSink::<OrderStatus>::listen(&simulated_account).unwrap();
        });
        std::thread::spawn(move || {
            EventSink::<TfTrades>::listen(&s_a1).unwrap();
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
            .broadcast(os.clone())
            .await
            .unwrap();
        while !order_statuses_channel.0.is_empty() {
            tokio::time::sleep(std::time::Duration::from_micros(100)).await;
        }

        tf_trades_channel
            .0
            .broadcast(vec![TfTrade {
                symbol: symbol.clone(),
                tf: 1,
                id: 1,
                timestamp: 124,
                trades: vec![TradeEntry {
                    id: 1,
                    price: 10.0,
                    qty: 90.0,
                    timestamp: 0,
                    delta: 0.0,
                    symbol: symbol.clone(),
                }],
            }])
            .await
            .unwrap();
        while !tf_trades_channel.0.is_empty() {
            tokio::time::sleep(std::time::Duration::from_micros(100)).await;
        }

        let open_orders = s_a.get_open_orders(&symbol).await;
        assert_eq!(open_orders.len(), 1);
        match open_orders.iter().next().unwrap() {
            OrderStatus::PartiallyFilled(order, filled_qty) => {
                assert_eq!(filled_qty, &Decimal::new(90, 0));
            }
            _ => panic!("Expected PartiallyFilled"),
        }
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
            tf_trades_channel.1,
            order_statuses_channel.1,
            vec![symbol.clone()],
        )
        .await;
        simulated_account.subscribe(trades_sender).await;
        let s_a1 = simulated_account.clone();
        let s_a = simulated_account.clone();
        std::thread::spawn(move || {
            EventSink::<OrderStatus>::listen(&simulated_account).unwrap();
        });
        std::thread::spawn(move || {
            EventSink::<TfTrades>::listen(&s_a1).unwrap();
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
        order_statuses_channel
            .0
            .broadcast(os.clone())
            .await
            .unwrap();
        order_statuses_channel
            .0
            .broadcast(closing_order.clone())
            .await
            .unwrap();
        s_a.emit().await?;

        while !order_statuses_channel.0.is_empty() {
            tokio::time::sleep(std::time::Duration::from_micros(100)).await;
        }
        while !tf_trades_channel.0.is_empty() {
            tokio::time::sleep(std::time::Duration::from_micros(100)).await;
        }
        let trade = trades_receiver.recv().await.unwrap();
        assert_eq!(trade.realized_pnl, Decimal::ZERO);
        assert_eq!(trade.position_side, Side::Bid);
        assert_eq!(trade.side, Side::Ask);

        let open_orders = s_a.get_open_orders(&symbol).await;
        assert_eq!(open_orders.len(), 0);
        Ok(())
    }
}
