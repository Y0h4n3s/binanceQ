use crate::{ExchangeAccount, ExchangeAccountInfo, Position, TradeExecutor};
use async_broadcast::{Receiver, Sender};
use async_trait::async_trait;
use binance_q_events::{EventEmitter, EventSink};
use binance_q_mongodb::client::MongoClient;
use binance_q_types::{
    ExchangeId, Order, OrderStatus, Side, Symbol, SymbolAccount, TfTrades, Trade,
};
use rust_decimal::prelude::FromPrimitive;
use rust_decimal::Decimal;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;

type ArcMap<K, V> = Arc<RwLock<HashMap<K, V>>>;
type ArcSet<T> = Arc<RwLock<HashSet<T>>>;

#[derive(Clone)]
pub struct SimulatedAccount {
    pub symbol_accounts: ArcMap<Symbol, SymbolAccount>,
    pub open_orders: ArcMap<Symbol, ArcSet<OrderStatus>>,
    pub order_history: ArcMap<Symbol, ArcSet<OrderStatus>>,
    pub trade_history: ArcMap<Symbol, ArcSet<Trade>>,
    trade_q: Arc<RwLock<VecDeque<Trade>>>,
    pub trade_subscribers: Arc<RwLock<Sender<Trade>>>,
    pub positions: ArcMap<Symbol, Arc<RwLock<Position>>>,
    tf_trades: Arc<RwLock<Receiver<TfTrades>>>,
    order_statuses: Arc<RwLock<Receiver<OrderStatus>>>,
    tf_trades_working: Arc<std::sync::RwLock<bool>>,
    order_statuses_working: Arc<std::sync::RwLock<bool>>,
}

#[derive(Clone)]
pub struct SimulatedExecutor {
    pub account: Arc<SimulatedAccount>,
    pub orders: Arc<RwLock<Receiver<Order>>>,
    order_status_q: Arc<RwLock<VecDeque<OrderStatus>>>,
    pub order_status_subscribers: Arc<RwLock<Sender<OrderStatus>>>,
    order_working: Arc<std::sync::RwLock<bool>>,
}
impl SimulatedAccount {
    pub async fn new(
        tf_trades: Receiver<TfTrades>,
        order_statuses: Receiver<OrderStatus>,
        symbols: Vec<Symbol>,
    ) -> Self {
        let symbol_accounts = Arc::new(RwLock::new(HashMap::new()));
        let open_orders = Arc::new(RwLock::new(HashMap::new()));
        let order_history = Arc::new(RwLock::new(HashMap::new()));
        let trade_history = Arc::new(RwLock::new(HashMap::new()));
        let positions = Arc::new(RwLock::new(HashMap::new()));
        for symbol in symbols {
            let symbol_account = SymbolAccount {
                symbol: symbol.clone(),
                base_asset_free: Default::default(),
                base_asset_locked: Default::default(),
                // TODO: extract this config
                quote_asset_free: Decimal::new(10000, 0),
                quote_asset_locked: Default::default(),
            };
            let position = Position::new(Side::Ask, symbol.clone(), Decimal::ZERO, Decimal::ZERO);
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
        }
        Self {
            symbol_accounts,
            open_orders,
            order_history,
            trade_history,
            positions,
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
        orders_rx: Receiver<Order>,
        trades_rx: Receiver<TfTrades>,
        symbols: Vec<Symbol>,
        trades: Sender<Trade>,
    ) -> Self {
        let order_statuses_channel = async_broadcast::broadcast(100);
        let mut account = SimulatedAccount::new(trades_rx, order_statuses_channel.1, symbols).await;
        account.subscribe(trades).await;
        account.emit().await;
        let ac = account.clone();
        std::thread::spawn(move || {
            EventSink::<TfTrades>::listen(&ac).unwrap();
        });
        let ac = account.clone();
        std::thread::spawn(move || {
            EventSink::<OrderStatus>::listen(&ac).unwrap();
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
    fn get_subscribers(&self) -> Arc<RwLock<Sender<Trade>>> {
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
                    subs.broadcast(trade.clone()).await.unwrap();
                }
            }
        }))
    }
}

impl EventSink<TfTrades> for SimulatedAccount {
    fn get_receiver(&self) -> Arc<RwLock<Receiver<TfTrades>>> {
        self.tf_trades.clone()
    }
    fn handle_event(&self, event_msg: TfTrades) -> anyhow::Result<JoinHandle<anyhow::Result<()>>> {
        let open_orders = self.open_orders.clone();
        let trade_q = self.trade_q.clone();
        let filled_orders = self.order_history.clone();
        let positions = self.positions.clone();
        Ok(tokio::spawn(async move {
            // if any open orders are fillable move them to filled orders and update position and push a trade event to trade queue if it is a order opposite to position
            for tf_trade in event_msg {
                let symbol = tf_trade.symbol.clone();
                let open_orders = open_orders.read().await.get(&symbol).unwrap().clone();
                let all_filled_orders = filled_orders.read().await;
                let all_positions = positions.read().await;
                let mut partial_fills = vec![];
                for trade in tf_trade.trades {
                    let mut open_orders = open_orders.write().await;
                    if open_orders.len() == 0 {
                        continue;
                    }
                    let last = open_orders.iter().next().unwrap().clone();
                    let rem = open_orders.remove(&last.clone());
                    drop(open_orders);
                    match last {
                        OrderStatus::Pending(order) => {
                            if (order.side == Side::Bid
                                && (Decimal::from_f64(trade.price).unwrap().lt(&order.price)
                                    || order.price.eq(&Decimal::from_f64(trade.price).unwrap())))
                                || (order.side == Side::Ask
                                    && (order.price.lt(&Decimal::from_f64(trade.price).unwrap())
                                        || order
                                            .price
                                            .eq(&Decimal::from_f64(trade.price).unwrap())))
                            {
                                let position = all_positions.get(&symbol).unwrap();
                                let mut w = position.write().await;
                                let filled_qty = Decimal::from_f64(trade.qty).unwrap();
                                let filled_price = Decimal::from_f64(trade.price).unwrap();
                                let mut filled_order = order.clone();
                                filled_order.quantity = filled_qty.max(order.quantity);
                                filled_order.price = filled_price;
                                filled_order.time = trade.timestamp;
    
                                if let Some(trade) = w.apply_order(&filled_order) {
                                    let mut trade_q = trade_q.write().await;
                                    trade_q.push_back(trade);
                                }
                                if filled_qty < order.quantity {
                                    partial_fills.push(OrderStatus::PartiallyFilled(
                                        order.clone(),
                                        Decimal::from_f64(trade.qty).unwrap(),
                                    ));
                                } else {
                                    let mut filled_orders =
                                        all_filled_orders.get(&symbol).unwrap().write().await;

                                    filled_orders.insert(OrderStatus::Filled(order.clone()));
                                }
                            }
                        }
                        OrderStatus::PartiallyFilled(order, filled_qty_so_far) => {
                            if (order.side == Side::Bid
                                && order.price.gt(&Decimal::from_f64(trade.price).unwrap()))
                                || (order.side == Side::Ask
                                    && order.price.lt(&Decimal::from_f64(trade.price).unwrap()))
                            {
                                let position = all_positions.get(&symbol).unwrap();
                                let mut w = position.write().await;
                                let filled_qty = Decimal::from_f64(trade.qty).unwrap();
                                let filled_price = Decimal::from_f64(trade.price).unwrap();

                                let mut filled_order = order.clone();
                                filled_order.quantity =
                                    filled_qty.max(order.quantity - filled_qty_so_far);
                                filled_order.price = filled_price;
                                filled_order.time = trade.timestamp;

                                if let Some(trade) = w.apply_order(&filled_order) {
                                    let mut trade_q = trade_q.write().await;
                                    trade_q.push_back(trade);
                                }
                                if filled_qty < order.quantity {
                                    partial_fills.push(OrderStatus::PartiallyFilled(
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
                            let position = all_positions.get(&symbol).unwrap();
                            let mut w = position.write().await;
                            let filled_qty = Decimal::from_f64(trade.qty).unwrap();
                            let filled_price = Decimal::from_f64(trade.price).unwrap();

                            let mut filled_order = order.clone();

                            filled_order.price = filled_price;
                            filled_order.time = trade.timestamp;
                            if let Some(trade) = w.apply_order(&filled_order) {
                                let mut trade_q = trade_q.write().await;
                                trade_q.push_back(trade);
                            }
                            let mut filled_orders =
                                all_filled_orders.get(&symbol).unwrap().write().await;
                            filled_orders.insert(OrderStatus::Filled(filled_order.clone()));
                        }
                        _ => {}
                    }
                }
                
                for order in partial_fills {
                    open_orders.write().await.insert(order);
                }
            }
            Ok(())
        }))
    }
    fn working(&self) -> bool {
        self.tf_trades_working.read().unwrap().clone()
    }
    fn set_working(&self, working: bool) -> anyhow::Result<()> {
        *self.tf_trades_working.write().unwrap() = working;
        Ok(())
    }
}

impl EventSink<OrderStatus> for SimulatedAccount {
    fn get_receiver(&self) -> Arc<RwLock<Receiver<OrderStatus>>> {
        self.order_statuses.clone()
    }
    fn handle_event(
        &self,
        event_msg: OrderStatus,
    ) -> anyhow::Result<JoinHandle<anyhow::Result<()>>> {
        let open_orders = self.open_orders.clone();
        let order_history = self.order_history.clone();
        let trade_q = self.trade_q.clone();
        let all_positions = self.positions.clone();
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
        self.order_statuses_working.read().unwrap().clone()
    }
    fn set_working(&self, working: bool) -> anyhow::Result<()> {
        *self.order_statuses_working.write().unwrap() = working;
        Ok(())
    }
}

impl EventSink<Order> for SimulatedExecutor {
    fn get_receiver(&self) -> Arc<RwLock<Receiver<Order>>> {
        self.orders.clone()
    }
    fn handle_event(&self, event_msg: Order) -> anyhow::Result<JoinHandle<anyhow::Result<()>>> {
        let fut = self.execute_order(event_msg);
        let order_status_q = self.order_status_q.clone();
        Ok(tokio::spawn(async move {
            if let Ok(res) = fut {
                order_status_q.write().await.push_back(res);
            }
            Ok(())
        }))
    }
    fn working(&self) -> bool {
        self.order_working.read().unwrap().clone()
    }
    fn set_working(&self, working: bool) -> anyhow::Result<()> {
        *self.order_working.write().unwrap() = working;
        Ok(())
    }
}

#[async_trait]
impl EventEmitter<OrderStatus> for SimulatedExecutor {
    fn get_subscribers(&self) -> Arc<RwLock<Sender<OrderStatus>>> {
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
                    match subs.broadcast(order_status.clone()).await {
                        Ok(_) => {}
                        Err(e) => {
                            eprintln!("error broadcasting order status: {:?}", e);
                        }
                    }
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }))
    }
}

#[async_trait]
impl ExchangeAccountInfo for SimulatedAccount {
    fn get_exchange_id(&self) -> ExchangeId {
        todo!()
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
}

#[async_trait]
impl ExchangeAccount for SimulatedAccount {
    async fn limit_long(&self, order: Order) -> anyhow::Result<OrderStatus> {
        let accounts = self.symbol_accounts.read().await;
        let symbol_account = accounts.get(&order.symbol).unwrap();
        if symbol_account.base_asset_free < order.quantity {
            return Ok(OrderStatus::Canceled(
                order,
                "Insufficient balance".to_string(),
            ));
        }

        Ok(OrderStatus::Pending(order))
    }

    async fn limit_short(&self, _order: Order) -> anyhow::Result<OrderStatus> {
        todo!()
    }

    async fn market_long(&self, order: Order) -> anyhow::Result<OrderStatus> {
        let accounts = self.symbol_accounts.read().await;
        let symbol_account = accounts.get(&order.symbol).unwrap();
        // if symbol_account.base_asset_free < order.quantity {
        //     return Ok(OrderStatus::Canceled(
        //         order,
        //         "Insufficient balance".to_string(),
        //     ));
        // }

        Ok(OrderStatus::Filled(order))
    }

    async fn market_short(&self, order: Order) -> anyhow::Result<OrderStatus> {
        let accounts = self.symbol_accounts.read().await;
        let symbol_account = accounts.get(&order.symbol).unwrap();
        // if symbol_account.base_asset_free < order.quantity {
        //     return Ok(OrderStatus::Canceled(
        //         order,
        //         "Insufficient balance".to_string(),
        //     ));
        // }

        Ok(OrderStatus::Filled(order))
    }
}

#[async_trait]
impl TradeExecutor for SimulatedExecutor {
    const ID: ExchangeId = ExchangeId::Simulated;
    type Account = SimulatedAccount;
    fn get_account(&self) -> Arc<Self::Account> {
        self.account.clone()
    }
}
