use crate::db::client::SQLiteClient;
/// This module implements a simulated trading account that processes trades and orders.
/// It uses the `EventSink` trait to handle incoming trade and order events.
use crate::events::EventSink;
use crate::executors::order_state_machine_candle::OrderStateMachine;
use crate::executors::{
    broadcast, broadcast_and_wait, ExchangeAccount, ExchangeAccountInfo, Position, Spread,
    TradeExecutor,
};
use crate::types::{
    ExchangeId, Order, OrderStatus, OrderType, Side, Symbol, SymbolAccount, TfTrades, Trade,
    TradeEntry,
};
use async_broadcast::{InactiveReceiver, Receiver, Sender};
use async_trait::async_trait;
use dashmap::{DashMap, DashSet};
use rayon::prelude::*;
use rust_decimal::prelude::FromPrimitive;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt::Debug;
use std::sync::Arc;
use tokio::sync::{Mutex, Notify, RwLock};
use tokio::task::JoinHandle;
use tracing::{debug, info, trace};
use uuid::Uuid;

// can't use dashmap because of https://github.com/xacrimon/dashmap/issues/79
type ArcMap<K, V> = Arc<Mutex<HashMap<K, V>>>;
type ArcSet<T> = Arc<Mutex<HashSet<T>>>;

#[derive(Clone)]
/// Represents a simulated trading account, which manages positions, orders, and trades.
/// It provides methods to create new accounts, handle events, and retrieve account information.

pub struct SimulatedCandleAccount {
    pub symbol_accounts: ArcMap<Symbol, SymbolAccount>,
    pub open_orders: ArcMap<Symbol, ArcSet<Order>>,
    #[cfg(test)]
    pub order_history: ArcMap<Symbol, ArcSet<OrderStatus>>,
    #[cfg(test)]
    pub trade_history: ArcMap<Symbol, ArcSet<Trade>>,
    pub trade_subscribers: Arc<RwLock<Sender<(Trade, Option<Arc<Notify>>)>>>,
    pub positions: ArcMap<Symbol, Position>,
    tf_trades: InactiveReceiver<(TfTrades, Option<Arc<Notify>>)>,
    order_statuses: InactiveReceiver<(OrderStatus, Option<Arc<Notify>>)>,
    new_trades: InactiveReceiver<(Trade, Option<Arc<Notify>>)>,
    spreads: ArcMap<Symbol, Arc<RwLock<Spread>>>,
    pub order_status_subscribers: Arc<RwLock<Sender<(OrderStatus, Option<Arc<Notify>>)>>>,
    sqlite_client: Arc<SQLiteClient>,
}

impl SimulatedCandleAccount {
    /// Creates a new `SimulatedCandleAccount` with the specified parameters.
    /// Initializes symbol accounts, open orders, positions, and spreads for the given symbols.
    pub async fn new(
        tf_trades: InactiveReceiver<(TfTrades, Option<Arc<Notify>>)>,
        order_statuses: InactiveReceiver<(OrderStatus, Option<Arc<Notify>>)>,
        trades: InactiveReceiver<(Trade, Option<Arc<Notify>>)>,
        symbols: Vec<Symbol>,
        order_status_subscribers: Sender<(OrderStatus, Option<Arc<Notify>>)>,
        trade_subscribers: Sender<(Trade, Option<Arc<Notify>>)>,
        sqlite_client: Arc<SQLiteClient>,
    ) -> Self {
        let symbol_accounts = Arc::new(Mutex::new(HashMap::new()));
        let open_orders = Arc::new(Mutex::new(HashMap::new()));
        #[cfg(test)]
        let order_history = Arc::new(Mutex::new(HashMap::new()));
        #[cfg(test)]
        let trade_history = Arc::new(Mutex::new(HashMap::new()));
        let positions = Arc::new(Mutex::new(HashMap::new()));
        let spreads = Arc::new(Mutex::new(HashMap::new()));

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
                .lock()
                .await
                .insert(symbol.clone(), symbol_account);
            open_orders
                .lock()
                .await
                .insert(symbol.clone(), Arc::new(Mutex::new(HashSet::new())));
            #[cfg(test)]
            {
                order_history
                    .lock()
                    .await
                    .insert(symbol.clone(), Arc::new(Mutex::new(HashSet::new())));
                trade_history
                    .lock()
                    .await
                    .insert(symbol.clone(), Arc::new(Mutex::new(HashSet::new())));
            }
            positions.lock().await.insert(symbol.clone(), position);
            spreads
                .lock()
                .await
                .insert(symbol.clone(), Arc::new(RwLock::new(spread)));
        }
        Self {
            symbol_accounts,
            open_orders,
            #[cfg(test)]
            order_history,
            #[cfg(test)]
            trade_history,
            positions,
            spreads,
            tf_trades,
            trade_subscribers: Arc::new(RwLock::new(trade_subscribers)),
            order_statuses,
            order_status_subscribers: Arc::new(RwLock::new(order_status_subscribers)),
            new_trades: trades,
            sqlite_client,
        }
    }
}

#[async_trait]
impl EventSink<TfTrades> for SimulatedCandleAccount {
    fn get_receiver(&self) -> Receiver<(TfTrades, Option<Arc<Notify>>)> {
        self.tf_trades.clone().activate()
    }
    async fn name(&self) -> String {
        "SimulatedCandleAccount TfTrades Sink".to_string()
    }

    /// Handles an incoming trade event by updating open orders and positions.
    /// Processes each trade in the event message and updates the account state accordingly.
    async fn handle_event(&self, event_msg: TfTrades) -> anyhow::Result<()> {
        if event_msg.is_empty() {
            return Ok(());
        }
        let open_orders = self.open_orders.clone();


        let spreads = self.spreads.clone();
        // update spread first with the last trade
        if let Some(last_trade) = event_msg.last() {
                if let Some(spread_lock) = spreads.lock().await.get(&last_trade.symbol) {
                    let mut spread = spread_lock.write().await;
                    spread.update(last_trade.close, last_trade.close_time);
                } else {
                    // Handle missing spread for the symbol
                }
        }

        // TODO: remove used orders to avoid trade reuse inconsistencies
        // TODO: wait for all order status wakes to be notified to increase reliability
        // if any open orders are fillable move them to filled orders and update position and push a trade event to trade queue if it is a order opposite to position

        for tf_trade in event_msg {
            let symbol = tf_trade.symbol.clone();
            let mut open_orders = open_orders
                .lock()
                .await
                .get(&symbol)
                .unwrap()
                .lock()
                .await
                .clone();

            if open_orders.is_empty() {
                return Ok(())
            }
            let mut oo: Vec<_> = open_orders.iter().cloned().collect();
            drop(open_orders);
            oo.sort();
            let len = oo.len();

            for (index, mut order) in oo.clone().into_iter().enumerate() {
                let mut state_machine = OrderStateMachine::new(
                    &mut order,
                    // account for orders removed on previous iterations
                    index - (len - oo.len()),
                    &self.positions,
                    &mut oo,
                    &self.trade_subscribers,
                    &self.order_status_subscribers,
                );
                state_machine.process_order(&tf_trade).await;
            }


        }


        Ok(())
    }
}

#[async_trait]
impl EventSink<Trade> for SimulatedCandleAccount {
    fn get_receiver(&self) -> Receiver<(Trade, Option<Arc<Notify>>)> {
        self.new_trades.activate_cloned()
    }
    async fn name(&self) -> String {
        "SimulatedCandleAccount Trade Sink".to_string()
    }
    async fn handle_event(&self, event_msg: Trade) -> anyhow::Result<()> {
        let sqlite_client = &self.sqlite_client;
        #[cfg(test)]
        self.trade_history
            .lock()
            .await
            .get(&event_msg.symbol)
            .unwrap()
            .lock()
            .await
            .insert(event_msg.clone());
        SQLiteClient::insert_trade(&sqlite_client.pool, event_msg).await;
        Ok(())
    }
}

// TODO: receive statuses from the TFTrades sink and process them here instead of there
#[async_trait]
impl EventSink<OrderStatus> for SimulatedCandleAccount {
    fn get_receiver(&self) -> Receiver<(OrderStatus, Option<Arc<Notify>>)> {
        self.order_statuses.clone().activate()
    }
    async fn name(&self) -> String {
        "SimulatedCandleAccount OrderStatus Sink".to_string()
    }
    async fn handle_event(&self, event_msg: OrderStatus) -> anyhow::Result<()> {
        let open_orders = self.open_orders.clone();
        #[cfg(test)]
        {
            if let Some(orders) = self
                .order_history
                .lock()
                .await
                .get(&event_msg.order().symbol)
            {
                let mut lock = orders.lock().await;
                lock.insert(event_msg.clone());
            } else {
                let mut new_set = Arc::new(Mutex::new(HashSet::new()));
                new_set.lock().await.insert(event_msg.clone());

                self.order_history
                    .lock()
                    .await
                    .insert(event_msg.order().symbol.clone(), new_set);
            }
        }
        match &event_msg {
            // add to order set
            OrderStatus::Pending(order) => {
                trace!("Adding pending order to order set: {:?}", order);
                if let Some(orders) = open_orders.lock().await.get(&order.symbol) {
                    orders.lock().await.insert(order.clone());
                } else {
                    let mut new_set = Arc::new(Mutex::new(HashSet::new()));
                    new_set.lock().await.insert(order.clone());
                    open_orders
                        .lock()
                        .await
                        .insert(order.symbol.clone(), new_set);
                }
            }
            // valid types are limit
            OrderStatus::PartiallyFilled(order, delta) => {
                let mut new_order = order.clone();
                new_order.quantity = order.quantity - delta;
                new_order.id = Uuid::new_v4();
                SQLiteClient::insert_order(&self.sqlite_client.pool, order.clone()).await;

                #[cfg(test)]
                {
                    if let Some(orders) = self.order_history.lock().await.get(&order.symbol) {
                        let mut lock = orders.lock().await;
                        lock.insert(OrderStatus::Filled(order.clone()));
                    } else {
                        let mut new_set = Arc::new(Mutex::new(HashSet::new()));
                        new_set
                            .lock()
                            .await
                            .insert(OrderStatus::Filled(order.clone()));

                        self.order_history
                            .lock()
                            .await
                            .insert(order.symbol.clone(), new_set);
                    }
                }
                trace!(
                    "adding new order {:?} for partially filled order: {:?}",
                    new_order,
                    order
                );
                if let Some(orders) = open_orders.lock().await.get(&order.symbol) {
                    let mut lock = orders.lock().await;
                    lock.remove(order);
                    lock.insert(new_order);
                } else {
                    let mut new_set = Arc::new(Mutex::new(HashSet::new()));
                    new_set.lock().await.insert(order.clone());
                    open_orders
                        .lock()
                        .await
                        .insert(order.symbol.clone(), new_set);
                }
            }
            // remove from order set
            OrderStatus::Filled(order) => {
                trace!("Removing filled order from order set: {:?}", order);
                if let Some(orders) = open_orders.lock().await.get(&order.symbol) {
                    SQLiteClient::insert_order(&self.sqlite_client.pool, order.clone()).await;

                    orders.lock().await.remove(order);
                }
            }
            // log reason and remove
            OrderStatus::Canceled(order, reason) => {
                trace!(
                    "Removing canceled order with reason `{}` from order set: {:?}",
                    reason,
                    order
                );
                if let Some(orders) = open_orders.lock().await.get(&order.symbol) {
                    orders.lock().await.remove(order);
                }
            }
        }
        Ok(())
    }
}

#[async_trait]
impl ExchangeAccountInfo for SimulatedCandleAccount {
    fn get_exchange_id(&self) -> ExchangeId {
        ExchangeId::Simulated
    }

    async fn get_open_orders(&self, symbol: &Symbol) -> Arc<HashSet<Order>> {
        Arc::new(
            self.open_orders
                .lock()
                .await
                .get(symbol)
                .unwrap()
                .lock()
                .await
                .clone(),
        )
    }

    async fn get_symbol_account(&self, symbol: &Symbol) -> SymbolAccount {
        self.symbol_accounts
            .lock()
            .await
            .get(symbol)
            .unwrap()
            .clone()
    }

    #[cfg(test)]
    async fn get_past_trades(&self, symbol: &Symbol, length: Option<usize>) -> Arc<HashSet<Trade>> {
        Arc::new(
            self.trade_history
                .lock()
                .await
                .get(symbol)
                .unwrap()
                .lock()
                .await
                .clone(),
        )
    }

    async fn get_position(&self, symbol: &Symbol) -> Arc<Position> {
        let lock = self.positions.lock().await;
        let position = lock.get(symbol).unwrap();
        Arc::new(position.clone())
    }

    async fn get_spread(&self, symbol: &Symbol) -> Arc<Spread> {
        let lock = self.spreads.lock().await;
        let spread_lock = lock.get(symbol).unwrap();
        let spread = spread_lock.read().await.clone();
        Arc::new(spread)
    }

    #[cfg(test)]
    async fn get_order(&self, symbol: &Symbol, id: &Uuid) -> Vec<Order> {
        let order_history = self.order_history.lock().await.get(symbol).unwrap().clone();
        let mut res = vec![];
        order_history.lock().await.iter().for_each(|o| {
            let or = o.order();
            if &or.id == id {
                res.push(or.clone())
            }
        });
        res
    }
}

#[async_trait]
impl ExchangeAccount for SimulatedCandleAccount {
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