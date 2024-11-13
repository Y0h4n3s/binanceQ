
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

pub struct BinanceLiveAccount {
    pub symbol_accounts: ArcMap<Symbol, SymbolAccount>,
    pub open_orders: ArcMap<Symbol, ArcSet<Order>>,
    pub positions: ArcMap<Symbol, Position>,
    order_statuses: InactiveReceiver<(OrderStatus, Option<Arc<Notify>>)>,
    spreads: ArcMap<Symbol, Arc<RwLock<Spread>>>,
    pub order_status_subscribers: Arc<RwLock<Sender<(OrderStatus, Option<Arc<Notify>>)>>>,
}

impl BinanceLiveAccount {
    /// Creates a new `BinanceLiveAccount` with the specified parameters.
    /// Initializes symbol accounts, open orders, positions, and spreads for the given symbols.
    pub async fn new(
        order_statuses: InactiveReceiver<(OrderStatus, Option<Arc<Notify>>)>,
        symbols: Vec<Symbol>,
        order_status_subscribers: Sender<(OrderStatus, Option<Arc<Notify>>)>,
    ) -> Self {
        let symbol_accounts = Arc::new(Mutex::new(HashMap::new()));
        let open_orders = Arc::new(Mutex::new(HashMap::new()));
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

            positions.lock().await.insert(symbol.clone(), position);
            spreads
                .lock()
                .await
                .insert(symbol.clone(), Arc::new(RwLock::new(spread)));
        }
        Self {
            symbol_accounts,
            open_orders,
            positions,
            spreads,
            order_statuses,
            order_status_subscribers: Arc::new(RwLock::new(order_status_subscribers)),
        }
    }
}

// TODO: receive statuses from the TFTrades sink and process them here instead of there
#[async_trait]
impl EventSink<OrderStatus> for BinanceLiveAccount {
    fn get_receiver(&self) -> Receiver<(OrderStatus, Option<Arc<Notify>>)> {
        self.order_statuses.clone().activate()
    }
    async fn name(&self) -> String {
        "BinanceLiveAccount OrderStatus Sink".to_string()
    }
    async fn handle_event(&self, event_msg: OrderStatus) -> anyhow::Result<()> {
        let open_orders = self.open_orders.clone();
        match &event_msg {
            // add to order set
            OrderStatus::Pending(order) => {
                trace!("Adding pending order to order set: {:?}", order);

            }
            // valid types are limit
            OrderStatus::PartiallyFilled(order, delta) => {
                let mut new_order = order.clone();
                new_order.quantity = order.quantity - delta;
                new_order.id = Uuid::new_v4();
                trace!(
                    "adding new order {:?} for partially filled order: {:?}",
                    new_order,
                    order
                );

            }
            // remove from order set
            OrderStatus::Filled(order) => {
                trace!("Removing filled order from order set: {:?}", order);

            }
            // log reason and remove
            OrderStatus::Canceled(order, reason) => {
                trace!(
                    "Removing canceled order with reason `{}` from order set: {:?}",
                    reason,
                    order
                );

            }
        }
        Ok(())
    }
}

#[async_trait]
impl ExchangeAccountInfo for BinanceLiveAccount {
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
        todo!()
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
        todo!()
    }
}

#[async_trait]
impl ExchangeAccount for BinanceLiveAccount {
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