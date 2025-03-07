use crate::db::client::SQLiteClient;
/// This module implements a simulated trading account that processes trades and orders.
/// It uses the `EventSink` trait to handle incoming trade and order events.
use crate::events::EventSink;
use crate::executors::order_state_machine::OrderStateMachine;
use crate::executors::{
    broadcast, broadcast_and_wait, ExchangeAccount, ExchangeAccountInfo, Position, Spread,
    TradeExecutor,
};
use crate::mongodb::MongoClient;
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
use tracing::{debug, trace};
use uuid::Uuid;

// can't use dashmap because of https://github.com/xacrimon/dashmap/issues/79
type ArcMap<K, V> = Arc<Mutex<HashMap<K, V>>>;
type ArcSet<T> = Arc<Mutex<HashSet<T>>>;

#[derive(Clone)]
/// Represents a simulated trading account, which manages positions, orders, and trades.
/// It provides methods to create new accounts, handle events, and retrieve account information.

pub struct SimulatedAccount {
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

impl SimulatedAccount {
    /// Creates a new `SimulatedAccount` with the specified parameters.
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
impl EventSink<TfTrades> for SimulatedAccount {
    fn get_receiver(&self) -> Receiver<(TfTrades, Option<Arc<Notify>>)> {
        self.tf_trades.clone().activate()
    }
    async fn name(&self) -> String {
        "SimulatedAccount TfTrades Sink".to_string()
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
            if let Some(last) = last_trade.trades.last() {
                if let Some(spread_lock) = spreads.lock().await.get(&last_trade.symbol) {
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
            let mut open_orders = open_orders
                .lock()
                .await
                .get(&symbol)
                .unwrap()
                .lock()
                .await
                .clone();

            if open_orders.is_empty() {
                continue;
            }
            let mut oo: Vec<_> = open_orders.iter().cloned().collect();
            drop(open_orders);
            oo.sort();
            for trade in &tf_trade.trades {
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
                    state_machine.process_order(trade).await;
                }

                if oo.is_empty() {
                    break;
                }
            }
        }

        Ok(())
    }
}

#[async_trait]
impl EventSink<Trade> for SimulatedAccount {
    fn get_receiver(&self) -> Receiver<(Trade, Option<Arc<Notify>>)> {
        self.new_trades.activate_cloned()
    }
    async fn name(&self) -> String {
        "SimulatedAccount Trade Sink".to_string()
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
impl EventSink<OrderStatus> for SimulatedAccount {
    fn get_receiver(&self) -> Receiver<(OrderStatus, Option<Arc<Notify>>)> {
        self.order_statuses.clone().activate()
    }
    async fn name(&self) -> String {
        "SimulatedAccount OrderStatus Sink".to_string()
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
impl ExchangeAccountInfo for SimulatedAccount {
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

    async fn setup_simulated_account(
        symbol: Symbol,
    ) -> (
        Arc<SimulatedAccount>,
        (
            Sender<(TfTrades, Option<Arc<Notify>>)>,
            InactiveReceiver<(TfTrades, Option<Arc<Notify>>)>,
        ),
        (
            Sender<(Trade, Option<Arc<Notify>>)>,
            InactiveReceiver<(Trade, Option<Arc<Notify>>)>,
        ),
        (
            Sender<(OrderStatus, Option<Arc<Notify>>)>,
            InactiveReceiver<(OrderStatus, Option<Arc<Notify>>)>,
        ),
    ) {
        let tf_trades_channel = async_broadcast::broadcast(100);
        let trades_channel = async_broadcast::broadcast(100);
        let order_statuses_channel = async_broadcast::broadcast(100);
        let sqlite_client = Arc::new(SQLiteClient::new().await);

        let simulated_account = Arc::new(
            SimulatedAccount::new(
                tf_trades_channel.1.clone().deactivate(),
                order_statuses_channel.1.clone().deactivate(),
                trades_channel.1.clone().deactivate(),
                vec![symbol.clone()],
                order_statuses_channel.0.clone(),
                trades_channel.0.clone(),
                sqlite_client,
            )
            .await,
        );

        let sa1 = simulated_account.clone();
        tokio::spawn(async move {
            EventSink::<OrderStatus>::listen(sa1).unwrap();
        });
        let sa2 = simulated_account.clone();
        tokio::spawn(async move {
            EventSink::<Trade>::listen(sa2).unwrap();
        });
        let sa3 = simulated_account.clone();
        tokio::spawn(async move {
            EventSink::<TfTrades>::listen(sa3).unwrap();
        });

        let tf = tf_trades_channel.1.deactivate();
        let t = trades_channel.1.deactivate();
        let os = order_statuses_channel.1.deactivate();
        (
            simulated_account,
            (tf_trades_channel.0, tf),
            (trades_channel.0, t),
            (order_statuses_channel.0, os),
        )
    }

    #[tokio::test]
    async fn test_market_long_with_stop_loss_hit() -> anyhow::Result<()> {
        let symbol = Symbol {
            symbol: "TST/USDT".to_string(),
            exchange: ExchangeId::Simulated,
            base_asset_precision: 1,
            quote_asset_precision: 2,
        };
        let (simulated_account, tf_trades_channel, trades_channel, order_statuses_channel) =
            setup_simulated_account(symbol.clone()).await;

        // Create a market long order
        let market_order = Order {
            id: Uuid::new_v4(),
            symbol: symbol.clone(),
            side: Side::Bid,
            price: Decimal::new(100, 1),
            quantity: Decimal::new(100, 1),
            time: 123,
            order_type: OrderType::Market,
            lifetime: 0,
            close_policy: ClosePolicy::None,
        };

        // Broadcast the market order
        let notifier = Arc::new(Notify::new());
        let n = notifier.notified();
        order_statuses_channel
            .0
            .broadcast((
                OrderStatus::Pending(market_order.clone()),
                Some(notifier.clone()),
            ))
            .await
            .unwrap();
        n.await;

        // Create a stop-loss order
        let stop_loss_order = Order {
            id: Uuid::new_v4(),
            symbol: symbol.clone(),
            side: Side::Ask,
            price: Decimal::new(90, 1), // Stop-loss price
            quantity: Decimal::new(100, 1),
            time: 123,
            order_type: OrderType::StopLossLimit,
            lifetime: 0,
            close_policy: ClosePolicy::None,
        };

        // Broadcast the stop-loss order
        let n = notifier.notified();
        order_statuses_channel
            .0
            .broadcast((
                OrderStatus::Pending(stop_loss_order.clone()),
                Some(notifier.clone()),
            ))
            .await
            .unwrap();
        n.await;

        // Create a take-profit order
        let take_profit_order = Order {
            id: Uuid::new_v4(),
            symbol: symbol.clone(),
            side: Side::Ask,
            price: Decimal::new(110, 1), // Take-profit price
            quantity: Decimal::new(100, 1),
            time: 123,
            order_type: OrderType::TakeProfitLimit,
            lifetime: 0,
            close_policy: ClosePolicy::None,
        };

        // Broadcast the take-profit order
        let n = notifier.notified();
        order_statuses_channel
            .0
            .broadcast((
                OrderStatus::Pending(take_profit_order.clone()),
                Some(notifier.clone()),
            ))
            .await
            .unwrap();
        n.await;

        // Simulate a trade that hits the stop-loss
        let n = notifier.notified();
        let trade_entry = TradeEntry {
            id: uuid::Uuid::new_v4().to_string(),
            price: Decimal::new(85, 1), // Price below stop-loss
            qty: Decimal::new(100, 1),
            timestamp: 0,
            delta: Decimal::new(0, 0),
            symbol: symbol.symbol.clone(),
        };
        tf_trades_channel
            .0
            .broadcast((
                vec![TfTrade {
                    symbol: symbol.clone(),
                    tf: 1,
                    id: uuid::Uuid::new_v4().to_string(),
                    timestamp: 124,
                    trades: vec![trade_entry.clone()],
                    min_trade_time: 0,
                    max_trade_time: 0,
                }],
                Some(notifier.clone()),
            ))
            .await
            .unwrap();
        n.await;

        // wait because we don't wait for notify when broadcasting trades
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

        // Check if stop-loss is executed and take-profit is removed
        let open_orders = simulated_account.get_open_orders(&symbol).await;
        assert!(
            open_orders.is_empty(),
            "Open orders should be empty after stop-loss is hit"
        );

        let position = simulated_account.get_position(&symbol).await;
        assert!(
            !position.is_open(),
            "Position should be closed after stop-loss is hit"
        );

        // Check trade history for negative realized PnL
        let trade_history = simulated_account.get_past_trades(&symbol, None).await;
        let trade = trade_history.iter().next().unwrap();
        assert!(
            trade.realized_pnl < Decimal::ZERO,
            "Realized PnL should be negative"
        );

        Ok(())
    }
    #[tokio::test]
    async fn test_multiple_market_orders_with_tp_sl() -> anyhow::Result<()> {
        let symbol = Symbol {
            symbol: "TST/USDT".to_string(),
            exchange: ExchangeId::Simulated,
            base_asset_precision: 1,
            quote_asset_precision: 2,
        };
        let (simulated_account, tf_trades_channel, trades_channel, order_statuses_channel) =
            setup_simulated_account(symbol.clone()).await;

        // First market order
        let market_order_1 = Order {
            id: Uuid::new_v4(),
            symbol: symbol.clone(),
            side: Side::Bid,
            price: Decimal::new(100, 1),
            quantity: Decimal::new(100, 1),
            time: 123,
            order_type: OrderType::Market,
            lifetime: 0,
            close_policy: ClosePolicy::None,
        };

        // Broadcast the first market order
        let notifier = Arc::new(Notify::new());
        let n = notifier.notified();
        order_statuses_channel
            .0
            .broadcast((
                OrderStatus::Pending(market_order_1.clone()),
                Some(notifier.clone()),
            ))
            .await
            .unwrap();
        n.await;

        // First stop-loss order
        let stop_loss_order_1 = Order {
            id: Uuid::new_v4(),
            symbol: symbol.clone(),
            side: Side::Ask,
            price: Decimal::new(90, 1), // Stop-loss price
            quantity: Decimal::new(100, 1),
            time: 123,
            order_type: OrderType::StopLossLimit,
            lifetime: 0,
            close_policy: ClosePolicy::None,
        };

        // Broadcast the first stop-loss order
        let n = notifier.notified();
        order_statuses_channel
            .0
            .broadcast((
                OrderStatus::Pending(stop_loss_order_1.clone()),
                Some(notifier.clone()),
            ))
            .await
            .unwrap();
        n.await;

        // First take-profit order
        let take_profit_order_1 = Order {
            id: Uuid::new_v4(),
            symbol: symbol.clone(),
            side: Side::Ask,
            price: Decimal::new(110, 1), // Take-profit price
            quantity: Decimal::new(100, 1),
            time: 123,
            order_type: OrderType::TakeProfitLimit,
            lifetime: 0,
            close_policy: ClosePolicy::None,
        };

        // Broadcast the first take-profit order
        let n = notifier.notified();
        order_statuses_channel
            .0
            .broadcast((
                OrderStatus::Pending(take_profit_order_1.clone()),
                Some(notifier.clone()),
            ))
            .await
            .unwrap();
        n.await;

        // Simulate a trade that does not hit the stop-loss or take-profit
        let n = notifier.notified();
        let trade_entry_1 = TradeEntry {
            id: uuid::Uuid::new_v4().to_string(),
            price: Decimal::new(105, 1), // Price between stop-loss and take-profit
            qty: Decimal::new(100, 1),
            timestamp: 0,
            delta: Decimal::new(0, 0),
            symbol: symbol.symbol.clone(),
        };
        tf_trades_channel
            .0
            .broadcast((
                vec![TfTrade {
                    symbol: symbol.clone(),
                    tf: 1,
                    id: uuid::Uuid::new_v4().to_string(),
                    timestamp: 124,
                    trades: vec![trade_entry_1.clone()],
                    min_trade_time: 0,
                    max_trade_time: 0,
                }],
                Some(notifier.clone()),
            ))
            .await
            .unwrap();
        n.await;

        // Second market order
        let market_order_2 = Order {
            id: Uuid::new_v4(),
            symbol: symbol.clone(),
            side: Side::Bid,
            price: Decimal::new(100, 1),
            quantity: Decimal::new(100, 1),
            time: 125,
            order_type: OrderType::Market,
            lifetime: 0,
            close_policy: ClosePolicy::None,
        };

        // Broadcast the second market order
        let n = notifier.notified();
        order_statuses_channel
            .0
            .broadcast((
                OrderStatus::Pending(market_order_2.clone()),
                Some(notifier.clone()),
            ))
            .await
            .unwrap();
        n.await;

        // Second stop-loss order
        let stop_loss_order_2 = Order {
            id: Uuid::new_v4(),
            symbol: symbol.clone(),
            side: Side::Ask,
            price: Decimal::new(95, 1), // Stop-loss price
            quantity: Decimal::new(100, 1),
            time: 125,
            order_type: OrderType::StopLossLimit,
            lifetime: 0,
            close_policy: ClosePolicy::None,
        };

        // Broadcast the second stop-loss order
        let n = notifier.notified();
        order_statuses_channel
            .0
            .broadcast((
                OrderStatus::Pending(stop_loss_order_2.clone()),
                Some(notifier.clone()),
            ))
            .await
            .unwrap();
        n.await;

        // Second take-profit order
        let take_profit_order_2 = Order {
            id: Uuid::new_v4(),
            symbol: symbol.clone(),
            side: Side::Ask,
            price: Decimal::new(115, 1), // Take-profit price
            quantity: Decimal::new(100, 1),
            time: 125,
            order_type: OrderType::TakeProfitLimit,
            lifetime: 0,
            close_policy: ClosePolicy::None,
        };

        // Broadcast the second take-profit order
        let n = notifier.notified();
        order_statuses_channel
            .0
            .broadcast((
                OrderStatus::Pending(take_profit_order_2.clone()),
                Some(notifier.clone()),
            ))
            .await
            .unwrap();
        n.await;

        // Simulate a trade that hits the take-profit of the second order
        let n = notifier.notified();
        let trade_entry_2 = TradeEntry {
            id: uuid::Uuid::new_v4().to_string(),
            price: Decimal::new(120, 1), // Price above take-profit
            qty: Decimal::new(100, 1),
            timestamp: 0,
            delta: Decimal::new(0, 0),
            symbol: symbol.symbol.clone(),
        };
        tf_trades_channel
            .0
            .broadcast((
                vec![TfTrade {
                    symbol: symbol.clone(),
                    tf: 1,
                    id: uuid::Uuid::new_v4().to_string(),
                    timestamp: 126,
                    trades: vec![trade_entry_2.clone()],
                    min_trade_time: 0,
                    max_trade_time: 0,
                }],
                Some(notifier.clone()),
            ))
            .await
            .unwrap();
        n.await;

        // Check if the final position is long
        let position = simulated_account.get_position(&symbol).await;
        assert!(position.is_long(), "Final position should be long");

        // Check trade history for positive realized PnL
        let trade_history = simulated_account.get_past_trades(&symbol, None).await;
        let trade = trade_history.iter().next().unwrap();
        assert!(
            trade.realized_pnl > Decimal::ZERO,
            "Realized PnL should be positive"
        );

        Ok(())
    }
    #[tokio::test]
    async fn test_empty_trade_list() -> anyhow::Result<()> {
        let symbol = Symbol {
            symbol: "TST/USDT".to_string(),
            exchange: ExchangeId::Simulated,
            base_asset_precision: 1,
            quote_asset_precision: 2,
        };
        let (simulated_account, tf_trades_channel, _, _) =
            setup_simulated_account(symbol.clone()).await;

        // Simulate an empty trade list
        let notifier = Arc::new(Notify::new());
        let n = notifier.notified();
        tf_trades_channel
            .0
            .broadcast((vec![], Some(notifier.clone())))
            .await
            .unwrap();
        n.await;

        let open_orders = simulated_account.get_open_orders(&symbol).await;
        assert!(
            open_orders.is_empty(),
            "Open orders should remain unchanged with empty trade list"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_trade_with_zero_quantity() -> anyhow::Result<()> {
        let symbol = Symbol {
            symbol: "TST/USDT".to_string(),
            exchange: ExchangeId::Simulated,
            base_asset_precision: 1,
            quote_asset_precision: 2,
        };
        let (simulated_account, tf_trades_channel, _, _) =
            setup_simulated_account(symbol.clone()).await;

        // Simulate a trade with zero quantity
        let notifier = Arc::new(Notify::new());
        let n = notifier.notified();
        let trade_entry = TradeEntry {
            id: uuid::Uuid::new_v4().to_string(),
            price: Decimal::new(100, 1),
            qty: Decimal::ZERO,
            timestamp: 0,
            delta: Decimal::ZERO,
            symbol: symbol.symbol.clone(),
        };
        tf_trades_channel
            .0
            .broadcast((
                vec![TfTrade {
                    symbol: symbol.clone(),
                    tf: 1,
                    id: uuid::Uuid::new_v4().to_string(),
                    timestamp: 124,
                    trades: vec![trade_entry.clone()],
                    min_trade_time: 0,
                    max_trade_time: 0,
                }],
                Some(notifier.clone()),
            ))
            .await
            .unwrap();
        n.await;

        let open_orders = simulated_account.get_open_orders(&symbol).await;
        assert!(
            open_orders.is_empty(),
            "Open orders should remain unchanged with zero quantity trade"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_trade_with_negative_price() -> anyhow::Result<()> {
        let symbol = Symbol {
            symbol: "TST/USDT".to_string(),
            exchange: ExchangeId::Simulated,
            base_asset_precision: 1,
            quote_asset_precision: 2,
        };
        let (simulated_account, tf_trades_channel, _, _) =
            setup_simulated_account(symbol.clone()).await;

        // Simulate a trade with a negative price
        let notifier = Arc::new(Notify::new());
        let n = notifier.notified();
        let trade_entry = TradeEntry {
            id: uuid::Uuid::new_v4().to_string(),
            price: Decimal::new(-100, 1),
            qty: Decimal::new(100, 1),
            timestamp: 0,
            delta: Decimal::ZERO,
            symbol: symbol.symbol.clone(),
        };
        tf_trades_channel
            .0
            .broadcast((
                vec![TfTrade {
                    symbol: symbol.clone(),
                    tf: 1,
                    id: uuid::Uuid::new_v4().to_string(),
                    timestamp: 124,
                    trades: vec![trade_entry.clone()],
                    min_trade_time: 0,
                    max_trade_time: 0,
                }],
                Some(notifier.clone()),
            ))
            .await
            .unwrap();
        n.await;

        let open_orders = simulated_account.get_open_orders(&symbol).await;
        assert!(
            open_orders.is_empty(),
            "Open orders should remain unchanged with negative price trade"
        );

        Ok(())
    }
    #[tokio::test]
    async fn test_simulated_account_cancel_order() -> anyhow::Result<()> {
        let symbol = Symbol {
            symbol: "TST/USDT".to_string(),
            exchange: ExchangeId::Simulated,
            base_asset_precision: 1,
            quote_asset_precision: 2,
        };
        let (simulated_account, tf_trades_channel, trades_channel, order_statuses_channel) =
            setup_simulated_account(symbol.clone()).await;
        let open_orders = simulated_account.get_open_orders(&symbol).await;

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

        let order_history = simulated_account.get_order(&symbol, &os.order().id).await;
        assert!(
            order_history.contains(&canceled_order.order()),
            "Order history does not contain the canceled order"
        );

        let position = simulated_account.get_position(&symbol).await;
        assert!(
            !position.is_open(),
            "Position should remain unchanged after cancellation"
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_simulated_account_apply_stop_loss() -> anyhow::Result<()> {
        let symbol = Symbol {
            symbol: "TST/USDT".to_string(),
            exchange: ExchangeId::Simulated,
            base_asset_precision: 1,
            quote_asset_precision: 2,
        };
        let (simulated_account, tf_trades_channel, trades_channel, order_statuses_channel) =
            setup_simulated_account(symbol.clone()).await;

        let order_id = Uuid::new_v4();

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
            id: uuid::Uuid::new_v4().to_string(),
            price: dec!(11.0),
            qty: dec!(100.0),
            timestamp: 0,
            delta: dec!(0.0),
            symbol: symbol.symbol.clone(),
        };
        tf_trades_channel
            .0
            .broadcast((
                vec![TfTrade {
                    symbol: symbol.clone(),
                    tf: 1,
                    id: uuid::Uuid::new_v4().to_string(),
                    timestamp: 124,
                    trades: vec![entry.clone()],
                    min_trade_time: 0,
                    max_trade_time: 0,
                }],
                Some(notifier.clone()),
            ))
            .await
            .unwrap();
        n.await;
        let open_orders = simulated_account.get_open_orders(&symbol).await;
        let position = simulated_account.get_position(&symbol).await;
        assert!(
            open_orders.is_empty(),
            "initial position order not properly executed"
        );
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
            id: uuid::Uuid::new_v4().to_string(),
            price: dec!(8.0),
            qty: dec!(100.0),
            timestamp: 0,
            delta: dec!(0.0),
            symbol: symbol.symbol.clone(),
        };
        tf_trades_channel
            .0
            .broadcast((
                vec![TfTrade {
                    symbol: symbol.clone(),
                    tf: 1,
                    id: uuid::Uuid::new_v4().to_string(),
                    timestamp: 124,
                    trades: vec![entry.clone()],
                    min_trade_time: 0,
                    max_trade_time: 0,
                }],
                Some(notifier.clone()),
            ))
            .await
            .unwrap();
        n.await;

        // Check if stop-loss is executed
        assert!(
            open_orders.is_empty(),
            "Stop-loss order was not properly executed"
        );

        let order_history = simulated_account.get_order(&symbol, &os.order().id).await;
        assert!(
            order_history.contains(&os.order()),
            "Order history does not contain the stop-loss order"
        );

        let position = simulated_account.get_position(&symbol).await;
        assert!(!position.is_open(), "Position is still open");
        Ok(())
    }
    #[tokio::test]
    async fn test_simulated_account_apply_take_profit() -> anyhow::Result<()> {
        let symbol = Symbol {
            symbol: "TST/USDT".to_string(),
            exchange: ExchangeId::Simulated,
            base_asset_precision: 1,
            quote_asset_precision: 2,
        };
        let (simulated_account, tf_trades_channel, trades_channel, order_statuses_channel) =
            setup_simulated_account(symbol.clone()).await;

        let order_id = Uuid::new_v4();

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
            id: uuid::Uuid::new_v4().to_string(),
            price: dec!(11.0),
            qty: dec!(100.0),
            timestamp: 0,
            delta: dec!(0.0),
            symbol: symbol.symbol.clone(),
        };
        tf_trades_channel
            .0
            .broadcast((
                vec![TfTrade {
                    symbol: symbol.clone(),
                    tf: 1,
                    id: uuid::Uuid::new_v4().to_string(),
                    timestamp: 124,
                    trades: vec![entry.clone()],
                    min_trade_time: 0,
                    max_trade_time: 0,
                }],
                Some(notifier.clone()),
            ))
            .await
            .unwrap();
        n.await;
        let open_orders = simulated_account.get_open_orders(&symbol).await;
        let position = simulated_account.get_position(&symbol).await;
        assert!(
            open_orders.is_empty(),
            "initial position order not properly executed"
        );
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
            id: uuid::Uuid::new_v4().to_string(),
            price: dec!(19.0),
            qty: dec!(100.0),
            timestamp: 0,
            delta: dec!(0.0),
            symbol: symbol.symbol.clone(),
        };
        tf_trades_channel
            .0
            .broadcast((
                vec![TfTrade {
                    symbol: symbol.clone(),
                    tf: 1,
                    id: uuid::Uuid::new_v4().to_string(),
                    timestamp: 124,
                    trades: vec![entry.clone()],
                    min_trade_time: 0,
                    max_trade_time: 0,
                }],
                Some(notifier.clone()),
            ))
            .await
            .unwrap();
        n.await;

        // Check if stop-loss is executed
        assert!(
            open_orders.is_empty(),
            "take-profit order was not properly executed"
        );

        let order_history = simulated_account.get_order(&symbol, &os.order().id).await;
        assert!(
            order_history.contains(&os.order()),
            "Order history does not contain the take-profit order"
        );

        let position = simulated_account.get_position(&symbol).await;
        assert!(!position.is_open(), "Position is still open");
        Ok(())
    }
    #[tokio::test]
    async fn test_simulated_account_add_pending_order() -> anyhow::Result<()> {
        let symbol = Symbol {
            symbol: "TST/USDT".to_string(),
            exchange: ExchangeId::Simulated,
            base_asset_precision: 1,
            quote_asset_precision: 2,
        };
        let (simulated_account, tf_trades_channel, trades_channel, order_statuses_channel) =
            setup_simulated_account(symbol.clone()).await;

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

        let open_orders = simulated_account.get_open_orders(&symbol).await;
        assert_eq!(open_orders.len(), 1);

        let order_history = simulated_account
            .order_history
            .lock()
            .await
            .get(&symbol)
            .unwrap()
            .lock()
            .await
            .clone();

        let remaining_quantity = open_orders.iter().next().unwrap().quantity;
        assert_eq!(
            remaining_quantity,
            dec!(10.0),
            "Remaining quantity should be correct after partial fill"
        );
        assert!(open_orders.contains(&os.order()));

        assert!(
            order_history.contains(&os),
            "Order history does not contain the pending order"
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_simulated_account_fill_pending_order() -> anyhow::Result<()> {
        let symbol = Symbol {
            symbol: "TST/USDT".to_string(),
            exchange: ExchangeId::Simulated,
            base_asset_precision: 1,
            quote_asset_precision: 2,
        };
        let (simulated_account, tf_trades_channel, trades_channel, order_statuses_channel) =
            setup_simulated_account(symbol.clone()).await;

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
            id: uuid::Uuid::new_v4().to_string(),
            price: dec!(9.0),
            qty: dec!(100.0),
            timestamp: 0,
            delta: dec!(0.0),
            symbol: symbol.symbol.clone(),
        };
        tf_trades_channel
            .0
            .broadcast((
                vec![TfTrade {
                    symbol: symbol.clone(),
                    tf: 1,
                    id: uuid::Uuid::new_v4().to_string(),
                    timestamp: 124,
                    trades: vec![entry.clone()],
                    min_trade_time: 0,
                    max_trade_time: 0,
                }],
                Some(notifier.clone()),
            ))
            .await
            .unwrap();
        n.await;

        let open_orders = simulated_account.get_open_orders(&symbol).await;
        assert_eq!(open_orders.len(), 0);

        let order_history = simulated_account
            .order_history
            .lock()
            .await
            .get(&symbol)
            .unwrap()
            .lock()
            .await
            .clone();
        assert!(
            order_history.contains(&OrderStatus::Filled(os.order())),
            "Order history does not contain the filled order"
        );

        let position = simulated_account.get_position(&symbol).await;
        assert!(
            position.is_long(),
            "Position should be updated after order fill"
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_simulated_account_partial_fill_pending_order() -> anyhow::Result<()> {
        let symbol = Symbol {
            symbol: "TST/USDT".to_string(),
            exchange: ExchangeId::Simulated,
            base_asset_precision: 1,
            quote_asset_precision: 2,
        };
        let (simulated_account, tf_trades_channel, trades_channel, order_statuses_channel) =
            setup_simulated_account(symbol.clone()).await;

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
            id: uuid::Uuid::new_v4().to_string(),
            price: dec!(10.0),
            qty: dec!(90.0),
            timestamp: 0,
            delta: dec!(0.0),
            symbol: symbol.symbol.clone(),
        };
        tf_trades_channel
            .0
            .broadcast((
                vec![TfTrade {
                    symbol: symbol.clone(),
                    tf: 1,
                    id: uuid::Uuid::new_v4().to_string(),
                    timestamp: 124,
                    trades: vec![entry.clone()],
                    min_trade_time: 0,
                    max_trade_time: 0,
                }],
                Some(notifier.clone()),
            ))
            .await
            .unwrap();

        n.await;
        let order_history = simulated_account
            .order_history
            .lock()
            .await
            .get(&symbol)
            .unwrap()
            .lock()
            .await
            .clone();
        assert!(
            order_history.contains(&OrderStatus::PartiallyFilled(os.order(), dec!(90.0))),
            "Order history does not contain the partially filled order"
        );

        let open_orders = simulated_account.get_open_orders(&symbol).await;
        assert_eq!(open_orders.len(), 1);

        Ok(())
    }

    #[tokio::test]
    async fn test_simulated_account_emits_trade() -> anyhow::Result<()> {
        let symbol = Symbol {
            symbol: "TST/USDT".to_string(),
            exchange: ExchangeId::Simulated,
            base_asset_precision: 1,
            quote_asset_precision: 2,
        };
        let (simulated_account, tf_trades_channel, trades_channel, order_statuses_channel) =
            setup_simulated_account(symbol.clone()).await;

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
        n2.await;
        let notifier = Arc::new(Notify::new());
        let n = notifier.notified();
        let entry = TradeEntry {
            id: uuid::Uuid::new_v4().to_string(),
            price: dec!(10.0),
            qty: dec!(90.0),
            timestamp: 0,
            delta: dec!(0.0),
            symbol: symbol.symbol.clone(),
        };
        tf_trades_channel
            .0
            .broadcast((
                vec![TfTrade {
                    symbol: symbol.clone(),
                    tf: 1,
                    id: uuid::Uuid::new_v4().to_string(),
                    timestamp: 124,
                    trades: vec![entry.clone()],
                    min_trade_time: 0,
                    max_trade_time: 0,
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
