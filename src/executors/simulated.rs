use crate::events::EventSink;
use crate::executors::{broadcast, broadcast_and_wait, ExchangeAccount, ExchangeAccountInfo, Position, Spread, TradeExecutor};
use crate::mongodb::MongoClient;
use crate::types::{ExchangeId, Order, OrderStatus, OrderType, Side, Symbol, SymbolAccount, TfTrades, Trade, TradeEntry};
use async_broadcast::{InactiveReceiver, Receiver, Sender};
use async_trait::async_trait;
use dashmap::{DashMap, DashSet};
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
use crate::db::client::SQLiteClient;
use crate::executors::order_state_machine::OrderStateMachine;

// can't use dashmap because of https://github.com/xacrimon/dashmap/issues/79
type ArcMap<K, V> = Arc<Mutex<HashMap<K, V>>>;
type ArcSet<T> = Arc<Mutex<HashSet<T>>>;



#[derive(Clone)]
pub struct SimulatedAccount {
    pub symbol_accounts: ArcMap<Symbol, SymbolAccount>,
    pub open_orders: ArcMap<Symbol, ArcSet<Order>>,
    pub order_history: ArcMap<Symbol, ArcSet<OrderStatus>>,
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
    pub async fn new(
        tf_trades: InactiveReceiver<(TfTrades, Option<Arc<Notify>>)>,
        order_statuses: InactiveReceiver<(OrderStatus, Option<Arc<Notify>>)>,
        trades: InactiveReceiver<(Trade, Option<Arc<Notify>>)>,
        symbols: Vec<Symbol>,
        order_status_subscribers: Sender<(OrderStatus, Option<Arc<Notify>>)>,
        trade_subscribers: Sender<(Trade, Option<Arc<Notify>>)>,
        sqlite_client: Arc<SQLiteClient>
    ) -> Self {
        let symbol_accounts = Arc::new(Mutex::new(HashMap::new()));
        let open_orders = Arc::new(Mutex::new(HashMap::new()));
        let order_history = Arc::new(Mutex::new(HashMap::new()));
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
            symbol_accounts.lock().await.insert(symbol.clone(), symbol_account);
            open_orders.lock().await.insert(symbol.clone(), Arc::new(Mutex::new(HashSet::new())));
            order_history.lock().await.insert(symbol.clone(), Arc::new(Mutex::new(HashSet::new())));
            trade_history.lock().await.insert(symbol.clone(), Arc::new(Mutex::new(HashSet::new())));
            positions.lock().await.insert(symbol.clone(), position);
            spreads.lock().await.insert(symbol.clone(), Arc::new(RwLock::new(spread)));
        }
        Self {
            symbol_accounts,
            open_orders,
            order_history,
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
            let mut open_orders = open_orders.lock().await.get(&symbol).unwrap().lock().await.clone();

            if open_orders.is_empty() {
                continue;
            }

            let mut order_search: HashMap<Uuid, Order> = open_orders.iter()
                .map(|o| (o.id, o.clone()))
                .collect();

            // let mut remove_orders: Vec<OrderStatus> = vec![];
            // let mut remove_for_orders = vec![];
            // let mut re_add = vec![];
            let positions = self.positions.clone();
            for trade in &tf_trade.trades {
                // handle cancel orders first
                for mut order in open_orders.clone() {
                        let mut state_machine = OrderStateMachine::new(
                            &mut order,
                            &self.positions,
                            &mut open_orders,
                            &self.trade_subscribers,
                            &self.order_status_subscribers,
                        );
                        state_machine.process_order(trade).await;
                }

                if open_orders.is_empty() {
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

    async fn handle_event(&self, event_msg: Trade) -> anyhow::Result<()> {
        let sqlite_client = &self.sqlite_client;
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
    async fn handle_event(&self, event_msg: OrderStatus) -> anyhow::Result<()> {
        let open_orders = self.open_orders.clone();
        match &event_msg {
            // add to order set
            OrderStatus::Pending(order) => {
                trace!("Adding pending order to order set: {:?}", order);
                if let Some(orders) = open_orders.lock().await.get(&order.symbol) {
                    orders.lock().await.insert(order.clone());
                } else {
                    let mut new_set = Arc::new(Mutex::new(HashSet::new()));
                    new_set.lock().await.insert(order.clone());
                    open_orders.lock().await.insert(order.symbol.clone(), new_set);
                }

            }
            // valid types are limit
            OrderStatus::PartiallyFilled(order, delta) => {
                let mut new_order = order.clone();
                new_order.quantity = order.quantity - delta;
                new_order.id = Uuid::new_v4();
                SQLiteClient::insert_order(&self.sqlite_client.pool, order.clone()).await;

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
                    open_orders.lock().await.insert(order.symbol.clone(), new_set);
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
        Arc::new(self.open_orders.lock().await.get(symbol).unwrap().lock().await.clone())
    }

    async fn get_symbol_account(&self, symbol: &Symbol) -> SymbolAccount {
        self.symbol_accounts.lock().await.get(symbol).unwrap().clone()
    }

    async fn get_past_trades(&self, symbol: &Symbol, length: Option<usize>) -> Arc<HashSet<Trade>> {
            Arc::new(self.trade_history.lock().await.get(symbol).unwrap().lock().await.clone())
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

    async fn get_order(&self, symbol: &Symbol, id: &Uuid) -> Vec<Order> {
        let order_history = self.order_history.lock().await.get(symbol).unwrap().clone();
        let mut res = vec![];
        order_history
            .lock().await.iter().for_each(|o| match &*o {
                OrderStatus::Filled(or) => {
                    if &or.id == id {
                        res.push(or.clone())
                    }
                }
                _ => {},
            })
            ;
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

    async fn setup_simulated_account(symbol: Symbol) -> (
        Arc<SimulatedAccount>,
        async_broadcast::Sender<(TfTrades, Option<Arc<Notify>>)>,
        async_broadcast::Sender<(Trade, Option<Arc<Notify>>)>,
        async_broadcast::Sender<(OrderStatus, Option<Arc<Notify>>)>,
    ) {
        let tf_trades_channel = async_broadcast::broadcast(100);
        let trades_channel = async_broadcast::broadcast(100);
        let order_statuses_channel = async_broadcast::broadcast(100);
        let sqlite_client = Arc::new(SQLiteClient::new().await);

        let simulated_account = Arc::new(
            SimulatedAccount::new(
                tf_trades_channel.1.deactivate(),
                order_statuses_channel.1.deactivate(),
                trades_channel.1.deactivate(),
                vec![symbol.clone()],
                order_statuses_channel.0.clone(),
                trades_channel.0.clone(),
                sqlite_client,
            )
            .await,
        );

        let sa_clone = simulated_account.clone();
        tokio::spawn(async move {
            EventSink::<OrderStatus>::listen(sa_clone.clone()).unwrap();
        });
        tokio::spawn(async move {
            EventSink::<Trade>::listen(sa_clone.clone()).unwrap();
        });
        tokio::spawn(async move {
            EventSink::<TfTrade>::listen(sa_clone.clone()).unwrap();
        });

        (
            simulated_account,
            tf_trades_channel.0,
            trades_channel.0,
            order_statuses_channel.0,
        )
    }
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
        Ok(())
    }

    #[tokio::test]
    async fn test_simulated_account_apply_stop_loss() -> anyhow::Result<()> {
        let tf_trades_channel = async_broadcast::broadcast(100);
        let trades_channel = async_broadcast::broadcast(100);
        let order_statuses_channel = async_broadcast::broadcast(100);
        let sqlite_client = Arc::new(SQLiteClient::new().await);
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
                trades_channel.1.deactivate(),
                vec![symbol.clone()],
                order_statuses_channel.0.clone(),
                trades_channel.0.clone(),
                sqlite_client
            )
                .await,
        );

        let sa1 = simulated_account.clone();
        let sa2 = simulated_account.clone();
        tokio::spawn(async move {
            EventSink::<OrderStatus>::listen(simulated_account.clone()).unwrap();
        });
        tokio::spawn(async move {
            EventSink::<Trade>::listen(simulated_account.clone()).unwrap();
        });

        tokio::spawn(async move {
            EventSink::<TfTrade>::listen(simulated_account.clone()).unwrap();
        });

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
        let position = simulated_account.get_position(&symbol).await;
        assert!(!position.is_open(), "Position is still open");
        Ok(())
    }
    #[tokio::test]
    async fn test_simulated_account_apply_take_profit() -> anyhow::Result<()> {
        let tf_trades_channel = async_broadcast::broadcast(100);
        let trades_channel = async_broadcast::broadcast(100);
        let order_statuses_channel = async_broadcast::broadcast(100);
        let sqlite_client = Arc::new(SQLiteClient::new().await);
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
                trades_channel.1.deactivate(),
                vec![symbol.clone()],
                order_statuses_channel.0.clone(),
                trades_channel.0.clone(),
                sqlite_client
            )
                .await,
        );

        let sa1 = simulated_account.clone();
        let sa2 = simulated_account.clone();
        tokio::spawn(async move {
            EventSink::<OrderStatus>::listen(simulated_account.clone()).unwrap();
        });
        tokio::spawn(async move {
            EventSink::<Trade>::listen(simulated_account.clone()).unwrap();
        });

        tokio::spawn(async move {
            EventSink::<TfTrade>::listen(simulated_account.clone()).unwrap();
        });

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
        let position = simulated_account.get_position(&symbol).await;
        assert!(!position.is_open(), "Position is still open");
        Ok(())
    }
    #[tokio::test]
    async fn test_simulated_account_add_pending_order() -> anyhow::Result<()> {
        let tf_trades_channel = async_broadcast::broadcast(100);
        let trades_channel = async_broadcast::broadcast(100);
        let order_statuses_channel = async_broadcast::broadcast(100);
        let sqlite_client = Arc::new(SQLiteClient::new().await);
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
                trades_channel.1.deactivate(),
                vec![symbol.clone()],
                order_statuses_channel.0.clone(),
                trades_channel.0.clone(),
                sqlite_client
            )
                .await,
        );
        let s_a = simulated_account.clone();
        tokio::spawn(async move {
            EventSink::<OrderStatus>::listen(simulated_account.clone()).unwrap();
        });
        tokio::spawn(async move {
            EventSink::<Trade>::listen(simulated_account.clone()).unwrap();
        });

        tokio::spawn(async move {
            EventSink::<TfTrade>::listen(simulated_account.clone()).unwrap();
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
        let trades_channel = async_broadcast::broadcast(100);
        let order_statuses_channel = async_broadcast::broadcast(100);
        let sqlite_client = Arc::new(SQLiteClient::new().await);
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
                trades_channel.1.deactivate(),
                vec![symbol.clone()],
                order_statuses_channel.0.clone(),
                trades_channel.0.clone(),
                sqlite_client
            )
                .await,
        );
        let s_a1 = simulated_account.clone();
        let s_a = simulated_account.clone();
        tokio::spawn(async move {
            EventSink::<OrderStatus>::listen(simulated_account.clone()).unwrap();
        });
        tokio::spawn(async move {
            EventSink::<Trade>::listen(simulated_account.clone()).unwrap();
        });

        tokio::spawn(async move {
            EventSink::<TfTrade>::listen(simulated_account.clone()).unwrap();
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
                    min_trade_time: 0,
                    max_trade_time: 0,
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
        let trades_channel = async_broadcast::broadcast(100);
        let order_statuses_channel = async_broadcast::broadcast(100);
        let sqlite_client = Arc::new(SQLiteClient::new().await);
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
                trades_channel.1.deactivate(),
                vec![symbol.clone()],
                order_statuses_channel.0.clone(),
                trades_channel.0.clone(),
                sqlite_client
            )
                .await,
        );
        let s_a1 = simulated_account.clone();
        let s_a = simulated_account.clone();
        tokio::spawn(async move {
            EventSink::<OrderStatus>::listen(simulated_account.clone()).unwrap();
        });
        tokio::spawn(async move {
            EventSink::<Trade>::listen(simulated_account.clone()).unwrap();
        });

        tokio::spawn(async move {
            EventSink::<TfTrade>::listen(simulated_account.clone()).unwrap();
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
                    min_trade_time: 0,
                    max_trade_time: 0,
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
        let trades_channel = async_broadcast::broadcast(100);
        let order_statuses_channel = async_broadcast::broadcast(100);
        let sqlite_client = Arc::new(SQLiteClient::new().await);
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
                trades_channel.1.deactivate(),
                vec![symbol.clone()],
                order_statuses_channel.0.clone(),
                trades_channel.0.clone(),
                sqlite_client
            )
                .await,
        );
        let simulated_account = simulated_account;
        let s_a1 = simulated_account.clone();
        let s_a = simulated_account.clone();
        tokio::spawn(async move {
            EventSink::<OrderStatus>::listen(simulated_account.clone()).unwrap();
        });
        tokio::spawn(async move {
            EventSink::<Trade>::listen(simulated_account.clone()).unwrap();
        });

        tokio::spawn(async move {
            EventSink::<TfTrade>::listen(simulated_account.clone()).unwrap();
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
