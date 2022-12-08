use std::collections::VecDeque;
use flurry::{HashMap, HashSet};
use std::sync::Arc;
use kanal::{AsyncReceiver, AsyncSender};
use rust_decimal::Decimal;
use tokio::sync::RwLock;
use async_trait::async_trait;
use tokio::task::JoinHandle;
use crate::{EventEmitter, EventSink, ExecutionCommand, TfTrades};
use crate::events::EventResult;
use crate::executors::{ExchangeAccount, ExchangeAccountInfo, ExchangeId, Order, OrderStatus, OrderType, Position, Side, Trade, TradeExecutor};
use crate::mongodb::models::TfTrade;
use crate::types::Symbol;

#[derive(Hash, Eq, PartialEq, Clone)]
pub struct SymbolAccount {
	pub symbol: Symbol,
	pub base_asset_free: Decimal,
	pub base_asset_locked: Decimal,
	pub quote_asset_free: Decimal,
	pub quote_asset_locked: Decimal,
}

pub type ArcMap<K, V> = Arc<HashMap<K, V>>;
pub type ArcSet<T> = Arc<HashSet<T>>;
pub struct SimulatedAccount {
	pub symbol_accounts: ArcMap<Symbol, SymbolAccount>,
	pub open_orders: ArcMap<Symbol, ArcSet<OrderStatus>>,
	pub filled_orders: ArcMap<Symbol, ArcSet<OrderStatus>>,
	pub trade_history: ArcMap<Symbol, ArcSet<Trade>>,
	trade_q: Arc<RwLock<VecDeque<Trade>>>,
	pub trade_subscribers: Arc<RwLock<Vec<AsyncSender<Trade>>>>,
	pub positions: ArcMap<Symbol, Arc<RwLock<Position>>>,
	tf_trades: Arc<AsyncReceiver<TfTrades>>,
	
}

pub struct SimulatedExecutor {
	pub account: Arc<SimulatedAccount>,
	pub orders: Arc<AsyncReceiver<Order>>,
}
impl SimulatedAccount {
	pub fn new(tf_trades: AsyncReceiver<TfTrades>) -> Self {
		Self {
			symbol_accounts: Arc::new(HashMap::new()),
			open_orders: Arc::new(HashMap::new()),
			filled_orders: Arc::new(HashMap::new()),
			trade_history:  Arc::new(HashMap::new()),
			positions:  Arc::new(HashMap::new()),
			tf_trades: Arc::new(tf_trades),
			trade_q: Arc::new(RwLock::new(VecDeque::new())),
			trade_subscribers: Arc::new(Vec::new()),
		}
	}
	
}

impl SimulatedExecutor{
	pub fn new(orders_rx: AsyncReceiver<Order>, trades_rx: AsyncReceiver<TfTrades>) -> Self {
		Self {
			account: Arc::new(SimulatedAccount::new(trades_rx)),
			orders: Arc::new(orders_rx),
		}
	}
	
	
}

#[async_trait]
impl EventEmitter<'_, Trade> for SimulatedAccount {
	fn get_subscribers(&self) -> Arc<RwLock<Vec<AsyncSender<Trade>>>> {
		self.trade_subscribers.clone()
	}
	
	async fn emit(&self) -> anyhow::Result<JoinHandle<()>> {
		let trade_q = self.trade_q.clone();
		Ok(tokio::spawn(async move {
			loop {
				let trade = trade_q.write().await.pop_front();
				if let Some(trade) = trade {
					let subscribers = self.trade_subscribers.read().await;
					for subscriber in subscribers {
						let _ = subscriber.send(trade.clone()).await;
					}
				}
			}
		}))
	}
}

// adjust to tf trade events, fill orders
#[async_trait]
impl EventSink<TfTrades> for SimulatedAccount {
	fn get_receiver(&self) -> Arc<AsyncReceiver<TfTrades>> {
		self.tf_trades.clone()
	}
	
	async fn handle_event(&self, event_msg: TfTrades) -> EventResult {
		let open_orders = self.open_orders.clone();
		Ok(tokio::spawn(async move {
			// if any open orders are fillable move them to filled orders and update position and push a trade event to trade queue if it is a order opposite to position
			for trade in event_msg {
			
			}
		}))
	}
}

#[async_trait]
impl EventSink<Order> for SimulatedExecutor {
	fn get_receiver(&self) -> Arc<AsyncReceiver<Order>> {
		self.orders.clone()
	}
	
	async fn handle_event(&self, event_msg: Order) -> EventResult {
		let res = self.execute_order(event_msg).await;
		Ok(tokio::spawn(async move {
			res
		}))
	}
}

#[async_trait]
impl EventEmitter<'_, OrderStatus> for SimulatedExecutor {
	fn get_subscribers(&self) -> Arc<RwLock<Vec<AsyncSender<OrderStatus>>>> {
		todo!()
	}
	
	async fn emit(&self) -> anyhow::Result<JoinHandle<()>> {
		todo!()
	}
}

#[async_trait]
impl ExchangeAccountInfo for SimulatedAccount {
	fn get_exchange_id(&self) -> ExchangeId {
		todo!()
	}
	
	async fn get_open_orders(&self, symbol: &Symbol) -> Arc<HashSet<OrderStatus>> {
		self.open_orders.pin().get(symbol).unwrap().clone()
	}
	
	async fn get_symbol_account(&self, symbol: &Symbol) -> SymbolAccount {
		self.symbol_accounts.pin().get(symbol).unwrap().clone()
	}
	
	
	async fn get_past_trades(&self, symbol: &Symbol, length: Option<usize>) -> Arc<HashSet<Trade>> {
		if let Some(length) = length {
			let guard = self.trade_history.guard();
			let mut trades_vec = self.trade_history.iter(&guard).collect::<Vec<&Trade>>();
			trades_vec.sort_by(|a, b| b.timestamp.cmp(&a.timestamp));
			trades_vec.truncate(length);
			let trades = trades_vec.into_iter().cloned().collect::<HashSet<Trade>>();
			Arc::new(trades)
		} else {
			self.trade_history.pin().get(symbol).unwrap().clone()
		}
	}
	
	async fn get_position(&self, symbol: &Symbol) -> Arc<Position> {
		let guard = self.positions.guard();
		let position_lock = self.positions.get(symbol, &guard).unwrap();
		let position = position_lock.read().await.clone();
		Arc::new(position)
	}
}


#[async_trait]
impl ExchangeAccount for SimulatedAccount {
	async fn limit_long(&self, order: Order) -> anyhow::Result<OrderStatus> {
		let guard = self.symbol_accounts.guard();
		let symbol_account = self.symbol_accounts.get(&order.symbol, &guard).unwrap();
		if symbol_account.base_asset_free < order.quantity {
			return Ok(OrderStatus::Canceled(order, "Insufficient balance".to_string()));
		}
		self.open_orders.get(&order.symbol, &guard).unwrap().pin().insert(OrderStatus::Pending(order.clone()));
		Ok(OrderStatus::Pending(order))
	}
	
	async fn limit_short(&self, order: Order) -> anyhow::Result<OrderStatus> {
		todo!()
	}
	
	async fn market_long(&self, order: Order) -> anyhow::Result<OrderStatus> {
		todo!()
	}
	
	async fn market_short(&self, order: Order) -> anyhow::Result<OrderStatus> {
		todo!()
	}
}

#[async_trait]
impl TradeExecutor for SimulatedExecutor {
	const ID: ExchangeId = ExchangeId::Simulated;
	type Account = SimulatedAccount;
	fn get_account(&self) -> &Self::Account {
		&self.account
	}
}