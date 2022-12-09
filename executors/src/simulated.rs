use std::collections::VecDeque;
use flurry::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use kanal::{AsyncReceiver, AsyncSender};
use rust_decimal::Decimal;
use tokio::sync::RwLock;
use async_trait::async_trait;
use rust_decimal::prelude::ToPrimitive;
use tokio::task::JoinHandle;
use binance_q_types::{Order, Symbol, OrderStatus, SymbolAccount, TfTrades, Trade, ExchangeId, Side};
use crate::{ExchangeAccount, ExchangeAccountInfo, Position, TradeExecutor};


pub type ArcMap<K, V> = Arc<HashMap<K, V>>;
pub type ArcSet<T> = Arc<HashSet<T>>;
pub struct SimulatedAccount {
	pub symbol_accounts: ArcMap<Symbol, SymbolAccount>,
	pub open_orders: ArcMap<Symbol, ArcSet<OrderStatus>>,
	pub order_history: ArcMap<Symbol, ArcSet<OrderStatus>>,
	pub trade_history: ArcMap<Symbol, ArcSet<Trade>>,
	trade_q: Arc<RwLock<VecDeque<Trade>>>,
	pub trade_subscribers: Arc<RwLock<Vec<AsyncSender<Trade>>>>,
	pub positions: ArcMap<Symbol, Arc<RwLock<Position>>>,
	tf_trades: Arc<AsyncReceiver<TfTrades>>,
	order_statuses: Arc<AsyncReceiver<OrderStatus>>,
	
}

pub struct SimulatedExecutor {
	pub account: Arc<SimulatedAccount>,
	pub orders: Arc<AsyncReceiver<Order>>,
	order_status_q: Arc<RwLock<VecDeque<OrderStatus>>>,
pub order_status_subscribers: Arc<RwLock<Vec<AsyncSender<OrderStatus>>>>,
}
impl SimulatedAccount {
	pub fn new(tf_trades: AsyncReceiver<TfTrades>, order_statuses: AsyncReceiver<OrderStatus>) -> Self {
		Self {
			symbol_accounts: Arc::new(HashMap::new()),
			open_orders: Arc::new(HashMap::new()),
			order_history: Arc::new(HashMap::new()),
			trade_history:  Arc::new(HashMap::new()),
			positions:  Arc::new(HashMap::new()),
			tf_trades: Arc::new(tf_trades),
			trade_q: Arc::new(RwLock::new(VecDeque::new())),
			trade_subscribers: Arc::new(RwLock::new(Vec::new())),
			order_statuses: Arc::new(order_statuses),
		}
	}
	
}

impl SimulatedExecutor{
	pub fn new(orders_rx: AsyncReceiver<Order>, trades_rx: AsyncReceiver<TfTrades>) -> Self {
		let order_statuses_channel = kanal::bounded_async(100);
		
		Self {
			account: Arc::new(SimulatedAccount::new(trades_rx, order_statuses_channel.1)),
			orders: Arc::new(orders_rx),
			order_status_q: Arc::new(RwLock::new(VecDeque::new())),
			order_status_subscribers: Arc::new(RwLock::new(vec![order_statuses_channel.0])),
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
		let subscribers = self.trade_subscribers.clone();
		let trade_history = self.trade_history.clone();
		Ok(tokio::spawn(async move {
			loop {
				let mut w = trade_q.write().await;
				let trade = w.pop_front();
				std::mem::drop(w);
				if let Some(trade) = trade {
					trade_history.pin().get(&trade.symbol).pin().insert(trade.clone());
					let subs = subscribers.read().await;
					for subscriber in subs {
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
		let trade_q = self.trade_q.clone();
		let filled_orders = self.order_history.clone();
		let positions = self.positions.clone();
		Ok(tokio::spawn(async move {
			// if any open orders are fillable move them to filled orders and update position and push a trade event to trade queue if it is a order opposite to position
			for tf_trade in event_msg {
				let symbol = tf_trade.symbol.clone();
				let open_orders = open_orders.pin().get(&symbol).unwrap().clone();
				let mut filled_orders = filled_orders.pin().get(&symbol).pin();
				for trade in tf_trade.trades {
					let guard = open_orders.guard();
					for order in open_orders.iter(&guard) {
						match order {
							// could potentially match on qty here and update order status to partially filled
							OrderStatus::Pending(order) => {
								if order.side == Side::Buy &&  order.price.gt(trade.price.into()) {
									let mut position = positions.pin().get(&symbol).pin();
									let mut w = position.write().await;
									if let Some(trade) = w.apply_order(order) {
										trades.push(trade.clone());
										let mut trade_q = trade_q.write().await;
										trade_q.push_back(trade);
										
									}
									filled_orders.insert(OrderStatus::Filled(order.clone()));
								} else if order.side == Side::Sell && order.price.lt(trade.price.into()) {
									let mut position = positions.pin().get(&symbol).pin();
									let mut w = position.write().await;
									if let Some(trade) = w.apply_order(order) {
										trades.push(trade.clone());
										let mut trade_q = trade_q.write().await;
										trade_q.push_back(trade);
										
									}
									filled_orders.insert(OrderStatus::Filled(order.clone()));
								}
							}
							_ => {}
						}
					}
				}
				
			}
		}))
	}
}

#[async_trait]
impl EventSink<OrderStatus> for SimulatedAccount {
	fn get_receiver(&self) -> Arc<AsyncReceiver<OrderStatus>> {
		self.order_statuses.clone()
	}
	
	async fn handle_event(&self, event_msg: OrderStatus) -> EventResult {
		let open_orders = self.open_orders.clone();
		let order_history = self.order_history.clone();
		let trade_q = self.trade_q.clone();
		let positions = self.positions.clone();
		
		Ok(tokio::spawn(async move {
			match event_msg {
				OrderStatus::Pending(order) => {
					open_orders.pin().get(&order.symbol).pin().insert(order.clone());
				
				}
				// if the order was a market order it is immediately filled so  move it to order history and adjust position
				OrderStatus::Filled(order) => {
					order_history.pin().get(&order.symbol).pin().insert(order.clone());
					// adjust position
					let position = positions.pin().get(&order.symbol).pin();
					let mut position = position.write().await;
					if let Some(trade) = position.apply_order(order) {
							trade_q.write().await.push_back(trade);
					}
					
				}
				OrderStatus::Canceled(order, reason) => {
					eprintln!("order canceled reason: {:?}", reason);
					order_history.pin().get(&order.symbol).pin().insert(order);
				}
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
		let order_status_q = self.order_status_q.clone();
		Ok(tokio::spawn(async move {
			if let Ok(res) = res {
				order_status_q.write().await.push_back(res);
			}
		}))
	}
}

#[async_trait]
impl EventEmitter<'_, OrderStatus> for SimulatedExecutor {
	fn get_subscribers(&self) -> Arc<RwLock<Vec<AsyncSender<OrderStatus>>>> {
		self.order_status_subscribers.clone()
	}
	
	async fn emit(&self) -> anyhow::Result<JoinHandle<()>> {
		let q = self.order_status_q.clone();
		Ok(tokio::spawn(async move {
			loop {
				let mut w = q.write().await;
				let order_status = w.pop_front();
				std::mem::drop(w);
				if let Some(order_status) = order_status {
					let subs = self.order_status_subscribers.read().await;
					for subscriber in subs {
						let _ = subscriber.send(order_status.clone()).await;
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