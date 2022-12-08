use flurry::{HashMap, HashSet};
use std::sync::Arc;
use kanal::{AsyncReceiver, AsyncSender};
use rust_decimal::Decimal;
use tokio::sync::RwLock;
use async_trait::async_trait;
use tokio::task::JoinHandle;
use crate::{EventEmitter, EventSink, ExecutionCommand};
use crate::events::EventResult;
use crate::executors::{ExchangeAccount, ExchangeAccountInfo, ExchangeId, Order, OrderStatus, OrderType, TradeExecutor};

#[derive(Hash, Eq, PartialEq, Clone)]
pub struct SymbolAccount {
	pub symbol: String,
	pub base_asset_precision: u32,
	pub quote_asset_precision: u32,
	pub base_asset_free: Decimal,
	pub base_asset_locked: Decimal,
	pub quote_asset_free: Decimal,
	pub quote_asset_locked: Decimal,
}

pub struct SimulatedAccount {
	pub symbol_accounts: HashMap<String, SymbolAccount>,
	pub open_orders: HashMap<String, HashSet<OrderStatus>>,
	pub filled_orders: HashMap<String, HashSet<OrderStatus>>,
}

pub struct SimulatedExecutor {
	pub account: Arc<SimulatedAccount>,
	pub orders: Arc<AsyncReceiver<Order>>,
}
impl SimulatedAccount {
	pub fn new() -> Self {
		Self {
			symbol_accounts: HashMap::new(),
			open_orders: HashMap::new(),
			filled_orders: HashMap::new(),
		}
	}
	
}

impl SimulatedExecutor{
	pub fn new(orders_rx: AsyncReceiver<Order>) -> Self {
		Self {
			account: Arc::new(SimulatedAccount::new()),
			orders: Arc::new(orders_rx),
		}
	}
	
	pub fn step(&self) {
	
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
	
	async fn get_open_orders(&self, symbol: String) -> Vec<OrderStatus> {
		todo!()
	}
	
	async fn get_symbol_account(&self, symbol: String) -> SymbolAccount {
		todo!()
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