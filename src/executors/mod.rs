pub mod simulated;

use async_std::sync::Arc;
use async_trait::async_trait;
use flurry::HashSet;
use rust_decimal::Decimal;
use crate::{EventEmitter, EventSink, ExecutionCommand};
use crate::executors::simulated::{SymbolAccount};
use crate::types::Symbol;

pub enum ExchangeId {
	Simulated
}
#[derive(Hash, Eq,Ord, PartialOrd, PartialEq, Clone)]
pub struct Position {
	pub side: Side,
	pub symbol: Symbol,
	pub qty: Decimal,
	pub quote_qty: Decimal
}
#[derive(Hash, Eq,Ord, PartialOrd, PartialEq, Clone)]
pub enum OrderType {
	Limit,
	Market,
	StopLoss,
	StopLossLimit,
	TakeProfit,
	TakeProfitLimit,
	StopLossTrailing,
}
#[derive(Hash, Eq,Ord, PartialOrd, PartialEq, Clone)]
pub enum Side {
	Buy,
	Sell,
}
#[derive(Hash, Eq, Ord, PartialOrd, PartialEq, Clone)]
pub enum OrderStatus {
	Pending(Order),
	Filled(Order),
	Canceled(Order, String),
}
#[derive(Clone,Hash, Eq,Ord, PartialOrd, PartialEq)]
pub struct Trade {
	pub id: u64,
	pub order_id: u64,
	pub symbol: Symbol,
	pub maker: bool,
	pub price: Decimal,
	pub commission: Decimal,
	pub position_side: Side,
	pub side: Side,
	pub realized_pnl: Decimal,
	pub qty: Decimal,
	pub quote_qty: Decimal,
	pub time: u64,
}

#[derive(Clone,Hash, Eq,Ord, PartialOrd, PartialEq)]
pub struct Order {
	pub id: u64,
	pub symbol: Symbol,
	pub side: Side,
	pub price: Decimal,
	pub quantity: Decimal,
	pub time: u64,
	pub order_type: OrderType,
}

#[async_trait]
pub trait ExchangeAccountInfo: Send + Sync {
	fn get_exchange_id(&self) -> ExchangeId;
	async fn get_open_orders(&self, symbol: &Symbol) -> Arc<HashSet<OrderStatus>>;
	async fn get_symbol_account(&self, symbol: &Symbol) -> SymbolAccount;
	async fn get_past_trades(&self, symbol: &Symbol, length: Option<usize>) -> Arc<HashSet<Trade>>;
	async fn get_position(&self, symbol: &Symbol) -> Arc<Position>;
}

#[async_trait]
pub trait ExchangeAccount: ExchangeAccountInfo {
	async fn limit_long(&self, order: Order) -> anyhow::Result<OrderStatus>;
	async fn limit_short(&self, order: Order) -> anyhow::Result<OrderStatus>;
	async fn market_long(&self, order: Order) -> anyhow::Result<OrderStatus>;
	async fn market_short(&self, order: Order) -> anyhow::Result<OrderStatus>;
}


#[async_trait]
pub trait TradeExecutor: EventSink<Order> + for <'a> EventEmitter<'a,OrderStatus> {
	const ID: ExchangeId;
	type Account: ExchangeAccount;
	fn get_id(&self) -> ExchangeId {
		Self::ID
	}
	fn get_account(&self) -> &Self::Account;
	
	async fn execute_order(&self, order: Order) -> anyhow::Result<()> {
		match order.order_type {
			OrderType::Limit => {
				match order.side {
					Side::Buy => {
						self.get_account().limit_long(order).await;
					},
					Side::Sell => {
						self.get_account().limit_short(order).await;
					}
				}
			},
			OrderType::Market => {
				match order.side {
					Side::Buy => {
						self.get_account().market_long(order).await;
					},
					Side::Sell => {
						self.get_account().market_short(order).await;
					}
				}
			},
			_ => {
				todo!()
			}
		}
		Ok(())
	}
}