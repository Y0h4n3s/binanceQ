pub mod simulated;

use async_trait::async_trait;
use rust_decimal::Decimal;
use crate::{EventEmitter, EventSink, ExecutionCommand};
use crate::executors::simulated::SymbolAccount;

pub enum ExchangeId {
	Simulated
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
pub struct Order {
	pub id: String,
	pub symbol: String,
	pub side: Side,
	pub price: Decimal,
	pub quantity: Decimal,
	pub time: u64,
	pub order_type: OrderType,
}

#[async_trait]
pub trait ExchangeAccountInfo: Send + Sync {
	fn get_exchange_id(&self) -> ExchangeId;
	async fn get_open_orders(&self, symbol: String) -> Vec<OrderStatus>;
	async fn get_symbol_account(&self, symbol: String) -> SymbolAccount;
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