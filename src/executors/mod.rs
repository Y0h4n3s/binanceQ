mod simulated;

use async_trait::async_trait;
use crate::{EventEmitter, EventSink, ExecutionCommand};

pub enum ExchangeId {
	Simulated
}

pub enum OrderType {
	Limit,
	Market,
	StopLoss,
	StopLossLimit,
	TakeProfit,
	TakeProfitLimit,
	StopLossTrailing,
}

pub enum OrderStatus {
	Pending,
	Filled,
	Canceled,
}
pub struct Order {
	pub id: String,
	pub symbol: String,
	pub side: String,
	pub price: f64,
	pub quantity: f64,
	pub status: OrderStatus,
	pub time: u64,
	pub order_type: OrderType,
}

#[async_trait]
pub trait TradeExecutor: EventSink<ExecutionCommand> + EventEmitter<OrderStatus> {
	const ID: ExchangeId;
	fn get_id(&self) -> ExchangeId {
		Self::ID
	}
	
	async fn execute_order(&self, order: Order) -> anyhow::Result<()>;
}