use async_trait::async_trait;
use crate::{EventSink, ExecutionCommand};

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
pub struct Order {
	pub id: String,
	pub symbol: String,
	pub side: String,
	pub price: f64,
	pub quantity: f64,
	pub status: String,
	pub time: u64,
	pub order_type: OrderType,
}

#[async_trait]
pub trait TradeExecutor: EventSink<ExecutionCommand> {
	const ID: ExchangeId;
	fn get_id(&self) -> ExchangeId {
		Self::ID
	}
	
	async fn execute_order(&self, order: Order) -> anyhow::Result<()>;
}