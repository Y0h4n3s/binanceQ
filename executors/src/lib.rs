pub mod back_tester;
mod simulated;


use async_std::sync::Arc;
use async_trait::async_trait;
use flurry::HashSet;
use rust_decimal::Decimal;
use binance_q_types::{ExchangeId, Order, OrderStatus, OrderType, Side, Symbol, SymbolAccount, Trade};

#[derive(Hash, Eq,Ord, PartialOrd, PartialEq, Clone)]
pub struct Position {
	pub side: Side,
	pub symbol: Symbol,
	pub qty: Decimal,
	pub quote_qty: Decimal
}
impl Position {
	pub fn new(side: Side, symbol: Symbol, qty: Decimal, quote_qty: Decimal) -> Self {
		Self {
			side,
			symbol,
			qty,
			quote_qty
		}
	}
	pub fn apply_order(&mut self, order: &Order) -> Option<Trade> {
		// TODO
		None
	}
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
	
	async fn execute_order(&self, order: Order) -> anyhow::Result<OrderStatus> {
		match order.order_type {
			OrderType::Limit => {
				match order.side {
					Side::Bid => {
						self.get_account().limit_long(order).await
					},
					Side::Ask => {
						self.get_account().limit_short(order).await
					}
				}
			},
			OrderType::Market => {
				match order.side {
					Side::Bid => {
						self.get_account().market_long(order).await
					},
					Side::Ask => {
						self.get_account().market_short(order).await
					}
				}
			},
			_ => {
				todo!()
			}
		}
	}
}