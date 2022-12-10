pub mod simulated;


use std::collections::HashSet;
use std::ops::Mul;
use async_std::sync::Arc;
use async_trait::async_trait;
use rust_decimal::Decimal;
use binance_q_types::{ExchangeId, Order, OrderStatus, OrderType, Side, Symbol, SymbolAccount, Trade};
use binance_q_events::{EventEmitter, EventSink};

#[derive(Hash, Eq,Ord, PartialOrd, PartialEq, Clone, Debug)]
pub struct Position {
	pub side: Side,
	pub symbol: Symbol,
	pub qty: Decimal,
	pub quote_qty: Decimal,
	pub avg_price: Decimal,
}
impl Position {
	
	pub fn new(side: Side, symbol: Symbol, qty: Decimal, quote_qty: Decimal) -> Self {
		Self {
			side,
			symbol,
			qty,
			quote_qty,
			avg_price: Decimal::ZERO,
		}
	}
	
	pub fn is_long(&self) -> bool {
		self.side == Side::Bid
	}
	pub fn is_short(&self) -> bool {
		self.side == Side::Ask
	}
	pub fn is_open(&self) -> bool {
		self.qty > Decimal::ZERO
	}
	pub fn apply_order(&mut self, order: &Order) -> Option<Trade> {
		if !self.is_open() {
			self.qty = order.quantity;
			self.quote_qty = order.quantity * order.price;
			self.side = order.side.clone();
			self.avg_price = order.price;
			None
		} else {
			let prev_avg_price = self.avg_price;
			self.avg_price = (self.avg_price.mul(self.qty) + order.price.mul(order.quantity)) / (self.qty + order.quantity);
			match order.side {
				Side::Ask => {
					if self.side == Side::Ask {
						self.qty += order.quantity;
						self.quote_qty += order.quantity * order.price;
						None
					} else {
						let mut trade = Trade {
							id: 1,
							order_id: order.id,
							symbol: order.symbol.clone(),
							maker: false,
							price: order.price,
							commission: Decimal::ZERO,
							position_side: Side::Ask,
							side: Side::Bid,
							realized_pnl: order.quantity * order.price - (prev_avg_price * order.quantity),
							qty: order.quantity,
							quote_qty: order.quantity * order.price,
							time: order.time
						};
						if self.qty >= order.quantity  {
							self.qty -= order.quantity;
							self.quote_qty -= order.quantity * order.price;
						} else {
							self.qty = order.quantity  - self.qty;
							self.quote_qty = order.quantity * order.price - self.quote_qty;
							self.side = Side::Ask;
						}
						Some(trade)
					}
				},
				Side::Bid => {
					if self.side == Side::Bid {
						self.qty+= order.quantity;
						self.quote_qty += order.quantity * order.price;
						None
					} else {
						let mut trade = Trade {
							id: 1,
							order_id: order.id,
							symbol: order.symbol.clone(),
							maker: false,
							price: order.price,
							commission: Decimal::ZERO,
							position_side: Side::Bid,
							side: Side::Ask,
							realized_pnl: order.quantity * order.price - (prev_avg_price * order.quantity),
							qty: order.quantity,
							quote_qty: order.quantity * order.price,
							time: order.time
						};
						if self.qty > order.quantity {
							self.qty -= order.quantity;
							self.quote_qty -= order.quantity * order.price;
						} else {
							self.qty = order.quantity - self.qty;
							self.quote_qty = order.quantity * order.price - self.quote_qty;
							self.side = Side::Bid;
						}
						Some(trade)
					}
				}
			}
		}
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


pub trait TradeExecutor: EventSink<Order> + for <'a> EventEmitter<'a,OrderStatus> {
	const ID: ExchangeId;
	type Account: ExchangeAccount;
	fn get_id(&self) -> ExchangeId {
		Self::ID
	}
	fn get_account(&self) -> &Self::Account;
	
	fn execute_order(&self, order: Order) -> anyhow::Result<OrderStatus> {
		let handle = tokio::runtime::Handle::try_current()?;
		match order.order_type {
			OrderType::Limit => {
				match order.side {
					Side::Bid => {
						handle.block_on(self.get_account().limit_long(order))
					},
					Side::Ask => {
						handle.block_on(self.get_account().limit_short(order))
					}
				}
			},
			OrderType::Market => {
				match order.side {
					Side::Bid => {
						handle.block_on(self.get_account().market_long(order))
					},
					Side::Ask => {
						handle.block_on(self.get_account().market_short(order))
					}
				}
			},
			_ => {
				todo!()
			}
		}
	}
}