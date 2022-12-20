pub mod simulated;
pub mod live;
pub mod notification;
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
	pub trade_id: u64,
}
impl Position {
	
	pub fn new(side: Side, symbol: Symbol, qty: Decimal, quote_qty: Decimal) -> Self {
		Self {
			side,
			symbol,
			qty,
			quote_qty,
			avg_price: Decimal::ZERO,
			trade_id: 0,
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
	pub fn increment_trade_id(&mut self) -> u64 {
		self.trade_id += 1;
		self.trade_id
	}
	pub fn apply_order(&mut self, order: &Order, time: u64) -> Option<Trade> {
		println!("[?] Delta Order: {} {:?} {:?}", order.symbol.symbol, order.order_type, order.side);
		println!("[?] Pre-position: {:?}", self);
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
						let trade = Trade {
							id: self.increment_trade_id(),
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
							time,
							exit_order_type: order.order_type.clone(),
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
						let trade = Trade {
							id: self.increment_trade_id(),
							order_id: order.id,
							symbol: order.symbol.clone(),
							maker: false,
							price: order.price,
							commission: Decimal::ZERO,
							position_side: Side::Ask,
							side: Side::Bid,
							realized_pnl: (prev_avg_price * order.quantity) - order.quantity * order.price,
							qty: order.quantity,
							quote_qty: order.quantity * order.price,
							time,
							exit_order_type: order.order_type.clone(),
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

#[derive(Clone, Debug, Default)]
pub struct Spread {
	pub symbol: Symbol,
	pub spread: Decimal,
	pub time: u64,
}

impl Spread {
	pub fn new(symbol: Symbol) -> Self {
		Self {
			symbol,
			spread: Decimal::ZERO,
			time: 0,
		}
	}
	
	pub fn update(&mut self, price: Decimal, time: u64) {
		self.spread = price;
		self.time = time;
	}
}
#[async_trait]
pub trait ExchangeAccountInfo: Send + Sync {
	fn get_exchange_id(&self) -> ExchangeId;
	async fn get_open_orders(&self, symbol: &Symbol) -> Arc<HashSet<OrderStatus>>;
	async fn get_symbol_account(&self, symbol: &Symbol) -> SymbolAccount;
	async fn get_past_trades(&self, symbol: &Symbol, length: Option<usize>) -> Arc<HashSet<Trade>>;
	async fn get_position(&self, symbol: &Symbol) -> Arc<Position>;
	async fn get_spread(&self, symbol: &Symbol) -> Arc<Spread>;
	async fn get_order(&self,  symbol: &Symbol, id: &uuid::Uuid) -> Vec<Order>;
}

#[async_trait]
pub trait ExchangeAccount: ExchangeAccountInfo {
	async fn limit_long(&self, order: Order) -> anyhow::Result<OrderStatus>;
	async fn limit_short(&self, order: Order) -> anyhow::Result<OrderStatus>;
	async fn market_long(&self, order: Order) -> anyhow::Result<OrderStatus>;
	async fn market_short(&self, order: Order) -> anyhow::Result<OrderStatus>;
	async fn cancel_order(&self, order: Order) -> anyhow::Result<OrderStatus>;
}


pub trait TradeExecutor: EventSink<Order> + EventEmitter<OrderStatus> {
	type Account: ExchangeAccount + 'static;
	fn get_account(&self) -> Arc<Self::Account>;
	
	fn process_order(&self, order: Order) -> anyhow::Result<OrderStatus> {
		let account = self.get_account();
		std::thread::spawn(move || {
			let rt = tokio::runtime::Runtime::new().unwrap();
			match order.order_type {
				OrderType::Limit | OrderType::TakeProfit(_) | OrderType::StopLoss(_) | OrderType::StopLossTrailing(_, _) => {
					match order.side {
						Side::Bid => {
								rt.block_on(account.limit_long(order))
						},
						Side::Ask => {
								rt.block_on(account.limit_short(order))
						}
					}
				},
				OrderType::Cancel(_) | OrderType::CancelFor(_) => {
						rt.block_on(account.cancel_order(order))
				},
				OrderType::Market => {
					match order.side {
						Side::Bid => {
								rt.block_on(account.market_long(order))
						},
						Side::Ask => {
								rt.block_on(account.market_short(order))
						}
					}
				},
				_ => {
					todo!()
				}
			}
		}).join().unwrap()
		
	}
}