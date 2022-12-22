use crate::{ExchangeAccount, ExchangeAccountInfo, Position, Spread, TradeExecutor};
use async_broadcast::{Receiver, Sender};
use async_std::io::WriteExt;
use async_trait::async_trait;
use binance_q_events::{EventEmitter, EventSink};
use binance_q_mongodb::client::MongoClient;
use binance_q_types::{AccessKey, ExchangeId, Order, OrderStatus, OrderType, Side, Symbol, SymbolAccount, TfTrades, Trade};
use std::cmp::max;
use rust_decimal::prelude::{FromPrimitive, ToPrimitive};
use rust_decimal::Decimal;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::Duration;
use binance::account::OrderSide;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use binance::api::Binance;
use headless_chrome::{Browser, protocol::page::ScreenshotFormat};

use binance::futures::account::{CustomOrderRequest, FuturesAccount, TimeInForce};
use binance::futures::market::FuturesMarket;
use uuid::Uuid;
use crate::notification::{Notification, Notifier, TelegramNotifier};

type ArcMap<K, V> = Arc<RwLock<HashMap<K, V>>>;
type ArcSet<T> = Arc<RwLock<HashSet<T>>>;

#[derive(Clone)]
pub struct BinanceLiveAccount {
	pub account: Arc<RwLock<FuturesAccount>>,
	pub market: Arc<RwLock<FuturesMarket>>,
	pub notifier: Arc<RwLock<TelegramNotifier>>,
	pub symbol_accounts: ArcMap<Symbol, SymbolAccount>,
	pub open_orders: ArcMap<Symbol, ArcSet<OrderStatus>>,
	pub order_history: ArcMap<Symbol, ArcSet<OrderStatus>>,
	pub trade_history: ArcMap<Symbol, ArcSet<Trade>>,
	trade_q: Arc<RwLock<VecDeque<Trade>>>,
	pub trade_subscribers: Arc<RwLock<Sender<Trade>>>,
	pub positions: ArcMap<Symbol, Arc<RwLock<Position>>>,
	tf_trades: Arc<RwLock<Receiver<TfTrades>>>,
	order_statuses: Arc<RwLock<Receiver<OrderStatus>>>,
	tf_trades_working: Arc<std::sync::RwLock<bool>>,
	order_statuses_working: Arc<std::sync::RwLock<bool>>,
}


pub struct BinanceLiveExecutor {
	account: Arc<BinanceLiveAccount>,
	pub orders: Arc<RwLock<Receiver<Order>>>,
	order_status_q: Arc<RwLock<VecDeque<OrderStatus>>>,
	pub order_status_subscribers: Arc<RwLock<Sender<OrderStatus>>>,
	order_working: Arc<std::sync::RwLock<bool>>,
}

impl BinanceLiveExecutor {
	pub async fn new(
		key: AccessKey,
		orders_rx: Receiver<Order>,
		trades_rx: Receiver<TfTrades>,
		symbols: Vec<Symbol>,
		trades: Sender<Trade>,
	) -> Self {
		let order_statuses_channel = async_broadcast::broadcast(100);
		let mut account = BinanceLiveAccount::new(key, trades_rx, order_statuses_channel.1, symbols).await;
		// account.subscribe(trades).await;
		// account.emit().await;
		let ac = account.clone();
		std::thread::spawn(move || {
			EventSink::<TfTrades>::listen(&ac).unwrap();
		});
		let ac = account.clone();
		std::thread::spawn(move || {
			EventSink::<OrderStatus>::listen(&ac).unwrap();
		});
		Self {
			account: Arc::new(account),
			orders: Arc::new(RwLock::new(orders_rx)),
			order_status_q: Arc::new(RwLock::new(VecDeque::new())),
			order_status_subscribers: Arc::new(RwLock::new(order_statuses_channel.0)),
			order_working: Arc::new(std::sync::RwLock::new(false)),
		}
	}
}

impl BinanceLiveAccount {
	pub async fn new(
		key: AccessKey,
		tf_trades: Receiver<TfTrades>,
		order_statuses: Receiver<OrderStatus>,
		symbols: Vec<Symbol>,
	) -> Self {
		let symbol_accounts = Arc::new(RwLock::new(HashMap::new()));
		let open_orders = Arc::new(RwLock::new(HashMap::new()));
		let order_history = Arc::new(RwLock::new(HashMap::new()));
		let trade_history = Arc::new(RwLock::new(HashMap::new()));
		let positions = Arc::new(RwLock::new(HashMap::new()));
		
		let account = FuturesAccount::new(Some(key.api_key.clone()), Some(key.secret_key.clone()));
		let market = FuturesMarket::new(None, None);
		for symbol in symbols {
			let symbol_account = SymbolAccount {
				symbol: symbol.clone(),
				base_asset_free: Default::default(),
				base_asset_locked: Default::default(),
				quote_asset_free: Decimal::new(100000, 0),
				quote_asset_locked: Default::default(),
			};
			let position = Position::new(Side::Ask, symbol.clone(), Decimal::ZERO, Decimal::ZERO);
			symbol_accounts
				  .write()
				  .await
				  .insert(symbol.clone(), symbol_account);
			open_orders
				  .write()
				  .await
				  .insert(symbol.clone(), Arc::new(RwLock::new(HashSet::new())));
			order_history
				  .write()
				  .await
				  .insert(symbol.clone(), Arc::new(RwLock::new(HashSet::new())));
			trade_history
				  .write()
				  .await
				  .insert(symbol.clone(), Arc::new(RwLock::new(HashSet::new())));
			positions
				  .write()
				  .await
				  .insert(symbol.clone(), Arc::new(RwLock::new(position)));

		}
		Self {
			symbol_accounts,
			open_orders,
			order_history,
			trade_history,
			positions,
			market: Arc::new(RwLock::new(market)),
			notifier: Arc::new(RwLock::new(TelegramNotifier::new())),
			tf_trades: Arc::new(RwLock::new(tf_trades)),
			account: Arc::new(RwLock::new(account)),
			trade_q: Arc::new(RwLock::new(VecDeque::new())),
			trade_subscribers: Arc::new(RwLock::new(async_broadcast::broadcast(1).0)),
			order_statuses: Arc::new(RwLock::new(order_statuses)),
			order_statuses_working: Arc::new(std::sync::RwLock::new(false)),
			tf_trades_working: Arc::new(std::sync::RwLock::new(false)),
		}
	}
}
#[async_trait]
impl ExchangeAccountInfo for BinanceLiveAccount {
	fn get_exchange_id(&self) -> ExchangeId {
		ExchangeId::Binance
	}
	async fn get_open_orders(&self, symbol: &Symbol) -> Arc<HashSet<OrderStatus>> {
		Arc::new(
			self.open_orders
			    .read()
			    .await
			    .get(symbol)
			    .unwrap()
			    .read()
			    .await
			    .clone(),
		)
	}
	
	async fn get_symbol_account(&self, symbol: &Symbol) -> SymbolAccount {
		self.symbol_accounts
		    .read()
		    .await
		    .get(symbol)
		    .unwrap()
		    .clone()
	}
	
	async fn get_past_trades(&self, symbol: &Symbol, length: Option<usize>) -> Arc<HashSet<Trade>> {
		if let Some(length) = length {
			let all_trades = self.trade_history.read().await;
			let guard = all_trades.get(symbol).unwrap().read().await;
			let mut trades_vec = guard.iter().collect::<Vec<&Trade>>();
			trades_vec.sort_by(|a, b| b.time.cmp(&a.time));
			trades_vec.truncate(length);
			let trades = trades_vec.into_iter().cloned().collect::<HashSet<Trade>>();
			Arc::new(trades)
		} else {
			Arc::new(
				self.trade_history
				    .read()
				    .await
				    .get(symbol)
				    .unwrap()
				    .read()
				    .await
				    .clone(),
			)
		}
	}
	
	async fn get_position(&self, symbol: &Symbol) -> Arc<Position> {
		let account = self.account.read().await;
		if let Ok(position_info) = account.position_information(symbol.symbol.clone()).await {
			if position_info.len() <= 0 {
				return Arc::new(Position::new(Side::Ask, symbol.clone(), Decimal::ZERO, Decimal::ZERO));
			} else {
				let pos = position_info.first().unwrap();
				let position = Position::new(
					if pos.position_amount > 0.0 {
						Side::Bid
					} else {
						Side::Ask
					},
					symbol.clone(),
					Decimal::from_f64(pos.position_amount.abs()).unwrap(),
					Decimal::from_f64(pos.entry_price).unwrap(),
				);
				return Arc::new(position);
			}
		} else {
			return Arc::new(Position::new(Side::Ask, symbol.clone(), Decimal::ZERO, Decimal::ZERO));
		}
		

	}
	
	async fn get_spread(&self, symbol: &Symbol) -> Arc<Spread> {
		let market = self.market.read().await;
		let price = market.get_price(symbol.symbol.clone()).await.unwrap();
		let mut spread = Spread::new( symbol.clone());
		spread.spread = Decimal::from_f64(price.price).unwrap();
		Arc::new(spread)
	}
	async fn get_order(&self, symbol: &Symbol, id: &Uuid) -> Vec<Order> {
		vec![]
	}
}

#[async_trait]
impl ExchangeAccount for BinanceLiveAccount {
	async fn limit_long(&self, order: Order) -> anyhow::Result<OrderStatus> {
		let account = self.account.read().await;
		let symbol = order.symbol.clone();
		match order.order_type {
			OrderType::Limit => {
				let res = account.limit_buy(symbol.symbol.clone(), order.quantity.to_f64().unwrap(), order.price.to_f64().unwrap(), TimeInForce::GTC).await;
				if let Ok(transaction) = res {
					Ok(OrderStatus::Pending(order))
				} else {
					Ok(OrderStatus::Canceled(order, res.unwrap_err().to_string()))
				}
			}
			OrderType::TakeProfit(_)=> {
				let or = CustomOrderRequest {
					symbol: symbol.symbol.clone(),
					side: binance::account::OrderSide::Buy,
					position_side: None,
					order_type: binance::futures::account::OrderType::TakeProfitMarket,
					time_in_force: Some(TimeInForce::GTC),
					qty: Some(order.quantity.to_f64().unwrap()),
					reduce_only: Some(true),
					price: None,
					stop_price: Some(order.price.to_f64().unwrap()),
					close_position: None,
					activation_price: None,
					callback_rate: None,
					working_type: None,
					price_protect: None
				};
				let res = account.custom_order(or).await;
				if let Ok(transaction) = res {
					Ok(OrderStatus::Pending(order))
				} else {
					Ok(OrderStatus::Canceled(order, res.unwrap_err().to_string()))
				}
			}
			
			
			OrderType::StopLoss(id) => {
				let res = account.stop_market_close_buy(symbol.symbol.clone(), order.price.to_f64().unwrap()).await;
				if let Ok(transaction) = res {
					Ok(OrderStatus::Pending(order))
				} else {
					Ok(OrderStatus::Canceled(order, res.unwrap_err().to_string()))
				}
			}
			
			
			OrderType::StopLossTrailing(id, distance) => {
				let trail_percent = (distance.to_f64().unwrap() * 100_f64) / order.price.to_f64().unwrap().min(5.0).max( 0.1);
				let or = CustomOrderRequest {
					symbol: symbol.symbol.clone(),
					side: binance::account::OrderSide::Buy,
					position_side: None,
					order_type: binance::futures::account::OrderType::TrailingStopMarket,
					time_in_force: Some(TimeInForce::GTC),
					qty: Some(order.quantity.to_f64().unwrap()),
					reduce_only: Some(true),
					price: Some(order.price.to_f64().unwrap()),
					stop_price: None,
					close_position: None,
					activation_price: None,
					callback_rate: Some(trail_percent),
					working_type: None,
					price_protect: None
				};
				let res = account.custom_order(or).await;
				if let Ok(transaction) =  res {
					Ok(OrderStatus::Pending(order))
				} else {
					Ok(OrderStatus::Canceled(order, res.unwrap_err().to_string()))
				}
			},
			_ => {todo!()}
		}
		
		
	}
	
	async fn limit_short(&self, order: Order) -> anyhow::Result<OrderStatus> {
		let account = self.account.read().await;
		let symbol = order.symbol.clone();
		match order.order_type {
			OrderType::Limit  => {
				let res = account.limit_sell(symbol.symbol.clone(), order.quantity.to_f64().unwrap(), order.price.to_f64().unwrap(), TimeInForce::GTC).await;
				if let Ok(transaction) = res {
					Ok(OrderStatus::Pending(order))
				} else {
					Ok(OrderStatus::Canceled(order, res.unwrap_err().to_string()))
				}
			}
			
			OrderType::TakeProfit(_)=> {
				let or = CustomOrderRequest {
					symbol: symbol.symbol.clone(),
					side: binance::account::OrderSide::Sell,
					position_side: None,
					order_type: binance::futures::account::OrderType::TakeProfitMarket,
					time_in_force: Some(TimeInForce::GTC),
					qty: Some(order.quantity.to_f64().unwrap()),
					reduce_only: Some(true),
					price: None,
					stop_price: Some(order.price.to_f64().unwrap()),
					close_position: None,
					activation_price: None,
					callback_rate: None,
					working_type: None,
					price_protect: None
				};
				let res = account.custom_order(or).await;
				if let Ok(transaction) = res {
					Ok(OrderStatus::Pending(order))
				} else {
					Ok(OrderStatus::Canceled(order, res.unwrap_err().to_string()))
				}
			}
			
			OrderType::StopLoss(id) => {
				let res = account.stop_market_close_sell(symbol.symbol.clone(), order.price.to_f64().unwrap()).await;
				if let Ok(transaction) =  res {
					Ok(OrderStatus::Pending(order))
				} else {
					Ok(OrderStatus::Canceled(order, res.unwrap_err().to_string()))
				}
			}
			
			
			OrderType::StopLossTrailing(id, distance) => {
				let trail_percent = (distance.to_f64().unwrap() * 100_f64) / order.price.to_f64().unwrap().min(5.0).max( 0.1);
				let or = CustomOrderRequest {
					symbol: symbol.symbol.clone(),
					side: binance::account::OrderSide::Sell,
					position_side: None,
					order_type: binance::futures::account::OrderType::TrailingStopMarket,
					time_in_force: Some(TimeInForce::GTC),
					qty: Some(order.quantity.to_f64().unwrap()),
					reduce_only: Some(true),
					price: Some(order.price.to_f64().unwrap()),
					stop_price: None,
					close_position: None,
					activation_price: None,
					callback_rate: Some(trail_percent),
					working_type: None,
					price_protect: None
				};
				let res = account.custom_order(or).await;
				if let Ok(transaction) =  res {
					Ok(OrderStatus::Pending(order))
				} else {
					Ok(OrderStatus::Canceled(order, res.unwrap_err().to_string()))
				}
			},
			_ => {todo!()}
			
		}
	}
	
	async fn market_long(&self, order: Order) -> anyhow::Result<OrderStatus> {
		let account = self.account.read().await;
		let symbol = order.symbol.clone();
		let res = account.market_buy(symbol.symbol.clone(), order.quantity.to_f64().unwrap()).await;
		if let Ok(transaction) = res {
			let mut or = order.clone();
			or.price = Decimal::from_f64(transaction.avg_price.into()).unwrap();
			Ok(OrderStatus::Filled(order))
		} else {
			Ok(OrderStatus::Canceled(order, res.unwrap_err().to_string()))
		}
	}
	
	async fn market_short(&self, order: Order) -> anyhow::Result<OrderStatus> {
		let account = self.account.read().await;
		let symbol = order.symbol.clone();
		let res = account.market_sell(symbol.symbol.clone(), order.quantity.to_f64().unwrap()).await;
		if let Ok(transaction) =  res {
			let mut or = order.clone();
			or.price = Decimal::from_f64(transaction.avg_price.into()).unwrap();
			Ok(OrderStatus::Filled(order))
		} else {
			Ok(OrderStatus::Canceled(order, res.unwrap_err().to_string()))
		}
	}
	
	async fn cancel_order(&self, order: Order) -> anyhow::Result<OrderStatus> {
		Ok(OrderStatus::Canceled(order, "canceled".to_string()))
	}
}

impl EventSink<Order> for BinanceLiveExecutor {
	fn get_receiver(&self) -> Arc<RwLock<Receiver<Order>>> {
		self.orders.clone()
	}
	
	
	fn handle_event(&self, event_msg: Order) -> anyhow::Result<JoinHandle<anyhow::Result<()>>> {
		let fut = self.process_order(event_msg);
		let order_status_q = self.order_status_q.clone();
		Ok(tokio::spawn(async move {
			if let Ok(res) = fut {
				order_status_q.write().await.push_back(res);
			}
			Ok(())
		}))
	}
	
	fn working(&self) -> bool {
		self.order_working.read().unwrap().clone()
	}
	fn set_working(&self, working: bool) -> anyhow::Result<()> {
		*self.order_working.write().unwrap() = working;
		Ok(())
	}
}
#[async_trait]
impl EventEmitter<OrderStatus> for BinanceLiveExecutor {
	fn get_subscribers(&self) -> Arc<RwLock<Sender<OrderStatus>>> {
		self.order_status_subscribers.clone()
	}
	
	async fn emit(&self) -> anyhow::Result<JoinHandle<()>> {
		let q = self.order_status_q.clone();
		let subs = self.order_status_subscribers.clone();
		Ok(tokio::spawn(async move {
			loop {
				let mut w = q.write().await;
				let order_status = w.pop_front();
				std::mem::drop(w);
				if let Some(order_status) = order_status {
					let subs = subs.read().await;
					match subs.broadcast(order_status.clone()).await {
						Ok(_) => {}
						Err(e) => {
							eprintln!("error broadcasting order status: {:?}", e);
						}
					}
				}
			}
		}))
	}
}

impl EventSink<OrderStatus> for BinanceLiveAccount {
	fn get_receiver(&self) -> Arc<RwLock<Receiver<OrderStatus>>> {
		self.order_statuses.clone()
	}
	fn handle_event(
		&self,
		event_msg: OrderStatus,
	) -> anyhow::Result<JoinHandle<anyhow::Result<()>>> {
		let open_orders = self.open_orders.clone();
		let order_history = self.order_history.clone();
		let trade_q = self.trade_q.clone();
		let all_positions = self.positions.clone();
		let notifier = self.notifier.clone();
		Ok(tokio::spawn(async move {
			match &event_msg {
				OrderStatus::Pending(order) => {
				
				}
				OrderStatus::Filled(order) => {
					match order.order_type {
						OrderType::Market => {
							// let browser = Browser::default().unwrap();
							// let tab = browser.wait_for_initial_tab().unwrap();
							// tab.navigate_to(&format!("https://www.binance.com/en/futures/{}", order.symbol.symbol)).unwrap();
							// tab.wait_until_navigated().unwrap();
							// tokio::time::sleep(Duration::from_secs(10));
							//
							// let png_data = tab.capture_screenshot(
							// 	ScreenshotFormat::PNG,
							// 	None,
							// 	true).unwrap();
							// let file = format!("{}-screenshot.png", order.id);
							//
							// std::fs::write(&file, png_data)?;
							//
							
							let notification = Notification {
								message: order.to_markdown_message(),
								message_sent: false,
								attachment: None
							};
							
							let n = notifier.read().await;
							n.notify(notification).await;
							
							
						}
						
						_ => {}
					}
				}
				OrderStatus::Canceled(order, reason) => {
					let notification = Notification {
						message: order.to_markdown_message() + "\n*Canceled*: " + reason,
						message_sent: false ,
						attachment: None
					};
					let n = notifier.read().await;
					n.notify(notification).await;
					
					
				}
				
				_ => {}
			}
			Ok(())
		}))
	}
	fn working(&self) -> bool {
		self.order_statuses_working.read().unwrap().clone()
	}
	fn set_working(&self, working: bool) -> anyhow::Result<()> {
		*self.order_statuses_working.write().unwrap() = working;
		Ok(())
	}
}


impl EventSink<TfTrades> for BinanceLiveAccount {
	fn get_receiver(&self) -> Arc<RwLock<Receiver<TfTrades>>> {
		self.tf_trades.clone()
	}
	fn handle_event(&self, event_msg: TfTrades) -> anyhow::Result<JoinHandle<anyhow::Result<()>>> {
		if event_msg.is_empty() {
			return Ok(tokio::spawn(async move { Ok(()) }));
		}
		let open_orders = self.open_orders.clone();
		
		let trade_q = self.trade_q.clone();
		let filled_orders = self.order_history.clone();
		let positions = self.positions.clone();
		
		// sync state
		Ok(tokio::spawn(async move {Ok(())}))
	}
	
	fn working(&self) -> bool {
		self.tf_trades_working.read().unwrap().clone()
	}
	fn set_working(&self, working: bool) -> anyhow::Result<()> {
		*self.tf_trades_working.write().unwrap() = working;
		Ok(())
	}
}

impl TradeExecutor for BinanceLiveExecutor {
	type Account = BinanceLiveAccount;
	fn get_account(&self) -> Arc<Self::Account> {
		self.account.clone()
	}
}
