use std::cmp::{ Ordering};
use async_std::sync::Arc;

use async_trait::async_trait;
use binance::account::Account;
use binance::api::Binance;
use binance::futures::account::FuturesAccount;
use binance::futures::general::FuturesGeneral;
use binance::futures::market::FuturesMarket;
use binance::futures::model::{ExchangeInformation, Symbol, TradeHistory};
use binance::futures::userstream::FuturesUserStream;
use binance::savings::Savings;
use binance::userstream::UserStream;
use kanal::{AsyncReceiver, AsyncSender};
use tokio::task::JoinHandle;
use crate::events::{EventEmitter, EventSink};
use crate::AccessKey;
use crate::events::TfTradeEmitter;
use crate::helpers::*;
use crate::helpers::request_with_retries;
use crate::managers::Manager;
use crate::types::{GlobalConfig, TfTrades};

#[derive(Debug)]
pub enum PositionSizeF {
	Kelly,
	Larry,
	Constant,
	John,
	MaxLevered,
}
pub struct MoneyManagerConfig {
	pub position_size_f: PositionSizeF,
	pub min_position_size: Option<f64>,
	pub constant_size: Option<f64>,
	pub max_position_size: Option<f64>,
	pub leverage: usize,
	pub initial_quote_balance: f64
	
}

pub struct ReturnStep {
	pub actual_trade: TradeHistory,
	pub f: PositionSizeF,
	pub trade_with_f: TradeHistory,
	pub actual_balance: f64,
	pub balance_with_f: f64,
}
pub struct ReturnHistory {
	pub f: PositionSizeF,
	pub history: Vec<TradeHistory>,
	pub returns: Vec<ReturnStep>
}

impl ReturnHistory {
	pub fn to_csv(&self) -> String {
		let mut csv = String::new();
		csv.push_str("symbol,order_id,id,buyer,price,qty,quote_qty,pnl,commission,balance,qty_after,quote_qty_after,pnl_after,commission,balance_after,f");
		for i in 0..self.returns.len() {
			csv.push_str(&format!("\n{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{:?}", self.returns[i].actual_trade.symbol, self.returns[i].actual_trade.order_id,self.returns[i].actual_trade.id,self.returns[i].actual_trade.buyer, self.returns[i].actual_trade.price, self.returns[i].actual_trade.qty, self.returns[i].actual_trade.quote_qty, self.returns[i].actual_trade.realized_pnl, self.returns[i].actual_trade.commission,self.returns[i].actual_balance, self.returns[i].trade_with_f.qty, self.returns[i].trade_with_f.quote_qty, self.returns[i].trade_with_f.realized_pnl,self.returns[i].trade_with_f.commission,  self.returns[i].balance_with_f, self.f,));
		}
		csv
		
	}
}

pub struct MoneyManager {
	pub global_config: Arc<GlobalConfig>,
	pub config: MoneyManagerConfig,
	pub futures_account: FuturesAccount,
	pub account: Account,
	pub futures_market: FuturesMarket,
	pub binance: FuturesGeneral,
	pub user_stream: UserStream,
	pub futures_user_stream: FuturesUserStream,
	pub symbols: Vec<Symbol>,
	pub savings: Savings,
	tf_trades: Arc<AsyncReceiver<TfTrades>>,
}
#[async_trait]
impl EventSink<TfTrades> for MoneyManager {
	fn get_receiver(&self) -> Arc<AsyncReceiver<TfTrades>> {
		self.tf_trades.clone()
	}
	
	async fn handle_event(&self, event: TfTrades) -> JoinHandle<()> {
		let global_config = self.global_config.clone();
		tokio::spawn(async move {
			for trade in event {
				match trade.tf {
					x if x == global_config.tf1 => {
					
					}
					x if x == global_config.tf2 => {
					
					}
					x if x == global_config.tf3 => {
					
					}
					_ => {}
				}
			}
		})
		
	}
	
}
// TODO: add fees to calculations, 2 functions, with_taker_fees and with_maker_fees
// TODO: implement position sizer for all strategies
impl MoneyManager {
	pub fn new(global_config: GlobalConfig, config: MoneyManagerConfig, tf_trades: AsyncReceiver<TfTrades>) -> Self {
		let key = global_config.key.clone();
		let futures_account =
			  FuturesAccount::new(Some(key.api_key.clone()), Some(key.secret_key.clone()));
		let binance = FuturesGeneral::new(Some(key.api_key.clone()), Some(key.secret_key.clone()));
		let account = Account::new(Some(key.api_key.clone()), Some(key.secret_key.clone()));
		let user_stream = UserStream::new(Some(key.api_key.clone()), Some(key.secret_key.clone()));
		let futures_user_stream =
			  FuturesUserStream::new(Some(key.api_key.clone()), Some(key.secret_key.clone()));
		let futures_market =
			  FuturesMarket::new(Some(key.api_key.clone()), Some(key.secret_key.clone()));
		let savings = Savings::new(Some(key.api_key.clone()), Some(key.secret_key.clone()));
		let symbols =
			  match request_with_retries::<ExchangeInformation>(5, || binance.exchange_info()) {
				  Ok(e) => e.symbols,
				  Err(e) => panic!("Error getting symbols: {}", e),
			  };
		
		MoneyManager {
			global_config: Arc::new(global_config),
			config,
			futures_account,
			binance,
			account,
			futures_market,
			user_stream,
			futures_user_stream,
			symbols,
			savings,
			tf_trades: Arc::new(tf_trades),
		}
	}
	pub async fn passes_position_size(&self) -> bool {
		let tasks = self
			  .execute_over_futures_symbols(|symbol, futures_market, futures_account| loop {
				  return 1
			  })
			  .await;
		let results = futures::future::join_all(tasks)
			  .await
			  .into_iter()
			  .reduce(|a, b| a + b)
			  .unwrap();
		return results < 1;
	}
	
	pub fn returns_for_constant(&self, history: &mut Vec<(TradeHistory, Symbol)>) -> ReturnHistory {
		let mut returns = ReturnHistory {
			f: PositionSizeF::Constant,
			history: history.clone().into_iter().map(|(a, _)| a.clone()).collect(),
			returns: vec![]
		};
		let size = if self.config.constant_size.is_some() {
			self.config.constant_size.unwrap()
		} else {
			history.first().unwrap().0.quote_qty
		};
		let mut balance = self.config.initial_quote_balance;
		let mut actual_balance = balance;
		for i in 0..history.len() {
			let (trade, symbol) = history[i].clone();
			let mut trade_with_f = trade.clone();
			trade_with_f.quote_qty = size;
			trade_with_f.qty = to_precision(size / trade.price, symbol.base_asset_precision as usize);
			trade_with_f.realized_pnl = (trade_with_f.quote_qty / trade.quote_qty) * trade.realized_pnl;
			trade_with_f.commission = (trade_with_f.quote_qty / trade.quote_qty) * trade.commission;
			
			balance = balance + trade_with_f.realized_pnl - trade_with_f.commission;
			actual_balance = actual_balance + trade.realized_pnl - trade.commission;
			
			returns.returns.push(ReturnStep {
				actual_trade: trade,
				f: PositionSizeF::Constant,
				trade_with_f,
				balance_with_f: balance,
				actual_balance
			})
		}
		returns
	}
	
	pub fn returns_for_max_levered(&self, history: &mut Vec<(TradeHistory, Symbol)>) -> ReturnHistory {
		let mut returns = ReturnHistory {
			f: PositionSizeF::MaxLevered,
			history: history.clone().into_iter().map(|(a, _)| a.clone()).collect(),
			returns: vec![]
		};
		let mut balance = self.config.initial_quote_balance;
		let mut actual_balance = balance;
		
		for i in 0..history.len() {
			let (trade, symbol) = history[i].clone();
			let mut trade_with_f = trade.clone();
			let entry_price = if trade.realized_pnl != 0.0 {
				((trade.realized_pnl - (trade.price * trade.qty)) / trade.qty).abs()
			} else {
				trade.price
			};
			trade_with_f.quote_qty = to_precision(balance * self.config.leverage as f64, symbol.quote_precision as usize);
			trade_with_f.qty = to_precision(trade_with_f.quote_qty / entry_price, symbol.base_asset_precision as usize);
			if trade.realized_pnl != 0.0 {
				trade_with_f.quote_qty = to_precision(trade.price * trade_with_f.qty, symbol.quote_precision as usize);
			}
			trade_with_f.realized_pnl = (trade_with_f.quote_qty / trade.quote_qty) * trade.realized_pnl;
			trade_with_f.commission = (trade_with_f.quote_qty / trade.quote_qty) * trade.commission;
			balance = balance + trade_with_f.realized_pnl - trade_with_f.commission;
			actual_balance = actual_balance + trade.realized_pnl - trade.commission;
			returns.returns.push(ReturnStep {
				actual_trade: trade,
				f: PositionSizeF::MaxLevered,
				trade_with_f,
				balance_with_f: balance,
				actual_balance
			})
		}
		returns
	}
	pub fn returns_for_larry(&self, history: &mut Vec<(TradeHistory, Symbol)>) -> ReturnHistory {
		let mut returns = ReturnHistory {
			f: PositionSizeF::Larry,
			history: history.clone().into_iter().map(|(a, _)| a.clone()).collect(),
			returns: vec![]
		};
		let mut balance = self.config.initial_quote_balance;
		let mut actual_balance = balance;
		let mut max_loss = balance * self.config.leverage as f64 * 0.02;
		for i in 0..history.len() {
			let (trade, symbol) = history[i].clone();
			let mut trade_with_f = trade.clone();
			
			let entry_price = if trade.realized_pnl != 0.0 {
				
				((trade.realized_pnl - (trade.price * trade.qty)) / trade.qty).abs()
			} else {
				trade.price
			};
			trade_with_f.quote_qty = to_precision((balance * self.config.leverage as f64 * 0.17) / max_loss, symbol.quote_precision as usize);
			trade_with_f.qty = to_precision(trade_with_f.quote_qty / entry_price, symbol.base_asset_precision as usize);
			if trade.realized_pnl != 0.0 {
				trade_with_f.quote_qty = to_precision(trade.price * trade_with_f.qty, symbol.quote_precision as usize);
			}
			trade_with_f.realized_pnl = (trade_with_f.quote_qty / trade.quote_qty) * trade.realized_pnl;
			max_loss = if trade_with_f.realized_pnl > max_loss {
				trade.realized_pnl
			} else {
				max_loss
			};
			trade_with_f.commission = (trade_with_f.quote_qty / trade.quote_qty) * trade.commission;
			balance = balance + trade_with_f.realized_pnl - trade_with_f.commission;
			actual_balance = actual_balance + trade.realized_pnl - trade.commission;
			returns.returns.push(ReturnStep {
				actual_trade: trade,
				f: PositionSizeF::MaxLevered,
				trade_with_f,
				balance_with_f: balance,
				actual_balance
			})
		}
		returns
	}
	pub fn compile_history_over_f(&self, f: PositionSizeF, history:  &mut Vec<(TradeHistory, Symbol)>) -> ReturnHistory {
		if history.is_empty() {
			return ReturnHistory {
				f,
				history: vec![],
				returns: vec![]
			}
		}
		match f {
			PositionSizeF::Constant => {
				self.returns_for_constant(history)
			}
			PositionSizeF::MaxLevered => {
				self.returns_for_max_levered(history)
			}
			PositionSizeF::Larry => {
				self.returns_for_larry(history)
			}
			
				_ => {
					ReturnHistory {
						f,
						history: vec![],
						returns: vec![]
					}
				}
		}
	}
	pub async fn simulate_returns_for_f(&self, f: PositionSizeF) -> ReturnHistory {
		let mut past_trades : Vec<(TradeHistory, Symbol)> = self.get_trade_history_with_symbol().await;
		
		self.compile_history_over_f(f, &mut past_trades)
		
	}
	
	pub async fn get_trade_history_with_symbol(&self) -> Vec<(TradeHistory, Symbol)> {
		let tasks = self
			  .execute_over_futures_symbols::<Vec<(TradeHistory, Symbol)>>(|symbol, futures_market, futures_account| loop {
				  if symbol.quote_asset.ne("USDT") && symbol.quote_asset.ne("BUSD") {
					  return vec![]
				  }
				  if let Ok(past_trades_for_symbol) = futures_account.get_user_trades(symbol.symbol.clone(), None, None, None, None) {
					  return past_trades_for_symbol.into_iter().map(|t| (t, symbol.clone())).collect()
				  }
			  })
			  .await;
		let mut results = futures::future::join_all(tasks)
			  .await
			  .into_iter()
			  .flatten()
			  
			  .collect::<Vec<(TradeHistory, Symbol)>>();
		results.sort_by(|a, b| a.0.time.cmp(&b.0.time));
		results
		
	}
	
	pub async fn print_next_size(&self) {
		let history = self.get_trade_history_with_symbol().await;
		println!("Next size Constant: {}", self.get_next_size_for_f(PositionSizeF::Constant, &history));
		println!("Next size Max Levered: {}", self.get_next_size_for_f(PositionSizeF::MaxLevered, &history));
		println!("Next size Larry: {}", self.get_next_size_for_f(PositionSizeF::Larry, &history));
	}
	pub fn get_next_size_for_f(&self, f: PositionSizeF, history: &Vec<(TradeHistory, Symbol)>) -> f64 {
		if history.len() <= 0 {
			return 0.0
		}
		if let Ok(balance) = self.futures_account.account_balance() {
			let usdt_balance = balance.iter().find(|b| b.asset.eq("USDT")).unwrap();
			match f {
				PositionSizeF::Constant => {
					return usdt_balance.balance
				}
				PositionSizeF::MaxLevered => {
					return usdt_balance.balance * self.config.leverage as f64
				}
				PositionSizeF::Larry => {
					let mut returns = self.returns_for_larry(&mut history.clone());
					if returns.history.is_empty() {
						return 0.0
					}
					returns.history.sort_by(|a, b| if a.realized_pnl > b.realized_pnl {Ordering::Greater} else {Ordering::Less});
					let mut max_loss = returns.history.last().unwrap().realized_pnl.abs();
					while max_loss < 1.0 {
						max_loss = max_loss * 10.0;
					}
					(usdt_balance.balance * self.config.leverage as f64 * 0.17) / max_loss
					
				}
				_ => {
					0.0
				}
			}
		} else {
			return 0.0
		}
		
	}
	
}
#[async_trait]
impl Manager for MoneyManager {
	
	fn get_symbols(&self) -> Vec<Symbol> {
		self.symbols.clone()
	}
	
	fn get_futures_account(&self) -> FuturesAccount {
		self.futures_account.clone()
	}
	
	fn get_futures_market(&self) -> FuturesMarket {
		self.futures_market.clone()
	}
	
	fn get_savings(&self) -> Savings {
		self.savings.clone()
	}
	
	async fn manage(&self) {
		self.print_next_size().await;
		loop {
			if !self.passes_position_size().await {
				println!("Does not pass max daily loss");
				self.close_all_positions().await;
				self.end_day().await
			}
			
			
			std::thread::sleep(std::time::Duration::from_secs(15));
		}
	}
}
