

use crate::helpers::request_with_retries;
use crate::AccessKey;
use async_std::prelude::*;
use async_std::task::JoinHandle;
use binance::account::Account;
use binance::api::Binance;
use binance::futures::account::FuturesAccount;
use binance::futures::general::FuturesGeneral;
use binance::futures::market::FuturesMarket;
use binance::futures::model::{ExchangeInformation, Symbol, TradeHistory};
use binance::futures::userstream::FuturesUserStream;
use binance::userstream::UserStream;
use std::rc::Rc;
use std::time::{SystemTime, UNIX_EPOCH};
use binance::model::SpotFuturesTransferType;
use binance::savings::Savings;
use crate::managers::Manager;
use async_trait::async_trait;

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
	pub leverage: usize
	
}

pub struct ReturnStep {
	pub actual_trade: TradeHistory,
	pub f: PositionSizeF,
	pub trade_with_f: TradeHistory,
}
pub struct ReturnHistory {
	pub f: PositionSizeF,
	pub history: Vec<TradeHistory>,
	pub returns: Vec<ReturnStep>
}

impl ReturnHistory {
	pub fn to_csv(&self) -> String {
		let mut csv = String::new();
		csv.push_str("symbol,buyer,price,qty,quote_qty,pnl,qty_after,quote_qty_after,pnl_after,f");
		for i in 0..self.returns.len() {
			csv.push_str(&format!("\n{},{},{},{},{},{},{},{},{},{:?}", self.returns[i].actual_trade.symbol, self.returns[i].actual_trade.buyer, self.returns[i].actual_trade.price, self.returns[i].actual_trade.qty, self.returns[i].actual_trade.quote_qty, self.returns[i].actual_trade.realized_pnl, self.returns[i].trade_with_f.qty, self.returns[i].trade_with_f.quote_qty, self.returns[i].trade_with_f.realized_pnl, self.f));
		}
		csv
		
	}
}

pub struct MoneyManager {
	pub config: MoneyManagerConfig,
	pub futures_account: FuturesAccount,
	pub account: Account,
	pub futures_market: FuturesMarket,
	pub binance: FuturesGeneral,
	pub user_stream: UserStream,
	pub futures_user_stream: FuturesUserStream,
	pub symbols: Vec<Symbol>,
	pub savings: Savings
}

impl MoneyManager {
	pub fn new(key: AccessKey, config: MoneyManagerConfig) -> Self {
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
			config,
			futures_account,
			binance,
			account,
			futures_market,
			user_stream,
			futures_user_stream,
			symbols,
			savings
		}
	}
	pub async fn passes_position_size(&self) -> bool {
		let mut tasks = self
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
	
	pub fn returns_for_constant(&self, history: &mut Vec<TradeHistory>) -> ReturnHistory {
		let mut returns = ReturnHistory {
			f: PositionSizeF::Constant,
			history: history.clone(),
			returns: vec![]
		};
		if history.is_empty() {
			return returns
		}
		let size = if self.config.constant_size.is_some() {
			self.config.constant_size.unwrap()
		} else {
			history.first().unwrap().quote_qty
		};
		
		for i in 0..history.len() {
			let trade = history[i].clone();
			let mut trade_with_f = trade.clone();
			trade_with_f.quote_qty = size;
			trade_with_f.qty = size / trade.price;
			trade_with_f.realized_pnl = (trade_with_f.quote_qty / trade.quote_qty) * trade.realized_pnl;
			returns.returns.push(ReturnStep {
				actual_trade: trade,
				f: PositionSizeF::Constant,
				trade_with_f
			})
		}
		returns
	}
	
	pub fn compile_history_over_f(&self, f: PositionSizeF, history:  &mut Vec<TradeHistory>) -> ReturnHistory {
		history.sort_by(|a, b| a.time.cmp(&b.time));
		match f {
			PositionSizeF::Constant => {
				self.returns_for_constant(history)
			}
			
				_ => {
					ReturnHistory {
						f,
						history: history.to_vec(),
						returns: vec![]
					}
				}
		}
	}
	pub async fn simulate_returns_for_f(&self, f: PositionSizeF) -> ReturnHistory {
		let mut past_trades : Vec<TradeHistory> = Vec::new();
		let mut tasks = self
			  .execute_over_futures_symbols::<Vec<TradeHistory>>(|symbol, futures_market, futures_account| loop {
				  if symbol.quote_asset.ne("USDT") && symbol.quote_asset.ne("BUSD") {
					  return vec![]
				  }
				  if let Ok(past_trades_for_symbol) = futures_account.get_user_trades(symbol.symbol.clone(), None, None, None, None) {
					 return past_trades_for_symbol
				  }
			  })
			  .await;
		past_trades = futures::future::join_all(tasks)
			  .await
			  .into_iter()
			  .flatten()
			  .collect::<Vec<TradeHistory>>();
		
		self.compile_history_over_f(f, &mut past_trades)
		
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
		println!("{}", self.simulate_returns_for_f(PositionSizeF::Constant).await.to_csv());
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
