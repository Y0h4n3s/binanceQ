use async_std::task::JoinHandle;
use binance::futures::account::FuturesAccount;
use binance::futures::market::FuturesMarket;
use binance::futures::model::Symbol;
use binance::model::SpotFuturesTransferType;
use binance::savings::Savings;
use async_trait::async_trait;
pub mod risk_manager;
pub mod money_manager;

#[async_trait]
pub trait Manager {
	
	fn get_symbols(&self) -> Vec<Symbol>;
	fn get_futures_account(&self) -> FuturesAccount;
	fn get_futures_market(&self) -> FuturesMarket;
	fn get_savings(&self) -> Savings;
	async fn execute_over_futures_symbols<R>(
		&self,
		f: impl Fn(Symbol, FuturesMarket, FuturesAccount) -> R + Send + Copy + 'static,
	) -> Vec<JoinHandle<R>>
		where
			  R: Send  + 'static,
	{
		let mut tasks = vec![];
		for symbol in &self.get_symbols() {
			let futures_account = self.get_futures_account().clone();
			let futures_market = self.get_futures_market().clone();
			let symbol = symbol.clone();
			tasks.push(async_std::task::spawn(async move {
				f(symbol, futures_market, futures_account)
			}))
		}
		return tasks;
	}
	
	async fn close_all_positions(&self) {
		println!("Closing open positions");
		let mut closed = false;
		while !closed {
			let tasks = self.execute_over_futures_symbols::<bool>(|symbol, market, account| {
				let position_info = account.position_information(symbol.symbol.clone());
				if let Ok(positions) = position_info {
					for position in positions {
						if position.position_amount <= 0.0 {
							continue;
						}
						println!("Closing position: {:?}", position);
						if (position.entry_price > position.mark_price && position.unrealized_profit > 0.0) || (position.entry_price < position.mark_price && position.unrealized_profit < 0.0) {
							account.market_buy(symbol.symbol.clone(), position.position_amount);
						} else if (position.entry_price < position.mark_price && position.unrealized_profit > 0.0) || (position.entry_price > position.mark_price && position.unrealized_profit < 0.0) {
							account.market_sell(symbol.symbol.clone(), position.position_amount);
						} else {
							return false
						}
						
					}
				}
				return true;
			}).await;
			
			closed = futures::future::join_all(tasks).await.into_iter().all(|t| t);
		}
		
	}
	
	async fn end_day(&self) {
		println!("Ending day, You're Done!");
		let futures_balance = self.get_futures_account().account_balance();
		if let Ok(balance) = futures_balance {
			for bal in balance {
				if bal.balance > 0.0 {
					self.get_savings().transfer_funds(bal.asset,  bal.balance, SpotFuturesTransferType::UsdtFuturesToSpot);
					
				}
			}
			
		}
	}
	
	async fn manage(&self);
}