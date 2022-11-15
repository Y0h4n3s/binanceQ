use crate::helpers::request_with_retries;
use crate::AccessKey;
use async_std::prelude::*;
use async_std::task::JoinHandle;
use binance::account::Account;
use binance::api::Binance;
use binance::futures::account::FuturesAccount;
use binance::futures::general::FuturesGeneral;
use binance::futures::market::FuturesMarket;
use binance::futures::model::{ExchangeInformation, Symbol};
use binance::futures::userstream::FuturesUserStream;
use binance::userstream::UserStream;
use std::rc::Rc;
use std::time::{SystemTime, UNIX_EPOCH};
use binance::model::SpotFuturesTransferType;
use binance::savings::Savings;
use async_trait::async_trait;
use crate::managers::Manager;

pub struct RiskManagerConfig {
    pub max_daily_losses: usize,
    pub max_risk_per_trade: f64,
}

pub struct RiskManager {
    pub config: RiskManagerConfig,
    pub futures_account: FuturesAccount,
    pub account: Account,
    pub futures_market: FuturesMarket,
    pub binance: FuturesGeneral,
    pub user_stream: UserStream,
    pub futures_user_stream: FuturesUserStream,
    pub symbols: Vec<Symbol>,
    pub savings: Savings
}

impl RiskManager {
    pub fn new(key: AccessKey, config: RiskManagerConfig) -> Self {
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

        RiskManager {
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
    pub async fn passes_max_daily_loss(&self) -> bool {
        let mut tasks = self
            .execute_over_futures_symbols(|symbol, futures_market, futures_account| loop {
                let t_trades_result =
                    futures_account.get_user_trades(symbol.symbol.clone(), None, None, None, None);
                if let Ok(trades) = t_trades_result {
                    let todays_trades = trades
                        .iter()
                        .filter(|t| {
                            let yesterday = SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .unwrap()
                                .as_secs()
                                - 86400;
                            t.time > yesterday
                        })
                        .collect::<Vec<_>>();
                    return todays_trades
                        .into_iter()
                        .filter(|t| t.realized_pnl < 0.0)
                        .count();
                } else {
                    eprintln!("Error getting trades: {:?}", t_trades_result.unwrap_err());
                }
            })
            .await;
        let results = futures::future::join_all(tasks)
            .await
            .into_iter()
            .reduce(|a, b| a + b)
            .unwrap();
        return results < self.config.max_daily_losses;
    }





}
#[async_trait]
impl Manager for RiskManager {
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
        loop {
            if !self.passes_max_daily_loss().await {
                println!("Does not pass max daily loss");
                self.close_all_positions().await;
                self.end_day().await
            }
        
        
            std::thread::sleep(std::time::Duration::from_secs(15));
        }
    }
}
