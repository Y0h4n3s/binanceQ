use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use binance::account::Account;
use binance::api::Binance;
use binance::futures::account::FuturesAccount;
use binance::futures::general::FuturesGeneral;
use binance::futures::market::FuturesMarket;
use binance::futures::model::{ExchangeInformation, Symbol};
use binance::futures::userstream::FuturesUserStream;
use binance::savings::Savings;
use binance::userstream::UserStream;
use kanal::{AsyncReceiver, AsyncSender};
use tokio::sync::RwLock;

use crate::{AccessKey, GlobalConfig};
use crate::events::{EventEmitter, EventSink};
use crate::helpers::request_with_retries;
use crate::managers::Manager;
use crate::types::TfTrades;

pub struct RiskManagerConfig {
    pub max_daily_losses: usize,
    pub max_risk_per_trade: f64,
}

pub enum ExecutionCommand {
    OpenLongPosition,
    OpenShortPosition,
    CloseLongPosition,
    CloseShortPosition,
}
pub struct RiskManager {
    pub global_config: GlobalConfig,
    pub config: RiskManagerConfig,
    pub futures_account: FuturesAccount,
    pub account: Account,
    pub futures_market: FuturesMarket,
    pub binance: FuturesGeneral,
    pub user_stream: UserStream,
    pub futures_user_stream: FuturesUserStream,
    pub symbols: Vec<Symbol>,
    pub savings: Savings,
    tf_trades: AsyncReceiver<TfTrades>,
    execution_commands: AsyncReceiver<ExecutionCommand>,
}
#[async_trait]
impl EventSink<ExecutionCommand> for RiskManager {
    fn get_receiver(&self) -> AsyncReceiver<ExecutionCommand> {
        self.execution_commands()
    }
    
    async fn handle_event(&self, event_msg: ExecutionCommand) {
        match event_msg {
            ExecutionCommand::OpenLongPosition => {
                let mut command_queue = self.command_queue.write().await;
                command_queue.push_back(ExecutionCommand::OpenLongPosition);
            }
        }
    }
}

#[async_trait]
impl EventSink<TfTrades> for RiskManager {
    fn get_receiver(&self) -> AsyncReceiver<TfTrades> {
        self.tf_trades()
    }
    // Act on trade events for risk manager
    async fn handle_event(&self, event: TfTrades) {
        for trade in event {
            match trade.tf {
                x if x == self.global_config.tf1 => {
                
                }
                x if x == self.global_config.tf2 => {
        
                }
                x if x == self.global_config.tf3 => {
        
                }
                _ => {}
            }
        }
    }
    
}

//TODO: create a binance account struct that holds all the binance account info
impl RiskManager {
    pub fn new(global_config: GlobalConfig, config: RiskManagerConfig, tf_trades: AsyncReceiver<TfTrades>, execution_commands: AsyncReceiver<ExecutionCommand>) -> Self {
        let key = &global_config.key;
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
            global_config,
            config,
            futures_account,
            binance,
            account,
            futures_market,
            user_stream,
            futures_user_stream,
            symbols,
            savings,
            tf_trades,
            execution_commands,
        }
    }
    pub async fn passes_max_daily_loss(&self, start_time: u128) -> bool {
        let tasks = self
            .execute_over_futures_symbols(move |symbol, futures_market, futures_account| loop {
                let t_trades_result =
                    futures_account.get_user_trades(symbol.symbol.clone(), None, None, None, None);
                if let Ok(trades) = t_trades_result {
                    let todays_trades = trades
                        .iter()
                        .filter(|t| {
                            t.time as u128 > start_time
                        })
                        .collect::<Vec<_>>();
                    return todays_trades
                        .into_iter()
                        .filter(|t| t.realized_pnl < 0.0)
                        .count();
                } else {
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
        let start_time = SystemTime::now()
              .duration_since(UNIX_EPOCH)
              .unwrap()
              .as_millis();
      
        loop {
            if !self.passes_max_daily_loss(start_time).await {
                println!("Does not pass max daily loss");
                self.close_all_positions().await;
                self.end_day().await;
                std::thread::sleep(std::time::Duration::from_secs(10000));
    
            } else {
                println!("Be patient, what is the probability price goes to x before y?");
            }
        
        
            std::thread::sleep(std::time::Duration::from_secs(45));
        }
    }
}
