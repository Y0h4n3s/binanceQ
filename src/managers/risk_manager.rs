use std::collections::VecDeque;
use async_std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use binance::account::Account;
use binance::api::Binance;
use binance::futures::account::FuturesAccount;
use binance::futures::general::FuturesGeneral;
use binance::futures::market::FuturesMarket;
use binance::futures::model::{ExchangeInformation};
use binance::futures::userstream::FuturesUserStream;
use binance::savings::Savings;
use binance::userstream::UserStream;
use kanal::{AsyncReceiver, AsyncSender};
use tokio::sync::RwLock;
use tokio::task::JoinHandle;

use crate::{AccessKey, GlobalConfig};
use crate::events::{EventEmitter, EventResult, EventSink};
use crate::executors::{ExchangeAccount, ExchangeAccountInfo, Order, OrderType, Side};
use crate::managers::Manager;
use crate::types::{Symbol, TfTrades};

pub struct RiskManagerConfig {
    pub max_daily_losses: usize,
    pub max_risk_per_trade: f64,
}

#[derive(Clone)]
pub enum ExecutionCommand {
    OpenLongPosition(Symbol, f64),
    OpenShortPosition(Symbol, f64),
    CloseLongPosition(Symbol, f64),
    CloseShortPosition(Symbol, f64),
}
pub struct RiskManager {
    pub global_config: Arc<GlobalConfig>,
    pub config: RiskManagerConfig,
    pub account: Box<Arc<dyn ExchangeAccountInfo>>,
    tf_trades: Arc<AsyncReceiver<TfTrades>>,
    execution_commands: Arc<AsyncReceiver<ExecutionCommand>>,
    subscribers: Arc<RwLock<Vec<AsyncSender<Order>>>>,
    order_q: Arc<RwLock<VecDeque<Order>>>,
}

#[async_trait]
impl EventEmitter<'_,Order> for RiskManager {
    fn get_subscribers(&self) -> Arc<RwLock<Vec<AsyncSender<Order>>>> {
        self.subscribers.clone()
    }
    
    async fn emit(&self) -> anyhow::Result<JoinHandle<()>> {
        let order_q = self.order_q.clone();
        let subscribers = self.subscribers.clone();
        Ok(tokio::spawn(async move {
            //send waiting orders to the executor
            loop {
                let mut oq = order_q.write().await;
                let order = oq.pop_front();
                std::mem::drop(oq);
                if let Some(order) = order {
                   // there is only one subscriber
                    let subs = subscribers.read().await;
                    let sender = subs.get(0).unwrap();
                    sender.send(order).await;
                } else {
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                }
            }
        }))
    }
}
#[async_trait]
impl EventSink<ExecutionCommand> for RiskManager {
    fn get_receiver(&self) -> Arc<AsyncReceiver<ExecutionCommand>> {
        self.execution_commands.clone()
    }
    
    async fn handle_event(&self, event_msg: ExecutionCommand) -> EventResult {
        let account = self.account.clone();
        let global_config = self.global_config.clone();
        /// decide on size and price and order_type and send to order_q
        Ok(tokio::spawn(async move {
            match event_msg {
                // try different configs here
                ExecutionCommand::OpenLongPosition(symbol, confidence) => {
                    let symbol_balance = account.get_symbol_account(&symbol).await;
                    let trade_history = account.get_past_trades(&symbol, None).await;
                    let position = account.get_position(&symbol).await;
                    // calculate size here
                    let size = symbol_balance.quote_asset_free;
                    // get the price based on confidence level
                    let price = 0.0;
                    let order = Order {
                        id: 0,
                        symbol,
                        side: Side::Buy,
                        price: Default::default(),
                        quantity: Default::default(),
                        time: 0,
                        order_type: OrderType::Limit
                    };
                    println!("OpenLongPosition");
                }
                ExecutionCommand::OpenShortPosition(symbol, confidence) => {
                    println!("OpenShortPosition");
                }
                ExecutionCommand::CloseLongPosition(symbol, confidence) => {
                    println!("CloseLongPosition");
                }
                ExecutionCommand::CloseShortPosition(symbol, confidence) => {
                    println!("CloseShortPosition");
                }
            }
            Ok(())
        }))

    }
}

#[async_trait]
impl EventSink< TfTrades> for RiskManager {
    fn get_receiver(&self) -> Arc<AsyncReceiver<TfTrades>> {
        self.tf_trades.clone()
    }
    // Act on trade events for risk manager
    async fn handle_event(&self, event: TfTrades) -> EventResult {
        let global_config = self.global_config.clone();
        Ok(tokio::spawn(async move {
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
            Ok(())
        }))
        
    }
    
}

impl RiskManager {
    pub fn new(global_config: GlobalConfig, config: RiskManagerConfig, tf_trades: AsyncReceiver<TfTrades>, execution_commands: AsyncReceiver<ExecutionCommand>, account: Box<Arc<dyn ExchangeAccountInfo>>) -> Self {
        let key = &global_config.key;
    

        RiskManager {
            global_config: Arc::new(global_config),
            config,
            account,
            tf_trades: Arc::new(tf_trades),
            execution_commands: Arc::new(execution_commands),
            subscribers: Arc::new(RwLock::new(Vec::new())),
            order_q: Arc::new(RwLock::new(VecDeque::new())),
        }
    }





}
#[async_trait]
impl Manager for RiskManager {

    async fn manage(&self) {
        let start_time = SystemTime::now()
              .duration_since(UNIX_EPOCH)
              .unwrap()
              .as_millis();
      
        loop {
           
        
            std::thread::sleep(std::time::Duration::from_secs(45));
        }
    }
}
