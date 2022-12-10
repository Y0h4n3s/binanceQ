use std::collections::VecDeque;
use std::time::Duration;
use async_std::sync::Arc;
use binance_q_mongodb::client::MongoClient;
use binance_q_mongodb::loader::TfTradeEmitter;
use tokio::sync::{ RwLock};
use binance_q_types::{ExecutionCommand, GlobalConfig, Order, OrderStatus, StudyConfig, TfTrade, TfTrades};
use futures::TryStreamExt;
use mongodb::bson::doc;

use mongodb::options::FindOptions;
use crate::ExchangeAccountInfo;

#[derive(Debug, Clone)]
pub struct BackTesterConfig {
    pub symbol: String,
    pub length: u64,
}
pub struct BackTester {
    global_config: GlobalConfig,
    config: BackTesterConfig,
}

enum BackTestEvent {
	TfTrade(TfTrade),
	ExecutionCommand(ExecutionCommand),
	Order(Order),
	OrderStatus(OrderStatus)
}

impl BackTester {
    pub fn new(global_config: GlobalConfig, config: BackTesterConfig) -> Self {
        Self {
            global_config,
            config,
        }
    }

    pub async fn run(&self) -> anyhow::Result<()> {
        let mongo_client = MongoClient::new().await;
        mongo_client.reset_db().await;

        binance_q_mongodb::loader::load_history(
            self.global_config.key.clone(),
            self.config.symbol.clone(),
            self.config.length * 1000,
        )
        .await;
	    let mut threads = vec![];

        let trades_channel = kanal::bounded_async(1000);
        let orders_channel = kanal::bounded_async(1000);
        let execution_commands_channel = kanal::bounded_async(100);
        // start tf trade emitters, don't subscribe to any trades, just build tf trades
        // Todo: move past trades logger to different func instead of in emit
        for tf in vec![self.global_config.tf1, self.global_config.tf2, self.global_config.tf3] {
            let mut tf_trade = TfTradeEmitter::new(tf);
            threads.push(tf_trade.emit().await?);
        }
        println!("Tf trade emitters started");
	
	    let config = StudyConfig {
		    symbol: self.config.symbol.clone(),
		    range: 10,
		    tf1: self.global_config.tf1,
		    tf2: self.global_config.tf2,
		    tf3: self.global_config.tf3,
	    };
	    let choppiness_study = ChoppinessStudy::new(&config, trades_channel.1.clone());
        let adi_study = DirectionalIndexStudy::new(&config, trades_channel.1.clone());
	    let chop_strategy = ChopDirectionalStrategy::new(self.global_config.clone(), adi_study.clone(), choppiness_study.clone());
	    // receive orders from risk manager and apply mutations on accounts
        let simulated_executor = executors::simulated::SimulatedExecutor::new(orders_channel.1);
	    
	    let inner_account: Box<Arc<dyn ExchangeAccountInfo>> = Box::new(simulated_executor.account.clone());
	    // calculate position size based on risk tolerance and send orders to executors
        let mut risk_manager = RiskManager::new(
            self.global_config.clone(),
            RiskManagerConfig {
                max_daily_losses: 100,
                max_risk_per_trade: 0.01,
            },
            trades_channel.1.clone(),
            execution_commands_channel.1.clone(),
            inner_account,
        );
	    
	    
	    // receive strategy edges from multiple strategies and forward them to risk manager
        let mut strategy_manager = StrategyManager::new(self.global_config.clone())
	          .with_strategy(chop_strategy).await;
	    
	    // sends orders to executors
        risk_manager.subscribe(orders_channel.0).await;
        
	    //sends edges to risk manager
	    strategy_manager
            .subscribe(execution_commands_channel.0)
            .await;
	
	    threads.push(adi_study.listen().await?);
	    threads.push(choppiness_study.listen().await?);
	    threads.push(strategy_manager.emit().await?);
	    threads.push(EventSink::<ExecutionCommand>::listen(&risk_manager).await?);
	    threads.push(EventSink::<TfTrades>::listen(&risk_manager).await?);
	    threads.push(EventSink::<Order>::listen(&simulated_executor).await?);
	    
        let mut tf_trade_steps = mongo_client
            .tf_trades
            .find(
                None,
                Some(
                    FindOptions::builder()
                        .sort(doc! {
                            "timestamp": 1
                        })
                        .build(),
                ),
            )
            .await?
            .try_collect()
            .await
            .unwrap_or_else(|_| vec![]);
	    
	    // let event_sequence: Arc<RwLock<VecDeque<Order>>> = Arc::new(RwLock::new(VecDeque::new()));
	    // let mut event_registers = vec![];
	    
	    // step over each tf_trade
	    // make use of tokio's barrier
	    'step: loop {
		    if let Some(trade) = tf_trade_steps.pop() {
			    trades_channel.0.send(vec![trade]).await;
			    tokio::time::sleep(Duration::from_millis(1)).await;
		    } else {
			    break 'step;
		    }
        }
	    
	    Ok(())
	    
    }
}
