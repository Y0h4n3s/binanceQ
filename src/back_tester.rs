use std::time::Duration;
use async_std::sync::Arc;
use binance_q_mongodb::client::MongoClient;
use binance_q_mongodb::loader::TfTradeEmitter;
use binance_q_strategies::chop_directional::{ChopDirectionalEntryStrategy, ChopDirectionalExitStrategy};
use binance_q_strategies::random_strategy::RandomStrategy;
use binance_q_events::{EventSink, EventEmitter};
use binance_q_managers::risk_manager::RiskManager;
use binance_q_managers::strategy_manager::StrategyManager;
use binance_q_types::{ExecutionCommand, GlobalConfig, Order, StudyConfig, Symbol, TfTrades, Trade};
use futures::{StreamExt, TryStreamExt};
use mongodb::bson::doc;

use mongodb::options::FindOptions;
use binance_q_executors::ExchangeAccountInfo;
use binance_q_studies::choppiness_study::ChoppinessStudy;
use binance_q_studies::directional_index_study::DirectionalIndexStudy;
use binance_q_managers::risk_manager::RiskManagerConfig;

#[derive(Debug, Clone)]
pub struct BackTesterConfig {
    pub symbol: Symbol,
    pub length: u64,
	pub load_history: bool,
}
pub struct BackTester {
    global_config: GlobalConfig,
    config: BackTesterConfig,
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
        
	    if self.config.load_history {
		    mongo_client.reset_db().await;
		    binance_q_mongodb::loader::load_history(
			    self.global_config.key.clone(),
			    self.config.symbol.clone(),
			    self.config.length * 1000,
			    true
		    )
			      .await;
		    for tf in vec![self.global_config.tf1, self.global_config.tf2, self.global_config.tf3] {
			    let tf_trade = TfTradeEmitter::new(tf, self.global_config.clone());
			    tf_trade.log_history().await;
		    }
		
	    }
	    mongo_client.reset_studies().await;
	    mongo_client.reset_trades().await;

        let tf_trades_channel = async_broadcast::broadcast(10000);
        let trades_channel = async_broadcast::broadcast(1000);
        let orders_channel = async_broadcast::broadcast(1000);
        let execution_commands_channel = async_broadcast::broadcast(100);

	    let config = StudyConfig {
		    symbol: self.config.symbol.clone(),
		    range: 10,
		    tf1: self.global_config.tf1,
		    tf2: self.global_config.tf2,
		    tf3: self.global_config.tf3,
	    };
	    let choppiness_study = ChoppinessStudy::new(&config, tf_trades_channel.1.clone());
        let adi_study = DirectionalIndexStudy::new(&config, tf_trades_channel.1.clone());
	    let chop_strategy = ChopDirectionalEntryStrategy::new(self.global_config.clone(), adi_study.clone(), choppiness_study.clone());
	    let chop_strategy_exit = ChopDirectionalExitStrategy::new(self.global_config.clone(), adi_study.clone(), choppiness_study.clone());
	    let random_strategy = RandomStrategy::new();
	    
	    // receive orders from risk manager and apply mutations on accounts
        let simulated_executor = binance_q_executors::simulated::SimulatedExecutor::new(orders_channel.1, tf_trades_channel.1.clone(), vec![self.global_config.symbol.clone()], trades_channel.0).await;
	    
	    let inner_account: Box<Arc<dyn ExchangeAccountInfo>> = Box::new(simulated_executor.account.clone());
	    // calculate position size based on risk tolerance and send orders to executors
        let mut risk_manager = RiskManager::new(
	        self.global_config.clone(),
	        RiskManagerConfig {
                max_daily_losses: 100,
                max_risk_per_trade: 0.01,
            },
	        tf_trades_channel.1.clone(),
	        trades_channel.1,
	        execution_commands_channel.1,
	        inner_account,
        );
	    
	    
	    // receive strategy edges from multiple strategies and forward them to risk manager
        let mut strategy_manager = StrategyManager::new(self.global_config.clone(), tf_trades_channel.1)
	          .with_entry_strategy(chop_strategy).await
	          .with_exit_strategy(chop_strategy_exit).await;
	    
	    // sends orders to executors
        risk_manager.subscribe(orders_channel.0).await;
        
	    //sends edges to risk manager
	    strategy_manager
            .subscribe(execution_commands_channel.0)
            .await;
		let mut r_threads = vec![];
	    let a_c = adi_study.clone();
	    r_threads.push(std::thread::spawn(move || {
		    a_c.listen().unwrap();
	    }));
	    let c_c = choppiness_study.clone();
	    r_threads.push(std::thread::spawn(move || {
		    c_c.listen().unwrap();
	    }));
	    let rc = risk_manager.clone();
	    r_threads.push(std::thread::spawn(move || {
		    EventSink::<ExecutionCommand>::listen(&rc).unwrap();
	    }));
	    let rc = risk_manager.clone();
	    r_threads.push(std::thread::spawn(move || {
		    EventSink::<Trade>::listen(&rc).unwrap();
	    }));
	    let sm = strategy_manager.clone();
	    r_threads.push(std::thread::spawn(move || {
		    EventSink::<TfTrades>::listen(&sm).unwrap();
	    }));
	    let rc_1 = risk_manager.clone();
	    r_threads.push(std::thread::spawn(move || {
		    EventSink::<TfTrades>::listen(&rc_1).unwrap();
	    }));
	    let s_e = simulated_executor.clone();
	    r_threads.push(std::thread::spawn(move || {
		    EventSink::<Order>::listen(&s_e).unwrap();
	    }));
	    let sm = strategy_manager.clone();
	    r_threads.push(std::thread::spawn(move || {
		    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
		    runtime.block_on(async move {
			    sm.emit().await.unwrap().await
		    });
	    }));
	    let rc = risk_manager.clone();
	    r_threads.push(std::thread::spawn(move || {
		    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
		    runtime.block_on(async move {
			    rc.emit().await.unwrap().await
		    });
	    }));
	    let s_e = simulated_executor.clone();
	    r_threads.push(std::thread::spawn(move || {
		    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
		    runtime.block_on(async move {
			    s_e.emit().await.unwrap().await
		    });
	    }));
	    // Todo: chunk these for longer backtests or just use the cursor
	    let mut last_timestamp = 0;
	    let chunk_size = 20000;
	    let start = std::time::SystemTime::now();
	    let mut i = 0;
	    let count = mongo_client
		      .tf_trades
		      .count_documents(
			      None,
			      None
		      )
		      .await?;
	    'main: loop {
		    let mut tf_trade_steps = mongo_client
			      .tf_trades
			      .find(
				      doc! {
					      "timestamp": {
						      "$gt": mongodb::bson::to_bson(&last_timestamp).unwrap()
					      }
				      },
				      FindOptions::builder()
						    .sort(doc! {
						"timestamp": 1
					}).allow_disk_use(true).limit(chunk_size)
						    .build(),
			      )
			      .await?
			      .try_collect()
			      .await
			      .unwrap_or_else(|_| vec![]);
		    
		    if tf_trade_steps.is_empty() {
			    break 'main;
		    }
		    last_timestamp = tf_trade_steps.last().unwrap().timestamp;
		    
		    
		    // let event_sequence: Arc<RwLock<VecDeque<Order>>> = Arc::new(RwLock::new(VecDeque::new()));
		    // let mut event_registers = vec![];
		
		    println!("Starting backtest for {} trades", count);
		    for trade in tf_trade_steps {
				    match tf_trades_channel.0.broadcast(vec![trade]).await {
					    Ok(_) => {
						    while tf_trades_channel.0.len() > 0 {
							    println!("event being processed {}", i);
							    tokio::time::sleep(Duration::from_millis(20)).await;
						    }
						
						    // wait for the trade to be propagated to all event sinks
						    // while EventSink::<TfTrades>::working(&risk_manager) ||
						    //       EventSink::<ExecutionCommand>::working(&risk_manager) ||
						    //       EventSink::<Order>::working(&simulated_executor) ||
						    //       EventSink::<TfTrades>::working(&strategy_manager) ||
						    //       EventSink::<TfTrades>::working(&choppiness_study) ||
						    //       EventSink::<TfTrades>::working(&adi_study)
						    //        {
						    //     // println!("event being processed {}", i);
						    //     tokio::time::sleep(Duration::from_millis(5)).await;
						    // }
						
					    }
					    // continue 'step
					    Err(e) => {
						    // don't do anything, print and continue
						    println!("Error: {}", e);
					    }
				    }
				    i += 1;
			    }
		    
		
	    }
	    
	    println!("Backtest finished in {:?} seconds", start.elapsed().unwrap());
	    
	    Ok(())
	    
    }
}
