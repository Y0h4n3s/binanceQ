use std::time::Duration;
use async_std::sync::Arc;
use binance_q_mongodb::client::MongoClient;
use binance_q_mongodb::loader::TfTradeEmitter;
use binance_q_strategies::chop_directional::ChopDirectionalEntryStrategy;
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
        mongo_client.reset_db().await;

        binance_q_mongodb::loader::load_history(
            self.global_config.key.clone(),
            self.config.symbol.clone(),
            self.config.length * 1000,
        )
        .await;
	    let mut threads = vec![];

        let tf_trades_channel = async_broadcast::broadcast(1000);
        let trades_channel = async_broadcast::broadcast(1000);
        let orders_channel = async_broadcast::broadcast(1000);
        let execution_commands_channel = async_broadcast::broadcast(100);
        // start tf trade emitters, don't subscribe to any trades, just build tf trades
        // Todo: move past trades logger to different func instead of in emit
        for tf in vec![self.global_config.tf1, self.global_config.tf2, self.global_config.tf3] {
            let tf_trade = TfTradeEmitter::new(tf, self.global_config.clone());
            tf_trade.log_history().await;
        }
	
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
	          .with_strategy(chop_strategy).await;
	    
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
	    threads.push(strategy_manager.emit().await?);
	    // Todo: chunk these for longer backtests or just use the cursor
        let mut tf_trade_steps = mongo_client
            .tf_trades
            .find(
	            None,
                FindOptions::builder()
					.sort(doc! {
						"timestamp": 1
					})
					.build(),
            )
            .await?;
	    
	    // let event_sequence: Arc<RwLock<VecDeque<Order>>> = Arc::new(RwLock::new(VecDeque::new()));
	    // let mut event_registers = vec![];
	    
	    // step over each tf_trade
	    // make use of tokio's barrier
	    let start = std::time::SystemTime::now();
	    println!("Starting backtest");
	
	    'step: loop {
		    if let Some(trade) = tf_trade_steps.next().await {
			    let trade = trade.unwrap();
			    match tf_trades_channel.0.broadcast(vec![trade]).await {
				    Ok(_) => {
					    tokio::time::sleep(Duration::from_micros(10)).await;
					
					    // wait for the trade to be propagated to all event sinks
					    while EventSink::<TfTrades>::working(&risk_manager) {
						    // println!("Risk manager is working tf trades");
						    tokio::time::sleep(Duration::from_micros(10)).await;
					    }
					    while EventSink::<ExecutionCommand>::working(&risk_manager) {
						    // println!("Risk manager is working execution commands");
						    tokio::time::sleep(Duration::from_micros(10)).await;
					    }
					    while EventSink::<Order>::working(&simulated_executor){
						    // println!("Simulated executor is working");
						    tokio::time::sleep(Duration::from_micros(10)).await;
					    }
					    while EventSink::<TfTrades>::working(&strategy_manager){
						    // println!("Strategy manager is working");
						    tokio::time::sleep(Duration::from_micros(10)).await;
					    }
					   while EventSink::<TfTrades>::working(&choppiness_study) {
						   // println!("choppiness study is working");
						   tokio::time::sleep(Duration::from_micros(10)).await;
					   }
					   while EventSink::<TfTrades>::working(&adi_study) {
						   // println!("adi study is working");
						   tokio::time::sleep(Duration::from_micros(10)).await;
					   }
					   
				    }
					    // continue 'step
				    Err(e) => {
					    // don't do anything, print and continue
					    println!("Error: {}", e);
				    }
			    }
		    } else {
			    break 'step;
		    }
        }
	    println!("Backtest finished in {:?} seconds", start.elapsed().unwrap());
	    
	    Ok(())
	    
    }
}
