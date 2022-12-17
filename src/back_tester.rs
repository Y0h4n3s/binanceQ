use std::fmt::Write;
use std::sync::{Mutex, RwLock};
use async_std::sync::Arc;
use binance_q_events::{EventEmitter, EventSink};
use binance_q_managers::risk_manager::RiskManager;
use binance_q_managers::strategy_manager::{StrategyManager, StrategyManagerConfig};
use binance_q_mongodb::client::MongoClient;
use binance_q_mongodb::loader::TfTradeEmitter;
use binance_q_types::{
    ExecutionCommand, GlobalConfig, Kline, Order, StudyConfig, Symbol, TfTrades, Trade,
};
use futures::{StreamExt, TryStreamExt};
use mongodb::bson::doc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use futures::stream::FuturesUnordered;
use indicatif::{ProgressBar, ProgressState, ProgressStyle};
use binance_q_executors::{ExchangeAccount, ExchangeAccountInfo};
use binance_q_managers::risk_manager::RiskManagerConfig;
use mongodb::options::FindOptions;
use subprocess::Exec;

#[derive(Debug, Clone)]
pub struct BackTesterConfig {
    pub symbol: Symbol,
    pub length: u64,
    pub load_history: bool,
    pub grpc_server_port: String,
    pub kline_tf: String,
}
pub struct BackTester {
    global_config: GlobalConfig,
    config: BackTesterConfig,
    // possible to batch 2 or more symbols on the same back-tester instance
    // when running on higher frequency cpus
    symbols: Vec<Symbol>,
    
}

#[derive(Debug, Clone)]
pub struct BackTesterMulti {
    global_config: GlobalConfig,
    config: Vec<BackTesterConfig>,
    symbols: Vec<Symbol>,
}

impl BackTesterMulti {
    pub fn new(global_config: GlobalConfig, config: Vec<BackTesterConfig>, symbols: Vec<Symbol>) -> Self {
        Self {
            global_config,
            config,
            symbols
        }
    }

    pub async fn run(&self, pb: Arc<Mutex<ProgressBar>>) -> anyhow::Result<()> {
        let mut workers = vec![];
        for config in self.config.iter() {
            let mut g_c = self.global_config.clone();
            g_c.symbol = config.symbol.clone();
           let back_tester = BackTester::new(g_c, config.clone(), self.symbols.clone());
            let p = pb.clone();
            workers.push(std::thread::spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                      .enable_all()
                      .build()
                      .unwrap();
                runtime.block_on(async move { back_tester.run(p).await.unwrap() });
            }));
        }
        for worker in workers {
            worker.join().unwrap();
        }
        Ok(())
    }
}

impl BackTester {
    pub fn new(global_config: GlobalConfig, config: BackTesterConfig, symbols: Vec<Symbol>) -> Self {
        Self {
            global_config,
            config,
            symbols
        }
    }

    pub async fn run(&self, pb: Arc<Mutex<ProgressBar>>) -> anyhow::Result<()> {
        let mongo_client = MongoClient::new().await;

        if self.config.load_history {
            binance_q_mongodb::loader::load_history(
                self.global_config.key.clone(),
                self.config.symbol.clone(),
                self.config.length * 1000,
                true,
            )
            .await;
            println!("[?] back_tester> Trades Loaded for {}", self.config.symbol.symbol);
    
            binance_q_mongodb::loader::load_klines_from_archive(
                self.config.symbol.clone(),
                self.config.kline_tf.clone(),
                (self.config.length * 1000) as i64,
            )
            .await;
    
            println!("[?] back_tester> Klines Loaded for {}", self.config.symbol.symbol);
    
            // for tf in vec![
            //     self.global_config.tf1,
            //     self.global_config.tf2,
            //     self.global_config.tf3,
            // ] {
                let tf_trade = TfTradeEmitter::new(self.global_config.tf1, self.global_config.clone());
                tf_trade.log_history(self.config.symbol.clone()).await;
            println!("[?] back_tester> Tftrades Loaded for {}", self.config.symbol.symbol);
    
            // }
        }
        
        // Start the python signal generator for this symbol
        let config = self.config.clone();
        let backtest_done = Arc::new(RwLock::new(false));
        let bd = backtest_done.clone();

        std::thread::spawn( move || {
            println!("[?] back_tester> Launching Signal Generators for {}", config.symbol.symbol);
            
            let mut process = Exec::cmd("python3")
                  .arg("./python/signals/signal_generator.py")
                  .arg("--symbol")
                  .arg(&config.symbol.symbol)
                  .arg("--grpc-port")
                  .arg(&config.grpc_server_port)
                  .arg("--mode")
                  .arg("Backtest")
                  .popen()
                  .unwrap();
            loop {
                if bd.read().unwrap().clone() {
                    process.kill().unwrap();
                    break;
                }
                std::thread::sleep(Duration::from_millis(1000));
            }
        });
        
        
        // wait for the signal generator to start to be safe
        tokio::time::sleep(Duration::from_secs(5)).await;
    
        mongo_client.reset_trades(&self.config.symbol).await;
        mongo_client.reset_orders(&self.config.symbol).await;

        let tf_trades_channel = async_broadcast::broadcast(10000);
        let klines_channel = async_broadcast::broadcast(10000);
        let trades_channel = async_broadcast::broadcast(1000);
        let orders_channel = async_broadcast::broadcast(1000);
        let execution_commands_channel = async_broadcast::broadcast(100);

        println!("[?] back_tester> Initializing executor {}",  self.config.symbol.symbol);
        // extract this out of single back tester, it should be shared across all back testers on multi mode
        let simulated_executor = binance_q_executors::simulated::SimulatedExecutor::new(
            orders_channel.1,
            tf_trades_channel.1,
            self.symbols.clone(),
            trades_channel.0,
        )
        .await;

        
        let inner_account: Box<Arc<dyn ExchangeAccount>> =
            Box::new(simulated_executor.account.clone());
        println!("[?] back_tester> Initializing Risk Manager {}",  self.config.symbol.symbol);
        // calculate position size based on risk tolerance and send orders to executors
        let mut risk_manager = RiskManager::new(
            self.global_config.clone(),
            RiskManagerConfig {
                max_daily_losses: 100,
                max_risk_per_trade: 0.01,
            },
            klines_channel.1.clone(),
            trades_channel.1,
            execution_commands_channel.1,
            inner_account,
        );
    
        // receive strategy edges from multiple strategies and forward them to risk manager
        println!("[?] back_tester> Initializing Strategy Manager {}",  self.config.symbol.symbol);
        let mut strategy_manager =
            StrategyManager::new(StrategyManagerConfig{ symbol: self.global_config.symbol.clone() }, self.global_config.clone(), klines_channel.1, self.config.grpc_server_port.clone()).await;

        // sends orders to executors
        risk_manager.subscribe(orders_channel.0).await;

        //sends edges to risk manager
        println!("[?] back_tester> Starting Listeners {}",  self.config.symbol.symbol);
        strategy_manager
            .subscribe(execution_commands_channel.0)
            .await;
        let mut r_threads = vec![];
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
            EventSink::<Kline>::listen(&sm).unwrap();
        }));
        let rc_1 = risk_manager.clone();
        r_threads.push(std::thread::spawn(move || {
            EventSink::<Kline>::listen(&rc_1).unwrap();
        }));
        let s_e = simulated_executor.clone();
        r_threads.push(std::thread::spawn(move || {
            EventSink::<Order>::listen(&s_e).unwrap();
        }));
        let sm = strategy_manager.clone();
        r_threads.push(std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            runtime.block_on(async move { sm.emit().await.unwrap().await });
        }));
        let rc = risk_manager.clone();
        r_threads.push(std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            runtime.block_on(async move { rc.emit().await.unwrap().await });
        }));
        let s_e = simulated_executor.clone();
        r_threads.push(std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            runtime.block_on(async move { s_e.emit().await.unwrap().await });
        }));
        // Todo: chunk these for longer backtests or just use the cursor
        let mut i = 0;

        
        
        let until = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() - (self.config.length * 1000) as u128;
        println!("[?] back_tester> {} Loading Data...",  self.config.symbol.symbol);
        let count = mongo_client.kline.count_documents(doc! {
            "symbol": mongodb::bson::to_bson(&self.config.symbol).unwrap(),
            "close_time": {
                        "$gt": mongodb::bson::to_bson(&(until as u64)).unwrap(),
            }
        }, None).await?;
        let mut w = pb.lock().unwrap();
        w.inc_length(count);
        drop(w);
        let mut klines = mongo_client
            .kline
            .find(
                doc! {
                    "symbol": mongodb::bson::to_bson(&self.config.symbol).unwrap(),
                    "close_time": {
                        "$gt": mongodb::bson::to_bson(&(until as u64)).unwrap(),
                    }
                },
                FindOptions::builder()
                    .sort(doc! {
                        "close_time": 1
                    })
                    .allow_disk_use(true)
                    .build(),
            )
            .await?;
        let mut tf_trade_steps = mongo_client
            .tf_trades
            .find(
                doc! {
                    "tf": mongodb::bson::to_bson(&self.global_config.tf1).unwrap(),
                    "symbol": mongodb::bson::to_bson(&self.config.symbol).unwrap(),
                    "timestamp": {
                        "$gt": mongodb::bson::to_bson(&(until as u64)).unwrap(),
                    }
                },
                FindOptions::builder()
                    .sort(doc! {
                        "timestamp": 1
                    })
                    .allow_disk_use(true)
                    .build(),
            )
            .await?;

        // let event_sequence: Arc<RwLock<VecDeque<Order>>> = Arc::new(RwLock::new(VecDeque::new()));
        // let mut event_registers = vec![];
        let strategy_manager = strategy_manager.clone();
        println!("[?] back_tester> Starting backtest for {} on {}  Klines",  self.config.symbol.symbol, count);
        while let Some(Ok(kline)) = klines.next().await {
            'i: while let Some(Ok(trade)) = tf_trade_steps.next().await {
                if trade.timestamp >= kline.close_time {
                    // send this kline to the strategy manager and let it decide what to do
                    match klines_channel.0.broadcast(kline.clone()).await {
                        Ok(_) => {
                            // wait for strategy manager to process the kline
                            while strategy_manager.working() || <RiskManager as EventSink<Kline>>::working(&risk_manager){
                                if self.global_config.verbose {
                                    println!("[?] back_tester> {} Kline being processed {}, Remaining trades: {}",  self.config.symbol.symbol,  i,  count - i);
    
                                }
                                tokio::time::sleep(Duration::from_millis(20)).await;
                            }
                        }
                        Err(e) => {
                            eprintln!("[-] {} Error sending kline to strategy manager {}",  self.config.symbol.symbol, e);
                        }

                    }
                    break 'i;
                }
                match tf_trades_channel.0.broadcast(vec![trade]).await {
                    Ok(_) => {
                        // only wait for all listeners to recv()
                        while tf_trades_channel.0.len() > 0  {
                            if self.global_config.verbose {
                                println!("[?] back_tester> {} Trade event being processed {}",  self.config.symbol.symbol, i);
        
                            }
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
                        println!("[-] {} Error: {}", self.config.symbol.symbol, e);
                    }
                }
                i += 1;
                
            }
            if !self.global_config.verbose {
                let mut w = pb.lock().unwrap();
                w.inc(1);
                drop(w);
            }
        }
        
        
        let mut w = backtest_done.write().unwrap();
        *w = true;
        drop(w);
        println!("[+] back_tester> {} Backtest done",  self.config.symbol.symbol);
        // wait for python process to be killed sleep for at least 2 times more
        tokio::time::sleep(Duration::from_millis(3000)).await;
        Ok(())
    }
}
