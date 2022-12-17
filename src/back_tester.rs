use std::fmt::Write;
use async_std::sync::Arc;
use binance_q_events::{EventEmitter, EventSink};
use binance_q_managers::risk_manager::RiskManager;
use binance_q_managers::strategy_manager::StrategyManager;
use binance_q_mongodb::client::MongoClient;
use binance_q_mongodb::loader::TfTradeEmitter;
use binance_q_types::{
    ExecutionCommand, GlobalConfig, Kline, Order, StudyConfig, Symbol, TfTrades, Trade,
};
use futures::{StreamExt, TryStreamExt};
use mongodb::bson::doc;
use std::time::Duration;
use indicatif::{ProgressBar, ProgressState, ProgressStyle};
use binance_q_executors::{ExchangeAccount, ExchangeAccountInfo};
use binance_q_managers::risk_manager::RiskManagerConfig;
use mongodb::options::FindOptions;

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
                true,
            )
            .await;

            binance_q_mongodb::loader::load_klines_from_archive(
                self.config.symbol.clone(),
                "15m".to_string(),
                (self.config.length * 1000) as i64,
            )
            .await;
            for tf in vec![
                self.global_config.tf1,
                self.global_config.tf2,
                self.global_config.tf3,
            ] {
                let tf_trade = TfTradeEmitter::new(tf, self.global_config.clone());
                tf_trade.log_history().await;
            }
        }
        mongo_client.reset_studies().await;
        mongo_client.reset_trades().await;
        mongo_client.reset_orders().await;

        let tf_trades_channel = async_broadcast::broadcast(10000);
        let klines_channel = async_broadcast::broadcast(10000);
        let trades_channel = async_broadcast::broadcast(1000);
        let orders_channel = async_broadcast::broadcast(1000);
        let execution_commands_channel = async_broadcast::broadcast(100);

        println!("[?] back_tester> Initializing executor");
        // receive orders from risk manager and apply mutations on accounts
        let simulated_executor = binance_q_executors::simulated::SimulatedExecutor::new(
            orders_channel.1,
            tf_trades_channel.1,
            vec![self.global_config.symbol.clone()],
            trades_channel.0,
        )
        .await;

        // different managers for different symbols
        
        let inner_account: Box<Arc<dyn ExchangeAccount>> =
            Box::new(simulated_executor.account.clone());
        println!("[?] back_tester> Initializing Risk Manager");
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
        println!("[?] back_tester> Initializing Strategy Manager");
        let mut strategy_manager =
            StrategyManager::new(self.global_config.clone(), klines_channel.1).await;

        // sends orders to executors
        risk_manager.subscribe(orders_channel.0).await;

        //sends edges to risk manager
        println!("[?] back_tester> Starting Listeners");
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
        let start = std::time::SystemTime::now();
        let mut i = 0;

        
        
        
        println!("[?] back_tester> Loading Data...");
        let count = mongo_client.tf_trades.count_documents(doc! {
            "tf": mongodb::bson::to_bson(&self.global_config.tf1).unwrap()
        }, None).await?;
        let pb = ProgressBar::new(count);
        pb.set_style(ProgressStyle::with_template("[?] {spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {percent}%}")
              .unwrap()
              .with_key("eta", |state: &ProgressState, w: &mut dyn Write| write!(w, "{:.1}s", state.eta().as_secs_f64()).unwrap())
              .progress_chars("#>-"));
        let mut klines = mongo_client
            .kline
            .find(
                doc! {
                    "close_time": {
                        "$gt": mongodb::bson::to_bson(&0).unwrap()
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
                    "timestamp": {
                        "$gt": mongodb::bson::to_bson(&0).unwrap()
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

        let strategy_manager = strategy_manager.clone();
        println!("[?] back_tester> Starting backtest for {} {} second interval trades", count, self.global_config.tf1);
        while let Some(Ok(kline)) = klines.next().await {
            'i: while let Some(Ok(trade)) = tf_trade_steps.next().await {
                if trade.timestamp >= kline.close_time {
                    // send this kline to the strategy manager and let it decide what to do
                    match klines_channel.0.broadcast(kline.clone()).await {
                        Ok(_) => {
                            // wait for strategy manager to process the kline
                            while strategy_manager.working() || <RiskManager as EventSink<Kline>>::working(&risk_manager){
                                if self.global_config.verbose {
                                    println!("[?] back_tester> Kline being processed {}, Remaining trades: {}", i,  count - i);
    
                                }
                                tokio::time::sleep(Duration::from_millis(20)).await;
                            }
                        }
                        Err(e) => {
                            eprintln!("Error sending kline to strategy manager {}", e);
                        }

                    }
                    break 'i;
                }
                match tf_trades_channel.0.broadcast(vec![trade]).await {
                    Ok(_) => {
                        // only wait for all listeners to recv()
                        while tf_trades_channel.0.len() > 0  {
                            if self.global_config.verbose {
                                println!("[?] back_tester> Trade event being processed {}", i);
        
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
                        println!("Error: {}", e);
                    }
                }
                i += 1;
                if !self.global_config.verbose {
                    pb.inc(1);
                }
            }
        }
        
        pb.finish_with_message(format!(
            "Back-test finished in {:?} seconds",
            start.elapsed().unwrap()
        ));

        

        Ok(())
    }
}
