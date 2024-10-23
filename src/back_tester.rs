use std::sync::atomic::{AtomicBool, Ordering};
use crate::executors::simulated::SimulatedAccount;
use crate::executors::TradeExecutor;
use crate::mongodb::MongoClient;
use crate::types::{GlobalConfig, Order, Symbol, TfTrades, Trade};
use async_broadcast::{InactiveReceiver, Sender};
use async_std::sync::Arc;
use futures::{stream, StreamExt};
use indicatif::ProgressBar;
use mongodb::bson::doc;
use mongodb::options::FindOptions;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::Notify;
use tracing::{error, info};
use tracing::log::debug;
use crate::db::client::SQLiteClient;

#[derive(Debug, Clone)]
pub struct BackTesterConfig {
    pub symbol: Symbol,
    pub length: u64,
    pub _log_history: bool,
    pub grpc_server_port: String,
    pub _kline_tf: String,
}
pub struct BackTester {
    global_config: GlobalConfig,
    config: BackTesterConfig,
}

#[derive(Debug, Clone)]
pub struct BackTesterMulti {
    global_config: GlobalConfig,
    config: Vec<BackTesterConfig>,
}

impl BackTesterMulti {
    pub fn new(
        global_config: GlobalConfig,
        config: Vec<BackTesterConfig>,
    ) -> Self {
        Self {
            global_config,
            config,
        }
    }

    pub async fn run(
        &self,
        pb: ProgressBar,
        trades_receiver: InactiveReceiver<(Trade, Option<Arc<Notify>>)>,
        tf_trades_tx: Sender<(TfTrades, Option<Arc<Notify>>)>,
        orders_tx: Sender<(Order, Option<Arc<Notify>>)>,
        executor: Box<Arc<dyn TradeExecutor<Account = SimulatedAccount>>>,
    ) -> anyhow::Result<()> {
        stream::iter(self.config.clone())
            .map(|config| {
                let mut g_c = self.global_config.clone();
                g_c.symbol = config.symbol.clone();
                let back_tester = BackTester::new(g_c, config.clone());
                let p = pb.clone();
                let t_r = trades_receiver.clone();
                let t_t = tf_trades_tx.clone();
                let o_t = orders_tx.clone();
                let e = executor.clone();
                async move {
                    back_tester.run(p, t_r, t_t, o_t, e).await.unwrap()

                }
            }).buffer_unordered(20)
            .collect::<Vec<_>>()
            .await;
        // drop the idle receiver
        drop(trades_receiver);
        Ok(())
    }
}

impl BackTester {
    pub fn new(
        global_config: GlobalConfig,
        config: BackTesterConfig,
    ) -> Self {
        Self {
            global_config,
            config,
        }
    }

    pub async fn run(
        &self,
        pb: ProgressBar,
        trades_receiver: InactiveReceiver<(Trade, Option<Arc<Notify>>)>,
        tf_trades_tx: Sender<(TfTrades, Option<Arc<Notify>>)>,
        orders_tx: Sender<(Order, Option<Arc<Notify>>)>,
        executor: Box<Arc<dyn TradeExecutor<Account = SimulatedAccount>>>,
    ) -> anyhow::Result<()> {
        let mongo_client = MongoClient::new().await;
        let sqlite_client = SQLiteClient::new().await;
        // Start the python signal generator for this symbol
        let config = self.config.clone();
        let backtest_done = Arc::new(AtomicBool::new(false));
        let bd = backtest_done.clone();

        // tokio::spawn(async move  {
        //     println!(
        //         "[?] back_tester> Launching Signal Generators for {}",
        //         config.symbol.symbol
        //     );
        //
        //     let mut process = tokio::process::Command::new("python3")
        //         .arg("./python/signals/signal_generator.py")
        //         .arg("--symbol")
        //         .arg(&config.symbol.symbol)
        //         .arg("--grpc-port")
        //         .arg(&config.grpc_server_port)
        //         .arg("--mode")
        //         .arg("Backtest")
        //         .spawn()
        //         .expect("Failed to start signal generator");
        //     while !bd.load(Ordering::Relaxed) {
        //         tokio::time::sleep(Duration::from_secs(1)).await;
        //     }
        //
        //     if let Err(e) = process.kill().await {
        //         eprintln!("Failed to kill signal generator: {}", e);
        //     }
        // });

        // wait for the signal generator to start to be safe

        mongo_client.reset_trades(&self.config.symbol).await;
        mongo_client.reset_orders(&self.config.symbol).await;

        // let klines_channel = async_broadcast::broadcast(10000);
        // let execution_commands_channel = async_broadcast::broadcast(100);
        //
        // let inner_account: Box<Arc<dyn ExchangeAccount>> = Box::new(executor.get_account().clone());
        // println!(
        //     "[?] back_tester> Initializing Risk Manager for {}",
        //     self.config.symbol.symbol
        // );
        // calculate position size based on risk tolerance and send orders to executors
        // let mut risk_manager = RiskManager::new(
        //     self.global_config.clone(),
        //     RiskManagerConfig {
        //         max_daily_losses: 100,
        //         max_risk_per_trade: 0.01,
        //     },
        //     klines_channel.1.clone(),
        //     trades_receiver,
        //     execution_commands_channel.1,
        //     inner_account,
        // );

        // let inner_account: Box<Arc<dyn ExchangeAccount>> = Box::new(executor.get_account().clone());

        // receive strategy edges from multiple strategies and forward them to risk manager
        info!(
            "Initializing Strategy Manager for {}",
            self.config.symbol.symbol
        );
        // let mut strategy_manager = StrategyManager::new(
        //     StrategyManagerConfig {
        //         symbol: self.global_config.symbol.clone(),
        //     },
        //     self.global_config.clone(),
        //     klines_channel.1,
        //     self.config.grpc_server_port.clone(),
        //     inner_account,
        // )
        // .await;

        // sends orders to executor
        // risk_manager.subscribe(orders_tx).await;

        //sends edges to risk manager
        // println!(
        //     "[?] back_tester> Starting Listeners {}",
        //     self.config.symbol.symbol
        // );
        // strategy_manager
        //     .subscribe(execution_commands_channel.0)
        //     .await;

        // let rc = Arc::new(risk_manager.clone());
        // tokio::spawn(async move {
        //     if let Err(e) = EventSink::<ExecutionCommand>::listen(rc) {
        //         eprintln!("Error in ExecutionCommand listener: {}", e);
        //     }
        // });
        // let rc = Arc::new(risk_manager.clone());
        // tokio::spawn(async move {
        //     if let Err(e) = EventSink::<Trade>::listen(rc) {
        //         eprintln!("Error in Trade listener: {}", e);
        //     }
        // });
        // let sm = Arc::new(strategy_manager.clone());
        // tokio::spawn(async move {
        //     if let Err(e) = EventSink::<Kline>::listen(sm) {
        //         eprintln!("Error in Kline listener: {}", e);
        //     }
        // });
        // let rc = Arc::new(risk_manager.clone());
        // tokio::spawn(async move {
        //     if let Err(e) = EventSink::<Kline>::listen(rc) {
        //         eprintln!("Error in Risk manager Kline listener: {}", e);
        //     }
        // });

        // let sm = strategy_manager.clone();
        // tokio::spawn(async move {
        //     sm.emit().await.unwrap().await
        // });

        // let rc = risk_manager.clone();
        // tokio::spawn(async move {
        //     rc.emit().await.unwrap().await
        // });
        //
        // let s_e = executor.clone();
        // tokio::spawn(async move {
        //     s_e.emit().await.unwrap().await
        // });


        let until = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis()
            - (self.config.length * 1000) as u128;
        info!(
            "{} Loading Data...",
            self.config.symbol.symbol
        );
        let mut klines = SQLiteClient::get_kline_stream(&sqlite_client.pool, &self.config.symbol);
        let mut tf_trade_steps = SQLiteClient::get_tf_trades_stream(&sqlite_client.pool, &self.config.symbol);

        if let Some(Err(e)) = klines.next().await {
            debug!("{:?}", e);
        }
        let count: u64 = sqlx::query_scalar("SELECT COUNT(*) FROM klines WHERE close_time > ?").bind(until.to_string()).fetch_one(&sqlite_client.pool).await.unwrap();
        info!(
            "Starting backtest for {} on {}  Klines",
            self.config.symbol.symbol, count
        );
       pb.inc_length(count);

        while let Some(Ok(kline)) = klines.next().await {
            debug!("{:?}", kline);
            'i: while let Some(Ok(mut trade)) = tf_trade_steps.next().await {
                if trade.timestamp >= kline.close_time {
                    let notify = Arc::new(Notify::new());
                    let sm_notifer = notify.notified();
                    let rm_notifier = notify.notified();
                    // send this kline to the strategy manager and let it decide what to do
                    // match klines_channel.0.broadcast((kline.clone(), Some(notify.clone()))).await {
                    //     Ok(_) => {
                    //         // sm_notifer.await;
                    //         // rm_notifier.await;
                    //     }
                    //     Err(e) => {
                    //         eprintln!(
                    //             "[-] {} Error sending kline to strategy manager {}",
                    //             self.config.symbol.symbol, e
                    //         );
                    //     }
                    // }
                    break 'i;
                }
                trade.trades = SQLiteClient::select_values_between_min_max(&sqlite_client.pool, trade.min_price_trade.to_string(), trade.max_price_trade.to_string()).await;
                debug!("{:?}", trade.trades.len());
                
                let notify = Arc::new(Notify::new());
                let sm_notifer = notify.notified();

                match tf_trades_tx.broadcast((vec![trade], Some(notify.clone()))).await {
                    Ok(_) => {
                        // only wait for all listeners to recv()
                       sm_notifer.await;

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
                        error!("{} Error: {}", self.config.symbol.symbol, e);
                    }
                }
            }
            if !self.global_config.verbose {
                pb.inc(1);
            }

        }

        // risk_manager.neutralize().await;

        backtest_done.store(true, Ordering::Relaxed);
        info!(
            "{} Backtest done",
            self.config.symbol.symbol
        );
        // wait for python process to be killed sleep for at least 2 times more
        tokio::time::sleep(Duration::from_millis(3000)).await;
        Ok(())
    }
}
