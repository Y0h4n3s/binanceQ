use crate::db::client::SQLiteClient;
use crate::events::EventSink;
#[cfg(feature = "trades")]
use crate::executors::simulated::SimulatedAccount;
#[cfg(feature = "candles")]
use crate::executors::simulated_candle::SimulatedCandleAccount as SimulatedAccount;

use crate::executors::{ExchangeAccount, ExchangeAccountInfo, TradeExecutor};
use crate::managers::risk_manager::{RiskManager, RiskManagerConfig};
use crate::managers::strategy_manager::StrategyManager;
use crate::strategies::consolidation_accumulator::BreakoutAccumulationStrategy;
use crate::strategies::rsi_basic::SimpleRSIStrategy;
use crate::types::{GlobalConfig, Kline, Order, OrderStatus, Symbol, TfTrades, Trade};
use async_broadcast::{InactiveReceiver, Sender};
use async_std::sync::Arc;
use futures::{stream, StreamExt};
use indicatif::ProgressBar;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::{Mutex, Notify};
use tracing::log::debug;
use tracing::{error, info};

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
    pub fn new(global_config: GlobalConfig, config: Vec<BackTesterConfig>) -> Self {
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
        orders_tx: Sender<(OrderStatus, Option<Arc<Notify>>)>,
        account: Arc<SimulatedAccount>,
        sqlite_client: Arc<SQLiteClient>,
        #[cfg(feature = "candles")] tf1: String,
        #[cfg(feature = "candles")] tf2: String,
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
                let a = account.clone();
                let sqlite_client = sqlite_client.clone();
                let tf1 = tf1.clone();
                let tf2 = tf2.clone();
                async move {
                    back_tester
                        .run(p, t_r, t_t, o_t, a, sqlite_client, tf1, tf2)
                        .await
                        .unwrap()
                }
            })
            .buffer_unordered(20)
            .collect::<Vec<_>>()
            .await;
        // drop the idle receiver
        drop(trades_receiver);
        Ok(())
    }
}

impl BackTester {
    pub fn new(global_config: GlobalConfig, config: BackTesterConfig) -> Self {
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
        orders_tx: Sender<(OrderStatus, Option<Arc<Notify>>)>,
        account: Arc<SimulatedAccount>,
        sqlite_client: Arc<SQLiteClient>,
        #[cfg(feature = "candles")] tf1: String,
        #[cfg(feature = "candles")] tf2: String,
    ) -> anyhow::Result<()> {
        // Start the python signal generator for this symbol
        let config = self.config.clone();
        sqlite_client.reset_trades(&config.symbol).await;
        sqlite_client.reset_orders(&config.symbol).await;
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

        let klines_channel = async_broadcast::broadcast(10000);
        let execution_commands_channel = async_broadcast::broadcast(100);
        let inner_account: Box<Arc<dyn ExchangeAccountInfo>> = Box::new(account.clone());
        println!(
            "[?] back_tester> Initializing Risk Manager for {}",
            self.config.symbol.symbol
        );
        // calculate position size based on risk tolerance and send orders to executors
        let risk_manager = RiskManager::new(
            self.global_config.clone(),
            RiskManagerConfig {
                max_daily_losses: 100,
                max_risk_per_trade: 0.01,
            },
            klines_channel.1.clone().deactivate(),
            trades_receiver.clone(),
            execution_commands_channel.1.deactivate(),
            inner_account,
        );

        let inner_account: Box<Arc<dyn ExchangeAccountInfo>> = Box::new(account.clone());

        // receive strategy edges from multiple strategies and forward them to risk manager
        info!(
            "Initializing Strategy Manager for {}",
            self.config.symbol.symbol
        );
        let mut strategy_manager = StrategyManager::new(
            self.global_config.clone(),
            orders_tx,
            inner_account,
            klines_channel.1.deactivate(),
            trades_receiver,
            risk_manager,
        );
        let strategy = BreakoutAccumulationStrategy::new(14);
        strategy_manager.load_strategy(Box::new(strategy)).await;

        let until = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis()
            .checked_sub((self.config.length * 1000) as u128)
            .unwrap_or(0);
        info!("{} Loading Data...", self.config.symbol.symbol);

        #[cfg(feature = "trades")]
        {
            let count: u64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM klines WHERE symbol = ? AND close_time > ?",
            )
            .bind(&self.config.symbol.symbol)
            .bind(until.to_string())
            .fetch_one(&sqlite_client.pool)
            .await
            .unwrap();
            info!(
                "Starting backtest for {} on {}  Klines",
                self.config.symbol.symbol, count
            );
            if !self.global_config.verbose {
                pb.inc_length(count);
            }
            // preload tf_trades on another thread
            let mut tf_trades_que = Arc::new(Mutex::new(VecDeque::new()));
            let load_window = 10000;
            let symbol = self.config.symbol.clone();
            let symbol1 = self.config.symbol.clone();
            let sqlite_client = sqlite_client.clone();
            let sqlite_client1 = sqlite_client.clone();
            let trades_q = tf_trades_que.clone();
            let no_more_trades = Arc::new(AtomicBool::new(false));
            let no_more_trades1 = no_more_trades.clone();
            let verbose = self.global_config.verbose;
            let sm = Arc::new(strategy_manager.clone());

            let (res1, res2, res3) = tokio::join!(
                tokio::spawn(async move {
                    if let Err(e) = EventSink::<Kline>::listen(sm) {
                        eprintln!("Error in Kline listener: {}", e);
                    }
                }),
                tokio::spawn(async move {
                    let mut tf_trade_steps = SQLiteClient::get_tf_trades_stream_new(
                        &sqlite_client.pool,
                        &symbol,
                        until.to_string(),
                    );

                    'loader: loop {
                        if let Some(Ok(trade)) = tf_trade_steps.next().await {
                            let mut w = tf_trades_que.lock().await;

                            w.push_back(trade);
                            if w.len() > load_window {
                                drop(w);
                                tokio::time::sleep(Duration::from_millis(400)).await;
                            }
                        } else {
                            no_more_trades.store(true, Ordering::Relaxed);
                            break 'loader;
                        }
                    }
                }),
                tokio::spawn(async move {
                    let mut klines = SQLiteClient::get_kline_stream(
                        &sqlite_client1.pool,
                        symbol1.clone(),
                        until.to_string(),
                    );
                    while let Some(Ok(kline)) = klines.next().await {
                        'i: loop {
                            let mut w = trades_q.lock().await;

                            if let Some(trade) = w.pop_front() {
                                drop(w);
                                if trade.timestamp >= kline.close_time {
                                    let notify = Arc::new(Notify::new());
                                    let sm_notifer = notify.notified();
                                    // let rm_notifier = notify.notified();
                                    // send this kline to the strategy manager and let it decide what to do
                                    match klines_channel
                                        .0
                                        .broadcast((kline.clone(), Some(notify.clone())))
                                        .await
                                    {
                                        Ok(_) => {
                                            sm_notifer.await;
                                            // rm_notifier.await;
                                        }
                                        Err(e) => {
                                            eprintln!(
                                                "[-] {} Error sending kline to strategy manager {}",
                                                symbol1.symbol, e
                                            );
                                        }
                                    }
                                    break 'i;
                                }
                                let notify = Arc::new(Notify::new());
                                let sm_notifer = notify.notified();
                                match tf_trades_tx
                                    .broadcast((vec![trade], Some(notify.clone())))
                                    .await
                                {
                                    Ok(_) => {
                                        sm_notifer.await;
                                    }
                                    Err(e) => {
                                        // don't do anything, print and continue
                                        error!("Error {}: {}", symbol1.symbol, e);
                                    }
                                }
                            } else {
                                drop(w);
                                if no_more_trades1.load(Ordering::Relaxed) {
                                    break 'i;
                                }
                            }
                        }
                        if !verbose {
                            pb.inc(1);
                        }
                    }
                })
            );
            res1.unwrap();
            res2.unwrap();
            res3.unwrap();
            info!("{} Backtest done", self.config.symbol.symbol);
            Ok(())
        }

        #[cfg(feature = "candles")]
        {
            let count: u64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM tf_trades WHERE symbol = ? AND close_time > ? AND tf = ?",
            )
            .bind(&self.config.symbol.symbol)
            .bind(until.to_string())
            .bind(&tf1)
            .fetch_one(&sqlite_client.pool)
            .await
            .unwrap();
            info!(
                "Starting backtest for {} on {}  Klines",
                self.config.symbol.symbol, count
            );
            if !self.global_config.verbose {
                pb.inc_length(count);
            }
            // preload tf_trades on another thread
            let mut tf_trades_que = Arc::new(Mutex::new(VecDeque::new()));
            let load_window = 100000;
            let symbol = self.config.symbol.clone();
            let symbol1 = self.config.symbol.clone();
            let sqlite_client = sqlite_client.clone();
            let sqlite_client1 = sqlite_client.clone();
            let trades_q = tf_trades_que.clone();
            let no_more_trades = Arc::new(AtomicBool::new(false));
            let no_more_trades1 = no_more_trades.clone();
            let verbose = self.global_config.verbose;
            let sm = Arc::new(strategy_manager.clone());
            let sm1 = Arc::new(strategy_manager.clone());

            let (res1, res2, res3, res4) = tokio::join!(
                tokio::spawn(async move {
                    if let Err(e) = EventSink::<Kline>::listen(sm) {
                        eprintln!("Error in Kline listener: {}", e);
                    }
                }),
                tokio::spawn(async move {
                    if let Err(e) = EventSink::<Trade>::listen(sm1) {
                        eprintln!("Error in Trade listener: {}", e);
                    }
                }),
                tokio::spawn(async move {
                    let mut kline_steps = SQLiteClient::get_tf_trade_stream_with_tf(
                        &sqlite_client.pool,
                        symbol.clone(),
                        until.to_string(),
                        tf2,
                    );

                    'loader: loop {
                        if let Some(Ok(trade)) = kline_steps.next().await {
                            let mut w = tf_trades_que.lock().await;

                            w.push_back(trade);
                             if w.len() > load_window {
                                drop(w);
                                tokio::time::sleep(Duration::from_millis(400)).await;

                            }
                        } else {
                            no_more_trades.store(true, Ordering::Relaxed);
                            break 'loader;
                        }
                    }
                }),
                tokio::spawn(async move {
                    let mut klines = SQLiteClient::get_kline_stream_with_tf(
                        &sqlite_client1.pool,
                        symbol1.clone(),
                        until.to_string(),
                        tf1,
                    );
                    while let Some(Ok(kline)) = klines.next().await {
                            let mut trades = vec![];
                        'i: loop {
                            let mut w = trades_q.lock().await;
                            if let Some(trade) = w.pop_front() {
                                drop(w);
                                if trade.close_time >= kline.close_time {
                                    let notify = Arc::new(Notify::new());
                                    let sm_notifer = notify.notified();
                                    match tf_trades_tx
                                        .broadcast((trades, Some(notify.clone())))
                                        .await
                                    {
                                        Ok(_) => {
                                            sm_notifer.await;
                                        }
                                        Err(e) => {
                                            // don't do anything, print and continue
                                            error!("Error {}: {}", symbol1.symbol, e);
                                        }
                                    }
                                    let notify = Arc::new(Notify::new());
                                    let sm_notifer = notify.notified();
                                    // let rm_notifier = notify.notified();
                                    // send this kline to the strategy manager and let it decide what to do
                                    match klines_channel
                                        .0
                                        .broadcast((kline.clone(), Some(notify.clone())))
                                        .await
                                    {
                                        Ok(_) => {
                                            sm_notifer.await;
                                            // rm_notifier.await;
                                        }
                                        Err(e) => {
                                            eprintln!(
                                                "[-] {} Error sending kline to strategy manager {}",
                                                symbol1.symbol, e
                                            );
                                        }
                                    }
                                    break 'i;
                                } else {
                                    trades.push(trade);
                                }
                            } else {
                                drop(w);
                                if no_more_trades1.load(Ordering::Relaxed) {
                                    break 'i;
                                }
                            }
                        }
                        if !verbose {
                            pb.inc(1);
                        }
                    }
                })
            );
            res1.unwrap();
            res2.unwrap();
            res3.unwrap();
            res4.unwrap();
            info!("{} Backtest done", self.config.symbol.symbol);
            Ok(())
        }
    }
}
