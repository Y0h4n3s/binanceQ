use std::collections::VecDeque;
use std::future::Future;
use std::time::Duration;
use anyhow::Error;
use async_std::sync::Arc;
use futures::stream::FuturesUnordered;
use async_broadcast::{Receiver, Sender};
use async_trait::async_trait;
use futures::future::BoxFuture;
use futures::StreamExt;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use binance_q_strategies::{SignalGenerator};
use binance_q_events::{EventEmitter, EventSink};
use binance_q_types::{ExecutionCommand, GlobalConfig, StrategyEdge, TfTrades};


pub struct StrategyManager {
	global_config: GlobalConfig,
	pub open_long_strategies: Arc<RwLock<Vec<Box<dyn SignalGenerator>>>>,
	pub open_short_strategies: Arc<RwLock<Vec<Box<dyn SignalGenerator>>>>,
	pub close_long_strategies: Arc<RwLock<Vec<Box<dyn SignalGenerator>>>>,
	pub close_short_strategies: Arc<RwLock<Vec<Box<dyn SignalGenerator>>>>,
	pub command_subscribers: Arc<RwLock<Sender<ExecutionCommand>>>,
	tf_trades: Arc<RwLock<Receiver<TfTrades>>>,
	tf_trades_working: Arc<std::sync::RwLock<bool>>,
	command_q: Arc<RwLock<VecDeque<ExecutionCommand>>>
}

#[async_trait]
impl EventEmitter<ExecutionCommand> for StrategyManager {
	fn get_subscribers(&self) -> Arc<RwLock<Sender<ExecutionCommand>>> {
		self.command_subscribers.clone()
	}
	async fn emit(&self) -> anyhow::Result<JoinHandle<()>> {
		let q = self.command_q.clone();
		let subs = self.command_subscribers.clone();
		Ok(tokio::spawn(async move {
			loop {
				let mut w = q.write().await;
				let first = w.pop_front();
				drop(w);
				if let Some(first) = first {
					let mut ws = subs.write().await;
					ws.broadcast(first).await;
				}
				tokio::time::sleep(Duration::from_micros(100)).await;
			}
		}))
	}
}

impl EventSink<TfTrades> for StrategyManager {
	fn get_receiver(&self) -> Arc<RwLock<Receiver<TfTrades>>> {
		self.tf_trades.clone()
	}
	fn working(&self) -> bool {
		self.tf_trades_working.read().unwrap().clone()
	}
	fn set_working(&self, working: bool) -> anyhow::Result<()> {
		*self.tf_trades_working.write().unwrap() = working;
		Ok(())
	}
	fn handle_event(&self, event_msg: TfTrades) -> anyhow::Result<JoinHandle<anyhow::Result<()>>> {
		let ols = self.open_long_strategies.clone();
		let oss = self.open_short_strategies.clone();
		let cls = self.close_long_strategies.clone();
		let css = self.close_short_strategies.clone();
		let q = self.command_q.clone();
		Ok(tokio::spawn(async move {
			let mut futures:Vec<BoxFuture<'_, ()>> = vec![];
			let oss = oss.read().await.iter().map(|s| dyn_clone::clone_box(&**s)).collect::<Vec<_>>();
			let ols = ols.read().await.iter().map(|s| dyn_clone::clone_box(&**s)).collect::<Vec<_>>();
			let cls = cls.read().await.iter().map(|s| dyn_clone::clone_box(&**s)).collect::<Vec<_>>();
			let css = css.read().await.iter().map(|s| dyn_clone::clone_box(&**s)).collect::<Vec<_>>();
			
			futures.push(Box::pin(StrategyManager::emit_open_short_signals(oss, q.clone())));
			futures.push(Box::pin(StrategyManager::emit_open_long_signals(ols, q.clone())));
			futures.push(Box::pin(StrategyManager::emit_close_long_signals(cls, q.clone())));
			futures.push(Box::pin(StrategyManager::emit_close_short_signals(css, q.clone())));
			futures::future::join_all(futures).await;
			Ok(())
		}))
		
	}
}

impl StrategyManager {
	pub fn new(global_config: GlobalConfig, tf_trades: Receiver<TfTrades>) -> Self {
		Self {
			global_config,
			open_long_strategies: Arc::new(RwLock::new(vec![])),
			open_short_strategies: Arc::new(RwLock::new(vec![])),
			close_long_strategies: Arc::new(RwLock::new(vec![])),
			close_short_strategies: Arc::new(RwLock::new(vec![])),
			command_subscribers: Arc::new(RwLock::new(async_broadcast::broadcast(1).0)),
			tf_trades: Arc::new(RwLock::new(tf_trades)),
			tf_trades_working: Arc::new(std::sync::RwLock::new(false)),
			command_q: Arc::new(RwLock::new(VecDeque::new()))
		}
	}
	
	
	
	async fn emit_open_short_signals( oss: Vec<Box<dyn SignalGenerator>>, q: Arc<RwLock<VecDeque<ExecutionCommand>>>){
		for s in oss.iter() {
			let s1 = dyn_clone::clone_box(&**s);
			let signal = s1.get_signal().await;
			match signal {
				StrategyEdge::Short(symbol, confidence) => {
					let mut w = q.write().await;
					w.push_back(ExecutionCommand::OpenShortPosition(symbol, confidence))
				}
				_ => {}
			}
		}
	}
	
	async fn emit_open_long_signals( ols: Vec<Box<dyn SignalGenerator>>, q: Arc<RwLock<VecDeque<ExecutionCommand>>>){
		for s in ols.iter() {
			let s1 = dyn_clone::clone_box(&**s);
			let signal = s1.get_signal().await;
			match signal {
				StrategyEdge::Long(symbol, confidence) => {
					let mut w = q.write().await;
					w.push_back(ExecutionCommand::OpenLongPosition(symbol, confidence))
				}
				_ => {}
				
			}
		}
	}
	
	async fn emit_close_long_signals( cls: Vec<Box<dyn SignalGenerator>>, q: Arc<RwLock<VecDeque<ExecutionCommand>>>){
		for s in cls.iter() {
			let s1 = dyn_clone::clone_box(&**s);
			let signal = s1.get_signal().await;
			match signal {
				StrategyEdge::CloseLong(symbol, confidence) => {
					let mut w = q.write().await;
					w.push_back(ExecutionCommand::CloseLongPosition(symbol, confidence))
				}
				_ => {}
			}
		}
	}
	
	async fn emit_close_short_signals( css: Vec<Box<dyn SignalGenerator>>, q: Arc<RwLock<VecDeque<ExecutionCommand>>>){
		for s in css.iter() {
			let s1 = dyn_clone::clone_box(&**s);
			let signal = s1.get_signal().await;
			match signal {
				StrategyEdge::CloseShort(symbol, confidence) => {
					let mut w = q.write().await;
					w.push_back(ExecutionCommand::CloseShortPosition(symbol, confidence))
				}
				_ => {}
			}
		}
	}
	
	pub async fn with_long_entry_strategy<S: SignalGenerator + Clone + Send + Sync + 'static>(mut self, strategy: S) -> Self {
		self.open_long_strategies.write().await.push(Box::new(strategy));
		self
	}
	
	pub async fn with_short_entry_strategy<S: SignalGenerator + Clone + Send + Sync + 'static> (mut self, strategy: S) -> Self {
		self.open_short_strategies.write().await.push(Box::new(strategy));
		self
	}
	
	pub async fn with_long_exit_strategy<S: SignalGenerator + Clone + Send + Sync + 'static> (mut self, strategy: S) -> Self {
		self.close_long_strategies.write().await.push(Box::new(strategy));
		self
	}
	
	pub async fn with_short_exit_strategy<S: SignalGenerator + Clone + Send + Sync + 'static> (mut self, strategy: S) -> Self {
		self.close_short_strategies.write().await.push(Box::new(strategy));
		self
	}
	
	pub async fn with_entry_strategy<S: SignalGenerator + Clone + Send + Sync + 'static> (mut self, strategy: S) -> Self {
		self.open_long_strategies.write().await.push(Box::new(strategy.clone()));
		self.open_short_strategies.write().await.push(Box::new(strategy));
		self
	}
	
	pub async fn with_exit_strategy<S: SignalGenerator + Clone + Send + Sync + 'static> (mut self, strategy: S) -> Self {
		self.close_long_strategies.write().await.push(Box::new(strategy.clone()));
		self.close_short_strategies.write().await.push(Box::new(strategy));
		self
	}
	pub async fn with_strategy<S: SignalGenerator + Clone + Send + Send + Sync + 'static> (mut self, strategy: S) -> Self {
		self.open_long_strategies.write().await.push(Box::new(strategy.clone()));
		self.open_short_strategies.write().await.push(Box::new(strategy.clone()));
		self.close_long_strategies.write().await.push(Box::new(strategy.clone()));
		self.close_short_strategies.write().await.push(Box::new(strategy));
		self
	}
}